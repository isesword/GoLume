package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"gocolumn/source"
)

// JoinKey maps one left column to one right column for an equality join.
type JoinKey struct{ Left, Right string }

// JoinSource is a reusable columnar hash-join source. The right side is built
// once during physical preparation; each reader probes left partitions and
// emits joined columnar batches that can feed downstream operators.
type JoinSource struct {
	left, right    source.Source
	typ            join.Type
	keys           []JoinKey
	suffix         string
	schema         memory.Schema
	bindings       []joinOutputBinding
	leftParts      []source.Partition
	parts          []source.Partition
	stats          source.Stats
	shape          joinShape
	pair           *join.PairTable
	radixPair      *join.RadixPairTable
	dict           *join.DictionaryTable
	strategy       string
	strategyReason string
	spillable      bool
	merge          bool
	boundedBuild   bool
	radixBits      uint8
	radixWorkers   int
	radixExec      join.ParallelExecutor
	leftDict       join.Dictionary
	rightDict      join.Dictionary
	rightBatches   []*memory.Batch
	buildLoc       []rowLocation
	buildBytes     int64
	reservations   []memory.Reservation
	closed         bool
	kernels        kernel.Set
	observer       observer.Observer
	feedback       func(JoinRuntimeFeedback)
}

// JoinRuntimeFeedback is one completed physical join's resource and skew
// observation. It is emitted outside row hot loops.
type JoinRuntimeFeedback struct {
	BuildBytes      int64
	ProbeBytes      int64
	ProbeRows       int64
	OutputRows      int64
	SpillBytes      int64
	MaxMultiplicity int
}

type joinShape uint8

const (
	joinFixed joinShape = iota + 1
	joinDictionary
)

type rowLocation struct{ batch, row int }
type joinOutputBinding struct {
	out, in string
	right   bool
	field   memory.Field
	dict    source.Dictionary
}

// JoinSourceOptions controls optional physical join experiments that are not
// enabled by the default cost model. RadixBits in [1,8] forces a fixed-key
// build into radix-partitioned hash tables; zero keeps the proven single-table
// default.
type JoinSourceOptions struct {
	// Kernels is the exact immutable dispatch set shared by the owning engine.
	Kernels kernel.Set
	// RadixBits in [1,8] enables radix-partitioned fixed-key hash tables.
	RadixBits uint8
	// Workers controls parallel radix build/probe. Values <=1 keep the serial
	// implementation even when RadixBits is set.
	Workers int
	// Executor supplies persistent worker resources. It is required when
	// Workers is greater than one.
	Executor join.ParallelExecutor
	// AutoParallelRadix enables the benchmark-backed large fixed-key heuristic
	// when RadixBits is zero. Explicit RadixBits always wins.
	AutoParallelRadix bool
	// Spillable selects execution-scoped radix partitioning and columnar runs
	// instead of retaining the complete build side during preparation.
	Spillable bool
	// RequireBounded rejects any physical fallback without a finite contract.
	RequireBounded bool
	// SpillConfigured reports whether the owning query can create finite runs.
	SpillConfigured bool
	// Observer receives one aggregate stage event outside row hot loops.
	Observer observer.Observer
	// MemoryLimitBytes is the finite per-query hard limit used only for
	// conservative adaptive-broadcast thresholds.
	MemoryLimitBytes int64
	// Feedback receives one aggregate runtime observation when a stream owns
	// the complete join execution.
	Feedback func(JoinRuntimeFeedback)
}

// NewJoinSource prepares a right-build hash table and derives safe output-row
// upper bounds from build-key multiplicity. The probe side is not pre-read
// during preparation; each left partition is consumed exactly when a reader
// executes it. Inputs should already include filters that must execute before
// the join.
func NewJoinSource(ctx context.Context, left, right source.Source, typ join.Type, keys []JoinKey, suffix string, manager memory.Manager, kernels kernel.Set) (*JoinSource, error) {
	return NewJoinSourceWithOptions(ctx, left, right, typ, keys, suffix, manager, JoinSourceOptions{Kernels: kernels})
}

// NewJoinSourceWithOptions prepares a reusable columnar join with explicit
// physical options. It is primarily used by benchmarks and future cost-model
// selection; production callers should normally use NewJoinSource.
func NewJoinSourceWithOptions(ctx context.Context, left, right source.Source, typ join.Type, keys []JoinKey, suffix string, manager memory.Manager, opts JoinSourceOptions) (*JoinSource, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("physical join requires left and right sources")
	}
	if !opts.Kernels.Valid() {
		return nil, fmt.Errorf("physical join requires a valid kernel set")
	}
	if len(keys) < 1 || len(keys) > 2 {
		return nil, fmt.Errorf("physical join requires one or two equality keys")
	}
	joinObserver := opts.Observer
	if joinObserver == nil {
		joinObserver = observer.Nop{}
	}
	s := &JoinSource{left: left, right: right, typ: typ, keys: append([]JoinKey(nil), keys...), suffix: suffix, radixBits: opts.RadixBits, radixWorkers: opts.Workers, radixExec: opts.Executor, kernels: opts.Kernels, spillable: opts.Spillable, observer: joinObserver, feedback: opts.Feedback}
	if s.radixBits > 8 {
		return nil, fmt.Errorf("physical join radix bits %d exceed 8", s.radixBits)
	}
	if s.radixWorkers < 0 {
		return nil, fmt.Errorf("physical join workers must be non-negative")
	}
	if s.radixWorkers > 1 && s.radixExec == nil {
		return nil, fmt.Errorf("physical join parallel radix requires executor")
	}
	ok := false
	defer func() {
		if !ok {
			s.release()
		}
	}()
	if err := s.compileBindings(ctx); err != nil {
		return nil, err
	}
	if s.spillable && !opts.SpillConfigured {
		return nil, fmt.Errorf("spillable join requires an explicit finite spill quota")
	}
	ordered := s.radixBits == 0 && mergeJoinOrdering(left, right, keys)
	initial, err := planner.ChooseJoin(planner.JoinInput{
		Ordered: ordered, Spillable: s.spillable, RequireBounded: opts.RequireBounded, MemoryLimitBytes: opts.MemoryLimitBytes,
	})
	if err != nil {
		return nil, err
	}
	if initial.Kind == planner.JoinMerge {
		s.merge, s.spillable = true, false
		s.strategy, s.strategyReason = initial.Kind.String(), initial.Reason
	}
	if s.radixBits == 0 && opts.AutoParallelRadix && s.shape == joinFixed && s.radixWorkers >= 4 && s.radixExec != nil {
		leftStats, lerr := left.Stats(ctx)
		rightStats, rerr := right.Stats(ctx)
		if lerr == nil && rerr == nil && shouldUseParallelRadixJoin(rightStats.Rows, leftStats.Rows, s.radixWorkers) {
			s.radixBits = 8
		}
	}
	if s.spillable {
		if s.canAttemptBoundedBroadcast(ctx, opts.MemoryLimitBytes) {
			s.spillable = false
			if buildErr := s.buildRight(ctx, manager); buildErr != nil {
				s.release()
				if !errors.Is(buildErr, memory.ErrBudgetExceeded) {
					return nil, buildErr
				}
				s.spillable = true
			} else {
				decision, chooseErr := planner.ChooseJoin(planner.JoinInput{
					Spillable: true, RequireBounded: opts.RequireBounded, StableReplay: true,
					MemoryLimitBytes: opts.MemoryLimitBytes, BuildBytes: s.residentStateBytes(), MaxOutputBatchBytes: s.maxResidentOutputBytes(ctx),
				})
				if chooseErr != nil {
					return nil, chooseErr
				}
				if decision.Kind == planner.JoinBroadcastHash {
					s.boundedBuild = true
					s.strategy, s.strategyReason = decision.Kind.String(), decision.Reason
				} else {
					s.release()
					s.spillable = true
				}
			}
		}
		if s.spillable {
			s.strategy, s.strategyReason = "partitioned-hash-spill", initial.Reason
		}
	} else if !s.merge {
		if err := s.buildRight(ctx, manager); err != nil {
			s.release()
			return nil, err
		}
		s.strategyReason = "resident memory-only fast path"
	}
	if err := s.initStreamingPartitions(ctx); err != nil {
		s.release()
		return nil, err
	}
	ok = true
	return s, nil
}

// Schema returns the joined output schema.
func (s *JoinSource) Schema() memory.Schema { return s.schema }

// StreamPlan reports the current resident build-side hash table as blocking.
// Phase 3.4 changes eligible strategies to spillable after partitioned join is
// implemented and verified.
func (s *JoinSource) StreamPlan() source.StreamPlanNode {
	class := source.StreamBlocking
	if s.spillable {
		class = source.StreamSpillable
	} else if s.boundedBuild {
		class = source.StreamBounded
	}
	return source.StreamPlanNode{
		Operator: "Join",
		Class:    class,
		Detail:   fmt.Sprintf("strategy=%s reason=%q", s.strategy, s.strategyReason),
		// Right and Full need one query-wide matched-build bitmap before their
		// unmatched build rows can be emitted. Running one stream per probe
		// partition would duplicate those rows or require an unbounded merge.
		PartitionParallel: !s.spillable && !s.merge && s.typ != join.Right && s.typ != join.Full,
		Children:          []source.StreamPlanNode{source.StreamPlanOf(s.left), source.StreamPlanOf(s.right)},
	}
}

func fixedJoinSchemaWidth(schema memory.Schema) (int64, bool) {
	var bytes int64
	for _, field := range schema.Fields {
		switch field.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
			memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
			bytes += 4
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
			memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
			bytes += 8
		case memory.TypeString:
			if field.Encoding != memory.EncodingDictionary {
				return 0, false
			}
			bytes += 4
		default:
			return 0, false
		}
		if field.Nullable {
			bytes++
		}
	}
	return bytes, true
}

func (s *JoinSource) canAttemptBoundedBroadcast(ctx context.Context, limit int64) bool {
	if limit <= 0 || s.radixBits != 0 || source.ReplayabilityOf(s.right) != source.ReplayStable {
		return false
	}
	rightWidth, rightFixed := fixedJoinSchemaWidth(s.right.Schema())
	_, outputFixed := fixedJoinSchemaWidth(s.schema)
	if !rightFixed || !outputFixed {
		return false
	}
	stats, err := s.right.Stats(ctx)
	if err != nil || stats.Rows < 0 {
		return false
	}
	estimate := stats.Bytes
	const maxInt64 = int64(^uint64(0) >> 1)
	if estimate <= 0 && (rightWidth <= 0 || stats.Rows > maxInt64/rightWidth) {
		return false
	}
	if estimate <= 0 {
		estimate = stats.Rows * rightWidth
	}
	return estimate > 0 && estimate <= limit/4
}

func (s *JoinSource) residentStateBytes() int64 {
	rows := len(s.buildLoc)
	tableBytes := join.EstimatePairTableBytes(maxInt(8, rows), rows)
	if s.shape == joinDictionary {
		tableBytes = join.EstimateI32TableBytes(maxInt(rows, s.rightDict.Len()), rows)
	}
	const rowLocationBytes = int64(16)
	if int64(rows) > int64(^uint64(0)>>1)/rowLocationBytes {
		return int64(^uint64(0) >> 1)
	}
	total := int64(0)
	for _, bytes := range []int64{s.buildBytes, tableBytes, int64(rows) * rowLocationBytes, int64(len(s.rightBatches)) * 8} {
		if err := addJoinEstimate(&total, bytes); err != nil {
			return int64(^uint64(0) >> 1)
		}
	}
	return total
}

func (s *JoinSource) maxResidentOutputBytes(ctx context.Context) int64 {
	width, ok := fixedJoinSchemaWidth(s.schema)
	if !ok || width <= 0 {
		return 0
	}
	parts, err := s.left.Partitions(ctx)
	if err != nil {
		return 0
	}
	var maxBytes int64
	for _, part := range parts {
		rows, mulErr := safeMulRows(part.Rows, int64(s.maxOutputMultiplicity()))
		if mulErr != nil {
			return 0
		}
		const maxInt64 = int64(^uint64(0) >> 1)
		perOutput := width*2 + 16
		if rows > maxInt64/perOutput || part.Rows > (maxInt64-rows*perOutput)/17 {
			return 0
		}
		bytes := rows*perOutput + part.Rows*17 + 1024
		if bytes > maxBytes {
			maxBytes = bytes
		}
	}
	if s.typ == join.Right || s.typ == join.Full {
		unmatched := int64(len(s.buildLoc))
		if unmatched > 4096 {
			unmatched = 4096
		}
		const maxInt64 = int64(^uint64(0) >> 1)
		perOutput := width*2 + 16
		if unmatched > maxInt64/perOutput {
			return 0
		}
		bytes := unmatched*perOutput + 1024
		if bytes > maxBytes {
			maxBytes = bytes
		}
	}
	if maxBytes <= 0 {
		return 0
	}
	return maxBytes
}

// Stats returns partition count and a safe output-row upper bound. Join output
// cardinality is discovered while probing, so RowsUpperBound is true.
func (s *JoinSource) Stats(context.Context) (source.Stats, error) { return s.stats, nil }

// Strategy returns the prepared build-side join strategy used by this source.
// Small fixed-width builds use a single hash table, large fixed-width builds
// use radix-partitioned tables, and dictionary joins use normalized codes.
func (s *JoinSource) Strategy() string { return s.strategy }

// Partitions returns one output partition per left input partition. Rows is a
// safe capacity upper bound, not a promise about the emitted batch size.
func (s *JoinSource) Partitions(context.Context) ([]source.Partition, error) {
	return append([]source.Partition(nil), s.parts...), nil
}

// Close releases memory reservations retained by the prepared build side.
func (s *JoinSource) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.release()
	return nil
}

// Dictionary exposes stable dictionary metadata for joined dictionary payloads.
func (s *JoinSource) Dictionary(_ context.Context, column string) (source.Dictionary, error) {
	for _, b := range s.bindings {
		if b.out == column && b.field.Type == memory.TypeString && b.field.Encoding == memory.EncodingDictionary {
			return b.dict, nil
		}
	}
	return source.Dictionary{}, fmt.Errorf("joined column %q has no stable dictionary", column)
}

func (s *JoinSource) compileBindings(ctx context.Context) error {
	ls, rs := s.left.Schema(), s.right.Schema()
	for _, k := range s.keys {
		lf, lok := ls.Field(k.Left)
		rf, rok := rs.Field(k.Right)
		if !lok || !rok {
			return fmt.Errorf("join key %q=%q not found", k.Left, k.Right)
		}
		if lf.Type != rf.Type {
			return fmt.Errorf("join key type mismatch %s vs %s", lf.Type, rf.Type)
		}
	}
	if len(s.keys) == 1 {
		lf, _ := ls.Field(s.keys[0].Left)
		rf, _ := rs.Field(s.keys[0].Right)
		if lf.Type == memory.TypeString {
			if lf.Encoding != memory.EncodingDictionary || rf.Encoding != memory.EncodingDictionary {
				return fmt.Errorf("string join requires dictionary encoding")
			}
			ldp, lok := s.left.(source.DictionaryProvider)
			rdp, rok := s.right.(source.DictionaryProvider)
			if !lok || !rok {
				return fmt.Errorf("dictionary join requires source dictionaries")
			}
			ld, err := ldp.Dictionary(ctx, s.keys[0].Left)
			if err != nil {
				return err
			}
			rd, err := rdp.Dictionary(ctx, s.keys[0].Right)
			if err != nil {
				return err
			}
			s.leftDict = join.Dictionary{Domain: ld.Domain, Offsets: ld.Offsets, Data: ld.Data}
			s.rightDict = join.Dictionary{Domain: rd.Domain, Offsets: rd.Offsets, Data: rd.Data}
			s.shape = joinDictionary
		}
	}
	if s.shape == 0 {
		s.shape = joinFixed
	}
	fields := append([]memory.Field(nil), ls.Fields...)
	for _, f := range ls.Fields {
		if !joinMaterializableField(f) {
			return fmt.Errorf("left join payload %q type %s is not materializable; project it away before join", f.Name, f.Type)
		}
		of := f
		if s.typ == join.Right || s.typ == join.Full {
			of.Nullable = true
		}
		fields[len(fields)-len(ls.Fields)+len(s.bindings)] = of
		b := joinOutputBinding{out: f.Name, in: f.Name, field: of}
		if f.Type == memory.TypeString && f.Encoding == memory.EncodingDictionary {
			if dp, ok := s.left.(source.DictionaryProvider); ok {
				d, _ := dp.Dictionary(ctx, f.Name)
				b.dict = d
			}
		}
		s.bindings = append(s.bindings, b)
	}
	if s.typ != join.Semi && s.typ != join.Anti {
		seen := map[string]struct{}{}
		for _, f := range fields {
			seen[f.Name] = struct{}{}
		}
		suf := s.suffix
		if suf == "" {
			suf = "_right"
		}
		for _, f := range rs.Fields {
			if !joinMaterializableField(f) {
				return fmt.Errorf("right join payload %q type %s is not materializable; project it away before join", f.Name, f.Type)
			}
			out := f.Name
			if _, dup := seen[out]; dup {
				out += suf
			}
			if _, dup := seen[out]; dup {
				return fmt.Errorf("join output column %q ambiguous", out)
			}
			seen[out] = struct{}{}
			of := f
			of.Name = out
			if s.typ == join.Left || s.typ == join.Full {
				of.Nullable = true
			}
			fields = append(fields, of)
			b := joinOutputBinding{out: out, in: f.Name, right: true, field: of}
			if f.Type == memory.TypeString && f.Encoding == memory.EncodingDictionary {
				if dp, ok := s.right.(source.DictionaryProvider); ok {
					d, _ := dp.Dictionary(ctx, f.Name)
					b.dict = d
				}
			}
			s.bindings = append(s.bindings, b)
		}
	}
	s.schema = memory.NewSchema(fields...)
	return nil
}

func (s *JoinSource) buildRight(ctx context.Context, manager memory.Manager) error {
	parts, err := s.right.Partitions(ctx)
	if err != nil {
		return err
	}
	var totalRows int64
	for _, part := range parts {
		if part.Rows < 0 || totalRows > int64(^uint64(0)>>1)-part.Rows {
			return fmt.Errorf("join build row count overflows int64")
		}
		totalRows += part.Rows
	}
	if totalRows > int64(int(^uint(0)>>1)) {
		return fmt.Errorf("join build has %d rows beyond platform capacity", totalRows)
	}
	const rowLocationBytes = int64(16)
	if totalRows > (int64(^uint64(0)>>1)-int64(len(parts))*8)/rowLocationBytes {
		return fmt.Errorf("join build metadata byte estimate overflows int64")
	}
	if manager != nil {
		persistentBytes := totalRows*rowLocationBytes + int64(len(parts))*8
		reservation, reserveErr := manager.Reserve(ctx, persistentBytes)
		if reserveErr != nil {
			return fmt.Errorf("join build location memory: %w", reserveErr)
		}
		s.reservations = append(s.reservations, reservation)
	}
	var scratch memory.Reservation
	if manager != nil && totalRows > 0 {
		// A fixed key uses 16 bytes plus one validity byte. Dictionary builds
		// need less, so this is conservative and also covers their validity
		// bitmap while the temporary code/valid slices remain live.
		if totalRows > int64(^uint64(0)>>1)/17 {
			return fmt.Errorf("join build scratch byte estimate overflows int64")
		}
		scratch, err = manager.Reserve(ctx, totalRows*17)
		if err != nil {
			return fmt.Errorf("join build key scratch memory: %w", err)
		}
		defer scratch.Release()
	}
	rd, err := source.OpenBatchStreamWithPartitions(ctx, s.right, source.ReadOptions{Kernels: s.kernels}, parts)
	if err != nil {
		return err
	}
	defer rd.Close()
	s.rightBatches = make([]*memory.Batch, 0, len(parts))
	s.buildLoc = make([]rowLocation, 0, int(totalRows))
	var fixed []hashagg.Key128
	var masks []uint8
	var dictCodes []uint32
	var dictValidRows []bool
	if s.shape == joinDictionary {
		dictCodes = make([]uint32, 0, int(totalRows))
		dictValidRows = make([]bool, 0, int(totalRows))
	} else {
		fixed = make([]hashagg.Key128, 0, int(totalRows))
		masks = make([]uint8, 0, int(totalRows))
	}
	allDictValid := true
	actualRows := int64(0)
	b := memory.NewBatch()
	defer b.ReleaseOwned()
	for {
		b.ClearColumns()
		ok, nextErr := rd.Next(ctx, b)
		if nextErr != nil {
			return nextErr
		}
		if !ok {
			break
		}
		if actualRows > totalRows-int64(b.Rows) {
			return fmt.Errorf("join build source emitted more rows than its partition contract")
		}
		actualRows += int64(b.Rows)
		if len(b.Opaque) != 0 {
			return fmt.Errorf("joined payload contains opaque Arrow columns; project them away before join")
		}
		bytes := estimateBatchBytes(b)
		s.buildBytes += bytes
		if manager != nil && bytes > 0 {
			r, e := manager.Reserve(ctx, bytes)
			if e != nil {
				return fmt.Errorf("join build payload memory: %w", e)
			}
			s.reservations = append(s.reservations, r)
		}
		clone, err := cloneBatch(b)
		if err != nil {
			return err
		}
		bi := len(s.rightBatches)
		s.rightBatches = append(s.rightBatches, clone)
		for row := 0; row < b.Rows; row++ {
			s.buildLoc = append(s.buildLoc, rowLocation{batch: bi, row: row})
			if s.shape == joinDictionary {
				v, e := b.DictionaryString(s.keys[0].Right)
				if e != nil {
					return e
				}
				dictCodes = append(dictCodes, v.Indices[row])
				valid := v.IsValid(row)
				dictValidRows = append(dictValidRows, valid)
				allDictValid = allDictValid && valid
			} else {
				k, m, e := encodePhysicalKey(b, s.keys, false, row)
				if e != nil {
					return e
				}
				fixed = append(fixed, k)
				masks = append(masks, m)
			}
		}
	}
	if actualRows != totalRows {
		return fmt.Errorf("join build source emitted %d rows, partition contract declared %d", actualRows, totalRows)
	}
	if s.shape == joinDictionary {
		s.strategy = "dictionary-hash"
		s.dict = join.NewDictionaryTable(maxInt(8, len(dictCodes)))
		var dictValid memory.Bitmap
		if !allDictValid {
			dictValid = memory.EnsureBitmap(nil, len(dictValidRows))
			for row, valid := range dictValidRows {
				if valid {
					memory.Set(dictValid, row)
				}
			}
		}
		if manager != nil {
			r, e := manager.Reserve(ctx, join.EstimateI32TableBytes(maxInt(len(dictCodes), s.rightDict.Len()), len(dictCodes)))
			if e != nil {
				return e
			}
			s.reservations = append(s.reservations, r)
		}
		return s.dict.Build(dictCodes, dictValid, s.rightDict)
	}
	if s.radixBits != 0 {
		if s.radixWorkers > 1 {
			s.strategy = fmt.Sprintf("parallel-radix-hash/%d/w%d", 1<<s.radixBits, s.radixWorkers)
		} else {
			s.strategy = fmt.Sprintf("radix-hash/%d", 1<<s.radixBits)
		}
		s.radixPair = join.NewRadixPairTable(s.radixBits)
		if manager != nil {
			r, e := manager.Reserve(ctx, join.EstimateRadixPairTableBytes(len(fixed), s.radixBits))
			if e != nil {
				return e
			}
			s.reservations = append(s.reservations, r)
		}
		if s.radixWorkers > 1 {
			return s.radixPair.BuildParallel(ctx, fixed, masks, s.radixWorkers, s.radixExec)
		}
		return s.radixPair.Build(fixed, masks)
	}
	s.strategy = "hash"
	s.pair = join.NewPairTable(maxInt(8, len(fixed)))
	if manager != nil {
		r, e := manager.Reserve(ctx, join.EstimatePairTableBytes(len(fixed), len(fixed)))
		if e != nil {
			return e
		}
		s.reservations = append(s.reservations, r)
	}
	return s.pair.Build(fixed, masks)
}

func (s *JoinSource) initStreamingPartitions(ctx context.Context) error {
	if s.spillable || s.merge {
		leftStats, err := s.left.Stats(ctx)
		if err != nil {
			return err
		}
		rightStats, err := s.right.Stats(ctx)
		if err != nil {
			return err
		}
		rows := leftStats.Rows
		if s.typ == join.Inner || s.typ == join.Left || s.typ == join.Right || s.typ == join.Full {
			var mulErr error
			rows, mulErr = safeMulRows(leftStats.Rows, rightStats.Rows)
			if mulErr != nil {
				rows = int64(^uint64(0) >> 1)
			}
			if s.typ == join.Right || s.typ == join.Full {
				if rows <= int64(^uint64(0)>>1)-rightStats.Rows {
					rows += rightStats.Rows
				}
			}
		}
		s.leftParts, _ = s.left.Partitions(ctx)
		s.parts = []source.Partition{{ID: 0, Rows: rows}}
		s.stats = source.Stats{Rows: rows, Partitions: 1, RowsUpperBound: true}
		return nil
	}
	parts, err := s.left.Partitions(ctx)
	if err != nil {
		return err
	}
	s.leftParts = append([]source.Partition(nil), parts...)
	multiplier := s.maxOutputMultiplicity()
	for i, p := range parts {
		rows, err := safeMulRows(p.Rows, int64(multiplier))
		if err != nil {
			return err
		}
		s.parts = append(s.parts, source.Partition{ID: i, Rows: rows})
		if s.stats.Rows > int64(^uint64(0)>>1)-rows {
			return fmt.Errorf("join output row upper bound overflows int64")
		}
		s.stats.Rows += rows
	}
	s.stats.Partitions = len(s.parts)
	if s.typ == join.Right || s.typ == join.Full {
		if int64(len(s.buildLoc)) > int64(^uint64(0)>>1)-s.stats.Rows {
			return fmt.Errorf("join output row upper bound overflows int64")
		}
		s.stats.Rows += int64(len(s.buildLoc))
	}
	s.stats.RowsUpperBound = true
	return nil
}

func (s *JoinSource) maxOutputMultiplicity() int {
	maxDup := 0
	if s.shape == joinDictionary && s.dict != nil {
		maxDup = s.dict.MaxMultiplicity()
	}
	if s.shape == joinFixed && s.radixPair != nil {
		maxDup = s.radixPair.MaxMultiplicity()
	} else if s.shape == joinFixed && s.pair != nil {
		maxDup = s.pair.MaxMultiplicity()
	}
	switch s.typ {
	case join.Inner, join.Right:
		return maxDup
	case join.Left, join.Full:
		if maxDup < 1 {
			return 1
		}
		return maxDup
	case join.Semi, join.Anti:
		return 1
	default:
		return 1
	}
}

func shouldUseParallelRadixJoin(buildRows, probeRows int64, workers int) bool {
	// Phase 2.7 validation on the shared EPYC host found no durable win at
	// 262K build / 1M probe, but a ~2.3x win at 1M build / 4M probe. Keep the
	// automatic region deliberately conservative until ARM and more workloads
	// are measured.
	return workers >= 4 && buildRows >= 1<<20 && probeRows >= 1<<22
}

func safeMulRows(rows, multiplier int64) (int64, error) {
	if rows < 0 || multiplier < 0 {
		return 0, fmt.Errorf("negative join row bound")
	}
	if rows == 0 || multiplier == 0 {
		return 0, nil
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if rows > maxInt64/multiplier {
		return 0, fmt.Errorf("join output row upper bound overflows int64")
	}
	return rows * multiplier, nil
}

func (s *JoinSource) probe(ctx context.Context, batch *memory.Batch, emit join.EmitFunc, radixWorkspace *join.RadixProbeWorkspace, parallelWorkspace *join.ParallelRadixProbeWorkspace, keyScratch *[]hashagg.Key128, maskScratch *[]uint8) (join.Summary, error) {
	if s.shape == joinDictionary {
		v, err := batch.DictionaryString(s.keys[0].Left)
		if err != nil {
			return join.Summary{}, err
		}
		return s.dict.Probe(v.Indices, v.Validity, s.leftDict, s.typ, emit)
	}
	if cap(*keyScratch) < batch.Rows {
		*keyScratch = make([]hashagg.Key128, batch.Rows)
	} else {
		*keyScratch = (*keyScratch)[:batch.Rows]
	}
	if cap(*maskScratch) < batch.Rows {
		*maskScratch = make([]uint8, batch.Rows)
	} else {
		*maskScratch = (*maskScratch)[:batch.Rows]
	}
	keys := *keyScratch
	masks := *maskScratch
	for i := 0; i < batch.Rows; i++ {
		k, m, e := encodePhysicalKey(batch, s.keys, true, i)
		if e != nil {
			return join.Summary{}, e
		}
		keys[i], masks[i] = k, m
	}
	if s.radixPair != nil {
		if s.radixWorkers > 1 {
			return s.radixPair.ProbeParallelWithWorkspace(ctx, keys, masks, s.typ, emit, s.radixWorkers, s.radixExec, parallelWorkspace)
		}
		return s.radixPair.ProbeWithWorkspace(keys, masks, s.typ, emit, radixWorkspace)
	}
	return s.pair.Probe(keys, masks, s.typ, emit)
}

func (s *JoinSource) release() {
	for _, r := range s.reservations {
		r.Release()
	}
	s.reservations = nil
	s.rightBatches = nil
	s.buildLoc = nil
	s.buildBytes = 0
	s.pair, s.radixPair, s.dict = nil, nil, nil
}

type joinReader struct {
	owner                  *JoinSource
	left                   source.BatchStream
	leftBatch              *memory.Batch
	pairs                  []join.Pair
	radixWorkspace         join.RadixProbeWorkspace
	parallelRadixWorkspace join.ParallelRadixProbeWorkspace
	keys                   []hashagg.Key128
	masks                  []uint8
	lastSummary            join.Summary
}

// Close releases the left-side probe stream owned by this join cursor.
func (r *joinReader) Close() error { return r.left.Close() }

// nextPairs probes one left child batch without materializing output. Keeping
// this boundary separate lets bounded callers reserve workspace before the
// potentially large joined batch is allocated.
func (r *joinReader) nextPairs(ctx context.Context) (bool, error) {
	ok, err := r.left.Next(ctx, r.leftBatch)
	if err != nil || !ok {
		return ok, err
	}
	r.pairs = r.pairs[:0]
	r.lastSummary, err = r.owner.probe(ctx, r.leftBatch, func(pair join.Pair) error { r.pairs = append(r.pairs, pair); return nil }, &r.radixWorkspace, &r.parallelRadixWorkspace, &r.keys, &r.masks)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Next probes one left child batch and materializes its joined rows into dst.
func (r *joinReader) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	ok, err := r.nextPairs(ctx)
	if !ok || err != nil {
		return ok, err
	}
	if err := materializeJoinBatch(dst, r.owner, r.leftBatch, r.pairs); err != nil {
		return false, err
	}
	return true, nil
}

func encodePhysicalKey(batch *memory.Batch, keys []JoinKey, left bool, row int) (hashagg.Key128, uint8, error) {
	var out hashagg.Key128
	mask := uint8(0)
	for i, k := range keys {
		name := k.Right
		if left {
			name = k.Left
		}
		v, valid, err := fixedPhysicalValue(batch, name, row)
		if err != nil {
			return out, 0, err
		}
		if i == 0 {
			out.A = v
		} else {
			out.B = v
		}
		if valid {
			mask |= 1 << i
		}
	}
	if len(keys) == 1 {
		if mask&1 != 0 {
			mask = 3
		} else {
			mask = 0
		}
	}
	return out, mask, nil
}
func fixedPhysicalValue(batch *memory.Batch, name string, row int) (uint64, bool, error) {
	if v, ok := batch.Int8s[name]; ok {
		return uint64(uint32(int32(v.Values[row]))), v.IsValid(row), nil
	}
	if v, ok := batch.Int16s[name]; ok {
		return uint64(uint32(int32(v.Values[row]))), v.IsValid(row), nil
	}
	if v, ok := batch.Int32s[name]; ok {
		return uint64(uint32(v.Values[row])), v.IsValid(row), nil
	}
	if v, ok := batch.Uint8s[name]; ok {
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	if v, ok := batch.Uint16s[name]; ok {
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	if v, ok := batch.Int64s[name]; ok {
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	if v, ok := batch.Uint32s[name]; ok {
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	if v, ok := batch.Uint64s[name]; ok {
		return v.Values[row], v.IsValid(row), nil
	}
	return 0, false, fmt.Errorf("join key %q must be a fixed-width integer", name)
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func joinMaterializableField(f memory.Field) bool {
	switch f.Type {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
		memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeBinary, memory.TypeList, memory.TypeStruct:
		return true
	default:
		return false
	}
}
