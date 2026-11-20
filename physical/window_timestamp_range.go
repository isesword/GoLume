package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"math/bits"
	"unsafe"
)

// WindowFrameKind distinguishes explicitly selected frame semantics.
type WindowFrameKind uint8

const (
	// WindowFrameRows is the zero/default ROWS kind, not accepted by this source.
	WindowFrameRows WindowFrameKind = iota
	// WindowFrameRange selects timestamp-distance RANGE semantics.
	WindowFrameRange
)

// TimestampRangeFrame describes inclusive trailing [t-PrecedingNS,t].
type TimestampRangeFrame struct {
	Kind        WindowFrameKind
	PrecedingNS int64
}

// TimestampRangeSpec selects outputs through nonempty, distinct aliases.
// Count results are non-null Int64; Sum is nullable Int64; Avg nullable Float64.
type TimestampRangeSpec struct {
	OrderBy, Column                                string
	Frame                                          TimestampRangeFrame
	CountRowsAlias, CountAlias, SumAlias, AvgAlias string
}

// ErrTimestampRangeNotImplemented marks the staged execution prerequisite.
// Retained for identification of archived staged failures; implemented execution
// no longer returns it.
var ErrTimestampRangeNotImplemented = errors.New("timestamp RANGE execution is not implemented")

type timestampRangeSource struct {
	input  source.Source
	spec   TimestampRangeSpec
	schema memory.Schema
}

// NewTimestampRangeSource validates the physical-only two-column RANGE profile.
// It does not insert sorting, filtering, partitioning, spill, or a Lazy plan.
func NewTimestampRangeSource(input source.Source, spec TimestampRangeSpec) (source.Source, error) {
	if input == nil || spec.OrderBy == "" || spec.Column == "" || spec.OrderBy == spec.Column || spec.Frame.Kind != WindowFrameRange || spec.Frame.PrecedingNS < 0 {
		return nil, fmt.Errorf("timestamp RANGE requires explicit RANGE, nonnegative duration and two distinct columns")
	}
	schema := input.Schema()
	if len(schema.Fields) != 2 {
		return nil, fmt.Errorf("timestamp RANGE requires exactly two input columns")
	}
	key, ok := schema.Field(spec.OrderBy)
	value, vok := schema.Field(spec.Column)
	plain := func(f memory.Field) bool {
		return f.Encoding == memory.EncodingPlain && f.DictionaryDomain == "" && f.ArrowType == "" && len(f.Children) == 0 && f.DecimalPrecision == 0 && f.DecimalScale == 0
	}
	if !ok || !vok || key.Type != memory.TypeTimestamp || key.Nullable || value.Type != memory.TypeInt64 || !plain(key) || !plain(value) {
		return nil, fmt.Errorf("timestamp RANGE requires required plain ns Timestamp and plain Int64 value")
	}
	root, err := timestampRangeLineage(input, 0)
	if err != nil {
		return nil, err
	}
	provider, ok := root.(source.OrderingProvider)
	if !ok {
		return nil, fmt.Errorf("timestamp RANGE requires original ascending source proof")
	}
	ordering := provider.Ordering()
	if len(ordering) == 0 || ordering[0].Name != spec.OrderBy || ordering[0].Descending {
		return nil, fmt.Errorf("timestamp RANGE requires original ascending source proof")
	}
	fields := append([]memory.Field(nil), schema.Fields...)
	aliases := [4]string{spec.CountRowsAlias, spec.CountAlias, spec.SumAlias, spec.AvgAlias}
	outputs := 0
	for i, alias := range aliases {
		if alias == "" {
			continue
		}
		outputs++
		for _, f := range fields {
			if f.Name == alias {
				return nil, fmt.Errorf("timestamp RANGE duplicate output alias %q", alias)
			}
		}
		typ := memory.TypeInt64
		if i == 3 {
			typ = memory.TypeFloat64
		}
		fields = append(fields, memory.Field{Name: alias, Type: typ, Nullable: i >= 2, Encoding: memory.EncodingPlain})
	}
	if outputs == 0 {
		return nil, fmt.Errorf("timestamp RANGE requires at least one output")
	}
	return &timestampRangeSource{input: input, spec: spec, schema: memory.NewSchema(fields...)}, nil
}
func (s *timestampRangeSource) Schema() memory.Schema { return memory.NewSchema(s.schema.Fields...) }
func (s *timestampRangeSource) Stats(ctx context.Context) (source.Stats, error) {
	st, err := s.input.Stats(ctx)
	st.Bytes = 0
	st.Partitions = 1
	return st, err
}
func (s *timestampRangeSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}
func (s *timestampRangeSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: s.spec.OrderBy}}
}
func (s *timestampRangeSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "TimestampRange", Class: source.StreamBounded, Detail: "physical-only; inclusive timestamp RANGE; complete peers; finite shared memory", Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)}}
}
func (s *timestampRangeSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("timestamp RANGE requires context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q == nil || q.Limit() <= 0 {
		return nil, fmt.Errorf("timestamp RANGE requires shared finite QueryBudget")
	}
	opts = opts.Normalize()
	if opts.BatchPayloadTargetBytes != 0 {
		return nil, fmt.Errorf("timestamp RANGE does not support a soft payload target")
	}
	manager := q.Operator("TimestampRange", memory.MemoryRetainedState)
	lease, err := manager.Reserve(ctx, 4096)
	if err != nil {
		return nil, err
	}
	// Revalidate original proof and current schema after state admission but
	// before opening the child. The fixed state allowance covers these small
	// temporary two-column descriptors.
	current, validationErr := NewTimestampRangeSource(s.input, s.spec)
	if validationErr != nil {
		lease.Release()
		return nil, validationErr
	}
	for _, f := range s.schema.Fields {
		now, ok := current.Schema().Field(f.Name)
		if !ok || now.Type != f.Type || now.Nullable != f.Nullable {
			lease.Release()
			return nil, fmt.Errorf("timestamp RANGE source schema changed")
		}
	}
	opts.MemoryManager = q.Operator("SourceScan", memory.MemoryWorkspace)
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		lease.Release()
		return nil, err
	}
	stream := &timestampRangeStream{src: s, base: ctx, query: q, manager: manager, child: child, input: memory.NewBatch(), baseLease: lease, opts: opts, pageRows: min(opts.BatchRows, 1024)}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, stream.cleanup())
	}
	return stream, nil
}

func timestampRangeLineage(input source.Source, depth int) (source.Source, error) {
	if input == nil || depth > 16 {
		return nil, fmt.Errorf("timestamp RANGE lineage exceeds bound")
	}
	switch n := input.(type) {
	case *ProjectSource:
		for _, named := range n.named {
			col, ok := named.Expr.(expr.Column)
			if !ok || named.Alias != col.Name {
				return nil, fmt.Errorf("timestamp RANGE requires identity column projection")
			}
		}
		return timestampRangeLineage(n.input, depth+1)
	case *DropColumnsSource:
		return timestampRangeLineage(n.input, depth+1)
	default:
		plan := source.StreamPlanOf(input)
		if plan.Operator != "Scan" || plan.Class != source.StreamPipeline || len(plan.Children) != 0 {
			return nil, fmt.Errorf("timestamp RANGE requires original scan lineage")
		}
		return input, nil
	}
}

// timestampRangeRow is the single owned representation used by both the active
// frame and its pending peer. It never borrows a source vector.
type timestampRangeRow struct {
	timestamp, value int64
	valid            bool
}
type timestampRangeStream struct {
	src                                   *timestampRangeSource
	base                                  context.Context
	query                                 *memory.QueryBudget
	manager                               memory.Manager
	child                                 source.BatchStream
	input                                 *memory.Batch
	inputAt                               int
	queue                                 []timestampRangeRow
	head, count                           int
	total, validCount                     int64
	sum                                   windowExactSum
	baseLease, queueLease, pageLease      memory.Reservation
	borrowed                              *memory.Batch
	opts                                  source.ReadOptions
	pageRows, peerStart, peerRows, peerAt int
	peerReady                             bool
	frameRows, frameCount, frameSum       int64
	frameAvg                              float64
	prev                                  int64
	havePrev, eof, closed                 bool
	terminal                              error
}

func (s *timestampRangeStream) check(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("timestamp RANGE Next requires context")
	}
	if err := s.base.Err(); err != nil {
		return err
	}
	return ctx.Err()
}
func (s *timestampRangeStream) at(i int) timestampRangeRow { return s.queue[(s.head+i)%len(s.queue)] }
func (s *timestampRangeStream) grow(ctx context.Context) error {
	if s.count < len(s.queue) {
		return nil
	}
	width := int64(unsafe.Sizeof(timestampRangeRow{}))
	maxCapacity := int64(int(^uint(0)>>1)) / width
	old := int64(len(s.queue))
	capacity := int64(16)
	if old > 0 {
		if old > maxCapacity/2 {
			capacity = maxCapacity
		} else {
			capacity = old * 2
		}
	}
	if capacity <= old || capacity > maxCapacity {
		return memory.ErrBudgetExceeded
	}
	lease, err := s.manager.Reserve(ctx, capacity*width)
	if err != nil {
		return err
	}
	if err := s.check(ctx); err != nil {
		lease.Release()
		return err
	}
	next := make([]timestampRangeRow, int(capacity))
	for i := 0; i < s.count; i++ {
		if i&255 == 0 {
			if err := s.check(ctx); err != nil {
				lease.Release()
				return err
			}
		}
		next[i] = s.at(i)
	}
	previous := s.queueLease
	s.queue = next
	s.head = 0
	s.queueLease = lease
	if previous != nil {
		previous.Release()
	}
	return s.check(ctx)
}
func (s *timestampRangeStream) append(ctx context.Context, row timestampRangeRow) error {
	if s.total == math.MaxInt64 {
		return fmt.Errorf("timestamp RANGE input row count overflow")
	}
	if err := s.grow(ctx); err != nil {
		return err
	}
	s.queue[(s.head+s.count)%len(s.queue)] = row
	s.count++
	s.total++
	if row.valid {
		s.validCount++
		s.sum.addSigned(row.value, 1)
	}
	s.prev = row.timestamp
	s.havePrev = true
	s.inputAt++
	return nil
}
func (s *timestampRangeStream) expire(ctx context.Context, t int64) error {
	lower := int64(math.MinInt64)
	if t >= math.MinInt64+s.src.spec.Frame.PrecedingNS {
		lower = t - s.src.spec.Frame.PrecedingNS
	}
	for s.count > 0 && s.at(0).timestamp < lower {
		if err := s.check(ctx); err != nil {
			return err
		}
		row := s.queue[s.head]
		s.queue[s.head] = timestampRangeRow{}
		s.head = (s.head + 1) % len(s.queue)
		s.count--
		if row.valid {
			s.validCount--
			s.sum.addSigned(row.value, -1)
		}
	}
	return nil
}
func (s *timestampRangeStream) validateBatch(ctx context.Context) error {
	b := s.input
	k, v := b.Int64s[s.src.spec.OrderBy], b.Int64s[s.src.spec.Column]
	if b.Rows < 0 || k == nil || v == nil || len(k.Values) != b.Rows || len(v.Values) != b.Rows || len(b.Int64s) != 2 ||
		(k.Encoding != memory.EncodingPlain && k.Encoding != memory.EncodingView) || (v.Encoding != memory.EncodingPlain && v.Encoding != memory.EncodingView) ||
		(k.Validity != nil && len(k.Validity) < memory.WordsFor(b.Rows)) || (v.Validity != nil && len(v.Validity) < memory.WordsFor(b.Rows)) {
		return fmt.Errorf("invalid timestamp RANGE input storage")
	}
	if len(b.Int8s)+len(b.Int16s)+len(b.Int32s)+len(b.Uint8s)+len(b.Uint16s)+len(b.Uint32s)+len(b.Uint64s)+len(b.Float32s)+len(b.Float64s)+len(b.Decimals)+len(b.Strings)+len(b.DictStrings)+len(b.StringLists)+len(b.Int64Lists)+len(b.Structs)+len(b.StructLists)+len(b.Arrays)+len(b.Opaque) != 0 {
		return fmt.Errorf("timestamp RANGE input has undeclared storage")
	}
	field, _ := s.src.schema.Field(s.src.spec.Column)
	for i := 0; i < b.Rows; i++ {
		if i&255 == 0 {
			if err := s.check(ctx); err != nil {
				return err
			}
		}
		if k.Validity != nil && !memory.IsSet(k.Validity, i) {
			return fmt.Errorf("timestamp RANGE order key is NULL")
		}
		if !field.Nullable && v.Validity != nil && !memory.IsSet(v.Validity, i) {
			return fmt.Errorf("timestamp RANGE required value is NULL")
		}
	}
	return nil
}
func (s *timestampRangeStream) peek(ctx context.Context) (timestampRangeRow, bool, error) {
	for !s.eof && s.inputAt >= s.input.Rows {
		if err := s.check(ctx); err != nil {
			return timestampRangeRow{}, false, err
		}
		s.input.ClearColumns()
		s.inputAt = 0
		childCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(s.base, cancel)
		ok, err := s.child.Next(childCtx, s.input)
		stop()
		cancel()
		if err != nil {
			return timestampRangeRow{}, false, err
		}
		if err = s.check(ctx); err != nil {
			return timestampRangeRow{}, false, err
		}
		if !ok {
			child := s.child
			s.child = nil
			s.eof = true
			s.input.ClearColumns()
			err = child.Close()
			err = errors.Join(err, s.check(ctx))
			return timestampRangeRow{}, false, err
		}
		if err = s.validateBatch(ctx); err != nil {
			return timestampRangeRow{}, false, err
		}
	}
	if s.eof {
		return timestampRangeRow{}, false, nil
	}
	k, v := s.input.Int64s[s.src.spec.OrderBy], s.input.Int64s[s.src.spec.Column]
	row := timestampRangeRow{timestamp: k.Values[s.inputAt], valid: v.Validity == nil || memory.IsSet(v.Validity, s.inputAt)}
	if row.valid {
		row.value = v.Values[s.inputAt]
	}
	if s.havePrev && row.timestamp < s.prev {
		return timestampRangeRow{}, false, fmt.Errorf("timestamp RANGE input is not nondecreasing")
	}
	return row, true, nil
}
func timestampRangeInt64(sum *windowExactSum) (int64, error) {
	if sum.high == 0 {
		return 0, nil
	}
	significant := (sum.high-1)*64 + bits.Len64(sum.words[sum.high-1])
	if significant > 1074+64 {
		return 0, fmt.Errorf("timestamp RANGE complete-window SUM overflows Int64")
	}
	magnitude := sum.shiftedWord(1074)
	limit := uint64(math.MaxInt64)
	if sum.negativeFinite {
		limit++
	}
	if magnitude > limit {
		return 0, fmt.Errorf("timestamp RANGE complete-window SUM overflows Int64")
	}
	if sum.negativeFinite {
		return int64(-magnitude), nil
	}
	return int64(magnitude), nil
}
func (s *timestampRangeStream) preparePeer(ctx context.Context) (bool, error) {
	first, ok, err := s.peek(ctx)
	if err != nil || !ok {
		return false, err
	}
	if err = s.expire(ctx, first.timestamp); err != nil {
		return false, err
	}
	s.peerStart = s.count
	s.peerRows = 0
	s.peerAt = 0
	for {
		row, ok, e := s.peek(ctx)
		if e != nil {
			return false, e
		}
		if !ok || row.timestamp != first.timestamp {
			break
		}
		if err = s.append(ctx, row); err != nil {
			return false, err
		}
		s.peerRows++
	}
	s.frameRows = int64(s.count)
	s.frameCount = s.validCount
	if s.validCount > 0 {
		if s.src.spec.SumAlias != "" {
			s.frameSum, err = timestampRangeInt64(&s.sum)
			if err != nil {
				return false, err
			}
		}
		if s.src.spec.AvgAlias != "" {
			s.frameAvg = s.sum.result(s.validCount)
		}
	}
	if err = s.check(ctx); err != nil {
		return false, err
	}
	s.peerReady = true
	return true, nil
}
func (s *timestampRangeStream) clearPage() {
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
	}
	if s.pageLease != nil {
		s.pageLease.Release()
		s.pageLease = nil
	}
}
func (s *timestampRangeStream) publish(ctx context.Context, dst *memory.Batch) error {
	n := min(s.pageRows, s.peerRows-s.peerAt)
	nullable := int64(0)
	for _, field := range s.src.schema.Fields {
		if field.Nullable {
			nullable++
		}
	}
	pageBytes := func(rows int) int64 {
		return int64(rows)*int64(len(s.src.schema.Fields))*8 + nullable*int64(memory.WordsFor(rows))*8
	}
	fits := func(rows int) bool {
		size := pageBytes(rows)
		return size <= s.opts.BatchBytes && (s.opts.BatchPayloadBytes <= 0 || size <= s.opts.BatchPayloadBytes)
	}
	if !fits(1) {
		limit := s.opts.BatchBytes
		if s.opts.BatchPayloadBytes > 0 && s.opts.BatchPayloadBytes < limit {
			limit = s.opts.BatchPayloadBytes
		}
		return &memory.ResourceError{Scope: "batch", Operator: "TimestampRange", Class: memory.MemoryWorkspace, RequestedBytes: pageBytes(1), LimitBytes: limit, Cause: memory.ErrBudgetExceeded}
	}
	if !fits(n) {
		lo, hi := 1, n
		for lo < hi {
			mid := lo + (hi-lo+1)/2
			if fits(mid) {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		n = lo
	}
	words := memory.WordsFor(n)
	// Full sparse maps, vector descriptors, all values and nullable masks are
	// admitted before allocation. No source or queue storage aliases the page.
	charge := int64(4096) + int64(len(s.src.schema.Fields))*(256+int64(n)*8+int64(words)*8)
	lease, err := s.manager.Reserve(ctx, charge)
	if err != nil {
		return err
	}
	s.pageLease = lease
	if err = s.check(ctx); err != nil {
		return err
	}
	page := memory.NewBatch()
	for _, f := range s.src.schema.Fields {
		validity := memory.Bitmap(nil)
		if f.Nullable {
			validity = make(memory.Bitmap, words)
		}
		if f.Type == memory.TypeFloat64 {
			page.Float64s[f.Name] = &memory.Float64Vector{Values: make([]float64, n), Validity: validity}
		} else {
			page.Int64s[f.Name] = &memory.Int64Vector{Values: make([]int64, n), Validity: validity}
		}
	}
	for i := 0; i < n; i++ {
		if err = s.check(ctx); err != nil {
			return err
		}
		row := s.at(s.peerStart + s.peerAt + i)
		page.Int64s[s.src.spec.OrderBy].Values[i] = row.timestamp
		value := page.Int64s[s.src.spec.Column]
		value.Values[i] = row.value
		if row.valid && value.Validity != nil {
			memory.Set(value.Validity, i)
		}
		if alias := s.src.spec.CountRowsAlias; alias != "" {
			page.Int64s[alias].Values[i] = s.frameRows
		}
		if alias := s.src.spec.CountAlias; alias != "" {
			page.Int64s[alias].Values[i] = s.frameCount
		}
		if s.frameCount > 0 {
			if alias := s.src.spec.SumAlias; alias != "" {
				page.Int64s[alias].Values[i] = s.frameSum
				memory.Set(page.Int64s[alias].Validity, i)
			}
			if alias := s.src.spec.AvgAlias; alias != "" {
				page.Float64s[alias].Values[i] = s.frameAvg
				memory.Set(page.Float64s[alias].Validity, i)
			}
		}
	}
	if err = s.check(ctx); err != nil {
		return err
	}
	bindAll(dst, page)
	dst.Rows = n
	s.borrowed = dst
	s.peerAt += n
	if s.peerAt == s.peerRows {
		s.peerReady = false
	}
	return nil
}
func (s *timestampRangeStream) cleanup() error {
	s.clearPage()
	s.queue = nil
	if s.queueLease != nil {
		s.queueLease.Release()
		s.queueLease = nil
	}
	var err error
	if s.child != nil {
		child := s.child
		s.child = nil
		err = child.Close()
	}
	if s.input != nil {
		s.input.ClearColumns()
	}
	if s.baseLease != nil {
		s.baseLease.Release()
		s.baseLease = nil
	}
	return err
}
func (s *timestampRangeStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.clearPage()
	if dst == nil {
		return false, s.fail(fmt.Errorf("timestamp RANGE requires output batch"))
	}
	dst.ClearColumns()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if err := s.check(ctx); err != nil {
		return false, s.fail(err)
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil && q != s.query {
		return false, s.fail(fmt.Errorf("timestamp RANGE cannot change QueryBudget"))
	} else if q == nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
	}
	if !s.peerReady {
		ok, err := s.preparePeer(ctx)
		if err != nil {
			return false, s.fail(err)
		}
		if !ok {
			err = s.cleanup()
			err = errors.Join(err, s.check(ctx))
			if err != nil {
				return false, s.fail(err)
			}
			return false, nil
		}
	}
	if err := s.publish(ctx, dst); err != nil {
		return false, s.fail(err)
	}
	if err := s.check(ctx); err != nil {
		return false, s.fail(err)
	}
	return true, nil
}
func (s *timestampRangeStream) fail(err error) error {
	s.terminal = errors.Join(err, s.cleanup())
	return s.terminal
}
func (s *timestampRangeStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.cleanup()
}
