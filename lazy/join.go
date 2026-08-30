package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/source"
	"sync"
)

// ErrPreparedJoinClosed is returned when Execute is called after PreparedJoin.Close.
var ErrPreparedJoinClosed = errors.New("prepared join closed")

// JoinType is the relational join mode used by LazyFrame.Join.
type JoinType = join.Type

const (
	// InnerJoin emits only matching left/right rows.
	InnerJoin JoinType = join.Inner
	// LeftJoin emits all left rows and uses an invalid right RowRef for misses.
	LeftJoin JoinType = join.Left
	// SemiJoin emits each left row once when at least one right match exists.
	SemiJoin JoinType = join.Semi
	// AntiJoin emits left rows that have no right-side match.
	AntiJoin JoinType = join.Anti
	// RightJoin emits all right rows and NULL left payloads for misses.
	RightJoin JoinType = join.Right
	// FullJoin emits matched rows and unmatched rows from both inputs.
	FullJoin JoinType = join.Full
)

// JoinOptions configures a lazy equality join. LeftOn and RightOn must contain
// one or two corresponding key columns. Suffix disambiguates duplicate right
// output column names in the logical schema.
type JoinOptions struct {
	Type    JoinType
	LeftOn  []string
	RightOn []string
	Suffix  string
	// RadixBits forces the experimental fixed-key radix join. Zero keeps the
	// default single-table hash join selected by the current planner.
	RadixBits uint8
	// Workers controls parallel radix build/probe when RadixBits is non-zero.
	// Zero uses the session engine worker count.
	Workers int
}

// RowRef identifies a row by source partition and row offset. Valid is false
// for the missing right side of left and anti join output rows.
type RowRef struct {
	Partition int
	Row       int
	Valid     bool
}

// JoinedRow identifies one logical join output row without prematurely
// materializing payload columns.
type JoinedRow struct {
	Left  RowRef
	Right RowRef
}

// JoinResult contains terminal lazy join row references and summary counters.
// Row references remain relative to the post-pushdown physical inputs.
type JoinResult struct {
	Rows            []JoinedRow
	LeftRows        int
	RightRows       int
	MatchedLeftRows int
	OutputRows      int
	Type            JoinType
	LeftPushdown    source.PushdownReport
	RightPushdown   source.PushdownReport
}

// PreparedJoin is a compiled terminal equality join. The immutable build-side
// hash table is initialized once and reused by subsequent Execute calls.
type PreparedJoin struct {
	typ         JoinType
	left        joinSide
	right       joinSide
	keys        []logical.JoinKey
	kernel      kernel.Set
	buildMu     sync.Mutex
	built       bool
	build       preparedBuild
	manager     memory.Manager
	reservation memory.Reservation
	lifeMu      sync.Mutex
	active      sync.WaitGroup
	closed      bool
}

type joinSide struct {
	src       source.Source
	predicate expr.Program
	parts     []source.Partition
	report    source.PushdownReport
}

type joinShape uint8

const (
	joinShapeFixed joinShape = iota + 1
	joinShapeDictionary
)

type preparedBuild struct {
	shape    joinShape
	pair     *join.PairTable
	dict     *join.DictionaryTable
	refs     []RowRef
	dictMeta join.Dictionary
}

// Join appends an equality-join node without executing either input.
func (f LazyFrame) Join(right LazyFrame, opts JoinOptions) LazyFrame {
	if f.err != nil {
		return f
	}
	if right.err != nil {
		f.err = right.err
		return f
	}
	if f.session == nil || right.session == nil || f.session != right.session {
		f.err = fmt.Errorf("lazy join inputs must belong to the same Session")
		return f
	}
	if len(opts.LeftOn) != len(opts.RightOn) || len(opts.LeftOn) < 1 || len(opts.LeftOn) > 2 {
		f.err = fmt.Errorf("lazy join requires one or two corresponding key columns")
		return f
	}
	keys := make([]logical.JoinKey, len(opts.LeftOn))
	for i := range keys {
		keys[i] = logical.JoinKey{Left: opts.LeftOn[i], Right: opts.RightOn[i]}
	}
	if f.hasExecutionPolicy && right.hasExecutionPolicy && f.executionPolicy != right.executionPolicy {
		f.err = fmt.Errorf("lazy join inputs have conflicting execution policies %s and %s", f.executionPolicy, right.executionPolicy)
		return f
	}
	if !f.hasExecutionPolicy && right.hasExecutionPolicy {
		f.executionPolicy = right.executionPolicy
		f.hasExecutionPolicy = true
	}
	f.plan = &logical.Join{Left: f.plan, Right: right.plan, Type: opts.Type, Keys: keys, Suffix: opts.Suffix, RadixBits: opts.RadixBits, Workers: opts.Workers}
	return f
}

// PrepareJoin optimizes a terminal lazy join into reusable row-reference hash-join
// state. It remains the low-materialization terminal API; general downstream lazy
// pipelines use PrepareFrame or Prepare and the columnar physical JoinSource.
func (f LazyFrame) PrepareJoin(ctx context.Context) (*PreparedJoin, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.session == nil || f.session.engine == nil {
		return nil, fmt.Errorf("lazy frame has no execution session")
	}
	plan := f.plan
	if root, ok := plan.(*logical.Join); ok {
		plan = trimTerminalJoinInputs(root)
	}
	optimized, err := optimizer.Optimize(plan)
	if err != nil {
		return nil, err
	}
	n, ok := optimized.Plan.(*logical.Join)
	if !ok {
		return nil, fmt.Errorf("PrepareJoin requires a terminal Join node, got %T", optimized.Plan)
	}
	left, err := prepareJoinSide(ctx, n.Left)
	if err != nil {
		return nil, fmt.Errorf("left join input: %w", err)
	}
	right, err := prepareJoinSide(ctx, n.Right)
	if err != nil {
		return nil, fmt.Errorf("right join input: %w", err)
	}
	if err := validateJoinKeys(left.src.Schema(), right.src.Schema(), n.Keys); err != nil {
		return nil, err
	}
	return &PreparedJoin{typ: n.Type, left: left, right: right, keys: append([]logical.JoinKey(nil), n.Keys...), kernel: f.session.engine.KernelSet(), manager: f.session.engine.Memory()}, nil
}

// CollectJoin prepares and executes a terminal lazy join once.
func (f LazyFrame) CollectJoin(ctx context.Context) (JoinResult, error) {
	p, err := f.PrepareJoin(ctx)
	if err != nil {
		return JoinResult{}, err
	}
	defer p.Close()
	return p.Execute(ctx)
}

// Explain returns physical terminal-join diagnostics after source pushdown.
func (p *PreparedJoin) Explain() string {
	if p == nil {
		return "JOIN <nil>"
	}
	return fmt.Sprintf("JOIN type=%s keys=%d left_partitions=%d/%d right_partitions=%d/%d build=right",
		p.typ, len(p.keys), p.left.report.SelectedPartitions, p.left.report.TotalPartitions, p.right.report.SelectedPartitions, p.right.report.TotalPartitions)
}

// Execute probes the retained build-side table and returns logical output row
// references. The build side is initialized only on the first execution.
func (p *PreparedJoin) Execute(ctx context.Context) (JoinResult, error) {
	if p == nil {
		return JoinResult{}, fmt.Errorf("nil PreparedJoin")
	}
	if err := p.begin(); err != nil {
		return JoinResult{}, err
	}
	defer p.active.Done()
	if err := p.ensureBuild(ctx); err != nil {
		return JoinResult{}, err
	}
	result := JoinResult{Type: p.typ, RightRows: len(p.build.refs), LeftPushdown: p.left.report, RightPushdown: p.right.report}
	matchedBuild := make([]bool, len(p.build.refs))
	reader, err := p.left.src.NewReader(ctx, source.ReadOptions{Kernels: p.kernel})
	if err != nil {
		return JoinResult{}, err
	}
	defer reader.Close()
	batch := memory.NewBatch()
	var ew expr.EvalWorkspace
	for _, part := range p.left.parts {
		if err := reader.ReadPartition(ctx, part, batch); err != nil {
			return JoinResult{}, err
		}
		selected, err := selectedRows(batch, p.left.predicate, &ew, p.kernel)
		if err != nil {
			return JoinResult{}, err
		}
		result.LeftRows += len(selected)
		leftRefs := make([]RowRef, len(selected))
		for i, row := range selected {
			leftRefs[i] = RowRef{Partition: part.ID, Row: row, Valid: true}
		}
		summary, err := p.probeBatch(batch, selected, func(pair join.Pair) error {
			jr := JoinedRow{Left: leftRefs[pair.ProbeRow], Right: RowRef{Valid: false}}
			if (p.typ == InnerJoin || p.typ == LeftJoin || p.typ == RightJoin || p.typ == FullJoin) && pair.BuildRow >= 0 {
				jr.Right = p.build.refs[pair.BuildRow]
				matchedBuild[pair.BuildRow] = true
			}
			result.Rows = append(result.Rows, jr)
			return nil
		})
		if err != nil {
			return JoinResult{}, err
		}
		result.MatchedLeftRows += summary.MatchedProbeRows
		result.OutputRows += summary.OutputRows
	}
	if p.typ == RightJoin || p.typ == FullJoin {
		for buildRow, matched := range matchedBuild {
			if matched {
				continue
			}
			result.Rows = append(result.Rows, JoinedRow{Left: RowRef{Valid: false}, Right: p.build.refs[buildRow]})
			result.OutputRows++
		}
	}
	return result, nil
}

func trimTerminalJoinInputs(n *logical.Join) logical.Plan {
	leftKeys := make([]string, len(n.Keys))
	rightKeys := make([]string, len(n.Keys))
	for i, key := range n.Keys {
		leftKeys[i], rightKeys[i] = key.Left, key.Right
	}
	return &logical.Join{
		Left: projectJoinInput(n.Left, leftKeys), Right: projectJoinInput(n.Right, rightKeys),
		Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers,
	}
}

func projectJoinInput(plan logical.Plan, keys []string) logical.Plan {
	required := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		required[key] = struct{}{}
	}
	var walk func(logical.Plan)
	walk = func(node logical.Plan) {
		switch n := node.(type) {
		case *logical.Filter:
			for _, col := range expr.Columns(n.Predicate) {
				required[col] = struct{}{}
			}
			walk(n.Input)
		case *logical.Projection:
			walk(n.Input)
		}
	}
	walk(plan)
	columns := make([]string, 0, len(required))
	if schema, err := logical.InferSchema(plan); err == nil {
		for _, f := range schema.Fields {
			if _, ok := required[f.Name]; ok {
				columns = append(columns, f.Name)
			}
		}
	}
	if len(columns) == 0 {
		return plan
	}
	return &logical.Projection{Input: plan, Columns: columns}
}

func prepareJoinSide(ctx context.Context, plan logical.Plan) (joinSide, error) {
	physical, report, err := applyPushdown(ctx, plan)
	if err != nil {
		return joinSide{}, err
	}
	scan, predicate, err := lowerScanPipeline(physical)
	if err != nil {
		return joinSide{}, err
	}
	program, _, err := expr.CompilePredicate(scan.Source.Schema(), predicate)
	if err != nil {
		return joinSide{}, err
	}
	parts, err := scan.Source.Partitions(ctx)
	if err != nil {
		return joinSide{}, err
	}
	return joinSide{src: scan.Source, predicate: program, parts: parts, report: report}, nil
}

func lowerScanPipeline(plan logical.Plan) (*logical.Scan, expr.Expr, error) {
	var predicate expr.Expr
	for {
		switch n := plan.(type) {
		case *logical.Filter:
			predicate = expr.MergeAnd(predicate, n.Predicate)
			plan = n.Input
		case *logical.Projection:
			plan = n.Input
		case *logical.Scan:
			return n, predicate, nil
		default:
			return nil, nil, fmt.Errorf("join input must lower to Scan/Filter/Projection, got %T", plan)
		}
	}
}

func validateJoinKeys(left, right memory.Schema, keys []logical.JoinKey) error {
	for _, key := range keys {
		lf, lok := left.Field(key.Left)
		rf, rok := right.Field(key.Right)
		if !lok || !rok {
			return fmt.Errorf("join key %q=%q not present after pushdown", key.Left, key.Right)
		}
		if lf.Type != rf.Type {
			return fmt.Errorf("join key type mismatch %q=%s vs %q=%s", key.Left, lf.Type, key.Right, rf.Type)
		}
		if lf.Type == memory.TypeString && (lf.Encoding != memory.EncodingDictionary || rf.Encoding != memory.EncodingDictionary) {
			return fmt.Errorf("phase-2.4 lazy string join requires dictionary encoding")
		}
	}
	return nil
}

func (p *PreparedJoin) begin() error {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.closed {
		return ErrPreparedJoinClosed
	}
	p.active.Add(1)
	return nil
}

// Close waits for active probes and releases retained build-side memory charged
// to the engine memory manager. It is safe to call more than once.
func (p *PreparedJoin) Close() error {
	if p == nil {
		return nil
	}
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return nil
	}
	p.closed = true
	p.lifeMu.Unlock()
	p.active.Wait()
	p.buildMu.Lock()
	if p.reservation != nil {
		p.reservation.Release()
		p.reservation = nil
	}
	p.build = preparedBuild{}
	p.built = false
	p.buildMu.Unlock()
	return nil
}

func (p *PreparedJoin) ensureBuild(ctx context.Context) error {
	p.buildMu.Lock()
	defer p.buildMu.Unlock()
	if p.built {
		return nil
	}
	if err := p.buildRight(ctx); err != nil {
		// A canceled or otherwise transient first execution must not permanently
		// poison the PreparedJoin. The next Execute may retry the immutable build.
		p.build = preparedBuild{}
		return err
	}
	p.built = true
	return nil
}

func (p *PreparedJoin) buildRight(ctx context.Context) error {
	reader, err := p.right.src.NewReader(ctx, source.ReadOptions{Kernels: p.kernel})
	if err != nil {
		return err
	}
	defer reader.Close()
	batch := memory.NewBatch()
	var ew expr.EvalWorkspace
	var refs []RowRef
	if len(p.keys) == 1 {
		lf, _ := p.left.src.Schema().Field(p.keys[0].Left)
		if lf.Type == memory.TypeString && lf.Encoding == memory.EncodingDictionary {
			p.build.shape = joinShapeDictionary
			p.build.dict = join.NewDictionaryTable(1024)
			rdp, ok := p.right.src.(source.DictionaryProvider)
			if !ok {
				return fmt.Errorf("right dictionary source does not expose stable dictionary metadata")
			}
			d, err := rdp.Dictionary(ctx, p.keys[0].Right)
			if err != nil {
				return err
			}
			p.build.dictMeta = join.Dictionary{Domain: d.Domain, Offsets: d.Offsets, Data: d.Data}
			var all []uint32
			var valid memory.Bitmap
			for _, part := range p.right.parts {
				if err := reader.ReadPartition(ctx, part, batch); err != nil {
					return err
				}
				selected, err := selectedRows(batch, p.right.predicate, &ew, p.kernel)
				if err != nil {
					return err
				}
				v, err := batch.DictionaryString(p.keys[0].Right)
				if err != nil {
					return err
				}
				base := len(all)
				for _, row := range selected {
					all = append(all, v.Indices[row])
					refs = append(refs, RowRef{Partition: part.ID, Row: row, Valid: true})
				}
				valid = memory.EnsureBitmap(valid, len(all))
				for i, row := range selected {
					if v.IsValid(row) {
						memory.Set(valid, base+i)
					}
				}
			}
			estimate := join.EstimateI32TableBytes(maxInt(len(all), p.build.dictMeta.Len()), len(all)) + int64(len(refs))*24
			if err := p.reserveBuild(ctx, estimate); err != nil {
				return err
			}
			p.build.refs = refs
			if err := p.build.dict.Build(all, valid, p.build.dictMeta); err != nil {
				p.releaseBuildReservation()
				return err
			}
			return nil
		}
	}
	p.build.shape = joinShapeFixed
	p.build.pair = join.NewPairTable(1024)
	var keys []hashagg.Key128
	var masks []uint8
	for _, part := range p.right.parts {
		if err := reader.ReadPartition(ctx, part, batch); err != nil {
			return err
		}
		selected, err := selectedRows(batch, p.right.predicate, &ew, p.kernel)
		if err != nil {
			return err
		}
		for _, row := range selected {
			key, mask, err := encodeJoinKey(batch, p.keys, false, row)
			if err != nil {
				return err
			}
			keys = append(keys, key)
			masks = append(masks, mask)
			refs = append(refs, RowRef{Partition: part.ID, Row: row, Valid: true})
		}
	}
	estimate := join.EstimatePairTableBytes(len(keys), len(keys)) + int64(len(refs))*24
	if err := p.reserveBuild(ctx, estimate); err != nil {
		return err
	}
	p.build.refs = refs
	if err := p.build.pair.Build(keys, masks); err != nil {
		p.releaseBuildReservation()
		return err
	}
	return nil
}

func (p *PreparedJoin) reserveBuild(ctx context.Context, bytes int64) error {
	if p.manager == nil || bytes <= 0 {
		return nil
	}
	r, err := p.manager.Reserve(ctx, bytes)
	if err != nil {
		return fmt.Errorf("lazy join build memory: %w", err)
	}
	p.reservation = r
	return nil
}

func (p *PreparedJoin) releaseBuildReservation() {
	if p.reservation != nil {
		p.reservation.Release()
		p.reservation = nil
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (p *PreparedJoin) probeBatch(batch *memory.Batch, selected []int, emit join.EmitFunc) (join.Summary, error) {
	if p.build.shape == joinShapeDictionary {
		v, err := batch.DictionaryString(p.keys[0].Left)
		if err != nil {
			return join.Summary{}, err
		}
		indices := make([]uint32, len(selected))
		valid := make(memory.Bitmap, memory.WordsFor(len(selected)))
		for i, row := range selected {
			indices[i] = v.Indices[row]
			if v.IsValid(row) {
				memory.Set(valid, i)
			}
		}
		ldp, ok := p.left.src.(source.DictionaryProvider)
		if !ok {
			return join.Summary{}, fmt.Errorf("left dictionary source does not expose stable dictionary metadata")
		}
		d, err := ldp.Dictionary(context.Background(), p.keys[0].Left)
		if err != nil {
			return join.Summary{}, err
		}
		return p.build.dict.Probe(indices, valid, join.Dictionary{Domain: d.Domain, Offsets: d.Offsets, Data: d.Data}, p.typ, emit)
	}
	keys := make([]hashagg.Key128, len(selected))
	masks := make([]uint8, len(selected))
	for i, row := range selected {
		key, mask, err := encodeJoinKey(batch, p.keys, true, row)
		if err != nil {
			return join.Summary{}, err
		}
		keys[i], masks[i] = key, mask
	}
	return p.build.pair.Probe(keys, masks, p.typ, emit)
}

func encodeJoinKey(batch *memory.Batch, keys []logical.JoinKey, left bool, row int) (hashagg.Key128, uint8, error) {
	var out hashagg.Key128
	mask := uint8(0)
	for i, key := range keys {
		name := key.Right
		if left {
			name = key.Left
		}
		value, valid, err := fixedJoinValue(batch, name, row)
		if err != nil {
			return hashagg.Key128{}, 0, err
		}
		if i == 0 {
			out.A = value
		} else {
			out.B = value
		}
		if valid {
			mask |= 1 << i
		}
	}
	if len(keys) == 1 {
		// PairTable requires both validity bits. The second component is a fixed
		// zero sentinel for single-column fixed-width joins.
		if mask&1 != 0 {
			mask = 3
		} else {
			mask = 0
		}
	}
	return out, mask, nil
}

func fixedJoinValue(batch *memory.Batch, name string, row int) (uint64, bool, error) {
	if v, ok := batch.Int32s[name]; ok {
		return uint64(uint32(v.Values[row])), v.IsValid(row), nil
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
	return 0, false, fmt.Errorf("fixed join column %q is not int32/int64/uint32/uint64", name)
}

func selectedRows(batch *memory.Batch, program expr.Program, ew *expr.EvalWorkspace, kernels kernel.Set) ([]int, error) {
	bools, err := program.Evaluate(batch, ew, kernels)
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, batch.Rows)
	for i := 0; i < batch.Rows; i++ {
		if memory.IsSet(bools.Truth, i) && memory.IsSet(bools.Validity, i) {
			out = append(out, i)
		}
	}
	return out, nil
}
