package physical

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// SortKey describes one physical column ordering key.
type SortKey struct {
	Column     string
	Descending bool
	NullsLast  bool
}

// SortSourceOptions controls optional parallel execution for global sorting.
// A non-nil Executor is required when Workers is greater than one. The default
// zero value keeps the serial implementation used by NewSortSource.
type SortSourceOptions struct {
	Workers  int
	Executor join.ParallelExecutor
}

// SortSource globally sorts all input rows. Sort materializes the full input
// and therefore charges an execution-time memory reservation.
type SortSource struct {
	input    source.Source
	keys     []SortKey
	manager  memory.Manager
	workers  int
	executor join.ParallelExecutor
}

// NewSortSource validates sort keys and returns a full-materialization sort.
func NewSortSource(input source.Source, keys []SortKey, manager memory.Manager) (source.Source, error) {
	return NewSortSourceWithOptions(input, keys, manager, SortSourceOptions{})
}

// NewSortSourceWithOptions validates sort keys and returns a full-materialization
// sort. When configured with a persistent Executor and multiple Workers, the
// materialized row ordinals are split into contiguous runs, sorted concurrently,
// then merged by the original global ordinal to preserve stable ordering.
func NewSortSourceWithOptions(input source.Source, keys []SortKey, manager memory.Manager, opts SortSourceOptions) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("sort input is nil")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("sort requires at least one key")
	}
	if err := validateSortKeys(input.Schema(), keys); err != nil {
		return nil, err
	}
	if opts.Workers < 0 {
		return nil, fmt.Errorf("sort workers must be non-negative")
	}
	if opts.Workers > 1 && opts.Executor == nil {
		return nil, fmt.Errorf("parallel sort requires executor")
	}
	return &SortSource{
		input:    input,
		keys:     append([]SortKey(nil), keys...),
		manager:  manager,
		workers:  opts.Workers,
		executor: opts.Executor,
	}, nil
}

// Schema preserves the input schema.
func (s *SortSource) Schema() memory.Schema { return s.input.Schema() }

// Ordering exposes the globally stable key order established by this source.
func (s *SortSource) Ordering() []source.OrderingColumn {
	out := make([]source.OrderingColumn, len(s.keys))
	for index, key := range s.keys {
		out[index] = source.OrderingColumn{Name: key.Column, Descending: key.Descending, NullsLast: key.NullsLast}
	}
	return out
}

// StreamPlan reports the current full-materialization implementation as
// blocking. Phase 3.5 changes this node to spillable after external merge is
// implemented and verified.
func (s *SortSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{
		Operator: "Sort",
		Class:    source.StreamBlocking,
		Detail:   fmt.Sprintf("keys=%d", len(s.keys)),
		Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)},
	}
}

// Stats preserves input row statistics.
func (s *SortSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Partitions exposes one global output partition because ordering crosses child boundaries.
func (s *SortSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.input.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}

// NewReader creates an independent global-sort reader.
func (s *SortSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	child, err := s.input.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	parts, err := s.input.Partitions(ctx)
	if err != nil {
		child.Close()
		return nil, err
	}
	return &sortReader{owner: s, child: child, parts: parts, scratch: memory.NewBatch()}, nil
}

type sortReader struct {
	owner       *SortSource
	child       source.Reader
	parts       []source.Partition
	scratch     *memory.Batch
	reservation memory.Reservation
	done        bool
}

// Close releases child and materialization reservation resources.
func (r *sortReader) Close() error {
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
	r.scratch.ReleaseOwned()
	return r.child.Close()
}

// ReadPartition materializes, stably sorts, and gathers the global input once.
func (r *sortReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if p.ID != 0 {
		return fmt.Errorf("sort output partition %d is invalid", p.ID)
	}
	if r.done {
		dst.ClearColumns()
		dst.Reset(0)
		return nil
	}
	r.done = true
	if err := r.reserve(ctx); err != nil {
		return err
	}
	full, err := materializeSource(ctx, r.owner.input.Schema(), r.parts, r.child, r.scratch)
	if err != nil {
		return err
	}
	defer full.ReleaseOwned()
	cmp := batchComparator{batch: full, schema: r.owner.input.Schema(), keys: r.owner.keys}
	rows, err := sortRows(ctx, full.Rows, cmp, r.owner.workers, r.owner.executor)
	if err != nil {
		return err
	}
	return takeRows(ctx, dst, full, rows)
}

// sortRows returns global input row ordinals in requested order. Each ordinal
// is also the stability tie-breaker: contiguous runs therefore remain stable
// locally and the merge reproduces the original source order for equal keys.
func sortRows(ctx context.Context, rowCount int, cmp batchComparator, workers int, exec join.ParallelExecutor) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([]int, rowCount)
	for i := range rows {
		rows[i] = i
	}
	if rowCount < 2 || workers <= 1 || exec == nil {
		sort.SliceStable(rows, func(i, j int) bool { return cmp.less(rows[i], rows[j]) })
		return rows, nil
	}
	if workers > exec.MaxWorkers() {
		workers = exec.MaxWorkers()
	}
	if workers > rowCount {
		workers = rowCount
	}
	if workers <= 1 {
		sort.SliceStable(rows, func(i, j int) bool { return cmp.less(rows[i], rows[j]) })
		return rows, nil
	}
	runs := make([][]int, workers)
	base, extra := rowCount/workers, rowCount%workers
	start := 0
	for i := range runs {
		end := start + base
		if i < extra {
			end++
		}
		runs[i] = rows[start:end]
		start = end
	}
	if err := exec.ParallelFor(ctx, len(runs), workers, func(run int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sort.SliceStable(runs[run], func(i, j int) bool { return cmp.less(runs[run][i], runs[run][j]) })
		return nil
	}); err != nil {
		return nil, err
	}
	return mergeSortedRuns(runs, cmp), nil
}

// mergeSortedRuns performs a k-way merge without losing global source order
// for equal keys. batchComparator's ordinal tie-breaker makes the heap order
// total and deterministic across run boundaries.
func mergeSortedRuns(runs [][]int, cmp batchComparator) []int {
	h := rowRunHeap{cmp: cmp, runs: make([]rowRun, len(runs))}
	for i, rows := range runs {
		h.runs[i] = rowRun{rows: rows}
	}
	heap.Init(&h)
	out := make([]int, 0)
	for h.Len() > 0 {
		row := h.runs[0].rows[h.runs[0].next]
		out = append(out, row)
		h.runs[0].next++
		if h.runs[0].next == len(h.runs[0].rows) {
			heap.Pop(&h)
		} else {
			heap.Fix(&h, 0)
		}
	}
	return out
}

type rowRun struct {
	rows []int
	next int
}

type rowRunHeap struct {
	cmp  batchComparator
	runs []rowRun
}

// Len returns the number of active merge runs.
func (h rowRunHeap) Len() int { return len(h.runs) }

// Less orders the current head row of two merge runs.
func (h rowRunHeap) Less(i, j int) bool {
	left := h.runs[i].rows[h.runs[i].next]
	right := h.runs[j].rows[h.runs[j].next]
	return h.cmp.less(left, right)
}

// Swap exchanges two merge runs.
func (h rowRunHeap) Swap(i, j int) { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }

// Push adds a merge run to the heap.
func (h *rowRunHeap) Push(x any) { h.runs = append(h.runs, x.(rowRun)) }

// Pop removes and returns the final heap run.
func (h *rowRunHeap) Pop() any {
	n := len(h.runs)
	run := h.runs[n-1]
	h.runs = h.runs[:n-1]
	return run
}

func (r *sortReader) reserve(ctx context.Context) error {
	if r.reservation != nil || r.owner.manager == nil {
		return nil
	}
	st, err := r.owner.input.Stats(ctx)
	if err != nil {
		return err
	}
	bytes := estimateOrderBytes(r.owner.input.Schema(), st.Rows)
	// Parallel merging retains the sorted-run ordinals and emits one additional
	// merged ordinal buffer. Charge it explicitly rather than silently widening
	// the full-materialization memory footprint.
	if r.owner.usesParallelSort() {
		if st.Rows > math.MaxInt64/8 || bytes > math.MaxInt64-st.Rows*8 {
			bytes = math.MaxInt64
		} else {
			bytes += st.Rows * 8
		}
	}
	if st.Bytes > bytes {
		bytes = st.Bytes
	}
	res, err := r.owner.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	r.reservation = res
	return nil
}

func (s *SortSource) usesParallelSort() bool {
	return s.workers > 1 && s.executor != nil && s.executor.MaxWorkers() > 1
}

// Dictionary forwards stable dictionary metadata from the input.
func (s *SortSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("sort input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

// TopKSource keeps only the best K ordering keys in O(K) heap memory, then
// re-reads only partitions containing winners to materialize payload columns.
type TopKSource struct {
	input    source.Source
	keyInput source.Source
	keys     []SortKey
	k        int
	manager  memory.Manager
}

// NewTopKSourceWithManager constructs a bounded-memory Top-K operator whose
// retained candidate and winner materialization state is charged to manager.
func NewTopKSourceWithManager(input source.Source, k int, keys []SortKey, manager memory.Manager) (source.Source, error) {
	return NewTopKSourceWithKeyInput(input, input, k, keys, manager)
}

// NewTopKSourceWithKeyInput constructs a Top-K source with a narrow source for
// the first ordering pass and a full payload source for the winning-row pass.
// Both inputs must expose the same partitions and row positions. keyInput may
// omit every column except the ordering keys, which avoids decoding payload
// columns while the bounded heap is being built.
func NewTopKSourceWithKeyInput(input, keyInput source.Source, k int, keys []SortKey, manager memory.Manager) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("top-k input is nil")
	}
	if keyInput == nil {
		return nil, fmt.Errorf("top-k key input is nil")
	}
	if k < 0 {
		return nil, fmt.Errorf("top-k must be non-negative")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("top-k requires at least one key")
	}
	if err := validateSortKeys(input.Schema(), keys); err != nil {
		return nil, err
	}
	if err := validateSortKeys(keyInput.Schema(), keys); err != nil {
		return nil, fmt.Errorf("top-k key input: %w", err)
	}
	return &TopKSource{input: input, keyInput: keyInput, keys: append([]SortKey(nil), keys...), k: k, manager: manager}, nil
}

// Schema preserves the input schema.
func (s *TopKSource) Schema() memory.Schema { return s.input.Schema() }

// StreamPlan reports the explicit O(K) state bound.
func (s *TopKSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{
		Operator: "TopK",
		Class:    source.StreamBounded,
		Detail:   fmt.Sprintf("k=%d", s.k),
		Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)},
	}
}

// Stats caps the input row upper bound at K.
func (s *TopKSource) Stats(ctx context.Context) (source.Stats, error) {
	st, err := s.input.Stats(ctx)
	if err != nil {
		return source.Stats{}, err
	}
	if st.Rows < 0 || st.Rows > int64(s.k) {
		st.Rows = int64(s.k)
	}
	st.RowsUpperBound = true
	return st, nil
}

// Partitions exposes a single ordered output partition.
func (s *TopKSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.k)}}, nil
}

// NewReader creates an independent Top-K reader.
func (s *TopKSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	first, err := s.keyInput.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	parts, err := s.keyInput.Partitions(ctx)
	if err != nil {
		first.Close()
		return nil, err
	}
	payloadParts, err := s.input.Partitions(ctx)
	if err != nil {
		first.Close()
		return nil, err
	}
	if !samePartitionLayout(parts, payloadParts) {
		first.Close()
		return nil, fmt.Errorf("top-k key input partitions do not match payload input")
	}
	return &topKReader{owner: s, first: first, parts: parts, payloadParts: payloadParts, scratch: memory.NewBatch(), readOpts: opts}, nil
}

type topKReader struct {
	owner        *TopKSource
	first        source.Reader
	parts        []source.Partition
	payloadParts []source.Partition
	scratch      *memory.Batch
	reservation  memory.Reservation
	done         bool
	readOpts     source.ReadOptions
}

// Close releases reader-local resources.
func (r *topKReader) Close() error {
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
	r.scratch.ReleaseOwned()
	return r.first.Close()
}

// ReadPartition scans ordering keys, maintains a bounded heap, then materializes
// only selected rows from the winning source partitions.
func (r *topKReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if p.ID != 0 {
		return fmt.Errorf("top-k output partition %d is invalid", p.ID)
	}
	if r.done || r.owner.k == 0 {
		r.done = true
		dst.ClearColumns()
		dst.Reset(0)
		return nil
	}
	r.done = true
	if err := r.reserve(ctx); err != nil {
		return err
	}
	h := &candidateHeap{keys: r.owner.keys}
	heap.Init(h)
	var ordinal int64
	for _, part := range r.parts {
		if err := r.first.ReadPartition(ctx, part, r.scratch); err != nil {
			return err
		}
		for row := 0; row < r.scratch.Rows; row++ {
			if h.Len() < r.owner.k {
				cand := rowCandidate{cells: make([]sortCell, len(r.owner.keys))}
				if err := snapshotCandidateInto(&cand, r.scratch, r.owner.keyInput.Schema(), r.owner.keys, part.ID, row, ordinal); err != nil {
					return err
				}
				heap.Push(h, cand)
				ordinal++
				continue
			}
			better, err := batchRowLessCandidate(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row, ordinal, h.items[0])
			if err != nil {
				return err
			}
			if better {
				if err := snapshotCandidateInto(&h.items[0], r.scratch, r.owner.keyInput.Schema(), r.owner.keys, part.ID, row, ordinal); err != nil {
					return err
				}
				heap.Fix(h, 0)
			}
			ordinal++
		}
	}
	winners := append([]rowCandidate(nil), h.items...)
	sort.SliceStable(winners, func(i, j int) bool { return candidateLess(winners[i], winners[j], r.owner.keys) })
	if len(winners) == 0 {
		dst.ClearColumns()
		dst.Reset(0)
		return nil
	}
	second, err := r.owner.input.NewReader(ctx, r.readOpts)
	if err != nil {
		return err
	}
	defer second.Close()
	type selectedPart struct {
		batch *memory.Batch
		index map[int]int
	}
	selected := make(map[int]*selectedPart)
	byPart := make(map[int][]int)
	for _, w := range winners {
		byPart[w.partition] = append(byPart[w.partition], w.row)
	}
	partByID := make(map[int]source.Partition, len(r.payloadParts))
	for _, part := range r.payloadParts {
		partByID[part.ID] = part
	}
	for partID, rows := range byPart {
		part, ok := partByID[partID]
		if !ok {
			return fmt.Errorf("top-k selected unknown partition %d", partID)
		}
		if err := second.ReadPartition(ctx, part, r.scratch); err != nil {
			return err
		}
		uniq := append([]int(nil), rows...)
		sort.Ints(uniq)
		uniq = uniqueInts(uniq)
		mini := memory.NewBatch()
		if err := takeRows(ctx, mini, r.scratch, uniq); err != nil {
			return err
		}
		idx := make(map[int]int, len(uniq))
		for i, row := range uniq {
			idx[row] = i
		}
		selected[partID] = &selectedPart{batch: mini, index: idx}
	}
	defer func() {
		for _, p := range selected {
			p.batch.ReleaseOwned()
		}
	}()
	acc := newBatchAccumulator(r.owner.input.Schema())
	for _, w := range winners {
		p := selected[w.partition]
		if err := acc.appendRow(p.batch, p.index[w.row]); err != nil {
			return err
		}
	}
	full, err := acc.finish()
	if err != nil {
		return err
	}
	defer full.ReleaseOwned()
	dst.ClearColumns()
	bindAll(dst, full)
	// Transfer owned opaque bindings from full to dst; dst now owns them.
	for name, v := range full.Opaque {
		dst.Opaque[name] = v
		delete(full.Opaque, name)
	}
	dst.Reset(full.Rows)
	return nil
}

func samePartitionLayout(left, right []source.Partition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ID != right[i].ID || left[i].Rows != right[i].Rows {
			return false
		}
	}
	return true
}

func (r *topKReader) reserve(ctx context.Context) error {
	if r.reservation != nil || r.owner.manager == nil || r.owner.k <= 0 {
		return nil
	}
	// Charge both the final K-row payload and candidate ordering state. The
	// estimate is deliberately conservative because strings may copy key bytes.
	bytes := estimateOrderBytes(r.owner.input.Schema(), int64(r.owner.k))
	keyWidth := int64(32)
	for range r.owner.keys {
		keyWidth += 32
	}
	if int64(r.owner.k) <= math.MaxInt64/keyWidth {
		bytes += int64(r.owner.k) * keyWidth
	} else {
		bytes = math.MaxInt64
	}
	res, err := r.owner.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	r.reservation = res
	return nil
}

// Dictionary forwards stable dictionary metadata from the input.
func (s *TopKSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("top-k input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

func validateSortKeys(schema memory.Schema, keys []SortKey) error {
	for _, k := range keys {
		f, ok := schema.Field(k.Column)
		if !ok {
			return fmt.Errorf("sort column %q not found", k.Column)
		}
		switch f.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
			memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
			memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
			memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeBinary:
		default:
			return fmt.Errorf("sort column %q type %s is not orderable yet", k.Column, f.Type)
		}
	}
	return nil
}

func estimateOrderBytes(schema memory.Schema, rows int64) int64 {
	if rows <= 0 {
		return 0
	}
	width := int64(8) // row-index buffer
	for _, f := range schema.Fields {
		switch f.Type {
		case memory.TypeInt8, memory.TypeUint8:
			width++
		case memory.TypeInt16, memory.TypeUint16:
			width += 2
		case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32:
			width += 4
		case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			width += 8
		default:
			width += 24
		}
		if f.Nullable {
			width++
		}
	}
	if rows > math.MaxInt64/width {
		return math.MaxInt64
	}
	return rows * width
}

// batchComparator compares rows from one materialized batch.
type batchComparator struct {
	batch  *memory.Batch
	schema memory.Schema
	keys   []SortKey
}

func (c batchComparator) less(a, b int) bool {
	for _, key := range c.keys {
		cmp := compareBatchRows(c.batch, c.schema, key, a, b)
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}
	return a < b
}

type sortCell struct {
	null bool
	i64  int64
	u64  uint64
	f64  float64
	text []byte
	typ  memory.DataType
}

type rowCandidate struct {
	partition int
	row       int
	ordinal   int64
	cells     []sortCell
}

func snapshotCandidateInto(c *rowCandidate, batch *memory.Batch, schema memory.Schema, keys []SortKey, partition, row int, ordinal int64) error {
	if cap(c.cells) < len(keys) {
		c.cells = make([]sortCell, len(keys))
	} else {
		c.cells = c.cells[:len(keys)]
	}
	c.partition = partition
	c.row = row
	c.ordinal = ordinal
	for i, k := range keys {
		cell, err := snapshotCell(batch, schema, k.Column, row)
		if err != nil {
			return err
		}
		if cell.text != nil {
			cell.text = append(c.cells[i].text[:0], cell.text...)
		}
		c.cells[i] = cell
	}
	return nil
}

func batchRowLessCandidate(batch *memory.Batch, schema memory.Schema, keys []SortKey, row int, ordinal int64, b rowCandidate) (bool, error) {
	for i, key := range keys {
		cell, err := snapshotCell(batch, schema, key.Column, row)
		if err != nil {
			return false, err
		}
		cmp := compareSortCells(cell, b.cells[i], key)
		if cmp == 0 {
			continue
		}
		return cmp < 0, nil
	}
	return ordinal < b.ordinal, nil
}

func snapshotCell(batch *memory.Batch, schema memory.Schema, column string, row int) (sortCell, error) {
	f, _ := schema.Field(column)
	cell := sortCell{typ: f.Type}
	switch f.Type {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v, err := batch.Int32(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.i64 = int64(v.Values[row])
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v, err := batch.Int64(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.i64 = v.Values[row]
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		v, err := batch.Uint32(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.u64 = uint64(v.Values[row])
	case memory.TypeUint64:
		v, err := batch.Uint64(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.u64 = v.Values[row]
	case memory.TypeFloat32, memory.TypeFloat64:
		v, err := batch.Float64(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.f64 = v.Values[row]
	case memory.TypeString:
		if f.Encoding == memory.EncodingDictionary {
			v, err := batch.DictionaryString(column)
			if err != nil {
				return cell, err
			}
			cell.null = !v.IsValid(row)
			cell.text = v.DictionaryValue(v.Indices[row])
		} else {
			v, err := batch.String(column)
			if err != nil {
				return cell, err
			}
			cell.null = !v.IsValid(row)
			cell.text = v.ValueBytes(row)
		}
	case memory.TypeBinary:
		v, err := batch.String(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.text = v.ValueBytes(row)
	default:
		return cell, fmt.Errorf("sort column %q type %s is not orderable", column, f.Type)
	}
	return cell, nil
}

func compareCells(a, b sortCell, nullsLast bool) int {
	if a.null || b.null {
		if a.null && b.null {
			return 0
		}
		if a.null {
			if nullsLast {
				return 1
			}
			return -1
		}
		if nullsLast {
			return -1
		}
		return 1
	}
	switch a.typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
		if a.i64 < b.i64 {
			return -1
		}
		if a.i64 > b.i64 {
			return 1
		}
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
		if a.u64 < b.u64 {
			return -1
		}
		if a.u64 > b.u64 {
			return 1
		}
	case memory.TypeFloat32, memory.TypeFloat64:
		an, bn := math.IsNaN(a.f64), math.IsNaN(b.f64)
		if an || bn {
			if an && bn {
				return 0
			}
			if an {
				return 1
			}
			return -1
		}
		if a.f64 < b.f64 {
			return -1
		}
		if a.f64 > b.f64 {
			return 1
		}
	case memory.TypeString, memory.TypeBinary:
		return bytes.Compare(a.text, b.text)
	}
	return 0
}

func compareBatchRows(batch *memory.Batch, schema memory.Schema, key SortKey, a, b int) int {
	ca, err := snapshotCell(batch, schema, key.Column, a)
	if err != nil {
		panic(err)
	}
	cb, err := snapshotCell(batch, schema, key.Column, b)
	if err != nil {
		panic(err)
	}
	return compareSortCells(ca, cb, key)
}

// compareSortCells applies NULL placement before value direction. Descending
// reverses only non-NULL values, so NullsLast remains last in either direction.
func compareSortCells(a, b sortCell, key SortKey) int {
	cmp := compareCells(a, b, key.NullsLast)
	if cmp != 0 && !a.null && !b.null && key.Descending {
		return -cmp
	}
	return cmp
}

func candidateLess(a, b rowCandidate, keys []SortKey) bool {
	for i, key := range keys {
		cmp := compareSortCells(a.cells[i], b.cells[i], key)
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}
	return a.ordinal < b.ordinal
}

type candidateHeap struct {
	keys  []SortKey
	items []rowCandidate
}

// Len returns the number of retained Top-K candidates.
func (h candidateHeap) Len() int { return len(h.items) }

// Less reverses the requested ordering so heap root is the current worst candidate.
func (h candidateHeap) Less(i, j int) bool { return candidateLess(h.items[j], h.items[i], h.keys) }

// Swap exchanges two retained candidates.
func (h candidateHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push appends a candidate required by container/heap.
func (h *candidateHeap) Push(x any) { h.items = append(h.items, x.(rowCandidate)) }

// Pop removes and returns the last candidate required by container/heap.
func (h *candidateHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func uniqueInts(in []int) []int {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// batchAccumulator owns a growing materialized batch and opaque Arrow slices.
type batchAccumulator struct {
	schema memory.Schema
	batch  *memory.Batch
	opaque map[string][]arrow.Array
}

func newBatchAccumulator(schema memory.Schema) *batchAccumulator {
	return &batchAccumulator{schema: schema, batch: memory.NewBatch(), opaque: make(map[string][]arrow.Array)}
}

func (a *batchAccumulator) appendRow(src *memory.Batch, row int) error {
	for _, f := range a.schema.Fields {
		switch f.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			v, ok := src.Int32s[f.Name]
			if !ok {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Int32s[f.Name]
			if out == nil {
				out = &memory.Int32Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Int32s[f.Name] = out
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			v, ok := src.Int64s[f.Name]
			if !ok {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Int64s[f.Name]
			if out == nil {
				out = &memory.Int64Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Int64s[f.Name] = out
		case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
			v, ok := src.Uint32s[f.Name]
			if !ok {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Uint32s[f.Name]
			if out == nil {
				out = &memory.Uint32Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Uint32s[f.Name] = out
		case memory.TypeUint64:
			v, ok := src.Uint64s[f.Name]
			if !ok {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Uint64s[f.Name]
			if out == nil {
				out = &memory.Uint64Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Uint64s[f.Name] = out
		case memory.TypeFloat32, memory.TypeFloat64:
			v, ok := src.Float64s[f.Name]
			if !ok {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Float64s[f.Name]
			if out == nil {
				out = &memory.Float64Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Float64s[f.Name] = out
		case memory.TypeString:
			if f.Encoding == memory.EncodingDictionary {
				v := src.DictStrings[f.Name]
				if v == nil {
					return fmt.Errorf("dictionary column %q missing", f.Name)
				}
				out := a.batch.DictStrings[f.Name]
				if out == nil {
					out = &memory.DictionaryStringVector{Offsets: v.Offsets, Data: v.Data, DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
				}
				out.Indices = append(out.Indices, v.Indices[row])
				out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Indices)-1)
				a.batch.DictStrings[f.Name] = out
			} else {
				v := src.Strings[f.Name]
				if v == nil {
					return fmt.Errorf("string column %q missing", f.Name)
				}
				out := a.batch.Strings[f.Name]
				if out == nil {
					out = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
				}
				out.Data = append(out.Data, v.ValueBytes(row)...)
				out.Offsets = append(out.Offsets, uint32(len(out.Data)))
				out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Offsets)-2)
				a.batch.Strings[f.Name] = out
			}
		case memory.TypeBinary:
			v := src.Strings[f.Name]
			if v == nil {
				if o := src.Opaque[f.Name]; o != nil {
					return a.appendOpaque(f.Name, o, row)
				}
				return fmt.Errorf("binary column %q missing", f.Name)
			}
			out := a.batch.Strings[f.Name]
			if out == nil {
				out = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
			}
			out.Data = append(out.Data, v.ValueBytes(row)...)
			out.Offsets = append(out.Offsets, uint32(len(out.Data)))
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Offsets)-2)
			a.batch.Strings[f.Name] = out
		case memory.TypeList:
			v := src.StringLists[f.Name]
			if v == nil {
				return fmt.Errorf("list<string> column %q missing", f.Name)
			}
			out := a.batch.StringLists[f.Name]
			if out == nil {
				out = resetStringList(nil)
			}
			if err := memory.AppendStringListRow(out, v, row); err != nil {
				return fmt.Errorf("append list<string> column %q: %w", f.Name, err)
			}
			a.batch.StringLists[f.Name] = out
		case memory.TypeStruct:
			v := src.Structs[f.Name]
			if v == nil {
				return fmt.Errorf("struct column %q missing", f.Name)
			}
			out := a.batch.Structs[f.Name]
			if out == nil {
				out = &memory.StructVector{Fields: append([]memory.Field(nil), v.Fields...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
			}
			if err := memory.AppendStructRow(out, v, row, v.IsValid(row)); err != nil {
				return fmt.Errorf("append struct column %q: %w", f.Name, err)
			}
			a.batch.Structs[f.Name] = out
		default:
			o := src.Opaque[f.Name]
			if o == nil {
				return fmt.Errorf("opaque column %q missing", f.Name)
			}
			if err := a.appendOpaque(f.Name, o, row); err != nil {
				return err
			}
		}
	}
	a.batch.Rows++
	return nil
}

func (a *batchAccumulator) appendOpaque(name string, v *memory.OpaqueVector, row int) error {
	arr, ok := v.Value.(arrow.Array)
	if !ok {
		return fmt.Errorf("opaque column %q is not Arrow array", name)
	}
	s := array.NewSlice(arr, int64(row), int64(row+1))
	a.opaque[name] = append(a.opaque[name], s)
	return nil
}

func (a *batchAccumulator) finish() (*memory.Batch, error) {
	for name, arrs := range a.opaque {
		if len(arrs) == 0 {
			continue
		}
		var out arrow.Array
		var err error
		if len(arrs) == 1 {
			out = arrs[0]
			arrs = nil
		} else {
			out, err = array.Concatenate(arrs, arrowmem.DefaultAllocator)
		}
		for _, arr := range arrs {
			arr.Release()
		}
		if err != nil {
			a.batch.ReleaseOwned()
			return nil, err
		}
		f, _ := a.schema.Field(name)
		a.batch.Opaque[name] = &memory.OpaqueVector{Rows: a.batch.Rows, Type: f.Type, ArrowType: f.ArrowType, Encoding: f.Encoding, Validity: validityFromArrow(out), Value: out, Owned: true}
	}
	return a.batch, nil
}

func appendOneValidity(dst, src memory.Bitmap, srcRow, dstRow int) memory.Bitmap {
	if src == nil && dst == nil {
		return nil
	}
	if dst == nil {
		dst = memory.EnsureBitmap(nil, dstRow+1)
		for i := 0; i < dstRow; i++ {
			memory.Set(dst, i)
		}
	} else if memory.WordsFor(dstRow+1) > len(dst) {
		old := dst
		dst = make(memory.Bitmap, memory.WordsFor(dstRow+1))
		copy(dst, old)
	}
	if src == nil || memory.IsSet(src, srcRow) {
		memory.Set(dst, dstRow)
	}
	return dst
}

func materializeSource(ctx context.Context, schema memory.Schema, parts []source.Partition, r source.Reader, scratch *memory.Batch) (*memory.Batch, error) {
	acc := newBatchAccumulator(schema)
	for _, p := range parts {
		if err := r.ReadPartition(ctx, p, scratch); err != nil {
			return nil, err
		}
		for row := 0; row < scratch.Rows; row++ {
			if err := acc.appendRow(scratch, row); err != nil {
				return nil, err
			}
		}
	}
	return acc.finish()
}
