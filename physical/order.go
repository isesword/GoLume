package physical

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
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
	// Spillable selects execution-scoped stable run generation and bounded
	// external merge instead of retaining the complete input in memory.
	Spillable bool
}

// SortSource globally sorts all input rows. Sort materializes the full input
// and therefore charges an execution-time memory reservation.
type SortSource struct {
	input     source.Source
	keys      []SortKey
	manager   memory.Manager
	workers   int
	executor  join.ParallelExecutor
	spillable bool
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
		input:     input,
		keys:      append([]SortKey(nil), keys...),
		manager:   manager,
		workers:   opts.Workers,
		executor:  opts.Executor,
		spillable: opts.Spillable,
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
	class := source.StreamBlocking
	detail := fmt.Sprintf("strategy=resident-stable keys=%d", len(s.keys))
	if s.spillable {
		class = source.StreamSpillable
		detail = fmt.Sprintf("strategy=external-stable keys=%d", len(s.keys))
	}
	return source.StreamPlanNode{
		Operator: "Sort",
		Class:    class,
		Detail:   detail,
		Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)},
	}
}

// Stats preserves input row statistics.
func (s *SortSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Replayability forwards the child snapshot contract. Sorting is a
// deterministic function of the input row sequence: the stable ordinal
// tie-breaker (including the parallel run merge) makes the output identical
// for identical input, so a replay-stable input yields a replay-stable sorted
// output.
func (s *SortSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// Partitions exposes one global output partition because ordering crosses child boundaries.
func (s *SortSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.input.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}

type sortReader struct {
	owner       *SortSource
	child       source.BatchStream
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

// Next materializes, stably sorts, and gathers the global input once.
func (r *sortReader) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if r.done {
		dst.ClearColumns()
		return false, nil
	}
	r.done = true
	if err := r.reserve(ctx); err != nil {
		return false, err
	}
	full, err := materializeStream(ctx, r.owner.input.Schema(), r.child, r.scratch)
	if err != nil {
		return false, err
	}
	defer full.ReleaseOwned()
	cmp := batchComparator{batch: full, schema: r.owner.input.Schema(), keys: r.owner.keys}
	rows, err := sortRows(ctx, full.Rows, cmp, r.owner.workers, r.owner.executor)
	if err != nil {
		return false, err
	}
	if err := takeRows(ctx, dst, full, rows); err != nil {
		return false, err
	}
	return dst.Rows != 0, nil
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
// Both inputs must expose the same logical row sequence; native batch
// boundaries may differ between passes. keyInput may
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

// Replayability forwards the payload input snapshot contract. Candidate
// admission and the final winner ordering both break ties by the global input
// ordinal, so the Top-K output is a deterministic function of the input row
// sequence and replays identically whenever the input does.
func (s *TopKSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// NewBatchStream creates an independent two-pass O(K) stream. When both inputs
// promise stable replay the first pass scans only ordering keys and the second
// pass replays the payload source for winner batches. Otherwise the payload
// input is materialized exactly once inside the stream and both passes run
// over that snapshot, so Top-K never depends on replay the input cannot
// provide.
func (s *TopKSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r := &topKReader{owner: s, scratch: memory.NewBatch(), readOpts: opts}
	passInput := s.keyInput
	if source.ReplayabilityOf(s.input) != source.ReplayStable || source.ReplayabilityOf(s.keyInput) != source.ReplayStable {
		// Two passes require identical logical row ordinals, not identical
		// native batch boundaries. A non-replayable input is snapshotted once
		// and supplies both passes, trading the O(K) first pass for one
		// full-payload materialization.
		passInput = s.input
		r.materialize = true
	}
	first, err := source.OpenBatchStream(ctx, passInput, opts)
	if err != nil {
		return nil, err
	}
	r.first = first
	return r, nil
}

type topKReader struct {
	owner         *TopKSource
	first         source.BatchStream
	scratch       *memory.Batch
	reservation   memory.Reservation
	extra         []memory.Reservation
	baseBytes     int64
	reservedBytes int64
	candidateText int64
	done          bool
	materialize   bool
	readOpts      source.ReadOptions
}

// Close releases reader-local resources.
func (r *topKReader) Close() error {
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
	for _, reservation := range r.extra {
		reservation.Release()
	}
	r.extra = nil
	r.baseBytes = 0
	r.reservedBytes = 0
	r.scratch.ReleaseOwned()
	return r.first.Close()
}

// Next scans ordering keys, maintains a bounded heap, then replays only the
// selected rows from winning source batches. For a non-replayable input the
// reader first snapshots the payload input into one materialized batch and
// runs both passes over that snapshot.
func (r *topKReader) Next(ctx context.Context, dst *memory.Batch) (produced bool, resultErr error) {
	if r.done || r.owner.k == 0 {
		r.done = true
		dst.ClearColumns()
		return false, nil
	}
	r.done = true
	if err := r.reserve(ctx); err != nil {
		return false, err
	}
	first := r.first
	var snapshot *memory.Batch
	if r.materialize {
		var err error
		snapshot, err = materializeStream(ctx, r.owner.input.Schema(), r.first, r.scratch)
		if err != nil {
			return false, err
		}
		defer snapshot.ReleaseOwned()
		first = &materializedBatchStream{batch: snapshot}
	}
	h := &candidateHeap{keys: r.owner.keys}
	heap.Init(h)
	trackActual := r.tracksActualMemory()
	var ordinal int64
	for {
		ok, err := first.Next(ctx, r.scratch)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		for row := 0; row < r.scratch.Rows; row++ {
			if ordinal == math.MaxInt64 {
				return false, fmt.Errorf("top-k input row ordinal overflows int64")
			}
			if h.Len() < r.owner.k {
				var textBytes int64
				if trackActual {
					var keyErr error
					textBytes, keyErr = sortKeyVariableBytes(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row)
					if keyErr != nil {
						return false, keyErr
					}
					if err := r.growReservation(ctx, r.baseBytes+r.candidateText+textBytes); err != nil {
						return false, fmt.Errorf("top-k candidate key memory: %w", err)
					}
				}
				cand := rowCandidate{cells: make([]sortCell, len(r.owner.keys))}
				if err := snapshotCandidateInto(&cand, r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row, ordinal); err != nil {
					return false, err
				}
				heap.Push(h, cand)
				r.candidateText += textBytes
				ordinal++
				continue
			}
			better, err := batchRowLessCandidate(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row, ordinal, h.items[0])
			if err != nil {
				return false, err
			}
			if better {
				candidateText := r.candidateText
				if trackActual {
					textBytes, err := sortKeyVariableBytes(r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row)
					if err != nil {
						return false, err
					}
					oldTextBytes := candidateVariableBytes(h.items[0])
					candidateText = r.candidateText - oldTextBytes + textBytes
					if err := r.growReservation(ctx, r.baseBytes+candidateText); err != nil {
						return false, fmt.Errorf("top-k candidate key memory: %w", err)
					}
				}
				if err := snapshotCandidateInto(&h.items[0], r.scratch, r.owner.keyInput.Schema(), r.owner.keys, row, ordinal); err != nil {
					return false, err
				}
				r.candidateText = candidateText
				heap.Fix(h, 0)
			}
			ordinal++
		}
	}
	winners := append([]rowCandidate(nil), h.items...)
	sort.SliceStable(winners, func(i, j int) bool { return candidateLess(winners[i], winners[j], r.owner.keys) })
	if len(winners) == 0 {
		dst.ClearColumns()
		return false, nil
	}
	var second source.BatchStream
	if r.materialize {
		second = &materializedBatchStream{batch: snapshot}
	} else {
		replayed, err := source.OpenBatchStream(ctx, r.owner.input, r.readOpts)
		if err != nil {
			return false, err
		}
		second = replayed
	}
	defer func() {
		if closeErr := second.Close(); closeErr != nil {
			produced = false
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	type selectedRow struct {
		batch *memory.Batch
		row   int
	}
	selected := make(map[int64]selectedRow, len(winners))
	ordinals := make([]int64, len(winners))
	for i, w := range winners {
		ordinals[i] = w.ordinal
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	selectedBatches := make([]*memory.Batch, 0, len(winners))
	defer func() {
		for _, batch := range selectedBatches {
			batch.ReleaseOwned()
		}
	}()
	var payloadBytes, replayOrdinal int64
	nextWinner := 0
	for {
		ok, err := second.Next(ctx, r.scratch)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		if int64(r.scratch.Rows) > math.MaxInt64-replayOrdinal {
			return false, fmt.Errorf("top-k replay row ordinal overflows int64")
		}
		start, end := replayOrdinal, replayOrdinal+int64(r.scratch.Rows)
		replayOrdinal = end
		firstWinner := nextWinner
		for nextWinner < len(ordinals) && ordinals[nextWinner] < end {
			nextWinner++
		}
		if firstWinner == nextWinner {
			continue
		}
		rows := make([]int, nextWinner-firstWinner)
		for i, winner := range ordinals[firstWinner:nextWinner] {
			if winner < start {
				return false, fmt.Errorf("top-k replay passed selected row ordinal %d", winner)
			}
			rows[i] = int(winner - start)
		}
		if trackActual {
			for _, row := range rows {
				bytes, estimateErr := memory.EstimateBatchRangeBytes(r.scratch, row, 1)
				if estimateErr != nil {
					return false, estimateErr
				}
				if bytes > math.MaxInt64-payloadBytes {
					return false, fmt.Errorf("top-k payload byte estimate overflows int64")
				}
				payloadBytes += bytes
			}
			if r.baseBytes > math.MaxInt64-r.candidateText || payloadBytes > (math.MaxInt64-r.baseBytes-r.candidateText)/2 {
				return false, fmt.Errorf("top-k payload reservation overflows int64")
			}
			if err := r.growReservation(ctx, r.baseBytes+r.candidateText+payloadBytes*2); err != nil {
				return false, fmt.Errorf("top-k winner payload memory: %w", err)
			}
		}
		mini := memory.NewBatch()
		if err := takeRows(ctx, mini, r.scratch, rows); err != nil {
			mini.ReleaseOwned()
			return false, err
		}
		selectedBatches = append(selectedBatches, mini)
		for i, winner := range ordinals[firstWinner:nextWinner] {
			selected[winner] = selectedRow{batch: mini, row: i}
		}
	}
	if replayOrdinal != ordinal {
		return false, fmt.Errorf("top-k replay changed row count: keys=%d payload=%d", ordinal, replayOrdinal)
	}
	if len(selected) != len(winners) {
		return false, fmt.Errorf("top-k replay selected %d of %d winner ordinals", len(selected), len(winners))
	}
	acc := newBatchAccumulator(r.owner.input.Schema())
	for _, winner := range winners {
		row := selected[winner.ordinal]
		if err := acc.appendRow(row.batch, row.row); err != nil {
			return false, err
		}
	}
	full, err := acc.finish()
	if err != nil {
		return false, err
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
	return true, nil
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
	if r.materialize {
		// The single-snapshot fallback retains the full payload input instead
		// of rescanning it, so charge the materialization like a resident sort.
		st, err := r.owner.input.Stats(ctx)
		if err != nil {
			return err
		}
		full := estimateOrderBytes(r.owner.input.Schema(), st.Rows)
		if st.Bytes > full {
			full = st.Bytes
		}
		if full > math.MaxInt64-bytes {
			bytes = math.MaxInt64
		} else {
			bytes += full
		}
	}
	res, err := r.owner.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	r.reservation = res
	r.baseBytes = res.Bytes()
	r.reservedBytes = res.Bytes()
	return nil
}

func (r *topKReader) growReservation(ctx context.Context, total int64) error {
	if r.owner.manager == nil || r.owner.manager.Limit() <= 0 || total <= r.reservedBytes {
		return nil
	}
	delta := total - r.reservedBytes
	reservation, err := r.owner.manager.Reserve(ctx, delta)
	if err != nil {
		return err
	}
	r.extra = append(r.extra, reservation)
	r.reservedBytes += delta
	return nil
}

func (r *topKReader) tracksActualMemory() bool {
	return r.owner.manager != nil && r.owner.manager.Limit() > 0
}

func sortKeyVariableBytes(batch *memory.Batch, schema memory.Schema, keys []SortKey, row int) (int64, error) {
	var bytes int64
	for _, key := range keys {
		cell, err := snapshotCell(batch, schema, key.Column, row)
		if err != nil {
			return 0, err
		}
		if int64(len(cell.text)) > math.MaxInt64-bytes {
			return 0, fmt.Errorf("top-k candidate string bytes overflow int64")
		}
		bytes += int64(len(cell.text))
	}
	return bytes, nil
}

func candidateVariableBytes(candidate rowCandidate) int64 {
	var bytes int64
	for _, cell := range candidate.cells {
		bytes += int64(len(cell.text))
	}
	return bytes
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
			memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeBinary,
			memory.TypeBool:
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
		case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32, memory.TypeBool:
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
	ordinal int64
	cells   []sortCell
}

func snapshotCandidateInto(c *rowCandidate, batch *memory.Batch, schema memory.Schema, keys []SortKey, row int, ordinal int64) error {
	if cap(c.cells) < len(keys) {
		c.cells = make([]sortCell, len(keys))
	} else {
		c.cells = c.cells[:len(keys)]
	}
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
	case memory.TypeInt8:
		v, err := batch.Int8(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.i64 = int64(v.Values[row])
	case memory.TypeInt16:
		v, err := batch.Int16(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.i64 = int64(v.Values[row])
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
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
	case memory.TypeUint8:
		v, err := batch.Uint8(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.u64 = uint64(v.Values[row])
	case memory.TypeUint16:
		v, err := batch.Uint16(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.u64 = uint64(v.Values[row])
	case memory.TypeUint32:
		v, err := batch.Uint32(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.u64 = uint64(v.Values[row])
	case memory.TypeBool:
		// Bools are physically uint32 0/1 vectors; they order false < true.
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
	case memory.TypeFloat32:
		v, err := batch.Float32(column)
		if err != nil {
			return cell, err
		}
		cell.null = !v.IsValid(row)
		cell.f64 = float64(v.Values[row])
	case memory.TypeFloat64:
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
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeBool:
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
		case memory.TypeInt8:
			v, ok := src.Int8s[f.Name]
			if !ok {
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Int8s[f.Name]
			if out == nil {
				out = &memory.Int8Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Int8s[f.Name] = out
		case memory.TypeInt16:
			v, ok := src.Int16s[f.Name]
			if !ok {
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Int16s[f.Name]
			if out == nil {
				out = &memory.Int16Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Int16s[f.Name] = out
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
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
		case memory.TypeUint8:
			v, ok := src.Uint8s[f.Name]
			if !ok {
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Uint8s[f.Name]
			if out == nil {
				out = &memory.Uint8Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Uint8s[f.Name] = out
		case memory.TypeUint16:
			v, ok := src.Uint16s[f.Name]
			if !ok {
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Uint16s[f.Name]
			if out == nil {
				out = &memory.Uint16Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Uint16s[f.Name] = out
		case memory.TypeUint32, memory.TypeBool:
			// Bools share the uint32 0/1 physical vector, so payload gather and
			// accumulation treat them as one family keyed by the schema type.
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
		case memory.TypeFloat32:
			v, ok := src.Float32s[f.Name]
			if !ok {
				return fmt.Errorf("column %q missing", f.Name)
			}
			out := a.batch.Float32s[f.Name]
			if out == nil {
				out = &memory.Float32Vector{Encoding: v.Encoding}
			}
			out.Values = append(out.Values, v.Values[row])
			out.Validity = appendOneValidity(out.Validity, v.Validity, row, len(out.Values)-1)
			a.batch.Float32s[f.Name] = out
		case memory.TypeFloat64:
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

func materializeStream(ctx context.Context, schema memory.Schema, stream source.BatchStream, scratch *memory.Batch) (*memory.Batch, error) {
	acc := newBatchAccumulator(schema)
	for {
		ok, err := stream.Next(ctx, scratch)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		for row := 0; row < scratch.Rows; row++ {
			if err := acc.appendRow(scratch, row); err != nil {
				return nil, err
			}
		}
	}
	return acc.finish()
}

// materializedBatchStream replays one owned materialized batch as a
// single-batch stream. It gives the two-pass Top-K winner gather a
// replay-stable view of an input that cannot rescan itself.
type materializedBatchStream struct {
	batch *memory.Batch
	done  bool
}

// Next binds the snapshot columns into dst exactly once.
func (s *materializedBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if err := ctx.Err(); err != nil {
		dst.ClearColumns()
		return false, err
	}
	if s.done {
		dst.ClearColumns()
		return false, nil
	}
	s.done = true
	dst.ClearColumns()
	bindAll(dst, s.batch)
	dst.Reset(s.batch.Rows)
	return true, nil
}

// Close is a no-op; the snapshot is owned and released by the caller.
func (s *materializedBatchStream) Close() error { return nil }

var _ source.ReplayabilityProvider = (*SortSource)(nil)
var _ source.ReplayabilityProvider = (*TopKSource)(nil)
var _ source.BatchStream = (*materializedBatchStream)(nil)
