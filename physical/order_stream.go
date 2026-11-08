package physical

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"math"
	"sort"
)

const (
	externalSortFanIn        = 4
	externalSortMetadataByte = int64(4096)
)

type sortSegmentRef struct {
	handle memory.SpillHandle
	rows   int
}

// externalSortRun is one logical sorted stream. A run may contain several
// bounded physical segments, allowing hierarchical merge to reduce open run
// fan-in without creating a single input-sized file or decode buffer.
type externalSortRun struct {
	segments spillmeta.Slice[sortSegmentRef]
	rows     int64
}

type externalSortStream struct {
	owner       *SortSource
	query       *memory.QueryBudget
	store       *spillmeta.Store
	opts        source.ReadOptions
	internal    memory.Schema
	fingerprint [32]byte
	ordinal     string
	runBytes    int64
	chunkRows   int

	sortBudget *memory.OperatorBudget
	encode     *memory.OperatorBudget
	restore    *memory.OperatorBudget
	output     *memory.OperatorBudget
	metadata   memory.Manager

	baseMetadata memory.Reservation
	finalRuns    spillmeta.Slice[externalSortRun]
	finalDecoded []*externalSortCursor
	finalHeap    externalSortHeap
	outputBatch  *memory.Batch
	outputMem    memory.Reservation

	prepared    bool
	closed      bool
	exhausted   bool
	terminalErr error
	inputRows   int64
	outputRows  int64
	spillRuns   int64
	spillBytes  int64
	mergeLevels int
}

// NewBatchStream opens the execution-scoped external stable sort when the
// planner selected Spillable. Resident sorting still consumes the child through BatchStream.
func (s *SortSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if !s.spillable {
		return newResidentSortStream(ctx, s, opts)
	}
	return newExternalSortStream(ctx, s, opts)
}

type residentSortStream struct {
	reader *sortReader
	done   bool
	closed bool
}

func newResidentSortStream(ctx context.Context, owner *SortSource, opts source.ReadOptions) (source.BatchStream, error) {
	child, err := source.OpenBatchStream(ctx, owner.input, opts)
	if err != nil {
		return nil, err
	}
	return &residentSortStream{reader: &sortReader{owner: owner, child: child, scratch: memory.NewBatch()}}, nil
}

// Next emits the one resident sorted batch.
func (s *residentSortStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("sort stream destination is nil")
	}
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.done {
		dst.ClearColumns()
		return false, nil
	}
	s.done = true
	return s.reader.Next(ctx, dst)
}

// Close releases the resident sort reader.
func (s *residentSortStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return s.reader.Close()
}

func newExternalSortStream(ctx context.Context, owner *SortSource, opts source.ReadOptions) (*externalSortStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("external sort context is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	query := memory.QueryBudgetFromContext(ctx)
	if query == nil || query.Limit() <= 0 {
		return nil, fmt.Errorf("external sort requires a finite query memory budget")
	}
	if !query.SpillConfigured() {
		return nil, fmt.Errorf("external sort requires an explicit finite spill quota")
	}
	store, err := query.SpillManager()
	if err != nil {
		return nil, err
	}
	metadata := spillmeta.GrowthManager(query.Operator("ExternalSortMetadata", memory.MemoryRetainedState))
	// Schema construction clones nested child descriptors too. Account the
	// temporary field array, cloned tree, and name-index capacity, not just the
	// top-level columns, before creating the execution schema.
	metadataBytes, err := externalSortSchemaMetadataBytes(owner.input.Schema().Fields)
	if err != nil {
		return nil, err
	}
	baseMetadata, err := metadata.Reserve(ctx, metadataBytes)
	if err != nil {
		return nil, err
	}
	ordinal := "__golume_sort_ordinal"
	for suffix := 0; ; suffix++ {
		if _, exists := owner.input.Schema().Field(ordinal); !exists {
			break
		}
		ordinal = fmt.Sprintf("__golume_sort_ordinal_%d", suffix+1)
	}
	fields := append([]memory.Field(nil), owner.input.Schema().Fields...)
	fields = append(fields, memory.Field{Name: ordinal, Type: memory.TypeUint64})
	internal := memory.NewSchema(fields...)
	rowBytes := pipeline.EstimateSchemaRowBytes(internal)
	if rowBytes < 1 {
		rowBytes = 1
	}
	runBytes := query.Limit() / 16
	if runBytes < 1024 {
		runBytes = 1024
	}
	chunkRows := int(runBytes / rowBytes)
	if chunkRows < 1 {
		chunkRows = 1
	}
	if chunkRows > 4096 {
		chunkRows = 4096
	}
	return &externalSortStream{
		owner: owner, query: query, store: spillmeta.NewStore(store, metadata), opts: opts, internal: internal,
		fingerprint: memory.SchemaFingerprint(internal), ordinal: ordinal, runBytes: runBytes, chunkRows: chunkRows,
		sortBudget: query.Operator("ExternalSortRun", memory.MemoryWorkspace),
		encode:     query.Operator("ExternalSortEncode", memory.MemorySpillBuffer),
		restore:    query.Operator("ExternalSortRestore", memory.MemorySpillBuffer),
		output:     query.Operator("ExternalSortOutput", memory.MemoryWorkspace),
		metadata:   metadata, baseMetadata: baseMetadata,
	}, nil
}

func externalSortSchemaMetadataBytes(fields []memory.Field) (int64, error) {
	// The added stable ordinal is another top-level field.
	total := externalSortMetadataByte + 512
	var visit func([]memory.Field) error
	visit = func(fields []memory.Field) error {
		for _, field := range fields {
			if total > math.MaxInt64-512 {
				return memory.ErrBudgetExceeded
			}
			total += 512
			if err := visit(field.Children); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(fields); err != nil {
		return 0, err
	}
	return total, nil
}

func (s *externalSortStream) spillBatch(ctx context.Context, batch *memory.Batch) (sortSegmentRef, error) {
	encoded, err := memory.EncodeColumnarRun(ctx, s.internal, batch, s.encode)
	if err != nil {
		return sortSegmentRef{}, err
	}
	defer encoded.Close()
	handle, err := s.store.Spill(ctx, memory.SpillObject{
		Kind: memory.SpillColumnarRun, Codec: memory.ColumnarRunCodec,
		SchemaFingerprint: s.fingerprint, Payload: encoded.Payload,
	})
	if err != nil {
		return sortSegmentRef{}, err
	}
	ref := sortSegmentRef{handle: handle, rows: batch.Rows}
	s.spillRuns++
	s.spillBytes += handle.Bytes()
	s.query.RecordSpilledBytes(handle.Bytes())
	return ref, nil
}

func (s *externalSortStream) deleteSegment(ctx context.Context, ref sortSegmentRef) error {
	if ref.handle == nil {
		return nil
	}
	return s.store.Delete(ctx, ref.handle)
}

func (s *externalSortStream) deleteRun(ctx context.Context, run externalSortRun) error {
	var out error
	for _, segment := range run.segments.Values {
		if err := s.deleteSegment(ctx, segment); err != nil && out == nil {
			out = err
		}
	}
	return out
}

func (s *externalSortStream) restoreSegment(ctx context.Context, ref sortSegmentRef) (*memory.DecodedColumnarRun, error) {
	restored, err := s.store.Restore(ctx, ref.handle, s.fingerprint, s.restore)
	if err != nil {
		return nil, err
	}
	if restored.Object.Kind != memory.SpillColumnarRun || restored.Object.Codec != memory.ColumnarRunCodec {
		_ = restored.Close()
		return nil, fmt.Errorf("%w: external sort run kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	decoded, err := memory.DecodeColumnarRun(ctx, s.internal, restored, s.restore)
	_ = restored.Close()
	if err != nil {
		return nil, err
	}
	if decoded.Batch.Rows != ref.rows {
		_ = decoded.Close()
		return nil, fmt.Errorf("%w: external sort run rows=%d want=%d", memory.ErrSpillCorrupt, decoded.Batch.Rows, ref.rows)
	}
	return decoded, nil
}

func (s *externalSortStream) makeInitialRun(ctx context.Context, batch *memory.Batch, start, end int, ordinalBase uint64) (externalSortRun, error) {
	rows := end - start
	estimate, err := memory.EstimateBatchRangeBytes(batch, start, rows)
	if err != nil {
		return externalSortRun{}, err
	}
	if estimate > (math.MaxInt64-int64(rows)*16-1024)/2 {
		return externalSortRun{}, fmt.Errorf("external sort run byte estimate overflows int64")
	}
	bindingBytes, err := sortBindingBytes(len(s.owner.keys))
	if err != nil || estimate*2+int64(rows)*16+1024 > math.MaxInt64-bindingBytes {
		return externalSortRun{}, memory.ErrBudgetExceeded
	}
	reservation, err := s.sortBudget.Reserve(ctx, estimate*2+int64(rows)*16+1024+bindingBytes)
	if err != nil {
		return externalSortRun{}, err
	}
	defer reservation.Release()
	indices := make([]int, rows)
	for index := range indices {
		indices[index] = start + index
	}
	cmp, err := newBatchComparator(batch, s.owner.input.Schema(), s.owner.keys)
	if err != nil {
		return externalSortRun{}, err
	}
	sort.SliceStable(indices, func(i, j int) bool { return cmp.less(indices[i], indices[j]) })
	if err := ctx.Err(); err != nil {
		return externalSortRun{}, err
	}
	sorted := memory.NewBatch()
	if err := takeRows(ctx, sorted, batch, indices); err != nil {
		return externalSortRun{}, err
	}
	defer sorted.ReleaseOwned()
	ordinals := make([]uint64, rows)
	for outputRow, inputRow := range indices {
		if uint64(inputRow) > math.MaxUint64-ordinalBase {
			return externalSortRun{}, fmt.Errorf("external sort row ordinal overflows uint64")
		}
		ordinals[outputRow] = ordinalBase + uint64(inputRow)
	}
	sorted.Uint64s[s.ordinal] = &memory.Uint64Vector{Values: ordinals}
	ref, err := s.spillBatch(ctx, sorted)
	if err != nil {
		return externalSortRun{}, err
	}
	var run externalSortRun
	if err := run.segments.Append(ctx, s.metadata, ref); err != nil {
		return externalSortRun{}, err
	}
	run.rows = int64(rows)
	return run, nil
}

func (s *externalSortStream) generateRuns(ctx context.Context) (result spillmeta.Slice[externalSortRun], resultErr error) {
	stream, err := source.OpenBatchStream(ctx, s.owner.input, s.opts)
	if err != nil {
		return result, err
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	var runs spillmeta.Slice[externalSortRun]
	defer func() {
		if resultErr != nil {
			releaseSortRuns(&runs)
		}
	}()
	var ordinalBase uint64
	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			return result, errors.Join(nextErr, stream.Close())
		}
		if !ok {
			if closeErr := stream.Close(); closeErr != nil {
				return result, closeErr
			}
			break
		}
		if uint64(batch.Rows) > math.MaxUint64-ordinalBase {
			_ = stream.Close()
			return result, fmt.Errorf("external sort input row ordinal overflows uint64")
		}
		for start := 0; start < batch.Rows; {
			end := start + s.chunkRows
			if end > batch.Rows {
				end = batch.Rows
			}
			// Variable-width rows may be much wider than the schema estimate.
			// Shrink before allocating until one run fits its target byte share.
			for end-start > 1 {
				estimate, estimateErr := memory.EstimateBatchRangeBytes(batch, start, end-start)
				if estimateErr != nil {
					_ = stream.Close()
					return result, estimateErr
				}
				if estimate <= s.runBytes {
					break
				}
				end = start + (end-start)/2
			}
			run, runErr := s.makeInitialRun(ctx, batch, start, end, ordinalBase)
			if runErr != nil {
				_ = stream.Close()
				return result, runErr
			}
			if err := runs.Append(ctx, s.metadata, run); err != nil {
				run.segments.Release()
				_ = stream.Close()
				return result, err
			}
			start = end
		}
		ordinalBase += uint64(batch.Rows)
		s.inputRows += int64(batch.Rows)
	}
	return runs, nil
}

type externalSortCursor struct {
	owner        *externalSortStream
	segments     []sortSegmentRef
	segment      int
	row          int
	decoded      *memory.DecodedColumnarRun
	deleteOnLoad bool
}

func (c *externalSortCursor) load(ctx context.Context) error {
	for c.decoded == nil && c.segment < len(c.segments) {
		ref := c.segments[c.segment]
		decoded, err := c.owner.restoreSegment(ctx, ref)
		if err != nil {
			return err
		}
		if c.deleteOnLoad {
			if err := c.owner.deleteSegment(ctx, ref); err != nil {
				_ = decoded.Close()
				return err
			}
		}
		c.decoded = decoded
		c.row = 0
		if decoded.Batch.Rows == 0 {
			_ = decoded.Close()
			c.decoded = nil
			c.segment++
		}
	}
	return nil
}

func (c *externalSortCursor) advance(ctx context.Context) error {
	c.row++
	if c.decoded != nil && c.row < c.decoded.Batch.Rows {
		return nil
	}
	if c.decoded != nil {
		_ = c.decoded.Close()
		c.decoded = nil
	}
	c.segment++
	return c.load(ctx)
}

func (c *externalSortCursor) close() {
	c.segments = nil
	if c.decoded != nil {
		_ = c.decoded.Close()
		c.decoded = nil
	}
}

type externalSortHeap struct {
	items []*externalSortCursor
	keys  []SortKey
	ord   string
}

// Len returns the number of active merge cursors.
func (h externalSortHeap) Len() int { return len(h.items) }

// Less orders cursor heads by user keys and stable ordinal.
func (h externalSortHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	for _, key := range h.keys {
		left, err := snapshotCell(a.decoded.Batch, a.owner.internal, key.Column, a.row)
		if err != nil {
			panic(err)
		}
		right, err := snapshotCell(b.decoded.Batch, b.owner.internal, key.Column, b.row)
		if err != nil {
			panic(err)
		}
		if cmp := compareSortCells(left, right, key); cmp != 0 {
			return cmp < 0
		}
	}
	return a.decoded.Batch.Uint64s[h.ord].Values[a.row] < b.decoded.Batch.Uint64s[h.ord].Values[b.row]
}

// Swap exchanges two merge cursors.
func (h externalSortHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

// Push adds one merge cursor.
func (h *externalSortHeap) Push(value any) { h.items = append(h.items, value.(*externalSortCursor)) }

// Pop removes the final merge cursor.
func (h *externalSortHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items[last] = nil
	h.items = h.items[:last]
	return value
}

func (s *externalSortStream) openCursors(ctx context.Context, runs []externalSortRun, deleteOnLoad bool) ([]*externalSortCursor, externalSortHeap, error) {
	if len(runs) > externalSortFanIn {
		return nil, externalSortHeap{}, fmt.Errorf("external sort fan-in exceeded")
	}
	cursors := make([]*externalSortCursor, 0, len(runs))
	h := externalSortHeap{keys: s.owner.keys, ord: s.ordinal, items: make([]*externalSortCursor, 0, len(runs))}
	for _, run := range runs {
		cursor := &externalSortCursor{owner: s, segments: run.segments.Values, deleteOnLoad: deleteOnLoad}
		if err := cursor.load(ctx); err != nil {
			for _, opened := range cursors {
				opened.close()
			}
			return nil, externalSortHeap{}, err
		}
		cursors = append(cursors, cursor)
		if cursor.decoded != nil {
			h.items = append(h.items, cursor)
		}
	}
	heap.Init(&h)
	return cursors, h, nil
}

func (s *externalSortStream) buildMergedBatch(ctx context.Context, h *externalSortHeap) (*memory.Batch, memory.Reservation, error) {
	if h.Len() == 0 {
		return nil, nil, nil
	}
	accumulator := newBatchAccumulator(s.internal)
	var reservation memory.Reservation
	var estimated int64
	for h.Len() > 0 && accumulator.batch.Rows < s.chunkRows {
		cursor := h.items[0]
		rowBytes, err := memory.EstimateBatchRangeBytes(cursor.decoded.Batch, cursor.row, 1)
		if err != nil {
			if reservation != nil {
				reservation.Release()
			}
			accumulator.batch.ReleaseOwned()
			return nil, nil, err
		}
		if accumulator.batch.Rows > 0 && estimated > s.runBytes-rowBytes {
			break
		}
		if reservation == nil {
			reserveBytes := s.runBytes*2 + 1024
			if rowBytes > s.runBytes {
				if rowBytes > (math.MaxInt64-1024)/2 {
					return nil, nil, fmt.Errorf("external sort output row estimate overflows int64")
				}
				reserveBytes = rowBytes*2 + 1024
			}
			reservation, err = s.output.Reserve(ctx, reserveBytes)
			if err != nil {
				accumulator.batch.ReleaseOwned()
				return nil, nil, err
			}
		}
		if err := accumulator.appendRow(cursor.decoded.Batch, cursor.row); err != nil {
			reservation.Release()
			accumulator.batch.ReleaseOwned()
			return nil, nil, err
		}
		estimated += rowBytes
		if err := cursor.advance(ctx); err != nil {
			reservation.Release()
			accumulator.batch.ReleaseOwned()
			return nil, nil, err
		}
		if cursor.decoded == nil {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
	}
	batch, err := accumulator.finish()
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return nil, nil, err
	}
	if reservation != nil && memory.BatchBytes(batch) > reservation.Bytes() {
		actual := memory.BatchBytes(batch)
		batch.ReleaseOwned()
		reservation.Release()
		return nil, nil, fmt.Errorf("external sort output used %d bytes beyond pre-reserved workspace", actual)
	}
	return batch, reservation, nil
}

func (s *externalSortStream) mergeRunGroup(ctx context.Context, inputs []externalSortRun) (result externalSortRun, resultErr error) {
	cursors, h, err := s.openCursors(ctx, inputs, false)
	if err != nil {
		return externalSortRun{}, err
	}
	defer func() {
		for _, cursor := range cursors {
			cursor.close()
		}
	}()
	var output externalSortRun
	defer func() {
		if resultErr != nil {
			output.segments.Release()
		}
	}()
	for h.Len() > 0 {
		batch, reservation, err := s.buildMergedBatch(ctx, &h)
		if err != nil {
			_ = s.deleteRun(context.Background(), output)
			return externalSortRun{}, err
		}
		ref, err := s.spillBatch(ctx, batch)
		batch.ReleaseOwned()
		reservation.Release()
		if err != nil {
			_ = s.deleteRun(context.Background(), output)
			return externalSortRun{}, err
		}
		if err := output.segments.Append(ctx, s.metadata, ref); err != nil {
			return externalSortRun{}, err
		}
		output.rows += int64(ref.rows)
	}
	for _, input := range inputs {
		if err := s.deleteRun(ctx, input); err != nil {
			_ = s.deleteRun(context.Background(), output)
			return externalSortRun{}, err
		}
	}
	return output, nil
}

// releaseSortRuns drops all segment backing arrays before releasing the run list.
func releaseSortRuns(runs *spillmeta.Slice[externalSortRun]) {
	for i := range runs.Values {
		runs.Values[i].segments.Release()
	}
	runs.Release()
}

// Ownership of runs is transferred in and either returned or released on error.
func (s *externalSortStream) mergeHierarchy(ctx context.Context, runs spillmeta.Slice[externalSortRun]) (result spillmeta.Slice[externalSortRun], resultErr error) {
	defer func() {
		if resultErr != nil {
			releaseSortRuns(&runs)
		}
	}()
	for len(runs.Values) > externalSortFanIn {
		var next spillmeta.Slice[externalSortRun]
		for start := 0; start < len(runs.Values); start += externalSortFanIn {
			end := minInt(start+externalSortFanIn, len(runs.Values))
			merged, err := s.mergeRunGroup(ctx, runs.Values[start:end])
			if err != nil {
				releaseSortRuns(&next)
				return result, err
			}
			if err := next.Append(ctx, s.metadata, merged); err != nil {
				merged.segments.Release()
				releaseSortRuns(&next)
				return result, err
			}
			for i := start; i < end; i++ {
				runs.Values[i].segments.Release()
			}
		}
		runs.Release()
		runs = next
		s.mergeLevels++
	}
	return runs, nil
}

func (s *externalSortStream) prepare(ctx context.Context) error {
	runs, err := s.generateRuns(ctx)
	if err != nil {
		return err
	}
	runs, err = s.mergeHierarchy(ctx, runs)
	if err != nil {
		return err
	}
	s.finalRuns = runs
	s.finalDecoded, s.finalHeap, err = s.openCursors(ctx, s.finalRuns.Values, true)
	if err != nil {
		return err
	}
	s.prepared = true
	return nil
}

func (s *externalSortStream) releaseOutput() {
	if s.outputBatch != nil {
		s.outputBatch.ReleaseOwned()
		s.outputBatch = nil
	}
	if s.outputMem != nil {
		s.outputMem.Release()
		s.outputMem = nil
	}
}

// Next emits the next bounded k-way merge batch.
func (s *externalSortStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("external sort destination is nil")
	}
	dst.ClearColumns()
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, s.abort(err)
	}
	if !s.prepared {
		if err := s.prepare(ctx); err != nil {
			return false, s.abort(err)
		}
	}
	if s.finalHeap.Len() == 0 {
		s.exhausted = true
		return false, s.cleanup()
	}
	batch, reservation, err := s.buildMergedBatch(ctx, &s.finalHeap)
	if err != nil {
		return false, s.abort(err)
	}
	delete(batch.Uint64s, s.ordinal)
	s.outputBatch, s.outputMem = batch, reservation
	s.outputRows += int64(batch.Rows)
	bindDecodedBatch(dst, batch)
	return true, nil
}

func (s *externalSortStream) cleanup() error {
	s.releaseOutput()
	for _, cursor := range s.finalDecoded {
		cursor.close()
	}
	s.finalDecoded = nil
	s.finalHeap.items = nil
	releaseSortRuns(&s.finalRuns)
	err := s.store.Close()
	s.store = nil
	s.internal = memory.Schema{}
	s.ordinal = ""
	if s.baseMetadata != nil {
		s.baseMetadata.Release()
		s.baseMetadata = nil
	}
	return err
}

func (s *externalSortStream) abort(err error) error {
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	return errors.Join(err, s.cleanup())
}

// Close releases decoded runs, reservations, and query-owned spill files.
func (s *externalSortStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return s.cleanup()
}

// StreamDiagnostics reports external run and merge metrics.
func (s *externalSortStream) StreamDiagnostics() map[string]string {
	snapshot := s.query.Snapshot()
	return map[string]string{
		"strategy": "external-stable", "input_rows": fmt.Sprint(s.inputRows), "output_rows": fmt.Sprint(s.outputRows),
		"spill_runs": fmt.Sprint(s.spillRuns), "spill_bytes": fmt.Sprint(s.spillBytes),
		"merge_levels": fmt.Sprint(s.mergeLevels), "merge_fan_in": fmt.Sprint(externalSortFanIn),
		"peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(snapshot.PeakBytes),
	}
}

var _ source.BatchStreamDiagnostics = (*externalSortStream)(nil)
