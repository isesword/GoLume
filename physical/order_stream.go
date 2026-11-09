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
	"unsafe"
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
	segments        spillmeta.Slice[sortSegmentRef]
	rows            int64
	maxSegmentBytes int64
	bytes           int64
	maxRowBytes     int64
	maxCodecBytes   int64
	depth           int
}

const externalSortLevels = 32

type externalSortLevel struct {
	runs  [externalSortFanIn - 1]externalSortRun
	count int
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

	sortBudget memory.Manager
	encode     memory.Manager
	restore    memory.Manager
	output     memory.Manager
	workspace  *sortWorkspace
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
	// Covers the workspace owner and its bounded live allocation tokens.
	if metadataBytes > math.MaxInt64-4096 {
		return nil, memory.ErrBudgetExceeded
	}
	metadataBytes += 4096
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
	workspace := &sortWorkspace{parent: query.Operator("ExternalSortWorkspace", memory.MemoryWorkspace)}
	return &externalSortStream{
		owner: owner, query: query, store: spillmeta.NewStore(store, metadata), opts: opts, internal: internal,
		fingerprint: memory.SchemaFingerprint(internal), ordinal: ordinal, runBytes: runBytes, chunkRows: chunkRows,
		sortBudget: workspace, encode: workspace, restore: workspace, output: workspace, workspace: workspace,
		metadata: metadata, baseMetadata: baseMetadata,
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
	sortBytes := estimate*2 + int64(rows)*16 + 1024 + bindingBytes
	encodedBytes, err := memory.EstimateColumnarRunRangeBytes(s.owner.input.Schema(), batch, start, rows)
	if err != nil {
		return externalSortRun{}, err
	}
	peak, err := sortWorkspaceSize(s.workspace.Used(), sortBytes, encodedBytes, int64(rows)*8, 4)
	if err != nil {
		return externalSortRun{}, err
	}
	if err = s.workspace.ensure(ctx, peak); err != nil {
		return externalSortRun{}, err
	}
	reservation, err := s.sortBudget.Reserve(ctx, sortBytes)
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
	run.maxSegmentBytes = ref.handle.Bytes()
	run.bytes = ref.handle.Bytes()
	run.maxRowBytes = estimate + 8
	run.maxCodecBytes, err = sortBatchCodecBytes(s.internal, sorted)
	if err != nil {
		run.segments.Release()
		return externalSortRun{}, err
	}
	return run, nil
}

func (s *externalSortStream) generateRuns(ctx context.Context) (result spillmeta.Slice[externalSortRun], resultErr error) {
	frontierBytes := 2 * int64(unsafe.Sizeof([externalSortLevels]externalSortLevel{}))
	frontier, err := s.metadata.Reserve(ctx, frontierBytes)
	if err != nil {
		return result, err
	}
	defer frontier.Release()
	var levels [externalSortLevels]externalSortLevel
	defer func() {
		for i := range levels {
			for j := 0; j < levels[i].count; j++ {
				levels[i].runs[j].segments.Release()
			}
		}
	}()
	addRun := func(owner *externalSortRun) error {
		// Move, rather than borrow, the incoming segment-array owner. A merge
		// may release that array while this call is still on the stack.
		run := *owner
		*owner = externalSortRun{}
		for i := range levels {
			level := &levels[i]
			if level.count < len(level.runs) {
				level.runs[level.count] = run
				level.count++
				return nil
			}
			var inputs [externalSortFanIn]externalSortRun
			copy(inputs[:], level.runs[:])
			inputs[externalSortFanIn-1] = run
			run = externalSortRun{}
			clear(level.runs[:])
			level.count = 0
			merged, err := s.mergeRunGroup(ctx, inputs[:])
			if err != nil {
				return err
			}
			run = merged
			merged = externalSortRun{}
		}
		run.segments.Release()
		return fmt.Errorf("external sort run frontier overflow")
	}
	stream, err := source.OpenBatchStream(ctx, s.owner.input, s.opts)
	if err != nil {
		return result, err
	}
	batch := memory.NewBatch()
	defer batch.ClearColumns()
	var runs spillmeta.Slice[externalSortRun]
	defer func() {
		if resultErr != nil {
			releaseSortRuns(&runs)
		}
	}()
	var ordinalBase uint64
	for {
		// The child may be another sort. Never hold idle phase credit while
		// asking it for input; its live output remains independently admitted.
		s.workspace.trim(0)
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			return result, errors.Join(nextErr, stream.Close())
		}
		if !ok {
			batch.ClearColumns()
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
			if err := addRun(&run); err != nil {
				_ = stream.Close()
				return result, err
			}
			start = end
		}
		ordinalBase += uint64(batch.Rows)
		s.inputRows += int64(batch.Rows)
		// Drop borrowed vector headers before the child invalidates its output
		// at Next/Close, not after later frontier merges have finished.
		batch.ClearColumns()
	}
	for i := range levels {
		for j := 0; j < levels[i].count; j++ {
			if err := runs.Append(ctx, s.metadata, levels[i].runs[j]); err != nil {
				return result, err
			}
			levels[i].runs[j] = externalSortRun{}
		}
		levels[i].count = 0
	}
	return runs, nil
}

type externalSortCursor struct {
	owner          *externalSortStream
	run            *externalSortRun
	segment        int
	row            int
	decoded        *memory.DecodedColumnarRun
	remainingBytes int64
	loadedBytes    int64
}

func (c *externalSortCursor) load(ctx context.Context) error {
	for c.decoded == nil && c.segment < len(c.run.segments.Values) {
		ref := c.run.segments.Values[c.segment]
		loadedBytes := ref.handle.Bytes()
		decoded, err := c.owner.restoreSegment(ctx, ref)
		if err != nil {
			return err
		}
		if err := c.owner.deleteSegment(ctx, ref); err != nil {
			_ = decoded.Close()
			return err
		}
		// The decoded batch is independent. Drop the consumed handle reference
		// before releasing its array when no unread segment remains.
		c.run.segments.Values[c.segment] = sortSegmentRef{}
		ref = sortSegmentRef{}
		if c.segment+1 == len(c.run.segments.Values) {
			c.run.segments.Release()
		}
		c.decoded = decoded
		c.loadedBytes = loadedBytes
		c.row = 0
		if decoded.Batch.Rows == 0 {
			_ = decoded.Close()
			c.decoded = nil
			c.remainingBytes -= c.loadedBytes
			c.loadedBytes = 0
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
		c.remainingBytes -= c.loadedBytes
		c.loadedBytes = 0
	}
	c.segment++
	return c.load(ctx)
}

func (c *externalSortCursor) close() {
	c.run = nil
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

func (s *externalSortStream) openCursors(ctx context.Context, runs []externalSortRun) ([]*externalSortCursor, externalSortHeap, error) {
	if len(runs) > externalSortFanIn {
		return nil, externalSortHeap{}, fmt.Errorf("external sort fan-in exceeded")
	}
	cursors := make([]*externalSortCursor, 0, len(runs))
	h := externalSortHeap{keys: s.owner.keys, ord: s.ordinal, items: make([]*externalSortCursor, 0, len(runs))}
	for i := range runs {
		cursor := &externalSortCursor{owner: s, run: &runs[i], remainingBytes: runs[i].bytes}
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

func sortDictionaryDomainBytes(batch *memory.Batch) (int64, error) {
	var total int64
	for _, v := range batch.DictStrings {
		if v == nil {
			return 0, fmt.Errorf("nil dictionary output")
		}
		var err error
		total, err = sortWorkspaceSize(total, int64(len(v.DictionaryDomain)))
		if err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Structs {
		if v == nil || v.Values == nil {
			return 0, fmt.Errorf("nil struct output")
		}
		children, err := sortDictionaryDomainBytes(v.Values)
		if err != nil {
			return 0, err
		}
		total, err = sortWorkspaceSize(total, children)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func sortBatchCodecBytes(schema memory.Schema, batch *memory.Batch) (int64, error) {
	encoded, err := memory.EstimateColumnarRunRangeBytes(schema, batch, 0, batch.Rows)
	if err != nil {
		return 0, err
	}
	data, err := memory.EstimateBatchRangeBytes(batch, 0, batch.Rows)
	if err != nil {
		return 0, err
	}
	// A merge may produce a new 64-byte SHA256 dictionary identity.
	// Reserve that maximum even when the input domain is shorter.
	extra, err := sortDictionaryIdentityGrowth(batch)
	if err != nil {
		return 0, err
	}
	return sortWorkspaceSize(encoded-data, extra)
}

func sortDictionaryIdentityGrowth(batch *memory.Batch) (int64, error) {
	var total int64
	for _, v := range batch.DictStrings {
		var err error
		total, err = sortWorkspaceSize(total, max(0, int64(64-len(v.DictionaryDomain))))
		if err != nil {
			return 0, err
		}
	}
	for _, v := range batch.Structs {
		n, err := sortDictionaryIdentityGrowth(v.Values)
		if err != nil {
			return 0, err
		}
		total, err = sortWorkspaceSize(total, n)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func (s *externalSortStream) buildMergedBatch(ctx context.Context, h *externalSortHeap) (*memory.Batch, memory.Reservation, error) {
	if h.Len() == 0 {
		return nil, nil, nil
	}
	var remainingBytes int64
	for _, cursor := range h.items {
		var err error
		remainingBytes, err = sortWorkspaceSize(remainingBytes, cursor.remainingBytes)
		if err != nil {
			return nil, nil, err
		}
	}
	targetBytes := min(s.runBytes, remainingBytes)
	if targetBytes > (math.MaxInt64-1024)/3 {
		return nil, nil, memory.ErrBudgetExceeded
	}
	accumulator := newBatchAccumulator(s.internal)
	accumulator.ownedDictionaries = true
	var reservation memory.Reservation
	var estimated, domainCharge int64
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
			reserveBytes := targetBytes*3 + 1024
			if rowBytes > targetBytes {
				if rowBytes > (math.MaxInt64-1024)/3 {
					return nil, nil, fmt.Errorf("external sort output row estimate overflows int64")
				}
				reserveBytes = rowBytes*3 + 1024
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
		// Dictionary domains are borrowed from decoded batches, just like their
		// dictionaries. BatchBytes excludes these strings. Transfer admission
		// to the output before advancing can close the last decoded owner.
		domains, err := sortDictionaryDomainBytes(accumulator.batch)
		if err == nil && domains > domainCharge {
			err = reservation.(*sortWorkspaceReservation).grow(ctx, domains-domainCharge)
			if err == nil {
				domainCharge = domains
			}
		}
		if err != nil {
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
	// A mixed dictionary receives a new content identity at finish. Admit
	// that identity too before exposing the output beyond this phase.
	domains, domainErr := sortDictionaryDomainBytes(batch)
	if domainErr == nil && domains > domainCharge {
		domainErr = reservation.(*sortWorkspaceReservation).grow(ctx, domains-domainCharge)
	}
	if domainErr != nil {
		batch.ReleaseOwned()
		reservation.Release()
		return nil, nil, domainErr
	}
	if reservation != nil && memory.BatchBytes(batch) > reservation.Bytes() {
		actual := memory.BatchBytes(batch)
		batch.ReleaseOwned()
		reservation.Release()
		return nil, nil, fmt.Errorf("external sort output used %d bytes beyond pre-reserved workspace", actual)
	}
	return batch, reservation, nil
}

// reserveMergeWorkspace protects the full decode/restore, output, and encode
// overlap using the inputs' actual admitted maximum segment and row sizes.
func (s *externalSortStream) reserveCursorWorkspace(ctx context.Context, h *externalSortHeap) error {
	// The plan borrows scalar sizes only, never another owner's segment slice.
	var plans [externalSortFanIn]externalSortRun
	for i, c := range h.items {
		plans[i] = externalSortRun{bytes: c.remainingBytes, maxSegmentBytes: c.run.maxSegmentBytes, maxRowBytes: c.run.maxRowBytes, maxCodecBytes: c.run.maxCodecBytes}
	}
	return s.reserveMergeWorkspace(ctx, plans[:len(h.items)], false)
}

func (s *externalSortStream) reserveMergeWorkspace(ctx context.Context, inputs []externalSortRun, encode bool) error {
	var decoded, restoreOverlap, totalBytes, maxRow, codecBytes int64
	for _, run := range inputs {
		var err error
		decoded, err = sortWorkspaceSize(decoded, run.maxSegmentBytes)
		if err != nil {
			return err
		}
		totalBytes, err = sortWorkspaceSize(totalBytes, run.bytes)
		if err != nil {
			return err
		}
		restoreOverlap = max(restoreOverlap, run.maxSegmentBytes)
		maxRow = max(maxRow, run.maxRowBytes)
		codecBytes, err = sortWorkspaceSize(codecBytes, run.maxCodecBytes)
		if err != nil {
			return err
		}
	}
	target := max(min(s.runBytes, totalBytes), maxRow)
	if target > (math.MaxInt64-4096)/4 {
		return memory.ErrBudgetExceeded
	}
	// Each append may briefly retain an old buffer plus a grown capacity.
	// Three times logical payload covers this overlap, not only steady capacity.
	output, err := sortWorkspaceSize(target*3, 1024, codecBytes)
	if err != nil {
		return err
	}
	overlap := restoreOverlap
	if encode {
		encoded, err := sortWorkspaceSize(target, 1024, codecBytes)
		if err != nil {
			return err
		}
		overlap = max(overlap, encoded)
	}
	peak, err := sortWorkspaceSize(decoded, output, overlap, 2048)
	if err != nil {
		return err
	}
	return s.workspace.ensure(ctx, peak)
}

func (s *externalSortStream) mergeRunGroup(ctx context.Context, inputs []externalSortRun) (result externalSortRun, resultErr error) {
	defer s.workspace.trim(0)
	defer func() {
		for i := range inputs {
			inputs[i].segments.Release()
		}
	}()
	depth := 0
	for _, run := range inputs {
		depth = max(depth, run.depth+1)
	}
	if err := s.reserveMergeWorkspace(ctx, inputs, true); err != nil {
		return result, err
	}
	cursors, h, err := s.openCursors(ctx, inputs)
	if err != nil {
		return externalSortRun{}, err
	}
	defer func() {
		for _, cursor := range cursors {
			cursor.close()
		}
	}()
	output := externalSortRun{depth: depth}
	for _, run := range inputs {
		output.maxRowBytes = max(output.maxRowBytes, run.maxRowBytes)
	}
	s.mergeLevels = max(s.mergeLevels, depth)
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
		codecBytes, codecErr := sortBatchCodecBytes(s.internal, batch)
		if codecErr != nil {
			batch.ReleaseOwned()
			reservation.Release()
			return externalSortRun{}, codecErr
		}
		ref, err := s.spillBatch(ctx, batch)
		batch.ReleaseOwned()
		batch = nil
		reservation.Release()
		if err != nil {
			_ = s.deleteRun(context.Background(), output)
			return externalSortRun{}, err
		}
		if err := output.segments.Append(ctx, s.metadata, ref); err != nil {
			return externalSortRun{}, err
		}
		output.rows += int64(ref.rows)
		output.maxSegmentBytes = max(output.maxSegmentBytes, ref.handle.Bytes())
		output.bytes += ref.handle.Bytes()
		output.maxCodecBytes = max(output.maxCodecBytes, codecBytes)
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
func (s *externalSortStream) mergeHierarchy(ctx context.Context, owner *spillmeta.Slice[externalSortRun]) (result spillmeta.Slice[externalSortRun], resultErr error) {
	runs := *owner
	*owner = spillmeta.Slice[externalSortRun]{}
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
			merged = externalSortRun{}
			for i := start; i < end; i++ {
				runs.Values[i].segments.Release()
			}
		}
		runs.Release()
		runs = next
		next = spillmeta.Slice[externalSortRun]{}
	}
	return runs, nil
}

func (s *externalSortStream) prepare(ctx context.Context) error {
	runs, err := s.generateRuns(ctx)
	if err != nil {
		return err
	}
	runs, err = s.mergeHierarchy(ctx, &runs)
	if err != nil {
		return err
	}
	s.finalRuns = runs
	runs = spillmeta.Slice[externalSortRun]{}
	if err = s.reserveMergeWorkspace(ctx, s.finalRuns.Values, false); err != nil {
		return err
	}
	s.finalDecoded, s.finalHeap, err = s.openCursors(ctx, s.finalRuns.Values)
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
	if err := s.reserveCursorWorkspace(ctx, &s.finalHeap); err != nil {
		return false, s.abort(err)
	}
	batch, reservation, err := s.buildMergedBatch(ctx, &s.finalHeap)
	if err != nil {
		return false, s.abort(err)
	}
	delete(batch.Uint64s, s.ordinal)
	s.outputBatch, s.outputMem = batch, reservation
	s.outputRows += int64(batch.Rows)
	bindDecodedBatch(dst, batch)
	// Downstream operators may need to prepare while this batch is borrowed.
	// Keep admission for live buffers, not unused headroom from this phase.
	s.workspace.trim(s.workspace.Used())
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
	s.workspace.close()
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
