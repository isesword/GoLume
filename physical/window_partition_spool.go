package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"unsafe"
)

type percentRankPage struct {
	handle  memory.SpillHandle
	encoded *memory.EncodedColumnarRun
	rows    int
}

type percentRankPartitionStream struct {
	tiles                                []*memory.Int64Vector
	metrics                              *percentRankCounters
	coldPages, coldBytes, coldPartitions int64
	owner                                *percentRankSource
	child                                source.BatchStream
	input                                *memory.Batch
	offset                               int
	eof, closed                          bool
	manager, metadata                    memory.Manager
	base                                 memory.Reservation
	store                                *spillmeta.Store
	query                                *memory.QueryBudget
	fingerprint                          [32]byte
	pages                                spillmeta.Slice[percentRankPage]
	page                                 int
	total, position, rank                int64
	partitionKeys                        []SortKey
	partition, previous                  rowCandidate
	partitionLease, previousLease        memory.Reservation
	decoded                              *memory.DecodedColumnarRun
	ratio                                *memory.Float64Vector
	outputLease                          memory.Reservation
	chunkRows                            int
	cume                                 *memory.Float64Vector
	peekDecoded                          *memory.DecodedColumnarRun
	peekPage, peekRow                    int
	peekPosition, peerEnd                int64
	peerKey                              rowCandidate
	peerLease                            memory.Reservation
	hasCume                              bool
	hasReplay                            bool
	coldPeekPages, coldReplayPages       int64
}

func newPercentRankPartitionStream(ctx context.Context, owner *percentRankSource, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("percent_rank context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := validatePercentRankPayload(owner.original); err != nil {
		return nil, err
	}
	manager, metadata := owner.manager, owner.manager
	query := memory.QueryBudgetFromContext(ctx)
	if query != nil {
		manager = query.Operator("PercentRankWorkspace", memory.MemoryWorkspace)
		metadata = spillmeta.GrowthManager(query.Operator("PercentRankMetadata", memory.MemoryRetainedState))
	}
	if manager == nil || metadata == nil {
		return nil, fmt.Errorf("percent_rank requires memory accounting")
	}
	if manager.Limit() <= 0 {
		return nil, fmt.Errorf("percent_rank streaming requires a finite memory budget")
	}
	bytes, err := futureBudgetAdd(8192, int64(len(owner.schema.Fields)), 512)
	if err == nil {
		bytes, err = futureBudgetAdd(bytes, int64(len(owner.partition)+len(owner.order)), 256)
	}
	if err != nil {
		return nil, err
	}
	base, err := metadata.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	metrics := owner.metrics
	if metrics == nil {
		metrics = new(percentRankCounters)
	}
	s := &percentRankPartitionStream{metrics: metrics, owner: owner, manager: manager, metadata: metadata, base: base, query: query, fingerprint: memory.SchemaFingerprint(owner.original), chunkRows: 1024}
	if opts.BatchRows > 0 && opts.BatchRows < s.chunkRows {
		s.chunkRows = opts.BatchRows
	}
	s.hasCume = owner.cumeHidden != ""
	s.hasReplay = s.hasCume || len(owner.ntilePlans) > 0
	s.input = memory.NewBatch()
	for _, name := range owner.partition {
		s.partitionKeys = append(s.partitionKeys, SortKey{Column: name})
	}
	if query != nil && query.SpillConfigured() {
		store, e := query.SpillManager()
		if e != nil {
			_ = s.Close()
			return nil, e
		}
		s.store = spillmeta.NewStore(store, metadata)
	}
	child, err := source.OpenBatchStream(ctx, owner.input, opts)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	s.child = child
	return s, nil
}

// replaceKey reserves the complete replacement before allocating/copying. The
// previous snapshot remains charged until every borrowed byte has been copied.
func (s *percentRankPartitionStream) replaceKey(ctx context.Context, dst *rowCandidate, lease *memory.Reservation, batch *memory.Batch, keys []SortKey, row int) error {
	text, err := sortKeyVariableBytes(batch, s.owner.original, keys, row)
	if err != nil {
		return err
	}
	bytes, err := futureBudgetAdd(256, int64(len(keys)), 2*int64(unsafe.Sizeof(sortCell{}))+128)
	if err == nil {
		bytes, err = futureBudgetAdd(bytes, text, 2)
	}
	if err != nil {
		return err
	}
	nextLease, err := s.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	next := rowCandidate{}
	if err = snapshotCandidateInto(&next, batch, s.owner.original, keys, row, 0); err != nil {
		nextLease.Release()
		return err
	}
	old := *lease
	*dst = next
	*lease = nextLease
	if old != nil {
		old.Release()
	}
	return nil
}

func (s *percentRankPartitionStream) ensureInput(ctx context.Context) (bool, error) {
	for s.offset >= s.input.Rows && !s.eof {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		// Removing bindings is NOT retirement of the upstream owner. The child is
		// still charged, and may retain its previous output until this Next/Close.
		s.input.ClearColumns()
		ok, err := s.child.Next(ctx, s.input)
		if err != nil {
			return false, err
		}
		s.offset = 0
		if !ok {
			s.eof = true
			return false, nil
		}
	}
	return !s.eof, nil
}

func (s *percentRankPartitionStream) appendPage(ctx context.Context, start, count int) error {
	estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(s.input, start, count)
	if err != nil {
		return err
	}
	if estimate.OpaquePresent {
		return fmt.Errorf("percent_rank cannot spool opaque payload")
	}
	lease, err := s.manager.Reserve(ctx, estimate.Bytes)
	if err != nil {
		return fmt.Errorf("percent_rank capture clone: %w", err)
	}
	clone, err := memory.CloneBatchRange(s.input, start, count)
	if err != nil {
		lease.Release()
		return err
	}
	encoded, err := memory.EncodeColumnarRun(ctx, s.owner.original, clone, s.manager)
	clone.ClearColumns()
	clone = nil
	lease.Release()
	if err != nil {
		return err
	}
	ref := percentRankPage{encoded: encoded, rows: count}
	if s.store != nil {
		handle, e := s.store.Spill(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: memory.ColumnarRunCodec, SchemaFingerprint: s.fingerprint, Payload: encoded.Payload})
		_ = encoded.Close()
		ref.encoded = nil
		if e != nil {
			return e
		}
		ref.handle = handle
		s.metrics.pages.Add(1)
		s.metrics.bytes.Add(handle.Bytes())
		s.query.RecordSpilledBytes(handle.Bytes())
	}
	if err = s.pages.Append(ctx, s.metadata, ref); err != nil {
		if ref.encoded != nil {
			_ = ref.encoded.Close()
		}
		if ref.handle != nil {
			err = errors.Join(err, s.store.Delete(context.Background(), ref.handle))
		}
		return err
	}
	return nil
}

func (s *percentRankPartitionStream) capturePartition(ctx context.Context) (bool, error) {
	ok, err := s.ensureInput(ctx)
	if err != nil || !ok {
		return ok, err
	}
	s.total, s.position, s.rank = 0, 0, 0
	if err = s.replaceKey(ctx, &s.partition, &s.partitionLease, s.input, s.partitionKeys, s.offset); err != nil {
		return false, err
	}
	for {
		ok, err = s.ensureInput(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		same, e := batchRowEqualsCandidate(s.input, s.owner.original, s.partitionKeys, s.offset, s.partition)
		if e != nil {
			return false, e
		}
		if !same {
			break
		}
		start, end := s.offset, s.offset
		limit := s.input.Rows
		if limit-start > s.chunkRows {
			limit = start + s.chunkRows
		}
		for end < limit {
			if err = ctx.Err(); err != nil {
				return false, err
			}
			same, e = batchRowEqualsCandidate(s.input, s.owner.original, s.partitionKeys, end, s.partition)
			if e != nil {
				return false, e
			}
			if !same {
				break
			}
			end++
		}
		count := end - start
		if count <= 0 {
			return false, fmt.Errorf("percent_rank partition made no progress")
		}
		if int64(count) > math.MaxInt64-s.total {
			return false, fmt.Errorf("percent_rank partition cardinality overflow")
		}
		if err = s.appendPage(ctx, start, count); err != nil {
			return false, err
		}
		s.total += int64(count)
		s.offset = end
	}
	s.page = 0
	s.metrics.partitions.Add(1)
	// The next partition's input tail remains borrowed from child throughout the
	// replay. Its owner is neither released nor deducted from the budget here.
	return true, nil
}

func (s *percentRankPartitionStream) releaseOutput() {
	for _, v := range s.tiles {
		v.Values = nil
		v.Validity = nil
	}
	s.tiles = nil
	if s.cume != nil {
		s.cume.Values = nil
		s.cume.Validity = nil
		s.cume = nil
	}
	if s.ratio != nil {
		s.ratio.Values = nil
		s.ratio.Validity = nil
		s.ratio = nil
	}
	if s.decoded != nil {
		_ = s.decoded.Close()
		s.decoded = nil
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
}
func (s *percentRankPartitionStream) releasePartition() error {
	if s.peekDecoded != nil {
		_ = s.peekDecoded.Close()
		s.peekDecoded = nil
	}
	s.peekPage = 0
	s.peekRow = 0
	s.peekPosition = 0
	s.peerEnd = 0
	s.peerKey = rowCandidate{}
	if s.peerLease != nil {
		s.peerLease.Release()
		s.peerLease = nil
	}
	var result error
	for i := range s.pages.Values {
		ref := &s.pages.Values[i]
		if ref.encoded != nil {
			_ = ref.encoded.Close()
			ref.encoded = nil
		}
		if ref.handle != nil {
			result = errors.Join(result, s.store.Delete(context.Background(), ref.handle))
			ref.handle = nil
		}
	}
	s.pages.Release()
	s.page = 0
	s.previous = rowCandidate{}
	s.partition = rowCandidate{}
	if s.previousLease != nil {
		s.previousLease.Release()
		s.previousLease = nil
	}
	if s.partitionLease != nil {
		s.partitionLease.Release()
		s.partitionLease = nil
	}
	return result
}
func (s *percentRankPartitionStream) decodePage(ctx context.Context, index int) (*memory.DecodedColumnarRun, error) {
	if index < 0 || index >= len(s.pages.Values) {
		return nil, fmt.Errorf("%w: page index", memory.ErrSpillCorrupt)
	}
	ref := &s.pages.Values[index]
	if ref.rows <= 0 {
		return nil, fmt.Errorf("%w: nonpositive page rows", memory.ErrSpillCorrupt)
	}
	var restored *memory.RestoredSpill
	var err error
	if ref.handle != nil {
		restored, err = s.store.Restore(ctx, ref.handle, s.fingerprint, s.manager)
		if err != nil {
			return nil, err
		}
	} else {
		if ref.encoded == nil {
			return nil, fmt.Errorf("%w: page retired before cursor read", memory.ErrSpillCorrupt)
		}
		restored = &memory.RestoredSpill{Object: memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: memory.ColumnarRunCodec, SchemaFingerprint: s.fingerprint, Payload: ref.encoded.Payload}}
	}
	if restored.Object.Kind != memory.SpillColumnarRun || restored.Object.Codec != memory.ColumnarRunCodec {
		_ = restored.Close()
		return nil, fmt.Errorf("%w: percent_rank page kind/codec", memory.ErrSpillCorrupt)
	}
	decoded, err := memory.DecodeColumnarRun(ctx, s.owner.original, restored, s.manager)
	_ = restored.Close()
	if err != nil {
		return nil, err
	}
	if decoded.Batch.Rows <= 0 || decoded.Batch.Rows != ref.rows {
		_ = decoded.Close()
		return nil, fmt.Errorf("%w: percent_rank page rows", memory.ErrSpillCorrupt)
	}
	return decoded, nil
}

func (s *percentRankPartitionStream) restorePage(ctx context.Context, index int) error {
	decoded, err := s.decodePage(ctx, index)
	if err != nil {
		return err
	}
	s.decoded = decoded
	if s.hasReplay {
		s.metrics.replayPages.Add(1)
	}
	ref := &s.pages.Values[index]
	// Decode is now independent: only now may encoded memory/disk be retired.
	if ref.encoded != nil {
		_ = ref.encoded.Close()
		ref.encoded = nil
	}
	if ref.handle != nil {
		if err = s.store.Delete(context.Background(), ref.handle); err != nil {
			return err
		}
		ref.handle = nil
	}
	return nil
}

// ensurePeerEnd advances only the lookahead cursor. Before output retires any
// page, this cursor has decoded it (and owns its unread suffix), or passed it.
// Each cursor decodes a page at most once; no peer-end array is retained.
func (s *percentRankPartitionStream) ensurePeerEnd(ctx context.Context) error {
	if s.position < s.peerEnd {
		return nil
	}
	if s.position != s.peekPosition || s.position >= s.total {
		return fmt.Errorf("%w: peer cursor position", memory.ErrSpillCorrupt)
	}
	haveKey := false
	defer func() {
		s.peerKey = rowCandidate{}
		if s.peerLease != nil {
			s.peerLease.Release()
			s.peerLease = nil
		}
	}()
	for s.peekPosition < s.total {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.peekDecoded != nil && s.peekRow == s.peekDecoded.Batch.Rows {
			_ = s.peekDecoded.Close()
			s.peekDecoded = nil
			s.peekPage++
			s.peekRow = 0
		}
		if s.peekDecoded == nil {
			if s.peekPage >= len(s.pages.Values) {
				return fmt.Errorf("%w: peer pages exhausted", memory.ErrSpillCorrupt)
			}
			d, err := s.decodePage(ctx, s.peekPage)
			if err != nil {
				return err
			}
			s.peekDecoded = d
			s.metrics.peekPages.Add(1)
		}
		b := s.peekDecoded.Batch
		if !haveKey {
			if err := s.replaceKey(ctx, &s.peerKey, &s.peerLease, b, s.owner.order, s.peekRow); err != nil {
				return err
			}
			haveKey = true
		}
		same, err := batchRowEqualsCandidate(b, s.owner.original, s.owner.order, s.peekRow, s.peerKey)
		if err != nil {
			return err
		}
		if !same {
			break
		}
		s.peekRow++
		s.peekPosition++
	}
	s.peerEnd = s.peekPosition
	if s.peerEnd <= s.position {
		return fmt.Errorf("%w: peer made no progress", memory.ErrSpillCorrupt)
	}
	return nil
}

// Next emits one independently owned decoded page after its partition is captured.
func (s *percentRankPartitionStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	dst.ClearColumns()
	s.releaseOutput()
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
			dst.ClearColumns()
		}
	}()
	if ctx == nil {
		return false, fmt.Errorf("percent_rank context is nil")
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if s.page >= len(s.pages.Values) {
		if err = s.releasePartition(); err != nil {
			return false, err
		}
		if ok, err = s.capturePartition(ctx); err != nil || !ok {
			return ok, err
		}
	}
	if s.hasCume {
		if err = s.ensurePeerEnd(ctx); err != nil {
			return false, err
		}
		if s.peekPage < s.page {
			return false, fmt.Errorf("%w: peek behind output", memory.ErrSpillCorrupt)
		}
	}
	if err = s.restorePage(ctx, s.page); err != nil {
		return false, err
	}
	s.page++
	batch := s.decoded.Batch
	// This includes the output vector/map descriptor and rounded data backing;
	// caller maps are not aliases of decoded owner maps.
	width := int64(0)
	if s.owner.hidden != "" {
		width += 16
	}
	if s.hasCume {
		width += 16
	}
	// make([]int64, rows) allocates once, without append spare capacity.
	// Go 1.27 noscan rounding <= 1.25*n+6.75: 10 bytes/row plus
	// per-vector 64-byte rounding slack (included in 256 metadata below).
	width, e := futureBudgetAdd(width, int64(len(s.owner.ntilePlans)), 10)
	if e != nil {
		return false, e
	}
	metadata, e := futureBudgetAdd(1024, int64(len(s.owner.ntilePlans)), 256)
	if e == nil && len(s.owner.ntilePlans) > 0 {
		metadata, e = futureBudgetAdd(metadata, int64(len(s.owner.original.Fields)), 128)
	}
	if e != nil {
		return false, e
	}
	bytes, e := futureBudgetAdd(metadata, int64(batch.Rows), width)
	if e != nil {
		return false, e
	}
	s.outputLease, err = s.manager.Reserve(ctx, bytes)
	if err != nil {
		return false, err
	}
	if s.owner.hidden != "" {
		s.ratio = &memory.Float64Vector{Values: make([]float64, batch.Rows)}
	}
	if s.hasCume {
		s.cume = &memory.Float64Vector{Values: make([]float64, batch.Rows)}
	}
	if len(s.owner.ntilePlans) > 0 {
		s.tiles = make([]*memory.Int64Vector, len(s.owner.ntilePlans))
		for i := range s.tiles {
			s.tiles[i] = &memory.Int64Vector{Values: make([]int64, batch.Rows)}
		}
	}
	for row := 0; row < batch.Rows; row++ {
		if err = ctx.Err(); err != nil {
			return false, err
		}
		if s.hasCume {
			if err = s.ensurePeerEnd(ctx); err != nil {
				return false, err
			}
			s.cume.Values[row] = float64(s.peerEnd) / float64(s.total)
		}
		for i, p := range s.owner.ntilePlans {
			value, e := windowNTileValue(s.position, s.total, p.buckets)
			if e != nil {
				return false, e
			}
			s.tiles[i].Values[row] = value
		}
		same := false
		if s.owner.hidden != "" && s.position > 0 {
			if row == 0 {
				same, err = batchRowEqualsCandidate(batch, s.owner.original, s.owner.order, row, s.previous)
			} else {
				same, err = percentRankRowsPeer(batch, s.owner.original, s.owner.order, row-1, row)
			}
			if err != nil {
				return false, err
			}
		}
		if s.position == math.MaxInt64 {
			return false, fmt.Errorf("percent_rank replay cardinality overflow")
		}
		s.position++
		if !same {
			s.rank = s.position
		}
		if s.ratio != nil && s.total > 1 {
			s.ratio.Values[row] = float64(s.rank-1) / float64(s.total-1)
		}
	}
	if s.ratio != nil && batch.Rows > 0 {
		if err = s.replaceKey(ctx, &s.previous, &s.previousLease, batch, s.owner.order, batch.Rows-1); err != nil {
			return false, err
		}
	}
	if s.position > s.total || s.page == len(s.pages.Values) && s.position != s.total {
		return false, fmt.Errorf("%w: percent_rank partition rows", memory.ErrSpillCorrupt)
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	bindAll(dst, batch)
	if s.ratio != nil {
		dst.Float64s[s.owner.hidden] = s.ratio
	}
	if s.cume != nil {
		dst.Float64s[s.owner.cumeHidden] = s.cume
	}
	if len(s.tiles) > 0 && dst.Int64s == nil {
		dst.Int64s = make(map[string]*memory.Int64Vector, len(s.tiles))
	}
	for i, p := range s.owner.ntilePlans {
		dst.Int64s[p.hidden] = s.tiles[i]
	}
	dst.Rows = batch.Rows
	return true, nil
}
func percentRankRowsPeer(batch *memory.Batch, schema memory.Schema, keys []SortKey, a, b int) (bool, error) {
	for _, key := range keys {
		x, e := snapshotCell(batch, schema, key.Column, a)
		if e != nil {
			return false, e
		}
		y, e := snapshotCell(batch, schema, key.Column, b)
		if e != nil {
			return false, e
		}
		if compareSortCells(x, y, key) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Close releases private pages and owners without closing the shared query store.
func (s *percentRankPartitionStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.releaseOutput()
	result := s.releasePartition()
	if s.child != nil {
		result = errors.Join(result, s.child.Close())
		s.child = nil
	}
	s.input = nil
	s.partitionKeys = nil
	s.owner = nil
	if s.metrics != nil {
		s.coldPages = s.metrics.pages.Load()
		s.coldBytes = s.metrics.bytes.Load()
		s.coldPartitions = s.metrics.partitions.Load()
		s.coldPeekPages = s.metrics.peekPages.Load()
		s.coldReplayPages = s.metrics.replayPages.Load()
		s.metrics = nil
	}
	if s.store != nil {
		result = errors.Join(result, s.store.Close())
		s.store = nil
	}
	if s.base != nil {
		s.base.Release()
		s.base = nil
	}
	s.manager = nil
	s.metadata = nil
	s.query = nil
	return result
}

// StreamDiagnostics reports only actual partition-spool work, also after Close.
func (s *percentRankPartitionStream) StreamDiagnostics() map[string]string {
	pages, bytes, partitions := s.coldPages, s.coldBytes, s.coldPartitions
	if s.metrics != nil {
		pages = s.metrics.pages.Load()
		bytes = s.metrics.bytes.Load()
		partitions = s.metrics.partitions.Load()
	}
	out := map[string]string{"strategy": "partition-spool-replay", "partition_spool_pages": fmt.Sprint(pages), "partition_spool_bytes": fmt.Sprint(bytes), "partition_spool_partitions": fmt.Sprint(partitions)}
	if s.hasReplay {
		peek, replay := s.coldPeekPages, s.coldReplayPages
		if s.metrics != nil {
			peek = s.metrics.peekPages.Load()
			replay = s.metrics.replayPages.Load()
		}
		if s.hasCume {
			out["peer_lookahead_pages"] = fmt.Sprint(peek)
		}
		out["partition_replay_pages"] = fmt.Sprint(replay)
	}
	return out
}
