package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source"
	"math"
	"sync"
	"time"
	"unsafe"
)

const (
	joinRadixBits       = uint(4)
	joinRadixPartitions = 1 << joinRadixBits
	maxJoinRadixDepth   = int(64 / joinRadixBits)
)

// ErrJoinSkewExhausted reports a build partition that cannot fit after all
// hash bits have been consumed. It is preferable to unbounded recursion or a
// silent resident fallback for an adversarial/colliding key distribution.
var ErrJoinSkewExhausted = errors.New("join radix repartition exhausted all hash bits")

type joinRunRef struct {
	handle    memory.SpillHandle
	rows      int
	partition int
}

type residentJoinBatchStream struct {
	reader             *joinReader
	closed             bool
	probeExhausted     bool
	exhausted          bool
	probeRows          int64
	probeBytes         int64
	matchedProbeRows   int64
	outputRows         int64
	matchedBuild       []bool
	unmatchedNext      int
	started            time.Time
	statsRecorded      bool
	feedbackEligible   bool
	workspace          *memory.OperatorBudget
	state              *memory.OperatorBudget
	outputReservation  memory.Reservation
	matchedReservation memory.Reservation
	byteOutput         *memory.Batch
	terminalErr        error
	byteQuery          *memory.QueryBudget
	bytePartitions     []source.Partition
	bytePartitionMem   memory.Reservation
}

// NewBatchStream opens either the execution-scoped spill implementation or a
// native stream over the retained broadcast build.
func (s *JoinSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, nil, false)
}

func (s *JoinSource) partitionParallel() bool {
	return !s.spillable && !s.merge && s.typ != join.Right && s.typ != join.Full && source.CanParallelizePartitions(source.StreamPlanOf(s.left))
}

// NewBatchStreamWithPartitions restricts a partition-safe resident join to the
// requested probe fragments. Global joins reject subsets instead of replaying
// the entire probe side for every worker.
func (s *JoinSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if !s.partitionParallel() {
		return nil, fmt.Errorf("join strategy %s type %s does not support partition subsets", s.Strategy(), s.typ)
	}
	var partitionMem memory.Reservation
	if s.shape == joinBytes {
		if ctx == nil {
			return nil, fmt.Errorf("join stream context is nil")
		}
		if manager := s.byteExecutionManager(ctx, opts); manager != nil {
			bytes, err := safeMulRows(int64(len(parts)), 16)
			if err != nil {
				return nil, err
			}
			reservation, err := manager.Reserve(ctx, bytes)
			if err != nil {
				return nil, err
			}
			partitionMem = reservation
		}
	}
	success := false
	defer func() {
		if !success && partitionMem != nil {
			partitionMem.Release()
		}
	}()
	selected := make([]source.Partition, len(parts))
	for i, part := range parts {
		if part.ID < 0 || part.ID >= len(s.leftParts) {
			return nil, fmt.Errorf("invalid join partition ID %d", part.ID)
		}
		selected[i] = s.leftParts[part.ID]
	}
	stream, err := s.newBatchStream(ctx, opts, selected, true)
	if err != nil {
		return nil, err
	}
	if s.shape == joinBytes {
		resident := stream.(*residentJoinBatchStream)
		resident.bytePartitions, resident.bytePartitionMem = selected, partitionMem
	}
	success = true
	return stream, nil
}

func (s *JoinSource) newBatchStream(ctx context.Context, opts source.ReadOptions, probeParts []source.Partition, subset bool) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("join stream context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.Kernels != s.kernels {
		return nil, fmt.Errorf("physical join stream kernel set differs from prepared build")
	}
	if s.closed {
		return nil, fmt.Errorf("physical join source closed")
	}
	if s.spillable {
		return newSpillableJoinStream(ctx, s, opts)
	}
	if s.merge {
		return newMergeJoinStream(ctx, s, opts)
	}
	if s.shape == joinBytes {
		if query := memory.QueryBudgetFromContext(ctx); query != nil {
			opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
		}
	}
	var left source.BatchStream
	var err error
	if subset {
		left, err = source.OpenBatchStreamWithPartitions(ctx, s.left, opts, probeParts)
	} else {
		left, err = source.OpenBatchStream(ctx, s.left, opts)
	}
	if err != nil {
		return nil, err
	}
	stream := &residentJoinBatchStream{
		reader:           &joinReader{owner: s, left: left, leftBatch: memory.NewBatch()},
		started:          time.Now(),
		feedbackEligible: !subset,
	}
	if s.shape == joinBytes {
		stream.byteQuery = memory.QueryBudgetFromContext(ctx)
		stream.reader.byteManager = s.byteExecutionManager(ctx, opts)
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query.Limit() > 0 {
		stream.workspace = query.Operator("BroadcastJoinWorkspace", memory.MemoryWorkspace)
		stream.state = query.Operator("BroadcastJoinMatchedBuild", memory.MemoryRetainedState)
	}
	if s.typ == join.Right || s.typ == join.Full {
		if stream.state == nil && s.shape == joinBytes && stream.reader.byteManager != nil {
			stream.matchedReservation, err = stream.reader.byteManager.Reserve(ctx, int64(len(s.buildLoc)))
			if err != nil {
				return nil, errors.Join(err, left.Close())
			}
		}
		if stream.state != nil {
			stream.matchedReservation, err = stream.state.Reserve(ctx, int64(len(s.buildLoc)))
			if err != nil {
				if s.shape == joinBytes {
					return nil, errors.Join(err, left.Close())
				}
				_ = left.Close()
				return nil, err
			}
		}
		stream.matchedBuild = make([]bool, len(s.buildLoc))
	}
	return stream, nil
}

// Next probes one native child batch. Bounded high-multiplicity output is
// handled by spillableJoinStream; MemoryOnly keeps the broadcast fast path.
func (s *residentJoinBatchStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, resultErr error) {
	if dst == nil {
		err := fmt.Errorf("join stream destination is nil")
		if s.reader.owner.shape == joinBytes {
			if s.terminalErr != nil {
				return false, s.terminalErr
			}
			err = errors.Join(err, s.Close())
			s.terminalErr = err
		}
		return false, err
	}
	if s.reader.owner.shape == joinBytes {
		if s.byteOutput != nil {
			s.byteOutput.ClearColumns()
			s.byteOutput = nil
		}
		dst.ClearColumns()
		if s.terminalErr != nil {
			return false, s.terminalErr
		}
		defer func() {
			if resultErr != nil && !s.closed {
				dst.ClearColumns()
				resultErr = errors.Join(resultErr, s.Close())
				s.terminalErr = resultErr
			} else if ok {
				s.byteOutput = dst
			}
		}()
		if s.closed {
			return false, source.ErrBatchStreamClosed
		}
		if ctx == nil {
			return false, fmt.Errorf("generic join context is nil")
		}
		if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.byteQuery {
			return false, fmt.Errorf("generic join stream cannot change query ownership")
		} else if query == nil && s.byteQuery != nil {
			ctx = memory.WithQueryBudget(ctx, s.byteQuery)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	if s.outputReservation != nil {
		s.outputReservation.Release()
		s.outputReservation = nil
	}
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.exhausted {
		dst.ClearColumns()
		return false, nil
	}
	if s.probeExhausted {
		if len(s.matchedBuild) != 0 {
			capacity := minInt(4096, len(s.matchedBuild)-s.unmatchedNext)
			var pairs []join.Pair
			if s.reader.owner.shape == joinBytes {
				if err := s.reader.resetBytePairs(ctx, int64(capacity)); err != nil {
					return false, err
				}
				pairs = s.reader.pairs
			} else {
				if err := s.reserveOutput(ctx, int64(capacity), 0); err != nil {
					return false, err
				}
				pairs = make([]join.Pair, 0, capacity)
			}
			for s.unmatchedNext < len(s.matchedBuild) && len(pairs) < cap(pairs) {
				buildRow := s.unmatchedNext
				s.unmatchedNext++
				if !s.matchedBuild[buildRow] {
					pairs = append(pairs, join.Pair{ProbeRow: -1, BuildRow: buildRow})
				}
			}
			if len(pairs) != 0 {
				if s.reader.owner.shape == joinBytes {
					if err := s.reserveByteOutput(ctx, nil, pairs); err != nil {
						return false, err
					}
				}
				if err := materializeJoinBatch(dst, s.reader.owner, nil, pairs); err != nil {
					return false, err
				}
				s.outputRows += int64(len(pairs))
				return true, nil
			}
			if s.unmatchedNext < len(s.matchedBuild) {
				return s.Next(ctx, dst)
			}
		}
		s.exhausted = true
		s.recordStats()
		dst.ClearColumns()
		return false, nil
	}
	ok, err := s.reader.nextPairs(ctx)
	if err != nil {
		if s.outputReservation != nil {
			s.outputReservation.Release()
			s.outputReservation = nil
		}
		return false, err
	}
	if !ok {
		s.probeExhausted = true
		return s.Next(ctx, dst)
	}
	if err := s.reserveOutput(ctx, int64(len(s.reader.pairs)), int64(s.reader.leftBatch.Rows)); err != nil {
		return false, err
	}
	if err := materializeJoinBatch(dst, s.reader.owner, s.reader.leftBatch, s.reader.pairs); err != nil {
		if s.outputReservation != nil {
			s.outputReservation.Release()
			s.outputReservation = nil
		}
		return false, err
	}
	s.probeRows += int64(s.reader.lastSummary.ProbeRows)
	s.probeBytes += memory.BatchBytes(s.reader.leftBatch)
	s.matchedProbeRows += int64(s.reader.lastSummary.MatchedProbeRows)
	s.outputRows += int64(s.reader.lastSummary.OutputRows)
	if len(s.matchedBuild) != 0 {
		for _, pair := range s.reader.pairs {
			if pair.BuildRow < 0 {
				continue
			}
			s.matchedBuild[pair.BuildRow] = true
		}
	}
	return true, nil
}

func (s *residentJoinBatchStream) reserveOutput(ctx context.Context, outputRows, probeRows int64) error {
	if s.reader.owner.shape == joinBytes {
		return s.reserveByteOutput(ctx, s.reader.leftBatch, s.reader.pairs)
	}
	if s.workspace == nil {
		return nil
	}
	width, fixed := fixedJoinSchemaWidth(s.reader.owner.schema)
	if !fixed {
		return nil
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	perOutput := width*2 + 16
	if outputRows < 0 || probeRows < 0 || perOutput <= 0 || outputRows > maxInt64/perOutput {
		return fmt.Errorf("broadcast join workspace byte estimate overflows int64")
	}
	bytes := outputRows * perOutput
	if probeRows > (maxInt64-bytes-1024)/17 {
		return fmt.Errorf("broadcast join probe workspace byte estimate overflows int64")
	}
	bytes += probeRows*17 + 1024
	reservation, err := s.workspace.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	s.outputReservation = reservation
	return nil
}

// Close releases the resident probe reader and is idempotent.
func (s *residentJoinBatchStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.recordStats()
	if s.reader.owner.shape == joinBytes {
		if s.byteOutput != nil {
			s.byteOutput.ClearColumns()
			s.byteOutput = nil
		}
		s.matchedBuild = nil
		s.byteQuery = nil
	}
	if s.outputReservation != nil {
		s.outputReservation.Release()
		s.outputReservation = nil
	}
	if s.matchedReservation != nil {
		s.matchedReservation.Release()
		s.matchedReservation = nil
	}
	closeErr := s.reader.Close()
	// The child may retain the selected partition slice until Close returns.
	// Drop its references before returning our metadata admission.
	if s.reader.owner.shape == joinBytes {
		s.bytePartitions = nil
		if s.bytePartitionMem != nil {
			s.bytePartitionMem.Release()
			s.bytePartitionMem = nil
		}
	}
	return closeErr
}

// StreamDiagnostics reports broadcast build/probe and multiplicity metrics.
func (s *residentJoinBatchStream) StreamDiagnostics() map[string]string {
	return map[string]string{
		"strategy": s.reader.owner.strategy, "build_rows": fmt.Sprint(len(s.reader.owner.buildLoc)),
		"build_bytes": fmt.Sprint(s.reader.owner.buildBytes), "probe_rows": fmt.Sprint(s.probeRows),
		"probe_bytes": fmt.Sprint(s.probeBytes), "matched_probe_rows": fmt.Sprint(s.matchedProbeRows), "output_rows": fmt.Sprint(s.outputRows),
		"max_multiplicity": fmt.Sprint(s.reader.owner.maxOutputMultiplicity()),
	}
}

func (s *residentJoinBatchStream) recordStats() {
	if s == nil || s.statsRecorded {
		return
	}
	s.statsRecorded = true
	if s.feedbackEligible && s.reader.owner.feedback != nil {
		s.reader.owner.feedback(JoinRuntimeFeedback{
			BuildBytes: s.reader.owner.buildBytes, ProbeBytes: s.probeBytes, ProbeRows: s.probeRows,
			OutputRows: s.outputRows, MaxMultiplicity: s.reader.owner.maxOutputMultiplicity(),
		})
	}
	if _, nop := s.reader.owner.observer.(observer.Nop); nop {
		return
	}
	s.reader.owner.observer.Observe(observer.Event{
		Stage: "resident-join", Duration: time.Since(s.started), Rows: s.probeRows,
		Attributes: s.StreamDiagnostics(),
	})
}

type spillableJoinStream struct {
	owner                 *JoinSource
	readOpts              source.ReadOptions
	query                 *memory.QueryBudget
	store                 *spillmeta.Store
	leftSchema            memory.Schema
	rightSchema           memory.Schema
	leftFP                [32]byte
	rightFP               [32]byte
	outputFP              [32]byte
	gather                *memory.OperatorBudget
	encoder               *memory.OperatorBudget
	restorer              *memory.OperatorBudget
	state                 *memory.OperatorBudget
	output                *memory.OperatorBudget
	metadata              memory.Manager
	baseReservation       memory.Reservation
	outputRows            int
	chunkRows             int
	leftCanonical         []uint64
	rightCanonical        []uint64
	dictionaryReservation memory.Reservation
	pairReservation       memory.Reservation

	mu          sync.Mutex
	leftRuns    [joinRadixPartitions]spillmeta.Slice[joinRunRef]
	rightRuns   [joinRadixPartitions]spillmeta.Slice[joinRunRef]
	outputRuns  spillmeta.Slice[joinRunRef]
	nextPart    int
	decoded     *memory.DecodedColumnarRun
	byteOutput  *memory.Batch
	started     time.Time
	prepared    bool
	closed      bool
	exhausted   bool
	terminalErr error

	buildRows       int64
	buildBytes      int64
	probeRows       int64
	probeBytes      int64
	matchedProbe    int64
	outputCount     int64
	spillRuns       int64
	spillBytes      int64
	maxDepth        int
	maxBuildRows    int
	maxMultiplicity int
	statsRecorded   bool
}

func newSpillableJoinStream(ctx context.Context, owner *JoinSource, opts source.ReadOptions) (*spillableJoinStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("spillable join context is nil")
	}
	query := memory.QueryBudgetFromContext(ctx)
	if query == nil || query.Limit() <= 0 {
		return nil, fmt.Errorf("spillable join requires a finite query memory budget")
	}
	if !query.SpillConfigured() {
		return nil, fmt.Errorf("spillable join requires an explicit finite spill quota")
	}
	if owner.shape == joinBytes {
		// Child source allocations must stay in the execution query even when
		// callers supply an unrelated or unlimited compatibility manager.
		opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
	}
	store, err := query.SpillManager()
	if err != nil {
		return nil, err
	}
	rowBytes := pipeline.EstimateSchemaRowBytes(owner.schema)
	outputRows := int(query.Limit() / 8 / maxInt64(1, rowBytes))
	if outputRows < 1 {
		outputRows = 1
	}
	if outputRows > 4096 {
		outputRows = 4096
	}
	inputWidth := pipeline.EstimateSchemaRowBytes(owner.left.Schema())
	if rightWidth := pipeline.EstimateSchemaRowBytes(owner.right.Schema()); rightWidth > inputWidth {
		inputWidth = rightWidth
	}
	chunkRows := int(query.Limit() / 8 / maxInt64(1, inputWidth))
	if chunkRows < 1 {
		chunkRows = 1
	}
	if chunkRows > 4096 {
		chunkRows = 4096
	}
	metadata := spillmeta.GrowthManager(query.Operator("JoinRunMetadata", memory.MemoryRetainedState))
	base, err := metadata.Reserve(ctx, 128+2*int64(unsafe.Sizeof(spillableJoinStream{}))+int64(unsafe.Sizeof(spillmeta.Store{})))
	if err != nil {
		return nil, err
	}
	stream := &spillableJoinStream{
		owner: owner, readOpts: opts, query: query, store: spillmeta.NewStore(store, metadata), baseReservation: base, leftSchema: owner.left.Schema(), rightSchema: owner.right.Schema(),
		leftFP: memory.SchemaFingerprint(owner.left.Schema()), rightFP: memory.SchemaFingerprint(owner.right.Schema()), outputFP: memory.SchemaFingerprint(owner.schema),
		gather: query.Operator("JoinPartitionGather", memory.MemoryWorkspace), encoder: query.Operator("JoinEncode", memory.MemorySpillBuffer),
		restorer: query.Operator("JoinRestore", memory.MemorySpillBuffer), state: query.Operator("JoinBuild", memory.MemoryRetainedState),
		output: query.Operator("JoinOutput", memory.MemoryWorkspace), metadata: metadata,
		outputRows: outputRows, chunkRows: chunkRows,
		started: time.Now(),
	}
	stream.pairReservation, err = stream.output.Reserve(ctx, int64(outputRows)*16)
	if err != nil {
		base.Release()
		return nil, err
	}
	if owner.shape == joinDictionary {
		if err := stream.initDictionaryCanonical(ctx); err != nil {
			stream.pairReservation.Release()
			base.Release()
			return nil, err
		}
	}
	return stream, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *spillableJoinStream) initDictionaryCanonical(ctx context.Context) error {
	leftLen, rightLen := s.owner.leftDict.Len(), s.owner.rightDict.Len()
	estimate := int64(leftLen+rightLen)*40 + int64(len(s.owner.leftDict.Data)+len(s.owner.rightDict.Data))
	reservation, err := s.state.Reserve(ctx, estimate)
	if err != nil {
		return err
	}
	values := make(map[string]uint64, leftLen+rightLen)
	next := uint64(1)
	assign := func(dictionary join.Dictionary) []uint64 {
		out := make([]uint64, dictionary.Len())
		for code := range out {
			value := string(dictionary.Value(uint32(code)))
			canonical, ok := values[value]
			if !ok {
				canonical = next
				next++
				values[value] = canonical
			}
			out[code] = canonical
		}
		return out
	}
	// Assigning right first keeps build codes compact while preserving exact
	// byte equality across different dictionary domains.
	s.rightCanonical = assign(s.owner.rightDict)
	s.leftCanonical = assign(s.owner.leftDict)
	s.dictionaryReservation = reservation
	return nil
}

func (s *spillableJoinStream) canonicalDictionaryKey(left bool, code uint32) (hashagg.Key128, bool, error) {
	codes := s.rightCanonical
	if left {
		codes = s.leftCanonical
	}
	if int(code) >= len(codes) {
		return hashagg.Key128{}, false, fmt.Errorf("join dictionary code %d out of range", code)
	}
	return hashagg.Key128{A: codes[code]}, true, nil
}

func (s *spillableJoinStream) encodedKey(batch *memory.Batch, left bool, row int) (hashagg.Key128, uint8, error) {
	if s.owner.shape == joinDictionary {
		name := s.owner.keys[0].Right
		if left {
			name = s.owner.keys[0].Left
		}
		vector, err := batch.DictionaryString(name)
		if err != nil {
			return hashagg.Key128{}, 0, err
		}
		if !vector.IsValid(row) {
			return hashagg.Key128{}, 0, nil
		}
		key, _, err := s.canonicalDictionaryKey(left, vector.Indices[row])
		return key, 3, err
	}
	return encodePhysicalKey(batch, s.owner.keys, left, row)
}

func (s *spillableJoinStream) spillTracked(ctx context.Context, object memory.SpillObject, rows int, partition int) (joinRunRef, error) {
	handle, err := s.store.Spill(ctx, object)
	if err != nil {
		return joinRunRef{}, err
	}
	s.mu.Lock()
	s.spillRuns++
	s.spillBytes += handle.Bytes()
	s.mu.Unlock()
	s.query.RecordSpilledBytes(handle.Bytes())
	ref := joinRunRef{handle: handle, rows: rows, partition: partition}
	return ref, nil
}

func (s *spillableJoinStream) spillRows(ctx context.Context, schema memory.Schema, fingerprint [32]byte, batch *memory.Batch, rows []int, partition int) (joinRunRef, error) {
	if len(rows) == 0 {
		return joinRunRef{}, nil
	}
	// takeRows uses append-based nested gathers. Reserving twice the complete
	// source morsel is conservative for any subset and covers slice growth
	// before the gather allocates.
	fullBytes := memory.BatchBytes(batch)
	if fullBytes > (math.MaxInt64-1024)/2 {
		return joinRunRef{}, fmt.Errorf("join gather byte estimate overflows int64")
	}
	estimate := fullBytes*2 + 1024
	reservation, err := s.gather.Reserve(ctx, estimate)
	if err != nil {
		return joinRunRef{}, err
	}
	gathered := memory.NewBatch()
	if err := takeRows(ctx, gathered, batch, rows); err != nil {
		reservation.Release()
		return joinRunRef{}, err
	}
	encoded, err := memory.EncodeColumnarRun(ctx, schema, gathered, s.encoder)
	gathered.ReleaseOwned()
	reservation.Release()
	if err != nil {
		return joinRunRef{}, err
	}
	ref, err := s.spillTracked(ctx, memory.SpillObject{Kind: memory.SpillJoinState, Partition: int32(partition), Codec: memory.ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: encoded.Payload}, len(rows), partition)
	_ = encoded.Close()
	if err != nil {
		return joinRunRef{}, err
	}
	return ref, nil
}

func (s *spillableJoinStream) partitionBatch(ctx context.Context, schema memory.Schema, fingerprint [32]byte, batch *memory.Batch, left bool, bitOffset uint, destinations []spillmeta.Slice[joinRunRef]) error {
	rowsReservation, err := s.gather.Reserve(ctx, int64(batch.Rows)*16+joinRadixPartitions*24)
	if err != nil {
		return err
	}
	defer rowsReservation.Release()
	rowsByPartition := make([][]int, joinRadixPartitions)
	for row := 0; row < batch.Rows; row++ {
		if row&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		var hash uint64
		if s.owner.shape == joinBytes {
			var err error
			hash, err = s.owner.bytePartitionHash(batch, left, row)
			if err != nil {
				return err
			}
		} else {
			key, mask, err := s.encodedKey(batch, left, row)
			if err != nil {
				return err
			}
			if mask == 3 {
				// The Auto Hash64Batch candidate was rejected for this spill path:
				// its end-to-end median regressed 3.39% against this scalar hash on
				// darwin/arm64. Keep the partitioner scalar until an architecture-
				// specific gate proves a stable whole-query win.
				hash = hashagg.Hash128(key)
			} else {
				// SQL NULL keys never match. Partition zero preserves deterministic
				// unmatched outer rows without hashing physical garbage values.
				hash = 0
			}
		}
		partition := int(hash >> bitOffset & (joinRadixPartitions - 1))
		rowsByPartition[partition] = append(rowsByPartition[partition], row)
	}
	for partition, rows := range rowsByPartition {
		for offset := 0; offset < len(rows); offset += s.chunkRows {
			end := offset + s.chunkRows
			if end > len(rows) {
				end = len(rows)
			}
			ref, err := s.spillRows(ctx, schema, fingerprint, batch, rows[offset:end], partition)
			if err != nil {
				return err
			}
			if err := destinations[partition].Append(ctx, s.metadata, ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *spillableJoinStream) partitionSource(ctx context.Context, input source.Source, schema memory.Schema, fingerprint [32]byte, left bool, destinations []spillmeta.Slice[joinRunRef]) error {
	stream, err := source.OpenBatchStream(ctx, input, s.readOpts)
	if err != nil {
		return err
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			return errors.Join(nextErr, stream.Close())
		}
		if !ok {
			return stream.Close()
		}
		if err := s.partitionBatch(ctx, schema, fingerprint, batch, left, 0, destinations); err != nil {
			return errors.Join(err, stream.Close())
		}
		if left {
			s.probeRows += int64(batch.Rows)
			s.probeBytes += memory.BatchBytes(batch)
		} else {
			s.buildRows += int64(batch.Rows)
			s.buildBytes += memory.BatchBytes(batch)
		}
	}
}

func (s *spillableJoinStream) restoreRun(ctx context.Context, schema memory.Schema, fingerprint [32]byte, ref joinRunRef) (*memory.DecodedColumnarRun, error) {
	restored, err := s.store.Restore(ctx, ref.handle, fingerprint, s.restorer)
	if err != nil {
		return nil, err
	}
	if restored.Object.Kind != memory.SpillJoinState || restored.Object.Codec != memory.ColumnarRunCodec {
		_ = restored.Close()
		return nil, fmt.Errorf("%w: join run kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	decoded, err := memory.DecodeColumnarRun(ctx, schema, restored, s.restorer)
	_ = restored.Close()
	if err != nil {
		return nil, err
	}
	if decoded.Batch.Rows != ref.rows {
		_ = decoded.Close()
		return nil, fmt.Errorf("%w: join run rows=%d want=%d", memory.ErrSpillCorrupt, decoded.Batch.Rows, ref.rows)
	}
	return decoded, nil
}

type loadedJoinBuild struct {
	decoded     []*memory.DecodedColumnarRun
	batches     []*memory.Batch
	locations   []rowLocation
	table       *join.PairTable
	reservation memory.Reservation
	byteOwner   *JoinSource
}

func (b *loadedJoinBuild) close() {
	if b == nil {
		return
	}
	if b.byteOwner != nil {
		b.byteOwner.bytesTable = nil
		for _, reservation := range b.byteOwner.reservations {
			reservation.Release()
		}
		b.byteOwner.reservations = nil
		b.byteOwner = nil
	}
	for _, decoded := range b.decoded {
		_ = decoded.Close()
	}
	b.decoded, b.batches, b.locations, b.table = nil, nil, nil, nil
	if b.reservation != nil {
		b.reservation.Release()
		b.reservation = nil
	}
}

func (s *spillableJoinStream) loadBuild(ctx context.Context, refs []joinRunRef) (*loadedJoinBuild, error) {
	if s.owner.shape == joinBytes {
		return s.loadByteBuild(ctx, refs)
	}
	total := 0
	for _, ref := range refs {
		if ref.rows > math.MaxInt-total {
			return nil, fmt.Errorf("join build partition rows overflow")
		}
		total += ref.rows
	}
	retained := int64(len(refs))*16 + int64(total)*(16+1+16) + join.EstimatePairTableBytes(maxInt(8, total), total)
	reservation, err := s.state.Reserve(ctx, retained)
	if err != nil {
		return nil, err
	}
	build := &loadedJoinBuild{decoded: make([]*memory.DecodedColumnarRun, 0, len(refs)), batches: make([]*memory.Batch, 0, len(refs)), reservation: reservation, table: join.NewPairTable(maxInt(8, total)), locations: make([]rowLocation, 0, total)}
	keys := make([]hashagg.Key128, 0, total)
	masks := make([]uint8, 0, total)
	for _, ref := range refs {
		decoded, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, ref)
		if err != nil {
			build.close()
			return nil, err
		}
		batchIndex := len(build.batches)
		build.decoded = append(build.decoded, decoded)
		build.batches = append(build.batches, decoded.Batch)
		for row := 0; row < decoded.Batch.Rows; row++ {
			key, mask, err := s.encodedKey(decoded.Batch, false, row)
			if err != nil {
				build.close()
				return nil, err
			}
			keys = append(keys, key)
			masks = append(masks, mask)
			build.locations = append(build.locations, rowLocation{batch: batchIndex, row: row})
		}
	}
	if err := build.table.Build(keys, masks); err != nil {
		build.close()
		return nil, err
	}
	if total > s.maxBuildRows {
		s.maxBuildRows = total
	}
	if multiplicity := build.table.MaxMultiplicity(); multiplicity > s.maxMultiplicity {
		s.maxMultiplicity = multiplicity
	}
	return build, nil
}

func (s *spillableJoinStream) flushPairs(ctx context.Context, build *loadedJoinBuild, left *memory.Batch, pairs []join.Pair, partition int) (joinRunRef, error) {
	if len(pairs) == 0 {
		return joinRunRef{}, nil
	}
	estimate, err := estimateJoinMaterializationBytes(s.ownerWithBuild(build), left, pairs)
	if err != nil {
		return joinRunRef{}, err
	}
	reservation, err := s.output.Reserve(ctx, estimate)
	if err != nil {
		return joinRunRef{}, err
	}
	batch := memory.NewBatch()
	materializer := s.ownerWithBuild(build)
	if err := materializeJoinBatch(batch, materializer, left, pairs); err != nil {
		reservation.Release()
		return joinRunRef{}, err
	}
	actual := memory.BatchBytes(batch)
	if actual > estimate {
		batch.ReleaseOwned()
		reservation.Release()
		return joinRunRef{}, fmt.Errorf("join materialization used %d bytes beyond pre-reserved %d", actual, estimate)
	}
	encoded, err := memory.EncodeColumnarRun(ctx, s.owner.schema, batch, s.encoder)
	batch.ReleaseOwned()
	reservation.Release()
	if err != nil {
		return joinRunRef{}, err
	}
	ref, err := s.spillTracked(ctx, memory.SpillObject{Kind: memory.SpillJoinState, Partition: int32(partition), Codec: memory.ColumnarRunCodec, SchemaFingerprint: s.outputFP, Payload: encoded.Payload}, len(pairs), partition)
	_ = encoded.Close()
	if err != nil {
		return joinRunRef{}, err
	}
	s.mu.Lock()
	s.outputCount += int64(len(pairs))
	s.mu.Unlock()
	return ref, nil
}

func (s *spillableJoinStream) ownerWithBuild(build *loadedJoinBuild) *JoinSource {
	materializer := *s.owner
	materializer.rightBatches = build.batches
	materializer.buildLoc = build.locations
	return &materializer
}

// Reserve scratch before allocating it so a rejected budget cannot cause a
// transient allocation proportional to an input partition.
func newJoinMatchedRows(ctx context.Context, manager memory.Manager, rows int) ([]bool, memory.Reservation, error) {
	if rows < 0 {
		return nil, nil, fmt.Errorf("negative join matched row count")
	}
	reservation, err := manager.Reserve(ctx, int64(rows))
	if err != nil {
		return nil, nil, err
	}
	return make([]bool, rows), reservation, nil
}

func newJoinProbeKeys(ctx context.Context, manager memory.Manager, rows int) ([]hashagg.Key128, []uint8, memory.Reservation, error) {
	if rows < 0 || int64(rows) > math.MaxInt64/17 {
		return nil, nil, nil, fmt.Errorf("join probe scratch byte estimate overflows int64")
	}
	reservation, err := manager.Reserve(ctx, int64(rows)*17)
	if err != nil {
		return nil, nil, nil, err
	}
	return make([]hashagg.Key128, rows), make([]uint8, rows), reservation, nil
}

func (s *spillableJoinStream) probePartition(ctx context.Context, build *loadedJoinBuild, leftRefs []joinRunRef, partition int) (result spillmeta.Slice[joinRunRef], resultErr error) {
	var created spillmeta.Slice[joinRunRef]
	defer func() {
		if resultErr != nil {
			created.Release()
		}
	}()
	matched, matchedReservation, err := newJoinMatchedRows(ctx, s.state, len(build.locations))
	if err != nil {
		return result, err
	}
	defer matchedReservation.Release()
	for _, ref := range leftRefs {
		decoded, err := s.restoreRun(ctx, s.leftSchema, s.leftFP, ref)
		if err != nil {
			s.deleteRefs(context.Background(), created.Values)
			return result, err
		}
		batch := decoded.Batch
		pairs := make([]join.Pair, 0, s.outputRows)
		flush := func() error {
			if len(pairs) == 0 {
				return nil
			}
			if err := s.appendOutputPairs(ctx, build, batch, pairs, partition, &created); err != nil {
				return err
			}
			pairs = pairs[:0]
			return nil
		}
		summary, err := s.probeBuild(ctx, build, batch, func(pair join.Pair) error {
			if pair.BuildRow >= 0 {
				matched[pair.BuildRow] = true
			}
			pairs = append(pairs, pair)
			if len(pairs) == s.outputRows {
				return flush()
			}
			return nil
		})
		if err == nil {
			err = flush()
		}
		s.matchedProbe += int64(summary.MatchedProbeRows)
		_ = decoded.Close()
		if err != nil {
			s.deleteRefs(context.Background(), created.Values)
			return result, err
		}
	}
	if s.owner.typ == join.Right || s.owner.typ == join.Full {
		pairs := make([]join.Pair, 0, s.outputRows)
		for buildRow, wasMatched := range matched {
			if wasMatched {
				continue
			}
			pairs = append(pairs, join.Pair{ProbeRow: -1, BuildRow: buildRow})
			if len(pairs) == s.outputRows {
				if err := s.appendOutputPairs(ctx, build, nil, pairs, partition, &created); err != nil {
					s.deleteRefs(context.Background(), created.Values)
					return result, err
				}
				pairs = pairs[:0]
			}
		}
		if len(pairs) != 0 {
			if err := s.appendOutputPairs(ctx, build, nil, pairs, partition, &created); err != nil {
				s.deleteRefs(context.Background(), created.Values)
				return result, err
			}
		}
	}
	return created, nil
}

func (s *spillableJoinStream) repartitionRefs(ctx context.Context, refs []joinRunRef, schema memory.Schema, fingerprint [32]byte, left bool, depth int, children []spillmeta.Slice[joinRunRef]) error {
	for _, ref := range refs {
		decoded, err := s.restoreRun(ctx, schema, fingerprint, ref)
		if err != nil {
			return err
		}
		err = s.partitionBatch(ctx, schema, fingerprint, decoded.Batch, left, uint(depth)*joinRadixBits, children)
		_ = decoded.Close()
		if err != nil {
			return err
		}
	}
	if err := s.deleteRefsStrict(ctx, refs); err != nil {
		return err
	}
	if depth+1 > s.maxDepth {
		s.maxDepth = depth + 1
	}
	return nil
}

func isJoinPressure(err error) bool { return errors.Is(err, memory.ErrBudgetExceeded) }

func equalJoinKey(a hashagg.Key128, am uint8, b hashagg.Key128, bm uint8) bool {
	return am == 3 && bm == 3 && a == b
}

// skewBuildKey verifies that every non-NULL build row has one identical key.
// Such a partition cannot benefit from further radix bits, but it can be
// joined run-by-run with memory proportional to two input morsels and one
// output morsel.
func (s *spillableJoinStream) skewBuildKey(ctx context.Context, refs []joinRunRef) (hashagg.Key128, bool, error) {
	var key hashagg.Key128
	found := false
	for _, ref := range refs {
		decoded, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, ref)
		if err != nil {
			return hashagg.Key128{}, false, err
		}
		for row := 0; row < decoded.Batch.Rows; row++ {
			candidate, mask, err := s.encodedKey(decoded.Batch, false, row)
			if err != nil {
				_ = decoded.Close()
				return hashagg.Key128{}, false, err
			}
			if mask != 3 {
				continue
			}
			if !found {
				key, found = candidate, true
			} else if candidate != key {
				_ = decoded.Close()
				return hashagg.Key128{}, false, ErrJoinSkewExhausted
			}
		}
		_ = decoded.Close()
	}
	return key, found, nil
}

func (s *spillableJoinStream) appendLeftOnly(ctx context.Context, left *memory.Batch, rows []int, partition int, created *spillmeta.Slice[joinRunRef]) error {
	empty := &loadedJoinBuild{}
	pairs := make([]join.Pair, 0, s.outputRows)
	for _, row := range rows {
		pairs = append(pairs, join.Pair{ProbeRow: row, BuildRow: -1})
		if len(pairs) == s.outputRows {
			if err := s.appendOutputPairs(ctx, empty, left, pairs, partition, created); err != nil {
				return err
			}
			pairs = pairs[:0]
		}
	}
	if len(pairs) != 0 {
		if err := s.appendOutputPairs(ctx, empty, left, pairs, partition, created); err != nil {
			return err
		}
	}
	return nil
}

func (s *spillableJoinStream) appendSkewMatches(ctx context.Context, left *memory.Batch, leftRows []int, right *memory.Batch, key hashagg.Key128, byteKey []byte, partition int, created *spillmeta.Slice[joinRunRef]) (int, error) {
	locationReservation, err := s.state.Reserve(ctx, int64(right.Rows)*16)
	if err != nil {
		return 0, err
	}
	defer locationReservation.Release()
	build := &loadedJoinBuild{batches: []*memory.Batch{right}, locations: make([]rowLocation, 0, right.Rows)}
	for row := 0; row < right.Rows; row++ {
		equal, err := s.skewRowEqual(right, row, key, byteKey)
		if err != nil {
			return 0, err
		}
		if equal {
			build.locations = append(build.locations, rowLocation{batch: 0, row: row})
		}
	}
	if len(build.locations) == 0 || len(leftRows) == 0 {
		return 0, nil
	}
	pairs := make([]join.Pair, 0, s.outputRows)
	outputs := 0
	for _, leftRow := range leftRows {
		for buildRow := range build.locations {
			pairs = append(pairs, join.Pair{ProbeRow: leftRow, BuildRow: buildRow})
			outputs++
			if len(pairs) == s.outputRows {
				if err := s.appendOutputPairs(ctx, build, left, pairs, partition, created); err != nil {
					return outputs, err
				}
				pairs = pairs[:0]
			}
		}
	}
	if len(pairs) != 0 {
		if err := s.appendOutputPairs(ctx, build, left, pairs, partition, created); err != nil {
			return outputs, err
		}
	}
	return outputs, nil
}

func (s *spillableJoinStream) appendSkewUnmatchedRight(ctx context.Context, right *memory.Batch, key hashagg.Key128, byteKey []byte, validKeyMatched bool, partition int, created *spillmeta.Slice[joinRunRef]) error {
	locationReservation, err := s.state.Reserve(ctx, int64(right.Rows)*16)
	if err != nil {
		return err
	}
	defer locationReservation.Release()
	build := &loadedJoinBuild{batches: []*memory.Batch{right}, locations: make([]rowLocation, 0, right.Rows)}
	for row := 0; row < right.Rows; row++ {
		equal, err := s.skewRowEqual(right, row, key, byteKey)
		if err != nil {
			return err
		}
		if !equal || !validKeyMatched {
			build.locations = append(build.locations, rowLocation{batch: 0, row: row})
		}
	}
	pairs := make([]join.Pair, 0, s.outputRows)
	for buildRow := range build.locations {
		pairs = append(pairs, join.Pair{ProbeRow: -1, BuildRow: buildRow})
		if len(pairs) == s.outputRows {
			if err := s.appendOutputPairs(ctx, build, nil, pairs, partition, created); err != nil {
				return err
			}
			pairs = pairs[:0]
		}
	}
	if len(pairs) != 0 {
		if err := s.appendOutputPairs(ctx, build, nil, pairs, partition, created); err != nil {
			return err
		}
	}
	return nil
}

func (s *spillableJoinStream) processSingleKeySkew(ctx context.Context, leftRefs, rightRefs []joinRunRef, partition int) error {
	if s.owner.shape == joinBytes {
		return s.processByteSkew(ctx, leftRefs, rightRefs, partition)
	}
	key, hasBuildKey, err := s.skewBuildKey(ctx, rightRefs)
	if err != nil {
		return err
	}
	var created spillmeta.Slice[joinRunRef]
	defer created.Release()
	validKeyMatched := false
	for _, leftRef := range leftRefs {
		left, err := s.restoreRun(ctx, s.leftSchema, s.leftFP, leftRef)
		if err != nil {
			s.deleteRefs(context.Background(), created.Values)
			return err
		}
		classification, reserveErr := s.state.Reserve(ctx, int64(left.Batch.Rows)*16)
		if reserveErr != nil {
			_ = left.Close()
			s.deleteRefs(context.Background(), created.Values)
			return reserveErr
		}
		matching := make([]int, 0, left.Batch.Rows)
		missing := make([]int, 0, left.Batch.Rows)
		for row := 0; row < left.Batch.Rows; row++ {
			candidate, mask, err := s.encodedKey(left.Batch, true, row)
			if err != nil {
				classification.Release()
				_ = left.Close()
				s.deleteRefs(context.Background(), created.Values)
				return err
			}
			if hasBuildKey && equalJoinKey(candidate, mask, key, 3) {
				matching = append(matching, row)
			} else {
				missing = append(missing, row)
			}
		}
		if len(matching) != 0 {
			validKeyMatched = true
			s.matchedProbe += int64(len(matching))
		}
		switch s.owner.typ {
		case join.Semi:
			err = s.appendLeftOnly(ctx, left.Batch, matching, partition, &created)
		case join.Anti:
			err = s.appendLeftOnly(ctx, left.Batch, missing, partition, &created)
		default:
			for _, rightRef := range rightRefs {
				right, restoreErr := s.restoreRun(ctx, s.rightSchema, s.rightFP, rightRef)
				if restoreErr != nil {
					err = restoreErr
					break
				}
				_, err = s.appendSkewMatches(ctx, left.Batch, matching, right.Batch, key, nil, partition, &created)
				_ = right.Close()
				if err != nil {
					break
				}
			}
			if err == nil && (s.owner.typ == join.Left || s.owner.typ == join.Full) {
				err = s.appendLeftOnly(ctx, left.Batch, missing, partition, &created)
			}
		}
		classification.Release()
		_ = left.Close()
		if err != nil {
			s.deleteRefs(context.Background(), created.Values)
			return err
		}
	}
	if s.owner.typ == join.Right || s.owner.typ == join.Full {
		for _, rightRef := range rightRefs {
			right, err := s.restoreRun(ctx, s.rightSchema, s.rightFP, rightRef)
			if err != nil {
				s.deleteRefs(context.Background(), created.Values)
				return err
			}
			err = s.appendSkewUnmatchedRight(ctx, right.Batch, key, nil, validKeyMatched, partition, &created)
			_ = right.Close()
			if err != nil {
				s.deleteRefs(context.Background(), created.Values)
				return err
			}
		}
	}
	if err := errors.Join(s.deleteRefsStrict(ctx, leftRefs), s.deleteRefsStrict(ctx, rightRefs)); err != nil {
		s.deleteRefs(context.Background(), created.Values)
		return err
	}
	if err := s.outputRuns.Append(ctx, s.metadata, created.Values...); err != nil {
		return err
	}
	return nil
}

func (s *spillableJoinStream) processRefs(ctx context.Context, leftRefs, rightRefs []joinRunRef, depth, partition int) error {
	build, err := s.loadBuild(ctx, rightRefs)
	if err == nil {
		var output spillmeta.Slice[joinRunRef]
		output, err = s.probePartition(ctx, build, leftRefs, partition)
		build.close()
		defer output.Release()
		if err == nil {
			if appendErr := s.outputRuns.Append(ctx, s.metadata, output.Values...); appendErr != nil {
				return appendErr
			}
			if deleteErr := errors.Join(s.deleteRefsStrict(ctx, leftRefs), s.deleteRefsStrict(ctx, rightRefs)); deleteErr != nil {
				return deleteErr
			}
			return nil
		}
		s.deleteRefs(context.Background(), output.Values)
		return err // Repartitioning cannot relieve output or metadata admission failures.
	}
	if !isJoinPressure(err) {
		return err
	}
	if s.owner.shape == joinBytes {
		if skewErr := s.processByteSkew(ctx, leftRefs, rightRefs, partition); skewErr == nil {
			return nil
		} else if !errors.Is(skewErr, ErrJoinSkewExhausted) {
			return skewErr
		}
	}
	if depth >= maxJoinRadixDepth {
		if skewErr := s.processSingleKeySkew(ctx, leftRefs, rightRefs, partition); skewErr == nil {
			return nil
		} else if !errors.Is(skewErr, ErrJoinSkewExhausted) {
			return skewErr
		}
		return fmt.Errorf("%w at partition=%d depth=%d: %v", ErrJoinSkewExhausted, partition, depth, err)
	}
	reservation, reserveErr := s.metadata.Reserve(ctx, 128+int64(4*joinRadixPartitions)*int64(unsafe.Sizeof(spillmeta.Slice[joinRunRef]{})))
	if reserveErr != nil {
		return reserveErr
	}
	var leftChildren, rightChildren [joinRadixPartitions]spillmeta.Slice[joinRunRef]
	defer func() {
		for i := range leftChildren {
			leftChildren[i].Release()
			rightChildren[i].Release()
		}
		reservation.Release()
	}()
	if childErr := s.repartitionRefs(ctx, leftRefs, s.leftSchema, s.leftFP, true, depth, leftChildren[:]); childErr != nil {
		return errors.Join(err, childErr)
	}
	if childErr := s.repartitionRefs(ctx, rightRefs, s.rightSchema, s.rightFP, false, depth, rightChildren[:]); childErr != nil {
		return errors.Join(err, childErr)
	}
	for child := 0; child < joinRadixPartitions; child++ {
		if len(leftChildren[child].Values) == 0 && len(rightChildren[child].Values) == 0 {
			continue
		}
		if err := s.processRefs(ctx, leftChildren[child].Values, rightChildren[child].Values, depth+1, child); err != nil {
			return err
		}
		leftChildren[child].Release()
		rightChildren[child].Release()
	}
	return nil
}

func (s *spillableJoinStream) prepare(ctx context.Context) error {
	if err := s.partitionSource(ctx, s.owner.right, s.rightSchema, s.rightFP, false, s.rightRuns[:]); err != nil {
		return err
	}
	if err := s.partitionSource(ctx, s.owner.left, s.leftSchema, s.leftFP, true, s.leftRuns[:]); err != nil {
		return err
	}
	s.maxDepth = 1
	s.prepared = true
	return nil
}

func (s *spillableJoinStream) closeDecoded() {
	if s.byteOutput != nil {
		s.byteOutput.ClearColumns()
		s.byteOutput = nil
	}
	if s.decoded != nil {
		_ = s.decoded.Close()
		s.decoded = nil
	}
}

func bindDecodedBatch(dst *memory.Batch, decoded *memory.Batch) {
	dst.ClearColumns()
	// Borrow every vector map, including native narrow numeric columns. The
	// stream retains the backing run until its next Next call or Close; copying
	// the complete header keeps newly added Batch types in this binding path.
	*dst = *decoded
}

// Next emits one bounded output run. Inputs are radix-partitioned on first
// demand; subsequent demand processes partitions lazily and therefore never
// retains the complete build, probe, or amplified output in memory.
func (s *spillableJoinStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.owner.shape == joinBytes {
		if s.terminalErr != nil {
			if dst != nil {
				dst.ClearColumns()
			}
			return false, s.terminalErr
		}
		if s.closed {
			if dst != nil {
				dst.ClearColumns()
			}
			return false, source.ErrBatchStreamClosed
		}
		if ctx == nil {
			return false, s.abort(fmt.Errorf("generic spill join context is nil"))
		}
		if dst == nil {
			return false, s.abort(fmt.Errorf("generic spill join destination is nil"))
		}
		if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.query {
			return false, s.abort(fmt.Errorf("generic spill join cannot change query ownership"))
		} else if query == nil {
			ctx = memory.WithQueryBudget(ctx, s.query)
		}
	}
	if dst == nil {
		return false, fmt.Errorf("join stream destination is nil")
	}
	// The previous decoded run is borrowed only until this call. Unbind the
	// caller's view before releasing its backing reservation.
	dst.ClearColumns()
	s.closeDecoded()
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		dst.ClearColumns()
		return false, s.terminalErr
	}
	if s.exhausted {
		dst.ClearColumns()
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
	for len(s.outputRuns.Values) == 0 && s.nextPart < joinRadixPartitions {
		partition := s.nextPart
		s.nextPart++
		leftRefs, rightRefs := s.leftRuns[partition], s.rightRuns[partition]
		s.leftRuns[partition], s.rightRuns[partition] = spillmeta.Slice[joinRunRef]{}, spillmeta.Slice[joinRunRef]{}
		err := s.processRefs(ctx, leftRefs.Values, rightRefs.Values, 1, partition)
		leftRefs.Release()
		rightRefs.Release()
		if err != nil {
			return false, s.abort(err)
		}
	}
	if len(s.outputRuns.Values) == 0 {
		s.exhausted = true
		s.recordStats()
		dst.ClearColumns()
		return false, s.cleanup()
	}
	ref := s.outputRuns.Values[0]
	s.outputRuns.Values[0] = joinRunRef{}
	s.outputRuns.Values = s.outputRuns.Values[1:]
	if len(s.outputRuns.Values) == 0 {
		s.outputRuns.Release()
	}
	decoded, err := s.restoreRun(ctx, s.owner.schema, s.outputFP, ref)
	if err != nil {
		return false, s.abort(err)
	}
	if err := s.deleteRef(ctx, ref); err != nil {
		_ = decoded.Close()
		return false, s.abort(err)
	}
	s.decoded = decoded
	bindDecodedBatch(dst, decoded.Batch)
	if s.owner.shape == joinBytes {
		s.byteOutput = dst
	}
	return true, nil
}

func (s *spillableJoinStream) deleteRefs(ctx context.Context, refs []joinRunRef) {
	for _, ref := range refs {
		_ = s.deleteRef(ctx, ref)
	}
}

func (s *spillableJoinStream) deleteRef(ctx context.Context, ref joinRunRef) error {
	if ref.handle == nil {
		return nil
	}
	return s.store.Delete(ctx, ref.handle)
}

func (s *spillableJoinStream) deleteRefsStrict(ctx context.Context, refs []joinRunRef) error {
	var out error
	for _, ref := range refs {
		if err := s.deleteRef(ctx, ref); err != nil && out == nil {
			out = err
		}
	}
	return out
}

func (s *spillableJoinStream) cleanup() error {
	s.closeDecoded()
	for i := range s.leftRuns {
		s.leftRuns[i].Release()
		s.rightRuns[i].Release()
	}
	s.outputRuns.Release()
	s.leftCanonical, s.rightCanonical = nil, nil
	if s.dictionaryReservation != nil {
		s.dictionaryReservation.Release()
		s.dictionaryReservation = nil
	}
	if s.pairReservation != nil {
		s.pairReservation.Release()
		s.pairReservation = nil
	}
	err := s.store.Close()
	if s.baseReservation != nil {
		s.baseReservation.Release()
		s.baseReservation = nil
	}
	return err
}

func (s *spillableJoinStream) abort(err error) error {
	if s.terminalErr == nil {
		s.terminalErr = errors.Join(err, s.cleanup())
	}
	return s.terminalErr
}

// Close releases restored buffers and every still-owned input/output run.
func (s *spillableJoinStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.recordStats()
	return s.cleanup()
}

// StreamDiagnostics returns cold-path spill, skew, and amplification metrics.
func (s *spillableJoinStream) StreamDiagnostics() map[string]string {
	snapshot := s.query.Snapshot()
	return map[string]string{
		"strategy": s.owner.strategy, "build_rows": fmt.Sprint(s.buildRows), "build_bytes": fmt.Sprint(s.buildBytes),
		"probe_rows": fmt.Sprint(s.probeRows), "probe_bytes": fmt.Sprint(s.probeBytes),
		"matched_probe_rows": fmt.Sprint(s.matchedProbe), "output_rows": fmt.Sprint(s.outputCount),
		"spill_runs": fmt.Sprint(s.spillRuns), "spill_bytes": fmt.Sprint(s.spillBytes),
		"peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(snapshot.PeakBytes), "max_radix_depth": fmt.Sprint(s.maxDepth),
		"max_partition_build_rows": fmt.Sprint(s.maxBuildRows), "max_multiplicity": fmt.Sprint(s.maxMultiplicity),
	}
}

func (s *spillableJoinStream) recordStats() {
	if s.statsRecorded {
		return
	}
	s.statsRecorded = true
	if s.owner.feedback != nil {
		s.owner.feedback(JoinRuntimeFeedback{
			BuildBytes: s.buildBytes, ProbeBytes: s.probeBytes, ProbeRows: s.probeRows, OutputRows: s.outputCount,
			SpillBytes: s.spillBytes, MaxMultiplicity: s.maxMultiplicity,
		})
	}
	if _, nop := s.owner.observer.(observer.Nop); nop {
		return
	}
	s.owner.observer.Observe(observer.Event{
		Stage: "spillable-join", Duration: time.Since(s.started), Rows: s.probeRows, Bytes: s.spillBytes,
		Attributes: s.StreamDiagnostics(),
	})
}

var _ source.BatchStreamProvider = (*JoinSource)(nil)
var _ source.PreparedBatchStreamProvider = (*JoinSource)(nil)
var _ source.BatchStreamDiagnostics = (*spillableJoinStream)(nil)
var _ source.BatchStreamDiagnostics = (*residentJoinBatchStream)(nil)
