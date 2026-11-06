package engine

import (
	"context"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"math/bits"
	"unsafe"
)

// residentPartitionedState gives every group exactly one state owner. Input
// scratch is limited to one native scan batch; no partition or total-row count
// determines a buffer size. Workers only share immutable borrowed input vectors.
type residentPartitionedState struct {
	wave                               *residentAggregateWave
	workerCounts, workerPositions      []int
	scatterKeyFlags, scatterValueFlags []uint8
	parallelLease                      memory.Reservation
	parallelClassified                 bool
	query                              *PreparedQuery
	budget                             *memory.QueryBudget
	stateBudget                        *memory.OperatorBudget
	initialCardinality                 int
	workspace                          *memory.OperatorBudget
	owners                             []*aggregateTableOwner
	counts, offsets, positions         []int
	checksums                          []int64
	partitionIDs                       []uint16
	rowIndices                         []int
	scratchRows                        int
	packedI32                          []int32
	packedU32                          []uint32
	packedI64                          []int64
	packedValues                       []int64
	packedKeyValidity                  memory.Bitmap
	packedValueValidity                memory.Bitmap
	allSelected                        memory.Bitmap
	keyNulls, valueNulls               bool
	metadataLease                      memory.Reservation
	scratchLease                       memory.Reservation
	workers                            int
	shift                              uint
	outputPartition                    int
	outputCursor                       int
}

func newResidentTableOwner(ctx context.Context, query *PreparedQuery, state *memory.OperatorBudget, cardinality int) (*aggregateTableOwner, error) {
	table, err := newAggregateStateTable(query.cq.GroupShape, query.cq.AggregateLayout, cardinality, query.cq.AverageKeyBytes, 0, func(bytes int64) (memory.Reservation, error) {
		return state.ReserveGrowth(ctx, bytes)
	})
	if err != nil {
		return nil, err
	}
	return &aggregateTableOwner{table: table}, nil
}

func newResidentPartitionedState(ctx context.Context, query *PreparedQuery, budget *memory.QueryBudget) (_ *residentPartitionedState, err error) {
	workers := max(1, query.strategy.Workers)
	partitions := 1
	for partitions < workers {
		partitions *= 2
	}
	// Approximately cache-sized tables, with enough independent partitions to
	// distribute skew. Small hints and finite budgets avoid hundreds of tiny tables.
	for partitions < 256 && partitions < query.cq.CardinalityHint/64 {
		partitions *= 2
	}
	if budget.Limit() > 0 {
		for partitions/2 >= workers && int64(partitions)*1024 > budget.Limit()/4 {
			partitions /= 2
		}
	}
	if partitions > 1<<16 {
		return nil, fmt.Errorf("partitioned aggregate supports at most 65536 state partitions")
	}
	state := &residentPartitionedState{query: query, budget: budget, workers: workers, shift: uint(64 - bits.TrailingZeros(uint(partitions))), workspace: budget.Operator("ResidentAggregatePartitionWorkspace", memory.MemoryWorkspace)}
	defer func() {
		if err != nil {
			state.close()
		}
	}()
	metadataBytes := int64(partitions)*(int64(unsafe.Sizeof((*aggregateTableOwner)(nil)))+3*int64(unsafe.Sizeof(int(0)))+int64(unsafe.Sizeof(int64(0)))) + int64(unsafe.Sizeof(int(0)))
	state.metadataLease, err = state.workspace.ReserveGrowth(ctx, metadataBytes)
	if err != nil {
		return nil, err
	}
	state.owners = make([]*aggregateTableOwner, partitions)
	state.counts = make([]int, partitions)
	state.offsets = make([]int, partitions+1)
	state.positions = make([]int, partitions)
	state.checksums = make([]int64, partitions)
	operator := budget.Operator("ResidentPartitionedHashAggregate", memory.MemoryRetainedState)
	cardinality := max(8, (query.cq.CardinalityHint+partitions-1)/partitions)
	// Start within a finite budget, letting accurate runtime growth replace an
	// optimistic cardinality hint. Actual reservations still enforce the hard cap.
	if budget.Limit() > 0 {
		bytesPerGroup := int64(128 + query.cq.AverageKeyBytes)
		capacity := budget.Limit() / 2 / int64(partitions) / bytesPerGroup
		if capacity < int64(cardinality) {
			cardinality = max(8, int(capacity))
		}
	}
	// Empty shards own no table. A large hint must not preallocate hundreds
	// of minimum tables when the actual input has only a few groups.
	state.stateBudget = operator
	state.initialCardinality = cardinality

	return state, nil
}

func (s *residentPartitionedState) packed() bool {
	return s.query.cq.ValueColumn != "" && s.query.cq.GroupShape != GroupShapePairFixed && s.query.cq.GroupShape != GroupShapeStringView
}

func (s *residentPartitionedState) ensureScratch(ctx context.Context, rows int) error {
	if rows <= s.scratchRows {
		return nil
	}
	maxInt := int(^uint(0) >> 1)
	if rows < 0 || rows > maxInt-len(s.owners)*63-63 || int64(rows+len(s.owners)*63) > int64(^uint64(0)>>1)/32 {
		return fmt.Errorf("aggregate partition scratch exceeds addressable memory")
	}
	packedRows := rows + len(s.owners)*63
	bytes := int64(rows) * 2
	if s.packed() {
		keyWidth := int64(4)
		if s.query.cq.GroupShape == GroupShapeI64 {
			keyWidth = 8
		}
		if int64(packedRows) > int64(^uint64(0)>>1)/(keyWidth+10) {
			return fmt.Errorf("aggregate partition scratch exceeds addressable memory")
		}
		bytes += int64(packedRows)*(keyWidth+8) + int64(memory.WordsFor(packedRows))*16 + int64(memory.WordsFor(min(4096, packedRows)))*8
	} else {
		bytes += int64(rows) * int64(unsafe.Sizeof(int(0)))
	}
	lease, err := s.workspace.ReserveGrowth(ctx, bytes)
	if err != nil {
		return fmt.Errorf("reserve aggregate partition scratch: %w", err)
	}
	ids := make([]uint16, rows)
	if s.packed() {
		switch s.query.cq.GroupShape {
		case GroupShapeI32:
			s.packedI32 = make([]int32, packedRows)
		case GroupShapeI64:
			s.packedI64 = make([]int64, packedRows)
		default:
			s.packedU32 = make([]uint32, packedRows)
		}
		s.packedValues = make([]int64, packedRows)
		s.packedKeyValidity = make(memory.Bitmap, memory.WordsFor(packedRows))
		s.packedValueValidity = make(memory.Bitmap, memory.WordsFor(packedRows))
		s.allSelected = make(memory.Bitmap, memory.WordsFor(min(4096, packedRows)))
		for i := range s.allSelected {
			s.allSelected[i] = ^uint64(0)
		}
	} else {
		s.rowIndices = make([]int, rows)
	}
	s.partitionIDs, s.scratchRows = ids, rows
	if s.scratchLease != nil {
		s.scratchLease.Release()
	}
	s.scratchLease = lease
	return nil
}

func (s *residentPartitionedState) classify(ctx context.Context, binding aggregateBatchBinding, selection memory.Bitmap, rows int) (int, error) {
	clear(s.counts)
	s.parallelClassified = false
	if len(selection) == 0 {
		return 0, nil
	}
	if s.packed() && s.ensureParallelClassification(ctx, rows) {
		return s.classifyParallel(ctx, binding, selection, rows)
	}
	selected := 0
	var err error
	switch s.query.cq.GroupShape {
	case GroupShapeI32:
		selected, err = classifyFixedKeys(ctx, binding.first.i32, binding.first.validity, selection, rows, s.shift, s.partitionIDs, s.counts)
	case GroupShapeI64:
		selected, err = classifyFixedKeys(ctx, binding.first.i64, binding.first.validity, selection, rows, s.shift, s.partitionIDs, s.counts)
	case GroupShapeU32, GroupShapeDictionaryString:
		selected, err = classifyFixedKeys(ctx, binding.first.u32, binding.first.validity, selection, rows, s.shift, s.partitionIDs, s.counts)
	default:
		err = forSelectedFrom(selection, rows, 0, func(row int) error {
			if selected&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			hash := aggregateRecordHash(s.query.cq.GroupShape, binding.record(s.query.cq.GroupShape, row))
			partition := int(hash >> s.shift)
			s.partitionIDs[row] = uint16(partition)
			s.counts[partition]++
			selected++
			return nil
		})
	}
	if err != nil {
		return 0, err
	}
	for partition, count := range s.counts {
		next := s.offsets[partition] + count
		if s.packed() {
			next = (next + 63) &^ 63
		}
		s.offsets[partition+1] = next
		s.positions[partition] = s.offsets[partition]
	}
	if s.packed() {
		s.keyNulls = memory.HasNulls(binding.first.validity, rows)
		s.valueNulls = memory.HasNulls(binding.values.Validity, rows)
		if s.keyNulls {
			clear(s.packedKeyValidity)
		}
		if s.valueNulls {
			clear(s.packedValueValidity)
		}
		switch s.query.cq.GroupShape {
		case GroupShapeI32:
			err = scatterFixedKeys(ctx, s, binding.first.i32, s.packedI32, binding, selection, rows)
		case GroupShapeI64:
			err = scatterFixedKeys(ctx, s, binding.first.i64, s.packedI64, binding, selection, rows)
		default:
			err = scatterFixedKeys(ctx, s, binding.first.u32, s.packedU32, binding, selection, rows)
		}
		return selected, err
	}
	for wi, word := range selection {
		if wi&15 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		for word != 0 {
			row := wi*64 + bits.TrailingZeros64(word)
			if row >= rows {
				break
			}
			partition := int(s.partitionIDs[row])
			s.rowIndices[s.positions[partition]] = row
			s.positions[partition]++
			word &= word - 1
		}
	}
	return selected, nil
}

// classifyFixedKeys hashes only the grouping column. The key-width branch is
// constant for each instantiation; no aggregate record or value is constructed.
// High bits choose ownership and low bits remain dispersed within each table.
func classifyFixedKeys[T int32 | uint32 | int64](ctx context.Context, keys []T, validity, selection memory.Bitmap, rows int, shift uint, ids []uint16, counts []int) (int, error) {
	selected := 0
	nullHash := hashagg.Hash64(0x9e3779b97f4a7c15)
	for wi, word := range selection {
		if wi&15 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		for word != 0 {
			row := wi*64 + bits.TrailingZeros64(word)
			if row >= rows {
				break
			}
			key := keys[row]
			u := uint64(key)
			if unsafe.Sizeof(key) == 4 {
				u = uint64(uint32(key))
			}
			hash := hashagg.Hash64(u)
			if validity != nil && !memory.IsSet(validity, row) {
				hash = nullHash
			}
			partition := int(hash >> shift)
			ids[row] = uint16(partition)
			counts[partition]++
			selected++
			word &= word - 1
		}
	}
	return selected, nil
}

// scatterFixedKeys packs only one batch into cache-local shard slices. Shards
// begin on bitmap word boundaries, so workers can reuse the ordinary nullable
// aggregation kernel without offset views or shared mutable validity words.
func scatterFixedKeys[T int32 | uint32 | int64](ctx context.Context, s *residentPartitionedState, keys, packed []T, binding aggregateBatchBinding, selection memory.Bitmap, rows int) error {
	for wi, word := range selection {
		if wi&15 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		for word != 0 {
			row := wi*64 + bits.TrailingZeros64(word)
			if row >= rows {
				break
			}
			partition := int(s.partitionIDs[row])
			position := s.positions[partition]
			s.positions[partition]++
			packed[position], s.packedValues[position] = keys[row], binding.values.Values[row]
			if s.keyNulls && memory.IsSet(binding.first.validity, row) {
				memory.Set(s.packedKeyValidity, position)
			}
			if s.valueNulls && memory.IsSet(binding.values.Validity, row) {
				memory.Set(s.packedValueValidity, position)
			}
			word &= word - 1
		}
	}
	return nil
}

func (s *residentPartitionedState) aggregatePacked(ctx context.Context, table *aggregateStateTable, partition int) (int64, error) {
	if s.parallelClassified && (s.keyNulls || s.valueNulls) {
		s.packParallelValidity(partition)
	}
	checksum := int64(0)
	end := s.offsets[partition] + s.counts[partition]
	for start := s.offsets[partition]; start < end; {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		next := min(start+4096, end)
		var keyValidity, valueValidity memory.Bitmap
		if s.keyNulls {
			keyValidity = s.packedKeyValidity[start/64 : memory.WordsFor(next)]
		}
		if s.valueNulls {
			valueValidity = s.packedValueValidity[start/64 : memory.WordsFor(next)]
		}
		selection := s.allSelected[:memory.WordsFor(next-start)]
		var summary hashagg.Summary
		var err error
		switch table.shape {
		case GroupShapeI32:
			summary, err = table.i32.AggregateSelectedNullableValues(s.packedI32[start:next], keyValidity, s.packedValues[start:next], valueValidity, selection)
		case GroupShapeI64:
			summary, err = table.u64.AggregateSelectedNullableValues(s.packedI64[start:next], keyValidity, s.packedValues[start:next], valueValidity, selection)
		default:
			summary, err = table.u32.AggregateSelectedNullableValues(s.packedU32[start:next], keyValidity, s.packedValues[start:next], valueValidity, selection)
		}
		if err != nil {
			return 0, err
		}
		checksum += summary.Checksum
		start = next
	}
	return checksum, nil
}

func (s *residentPartitionedState) aggregateCurrentBatch(ctx context.Context, binding aggregateBatchBinding, selection memory.Bitmap, rows int) (int, int64, error) {
	if err := s.ensureScratch(ctx, rows); err != nil {
		return 0, 0, err
	}
	selected, err := s.classify(ctx, binding, selection, rows)
	if err != nil {
		return 0, 0, err
	}
	clear(s.checksums)
	// One task per worker, not one scheduler job per tiny state partition.
	// ParallelFor joins every submitted task before the borrowed batch advances.
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	err = s.query.e.scheduler.ParallelFor(workCtx, s.workers, s.workers, func(worker int) error {
		for partition := worker; partition < len(s.owners); partition += s.workers {
			if err := workCtx.Err(); err != nil {
				return err
			}
			if s.counts[partition] == 0 {
				continue
			}
			if s.owners[partition] == nil {
				owner, err := newResidentTableOwner(ctx, s.query, s.stateBudget, s.initialCardinality)
				if err != nil {
					cancel(err)
					return err
				}
				s.owners[partition] = owner
			}
			var checksum int64
			var err error
			if s.packed() {
				checksum, err = s.aggregatePacked(workCtx, s.owners[partition].table, partition)
			} else {
				indices := s.rowIndices[s.offsets[partition] : s.offsets[partition]+s.counts[partition]]
				checksum, err = aggregateIndexedBatch(workCtx, s.owners[partition].table, binding, indices)
			}
			if err != nil {
				cancel(err)
				return err
			}
			s.checksums[partition] = checksum
		}
		return nil
	})
	if err != nil {
		if cause := context.Cause(workCtx); cause != nil {
			return 0, 0, cause
		}
		return 0, 0, err
	}
	checksum := int64(0)
	for _, value := range s.checksums {
		checksum += value
	}
	return selected, checksum, nil
}

// aggregateIndexedBatch updates the same typed state as the scalar native
// operator. Index classification changes ownership and locality, not semantics.
func aggregateIndexedBatch(ctx context.Context, table *aggregateStateTable, binding aggregateBatchBinding, indices []int) (int64, error) {
	checksum := int64(0)
	// Share the same optimized typed kernel with bitmap aggregation. Bound
	// each call so cancellation is checked even when a skewed shard is huge.
	if binding.values != nil && table.shape != GroupShapePairFixed && table.shape != GroupShapeStringView {
		for len(indices) > 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			n := min(4096, len(indices))
			chunk := indices[:n]
			var summary hashagg.Summary
			var err error
			values := binding.values
			switch table.shape {
			case GroupShapeI32:
				summary, err = table.i32.AggregateIndexedNullableValues(binding.first.i32, binding.first.validity, values.Values, values.Validity, chunk)
			case GroupShapeI64:
				summary, err = table.u64.AggregateIndexedNullableValues(binding.first.i64, binding.first.validity, values.Values, values.Validity, chunk)
			case GroupShapeU32, GroupShapeDictionaryString:
				summary, err = table.u32.AggregateIndexedNullableValues(binding.first.u32, binding.first.validity, values.Values, values.Validity, chunk)
			}
			if err != nil {
				return 0, err
			}
			checksum += summary.Checksum
			indices = indices[n:]
		}
		return checksum, nil
	}
	for index, row := range indices {
		if index&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		value, valid := binding.value(row)
		_, err := table.add(binding.record(table.shape, row), value, valid)
		if err != nil {
			return 0, err
		}
		if valid {
			checksum += value
		}
	}
	return checksum, nil
}

func (s *residentPartitionedState) len() int {
	groups := 0
	for _, owner := range s.owners {
		if owner != nil {
			groups += owner.table.len()
		}
	}
	return groups
}
func (s *residentPartitionedState) visit(fn func(aggregateRecord) error) error {
	for _, owner := range s.owners {
		if owner == nil {
			continue
		}
		if err := owner.table.visit(fn); err != nil {
			return err
		}
	}
	return nil
}
func (s *residentPartitionedState) visitChunk(limit int, fn func(aggregateRecord) error) (bool, error) {
	emitted := 0
	for s.outputPartition < len(s.owners) && emitted < limit {
		owner := s.owners[s.outputPartition]
		if owner == nil {
			s.outputPartition++
			continue
		}
		done, err := owner.table.visitChunk(&s.outputCursor, limit-emitted, func(record aggregateRecord) error { emitted++; return fn(record) })
		if err != nil {
			return false, err
		}
		if !done {
			return false, nil
		}
		s.outputPartition++
		s.outputCursor = 0
	}
	return s.outputPartition == len(s.owners), nil
}
func (s *residentPartitionedState) close() {
	if s == nil {
		return
	}
	s.wave.close()
	s.wave = nil
	s.releaseParallelClassification()
	for _, owner := range s.owners {
		if owner != nil {
			owner.close()
		}
	}
	s.owners = nil
	s.counts, s.offsets, s.positions, s.rowIndices = nil, nil, nil, nil
	s.checksums, s.partitionIDs = nil, nil
	s.packedI32, s.packedU32, s.packedI64, s.packedValues = nil, nil, nil, nil
	s.packedKeyValidity, s.packedValueValidity, s.allSelected = nil, nil, nil
	s.scratchRows = 0
	if s.scratchLease != nil {
		s.scratchLease.Release()
		s.scratchLease = nil
	}
	if s.metadataLease != nil {
		s.metadataLease.Release()
		s.metadataLease = nil
	}
}
