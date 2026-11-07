package engine

import (
	"context"
	"gocolumn/memory"
	"math/bits"
	"unsafe"
)

// Parallel classification owns worker-local histograms and output positions.
// Byte validity flags permit disjoint scatter slots even when workers meet
// inside one bitmap word. The shard owner packs those flags after the join.
func (s *residentPartitionedState) ensureParallelClassification(ctx context.Context, rows int) bool {
	if s.workers < 2 || rows < 4096 {
		return false
	}
	if len(s.scatterKeyFlags) >= len(s.packedValues) {
		return true
	}
	entries := s.workers * len(s.owners)
	bytes := int64(entries)*2*int64(unsafe.Sizeof(int(0))) + int64(len(s.packedValues))*2
	lease, err := s.workspace.ReserveGrowth(ctx, bytes)
	if err != nil {
		s.releaseParallelClassification()
		return false // Optional parallel scratch must not reject a serial-fit query.
	}
	counts := make([]int, entries)
	positions := make([]int, entries)
	keyFlags := make([]uint8, len(s.packedValues))
	valueFlags := make([]uint8, len(s.packedValues))
	s.workerCounts, s.workerPositions = counts, positions
	s.scatterKeyFlags, s.scatterValueFlags = keyFlags, valueFlags
	if s.parallelLease != nil {
		s.parallelLease.Release()
	}
	s.parallelLease = lease
	return true
}

func (s *residentPartitionedState) releaseParallelClassification() {
	s.workerCounts, s.workerPositions = nil, nil
	s.scatterKeyFlags, s.scatterValueFlags = nil, nil
	if s.parallelLease != nil {
		s.parallelLease.Release()
		s.parallelLease = nil
	}
}

func (s *residentPartitionedState) classifyParallel(ctx context.Context, binding aggregateBatchBinding, selection memory.Bitmap, rows int) (int, error) {
	clear(s.workerCounts)
	parts := len(s.owners)
	wordsPerWorker := (memory.WordsFor(rows) + s.workers - 1) / s.workers
	err := s.query.e.scheduler.ParallelFor(ctx, s.workers, s.workers, func(worker int) error {
		lo := worker * wordsPerWorker * 64
		if lo >= rows {
			return nil
		}
		hi := min(rows, lo+wordsPerWorker*64)
		var validity memory.Bitmap
		if binding.first.validity != nil {
			validity = binding.first.validity[lo/64 : memory.WordsFor(hi)]
		}
		selected := selection[lo/64 : memory.WordsFor(hi)]
		counts := s.workerCounts[worker*parts : (worker+1)*parts]
		var err error
		switch s.query.cq.GroupShape {
		case GroupShapeI32:
			_, err = classifyFixedKeys(ctx, binding.first.i32[lo:hi], validity, selected, hi-lo, s.shift, s.partitionIDs[lo:hi], counts)
		case GroupShapeI64:
			_, err = classifyFixedKeys(ctx, binding.first.i64[lo:hi], validity, selected, hi-lo, s.shift, s.partitionIDs[lo:hi], counts)
		default:
			_, err = classifyFixedKeys(ctx, binding.first.u32[lo:hi], validity, selected, hi-lo, s.shift, s.partitionIDs[lo:hi], counts)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	selected := 0
	for partition := range s.owners {
		count := 0
		for worker := 0; worker < s.workers; worker++ {
			index := worker*parts + partition
			s.workerPositions[index] = s.offsets[partition] + count
			count += s.workerCounts[index]
		}
		s.counts[partition] = count
		selected += count
		s.offsets[partition+1] = (s.offsets[partition] + count + 63) &^ 63
	}
	s.keyNulls = memory.HasNulls(binding.first.validity, rows)
	s.valueNulls = memory.HasNulls(binding.values.Validity, rows)
	err = s.query.e.scheduler.ParallelFor(ctx, s.workers, s.workers, func(worker int) error {
		lo := worker * wordsPerWorker * 64
		if lo >= rows {
			return nil
		}
		hi := min(rows, lo+wordsPerWorker*64)
		switch s.query.cq.GroupShape {
		case GroupShapeI32:
			return scatterParallelKeys(ctx, s, binding.first.i32, s.packedI32, binding, selection, lo, hi, worker)
		case GroupShapeI64:
			return scatterParallelKeys(ctx, s, binding.first.i64, s.packedI64, binding, selection, lo, hi, worker)
		default:
			return scatterParallelKeys(ctx, s, binding.first.u32, s.packedU32, binding, selection, lo, hi, worker)
		}
	})
	if err != nil {
		return 0, err
	}
	s.parallelClassified = true
	return selected, nil
}

func scatterParallelKeys[T int32 | uint32 | int64](ctx context.Context, s *residentPartitionedState, keys, packed []T, binding aggregateBatchBinding, selection memory.Bitmap, lo, hi, worker int) error {
	parts := len(s.owners)
	positions := s.workerPositions[worker*parts : (worker+1)*parts]
	for wi := lo / 64; wi < memory.WordsFor(hi); wi++ {
		if wi&15 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		for word := selection[wi]; word != 0; word &= word - 1 {
			row := wi*64 + bits.TrailingZeros64(word)
			if row >= hi {
				break
			}
			partition := int(s.partitionIDs[row])
			position := positions[partition]
			positions[partition]++
			packed[position], s.packedValues[position] = keys[row], binding.values.Values[row]
			if s.keyNulls {
				valid := uint8(0)
				if memory.IsSet(binding.first.validity, row) {
					valid = 1
				}
				s.scatterKeyFlags[position] = valid
			}
			if s.valueNulls {
				valid := uint8(0)
				if memory.IsSet(binding.values.Validity, row) {
					valid = 1
				}
				s.scatterValueFlags[position] = valid
			}
		}
	}
	return nil
}

func (s *residentPartitionedState) packParallelValidity(partition int) {
	start := s.offsets[partition]
	end := start + s.counts[partition]
	for first := start; first < end; first += 64 {
		var keys, values uint64
		for row := first; row < min(first+64, end); row++ {
			if s.keyNulls {
				keys |= uint64(s.scatterKeyFlags[row]) << uint(row-first)
			}
			if s.valueNulls {
				values |= uint64(s.scatterValueFlags[row]) << uint(row-first)
			}
		}
		if s.keyNulls {
			s.packedKeyValidity[first/64] = keys
		}
		if s.valueNulls {
			s.packedValueValidity[first/64] = values
		}
	}
}
