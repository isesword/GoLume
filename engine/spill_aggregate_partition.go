package engine

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

// aggregatePartitionDirectory stores table cursors grouped into radix ranges.
// The table remains immutable and authoritative until every run is published.
// Each entry packs the cursor above its low radix bits, so partitioning uses the
// same eight bytes per group as the old hash workspace, not a record snapshot.
// Scratch and range metadata are fixed-size and included in the reservation.
type aggregatePartitionDirectory struct {
	entries []uint64
	offsets [1<<aggregateRadixBits + 1]int
	next    [1 << aggregateRadixBits]int
	hashes  [256]uint64
	lease   memory.Reservation
}

func (d *aggregatePartitionDirectory) close() {
	d.entries = nil
	if d.lease != nil {
		d.lease.Release()
		d.lease = nil
	}
}

func (s *spillableAggregateStream) partitionTable(ctx context.Context, table *aggregateStateTable, bitOffset uint, partitions int) (*aggregatePartitionDirectory, error) {
	if partitions < 1 || partitions > 1<<aggregateRadixBits || partitions&(partitions-1) != 0 {
		return nil, fmt.Errorf("invalid aggregate partition count %d", partitions)
	}
	groups := table.len()
	fixedBytes := int64(unsafe.Sizeof(aggregatePartitionDirectory{}))
	if int64(groups) > (int64(math.MaxInt)-fixedBytes)/8 {
		return nil, fmt.Errorf("aggregate partition cursor workspace exceeds addressable memory")
	}
	lease, err := s.encoder.ReserveGrowth(ctx, fixedBytes+int64(groups)*8)
	if err != nil {
		return nil, fmt.Errorf("reserve aggregate partition cursors: %w", err)
	}
	d := &aggregatePartitionDirectory{entries: make([]uint64, groups), lease: lease}
	success := false
	defer func() {
		if !success {
			d.close()
		}
	}()
	shape := table.shape
	batchHash64 := shape == GroupShapeI32 || shape == GroupShapeI64 || shape == GroupShapeU32 || shape == GroupShapeDictionaryString
	mask := uint64(partitions - 1)
	flush := func(end, count int) {
		hashes := d.hashes[:count]
		if batchHash64 {
			// Retain operation-specific batch dispatch without an unbounded
			// second array. Pair, string and generic keys remain scalar.
			s.query.e.kernels.Hash64Batch(hashes, hashes)
		}
		for i, hash := range hashes {
			partition := hash >> bitOffset & mask
			d.entries[end-count+i] |= partition
			d.offsets[partition+1]++
		}
	}
	cursor, index, buffered := 0, 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		emitted := false
		done, err := table.visitChunk(&cursor, 1, func(record aggregateRecord) error {
			if index >= groups {
				return fmt.Errorf("aggregate table visit exceeded group count")
			}
			emitted = true
			if batchHash64 {
				if record.validMask == 0 {
					d.hashes[buffered] = 0x9e3779b97f4a7c15
				} else if shape == GroupShapeI64 {
					d.hashes[buffered] = record.a
				} else {
					d.hashes[buffered] = uint64(uint32(record.a))
				}
			} else {
				d.hashes[buffered] = aggregateRecordHash(shape, record)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if emitted {
			// All table cursors advance past the emitted group, including NULL.
			// Record the occupied slot itself, avoiding rescanning sparse gaps
			// when the range is visited during sizing and serialization.
			position := cursor - 1
			if position < 0 || uint64(position) > math.MaxUint64>>aggregateRadixBits {
				return nil, fmt.Errorf("aggregate partition cursor exceeds addressable range")
			}
			d.entries[index] = uint64(position) << aggregateRadixBits
			index++
			buffered++
			if buffered == len(d.hashes) {
				flush(index, buffered)
				buffered = 0
			}
		}
		if done {
			break
		}
	}
	if index != groups {
		return nil, fmt.Errorf("aggregate table visit returned %d groups, want %d", index, groups)
	}
	flush(index, buffered)
	for partition := 0; partition < partitions; partition++ {
		d.offsets[partition+1] += d.offsets[partition]
		d.next[partition] = d.offsets[partition]
	}
	// In-place counting partition: each swap puts an entry in its final
	// partition range, so this does O(groups + partitions) directory work.
	moved := 0
	for partition := 0; partition < partitions; partition++ {
		for d.next[partition] < d.offsets[partition+1] {
			if moved&255 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			moved++
			index := d.next[partition]
			target := int(d.entries[index] & mask)
			if target == partition {
				d.next[partition]++
				continue
			}
			slot := d.next[target]
			d.entries[index], d.entries[slot] = d.entries[slot], d.entries[index]
			d.next[target]++
		}
	}
	success = true
	return d, nil
}

func (d *aggregatePartitionDirectory) visit(ctx context.Context, table *aggregateStateTable, partition int, fn func(aggregateRecord) error) error {
	for i, entry := range d.entries[d.offsets[partition]:d.offsets[partition+1]] {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		cursor := int(entry >> aggregateRadixBits)
		if _, err := table.visitChunk(&cursor, 1, fn); err != nil {
			return err
		}
	}
	return nil
}
