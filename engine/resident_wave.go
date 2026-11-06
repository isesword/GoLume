package engine

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"math/bits"
	"unsafe"
)

// residentAggregateWave owns only projected, already-selected fixed-width rows.
// It never retains a source batch past Next. Capacity is independent of total
// input rows: only scan bounds, workers, in-flight limits, and budget affect it.
// Flushing invokes the same partition classifier and typed aggregate state.
type residentAggregateWave struct {
	i32                                   []int32
	u32                                   []uint32
	i64                                   []int64
	values                                []int64
	keyValidity, valueValidity, selection memory.Bitmap
	keyNulls, valueNulls                  bool
	rows, capacity                        int
	lease                                 memory.Reservation
}

func (s *residentPartitionedState) prepareWave(ctx context.Context, readOpts source.ReadOptions) {
	if !s.packed() || s.workers < 2 {
		return
	}
	batchRows := s.query.e.opts.Pipeline.MaxMorselRows
	if readOpts.BatchRows > 0 {
		batchRows = min(batchRows, readOpts.BatchRows)
	}
	if batchRows < 1 {
		return
	}
	// Forty bytes covers both the widest projected row and its classifier/
	// scatter storage. Also allow bitmap alignment padding in every shard.
	// Keep ample space for producers and aggregate state.
	byteLimit := s.query.e.opts.Pipeline.MaxInFlightBytes / 4
	if s.budget.Limit() > 0 {
		byteLimit = min(byteLimit, s.budget.Limit()/8)
		byteLimit = min(byteLimit, max(int64(0), s.budget.Limit()-s.budget.Used())/4)
	}
	paddingBytes := int64(len(s.owners))*63*19 + 512 + int64(s.workers)*int64(len(s.owners))*2*int64(unsafe.Sizeof(int(0)))
	maxRows := max(int64(0), byteLimit-paddingBytes) / 40
	maxInt := int64(int(^uint(0) >> 1))
	maxRows = min(maxRows, maxInt/40)
	if int64(batchRows) <= maxRows/int64(s.workers) {
		maxRows = int64(batchRows) * int64(s.workers)
	}
	if maxRows <= int64(batchRows) {
		return
	}
	width := int64(12)
	if s.query.cq.GroupShape == GroupShapeI64 {
		width = 16
	}
	for maxRows > int64(batchRows) {
		rows := int(maxRows)
		bytes := maxRows*width + int64(memory.WordsFor(rows))*24
		lease, err := s.workspace.ReserveGrowth(ctx, bytes)
		if err != nil {
			if ctx.Err() != nil || !errors.Is(err, memory.ErrBudgetExceeded) {
				return
			}
			maxRows /= 2
			continue
		}
		wave := &residentAggregateWave{capacity: rows, lease: lease}
		switch s.query.cq.GroupShape {
		case GroupShapeI32:
			wave.i32 = make([]int32, rows)
		case GroupShapeI64:
			wave.i64 = make([]int64, rows)
		default:
			wave.u32 = make([]uint32, rows)
		}
		wave.values = make([]int64, rows)
		wave.keyValidity = make(memory.Bitmap, memory.WordsFor(rows))
		wave.valueValidity = make(memory.Bitmap, memory.WordsFor(rows))
		wave.selection = make(memory.Bitmap, memory.WordsFor(rows))
		for i := range wave.selection {
			wave.selection[i] = ^uint64(0)
		}
		s.wave = wave
		return
	}
}

func setWaveValidPrefix(validity memory.Bitmap, rows int) {
	words := rows / 64
	for i := 0; i < words; i++ {
		validity[i] = ^uint64(0)
	}
	if rows&63 != 0 {
		validity[words] = memory.TailMask(rows)
	}
}

func (s *residentPartitionedState) aggregateBatch(ctx context.Context, binding aggregateBatchBinding, selection memory.Bitmap, rows int) (int, int64, error) {
	if s.wave == nil {
		return s.aggregateCurrentBatch(ctx, binding, selection, rows)
	}
	switch s.query.cq.GroupShape {
	case GroupShapeI32:
		return appendResidentWave(ctx, s, binding.first.i32, s.wave.i32, binding, selection, rows)
	case GroupShapeI64:
		return appendResidentWave(ctx, s, binding.first.i64, s.wave.i64, binding, selection, rows)
	default:
		return appendResidentWave(ctx, s, binding.first.u32, s.wave.u32, binding, selection, rows)
	}
}

func appendResidentWave[T int32 | uint32 | int64](ctx context.Context, s *residentPartitionedState, keys, dst []T, binding aggregateBatchBinding, selection memory.Bitmap, rows int) (int, int64, error) {
	wave := s.wave
	keyNulls := memory.HasNulls(binding.first.validity, rows)
	valueNulls := memory.HasNulls(binding.values.Validity, rows)
	selected := 0
	checksum := int64(0)
	for wi, word := range selection {
		if wi&15 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, 0, err
			}
		}
		for word != 0 {
			row := wi*64 + bits.TrailingZeros64(word)
			if row >= rows {
				break
			}
			if keyNulls && !wave.keyNulls {
				setWaveValidPrefix(wave.keyValidity, wave.rows)
				wave.keyNulls = true
			}
			if valueNulls && !wave.valueNulls {
				setWaveValidPrefix(wave.valueValidity, wave.rows)
				wave.valueNulls = true
			}
			position := wave.rows
			dst[position], wave.values[position] = keys[row], binding.values.Values[row]
			if wave.keyNulls && (!keyNulls || memory.IsSet(binding.first.validity, row)) {
				memory.Set(wave.keyValidity, position)
			}
			if wave.valueNulls && (!valueNulls || memory.IsSet(binding.values.Validity, row)) {
				memory.Set(wave.valueValidity, position)
			}
			wave.rows++
			selected++
			if wave.rows == wave.capacity {
				sum, err := s.finishInput(ctx)
				if err != nil {
					return 0, 0, err
				}
				checksum += sum
			}
			word &= word - 1
		}
	}
	return selected, checksum, nil
}

func (s *residentPartitionedState) finishInput(ctx context.Context) (int64, error) {
	wave := s.wave
	if wave == nil || wave.rows == 0 {
		return 0, nil
	}
	binding := aggregateBatchBinding{first: fixedColumn{typ: s.query.cq.GroupBy[0].Type, i32: wave.i32, u32: wave.u32, i64: wave.i64}, values: &memory.Int64Vector{Values: wave.values[:wave.rows]}}
	if wave.keyNulls {
		binding.first.validity = wave.keyValidity
	}
	if wave.valueNulls {
		binding.values.Validity = wave.valueValidity
	}
	_, checksum, err := s.aggregateCurrentBatch(ctx, binding, wave.selection[:memory.WordsFor(wave.rows)], wave.rows)
	if err != nil {
		return 0, err
	}
	wave.rows = 0
	if wave.keyNulls {
		clear(wave.keyValidity)
	}
	if wave.valueNulls {
		clear(wave.valueValidity)
	}
	wave.keyNulls, wave.valueNulls = false, false
	return checksum, nil
}

func (w *residentAggregateWave) close() {
	if w == nil {
		return
	}
	w.i32, w.u32, w.i64, w.values = nil, nil, nil, nil
	w.keyValidity, w.valueValidity, w.selection = nil, nil, nil
	if w.lease != nil {
		w.lease.Release()
		w.lease = nil
	}
	w.rows, w.capacity = 0, 0
}
