package engine

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"math/bits"
	"unsafe"
)

// globalAggregateLaneBinding borrows only the current input batch. COUNT(column)
// needs parent validity, not its values (including opaque and nested columns).
// Keeping typed pointers avoids both map lookups and per-row interface dispatch.
type globalAggregateLaneBinding struct {
	typ      memory.DataType
	rowsOnly bool
	validity memory.Bitmap
	int8s    *memory.Int8Vector
	int16s   *memory.Int16Vector
	int32s   *memory.Int32Vector
	int64s   *memory.Int64Vector
	uint8s   *memory.Uint8Vector
	uint16s  *memory.Uint16Vector
	uint32s  *memory.Uint32Vector
	uint64s  *memory.Uint64Vector
	float32s *memory.Float32Vector
	float64s *memory.Float64Vector
}

// globalAggregateBindingsEligible deliberately excludes grouped and richer
// layouts. Their existing readers and aggregation state machines are unchanged.
func (p *PreparedQuery) globalAggregateBindingsEligible() bool {
	if p.cq.GroupShape != GroupShapeGlobal || !p.nativeAggregate() {
		return false
	}
	m := p.cq.MultiAggregate
	if len(m.Columns) == 0 || m.Statistics != nil || m.Distinct != nil || m.Quantiles != nil {
		return false
	}
	for lane, layout := range m.Layouts {
		if layout.Min || layout.Max || layout.Avg {
			return false
		}
		if layout.Sum {
			switch m.Types[lane] {
			case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
				memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
				memory.TypeFloat32, memory.TypeFloat64:
			default:
				return false
			}
		}
	}
	return true
}

func (s *sortedAggregateStream) initGlobalAggregateBindings(ctx context.Context) error {
	if !s.query.globalAggregateBindingsEligible() {
		return nil
	}
	lanes := len(s.query.cq.MultiAggregate.Columns)
	per := uint64(unsafe.Sizeof(globalAggregateLaneBinding{}))
	if uint64(lanes) > min(uint64(math.MaxInt64), uint64(^uint(0)>>1))/per {
		return fmt.Errorf("global aggregate binding size overflow")
	}
	lease, err := s.stateBudget.ReserveGrowth(ctx, int64(uint64(lanes)*per))
	if err != nil {
		return fmt.Errorf("reserve global aggregate bindings: %w", err)
	}
	s.globalBindingLease = lease
	s.globalBindings = make([]globalAggregateLaneBinding, lanes)
	return nil
}

// bindGlobalAggregateInputs runs only after bindSortedBatch has validated every
// lane's presence, length and validity. No allocation or validation order changes
// are introduced at batch boundaries. The next load clears every old reference.
func (s *sortedAggregateStream) bindGlobalAggregateInputs() {
	m := s.query.cq.MultiAggregate
	for lane := range s.globalBindings {
		bound := &s.globalBindings[lane]
		name := m.Columns[lane]
		if name == "" {
			bound.rowsOnly = true
			continue
		}
		_, bound.validity, _ = aggregateColumnInfo(s.inputBatch, name, m.Types[lane], m.Layouts[lane].Sum)
		if !m.Layouts[lane].Sum {
			continue
		}
		bound.typ = m.Types[lane]
		switch bound.typ {
		case memory.TypeInt8:
			bound.int8s = s.inputBatch.Int8s[name]
		case memory.TypeInt16:
			bound.int16s = s.inputBatch.Int16s[name]
		case memory.TypeInt32:
			bound.int32s = s.inputBatch.Int32s[name]
		case memory.TypeInt64:
			bound.int64s = s.inputBatch.Int64s[name]
		case memory.TypeUint8:
			bound.uint8s = s.inputBatch.Uint8s[name]
		case memory.TypeUint16:
			bound.uint16s = s.inputBatch.Uint16s[name]
		case memory.TypeUint32:
			bound.uint32s = s.inputBatch.Uint32s[name]
		case memory.TypeUint64:
			bound.uint64s = s.inputBatch.Uint64s[name]
		case memory.TypeFloat32:
			bound.float32s = s.inputBatch.Float32s[name]
		case memory.TypeFloat64:
			bound.float64s = s.inputBatch.Float64s[name]
		}
	}
}

func (b *globalAggregateLaneBinding) bits(row int) (uint64, bool) {
	if b.rowsOnly {
		return 0, false
	}
	valid := b.validity == nil || memory.IsSet(b.validity, row)
	switch b.typ {
	case memory.TypeInt8:
		return uint64(int64(b.int8s.Values[row])), valid
	case memory.TypeInt16:
		return uint64(int64(b.int16s.Values[row])), valid
	case memory.TypeInt32:
		return uint64(int64(b.int32s.Values[row])), valid
	case memory.TypeInt64:
		return uint64(b.int64s.Values[row]), valid
	case memory.TypeUint8:
		return uint64(b.uint8s.Values[row]), valid
	case memory.TypeUint16:
		return uint64(b.uint16s.Values[row]), valid
	case memory.TypeUint32:
		return uint64(b.uint32s.Values[row]), valid
	case memory.TypeUint64:
		return b.uint64s.Values[row], valid
	case memory.TypeFloat32:
		return uint64(math.Float32bits(b.float32s.Values[row])), valid
	case memory.TypeFloat64:
		return math.Float64bits(b.float64s.Values[row]), valid
	default:
		return 0, valid
	}
}

// consumeGlobalAggregateInput bypasses the adjacent-key state machine only for
// the already-bound global SUM/COUNT lanes. Predicate evaluation still uses the
// selected kernel set. SUM updates retain their original row and lane order:
// horizontal floating-point reductions would change rounding and NaN results.
// No row-sized scratch or additional retained state is required.
func (s *sortedAggregateStream) consumeGlobalAggregateInput(ctx context.Context) error {
	for !s.inputDone {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.loadInput(ctx); err != nil {
			return err
		}
		if err := s.consumeGlobalAggregateBatch(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *sortedAggregateStream) consumeGlobalAggregateBatch(ctx context.Context) error {
	rows := s.inputBatch.Rows
	for base := 0; base < rows; base += 64 {
		// Check even empty selection words so sparse inputs cannot delay
		// cancellation until a selected row or the next source batch.
		if err := ctx.Err(); err != nil {
			return err
		}
		word := s.selection[base>>6]
		if remaining := rows - base; remaining < 64 {
			word &= (uint64(1) << uint(remaining)) - 1
		}
		for word != 0 {
			row := base + bits.TrailingZeros64(word)
			word &= word - 1
			for lane := range s.globalBindings {
				value, valid := s.globalBindings[lane].bits(row)
				s.numeric.AddBits(0, lane, value, valid)
			}
			s.selectedRows++
			// This is the legacy Int64 diagnostic checksum, not a native
			// reduction of arbitrary SUM lanes.
			if value, valid := s.binding.value(row); valid {
				s.checksum += value
			}
		}
	}
	s.nextRow = rows
	return nil
}
