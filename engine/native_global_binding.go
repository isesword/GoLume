package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/kernel"
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

// consumeGlobalAggregateInput bypasses the adjacent-key state machine only for
// the already-bound global SUM/COUNT lanes. Predicate evaluation still uses the
// selected kernel set. Each SUM retains its original encounter order:
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
	kernels := s.query.e.kernels
	for base := 0; base < rows; base += 64 {
		// Check even empty selection words so sparse inputs cannot delay
		// cancellation until a selected row or the next source batch.
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(base+64, rows)
		word := s.selection[base>>6] & memory.TailMask(end-base)
		if word == 0 {
			continue
		}
		selected := uint64(bits.OnesCount64(word))
		for lane := range s.globalBindings {
			bound := &s.globalBindings[lane]
			validWord := word
			if bound.rowsOnly {
				validWord = 0
			} else if bound.validity != nil {
				validWord &= bound.validity[base>>6]
			}
			var state aggregate.NumericSumCountState
			if !s.numeric.BorrowSumCountState(0, lane, &state) {
				return fmt.Errorf("global aggregate lane %d is not SUM/COUNT state", lane)
			}
			sum := state.SumBits()
			if validWord != 0 {
				sum = bound.sumWord(kernels, base, end, validWord, sum)
			}
			state.Update(sum, selected, uint64(bits.OnesCount64(validWord)))
		}
		s.selectedRows += int(selected)
		// Preserve the legacy Int64 diagnostic checksum independently of which
		// SUM/COUNT lanes the query requests. Its integer sum is associative.
		if values := s.binding.values; values != nil {
			validWord := word
			if values.Validity != nil {
				validWord &= values.Validity[base>>6]
			}
			s.checksum += kernels.SumInt64Masked(values.Values[base:end], validWord)
		}
	}
	s.nextRow = rows
	return nil
}

// sumWord dispatches once per column and selection word. Integer reductions
// may reassociate under native SIMD because their wrapping arithmetic is exact.
// Floating reductions continue from the previous state, never a word subtotal.
func (b *globalAggregateLaneBinding) sumWord(kernels kernel.Set, base, end int, selected, sum uint64) uint64 {
	switch b.typ {
	case memory.TypeInt8:
		return sum + uint64(kernels.SumInt8Masked(b.int8s.Values[base:end], selected))
	case memory.TypeInt16:
		return sum + uint64(kernels.SumInt16Masked(b.int16s.Values[base:end], selected))
	case memory.TypeInt32:
		return uint64(int64(int32(sum) + kernels.SumInt32Masked(b.int32s.Values[base:end], selected)))
	case memory.TypeInt64:
		return sum + uint64(kernels.SumInt64Masked(b.int64s.Values[base:end], selected))
	case memory.TypeUint8:
		return sum + uint64(kernels.SumUint8Masked(b.uint8s.Values[base:end], selected))
	case memory.TypeUint16:
		return sum + uint64(kernels.SumUint16Masked(b.uint16s.Values[base:end], selected))
	case memory.TypeUint32:
		return uint64(uint32(sum) + kernels.SumUint32Masked(b.uint32s.Values[base:end], selected))
	case memory.TypeUint64:
		return sum + kernels.SumUint64Masked(b.uint64s.Values[base:end], selected)
	case memory.TypeFloat32:
		return uint64(math.Float32bits(sumSelectedFloat(b.float32s.Values[base:end], selected, math.Float32frombits(uint32(sum)))))
	case memory.TypeFloat64:
		return math.Float64bits(sumSelectedFloat(b.float64s.Values[base:end], selected, math.Float64frombits(sum)))
	default:
		// Count-only lanes bind validity without accessing a numeric payload.
		return sum
	}
}

func sumSelectedFloat[T float32 | float64](values []T, selected uint64, sum T) T {
	if selected == memory.TailMask(len(values)) {
		for _, value := range values {
			sum += value
		}
		return sum
	}
	for selected != 0 {
		sum += values[bits.TrailingZeros64(selected)]
		selected &= selected - 1
	}
	return sum
}
