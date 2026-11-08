package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/bits"
)

// consumeExtremaBatch continues a complete lane snapshot and commits it only
// after the batch succeeds. No state pointer or source data outlives this call.
// SUM stays in encounter order for floats; extrema use bounded SIMD reducers.
func (b *globalAggregateLaneBinding) consumeExtremaBatch(ctx context.Context, kernels kernel.Set, selection memory.Bitmap, rows int, selectedRows uint64, state *aggregate.NumericResult) error {
	switch b.typ {
	case memory.TypeInt32:
		return consumeGlobalIntegerExtremaBatch(ctx, b.int32s.Values[:rows], b.validity, selection, selectedRows, state, kernels.SumInt32Masked, kernels.MinMaxInt32Masked)
	case memory.TypeInt64:
		return consumeGlobalIntegerExtremaBatch(ctx, b.int64s.Values[:rows], b.validity, selection, selectedRows, state, kernels.SumInt64Masked, kernels.MinMaxInt64Masked)
	case memory.TypeUint32:
		return consumeGlobalIntegerExtremaBatch(ctx, b.uint32s.Values[:rows], b.validity, selection, selectedRows, state, kernels.SumUint32Masked, kernels.MinMaxUint32Masked)
	case memory.TypeUint64:
		return consumeGlobalIntegerExtremaBatch(ctx, b.uint64s.Values[:rows], b.validity, selection, selectedRows, state, kernels.SumUint64Masked, kernels.MinMaxUint64Masked)
	case memory.TypeFloat32:
		return consumeGlobalFloatExtremaBatch(ctx, b.float32s.Values[:rows], b.validity, selection, selectedRows, state, kernels.MinMaxFloat32Masked, math.Float32frombits(uint32(state.SumBits)), math.Float32frombits(uint32(state.MinBits)), math.Float32frombits(uint32(state.MaxBits)))
	case memory.TypeFloat64:
		return consumeGlobalFloatExtremaBatch(ctx, b.float64s.Values[:rows], b.validity, selection, selectedRows, state, kernels.MinMaxFloat64Masked, math.Float64frombits(state.SumBits), math.Float64frombits(state.MinBits), math.Float64frombits(state.MaxBits))
	default:
		return fmt.Errorf("unsupported global extrema input %s", b.typ)
	}
}

func consumeGlobalIntegerExtremaBatch[T int32 | int64 | uint32 | uint64](ctx context.Context, values []T, validity, selection memory.Bitmap, selectedRows uint64, state *aggregate.NumericResult, reduceSum func([]T, uint64) T, reduceExtrema func([]T, uint64) (T, T)) error {
	sum, lo, hi := T(state.SumBits), T(state.MinBits), T(state.MaxBits)
	var validValues uint64
	for base := 0; base < len(values); base += 64 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(base+64, len(values))
		word := selection[base>>6] & memory.TailMask(end-base)
		if validity != nil {
			word &= validity[base>>6]
		}
		if word == 0 {
			continue
		}
		validValues += uint64(bits.OnesCount64(word))
		if state.Layout.Sum {
			sum += reduceSum(values[base:end], word)
		}
		wordLo, wordHi := reduceExtrema(values[base:end], word)
		if state.Layout.Min {
			if !state.MinValid || wordLo < lo {
				lo = wordLo
			}
			state.MinValid = true
		}
		if state.Layout.Max {
			if !state.MaxValid || wordHi > hi {
				hi = wordHi
			}
			state.MaxValid = true
		}
	}
	if validValues != 0 {
		if state.Layout.Sum {
			state.SumBits, state.SumValid = uint64(sum), true
		}
		if state.Layout.Min {
			state.MinBits = uint64(lo)
		}
		if state.Layout.Max {
			state.MaxBits = uint64(hi)
		}
	}
	if state.Layout.CountRows {
		state.CountRows += selectedRows
	}
	if state.Layout.CountValues {
		state.CountValues += validValues
	}
	return nil
}

func consumeGlobalFloatExtremaBatch[T float32 | float64](ctx context.Context, values []T, validity, selection memory.Bitmap, selectedRows uint64, state *aggregate.NumericResult, reduceExtrema func([]T, uint64) (T, T), sum, lo, hi T) error {
	var validValues uint64
	for base := 0; base < len(values); base += 64 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(base+64, len(values))
		word := selection[base>>6] & memory.TailMask(end-base)
		if validity != nil {
			word &= validity[base>>6]
		}
		if word == 0 {
			continue
		}
		validValues += uint64(bits.OnesCount64(word))
		if state.Layout.Sum {
			sum = sumSelectedFloat(values[base:end], word, sum)
		}
		wordLo, wordHi := reduceExtrema(values[base:end], word)
		if state.Layout.Min {
			if !state.MinValid {
				lo = wordLo
			} else {
				lo = globalFloatExtreme(lo, wordLo, true)
			}
			state.MinValid = true
		}
		if state.Layout.Max {
			if !state.MaxValid {
				hi = wordHi
			} else {
				hi = globalFloatExtreme(hi, wordHi, false)
			}
			state.MaxValid = true
		}
	}
	if validValues != 0 {
		if state.Layout.Sum {
			state.SumBits, state.SumValid = globalFloatBits(sum), true
		}
		if state.Layout.Min {
			state.MinBits = globalFloatBits(lo)
		}
		if state.Layout.Max {
			state.MaxBits = globalFloatBits(hi)
		}
	}
	if state.Layout.CountRows {
		state.CountRows += selectedRows
	}
	if state.Layout.CountValues {
		state.CountValues += validValues
	}
	return nil
}

// NaN checks precede conversions so an all-NaN lane preserves the final raw
// payload, including Float32 signaling NaNs. Numeric extrema canonicalize
// signed zero exactly like aggregate.numericExtreme.
func globalFloatExtreme[T float32 | float64](left, right T, minimum bool) T {
	if left != left {
		return right
	}
	if right != right {
		return left
	}
	if minimum {
		return T(math.Min(float64(left), float64(right)))
	}
	return T(math.Max(float64(left), float64(right)))
}

func globalFloatBits[T float32 | float64](value T) uint64 {
	switch x := any(value).(type) {
	case float32:
		return uint64(math.Float32bits(x))
	case float64:
		return math.Float64bits(x)
	}
	panic("unsupported float type")
}
