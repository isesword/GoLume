package engine

import (
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/bits"
)

// forSimpleSelected produces one temporary 64-row mask at a time and consumes
// its set lanes immediately. It gives DirectFused the selected kernel backend
// without materializing an input-batch selection bitmap.
func forSimpleSelected(batch *memory.Batch, comparison *expr.SimpleComparison, kernels kernel.Set, word memory.Bitmap, fn func(int) error) (int, error) {
	return forSimpleSelectedBlocks(batch, comparison, kernels, word, func(base int, word memory.Bitmap) error { return consumeSelectedWord(base, word[0], fn) })
}

// forSimpleSelectedBlocks emits one bounded mask at a time. Fixed-key aggregate
// consumers pass it directly to the same selected-row kernel used by bitmap
// execution; generic consumers iterate its selected rows without another bitmap.
func forSimpleSelectedBlocks(batch *memory.Batch, comparison *expr.SimpleComparison, kernels kernel.Set, word memory.Bitmap, fn func(int, memory.Bitmap) error) (int, error) {
	op, err := directCompareOp(comparison.Op)
	if err != nil {
		return 0, err
	}
	selected := 0
	switch comparison.Type {
	case memory.TypeInt8:
		v, err := batch.Int8(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralI64 < math.MinInt8 || comparison.LiteralI64 > math.MaxInt8 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideSigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralI64, op, emit)
			}, word, fn)
		}
		literal := int8(comparison.LiteralI64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt8(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeInt16:
		v, err := batch.Int16(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralI64 < math.MinInt16 || comparison.LiteralI64 > math.MaxInt16 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideSigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralI64, op, emit)
			}, word, fn)
		}
		literal := int16(comparison.LiteralI64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt16(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint8:
		v, err := batch.Uint8(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralU64 > math.MaxUint8 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideUnsigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralU64, op, emit)
			}, word, fn)
		}
		literal := uint8(comparison.LiteralU64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint8(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint16:
		v, err := batch.Uint16(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralU64 > math.MaxUint16 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideUnsigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralU64, op, emit)
			}, word, fn)
		}
		literal := uint16(comparison.LiteralU64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint16(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeFloat32:
		v, err := batch.Float32(comparison.Column)
		if err != nil {
			return 0, err
		}
		if float64(float32(comparison.LiteralF64)) != comparison.LiteralF64 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideFloat32(v.Values[:batch.Rows], v.Validity, comparison.LiteralF64, op, emit)
			}, word, fn)
		}
		literal := float32(comparison.LiteralF64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareFloat32(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}

	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v, err := batch.Int32(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralI64 < math.MinInt32 || comparison.LiteralI64 > math.MaxInt32 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideSigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralI64, op, emit)
			}, word, fn)
		}
		literal := int32(comparison.LiteralI64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt32(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v, err := batch.Int64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralI64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareInt64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint32, memory.TypeBool:
		v, err := batch.Uint32(comparison.Column)
		if err != nil {
			return 0, err
		}
		if comparison.LiteralU64 > math.MaxUint32 {
			return collectSelectedMasks(func(emit func(int) error) (int, error) {
				return forWideUnsigned(v.Values[:batch.Rows], v.Validity, comparison.LiteralU64, op, emit)
			}, word, fn)
		}
		literal := uint32(comparison.LiteralU64)
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint32(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeUint64:
		v, err := batch.Uint64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralU64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareUint64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	case memory.TypeFloat64:
		v, err := batch.Float64(comparison.Column)
		if err != nil {
			return 0, err
		}
		literal := comparison.LiteralF64
		for base := 0; base < batch.Rows; base += 64 {
			end := min(base+64, batch.Rows)
			word[0] = 0
			selected += kernels.CompareFloat64(v.Values[base:end], blockValidity(v.Validity, base), word, literal, op)
			if err := fn(base, word); err != nil {
				return 0, err
			}
		}
	default:
		return 0, fmt.Errorf("direct-fused comparison type %s unsupported", comparison.Type)
	}
	return selected, nil
}

// collectSelectedMasks adapts the exact wider-literal scalar oracle to the
// same bounded-mask consumer. It owns one word, never a whole-batch bitmap.
func collectSelectedMasks(produce func(func(int) error) (int, error), mask memory.Bitmap, consume func(int, memory.Bitmap) error) (int, error) {
	base := -1
	var word uint64
	selected, err := produce(func(row int) error {
		next := row &^ 63
		if next != base {
			if base >= 0 {
				mask[0] = word
				if err := consume(base, mask); err != nil {
					return err
				}
			}
			base, word = next, 0
		}
		word |= uint64(1) << uint(row-base)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if base >= 0 {
		mask[0] = word
		if err := consume(base, mask); err != nil {
			return 0, err
		}
	}
	return selected, nil
}

func consumeSelectedWord(base int, word uint64, fn func(int) error) error {
	for mask := word; mask != 0; mask &= mask - 1 {
		if err := fn(base + bits.TrailingZeros64(mask)); err != nil {
			return err
		}
	}
	return nil
}

func blockValidity(validity memory.Bitmap, base int) memory.Bitmap {
	if validity == nil {
		return nil
	}
	return validity[base/64:]
}

func directCompareOp(op expr.Op) (kernel.CompareOp, error) {
	switch op {
	case expr.OpGT:
		return kernel.CompareGT, nil
	case expr.OpGE:
		return kernel.CompareGE, nil
	case expr.OpLT:
		return kernel.CompareLT, nil
	case expr.OpLE:
		return kernel.CompareLE, nil
	case expr.OpEQ:
		return kernel.CompareEQ, nil
	default:
		return 0, fmt.Errorf("direct-fused comparison operator %s unsupported", op)
	}
}

// Wider literals must not wrap or round into the column's native type. These
// uncommon scalar paths compare in the literal domain and preserve SQL NULLs.
func forWideSigned[T ~int8 | ~int16 | ~int32](values []T, validity memory.Bitmap, literal int64, op kernel.CompareOp, fn func(int) error) (int, error) {
	selected := 0
	for row, value := range values {
		if validity != nil && !memory.IsSet(validity, row) {
			continue
		}
		if wideCompare(int64(value), literal, op) {
			if err := fn(row); err != nil {
				return 0, err
			}
			selected++
		}
	}
	return selected, nil
}
func forWideUnsigned[T ~uint8 | ~uint16 | ~uint32](values []T, validity memory.Bitmap, literal uint64, op kernel.CompareOp, fn func(int) error) (int, error) {
	selected := 0
	for row, value := range values {
		if validity != nil && !memory.IsSet(validity, row) {
			continue
		}
		if wideCompare(uint64(value), literal, op) {
			if err := fn(row); err != nil {
				return 0, err
			}
			selected++
		}
	}
	return selected, nil
}
func forWideFloat32(values []float32, validity memory.Bitmap, literal float64, op kernel.CompareOp, fn func(int) error) (int, error) {
	selected := 0
	for row, value := range values {
		if validity != nil && !memory.IsSet(validity, row) {
			continue
		}
		if wideCompare(float64(value), literal, op) {
			if err := fn(row); err != nil {
				return 0, err
			}
			selected++
		}
	}
	return selected, nil
}
func wideCompare[T ~int64 | ~uint64 | ~float64](left, right T, op kernel.CompareOp) bool {
	switch op {
	case kernel.CompareGT:
		return left > right
	case kernel.CompareGE:
		return left >= right
	case kernel.CompareLT:
		return left < right
	case kernel.CompareLE:
		return left <= right
	case kernel.CompareEQ:
		return left == right
	default:
		return false
	}
}
