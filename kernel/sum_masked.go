package kernel

import (
	"gocolumn/memory"
	"math/bits"
)

// SumID returns the bounded masked integer SUM identifier. Temporal aliases
// use their physical integer width. Narrow integers widen to int64; floats are unsupported.
func SumID(typ memory.DataType) (KernelID, bool) {
	switch typ {
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		typ = memory.TypeInt32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		typ = memory.TypeInt64
	case memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
	default:
		return KernelID{}, false
	}
	return KernelID{kind: KernelReduction, typ: typ, op: 1}, true
}

// checkedSumMask enforces the bounded work contract before any early return.
func checkedSumMask(n int, mask uint64) uint64 {
	if n > 64 {
		panic("kernel: masked sum length exceeds 64")
	}
	return mask & memory.TailMask(n)
}

type sumInteger interface {
	int32 | uint32 | int64 | uint64
}

func scalarSumMasked[T sumInteger](v []T, mask uint64) (sum T) {
	mask = checkedSumMask(len(v), mask)
	for mask != 0 {
		sum += v[bits.TrailingZeros64(mask)]
		mask &= mask - 1
	}
	return
}

func installSumScalar(d *dispatch) {
	d.sumInt8Masked = scalarNarrowSumMasked[int8]
	d.sumInt16Masked = scalarNarrowSumMasked[int16]
	d.sumUint8Masked = scalarNarrowSumMasked[uint8]
	d.sumUint16Masked = scalarNarrowSumMasked[uint16]

	d.sumInt32Masked = scalarSumMasked[int32]
	d.sumUint32Masked = scalarSumMasked[uint32]
	d.sumInt64Masked = scalarSumMasked[int64]
	d.sumUint64Masked = scalarSumMasked[uint64]
}

// SumInt32Masked sums selected elements with native-width wrapping arithmetic.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumInt32Masked(v []int32, mask uint64) int32 {
	return s.d.sumInt32Masked(v, mask)
}

// SumUint32Masked sums selected elements with native-width wrapping arithmetic.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumUint32Masked(v []uint32, mask uint64) uint32 {
	return s.d.sumUint32Masked(v, mask)
}

// SumInt64Masked sums selected elements with native-width wrapping arithmetic.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumInt64Masked(v []int64, mask uint64) int64 {
	return s.d.sumInt64Masked(v, mask)
}

// SumUint64Masked sums selected elements with native-width wrapping arithmetic.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumUint64Masked(v []uint64, mask uint64) uint64 {
	return s.d.sumUint64Masked(v, mask)
}
