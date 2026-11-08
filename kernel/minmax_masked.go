package kernel

import (
	"gocolumn/memory"
	"math/bits"
)

// MinMaxID identifies bounded masked extrema. Temporal aliases use their physical width.
func MinMaxID(typ memory.DataType) (KernelID, bool) {
	switch typ {
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		typ = memory.TypeInt32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		typ = memory.TypeInt64
	case memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
	default:
		return KernelID{}, false
	}
	return KernelID{kind: KernelReduction, typ: typ, op: 2}, true
}

func checkedMinMaxMask(n int, mask uint64) uint64 {
	if n > 64 {
		panic("kernel: masked min/max length exceeds 64")
	}
	return mask & memory.TailMask(n)
}

type extremeNumber interface {
	int32 | int64 | uint32 | uint64 | float32 | float64
}

func scalarMinMaxMasked[T extremeNumber](v []T, mask uint64) (lo, hi T) {
	mask = checkedMinMaxMask(len(v), mask)
	if mask == 0 {
		return
	}
	lo = v[bits.TrailingZeros64(mask)]
	hi = lo
	mask &= mask - 1
	for mask != 0 {
		x := v[bits.TrailingZeros64(mask)]
		if lo != lo {
			lo, hi = x, x
		} else if x == x {
			lo = min(lo, x)
			hi = max(hi, x)
		}
		mask &= mask - 1
	}
	return
}

func installMinMaxScalar(d *dispatch) {
	d.minMaxInt32Masked = scalarMinMaxMasked[int32]
	d.minMaxInt64Masked = scalarMinMaxMasked[int64]
	d.minMaxUint32Masked = scalarMinMaxMasked[uint32]
	d.minMaxUint64Masked = scalarMinMaxMasked[uint64]
	d.minMaxFloat32Masked = scalarMinMaxMasked[float32]
	d.minMaxFloat64Masked = scalarMinMaxMasked[float64]
}

// MinMaxInt32Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
func (s Set) MinMaxInt32Masked(v []int32, mask uint64) (int32, int32) {
	return s.d.minMaxInt32Masked(v, mask)
}

// MinMaxInt64Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
func (s Set) MinMaxInt64Masked(v []int64, mask uint64) (int64, int64) {
	return s.d.minMaxInt64Masked(v, mask)
}

// MinMaxUint32Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
func (s Set) MinMaxUint32Masked(v []uint32, mask uint64) (uint32, uint32) {
	return s.d.minMaxUint32Masked(v, mask)
}

// MinMaxUint64Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
func (s Set) MinMaxUint64Masked(v []uint64, mask uint64) (uint64, uint64) {
	return s.d.minMaxUint64Masked(v, mask)
}

// MinMaxFloat32Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
// NaNs are ignored unless all selected values are NaN (the last payload wins).
// For zero ties the minimum is -0 and the maximum is +0.
func (s Set) MinMaxFloat32Masked(v []float32, mask uint64) (float32, float32) {
	return s.d.minMaxFloat32Masked(v, mask)
}

// MinMaxFloat64Masked returns extrema of selected elements. Bits beyond len(v) are
// ignored; empty selections return (0,0). It allocates nothing and panics above 64 rows.
// NaNs are ignored unless all selected values are NaN (the last payload wins).
// For zero ties the minimum is -0 and the maximum is +0.
func (s Set) MinMaxFloat64Masked(v []float64, mask uint64) (float64, float64) {
	return s.d.minMaxFloat64Masked(v, mask)
}
