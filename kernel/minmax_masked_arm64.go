//go:build goexperiment.simd && arm64

package kernel

import (
	"simd"
	"simd/archsimd"
)

func reduceMinMaxUint32(lo, hi simd.Uint32s) (uint32, uint32) {
	return lo.ToArch().(archsimd.Uint32x4).ReduceMin(), hi.ToArch().(archsimd.Uint32x4).ReduceMax()
}
func reduceMinMaxUint64(lo, hi simd.Uint64s) (uint64, uint64) {
	a := lo.ToArch().(archsimd.Uint64x2)
	z := hi.ToArch().(archsimd.Uint64x2)
	b := a.ConcatOdd(a)
	c := z.ConcatOdd(z)
	return a.IfElse(a.Less(b), b).GetElem(0), z.IfElse(z.Greater(c), c).GetElem(0)
}
