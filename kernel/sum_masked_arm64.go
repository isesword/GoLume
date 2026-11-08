//go:build goexperiment.simd && arm64

package kernel

import (
	"simd"
	"simd/archsimd"
)

func installArchSumSIMD(*Set, Mode, int) {}

func reduceSumInt32(x simd.Int32s) int32 {
	a := x.ToArch().(archsimd.Int32x4)
	return a.ReduceSum()
}

func reduceSumUint32(x simd.Uint32s) uint32 {
	a := x.ToArch().(archsimd.Uint32x4)
	return a.ReduceSum()
}

func reduceSumInt64(x simd.Int64s) int64 {
	a := x.ToArch().(archsimd.Int64x2)
	return a.GetElem(0) + a.GetElem(1)
}

func reduceSumUint64(x simd.Uint64s) uint64 {
	a := x.ToArch().(archsimd.Uint64x2)
	return a.GetElem(0) + a.GetElem(1)
}
