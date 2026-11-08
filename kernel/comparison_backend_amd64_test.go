//go:build goexperiment.simd && amd64

package kernel

import (
	"simd"
	"simd/archsimd"
)

func expectedComparisonBackend() string {
	width := simd.Int64s{}.Len() * 64
	if expectedNativeSIMD() && ((width == 128 && archsimd.X86.AVX2()) || (width == 256 && archsimd.X86.AVX2()) || (width == 512 && archsimd.X86.AVX512())) {
		return "archsimd-amd64-mask"
	}
	return "portable-simd"
}
