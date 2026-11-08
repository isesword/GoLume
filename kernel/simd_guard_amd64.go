//go:build goexperiment.simd && amd64

package kernel

import "simd/archsimd"

func archSIMDSupported() bool { return archsimd.X86.AVX2() }
