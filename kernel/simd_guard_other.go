//go:build goexperiment.simd && !amd64

package kernel

func archSIMDSupported() bool { return true }
