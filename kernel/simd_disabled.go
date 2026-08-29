//go:build !goexperiment.simd

package kernel

func installSIMD(*Set, Mode) {}
