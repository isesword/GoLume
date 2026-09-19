//go:build !goexperiment.simd

package kernel

func installFloat32ColumnSIMD(*Set, Mode, int) {}
