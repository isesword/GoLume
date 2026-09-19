//go:build !goexperiment.simd

package kernel

func installInt32ColumnSIMD(*Set, Mode, int) {}
