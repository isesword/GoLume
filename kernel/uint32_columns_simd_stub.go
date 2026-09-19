//go:build !goexperiment.simd

package kernel

func installUint32ColumnSIMD(*Set, Mode, int) {}
