//go:build goexperiment.simd && !amd64 && !arm64

package kernel

func installArchSIMDHash(*Set, Mode, int) {}

func installArchSIMDCompact(*Set, Mode, int) {}
