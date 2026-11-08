//go:build goexperiment.simd && !amd64 && !arm64

package kernel

func installNarrowSumSIMD(*Set, int) {}
