//go:build goexperiment.simd && !amd64

package kernel

// The arm64 literal Int32/Uint32 overrides have their own assertions.
func expectedComparisonBackend() string { return "portable-simd" }
