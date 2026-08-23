package kernel

import "gocolumn/memory"

// Mode controls how a kernel set selects scalar or SIMD implementations.
type Mode uint8

const (
	// ModeAuto selects hardware SIMD when a profitable supported backend is available.
	ModeAuto Mode = iota
	// ModeScalar disables SIMD and always selects scalar kernels.
	ModeScalar
	// ModeSIMD requests SIMD but still falls back when the runtime reports emulation or no implementation.
	ModeSIMD
)

// Capabilities describes the implementation selected for a kernel set.
type Capabilities struct {
	Mode         Mode
	Arch         string
	HardwareSIMD bool
	Emulated     bool
	VectorBits   int
	Backend      string
}

// Int64GTFunc filters values greater than threshold into out and returns the selected-row count.
// validity may be nil to indicate that every input row is valid.
type Int64GTFunc func(values []int64, validity memory.Bitmap, out memory.Bitmap, threshold int64) int

// Set groups hot column kernels selected for the current runtime capabilities.
type Set struct {
	Int64GT Int64GTFunc
	Caps    Capabilities
}

// New constructs a kernel set for mode and the current architecture.
func New(mode Mode) Set {
	if mode != ModeScalar {
		if fn, caps, ok := newSIMDInt64GT(mode); ok {
			return Set{Int64GT: fn, Caps: caps}
		}
	}
	return Set{
		Int64GT: scalarInt64GT,
		Caps:    Capabilities{Mode: ModeScalar, Arch: runtimeArch(), Backend: "scalar"},
	}
}
