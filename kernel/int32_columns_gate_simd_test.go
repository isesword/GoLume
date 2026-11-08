//go:build goexperiment.simd

package kernel

import (
	"runtime"
	"simd"
	"testing"
)

func TestColumn32GateEnvironment(t *testing.T) {
	emulated := simd.Emulated()
	t.Logf("arch=%s GOEXPERIMENT=simd emulated=%t int32_vector_bits=%d uint32_vector_bits=%d", runtime.GOARCH, emulated, simd.Int32s{}.Len()*32, simd.Uint32s{}.Len()*32)
	native := expectedComparisonBackend()
	if emulated {
		native = "scalar"
	}
	column32CheckDispatch(t, native)
}
