//go:build goexperiment.simd

package kernel

import (
	"runtime"
	"simd"
	"testing"
)

func TestScaleNativeCapability(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		got, ok := set.Capability(KernelInt64Scale)
		want := mode != ModeScalar && expectedNativeSIMD() && (runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64")
		if !ok || got.HardwareSIMD != want || got.Emulated {
			t.Fatalf("%s %+v want native %v", mode, got, want)
		}
		if want && (got.Backend != "archsimd-"+runtime.GOARCH+"-scale" || got.VectorBits != simd.Int64s{}.Len()*64) {
			t.Fatalf("unexpected capability %+v", got)
		}
	}
}
