//go:build goexperiment.simd && amd64

package kernel

import (
	"simd"
	"simd/archsimd"
	"testing"
)

func expectedNativeSIMD() bool { return !simd.Emulated() && archsimd.X86.AVX2() }

func TestSIMDRequiresAVX2(t *testing.T) {
	if archsimd.X86.AVX2() {
		t.Skip("requires GODEBUG=cpu.avx2=off")
	}
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, id := range KernelIDs() {
			c, ok := s.Capability(id)
			if !ok || c != (KernelCapability{Backend: "scalar"}) {
				t.Fatalf("%s/%s: %+v", mode, id, c)
			}
		}
	}
}
