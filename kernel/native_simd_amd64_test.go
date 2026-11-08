//go:build goexperiment.simd && amd64

package kernel

import (
	"simd"
	"simd/archsimd"
	"testing"
)

func TestAVX512AutoDispatchHonorsHardwareAndVectorWidth(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		wantNative := mode != ModeScalar && expectedNativeSIMD() && simd.Int64s{}.Len()*64 >= 512 && archsimd.X86.AVX512()
		for _, id := range []KernelID{KernelHash64Batch, KernelCompactInt32, KernelCompactInt64, KernelCompactUint32, KernelCompactUint64, KernelCompactFloat64} {
			capability, _ := set.Capability(id)
			if capability.HardwareSIMD != wantNative || capability.Emulated {
				t.Fatalf("mode=%s id=%s capability=%+v want_native=%t", mode, id, capability, wantNative)
			}
			if wantNative && capability.VectorBits != 512 {
				t.Fatalf("mode=%s id=%s vector_bits=%d", mode, id, capability.VectorBits)
			}
		}
		// A narrower portable runtime must never select wide archsimd paths,
		// even on a machine with AVX-512 hardware.
		for _, width := range []int{128, 256} {
			narrow := newScalarSet(mode)
			installArchSIMDHash(&narrow, mode, width)
			installArchSIMDCompact(&narrow, mode, width)
			for _, id := range []KernelID{KernelHash64Batch, KernelCompactInt32, KernelCompactInt64, KernelCompactUint32, KernelCompactUint64, KernelCompactFloat64} {
				capability, _ := narrow.Capability(id)
				if capability != (KernelCapability{Backend: "scalar"}) {
					t.Fatalf("mode=%s width=%d id=%s capability=%+v", mode, width, id, capability)
				}
			}
		}
	}
}
