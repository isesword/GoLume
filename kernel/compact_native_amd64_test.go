//go:build goexperiment.simd && amd64

package kernel

import (
	"simd"
	"simd/archsimd"
	"testing"
)

func TestNativeCompactISAAndWidthGuards(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		for _, width := range []int{128, 256, 512} {
			set := newScalarSet(mode)
			installArchSIMDCompact(&set, mode, width)
			native := mode != ModeScalar && !simd.Emulated() && width >= 512 && archsimd.X86.AVX512()
			for _, id := range []KernelID{KernelCompactFloat32, KernelCompactInt8, KernelCompactUint8, KernelCompactInt16, KernelCompactUint16} {
				want := native && (id == KernelCompactFloat32 || archsimd.X86.AVX512VBMI2())
				c, _ := set.Capability(id)
				if c.HardwareSIMD != want || c.Emulated {
					t.Fatalf("mode=%v width=%d id=%s cap=%+v want=%v", mode, width, id, c, want)
				}
				if want && c.VectorBits != 512 {
					t.Fatalf("width=%d", c.VectorBits)
				}
				if !want && c != (KernelCapability{Backend: "scalar"}) {
					t.Fatalf("fallback=%+v", c)
				}
			}
		}
	}
}

// The toolchain does not expose a GODEBUG switch for VBMI2 alone. Exercise
// that registration branch explicitly, without mutating CPU flags or executing
// unsupported instructions on machines lacking the AVX-512 bundle.
func TestNativeCompactWithoutVBMI2(t *testing.T) {
	set := newScalarSet(ModeAuto)
	installNarrowSIMDCompact(&set, false)
	for _, id := range []KernelID{KernelCompactInt8, KernelCompactUint8, KernelCompactInt16, KernelCompactUint16} {
		c, _ := set.Capability(id)
		if c != (KernelCapability{Backend: "scalar"}) {
			t.Fatalf("id=%s cap=%+v", id, c)
		}
	}
	c, _ := set.Capability(KernelCompactFloat32)
	if !c.HardwareSIMD || c.VectorBits != 512 {
		t.Fatalf("Float32 cap=%+v", c)
	}
}
