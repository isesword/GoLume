//go:build goexperiment.simd

package kernel

import (
	"runtime"
	"simd"
	"simd/archsimd"
	"testing"

	"gocolumn/memory"
)

func TestWidenReviewCapabilityGuards(t *testing.T) {
	eligible := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeFloat32}
	ineligible := []memory.DataType{memory.TypeFloat64, memory.TypeBool, memory.TypeString}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		bits := simd.Int64s{}.Len() * 64
		want := 0
		if mode != ModeScalar && nativeSIMDAvailable() {
			if runtime.GOARCH == "amd64" && bits >= 256 {
				want = 256
			}
			if runtime.GOARCH == "arm64" && bits == 128 {
				want = 128
			}
		}
		for _, typ := range eligible {
			cap := s.Float64WideningCapability(typ)
			if cap.HardwareSIMD != (want != 0) || cap.VectorBits != want {
				t.Fatalf("%v %v got %+v want SIMD bits %d", mode, typ, cap, want)
			}
			if want == 0 && cap.Backend != "scalar" {
				t.Fatalf("disabled widening reported %q", cap.Backend)
			}
		}
		wideWant := want
		if runtime.GOARCH == "amd64" && !archsimd.X86.AVX512() {
			wideWant = 0
		}
		for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
			cap := s.Float64WideningCapability(typ)
			if cap.HardwareSIMD != (wideWant != 0) || cap.VectorBits != wideWant {
				t.Fatalf("%v %v got %+v want wide bits %d", mode, typ, cap, wideWant)
			}
		}

		for _, typ := range ineligible {
			if cap := s.Float64WideningCapability(typ); cap != (KernelCapability{Backend: "scalar"}) {
				t.Fatalf("unsupported widening %v got %+v", typ, cap)
			}
		}
	}
}
