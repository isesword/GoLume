//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestNarrowSumNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
			id, _ := SumID(typ)
			got, ok := set.Capability(id)
			want := KernelCapability{Backend: "scalar"}
			if mode != ModeScalar && nativeSIMDAvailable() && (runtimeArch() == "amd64" || runtimeArch() == "arm64") {
				want = KernelCapability{HardwareSIMD: true, VectorBits: (simd.Int32s{}).Len() * 32, Backend: "archsimd-widen-sum"}
			}
			if want.HardwareSIMD && want.VectorBits == 512 && runtimeArch() == "amd64" {
				want.Backend = "archsimd-avx512-widen-sum"
			}
			if !ok || got != want {
				t.Fatalf("mode=%s id=%s got=%+v want=%+v", mode, id, got, want)
			}
		}
	}
}
