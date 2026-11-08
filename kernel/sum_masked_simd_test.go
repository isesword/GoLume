//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestMaskedSumNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
			id, _ := SumID(typ)
			got, ok := set.Capability(id)
			want := KernelCapability{Backend: "scalar"}
			if mode != ModeScalar && nativeSIMDAvailable() {
				want = KernelCapability{HardwareSIMD: true, VectorBits: simd.Int64s{}.Len() * 64, Backend: "portable-simd"}
			}
			if want.HardwareSIMD && want.VectorBits == 512 && runtimeArch() == "amd64" {
				want.Backend = "archsimd-avx512-sum"
			}
			if !ok || got != want {
				t.Fatalf("mode=%s id=%s got=%+v want=%+v", mode, id, got, want)
			}
		}
	}
}
