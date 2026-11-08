//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"reflect"
	"simd"
	"testing"
)

func TestMaskedMinMaxNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
			id, _ := MinMaxID(typ)
			got, ok := s.Capability(id)
			want := KernelCapability{Backend: "scalar"}
			if mode != ModeScalar && nativeSIMDAvailable() {
				want = KernelCapability{HardwareSIMD: true, VectorBits: simd.Int64s{}.Len() * 64, Backend: "portable-simd"}
			}
			if !ok || got != want {
				t.Fatalf("mode=%s id=%s got=%+v want=%+v", mode, id, got, want)
			}
		}
		if mode != ModeScalar && nativeSIMDAvailable() {
			reference := newScalarSet(mode)
			installMinMaxSIMD(&reference, mode, simd.Int64s{}.Len()*64)
			scalar := New(ModeScalar)
			if reflect.ValueOf(s.d.minMaxInt32Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxInt32Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Int32")
			}
			if reflect.ValueOf(s.d.minMaxInt32Masked).Pointer() != reflect.ValueOf(reference.d.minMaxInt32Masked).Pointer() {
				t.Fatal("Int32 not native")
			}
			if reflect.ValueOf(s.d.minMaxInt64Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxInt64Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Int64")
			}
			if reflect.ValueOf(s.d.minMaxInt64Masked).Pointer() != reflect.ValueOf(reference.d.minMaxInt64Masked).Pointer() {
				t.Fatal("Int64 not native")
			}
			if reflect.ValueOf(s.d.minMaxUint32Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxUint32Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Uint32")
			}
			if reflect.ValueOf(s.d.minMaxUint32Masked).Pointer() != reflect.ValueOf(reference.d.minMaxUint32Masked).Pointer() {
				t.Fatal("Uint32 not native")
			}
			if reflect.ValueOf(s.d.minMaxUint64Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxUint64Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Uint64")
			}
			if reflect.ValueOf(s.d.minMaxUint64Masked).Pointer() != reflect.ValueOf(reference.d.minMaxUint64Masked).Pointer() {
				t.Fatal("Uint64 not native")
			}
			if reflect.ValueOf(s.d.minMaxFloat32Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxFloat32Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Float32")
			}
			if reflect.ValueOf(s.d.minMaxFloat32Masked).Pointer() != reflect.ValueOf(reference.d.minMaxFloat32Masked).Pointer() {
				t.Fatal("Float32 not native")
			}
			if reflect.ValueOf(s.d.minMaxFloat64Masked).Pointer() == reflect.ValueOf(scalar.d.minMaxFloat64Masked).Pointer() {
				t.Fatal("native dispatch retained scalar Float64")
			}
			if reflect.ValueOf(s.d.minMaxFloat64Masked).Pointer() != reflect.ValueOf(reference.d.minMaxFloat64Masked).Pointer() {
				t.Fatal("Float64 not native")
			}
		}
	}
}
