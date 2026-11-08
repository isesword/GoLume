//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"reflect"
	"simd"
	"testing"
)

func TestNarrowMinMaxNativeDispatch(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		scalar := New(ModeScalar)
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
			id, _ := MinMaxID(typ)
			got, ok := s.Capability(id)
			want := KernelCapability{Backend: "scalar"}
			if mode != ModeScalar && nativeSIMDAvailable() && (runtimeArch() == "amd64" || runtimeArch() == "arm64") {
				want = KernelCapability{HardwareSIMD: true, VectorBits: (simd.Int32s{}).Len() * 32, Backend: "archsimd-widen-minmax"}
			}
			if !ok || got != want {
				t.Fatalf("mode=%s id=%s got=%+v want=%+v", mode, id, got, want)
			}
			var native, fallback any
			switch typ {
			case memory.TypeInt8:
				native, fallback = s.d.minMaxInt8Masked, scalar.d.minMaxInt8Masked
			case memory.TypeInt16:
				native, fallback = s.d.minMaxInt16Masked, scalar.d.minMaxInt16Masked
			case memory.TypeUint8:
				native, fallback = s.d.minMaxUint8Masked, scalar.d.minMaxUint8Masked
			case memory.TypeUint16:
				native, fallback = s.d.minMaxUint16Masked, scalar.d.minMaxUint16Masked
			}
			if different := reflect.ValueOf(native).Pointer() != reflect.ValueOf(fallback).Pointer(); different != want.HardwareSIMD {
				t.Fatalf("dispatch does not match capability: mode=%s id=%s capability=%+v", mode, id, want)
			}
		}
	}
}
