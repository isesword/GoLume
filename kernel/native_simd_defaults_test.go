//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestAutoSelectsAvailableNativeSIMD(t *testing.T) {
	t.Logf("arch=%s emulated=%t vector_bits=%d", runtimeArch(), simd.Emulated(), simd.Int64s{}.Len()*64)
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		wantNative := mode != ModeScalar && !simd.Emulated()
		check := func(id KernelID, backend string) {
			t.Helper()
			capability, ok := set.Capability(id)
			if !ok || capability.HardwareSIMD != wantNative || capability.Emulated {
				t.Fatalf("mode=%s id=%s capability=%+v ok=%t", mode, id, capability, ok)
			}
			if wantNative && (capability.Backend != backend || capability.VectorBits != simd.Int64s{}.Len()*64) {
				t.Fatalf("native mode=%s id=%s capability=%+v", mode, id, capability)
			}
		}
		for op := CompareGT; op <= CompareEQ; op++ {
			id, _ := ComparisonID(memory.TypeFloat32, op)
			check(id, expectedComparisonBackend())
			for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeFloat64} {
				capability, ok := set.ColumnComparisonCapability(typ, op)
				if !ok || capability.HardwareSIMD != wantNative || capability.Emulated {
					t.Fatalf("mode=%s column type=%s op=%s capability=%+v ok=%t", mode, typ, op, capability, ok)
				}
				if wantNative && (capability.Backend != expectedComparisonBackend() || capability.VectorBits != simd.Int32s{}.Len()*32) {
					t.Fatalf("native column mode=%s type=%s op=%s capability=%+v", mode, typ, op, capability)
				}
			}
		}
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
			id, _ := ArithmeticID(memory.TypeFloat64, op)
			check(id, "portable-simd")
		}
		for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				id, _ := MathUnaryID(typ, op)
				check(id, "portable-simd")
			}
		}
		if mode == ModeScalar || simd.Emulated() {
			for _, id := range KernelIDs() {
				capability, _ := set.Capability(id)
				if capability != (KernelCapability{Backend: "scalar"}) {
					t.Fatalf("fallback mode=%s id=%s capability=%+v", mode, id, capability)
				}
			}
		}
	}
}

// Auto must not silently drop a native implementation selected by explicit SIMD.
func TestAutoMatchesExplicitNativeCapabilities(t *testing.T) {
	auto, explicit := New(ModeAuto), New(ModeSIMD)
	for _, id := range KernelIDs() {
		got, _ := auto.Capability(id)
		want, _ := explicit.Capability(id)
		if want.HardwareSIMD && got != want {
			t.Fatalf("id=%s auto=%+v explicit=%+v", id, got, want)
		}
	}
}
