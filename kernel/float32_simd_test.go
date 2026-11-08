//go:build goexperiment.simd

package kernel

import (
	"gocolumn/memory"
	"simd"
	"testing"
)

func TestFloat32ForcedSIMDRegistrationAndDifferential(t *testing.T) {
	if simd.Emulated() {
		t.Skip("Go SIMD is emulated on this host")
	}
	set := New(ModeSIMD)
	id, _ := ComparisonID(memory.TypeFloat32, CompareGT)
	capability, ok := set.Capability(id)
	if !ok || capability.Backend != expectedComparisonBackend() || !capability.HardwareSIMD {
		t.Fatalf("Float32 forced backend not registered: %+v, ok=%t", capability, ok)
	}
	values := []float32{-2, -0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	valid := memory.Bitmap{0b11111110111}
	for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
		got := make(memory.Bitmap, 1)
		want := make(memory.Bitmap, 1)
		set.CompareFloat32(values, valid, got, 1, op)
		New(ModeScalar).CompareFloat32(values, valid, want, 1, op)
		if got[0] != want[0] {
			t.Fatalf("op=%s got=%064b want=%064b", op, got[0], want[0])
		}
	}
}

func TestFloat32ArithmeticHasHonestNativeCapability(t *testing.T) {
	if simd.Emulated() {
		t.Skip("Go SIMD is emulated on this host")
	}
	set := New(ModeSIMD)
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
		id, _ := ArithmeticID(memory.TypeFloat32, op)
		capability, ok := set.Capability(id)
		if !ok || capability.Backend != "portable-simd" || !capability.HardwareSIMD {
			t.Fatalf("op=%s capability=%+v ok=%t", op, capability, ok)
		}
	}
	a := []float32{1, 2, 3, 4, 5, 6, 7}
	b := []float32{2, 2, 2, 2, 2, 2, 2}
	valid := memory.Bitmap{0b11110111}
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
		got, want := make([]float32, len(a)), make([]float32, len(a))
		New(ModeSIMD).Float32Arithmetic(a, b, got, valid, op)
		New(ModeScalar).Float32Arithmetic(a, b, want, valid, op)
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("op=%s row=%d got=%v want=%v", op, i, got[i], want[i])
			}
		}
	}
}
