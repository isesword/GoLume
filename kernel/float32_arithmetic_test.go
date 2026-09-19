package kernel

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestFloat32ArithmeticSemanticsAndValidity(t *testing.T) {
	a := []float32{1, 2, math.SmallestNonzeroFloat32, -0, float32(math.Inf(1)), float32(math.NaN()), 7}
	b := []float32{2, 3, 4, 5, float32(math.Inf(1)), 1, 0}
	valid := memory.Bitmap{0b11110111}
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
		got := make([]float32, len(a))
		New(ModeScalar).Float32Arithmetic(a, b, got, valid, op)
		want := make([]float32, len(a))
		for i := range a {
			if valid[i/64]&(uint64(1)<<uint(i%64)) == 0 {
				continue
			}
			switch op {
			case ArithmeticAdd:
				want[i] = a[i] + b[i]
			case ArithmeticSub:
				want[i] = a[i] - b[i]
			case ArithmeticMul:
				want[i] = a[i] * b[i]
			case ArithmeticDiv:
				want[i] = a[i] / b[i]
			}
		}
		for i := range got {
			if math.IsNaN(float64(got[i])) && math.IsNaN(float64(want[i])) {
				continue
			}
			if got[i] != want[i] {
				t.Fatalf("op=%s row=%d got=%v want=%v", op, i, got[i], want[i])
			}
		}
	}
	if id, ok := ArithmeticID(memory.TypeFloat32, ArithmeticDiv); !ok {
		t.Fatal("Float32 division must have a kernel id")
	} else if id.String() != "arithmetic-float32-div" {
		t.Fatalf("Float32 arithmetic widened in kernel id: %s", id)
	}
}
