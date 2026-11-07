package kernel

import (
	"gocolumn/memory"
	"testing"
)

func TestMathReviewExhaustiveInt8Division(t *testing.T) {
	s := New(ModeScalar)
	a, b, o := []int8{0}, []int8{0}, []int8{0}
	failures := memory.Bitmap{0}
	for x := -128; x <= 127; x++ {
		for y := -128; y <= 127; y++ {
			a[0], b[0] = int8(x), int8(y)
			for _, op := range []MathBinaryOp{MathMod, MathFloorDiv} {
				n := s.MathBinaryInt8(a, b, o, nil, failures, op)
				if y == 0 {
					if n != 1 || failures[0] != 1 {
						t.Fatal("zero divisor", x, op, n, failures)
					}
					continue
				}
				q, r := x/y, x%y
				if r != 0 && (r < 0) != (y < 0) {
					q--
					r += y
				}
				want := int8(q)
				if op == MathMod {
					want = int8(r)
				}
				if n != 0 || failures[0] != 0 || o[0] != want {
					t.Fatalf("%d,%d op=%v got=%d failures=%v want=%d", x, y, op, o[0], failures, want)
				}
			}
		}
	}
}
