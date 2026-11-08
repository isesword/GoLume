package kernel

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestFloat32NativeArithmeticModesTailsAndNulls(t *testing.T) {
	seeds := []float32{float32(math.NaN()), float32(math.Inf(-1)), float32(math.Inf(1)), float32(math.Copysign(0, -1)), 0, -1, 1, math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32, math.MaxFloat32, 2.5}
	for _, rows := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 63, 64, 65, 129, 1025} {
		left, right := make([]float32, rows), make([]float32, rows)
		mixed, nulls := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
		for row := range left {
			left[row], right[row] = seeds[row%len(seeds)], seeds[(row*7+3)%len(seeds)]
			if row%3 != 0 {
				memory.Set(mixed, row)
			}
		}
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			set := New(mode)
			for _, valid := range []memory.Bitmap{nil, mixed, nulls} {
				for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
					got, want := make([]float32, rows+1), make([]float32, rows+1)
					got[rows], want[rows] = 12345, 12345
					New(ModeScalar).Float32Arithmetic(left, right, want, valid, op)
					set.Float32Arithmetic(left, right, got, valid, op)
					for row := range got {
						if !equalMathFloat(float64(got[row]), float64(want[row])) {
							t.Fatalf("rows=%d mode=%s op=%s row=%d got=%v want=%v", rows, mode, op, row, got[row], want[row])
						}
					}
					// Both exact input aliases are supported by the arithmetic kernels.
					for _, aliasLeft := range []bool{false, true} {
						a, b := append([]float32(nil), left...), append([]float32(nil), right...)
						alias := b
						if aliasLeft {
							alias = a
						}
						set.Float32Arithmetic(a, b, alias, valid, op)
						for row := range alias {
							if !equalMathFloat(float64(alias[row]), float64(want[row])) {
								t.Fatalf("alias rows=%d mode=%s op=%s row=%d", rows, mode, op, row)
							}
						}
					}
				}
			}
		}
	}
}
