package kernel

import (
	"gocolumn/memory"
	"slices"
	"testing"
)

func reviewCheckedBounds[T reviewInteger](t *testing.T, run func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	const n = 65
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
		for _, bad := range []int{0, 1, 2} {
			a, b, out := make([]T, n), make([]T, n), make([]T, n+3)
			over := memory.Bitmap{123, 456, 789}
			for i := range out {
				out[i] = 117
			}
			want := slices.Clone(out)
			ow := slices.Clone(over)
			dest := out
			ob := over
			switch bad {
			case 0:
				dest = out[:n-1]
			case 1:
				b = b[:n-1]
			case 2:
				ob = over[:1]
			}
			didPanic := false
			func() { defer func() { didPanic = recover() != nil }(); run(a, b, dest, nil, ob, op) }()
			if !didPanic || !slices.Equal(out, want) || !slices.Equal(over, ow) {
				t.Fatalf("preflight op%s bad%d", op, bad)
			}
		}
	}
}
func TestIndependentCheckedPreflight(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewCheckedBounds(t, s.Int64Arithmetic)
		reviewCheckedBounds(t, s.Uint64Arithmetic)
	}
}
