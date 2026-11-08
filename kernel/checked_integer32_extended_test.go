package kernel

import (
	"fmt"
	"gocolumn/memory"
	"slices"
	"testing"
)

func TestCheckedInteger32ExtendedReference(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(fmt.Sprint(mode)+"/signed", func(t *testing.T) { reviewIntegerCases(t, true, s.Int32Arithmetic) })
		t.Run(fmt.Sprint(mode)+"/unsigned", func(t *testing.T) { reviewIntegerCases(t, false, s.Uint32Arithmetic) })
		reviewPartial(t, true, s.Int32Arithmetic)
		reviewPartial(t, false, s.Uint32Arithmetic)
		reviewCheckedBounds(t, s.Int32Arithmetic)
		reviewCheckedBounds(t, s.Uint32Arithmetic)
	}
}

func checkInteger32ShortAliases[T checkedInteger32](t *testing.T, run, scalar func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	t.Helper()
	for n := 0; n < 16; n++ {
		for _, shift := range []int{-3, -1, 0, 1, 3} {
			for side := 0; side < 3; side++ {
				for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
					back := make([]T, 32)
					other := make([]T, n)
					for i := range back {
						back[i] = T(uint64(i+1) * 0xa28f64e581d5f7c1)
					}
					for i := range other {
						other[i] = T(i%5 + 1)
					}
					want := slices.Clone(back)
					a, b, wa, wb := back[8:8+n], other, want[8:8+n], other
					if side == 1 {
						a, b, wa, wb = b, a, wb, wa
					}
					if side == 2 {
						b = back[9 : 9+n]
						wb = want[9 : 9+n]
					}
					expected := scalar(wa, wb, want[8+shift:8+shift+n], nil, nil, op)
					got := run(a, b, back[8+shift:8+shift+n], nil, nil, op)
					if got != expected || !slices.Equal(back, want) {
						t.Fatalf("short alias n%d shift%d side%d op%s first%d want%d", n, shift, side, op, got, expected)
					}
				}
			}
		}
	}
}

func TestCheckedInteger32ShortAliases(t *testing.T) {
	scalar := New(ModeScalar)
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		checkInteger32ShortAliases(t, s.Int32Arithmetic, scalar.Int32Arithmetic)
		checkInteger32ShortAliases(t, s.Uint32Arithmetic, scalar.Uint32Arithmetic)
	}
}

func checkInteger32ShortMaskedAliases[T checkedInteger32](t *testing.T, run, scalar func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	t.Helper()
	for n := 0; n < 16; n++ {
		for _, shift := range []int{-3, -1, 0, 1, 3} {
			for side := 0; side < 3; side++ {
				for _, valid := range []memory.Bitmap{nil, {^uint64(0)}, {0xaaaaaaaaaaaaaaaa}, {0}} {
					for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
						back := make([]T, 32)
						other := make([]T, n)
						for i := range back {
							back[i] = T(uint64(i+1) * 0xa28f64e581d5f7c1)
						}
						for i := range other {
							other[i] = T(i%5 + 1)
						}
						want := slices.Clone(back)
						a, b, wa, wb := back[8:8+n], other, want[8:8+n], other
						if side == 1 {
							a, b, wa, wb = b, a, wb, wa
						}
						if side == 2 {
							b = back[9 : 9+n]
							wb = want[9 : 9+n]
						}
						over, wantOver := memory.Bitmap{123, 456}, memory.Bitmap{123, 456}
						expected := scalar(wa, wb, want[8+shift:8+shift+n], valid, wantOver, op)
						got := run(a, b, back[8+shift:8+shift+n], valid, over, op)
						if got != expected || !slices.Equal(back, want) || !slices.Equal(over, wantOver) {
							t.Fatalf("short masked alias n%d shift%d side%d op%s first%d want%d", n, shift, side, op, got, expected)
						}
					}
				}
			}
		}
	}
}

func TestCheckedInteger32ShortMaskedAliases(t *testing.T) {
	scalar := New(ModeScalar)
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		s := New(mode)
		checkInteger32ShortMaskedAliases(t, s.Int32Arithmetic, scalar.Int32Arithmetic)
		checkInteger32ShortMaskedAliases(t, s.Uint32Arithmetic, scalar.Uint32Arithmetic)
	}
}

func TestCheckedInteger32EmptyMasks(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			for _, valid := range []memory.Bitmap{nil, {}, {123}} {
				out, uout, over := []int32{123}, []uint32{456}, memory.Bitmap{789}
				if got := s.Int32Arithmetic(nil, nil, out, valid, over, op); got != -1 || out[0] != 123 || over[0] != 789 {
					t.Fatalf("empty signed %s/%s: %d %v %v", mode, op, got, out, over)
				}
				if got := s.Uint32Arithmetic(nil, nil, uout, valid, over, op); got != -1 || uout[0] != 456 || over[0] != 789 {
					t.Fatalf("empty unsigned %s/%s: %d %v %v", mode, op, got, uout, over)
				}
			}
		}
	}
}
