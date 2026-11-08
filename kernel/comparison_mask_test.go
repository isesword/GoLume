package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"slices"
	"testing"
)

type maskNumber interface {
	~int8 | ~int32 | ~int64 | ~uint32 | ~uint64 | ~float32 | ~float64
}
type maskLiteral[T maskNumber] func([]T, memory.Bitmap, memory.Bitmap, T, CompareOp) int
type maskColumns[T maskNumber] func([]T, []T, memory.Bitmap, memory.Bitmap, memory.Bitmap, CompareOp) int

// Every bit position is exercised in each of two words plus a tail. Offset
// slices intentionally avoid vector alignment. This detects narrow ToBits
// shifts that discard bits above 7/15/31 as well as reversed lane ordering.
func checkMaskOneHot[T maskNumber](t *testing.T, literal maskLiteral[T], columns maskColumns[T]) {
	t.Helper()
	const n = 129
	a, b := make([]T, n+1)[1:], make([]T, n+1)[1:]
	for i := range b {
		b[i] = 1
	}
	for op := CompareGT; op <= CompareEQ; op++ {
		baseline, hot := T(0), T(1)
		if op == CompareGT {
			hot = 2
		}
		if op == CompareLT {
			baseline, hot = 2, 0
		}
		if op == CompareLE {
			baseline = 2
		}
		for i := range a {
			a[i] = baseline
		}
		for row := range a {
			a[row] = hot
			want := memory.Bitmap{0, 0, 0, 0xbeef}
			want[row/64] = uint64(1) << uint(row%64)
			got := memory.Bitmap{^uint64(0), ^uint64(0), ^uint64(0), 0xbeef}
			if count := literal(a, nil, got, 1, op); count != 1 || !slices.Equal(got, want) {
				t.Fatalf("literal op=%s row=%d count=%d got=%x want=%x", op, row, count, got, want)
			}
			if columns != nil {
				got = memory.Bitmap{^uint64(0), ^uint64(0), ^uint64(0), 0xbeef}
				if count := columns(a, b, nil, nil, got, op); count != 1 || !slices.Equal(got, want) {
					t.Fatalf("columns op=%s row=%d count=%d got=%x want=%x", op, row, count, got, want)
				}
			}
			a[row] = baseline
		}
	}
	// A completely invalid first word, disjoint validity in the second,
	// and dirty high tail bits must clear logical output but preserve suffix.
	av, bv := memory.Bitmap{0, 0xaaaaaaaaaaaaaaaa, ^uint64(0)}, memory.Bitmap{^uint64(0), 0x5555555555555555, ^uint64(0)}
	for i := range a {
		a[i] = 1
	}
	for _, valid := range []memory.Bitmap{nil, av, {0, 0, 0}} {
		got, want := memory.Bitmap{^uint64(0), ^uint64(0), ^uint64(0), 0xbeef}, memory.Bitmap{0, 0, 0, 0xbeef}
		count := 0
		for row := range a {
			if valid == nil || valid[row/64]&(1<<uint(row%64)) != 0 {
				want[row/64] |= 1 << uint(row%64)
				count++
			}
		}
		if c := literal(a, valid, got, 1, CompareEQ); c != count || !slices.Equal(got, want) {
			t.Fatalf("null literal count=%d want=%d got=%x want=%x", c, count, got, want)
		}
	}
	if columns != nil {
		got := memory.Bitmap{^uint64(0), ^uint64(0), ^uint64(0), 0xbeef}
		if c := columns(a, b, av, bv, got, CompareEQ); c != 1 || !slices.Equal(got, memory.Bitmap{0, 0, 1, 0xbeef}) {
			t.Fatalf("disjoint validity count=%d got=%x", c, got)
		}
	}
	out := make(memory.Bitmap, 3)
	for op := CompareGT; op <= CompareEQ; op++ {
		if allocs := testing.AllocsPerRun(20, func() {
			literal(a, av, out, 1, op)
			if columns != nil {
				columns(a, b, av, bv, out, op)
			}
		}); allocs != 0 {
			t.Fatalf("op=%s allocs=%g", op, allocs)
		}
	}
}

func TestComparisonMaskOneHotAndNullWords(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			t.Run("int8", func(t *testing.T) { checkMaskOneHot[int8](t, s.CompareInt8, nil) })
			t.Run("int32", func(t *testing.T) { checkMaskOneHot[int32](t, s.CompareInt32, s.CompareInt32Columns) })
			t.Run("int64", func(t *testing.T) { checkMaskOneHot[int64](t, s.CompareInt64, nil) })
			t.Run("uint32", func(t *testing.T) { checkMaskOneHot[uint32](t, s.CompareUint32, s.CompareUint32Columns) })
			t.Run("uint64", func(t *testing.T) { checkMaskOneHot[uint64](t, s.CompareUint64, nil) })
			t.Run("float32", func(t *testing.T) { checkMaskOneHot[float32](t, s.CompareFloat32, s.CompareFloat32Columns) })
			t.Run("float64", func(t *testing.T) { checkMaskOneHot[float64](t, s.CompareFloat64, s.CompareFloat64Columns) })
		})
	}
}

func checkMaskIEEE[T ~float32 | ~float64](t *testing.T, edge []T, literal maskLiteral[T], columns maskColumns[T]) {
	t.Helper()
	for n := 0; n <= 129; n++ {
		a, b := make([]T, n+1)[1:], make([]T, n+1)[1:]
		for i := range a {
			a[i], b[i] = edge[i%len(edge)], edge[(i/len(edge))%len(edge)]
		}
		for _, av := range []memory.Bitmap{nil, column32GateValidity(n, "mixed"), column32GateValidity(n, "null")} {
			for _, bv := range []memory.Bitmap{nil, column32GateValidity(n, "mixed")} {
				for op := CompareGT; op <= CompareEQ; op++ {
					for _, threshold := range edge {
						for _, col := range []bool{false, true} {
							words := memory.WordsFor(n)
							got, want := make(memory.Bitmap, words+1), make(memory.Bitmap, words+1)
							for i := range got {
								got[i] = ^uint64(0)
							}
							got[words], want[words] = 0xbeef, 0xbeef
							count := 0
							for i, x := range a {
								y := threshold
								if col {
									y = b[i]
								}
								valid := (av == nil || av[i/64]&(1<<uint(i%64)) != 0) && (!col || bv == nil || bv[i/64]&(1<<uint(i%64)) != 0)
								match := false
								switch op {
								case CompareGT:
									match = x > y
								case CompareGE:
									match = x >= y
								case CompareLT:
									match = x < y
								case CompareLE:
									match = x <= y
								case CompareEQ:
									match = x == y
								}
								if valid && match {
									want[i/64] |= 1 << uint(i%64)
									count++
								}
							}
							var c int
							if col {
								c = columns(a, b, av, bv, got, op)
							} else {
								c = literal(a, av, got, threshold, op)
							}
							if c != count || !slices.Equal(got, want) {
								t.Fatalf("n=%d op=%s columns=%t threshold=%v count=%d want=%d got=%x want=%x", n, op, col, threshold, c, count, got, want)
							}
						}
					}
				}
			}
		}
	}
}

func TestComparisonMaskIEEEAndTails(t *testing.T) {
	edge64 := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1, math.Inf(1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8000000000001)}
	edge32 := []float32{float32(math.Inf(-1)), -1, float32(math.Copysign(0, -1)), 0, math.SmallestNonzeroFloat32, 1, float32(math.Inf(1)), math.Float32frombits(0x7fc00001), math.Float32frombits(0x7f800001), math.Float32frombits(0xffc00001)}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(fmt.Sprint(mode)+"/float32", func(t *testing.T) { checkMaskIEEE[float32](t, edge32, s.CompareFloat32, s.CompareFloat32Columns) })
		t.Run(fmt.Sprint(mode)+"/float64", func(t *testing.T) { checkMaskIEEE[float64](t, edge64, s.CompareFloat64, s.CompareFloat64Columns) })
	}
}
