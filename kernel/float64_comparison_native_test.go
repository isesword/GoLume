package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"slices"
	"testing"
)

func TestFloat64NativeComparisonModesTailsAndNulls(t *testing.T) {
	values := []float64{float64(math.NaN()), float64(math.Inf(-1)), float64(math.Inf(1)), float64(math.Copysign(0, -1)), 0, -1, 1, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64}
	for _, rows := range []int{0, 1, 3, 4, 7, 8, 15, 16, 17, 63, 64, 65, 127, 129, 1025} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			left, right := make([]float64, rows), make([]float64, rows)
			mixed, nulls := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
			for row := range left {
				left[row], right[row] = values[row%len(values)], values[(row*7+2)%len(values)]
				if row%3 != 0 {
					memory.Set(mixed, row)
				}
			}
			scalar := New(ModeScalar)
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				set := New(mode)
				for _, av := range []memory.Bitmap{nil, mixed, nulls} {
					for _, bv := range []memory.Bitmap{nil, mixed, nulls} {
						for op := CompareGT; op <= CompareEQ; op++ {
							got, want := make(memory.Bitmap, memory.WordsFor(rows)+1), make(memory.Bitmap, memory.WordsFor(rows)+1)
							for i := range got {
								got[i], want[i] = ^uint64(0), ^uint64(0)
							}
							gn := set.CompareFloat64Columns(left, right, av, bv, got, op)
							wn := scalar.CompareFloat64Columns(left, right, av, bv, want, op)
							if gn != wn || !slices.Equal(got, want) {
								t.Fatalf("column mode=%s op=%s got=(%d,%x) want=(%d,%x)", mode, op, gn, got, wn, want)
							}
							for _, threshold := range values {
								gn = set.CompareFloat64(left, av, got, threshold, op)
								wn = scalar.CompareFloat64(left, av, want, threshold, op)
								if gn != wn || !slices.Equal(got, want) {
									t.Fatalf("literal mode=%s op=%s threshold=%v got=(%d,%x) want=(%d,%x)", mode, op, threshold, gn, got, wn, want)
								}
							}
						}
					}
				}
			}
		})
	}
}

func TestFloat64NativeComparisonsAllocateZero(t *testing.T) {
	left, right := make([]float64, 1025), make([]float64, 1025)
	valid, out := make(memory.Bitmap, memory.WordsFor(len(left))), make(memory.Bitmap, memory.WordsFor(len(left)))
	for i := range valid {
		valid[i] = 0xaaaaaaaaaaaaaaaa
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for op := CompareGT; op <= CompareEQ; op++ {
			if allocs := testing.AllocsPerRun(100, func() { set.CompareFloat64(left, valid, out, 0, op) }); allocs != 0 {
				t.Fatalf("literal mode=%s op=%s allocations=%g", mode, op, allocs)
			}
			if allocs := testing.AllocsPerRun(100, func() { set.CompareFloat64Columns(left, right, valid, valid, out, op) }); allocs != 0 {
				t.Fatalf("column mode=%s op=%s allocations=%g", mode, op, allocs)
			}
		}
	}
}
