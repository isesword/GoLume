package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"testing"
)

type literalNumber interface {
	~int8 | ~int32 | ~int64 | ~uint32 | ~uint64
}
type literalCompare[T literalNumber] func([]T, memory.Bitmap, memory.Bitmap, T, CompareOp) int

func numericLiteralOracle[T literalNumber](a, b T, op CompareOp) bool {
	switch op {
	case CompareGT:
		return a > b
	case CompareGE:
		return a >= b
	case CompareLT:
		return a < b
	case CompareLE:
		return a <= b
	case CompareEQ:
		return a == b
	}
	panic("bad comparison")
}

func checkNumericLiterals[T literalNumber](t *testing.T, edge []T, compare func(Set) literalCompare[T]) {
	t.Helper()
	rng := rand.New(rand.NewPCG(42, 73))
	comparisons := make(map[Mode]literalCompare[T], 3)
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		comparisons[mode] = compare(New(mode))
	}
	lengths := []int{257, 1025, 4096 + 7}
	for n := 0; n <= 129; n++ {
		lengths = append(lengths, n)
	}
	for _, n := range lengths {
		v := make([]T, n)
		for i := range v {
			if i < len(edge)*len(edge) {
				v[i] = edge[i%len(edge)]
			} else {
				v[i] = T(rng.Uint64())
			}
		}
		for _, kind := range []string{"nil", "all", "mixed", "null"} {
			valid := column32GateValidity(n, kind)
			for _, threshold := range edge {
				for op := CompareGT; op <= CompareEQ; op++ {
					want := make(memory.Bitmap, memory.WordsFor(n)+1)
					want[len(want)-1] = 0xbeef
					count := 0
					for i, x := range v {
						if (valid == nil || valid[i/64]&(1<<uint(i%64)) != 0) && numericLiteralOracle(x, threshold, op) {
							want[i/64] |= 1 << uint(i%64)
							count++
						}
					}
					for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
						got := make(memory.Bitmap, len(want))
						for i := range got {
							got[i] = ^uint64(0)
						}
						got[len(got)-1] = 0xbeef
						if c := comparisons[mode](v, valid, got, threshold, op); c != count || !slices.Equal(got, want) {
							t.Fatalf("n=%d valid=%s op=%s mode=%s threshold=%v got=%x want=%x count=%d wantcount=%d", n, kind, op, mode, threshold, got, want, c, count)
						}
					}
				}
			}
		}
	}
	v := make([]T, 1025)
	valid, out := column32GateValidity(len(v), "mixed"), make(memory.Bitmap, memory.WordsFor(len(v)))
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		f := compare(New(mode))
		for op := CompareGT; op <= CompareEQ; op++ {
			if a := testing.AllocsPerRun(20, func() { f(v, valid, out, 0, op) }); a != 0 {
				t.Fatalf("%s/%s allocs=%g", mode, op, a)
			}
		}
	}
}
func TestNativeIntegerLiteralComparisons(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		checkNumericLiterals(t, []int8{-128, -127, -1, 0, 1, 126, 127}, func(s Set) literalCompare[int8] { return s.CompareInt8 })
	})
	t.Run("int32", func(t *testing.T) {
		checkNumericLiterals(t, []int32{math.MinInt32, -1, 0, 1, math.MaxInt32}, func(s Set) literalCompare[int32] { return s.CompareInt32 })
	})
	t.Run("int64", func(t *testing.T) {
		checkNumericLiterals(t, []int64{math.MinInt64, math.MinInt32, -1, 0, 1, math.MaxInt32, math.MaxInt64}, func(s Set) literalCompare[int64] { return s.CompareInt64 })
	})
	t.Run("uint32", func(t *testing.T) {
		checkNumericLiterals(t, []uint32{0, 1, 1<<31 - 1, 1 << 31, math.MaxUint32}, func(s Set) literalCompare[uint32] { return s.CompareUint32 })
	})
	t.Run("uint64", func(t *testing.T) {
		checkNumericLiterals(t, []uint64{0, 1, 1<<32 - 1, 1<<63 - 1, 1 << 63, math.MaxUint64}, func(s Set) literalCompare[uint64] { return s.CompareUint64 })
	})
}

func benchmarkNumericLiteral[T literalNumber](b *testing.B, selectCompare func(Set) literalCompare[T]) {
	for _, n := range []int{7, 1025, 65536} {
		for _, nullable := range []bool{false, true} {
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				b.Run(fmt.Sprintf("rows=%d/null=%t/%s", n, nullable, mode), func(b *testing.B) {
					v := make([]T, n)
					for i := range v {
						v[i] = T(i * 37)
					}
					var valid memory.Bitmap
					if nullable {
						valid = column32GateValidity(n, "mixed")
					}
					out := make(memory.Bitmap, memory.WordsFor(n))
					f := selectCompare(New(mode))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						f(v, valid, out, 17, CompareGT)
					}
					b.StopTimer()
					count := 0
					for _, w := range out {
						count += bits.OnesCount64(w)
					}
					b.ReportMetric(float64(count), "selected")
				})
			}
		}
	}
}
func BenchmarkNativeNumericLiteral(b *testing.B) {
	b.Run("int8", func(b *testing.B) {
		benchmarkNumericLiteral(b, func(s Set) literalCompare[int8] { return s.CompareInt8 })
	})
	b.Run("int32", func(b *testing.B) {
		benchmarkNumericLiteral(b, func(s Set) literalCompare[int32] { return s.CompareInt32 })
	})
	b.Run("int64", func(b *testing.B) {
		benchmarkNumericLiteral(b, func(s Set) literalCompare[int64] { return s.CompareInt64 })
	})
	b.Run("uint32", func(b *testing.B) {
		benchmarkNumericLiteral(b, func(s Set) literalCompare[uint32] { return s.CompareUint32 })
	})
	b.Run("uint64", func(b *testing.B) {
		benchmarkNumericLiteral(b, func(s Set) literalCompare[uint64] { return s.CompareUint64 })
	})
}
