package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

// Independent wide-integer oracle checks both the first failure and the precise
// prefix-only mutation contract, including values hidden beneath NULLs.
func scaleReference(a []int64, valid memory.Bitmap, factor int64) int {
	var x, y, z big.Int
	y.SetInt64(factor)
	for i, v := range a {
		if valid != nil && !memory.IsSet(valid, i) {
			continue
		}
		x.SetInt64(v)
		z.Mul(&x, &y)
		if !z.IsInt64() {
			return i
		}
		a[i] = z.Int64()
	}
	return -1
}

func TestScaleWideOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(719))
	factors := []int64{0, 1, -1, 2, -2, 3, -3, 1000, -1000, 1000000, -1000000, 1 << 31, (1 << 32) - 1, 1 << 32, -(1 << 32), math.MaxInt64, math.MinInt64}
	for i := 0; i < 30; i++ {
		factors = append(factors, int64(rng.Uint64()))
	}
	for _, n := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 127, 128, 129, 257} {
		for _, factor := range factors {
			for pattern := 0; pattern < 6; pattern++ {
				input := make([]int64, n+4)
				for i := range input {
					input[i] = int64(rng.Uint64())
				}
				if pattern != 0 {
					for i := 2; i < n+2; i++ {
						input[i] = int64(i%3 - 1)
					}
				}
				valid := make(memory.Bitmap, memory.WordsFor(n))
				for i := 0; i < n; i++ {
					if pattern < 2 || pattern == 2 && i%3 != 0 || pattern == 3 && i%17 == 0 {
						memory.Set(valid, i)
					}
				}
				if n > 0 {
					valid[len(valid)-1] |= ^memory.TailMask(n)
				}
				if pattern == 5 {
					valid = nil
				}
				// Random large inputs exercise failures; safe prefixes and a late poison
				// exercise every vector lane with all signs and extreme factors.
				for _, badAt := range []int{-1, n / 2, n - 1} {
					base := append([]int64(nil), input...)
					if badAt >= 0 {
						base[badAt+2] = math.MinInt64
					}
					want := append([]int64(nil), base...)
					wantRow := scaleReference(want[2:2+n], valid, factor)
					for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
						got := append([]int64(nil), base...)
						row := New(mode).ScaleInt64(got[2:2+n], valid, factor)
						if row != wantRow || !reflect.DeepEqual(got, want) {
							t.Fatalf("n%d factor%d pattern%d bad%d mode%s row%d want%d", n, factor, pattern, badAt, mode, row, wantRow)
						}
					}
				}
			}
		}
	}
}

func TestScaleEveryOverflowPosition(t *testing.T) {
	for _, factor := range []int64{2, -2, 1000, -1000, 1000000, -1000000, math.MaxInt64, math.MinInt64} {
		for n := 1; n <= 130; n++ {
			for badAt := 0; badAt < n; badAt++ {
				input := make([]int64, n)
				valid := make(memory.Bitmap, memory.WordsFor(n))
				for i := range input {
					input[i] = 1
					if i%5 != 0 {
						memory.Set(valid, i)
					} else {
						input[i] = math.MinInt64
					}
				}
				input[badAt] = math.MaxInt64
				memory.Set(valid, badAt)
				want := append([]int64(nil), input...)
				wantRow := scaleReference(want, valid, factor)
				got := append([]int64(nil), input...)
				gotRow := New(ModeSIMD).ScaleInt64(got, valid, factor)
				if gotRow != wantRow || !reflect.DeepEqual(got, want) {
					t.Fatalf("n%d factor%d row%d got%d want%d", n, factor, badAt, gotRow, wantRow)
				}
			}
		}
	}
}

func TestScaleAllocation(t *testing.T) {
	const n = 129
	base, out := make([]int64, n), make([]int64, n)
	valid := memory.Bitmap{0x5555555555555555, 0, 1}
	for i := range base {
		base[i] = int64(i + 1)
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, factor := range []int64{-1000, 0, 1, 1000} {
			if a := testing.AllocsPerRun(30, func() { copy(out, base); s.ScaleInt64(out, valid, factor) }); a != 0 {
				t.Fatalf("%s factor%d allocs%g", mode, factor, a)
			}
		}
	}
}

func BenchmarkScaleMatrix(b *testing.B) {
	for _, n := range []int{7, 65, 4096, 65536} {
		base, out := make([]int64, n), make([]int64, n)
		for i := range base {
			base[i] = int64(i + 1)
		}
		for _, pattern := range []string{"dense", "full", "nullable", "sparse", "null"} {
			var valid memory.Bitmap
			if pattern != "dense" {
				valid = make(memory.Bitmap, memory.WordsFor(n))
				for i := range base {
					if pattern == "full" || pattern == "nullable" && i%5 != 0 || pattern == "sparse" && i%19 == 0 {
						memory.Set(valid, i)
					}
				}
			}
			for _, mode := range []Mode{ModeScalar, ModeSIMD} {
				set := New(mode)
				b.Run(fmt.Sprintf("n%d/%s/%s", n, pattern, mode), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(n * 8))
					for b.Loop() {
						copy(out, base)
						if set.ScaleInt64(out, valid, 1000) != -1 {
							b.Fatal("overflow")
						}
					}
				})
			}
		}
	}
}
