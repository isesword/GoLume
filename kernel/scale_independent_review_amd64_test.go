//go:build goexperiment.simd && amd64

package kernel

import (
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand/v2"
	"simd/archsimd"
	"slices"
	"testing"
)

func TestScaleIndependentReview(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 required")
	}
	funcs := []struct {
		name string
		fn   func([]int64, memory.Bitmap, int64) int
	}{{"128", scaleamd64128}, {"256", scaleamd64256}}
	if archsimd.X86.AVX512() {
		funcs = append(funcs, struct {
			name string
			fn   func([]int64, memory.Bitmap, int64) int
		}{"512", scaleamd64512})
	}
	for _, f := range funcs {
		t.Run(f.name, func(t *testing.T) {
			r := rand.New(rand.NewPCG(20261108, 971))
			factors := []int64{math.MinInt64, math.MinInt64 + 1, -4294967297, -4294967296, -2147483648, -3, -2, -1, 0, 1, 2, 3, 2147483647, 4294967295, 4294967296, 4294967297, math.MaxInt64 - 1, math.MaxInt64}
			for iteration := 0; iteration < 20000; iteration++ {
				n := r.IntN(260)
				factor := int64(r.Uint64())
				if iteration%2 == 0 {
					factor = factors[r.IntN(len(factors))]
				}
				storage := make([]int64, n+18)
				for i := range storage {
					storage[i] = int64(r.Uint64())
				}
				values := storage[9 : 9+n]
				valid := make(memory.Bitmap, (n+63)/64)
				for i := range valid {
					valid[i] = r.Uint64()
				}
				if iteration%3 == 0 {
					valid = nil
				}
				lo, hi := int64(0), int64(0)
				if factor != 0 {
					lo, hi = math.MinInt64/factor, math.MaxInt64/factor
					if factor < 0 {
						lo, hi = hi, lo
					}
				}
				candidates := []int64{lo, hi, lo - 1, hi + 1, 0, 1, -1, math.MinInt64, math.MaxInt64}
				for i := range values {
					if r.IntN(8) != 0 {
						values[i] = candidates[r.IntN(len(candidates))]
					}
				}
				want := slices.Clone(storage)
				fail := -1
				var x, y, z big.Int
				y.SetInt64(factor)
				for i, v := range values {
					if valid != nil && valid[i/64]&(uint64(1)<<uint(i%64)) == 0 {
						continue
					}
					x.SetInt64(v)
					z.Mul(&x, &y)
					if !z.IsInt64() {
						fail = i
						break
					}
					want[9+i] = z.Int64()
				}
				got := f.fn(values, valid, factor)
				if got != fail || !slices.Equal(storage, want) {
					t.Fatalf("iteration=%d n=%d factor=%d failure got=%d want=%d\ngot=%v\nwant=%v", iteration, n, factor, got, fail, storage, want)
				}
			}
		})
	}
}

func TestScaleIndependentLateFailureReview(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 required")
	}
	funcs := []func([]int64, memory.Bitmap, int64) int{scaleamd64128, scaleamd64256}
	if archsimd.X86.AVX512() {
		funcs = append(funcs, scaleamd64512)
	}
	for width, fn := range funcs {
		for _, factor := range []int64{2, 3, -2, -3, 4294967297, -4294967297, math.MinInt64, math.MaxInt64} {
			lo, hi := int64(math.MinInt64)/factor, int64(math.MaxInt64)/factor
			if factor < 0 {
				lo, hi = hi, lo
			}
			for n := 1; n <= 137; n++ {
				for fail := -1; fail < n; fail++ {
					for _, dense := range []bool{false, true} {
						values := make([]int64, n)
						valid := make(memory.Bitmap, (n+63)/64)
						for i := range values {
							values[i] = []int64{lo, hi, 0}[i%3]
							if dense || i%5 != 1 {
								valid[i/64] |= uint64(1) << uint(i%64)
							} else {
								values[i] = math.MinInt64
							}
						}
						if fail >= 0 {
							values[fail] = hi + 1
							valid[fail/64] |= uint64(1) << uint(fail%64)
						}
						want := slices.Clone(values)
						end := fail
						if end < 0 {
							end = n
						}
						for i := 0; i < end; i++ {
							if valid[i/64]&(uint64(1)<<uint(i%64)) != 0 {
								want[i] *= factor
							}
						}
						if dense {
							valid = nil
						}
						if got := fn(values, valid, factor); got != fail || !slices.Equal(values, want) {
							t.Fatalf("width=%d factor=%d n=%d fail=%d got=%d", width, factor, n, fail, got)
						}
					}
				}
			}
		}
	}
}

func TestScaleIndependentMixedWordsReview(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 required")
	}
	funcs := []func([]int64, memory.Bitmap, int64) int{scaleamd64128, scaleamd64256}
	if archsimd.X86.AVX512() {
		funcs = append(funcs, scaleamd64512)
	}
	for width, fn := range funcs {
		for _, factor := range []int64{0, -1, 2, -3, 4294967297, math.MinInt64} {
			for n := 193; n <= 263; n++ {
				for fail := -1; fail < n; fail++ {
					valid := memory.Bitmap{^uint64(0), 0xaaaaaaaaaaaaaaaa, 0, ^uint64(0), ^uint64(0)}
					values := make([]int64, n)
					for i := range values {
						values[i] = int64(i % 2)
					}
					if fail >= 0 {
						values[fail] = math.MinInt64
					}
					want := slices.Clone(values)
					wantFail := scalarInt64Scale(want, valid, factor)
					if got := fn(values, valid, factor); got != wantFail || !slices.Equal(values, want) {
						t.Fatalf("width=%d factor=%d n=%d fail=%d got=%d want=%d", width, factor, n, fail, got, wantFail)
					}
				}
			}
		}
	}
}

func TestScaleIndependentEightLaneMasksReview(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 required")
	}
	funcs := []func([]int64, memory.Bitmap, int64) int{scaleamd64128, scaleamd64256}
	if archsimd.X86.AVX512() {
		funcs = append(funcs, scaleamd64512)
	}
	for width, fn := range funcs {
		for mask := uint64(0); mask < 256; mask++ {
			for _, factor := range []int64{0, -1, 3, -4294967297} {
				for fail := -1; fail < 8; fail++ {
					values := make([]int64, 73)
					valid := memory.Bitmap{mask * 0x0101010101010101, mask | 256}
					for i := range values {
						values[i] = int64(i % 2)
						if valid[i/64]&(uint64(1)<<uint(i%64)) == 0 {
							values[i] = math.MinInt64
						}
					}
					if fail >= 0 {
						values[64+fail] = math.MinInt64
					}
					want := slices.Clone(values)
					wantFail := scalarInt64Scale(want, valid, factor)
					if got := fn(values, valid, factor); got != wantFail || !slices.Equal(values, want) {
						t.Fatalf("width=%d mask=%x factor=%d fail=%d got=%d want=%d", width, mask, factor, fail, got, wantFail)
					}
				}
			}
		}
	}
}
