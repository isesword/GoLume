package aggregate

import (
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"testing"
)

func TestQuantileExactRanks(t *testing.T) {
	rng := rand.New(rand.NewSource(18))
	ns := []uint64{0, 1, 2, 3, 1<<53 - 1, 1<<53 + 1, math.MaxInt64, math.MaxUint64}
	qs := []float64{0, math.SmallestNonzeroFloat64, .1, .25, .5, math.Nextafter(1, 0), 1}
	for i := 0; i < 2000; i++ {
		ns = append(ns, rng.Uint64())
		qs = append(qs, rng.Float64())
	}
	for i, n := range ns {
		for j := 0; j < 7; j++ {
			q := qs[(i+j)%len(qs)]
			for mode := QuantileNearest; mode <= QuantileEquiprobable; mode++ {
				got, err := PlanQuantile(n, q, mode)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					if got.Valid {
						t.Fatal(got)
					}
					continue
				}
				count := n - 1
				if mode == QuantileEquiprobable {
					count = n
				}
				rat := new(big.Rat).Mul(new(big.Rat).SetUint64(count), new(big.Rat).SetFloat64(q))
				f, rem := new(big.Int), new(big.Int)
				f.QuoRem(rat.Num(), rat.Denom(), rem)
				lo, hi := f.Uint64(), f.Uint64()
				switch mode {
				case QuantileNearest:
					if new(big.Int).Lsh(rem, 1).Cmp(rat.Denom()) >= 0 {
						lo++
						hi++
					}
				case QuantileHigher:
					if rem.Sign() != 0 {
						lo++
						hi++
					}
				case QuantileMidpoint, QuantileLinear:
					if rem.Sign() != 0 {
						hi++
					}
				case QuantileEquiprobable:
					if rem.Sign() != 0 {
						lo++
					}
					if lo > 0 {
						lo--
					}
					hi = lo
				}
				if !got.Valid || got.Lower != lo || got.Upper != hi || got.Upper >= n {
					t.Fatalf("n=%d q=%g mode=%d got=%+v want=%d,%d", n, q, mode, got, lo, hi)
				}
			}
		}
	}
}
func TestQuantileValidationAndInterpolation(t *testing.T) {
	for _, q := range []float64{math.NaN(), math.Inf(1), -.1, 1.1} {
		if _, err := PlanQuantile(1, q, QuantileLinear); err == nil {
			t.Fatal(q)
		}
	}
	if _, err := PlanQuantile(1, .5, 99); err == nil {
		t.Fatal("mode")
	}
	p := QuantileRanks{Lower: 0, Upper: 1, Weight: .5, Valid: true}
	for _, tc := range []struct{ a, b, want float64 }{{-math.MaxFloat64, math.MaxFloat64, 0}, {math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}, {math.Inf(1), math.Inf(1), math.Inf(1)}, {1, math.Inf(1), math.Inf(1)}, {math.Inf(-1), math.Inf(1), math.NaN()}, {1, 3, 2}, {1, math.NaN(), math.NaN()}} {
		got, ok, err := InterpolateQuantile(memory.TypeFloat64, math.Float64bits(tc.a), math.Float64bits(tc.b), p)
		v := math.Float64frombits(got)
		if err != nil || !ok || !(v == tc.want || math.IsNaN(v) && math.IsNaN(tc.want)) {
			t.Fatalf("%+v => %g %v %v", tc, v, ok, err)
		}
	}
	if _, valid, err := InterpolateQuantile(memory.TypeInt64, 0, 0, QuantileRanks{}); err != nil || valid {
		t.Fatal(valid, err)
	}
}
func BenchmarkQuantileRanks(b *testing.B) {
	for b.Loop() {
		_, _ = PlanQuantile(math.MaxUint64, .731, QuantileLinear)
	}
}
