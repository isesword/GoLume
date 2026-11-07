package aggregate

import (
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"testing"
)

// Exact rational oracle is independent of the online/merge recurrence.
func TestStatisticsIndependentIntegerMergeOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(8675309))
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for trial := 0; trial < 120; trial++ {
			n := 2 + rng.Intn(80)
			sum, squares := new(big.Int), new(big.Int)
			states := make([]MomentsState, 7)
			for i := range states {
				states[i], _ = NewMomentsState(typ)
			}
			for i := 0; i < n; i++ {
				bits := uint64(math.MaxUint64) - uint64(rng.Intn(2000))
				if typ == memory.TypeInt64 {
					bits = uint64(math.MaxInt64) - uint64(rng.Intn(2000))
					if trial%2 == 0 {
						bits = uint64(1)<<63 | uint64(rng.Intn(2000))
					}
				}
				x := new(big.Int).SetUint64(bits)
				if typ == memory.TypeInt64 {
					x.SetInt64(int64(bits))
				}
				sum.Add(sum, x)
				squares.Add(squares, new(big.Int).Mul(x, x))
				if err := states[i%len(states)].AddBits(bits, true); err != nil {
					t.Fatal(err)
				}
			}
			total, _ := NewMomentsState(typ)
			for _, i := range rng.Perm(len(states)) {
				if err := total.Merge(states[i]); err != nil {
					t.Fatal(err)
				}
			}
			num := new(big.Int).Sub(new(big.Int).Mul(squares, big.NewInt(int64(n))), new(big.Int).Mul(sum, sum))
			for _, ddof := range []uint64{0, 1} {
				exact := new(big.Rat).SetFrac(num, big.NewInt(int64(n)*(int64(n)-int64(ddof))))
				want, _ := exact.Float64()
				got, ok := total.Variance(ddof)
				if !ok || math.Abs(got-want) > math.Max(1, want)*2e-13 {
					t.Fatalf("type %v trial %d ddof %d got %.17g want %.17g", typ, trial, ddof, got, want)
				}
			}
		}
	}
}

func TestStatisticsIndependentExtremeMerge(t *testing.T) {
	for _, scale := range []float64{math.MaxFloat64, 1e154, 1e-150, math.SmallestNonzeroFloat64} {
		a, _ := NewMomentsState(memory.TypeFloat64)
		b, _ := NewMomentsState(memory.TypeFloat64)
		a.AddBits(math.Float64bits(-scale), true)
		b.AddBits(math.Float64bits(scale), true)
		if err := a.Merge(b); err != nil {
			t.Fatal(err)
		}
		std, ok := a.StdDev(0)
		if !ok || math.IsInf(std, 0) || math.IsNaN(std) || std != scale {
			t.Fatalf("scale %g: std %g", scale, std)
		}
		if _, err := RestoreMoments(a.Snapshot()); err != nil {
			t.Fatal(err)
		}
	}
}
