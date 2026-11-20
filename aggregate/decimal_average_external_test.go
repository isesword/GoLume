package aggregate_test

import (
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
)

func TestN19PublicAverageMaxCount(t *testing.T) {
	max, err := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, negative := range []bool{false, true} {
		v := max
		if negative {
			v = v.Negate()
		}
		var power, total aggregate.DecimalSumState
		if e := power.Add(v); e != nil {
			t.Fatal(e)
		}
		// Construct MaxInt64 identical values via public Merge, without private limbs.
		for bit := 0; bit < 63; bit++ {
			if e := total.Merge(power); e != nil {
				t.Fatal(e)
			}
			if bit < 62 {
				if e := power.Merge(power); e != nil {
					t.Fatal(e)
				}
			}
		}
		if total.Count() != math.MaxInt64 {
			t.Fatal(total.Count())
		}
		before := total
		for _, scale := range []int32{0, 38} {
			got, valid, e := total.FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: scale, Precision: 38, Scale: scale, Mode: aggregate.DecimalAverageHalfEven})
			if e != nil || !valid || got != v || total != before {
				t.Fatalf("extreme count %v %v %v", got, valid, e)
			}
		}
		if _, _, e := total.Finalize(); !errors.Is(e, aggregate.ErrDecimalSumOverflow) {
			t.Fatal("SUM must remain overflow", e)
		}
		if e := total.Add(decimal128.FromI64(0)); !errors.Is(e, aggregate.ErrDecimalSumCount) || total != before {
			t.Fatal(e)
		}
	}
}

func TestN19PublicAveragePartitionNotMeanOfMeans(t *testing.T) {
	var left, right aggregate.DecimalSumState
	_ = left.Add(decimal128.FromI64(0))
	for i := 0; i < 3; i++ {
		_ = right.Add(decimal128.FromI64(10))
	}
	opt := aggregate.DecimalAverageOptions{InputScale: 0, Precision: 3, Scale: 1, Mode: aggregate.DecimalAverageHalfEven}
	whole := left
	if e := whole.Merge(right); e != nil {
		t.Fatal(e)
	}
	got, valid, e := whole.FinalizeAverage(opt)
	if e != nil || !valid || got.BigInt().Cmp(big.NewInt(75)) != 0 {
		t.Fatal(got, valid, e)
	}
	a, _, _ := left.FinalizeAverage(opt)
	b, _, _ := right.FinalizeAverage(opt)
	naive := new(big.Int).Add(a.BigInt(), b.BigInt())
	naive.Quo(naive, big.NewInt(2))
	if naive.Cmp(got.BigInt()) == 0 {
		t.Fatal("fixture must distinguish mean of means")
	}
}

func TestN19PublicAverageExplicitModes(t *testing.T) {
	modes := []aggregate.DecimalAverageRoundingMode{aggregate.DecimalAverageTowardZero, aggregate.DecimalAverageAwayFromZero, aggregate.DecimalAverageFloor, aggregate.DecimalAverageCeil, aggregate.DecimalAverageHalfEven, aggregate.DecimalAverageHalfAwayFromZero}
	for _, tc := range []struct {
		a, b int64
		want [6]int64
	}{
		{2, 3, [6]int64{2, 3, 2, 3, 2, 3}}, {-2, -3, [6]int64{-2, -3, -3, -2, -2, -3}},
		{3, 4, [6]int64{3, 4, 3, 4, 4, 4}}, {-3, -4, [6]int64{-3, -4, -4, -3, -4, -4}},
		{1, 2, [6]int64{1, 2, 1, 2, 2, 2}},
	} {
		var s aggregate.DecimalSumState
		_ = s.Add(decimal128.FromI64(tc.a))
		_ = s.Add(decimal128.FromI64(tc.b))
		before := s
		for i, m := range modes {
			v, ok, e := s.FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: 0, Precision: 2, Scale: 0, Mode: m})
			if e != nil || !ok || v != decimal128.FromI64(tc.want[i]) || s != before {
				t.Fatalf("%d %d mode%d got%v %v %v", tc.a, tc.b, m, v, ok, e)
			}
		}
	}
	// A final rounding carry exceeds target precision even though truncated 9 fits.
	var s aggregate.DecimalSumState
	_ = s.Add(decimal128.FromI64(95))
	opt := aggregate.DecimalAverageOptions{InputScale: 1, Precision: 1, Scale: 0, Mode: aggregate.DecimalAverageHalfEven}
	before := s
	if _, ok, e := s.FinalizeAverage(opt); ok || !errors.Is(e, aggregate.ErrDecimalAverageOverflow) || s != before {
		t.Fatal(ok, e)
	}
	opt.Mode = aggregate.DecimalAverageTowardZero
	if v, ok, e := s.FinalizeAverage(opt); e != nil || !ok || v != decimal128.FromI64(9) {
		t.Fatal(v, ok, e)
	}
}

func TestN19PublicAverageValidateBeforeNull(t *testing.T) {
	var empty, zero aggregate.DecimalSumState
	_ = zero.Add(decimal128.FromI64(0))
	valid := aggregate.DecimalAverageOptions{InputScale: 0, Precision: 38, Scale: 0, Mode: aggregate.DecimalAverageHalfEven}
	for _, s := range []aggregate.DecimalSumState{empty, zero} {
		for _, bad := range []aggregate.DecimalAverageOptions{{}, {InputScale: -1, Precision: 38, Scale: 0, Mode: valid.Mode}, {InputScale: 39, Precision: 38, Scale: 0, Mode: valid.Mode}, {InputScale: 0, Precision: 39, Scale: 0, Mode: valid.Mode}, {InputScale: 0, Precision: 1, Scale: 2, Mode: valid.Mode}, {InputScale: 0, Precision: 1, Scale: 0, Mode: 255}} {
			if _, ok, e := s.FinalizeAverage(bad); ok || !errors.Is(e, aggregate.ErrDecimalAverageOptions) {
				t.Fatal(ok, e, bad)
			}
		}
		_, ok, e := s.FinalizeAverage(valid)
		if e != nil || ok != (s.Count() != 0) {
			t.Fatal(ok, e)
		}
	}
}

func TestN19PublicAverageCarryAndCancellationZero(t *testing.T) {
	boundary := new(big.Int).Lsh(big.NewInt(1), 64)
	below := new(big.Int).Sub(new(big.Int).Set(boundary), big.NewInt(1))
	for _, negative := range []bool{false, true} {
		a, b := new(big.Int).Set(below), new(big.Int).Set(boundary)
		if negative {
			a.Neg(a)
			b.Neg(b)
		}
		var s aggregate.DecimalSumState
		_ = s.Add(decimal128.FromBigInt(a))
		_ = s.Add(decimal128.FromBigInt(b))
		for _, m := range []aggregate.DecimalAverageRoundingMode{aggregate.DecimalAverageTowardZero, aggregate.DecimalAverageAwayFromZero, aggregate.DecimalAverageFloor, aggregate.DecimalAverageCeil, aggregate.DecimalAverageHalfEven, aggregate.DecimalAverageHalfAwayFromZero} {
			want := new(big.Int).Set(boundary)
			if m == aggregate.DecimalAverageTowardZero || m == aggregate.DecimalAverageFloor && !negative || m == aggregate.DecimalAverageCeil && negative {
				want.Set(below)
			}
			if negative {
				want.Neg(want)
			}
			got, ok, e := s.FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: 0, Precision: 20, Scale: 0, Mode: m})
			if e != nil || !ok || got.BigInt().Cmp(want) != 0 {
				t.Fatal(negative, m, got, ok, e, want)
			}
		}
	}
	max, _ := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	var s aggregate.DecimalSumState
	_ = s.Add(max)
	_ = s.Add(max.Negate())
	got, ok, e := s.FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: 38, Precision: 38, Scale: 38, Mode: aggregate.DecimalAverageHalfEven})
	if e != nil || !ok || got != decimal128.FromI64(0) || s.Count() != 2 {
		t.Fatal(got, ok, e)
	}
}
