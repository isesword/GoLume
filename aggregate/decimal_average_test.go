package aggregate

import (
	"errors"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

var n19Modes = []DecimalAverageRoundingMode{DecimalAverageTowardZero, DecimalAverageAwayFromZero, DecimalAverageFloor, DecimalAverageCeil, DecimalAverageHalfEven, DecimalAverageHalfAwayFromZero}

func n19State(t *testing.T, values ...*big.Int) DecimalSumState {
	t.Helper()
	var s DecimalSumState
	for _, v := range values {
		if e := s.Add(decimal128.FromBigInt(v)); e != nil {
			t.Fatal(e)
		}
	}
	return s
}
func n19Pow(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }

// The oracle keeps an exact rational throughout and rounds its integer quotient
// independently of the production fixed-width representation.
func n19Oracle(sum *big.Int, count uint64, o DecimalAverageOptions) (*big.Int, bool) {
	r := new(big.Rat).SetFrac(new(big.Int).Set(sum), new(big.Int).SetUint64(count))
	delta := o.Scale - o.InputScale
	if delta >= 0 {
		r.Mul(r, new(big.Rat).SetInt(n19Pow(delta)))
	} else {
		r.Quo(r, new(big.Rat).SetInt(n19Pow(-delta)))
	}
	neg := r.Sign() < 0
	n := new(big.Int).Abs(r.Num())
	d := r.Denom()
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(n, d, rem)
	inc := false
	if rem.Sign() != 0 {
		switch o.Mode {
		case DecimalAverageAwayFromZero:
			inc = true
		case DecimalAverageFloor:
			inc = neg
		case DecimalAverageCeil:
			inc = !neg
		case DecimalAverageHalfEven, DecimalAverageHalfAwayFromZero:
			cmp := new(big.Int).Lsh(rem, 1).Cmp(d)
			inc = cmp > 0 || cmp == 0 && (o.Mode == DecimalAverageHalfAwayFromZero || q.Bit(0) != 0)
		}
	}
	if inc {
		q.Add(q, big.NewInt(1))
	}
	if neg {
		q.Neg(q)
	}
	return q, new(big.Int).Abs(q).Cmp(n19Pow(o.Precision)) >= 0
}
func n19Check(t *testing.T, s DecimalSumState, sum *big.Int, o DecimalAverageOptions) {
	t.Helper()
	before := s
	got, valid, e := s.FinalizeAverage(o)
	if s != before {
		t.Fatal("state mutated")
	}
	want, overflow := n19Oracle(sum, s.Count(), o)
	if overflow {
		if valid || !errors.Is(e, ErrDecimalAverageOverflow) {
			t.Fatalf("overflow: %v %v %v", got, valid, e)
		}
		return
	}
	if e != nil || !valid || got.BigInt().Cmp(want) != 0 {
		t.Fatalf("sum=%v count=%d options=%+v got=%v valid=%v err=%v want=%v", sum, s.Count(), o, got.BigInt(), valid, e, want)
	}
}
func TestN19AverageExplicitOptions(t *testing.T) {
	var empty DecimalSumState
	invalid := []DecimalAverageOptions{{}, {InputScale: -1, Precision: 1, Mode: DecimalAverageHalfEven}, {InputScale: 39, Precision: 1, Mode: DecimalAverageHalfEven}, {Precision: 0, Mode: DecimalAverageHalfEven}, {Precision: 39, Mode: DecimalAverageHalfEven}, {Precision: 1, Scale: -1, Mode: DecimalAverageHalfEven}, {Precision: 1, Scale: 2, Mode: DecimalAverageHalfEven}, {Precision: 1, Mode: 0}, {Precision: 1, Mode: 255}}
	for _, s := range []DecimalSumState{empty, n19State(t, big.NewInt(1))} {
		for _, o := range invalid {
			before := s
			_, valid, e := s.FinalizeAverage(o)
			if valid || !errors.Is(e, ErrDecimalAverageOptions) || s != before {
				t.Fatal(o, valid, e)
			}
		}
	}
	for _, mode := range n19Modes {
		v, valid, e := empty.FinalizeAverage(DecimalAverageOptions{Precision: 38, Scale: 38, Mode: mode})
		if e != nil || valid || v != (memory.Decimal128{}) {
			t.Fatal(v, valid, e)
		}
	}
}
func TestN19AverageWideSumAndMerge(t *testing.T) {
	m := new(big.Int).Sub(n19Pow(38), big.NewInt(1))
	for _, sign := range []int64{-1, 1} {
		v := new(big.Int).Mul(m, big.NewInt(sign))
		s := n19State(t, v, v)
		if _, _, e := s.Finalize(); !errors.Is(e, ErrDecimalSumOverflow) {
			t.Fatal("expected SUM overflow", e)
		}
		for _, mode := range n19Modes {
			n19Check(t, s, new(big.Int).Lsh(new(big.Int).Set(v), 1), DecimalAverageOptions{Precision: 38, Mode: mode})
		}
		// Build a legal MaxInt64-count state using logarithmic merges, not a raw
		// fabricated limb/count pair. Its exact average remains the original value.
		block := n19State(t, v)
		var total DecimalSumState
		for i := 0; i < 63; i++ {
			if e := total.Merge(block); e != nil {
				t.Fatal(e)
			}
			if i < 62 {
				if e := block.Merge(block); e != nil {
					t.Fatal(e)
				}
			}
		}
		if total.Count() != math.MaxInt64 {
			t.Fatal(total.Count())
		}
		sum := new(big.Int).Mul(v, new(big.Int).SetUint64(math.MaxInt64))
		for _, mode := range n19Modes {
			for _, sc := range []int32{0, 19, 38} {
				for _, inputScale := range []int32{0, 19, 38} {
					n19Check(t, total, sum, DecimalAverageOptions{InputScale: inputScale, Precision: 38, Scale: sc, Mode: mode})
				}
			}
		}
	}
	// Means must be weighted by valid counts, never averaged per partition.
	a := n19State(t, big.NewInt(0))
	b := n19State(t, big.NewInt(9), big.NewInt(9), big.NewInt(9))
	if e := a.Merge(b); e != nil {
		t.Fatal(e)
	}
	n19Check(t, a, big.NewInt(27), DecimalAverageOptions{Precision: 4, Scale: 2, Mode: DecimalAverageHalfEven})
	// Intermediate cancellation may be arbitrarily ordered, but never rounded.
	for _, vals := range [][]*big.Int{{m, m, new(big.Int).Neg(m)}, {m, new(big.Int).Neg(m), m}, {new(big.Int).Neg(m), m, m}} {
		s := n19State(t, vals...)
		for _, mode := range n19Modes {
			n19Check(t, s, m, DecimalAverageOptions{InputScale: 38, Precision: 38, Scale: 38, Mode: mode})
		}
	}
}
func TestN19AverageRoundingAndScale(t *testing.T) {
	for _, n := range []int64{-199, -195, -190, -155, -151, -150, -149, -105, -99, -95, -15, -10, -5, -1, 0, 1, 5, 10, 15, 95, 99, 105, 149, 150, 151, 155, 190, 195, 199} {
		s := n19State(t, big.NewInt(n), big.NewInt(0))
		for _, mode := range n19Modes {
			for _, is := range []int32{0, 1, 19, 38} {
				for _, os := range []int32{0, 1, 19, 38} {
					for _, p := range []int32{1, 2, 19, 38} {
						if os > p {
							continue
						}
						n19Check(t, s, big.NewInt(n), DecimalAverageOptions{InputScale: is, Precision: p, Scale: os, Mode: mode})
					}
				}
			}
		}
	}
	// 9.5 rounds across the target p1 boundary only in the incrementing modes.
	s := n19State(t, big.NewInt(19), big.NewInt(0))
	for _, m := range n19Modes {
		n19Check(t, s, big.NewInt(19), DecimalAverageOptions{Precision: 1, Mode: m})
	}
}
func TestN19AverageRandomOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(19019))
	limit := n19Pow(38)
	for trial := 0; trial < 1200; trial++ {
		var s DecimalSumState
		sum := new(big.Int)
		for i, n := 0, 1+rng.Intn(30); i < n; i++ {
			v := new(big.Int).Rand(rng, limit)
			if rng.Intn(2) == 0 {
				v.Neg(v)
			}
			if e := s.Add(decimal128.FromBigInt(v)); e != nil {
				t.Fatal(e)
			}
			sum.Add(sum, v)
		}
		p := int32(1 + rng.Intn(38))
		o := DecimalAverageOptions{InputScale: int32(rng.Intn(39)), Precision: p, Scale: int32(rng.Intn(int(p) + 1))}
		for _, mode := range n19Modes {
			o.Mode = mode
			n19Check(t, s, sum, o)
		}
	}
}

var n19Sink memory.Decimal128

func TestN19AverageNoAllocAndUnchanged(t *testing.T) {
	s := n19State(t, big.NewInt(-17), big.NewInt(2), big.NewInt(1))
	before := s
	for _, mode := range n19Modes {
		for _, o := range []DecimalAverageOptions{{InputScale: 38, Precision: 38, Scale: 0, Mode: mode}, {Precision: 38, Scale: 36, Mode: mode}, {InputScale: 1, Precision: 38, Scale: 38, Mode: mode}} {
			allocations := testing.AllocsPerRun(100, func() { v, _, _ := s.FinalizeAverage(o); n19Sink = v })
			if allocations != 0 {
				t.Fatal("allocations", allocations, o)
			}
		}
	}
	if s != before {
		t.Fatal("finalization modified state")
	}
	if _, _, e := s.Finalize(); e != nil {
		t.Fatal(e)
	}
}
