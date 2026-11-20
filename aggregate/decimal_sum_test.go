package aggregate

import (
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"math"
	"math/big"
	"testing"
)

func TestN14DecimalSumExact(t *testing.T) {
	max, _ := decimal128.FromString("99999999999999999999999999999999999999", 38, 0)
	neg := max.Negate()
	for _, vs := range [][]decimal128.Num{{max, max, neg}, {max, neg, max}, {neg, max, max}, {neg, neg, max}} {
		var s DecimalSumState
		want := new(big.Int)
		for _, v := range vs {
			if e := s.Add(v); e != nil {
				t.Fatal(e)
			}
			want.Add(want, v.BigInt())
		}
		got, valid, e := s.Finalize()
		if e != nil || !valid || got.BigInt().Cmp(want) != 0 {
			t.Fatal(got, valid, e, want)
		}
	}
	var empty DecimalSumState
	if _, v, e := empty.Finalize(); v || e != nil {
		t.Fatal(v, e)
	}
	_ = empty.Add(max)
	_ = empty.Add(max)
	if _, _, e := empty.Finalize(); !errors.Is(e, ErrDecimalSumOverflow) {
		t.Fatal(e)
	}
}
func TestN14DecimalSumMergeCountAndNoAlloc(t *testing.T) {
	var whole, a, b DecimalSumState
	for i := -1000; i < 1000; i++ {
		v := decimal128.FromI64(int64(i))
		_ = whole.Add(v)
		if i%2 == 0 {
			_ = a.Add(v)
		} else {
			_ = b.Add(v)
		}
	}
	if e := a.Merge(b); e != nil {
		t.Fatal(e)
	}
	if a != whole {
		t.Fatal("partition merge differs")
	}
	s := DecimalSumState{count: math.MaxInt64}
	before := s
	if e := s.Add(decimal128.FromI64(0)); !errors.Is(e, ErrDecimalSumCount) || s != before {
		t.Fatal("count update changed on failure")
	}
	if e := s.Merge(DecimalSumState{count: 1}); !errors.Is(e, ErrDecimalSumCount) || s != before {
		t.Fatal("count merge changed on failure")
	}
	if n := testing.AllocsPerRun(100, func() {
		var x DecimalSumState
		for i := 0; i < 64; i++ {
			_ = x.Add(decimal128.FromI64(1))
		}
		_, _, _ = x.Finalize()
	}); n != 0 {
		t.Fatal(n)
	}
}
