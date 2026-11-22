package expr

import (
	"math"
	"math/big"
	"math/rand/v2"
	"testing"

	"gocolumn/memory"
)

func TestInt128ToStringFormatterAgainstBigInt(t *testing.T) {
	cases := []memory.Int128{
		{},
		{Low: 1},
		{High: -1, Low: math.MaxUint64},
		{High: -1},
		{Low: math.MaxUint64},
		{High: 1},
		{High: math.MinInt64},
		{High: math.MaxInt64, Low: math.MaxUint64},
	}
	rng := rand.New(rand.NewPCG(128, 39))
	for range 10000 {
		cases = append(cases, memory.Int128{High: int64(rng.Uint64()), Low: rng.Uint64()})
	}
	var scratch [40]byte
	for _, v := range cases {
		want := new(big.Int).Lsh(big.NewInt(v.High), 64)
		want.Add(want, new(big.Int).SetUint64(v.Low))
		if got := string(formatInt128Decimal(&scratch, v)); got != want.String() {
			t.Fatalf("%+v: got %q, want %s", v, got, want)
		}
	}
}
