package physical

import (
	"encoding/binary"
	"math"
	"math/big"
	"testing"
)

func FuzzWindowExactSumAverage(f *testing.F) {
	for _, values := range [][]float64{
		{1e16, 1, 1, -1e16},
		{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1, 2},
		{math.NaN(), math.Inf(1), math.Inf(-1), 1, 2, 3},
		{math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64, 0},
	} {
		seed := make([]byte, 1+8*len(values))
		seed[0] = 1 // A two-row trailing frame.
		for i, value := range values {
			binary.LittleEndian.PutUint64(seed[1+8*i:], math.Float64bits(value))
		}
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 9 {
			return
		}
		data = data[:min(len(data), 2049)]
		width := int(data[0]&31) + 1
		var sum windowExactSum
		var values []float64
		for offset := 1; offset+8 <= len(data); offset += 8 {
			value := math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
			values = append(values, value)
			sum.addFloat(value, 1)
			if len(values) > width {
				sum.addFloat(values[0], -1)
				values = values[1:]
			}
			rats := make([]*big.Rat, len(values))
			valid := make([]bool, len(values))
			groups := make([]int64, len(values))
			for i, v := range values {
				rats[i], valid[i] = new(big.Rat).SetFloat64(v), true
			}
			for _, avg := range []bool{false, true} {
				count := int64(1)
				if avg {
					count = int64(len(values))
				}
				want, _ := exactReviewOracle(rats, values, valid, groups, 0, 0, len(values), avg)
				got := sum.result(count)
				if !(math.IsNaN(got) && math.IsNaN(want)) && math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("values=%v avg=%v got=%g want=%g", values, avg, got, want)
				}
			}
		}
	})
}
