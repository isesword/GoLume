package physical

import (
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"testing"
)

// stddevExactRootOracle searches positive Float64 bit patterns using only exact
// rational squares. Its overflow neighbor is the exact real number 2^1024.
// It deliberately shares no production exponent, quotient, or limb algorithm.
func stddevExactRootOracle(r *big.Rat) float64 {
	if r.Sign() < 0 {
		panic("negative root oracle")
	}
	if r.Sign() == 0 {
		return 0
	}
	const maxBits = uint64(0x7fefffffffffffff)
	low, high := uint64(0), maxBits
	square := func(bits uint64) *big.Rat {
		x := new(big.Rat).SetFloat64(math.Float64frombits(bits))
		return new(big.Rat).Mul(x, x)
	}
	for low < high {
		mid := low + (high-low)/2 + 1
		if square(mid).Cmp(r) <= 0 {
			low = mid
		} else {
			high = mid - 1
		}
	}
	lower := new(big.Rat).SetFloat64(math.Float64frombits(low))
	var upper *big.Rat
	if low == maxBits {
		upper = new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 1024))
	} else {
		upper = new(big.Rat).SetFloat64(math.Float64frombits(low + 1))
	}
	midpoint := new(big.Rat).Quo(new(big.Rat).Add(lower, upper), big.NewRat(2, 1))
	cmp := r.Cmp(new(big.Rat).Mul(midpoint, midpoint))
	if cmp > 0 || cmp == 0 && low&1 != 0 {
		if low == maxBits {
			return math.Inf(1)
		}
		low++
	}
	return math.Float64frombits(low)
}
func stddevKernelOracle(typ memory.DataType, cells []windowNumericCell, sample bool) (float64, bool) {
	s, q := new(big.Rat), new(big.Rat)
	n := int64(0)
	special := false
	for _, c := range cells {
		if !c.valid {
			continue
		}
		n++
		x := new(big.Rat)
		switch {
		case windowSigned(typ):
			x.SetInt64(c.i64)
		case windowUnsigned(typ):
			x.SetInt(new(big.Int).SetUint64(c.u64))
		default:
			if math.IsNaN(c.f64) || math.IsInf(c.f64, 0) {
				special = true
				continue
			}
			x.SetFloat64(c.f64)
		}
		s.Add(s, x)
		q.Add(q, new(big.Rat).Mul(x, x))
	}
	ddof := int64(0)
	if sample {
		ddof = 1
	}
	if n <= ddof {
		return 0, false
	}
	if special {
		return math.NaN(), true
	}
	numerator := new(big.Rat).Sub(new(big.Rat).Mul(q, new(big.Rat).SetInt64(n)), new(big.Rat).Mul(s, s))
	denominator := new(big.Int).Mul(big.NewInt(n), big.NewInt(n-ddof))
	numerator.Quo(numerator, new(big.Rat).SetInt(denominator))
	return stddevExactRootOracle(numerator), true
}
func checkStddevKernel(t *testing.T, m *windowExactMoments, typ memory.DataType, cells []windowNumericCell) {
	t.Helper()
	for _, sample := range []bool{false, true} {
		want, valid := stddevKernelOracle(typ, cells, sample)
		for attempt := 0; attempt < 2; attempt++ {
			got, ok, err := m.stddev(sample)
			if err != nil || ok != valid || ok && !(math.IsNaN(got) && math.IsNaN(want)) && math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("sample%v got%g/%v err%v want%g/%v", sample, got, ok, err, want, valid)
			}
			checkVarianceKernel(t, m, typ, cells)
		}
	}
}
func TestStddevExactMidpoints(t *testing.T) {
	eps := math.SmallestNonzeroFloat64
	normal := math.Float64frombits(uint64(1) << 52)
	cases := []struct{ a, b, want float64 }{{2, -math.Ldexp(1, -52), 1}, {2, -3 * math.Ldexp(1, -52), 1 + math.Ldexp(1, -51)}, {eps, 0, 0}, {3 * eps, 0, 2 * eps}, {5 * eps, 0, 2 * eps}, {2*normal - eps, 0, normal}, {math.MaxFloat64, -math.MaxFloat64, math.MaxFloat64}, {1e200, -1e200, 1e200}, {1e-200, -1e-200, 1e-200}}
	for _, tc := range cases {
		var m windowExactMoments
		cells := []windowNumericCell{{valid: true, f64: tc.a}, {valid: true, f64: tc.b}}
		for _, c := range cells {
			if err := m.addCell(memory.TypeFloat64, c, 1); err != nil {
				t.Fatal(err)
			}
		}
		got, valid, err := m.stddev(false)
		if err != nil || !valid || math.Float64bits(got) != math.Float64bits(tc.want) {
			t.Fatal(tc, got, valid, err)
		}
		checkStddevKernel(t, &m, memory.TypeFloat64, cells)
	}
}
func TestStddevExactSpecialRemovalAndIntegers(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat64, memory.TypeInt64, memory.TypeUint64} {
		cells := []windowNumericCell{{valid: false}, {valid: true, f64: math.NaN(), i64: math.MinInt64, u64: math.MaxUint64}, {valid: true, f64: math.Inf(1), i64: math.MinInt64 + 1, u64: math.MaxUint64 - 1}, {valid: true, f64: math.Inf(-1), i64: math.MaxInt64, u64: 1}, {valid: true, f64: 1}, {valid: true, f64: 2}}
		var m windowExactMoments
		for _, c := range cells {
			if err := m.addCell(typ, c, 1); err != nil {
				t.Fatal(err)
			}
		}
		for len(cells) > 0 {
			checkStddevKernel(t, &m, typ, cells)
			if err := m.addCell(typ, cells[0], -1); err != nil {
				t.Fatal(err)
			}
			cells = cells[1:]
		}
		checkStddevKernel(t, &m, typ, cells)
	}
}
func TestStddevExactRandomSliding(t *testing.T) {
	rng := rand.New(rand.NewSource(912))
	var m windowExactMoments
	var cells []windowNumericCell
	for i := 0; i < 80; i++ {
		c := windowNumericCell{valid: i%13 != 0, f64: math.Float64frombits(rng.Uint64())}
		if err := m.addCell(memory.TypeFloat64, c, 1); err != nil {
			t.Fatal(err)
		}
		cells = append(cells, c)
		if len(cells) > 7 {
			if err := m.addCell(memory.TypeFloat64, cells[0], -1); err != nil {
				t.Fatal(err)
			}
			cells = cells[1:]
		}
		checkStddevKernel(t, &m, memory.TypeFloat64, cells)
	}
}
func TestStddevExactLargeCounts(t *testing.T) {
	for _, n := range []int64{1<<32 - 1, 1 << 32, 1<<32 + 1, math.MaxInt64} {
		var m windowExactMoments
		if err := m.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: 1}, 1); err != nil {
			t.Fatal(err)
		}
		m.count = n
		for _, sample := range []bool{false, true} {
			num, den := big.NewInt(n-1), new(big.Int).Mul(big.NewInt(n), big.NewInt(n))
			if sample {
				num = big.NewInt(1)
				den = big.NewInt(n)
			}
			want := stddevExactRootOracle(new(big.Rat).SetFrac(num, den))
			got, valid, err := m.stddev(sample)
			if err != nil || !valid || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatal(n, sample, got, want, err)
			}
		}
	}
}
func TestStddevExactNoAllocation(t *testing.T) {
	var m windowExactMoments
	a, b := windowNumericCell{valid: true, f64: 1.25}, windowNumericCell{valid: true, f64: 2.5}
	_ = m.addCell(memory.TypeFloat64, a, 1)
	allocs := testing.AllocsPerRun(100, func() {
		if err := m.addCell(memory.TypeFloat64, b, 1); err != nil {
			panic(err)
		}
		if _, _, err := m.stddev(false); err != nil {
			panic(err)
		}
		if _, _, err := m.result(true); err != nil {
			panic(err)
		}
		if _, _, err := m.stddev(true); err != nil {
			panic(err)
		}
		if err := m.addCell(memory.TypeFloat64, b, -1); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatal(allocs)
	}
}
