package physical

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"testing"
	"unsafe"
)

func varianceKernelOracle(typ memory.DataType, cells []windowNumericCell, sample bool) (float64, bool) {
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
	v, _ := numerator.Float64()
	return v, true
}
func checkVarianceKernel(t *testing.T, m *windowExactMoments, typ memory.DataType, cells []windowNumericCell) {
	t.Helper()
	for _, sample := range []bool{false, true} {
		got, valid, err := m.result(sample)
		want, wvalid := varianceKernelOracle(typ, cells, sample)
		if err != nil || valid != wvalid || valid && !(math.IsNaN(got) && math.IsNaN(want)) && math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("sample%v got%g/%v err%v want%g/%v", sample, got, valid, err, want, wvalid)
		}
	}
}
func TestVarianceExactKernelFloatAndRemoval(t *testing.T) {
	cases := [][]float64{{}, {0}, {-0.0, 0}, {math.MaxFloat64, math.MaxFloat64}, {math.MaxFloat64, -math.MaxFloat64}, {math.SmallestNonzeroFloat64, 0}, {math.Ldexp(1, -537), 0}, {math.Ldexp(3, -537), 0}, {math.Ldexp(5, -537), 0}, {math.Ldexp(1, -536), 0}, {1e16, 1e16 + 2, 1e16 + 4}, {math.Inf(1), math.Inf(-1), 1, 2}, {math.NaN(), 1, 2}}
	for _, values := range cases {
		var m windowExactMoments
		var cells []windowNumericCell
		for _, v := range values {
			c := windowNumericCell{valid: true, f64: v}
			if err := m.addCell(memory.TypeFloat64, c, 1); err != nil {
				t.Fatal(err)
			}
			cells = append(cells, c)
			checkVarianceKernel(t, &m, memory.TypeFloat64, cells)
		}
		for len(cells) > 0 {
			if err := m.addCell(memory.TypeFloat64, cells[0], -1); err != nil {
				t.Fatal(err)
			}
			cells = cells[1:]
			checkVarianceKernel(t, &m, memory.TypeFloat64, cells)
		}
	}
}
func TestVarianceExactKernelIntegers(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		var m windowExactMoments
		cells := []windowNumericCell{{valid: true, i64: math.MinInt64, u64: math.MaxUint64}, {valid: true, i64: math.MinInt64 + 1, u64: math.MaxUint64 - 1}, {valid: true, i64: math.MaxInt64, u64: 1}, {valid: false}}
		for _, c := range cells {
			if err := m.addCell(typ, c, 1); err != nil {
				t.Fatal(err)
			}
		}
		checkVarianceKernel(t, &m, typ, cells)
		for len(cells) > 0 {
			if err := m.addCell(typ, cells[0], -1); err != nil {
				t.Fatal(err)
			}
			cells = cells[1:]
			checkVarianceKernel(t, &m, typ, cells)
		}
	}
}
func TestVarianceExactKernelRandomSliding(t *testing.T) {
	rng := rand.New(rand.NewSource(81))
	var m windowExactMoments
	var cells []windowNumericCell
	for i := 0; i < 200; i++ {
		v := math.Float64frombits(rng.Uint64())
		c := windowNumericCell{valid: i%11 != 0, f64: v}
		if err := m.addCell(memory.TypeFloat64, c, 1); err != nil {
			t.Fatal(err)
		}
		cells = append(cells, c)
		if len(cells) > 9 {
			if err := m.addCell(memory.TypeFloat64, cells[0], -1); err != nil {
				t.Fatal(err)
			}
			cells = cells[1:]
		}
		checkVarianceKernel(t, &m, memory.TypeFloat64, cells)
	}
}
func TestVarianceExactKernelNoAllocAndAccounting(t *testing.T) {
	var m windowExactMoments
	a := windowNumericCell{valid: true, f64: 1.25}
	b := windowNumericCell{valid: true, f64: 2.5}
	if err := m.addCell(memory.TypeFloat64, a, 1); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if err := m.addCell(memory.TypeFloat64, b, 1); err != nil {
			panic(err)
		}
		if _, _, err := m.result(false); err != nil {
			panic(err)
		}
		if _, _, err := m.result(true); err != nil {
			panic(err)
		}
		if err := m.addCell(memory.TypeFloat64, b, -1); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatal("kernel allocations", allocations)
	}
	if windowExactMomentsBytes < int64(unsafe.Sizeof(m)) {
		t.Fatal("state/scratch undercharged")
	}
	t.Log("state bytes", unsafe.Sizeof(m), "admission", windowExactMomentsBytes)
}
func TestVarianceExactKernelInvalidState(t *testing.T) {
	var m windowExactMoments
	c := windowNumericCell{valid: true, f64: 1}
	if err := m.addCell(memory.TypeFloat64, c, -1); err == nil {
		t.Fatal("count underflow")
	}
	m.count = math.MaxInt64
	if err := m.addCell(memory.TypeFloat64, c, 1); err == nil {
		t.Fatal("count overflow")
	}
	m = windowExactMoments{}
	m.count = 1
	m.sum.words[0] = 1
	m.sum.high = 1
	if _, _, err := m.result(false); err == nil {
		t.Fatal("negative numerator accepted")
	}
}

func TestVarianceExactKernelLargeLegalCounts(t *testing.T) {
	for _, n := range []int64{1<<32 - 1, 1 << 32, 1<<32 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var m windowExactMoments
			if err := m.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: 1}, 1); err != nil {
				t.Fatal(err)
			}
			// The remaining n-1 observations are zero: this is a legal complete state,
			// not merely an artificial denominator unrelated to the stored moments.
			m.count = n
			for _, sample := range []bool{false, true} {
				numerator, denominator := big.NewInt(n-1), new(big.Int).Mul(big.NewInt(n), big.NewInt(n))
				if sample {
					numerator = big.NewInt(1)
					denominator = big.NewInt(n)
				}
				want, _ := new(big.Rat).SetFrac(numerator, denominator).Float64()
				got, valid, err := m.result(sample)
				if err != nil || !valid || math.Float64bits(got) != math.Float64bits(want) {
					t.Fatal(n, sample, got, want, valid, err)
				}
			}
		})
	}
}

func TestVarianceExactKernelMaximumMagnitudeLargeCount(t *testing.T) {
	// MaxFloat64 in the kernel's 2^-1074 units: (2^53-1)*2^2045.
	value := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 53), big.NewInt(1))
	value.Lsh(value, 2045)
	n := big.NewInt(math.MaxInt64)
	sum := new(big.Int).Mul(value, n)
	squares := new(big.Int).Mul(new(big.Int).Mul(value, value), n)
	var m windowExactMoments
	m.count = math.MaxInt64
	mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))
	fill := func(dst []uint64, v *big.Int) {
		v = new(big.Int).Set(v)
		for i := range dst {
			dst[i] = new(big.Int).And(v, mask).Uint64()
			v.Rsh(v, 64)
		}
		if v.Sign() != 0 {
			t.Fatal("test magnitude exceeds allocated width")
		}
	}
	fill(m.sum.words[:], sum)
	fill(m.squares[:], squares)
	m.sum.high = len(m.sum.words)
	for m.sum.high > 0 && m.sum.words[m.sum.high-1] == 0 {
		m.sum.high--
	}
	if sum.BitLen() != 2161 || squares.BitLen() != 4259 {
		t.Fatal("fixture does not reach bit bounds", sum.BitLen(), squares.BitLen())
	}
	for _, sample := range []bool{false, true} {
		got, valid, err := m.result(sample)
		if err != nil || !valid || math.Float64bits(got) != 0 {
			t.Fatal("identical max values must cancel exactly", sample, got, valid, err)
		}
	}
	// Removing one maximal observation exercises multiword Q borrow and S carry
	// with the same high limbs; all remaining observations still equal MaxFloat64.
	if err := m.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: math.MaxFloat64}, -1); err != nil {
		t.Fatal(err)
	}
	for _, sample := range []bool{false, true} {
		got, valid, err := m.result(sample)
		if err != nil || !valid || math.Float64bits(got) != 0 {
			t.Fatal(sample, got, valid, err)
		}
	}
}
