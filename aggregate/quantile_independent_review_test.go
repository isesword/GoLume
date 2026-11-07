package aggregate

import (
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"testing"
)

func TestReviewQuantileRanksExponentBoundaries(t *testing.T) {
	ns := []uint64{1, 2, 3, 4, 7, 1 << 32, 1<<53 - 1, 1<<53 + 1, 1 << 63, math.MaxUint64 - 1, math.MaxUint64}
	qs := []float64{0, math.Copysign(0, -1), 1, math.Nextafter(1, 0)}
	for exponent := -1074; exponent <= 0; exponent++ {
		q := math.Ldexp(1, exponent)
		qs = append(qs, q)
		if q > 0 {
			qs = append(qs, math.Nextafter(q, 0))
		}
		if q < 1 {
			qs = append(qs, math.Nextafter(q, 1))
		}
	}
	for _, n := range ns {
		for _, q := range qs {
			for m := QuantileNearest; m <= QuantileEquiprobable; m++ {
				count := n - 1
				if m == QuantileEquiprobable {
					count = n
				}
				r := new(big.Rat).Mul(new(big.Rat).SetUint64(count), new(big.Rat).SetFloat64(q))
				f, rem := new(big.Int), new(big.Int)
				f.QuoRem(r.Num(), r.Denom(), rem)
				lo, hi := f.Uint64(), f.Uint64()
				switch m {
				case QuantileNearest:
					if new(big.Int).Lsh(new(big.Int).Set(rem), 1).Cmp(r.Denom()) >= 0 {
						lo++
						hi++
					}
				case QuantileHigher:
					if rem.Sign() != 0 {
						lo++
						hi++
					}
				case QuantileLinear, QuantileMidpoint:
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
				got, err := PlanQuantile(n, q, m)
				if err != nil || !got.Valid || got.Lower != lo || got.Upper != hi || hi >= n {
					t.Fatalf("n=%d q=%g mode=%d got=%+v err=%v want=%d,%d", n, q, m, got, err, lo, hi)
				}
			}
		}
	}
}

func TestReviewQuantileIntegerInterpolationRational(t *testing.T) {
	rng := rand.New(rand.NewSource(9451))
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		for i := 0; i < 20000; i++ {
			a, b := rng.Uint64(), rng.Uint64()
			if typ == memory.TypeInt64 {
				if int64(a) > int64(b) {
					a, b = b, a
				}
			} else if a > b {
				a, b = b, a
			}
			w := rng.Float64()
			if i%3 == 0 {
				w = .5
			}
			toRat := func(v uint64) *big.Rat {
				if typ == memory.TypeInt64 {
					return new(big.Rat).SetInt64(int64(v))
				}
				return new(big.Rat).SetUint64(v)
			}
			ar, br, wr := toRat(a), toRat(b), new(big.Rat).SetFloat64(w)
			wantRat := new(big.Rat).Add(ar, new(big.Rat).Mul(new(big.Rat).Sub(br, ar), wr))
			want, _ := wantRat.Float64()
			bits, valid, err := InterpolateQuantile(typ, a, b, QuantileRanks{Lower: 0, Upper: 1, Weight: w, Valid: true})
			got := math.Float64frombits(bits)
			ulp := math.Abs(math.Nextafter(want, math.Inf(1)) - want)
			if err != nil || !valid || math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > 2*ulp {
				t.Fatalf("type=%v a=%x b=%x w=%.17g got=%.17g want=%.17g ulp=%g err=%v", typ, a, b, w, got, want, ulp, err)
			}
		}
	}
}

func TestReviewQuantileNativeOrderAndZeros(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64} {
		vals := []uint64{0, 1, 2, math.MaxUint64, 1 << 63, math.MaxInt64}
		for i := range vals {
			vals[i] = normalizeNumericBits(typ, vals[i])
		}
		sort.Slice(vals, func(i, j int) bool {
			a, e := QuantileOrderKey(typ, vals[i])
			if e != nil {
				t.Fatal(e)
			}
			b, e := QuantileOrderKey(typ, vals[j])
			if e != nil {
				t.Fatal(e)
			}
			return a < b
		})
		for i := 1; i < len(vals); i++ {
			signed := typ == memory.TypeInt8 || typ == memory.TypeInt16 || typ == memory.TypeInt32 || typ == memory.TypeInt64
			if signed {
				if int64(vals[i]) < int64(vals[i-1]) {
					t.Fatal(typ, vals)
				}
			} else if vals[i] < vals[i-1] {
				t.Fatal(typ, vals)
			}
		}
	}
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		encode := func(v float64) uint64 {
			if typ == memory.TypeFloat32 {
				return uint64(math.Float32bits(float32(v)))
			}
			return math.Float64bits(v)
		}
		vals := []float64{math.Inf(-1), -math.MaxFloat32, -1, math.Copysign(0, -1), 0, 1, math.MaxFloat32, math.Inf(1), math.NaN()}
		var prior uint64
		for i, v := range vals {
			key, e := QuantileOrderKey(typ, encode(v))
			if e != nil || i > 0 && key < prior {
				t.Fatal(typ, v, key, prior, e)
			}
			prior = key
		}
		nz, pz := encode(math.Copysign(0, -1)), encode(0)
		a, _ := QuantileOrderKey(typ, nz)
		b, _ := QuantileOrderKey(typ, pz)
		if a != b {
			t.Fatal("zero equality")
		}
		bits, ok, e := InterpolateQuantile(typ, nz, pz, QuantileRanks{Valid: true})
		if e != nil || !ok || bits != nz {
			t.Fatal("selected zero payload lost", typ, bits, e)
		}
	}
}

func TestReviewQuantileOppositeSignNearCancellation(t *testing.T) {
	for _, pair := range [][2]float64{{-1, 1}, {-1, 2}, {-2, 1}, {-math.MaxFloat64, math.MaxFloat64}, {-1e100, 1e100}} {
		middle := -pair[0] / (pair[1] - pair[0])
		if math.IsInf(pair[1]-pair[0], 0) {
			middle = .5
		}
		for _, w := range []float64{math.Nextafter(middle, 0), middle, math.Nextafter(middle, 1)} {
			ar, br, wr := new(big.Rat).SetFloat64(pair[0]), new(big.Rat).SetFloat64(pair[1]), new(big.Rat).SetFloat64(w)
			want, _ := new(big.Rat).Add(ar, new(big.Rat).Mul(new(big.Rat).Sub(br, ar), wr)).Float64()
			got, _, err := InterpolateQuantile(memory.TypeFloat64, math.Float64bits(pair[0]), math.Float64bits(pair[1]), QuantileRanks{Lower: 0, Upper: 1, Weight: w, Valid: true})
			ulp := math.Abs(math.Nextafter(want, math.Inf(1)) - want)
			if err != nil || math.IsNaN(math.Float64frombits(got)) || math.Abs(math.Float64frombits(got)-want) > 2*ulp {
				t.Errorf("a=%g b=%g w=%.17g got %.17g want %.17g err=%v", pair[0], pair[1], w, math.Float64frombits(got), want, err)
			}
		}
	}
}

// Compensated interpolation is not promised correctly rounded. Bound residual
// arithmetic error relative to both the result ULP and the endpoint scale.
func TestReviewQuantileFloatCancellationRational(t *testing.T) {
	rng := rand.New(rand.NewSource(34091))
	for i := 0; i < 6000; i++ {
		exponent := rng.Intn(2040) - 1020
		a := -math.Ldexp(1+rng.Float64(), exponent)
		b := math.Ldexp(1+rng.Float64(), exponent)
		w := (-a / 2) / (b/2 - a/2)
		if i%3 == 0 {
			w = math.Nextafter(w, 0)
		} else if i%3 == 1 {
			w = math.Nextafter(w, 1)
		}
		ar, br, wr := new(big.Rat).SetFloat64(a), new(big.Rat).SetFloat64(b), new(big.Rat).SetFloat64(w)
		want, _ := new(big.Rat).Add(ar, new(big.Rat).Mul(new(big.Rat).Sub(br, ar), wr)).Float64()
		got, ok, err := InterpolateQuantile(memory.TypeFloat64, math.Float64bits(a), math.Float64bits(b), QuantileRanks{Lower: 0, Upper: 1, Weight: w, Valid: true})
		ulp := math.Abs(math.Nextafter(want, math.Inf(1)) - want)
		if err != nil || !ok || math.IsNaN(math.Float64frombits(got)) || math.Abs(math.Float64frombits(got)-want) > 4*ulp+math.Ldexp(math.Abs(a)+math.Abs(b), -100) {
			t.Fatalf("a=%.17g b=%.17g w=%.17g got=%.17g want=%.17g ulp=%g err=%v", a, b, w, math.Float64frombits(got), want, ulp, err)
		}
	}
}
