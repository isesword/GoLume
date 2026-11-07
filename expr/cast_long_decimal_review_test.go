package expr

import (
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestReviewLongDecimalExactRounding(t *testing.T) {
	const decimalPlaces = 5000
	one := big.NewRat(1, 1)
	pow2 := func(n int) *big.Int { return new(big.Int).Lsh(big.NewInt(1), uint(n)) }
	epsilon := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalPlaces), nil))
	for _, bits := range []int{32, 64} {
		minExp, precision := 150, 24
		boundary := float32OverflowBoundary
		if bits == 64 {
			minExp, precision = 1075, 53
			boundary = float64OverflowBoundary
		}
		over, _ := new(big.Int).SetString(boundary, 10)
		points := []*big.Rat{
			new(big.Rat),
			new(big.Rat).SetFrac(big.NewInt(1), pow2(minExp)),
			new(big.Rat).SetFrac(big.NewInt(3), pow2(minExp)),
			new(big.Rat).SetFrac(new(big.Int).Sub(pow2(precision), big.NewInt(1)), pow2(minExp)),
			new(big.Rat).Add(one, new(big.Rat).SetFrac(big.NewInt(1), pow2(precision))),
			new(big.Rat).SetInt(over),
		}
		for pi, point := range points {
			for _, delta := range []int64{-1, 0, 1} {
				value := new(big.Rat).Add(point, new(big.Rat).Mul(epsilon, big.NewRat(delta, 1)))
				if value.Sign() < 0 {
					continue
				}
				plain := value.FloatString(decimalPlaces)
				mantissa := strings.ReplaceAll(plain, ".", "")
				forms := []string{plain, "00000" + plain, mantissa + "e-" + strconv.Itoa(decimalPlaces), mantissa + strings.Repeat("0", 5000) + "e-000" + strconv.Itoa(decimalPlaces+5000)}
				for _, sign := range []string{"", "-"} {
					for fi, form := range forms {
						token := sign + form
						exact, ok := new(big.Rat).SetString(token)
						if !ok {
							t.Fatal("bad rational input")
						}
						want := float64(0)
						if bits == 64 {
							want, _ = exact.Float64()
						} else {
							f, _ := new(big.Float).SetPrec(40000).SetRat(exact).Float32()
							want = float64(f)
						}
						if exact.Sign() == 0 && sign == "-" {
							want = math.Copysign(0, -1)
						}
						got, err := parseDecimalFloat([]byte(token), bits)
						if err != nil || math.Float64bits(got) != math.Float64bits(want) {
							t.Fatalf("bits=%d point=%d delta=%d sign=%q form=%d got=%g want=%g err=%v", bits, pi, delta, sign, fi, got, want, err)
						}
					}
				}
			}
		}
	}
}

func TestReviewLongDecimalRandomExact(t *testing.T) {
	rng := rand.New(rand.NewSource(35129))
	for i := 0; i < 400; i++ {
		n := 769 + rng.Intn(5000)
		digits := make([]byte, n)
		for j := range digits {
			digits[j] = byte('0' + rng.Intn(10))
		}
		dot := rng.Intn(n + 1)
		// Compensate the long mantissa into the observable finite/subnormal range.
		exponent := -dot + rng.Intn(720) - 360
		text := string(digits[:dot]) + "." + string(digits[dot:]) + "e" + strconv.Itoa(exponent)
		if i%2 == 0 {
			text = "-" + text
		}
		exact, ok := new(big.Rat).SetString(text)
		if !ok {
			t.Fatal("bad test token")
		}
		for _, bits := range []int{32, 64} {
			var want float64
			if bits == 64 {
				want, _ = exact.Float64()
			} else {
				f, _ := new(big.Float).SetPrec(40000).SetRat(exact).Float32()
				want = float64(f)
			}
			got, err := parseDecimalFloat([]byte(text), bits)
			if err != nil || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("case=%d bits=%d got=%g want=%g err=%v", i, bits, got, want, err)
			}
		}
	}
}
