package fixturegen

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// zipfSampler draws rank indexes in [0, n) from a Zipf distribution with skew
// s > 1 and shift v >= 1, following the rejection method of Luc Devroye
// (Non-Uniform Random Variate Generation, 1986, p. 551), the same algorithm
// the Go standard library uses. Samples are deterministic for a given RNG.
type zipfSampler struct {
	rng          *rand.Rand
	s            float64
	v            float64
	oneminusS    float64
	oneminusSinv float64
	hxm          float64
	hx0minusHxm  float64
}

func newZipfSampler(rng *rand.Rand, s, v float64, n uint64) (*zipfSampler, error) {
	if s <= 1 || math.IsNaN(s) {
		return nil, fmt.Errorf("zipf skew s must be > 1, got %v", s)
	}
	if v < 1 || math.IsNaN(v) {
		return nil, fmt.Errorf("zipf shift v must be >= 1, got %v", v)
	}
	if n == 0 {
		return nil, fmt.Errorf("zipf cardinality must be positive")
	}
	z := &zipfSampler{rng: rng, s: s, v: v, oneminusS: 1 - s, oneminusSinv: 1 / (1 - s)}
	imax := float64(n - 1)
	z.hxm = z.h(imax + 0.5)
	z.hx0minusHxm = z.h(0.5) - math.Exp(math.Log(v)*(-s)) - z.hxm
	return z, nil
}

func (z *zipfSampler) h(x float64) float64 {
	return math.Exp(z.oneminusS*math.Log(z.v+x)) * z.oneminusSinv
}

func (z *zipfSampler) hinv(x float64) float64 {
	return math.Exp(z.oneminusSinv*math.Log(z.oneminusS*x)) - z.v
}

// next draws the next rank index in [0, n).
func (z *zipfSampler) next() uint64 {
	k := 0.0
	for {
		r := z.rng.Float64()
		ur := z.hxm + r*z.hx0minusHxm
		x := z.hinv(ur)
		k = math.Floor(x + 0.5)
		if k-x <= z.s {
			break
		}
		if ur >= z.h(k+0.5)-math.Exp(-math.Log(k+z.v)*z.s) {
			break
		}
	}
	return uint64(k)
}
