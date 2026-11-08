package engine

import (
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/rand"
	"testing"
)

// Compare with the original state update, not another implementation of the
// word reducer. Include competing NaN payloads, signed zero, subnormals and
// rounding boundaries, carrying the actual prior bits across words.
func TestGlobalReductionReviewFloatBits(t *testing.T) {
	rng := rand.New(rand.NewSource(81291))
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		special := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff0000000000011, 0x7ff8000000000022, 0xfff8000000000033, math.Float64bits(1e16), math.Float64bits(-1e16), math.Float64bits(1)}
		if typ == memory.TypeFloat32 {
			special = []uint64{0, 1, 0x80000000, 0x7f800000, 0xff800000, 0x7f800011, 0x7fc00022, 0xffc00033, uint64(math.Float32bits(1 << 24)), uint64(math.Float32bits(-(1 << 24))), uint64(math.Float32bits(1))}
		}
		for trial := 0; trial < 300; trial++ {
			ref, err := aggregate.NewNumericVector(1, typ, aggregate.Layout{Sum: true})
			if err != nil {
				t.Fatal(err)
			}
			var running uint64
			for word := 0; word < 5; word++ {
				n := 1 + rng.Intn(64)
				mask := rng.Uint64() & memory.TailMask(n)
				if trial%3 == 0 {
					mask = memory.TailMask(n)
				}
				f32, f64 := make([]float32, n), make([]float64, n)
				for i := 0; i < n; i++ {
					raw := special[rng.Intn(len(special))]
					if trial%5 == 0 {
						raw = rng.Uint64()
					}
					f32[i], f64[i] = math.Float32frombits(uint32(raw)), math.Float64frombits(raw)
					if mask&(uint64(1)<<uint(i)) != 0 {
						ref.AddBits(0, raw, true)
					}
				}
				b := globalAggregateLaneBinding{typ: typ, float32s: &memory.Float32Vector{Values: f32}, float64s: &memory.Float64Vector{Values: f64}}
				running = b.sumWord(kernel.New(kernel.ModeScalar), 0, n, mask, running)
				if want := ref.Result(0).SumBits; running != want {
					t.Fatalf("type=%s trial=%d word=%d mask=%x got=%x want=%x", typ, trial, word, mask, running, want)
				}
			}
		}
	}
}
