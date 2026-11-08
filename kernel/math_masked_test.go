package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func mathMaskedValidity(rows, pattern int) memory.Bitmap {
	if pattern == 0 {
		return nil
	}
	v := make(memory.Bitmap, memory.WordsFor(rows)+1)
	for i := 0; i < rows; i++ {
		active := pattern == 1 || pattern == 3 && i%2 == 1 || pattern == 4 && i%97 != 0 || pattern == 5 && i%97 == 0 || pattern == 6 && i/64%3 == 1
		if active {
			memory.Set(v, i)
		}
	}
	if rows%64 != 0 {
		v[rows/64] |= ^memory.TailMask(rows)
	}
	v[len(v)-1] = ^uint64(0)
	return v
}

func testMathMasked[T mathFloat](t *testing.T, fn func(Set, []T, []T, memory.Bitmap, MathUnaryOp), value func(int) T, bits func(T) uint64) {
	t.Helper()
	for _, rows := range []int{0, 1, 2, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1021, 4095, 4096, 4097, 8193} {
		for pattern := 0; pattern < 7; pattern++ {
			in := make([]T, rows+1)[1:]
			for i := range in {
				in[i] = value(i)
			}
			validity := mathMaskedValidity(rows, pattern)
			validityBefore := append(memory.Bitmap(nil), validity...)
			original := append([]T(nil), in...)
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				want := make([]T, rows)
				fn(New(ModeScalar), in, want, validity, op)
				for _, mode := range []Mode{ModeAuto, ModeSIMD} {
					out := make([]T, rows+2)
					for i := range out {
						out[i] = -123
					}
					fn(New(mode), in, out[1:], validity, op)
					alias := append([]T(nil), in...)
					fn(New(mode), alias, alias, validity, op)
					for i, x := range want {
						equal := func(y T) bool { return bits(x) == bits(y) || math.IsNaN(float64(x)) && math.IsNaN(float64(y)) }
						if !equal(out[i+1]) || !equal(alias[i]) {
							t.Fatalf("rows=%d pattern=%d mode=%s op=%s row=%d got=%x alias=%x want=%x", rows, pattern, mode, op, i, bits(out[i+1]), bits(alias[i]), bits(x))
						}
						if bits(in[i]) != bits(original[i]) {
							t.Fatal("input changed")
						}
					}
					if out[0] != -123 || out[len(out)-1] != -123 {
						t.Fatal("output canary changed")
					}
				}
			}
			if !reflect.DeepEqual(validity, validityBefore) {
				t.Fatal("validity changed")
			}
		}
	}
}

func TestMathMaskedNativeParity(t *testing.T) {
	r := rand.New(rand.NewPCG(173, 419))
	values32 := []uint32{0, 0x80000000, 1, 0x80000001, 0x007fffff, 0x00800000, 0x3f800000, 0xbf800000, 0x7f7fffff, 0xff7fffff, 0x7f800000, 0xff800000, 0x7fc01234, 0xffc01234, 0x7f800001, 0xff800001}
	values64 := []uint64{0, 0x8000000000000000, 1, 0x8000000000000001, 0x000fffffffffffff, 0x0010000000000000, 0x3ff0000000000000, 0xbff0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0xfff8000000001234, 0x7ff0000000000001, 0xfff0000000000001}
	for i := 0; i < 8193; i++ {
		values32 = append(values32, r.Uint32())
		values64 = append(values64, r.Uint64())
	}
	t.Run("Float32", func(t *testing.T) {
		testMathMasked(t, Set.MathUnaryFloat32, func(i int) float32 { return math.Float32frombits(values32[i%len(values32)]) }, func(x float32) uint64 { return uint64(math.Float32bits(x)) })
	})
	t.Run("Float64", func(t *testing.T) {
		testMathMasked(t, Set.MathUnaryFloat64, func(i int) float64 { return math.Float64frombits(values64[i%len(values64)]) }, math.Float64bits)
	})
}

func TestMathMaskedZeroAllocations(t *testing.T) {
	in32, out32 := make([]float32, 4099), make([]float32, 4099)
	in64, out64 := make([]float64, 4099), make([]float64, 4099)
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for pattern := 0; pattern < 7; pattern++ {
			v := mathMaskedValidity(len(in32), pattern)
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				if a := testing.AllocsPerRun(20, func() { s.MathUnaryFloat32(in32, out32, v, op); s.MathUnaryFloat64(in64, out64, v, op) }); a != 0 {
					t.Fatalf("mode=%s op=%s pattern=%d allocs=%v", mode, op, pattern, a)
				}
			}
		}
	}
}

func BenchmarkMathMasked(b *testing.B) {
	for _, rows := range []int{65, 1021, 65539} {
		for _, typ := range []string{"Float32", "Float64"} {
			for _, op := range []MathUnaryOp{MathAbs, MathSqrt} {
				for pattern := 0; pattern < 7; pattern++ {
					b.Run(fmt.Sprintf("%s/%s/rows%d/mask%d", typ, op, rows, pattern), func(b *testing.B) {
						s := New(ModeAuto)
						in32, out32 := make([]float32, rows), make([]float32, rows)
						in64, out64 := make([]float64, rows), make([]float64, rows)
						for i := range in32 {
							in32[i] = float32(i) + .25
							in64[i] = float64(i) + .25
						}
						v := mathMaskedValidity(rows, pattern)
						b.ReportAllocs()
						b.ResetTimer()
						if typ == "Float32" {
							for b.Loop() {
								s.MathUnaryFloat32(in32, out32, v, op)
							}
						} else {
							for b.Loop() {
								s.MathUnaryFloat64(in64, out64, v, op)
							}
						}
					})
				}
			}
		}
	}
}
