package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/rand/v2"
	"testing"
)

func TestNativeFloatArithmeticBitPatterns(t *testing.T) {
	edge32 := []uint32{0, 1, 0x80000000, 0x80000001, 0x007fffff, 0x00800000, 0x3f800000, 0xbf800000, 0x3f800001, 0x33800000, 0x7f7fffff, 0xff7fffff, 0x7f800000, 0xff800000, 0x7fc00001, 0x7f800001, 0xffc01234}
	edge64 := []uint64{0, 1, 0x8000000000000000, 0x8000000000000001, 0xfffffffffffff, 0x10000000000000, 0x3ff0000000000000, 0xbff0000000000000, 0x3ff0000000000001, 0x3ca0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000001, 0xfff8000000001234}
	const n = 65537
	a, b := make([]float32, n), make([]float32, n)
	x, y := make([]float64, n), make([]float64, n)
	rng := rand.New(rand.NewPCG(17, 23))
	for i := range a {
		if i < len(edge32)*len(edge32) {
			a[i] = math.Float32frombits(edge32[i%len(edge32)])
			b[i] = math.Float32frombits(edge32[i/len(edge32)])
		} else {
			a[i] = math.Float32frombits(rng.Uint32())
			b[i] = math.Float32frombits(rng.Uint32())
		}
		if i < len(edge64)*len(edge64) {
			x[i] = math.Float64frombits(edge64[i%len(edge64)])
			y[i] = math.Float64frombits(edge64[i/len(edge64)])
		} else {
			x[i] = math.Float64frombits(rng.Uint64())
			y[i] = math.Float64frombits(rng.Uint64())
		}
	}
	valid := column32GateValidity(n, "mixed")
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		for _, v := range []memory.Bitmap{nil, valid} {
			for op := ArithmeticAdd; op <= ArithmeticDiv; op++ {
				got32, got64 := make([]float32, n), make([]float64, n)
				s.Float32Arithmetic(a, b, got32, v, op)
				s.Float64Arithmetic(x, y, got64, v, op)
				for i := range a {
					var w32 float32
					var w64 float64
					if v == nil || v[i/64]&(1<<uint(i%64)) != 0 {
						switch op {
						case ArithmeticAdd:
							w32 = a[i] + b[i]
							w64 = x[i] + y[i]
						case ArithmeticSub:
							w32 = a[i] - b[i]
							w64 = x[i] - y[i]
						case ArithmeticMul:
							w32 = a[i] * b[i]
							w64 = x[i] * y[i]
						case ArithmeticDiv:
							w32 = a[i] / b[i]
							w64 = x[i] / y[i]
						}
					}
					if math.Float32bits(got32[i]) != math.Float32bits(w32) && !(math.IsNaN(float64(got32[i])) && math.IsNaN(float64(w32))) {
						t.Fatalf("Float32 mode=%s op=%s row=%d bits=%x want=%x", mode, op, i, math.Float32bits(got32[i]), math.Float32bits(w32))
					}
					if math.Float64bits(got64[i]) != math.Float64bits(w64) && !(math.IsNaN(got64[i]) && math.IsNaN(w64)) {
						t.Fatalf("Float64 mode=%s op=%s row=%d bits=%x want=%x", mode, op, i, math.Float64bits(got64[i]), math.Float64bits(w64))
					}
				}
				if allocs := testing.AllocsPerRun(10, func() { s.Float32Arithmetic(a, b, got32, v, op); s.Float64Arithmetic(x, y, got64, v, op) }); allocs != 0 {
					t.Fatalf("mode=%s op=%s allocations=%g", mode, op, allocs)
				}
			}
		}
	}
}

func BenchmarkNativeFloatArithmetic(b *testing.B) {
	for _, n := range []int{7, 1025, 65536} {
		for _, nullable := range []bool{false, true} {
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				for op := ArithmeticAdd; op <= ArithmeticDiv; op++ {
					b.Run(fmt.Sprintf("float32/%s/rows=%d/null=%t/%s", op, n, nullable, mode), func(b *testing.B) {
						a, c, out := make([]float32, n), make([]float32, n), make([]float32, n)
						for i := range a {
							a[i] = float32(i) * 1.25
							c[i] = 2.5
						}
						var v memory.Bitmap
						if nullable {
							v = column32GateValidity(n, "mixed")
						}
						s := New(mode)
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							s.Float32Arithmetic(a, c, out, v, op)
						}
					})
					if op == ArithmeticAdd {
						b.Run(fmt.Sprintf("float64/%s/rows=%d/null=%t/%s", op, n, nullable, mode), func(b *testing.B) {
							a, c, out := make([]float64, n), make([]float64, n), make([]float64, n)
							for i := range a {
								a[i] = float64(i) * 1.25
								c[i] = 2.5
							}
							var v memory.Bitmap
							if nullable {
								v = column32GateValidity(n, "mixed")
							}
							s := New(mode)
							b.ReportAllocs()
							b.ResetTimer()
							for b.Loop() {
								s.Float64Arithmetic(a, c, out, v, op)
							}
						})
					}
				}
			}
		}
	}
}

func BenchmarkNativeFloat64Comparison(b *testing.B) {
	for _, n := range []int{7, 1025, 65536} {
		for _, nullable := range []bool{false, true} {
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				for _, columns := range []bool{false, true} {
					b.Run(fmt.Sprintf("columns=%t/rows=%d/null=%t/%s", columns, n, nullable, mode), func(b *testing.B) {
						a, c := make([]float64, n), make([]float64, n)
						for i := range a {
							a[i] = float64(i % 47)
							c[i] = 17
						}
						out := make(memory.Bitmap, memory.WordsFor(n))
						var v memory.Bitmap
						if nullable {
							v = column32GateValidity(n, "mixed")
						}
						s := New(mode)
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							if columns {
								s.CompareFloat64Columns(a, c, v, v, out, CompareGT)
							} else {
								s.CompareFloat64(a, v, out, 17, CompareGT)
							}
						}
					})
				}
			}
		}
	}
}
