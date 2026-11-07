package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestMathIntegerBoundaries(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			s := New(mode)
			a := []int64{math.MinInt64, -5, 5, 0, 2, 2, math.MaxInt64}
			b := []int64{-1, 2, -2, 0, 63, 64, 1}
			out := make([]int64, len(a))
			fail := memory.Bitmap{^uint64(0)}
			s.MathUnaryInt64(a, out, nil, MathAbs)
			if out[0] != math.MinInt64 || out[1] != 5 {
				t.Fatalf("abs %v", out)
			}
			if n := s.MathBinaryInt64(a, b, out, nil, fail, MathFloorDiv); n != 1 || fail[0] != 8 {
				t.Fatalf("failure count/bitmap %d %x", n, fail)
			}
			if out[0] != math.MinInt64 || out[1] != -3 || out[2] != -3 {
				t.Fatalf("floor-div %v", out)
			}
			s.MathBinaryInt64(a, b, out, nil, fail, MathMod)
			if out[0] != 0 || out[1] != 1 || out[2] != -1 {
				t.Fatalf("mod %v", out)
			}
			s.MathBinaryInt64(a, b, out, nil, fail, MathPow)
			if fail[0] != 5 || out[4] != math.MinInt64 || out[5] != 0 || out[3] != 1 {
				t.Fatalf("pow out=%v fail=%x", out, fail)
			}
			valid := memory.Bitmap{^uint64(0) &^ 8}
			if n := s.MathBinaryInt64(a, b, out, valid, fail, MathFloorDiv); n != 0 || fail[0] != 0 || out[3] != 0 {
				t.Fatalf("masked divisor zero %v %x", out, fail)
			}
			ua := []uint64{math.MaxUint64, math.MaxUint64, 2}
			ub := []uint64{3, 1, 64}
			uo := make([]uint64, 3)
			s.MathBinaryUint64(ua, ub, uo, nil, nil, MathFloorDiv)
			if uo[0] != math.MaxUint64/3 || uo[1] != math.MaxUint64 {
				t.Fatalf("uint64 precision %v", uo)
			}
			s.MathBinaryUint64(ua, ub, uo, nil, nil, MathPow)
			if uo[2] != 0 {
				t.Fatalf("uint64 pow wrap %v", uo)
			}
		})
	}
}

func equalMathFloat(a, b float64) bool {
	return (math.IsNaN(a) && math.IsNaN(b)) || math.Float64bits(a) == math.Float64bits(b)
}
func TestMathFloatSemantics(t *testing.T) {
	s := New(ModeScalar)
	a := []float64{-5, 5, 1e20, -1e20, 0, math.Copysign(0, -1), 3, math.Inf(1), math.NaN()}
	b := []float64{2, -2, 3, 3, -2, 2, math.Inf(1), 2, 1}
	out := make([]float64, len(a))
	fail := memory.Bitmap{^uint64(0)}
	s.MathBinaryFloat64(a, b, out, nil, fail, MathMod)
	want := []float64{1, -1, 1, 2, math.Copysign(0, -1), 0, math.NaN(), math.NaN(), math.NaN()}
	if fail[0] != 0 {
		t.Fatal("float failures not cleared")
	}
	for i, v := range want {
		if !equalMathFloat(out[i], v) {
			t.Fatalf("mod[%d]=%v want %v", i, out[i], v)
		}
	}
	values := []float64{-2.5, -1.5, 1.5, 2.5, math.Copysign(0, -1), math.Inf(1), math.NaN()}
	rounded := make([]float64, len(values))
	s.MathUnaryFloat64(values, rounded, nil, MathRound)
	expected := []float64{-2, -2, 2, 2, math.Copysign(0, -1), math.Inf(1), math.NaN()}
	for i, v := range expected {
		if !equalMathFloat(rounded[i], v) {
			t.Fatalf("round[%d]=%v", i, rounded[i])
		}
	}
	for _, op := range []MathUnaryOp{MathAbs, MathRound, MathFloor, MathCeil, MathSqrt, MathLog} {
		scalarMathFloatUnary(values, rounded, nil, op)
		s.MathUnaryFloat64(values, out[:len(values)], nil, op)
		for i, v := range rounded {
			if !equalMathFloat(out[i], v) {
				t.Fatalf("%s[%d]", op, i)
			}
		}
	}
}

func TestMathFloatModeDifferential(t *testing.T) {
	for _, rows := range []int{0, 1, 3, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 129, 1024} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/nullable=%v", rows, nullable), func(t *testing.T) {
				seeds := []float64{math.Copysign(0, -1), 0, -1, 2, 4, math.SmallestNonzeroFloat64, math.MaxFloat64, math.Inf(-1), math.Inf(1), math.NaN(), -2.5, 2.5}
				in := make([]float64, rows)
				in32 := make([]float32, rows)
				var valid memory.Bitmap
				if nullable {
					valid = make(memory.Bitmap, memory.WordsFor(rows))
				}
				for i := range in {
					in[i] = seeds[i%len(seeds)]
					in32[i] = float32(in[i])
					if nullable && i%3 != 0 {
						memory.Set(valid, i)
					}
				}
				want, got := make([]float64, rows), make([]float64, rows)
				want32, got32 := make([]float32, rows), make([]float32, rows)
				for op := MathAbs; op <= MathLog; op++ {
					New(ModeScalar).MathUnaryFloat64(in, want, valid, op)
					New(ModeScalar).MathUnaryFloat32(in32, want32, valid, op)
					for _, mode := range []Mode{ModeSIMD, ModeAuto} {
						New(mode).MathUnaryFloat64(in, got, valid, op)
						New(mode).MathUnaryFloat32(in32, got32, valid, op)
						for i := range in {
							if !equalMathFloat(got[i], want[i]) || !equalMathFloat(float64(got32[i]), float64(want32[i])) {
								t.Fatalf("%s/%s row=%d got=(%v,%v) want=(%v,%v)", mode, op, i, got[i], got32[i], want[i], want32[i])
							}
						}
					}
				}
			})
		}
	}
}

func TestMathIDsAndCapabilities(t *testing.T) {
	for _, id := range KernelIDs() {
		if id.Kind() != KernelMathUnary && id.Kind() != KernelMathBinary {
			continue
		}
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			cap, ok := New(mode).Capability(id)
			if !ok {
				t.Fatalf("missing %s", id)
			}
			if mode != ModeSIMD && (cap.HardwareSIMD || cap.Backend != "scalar") {
				t.Fatalf("ungated math auto backend %s %+v", id, cap)
			}
		}
	}
	if _, ok := MathUnaryID(memory.TypeInt64, MathSqrt); ok {
		t.Fatal("integer sqrt ID should require floating conversion")
	}
	if _, ok := MathBinaryID(memory.TypeString, MathPow); ok {
		t.Fatal("string math accepted")
	}
}

func BenchmarkMathUnary(b *testing.B) {
	for _, rows := range []int{64, 1024, 65536} {
		for _, op := range []MathUnaryOp{MathAbs, MathSqrt, MathRound, MathLog} {
			for _, mode := range []Mode{ModeScalar, ModeSIMD} {
				b.Run(fmt.Sprintf("%s/%s/%d", op, mode, rows), func(b *testing.B) {
					in, out := make([]float64, rows), make([]float64, rows)
					for i := range in {
						in[i] = float64(i) + 0.5
					}
					s := New(mode)
					b.SetBytes(int64(rows) * 8)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						s.MathUnaryFloat64(in, out, nil, op)
					}
				})
			}
		}
	}
}

func TestMathAllNativeWidths(t *testing.T) {
	s := New(ModeScalar)
	t.Run("Int8", func(t *testing.T) {
		a, b, o := []int8{5, 0, 2}, []int8{2, 0, 3}, make([]int8, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryInt8(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryInt8(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt8(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt8(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Int16", func(t *testing.T) {
		a, b, o := []int16{5, 0, 2}, []int16{2, 0, 3}, make([]int16, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryInt16(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryInt16(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt16(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt16(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Int32", func(t *testing.T) {
		a, b, o := []int32{5, 0, 2}, []int32{2, 0, 3}, make([]int32, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryInt32(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryInt32(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt32(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt32(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Int64", func(t *testing.T) {
		a, b, o := []int64{5, 0, 2}, []int64{2, 0, 3}, make([]int64, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryInt64(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryInt64(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt64(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryInt64(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Uint8", func(t *testing.T) {
		a, b, o := []uint8{5, 0, 2}, []uint8{2, 0, 3}, make([]uint8, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryUint8(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryUint8(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint8(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint8(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Uint16", func(t *testing.T) {
		a, b, o := []uint16{5, 0, 2}, []uint16{2, 0, 3}, make([]uint16, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryUint16(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryUint16(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint16(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint16(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Uint32", func(t *testing.T) {
		a, b, o := []uint32{5, 0, 2}, []uint32{2, 0, 3}, make([]uint32, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryUint32(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryUint32(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint32(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint32(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
	t.Run("Uint64", func(t *testing.T) {
		a, b, o := []uint64{5, 0, 2}, []uint64{2, 0, 3}, make([]uint64, 3)
		for op := MathAbs; op <= MathCeil; op++ {
			s.MathUnaryUint64(a, o, nil, op)
			if o[0] != 5 {
				t.Fatal(o)
			}
		}
		f := memory.Bitmap{0}
		s.MathBinaryUint64(a, b, o, nil, f, MathMod)
		if o[0] != 1 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint64(a, b, o, nil, f, MathFloorDiv)
		if o[0] != 2 || f[0] != 2 {
			t.Fatal(o, f)
		}
		s.MathBinaryUint64(a, b, o, nil, f, MathPow)
		if o[2] != 8 || o[1] != 1 || f[0] != 0 {
			t.Fatal(o, f)
		}
	})
}

func TestMathExhaustiveInt8Division(t *testing.T) {
	s := New(ModeScalar)
	a, b := make([]int8, 65536), make([]int8, 65536)
	for i := -128; i < 128; i++ {
		for j := -128; j < 128; j++ {
			n := (i+128)*256 + j + 128
			a[n] = int8(i)
			b[n] = int8(j)
		}
	}
	div, mod := make([]int8, len(a)), make([]int8, len(a))
	fail := make(memory.Bitmap, memory.WordsFor(len(a)))
	if n := s.MathBinaryInt8(a, b, div, nil, fail, MathFloorDiv); n != 256 {
		t.Fatal(n)
	}
	s.MathBinaryInt8(a, b, mod, nil, nil, MathMod)
	for i, x := range a {
		y := b[i]
		if y == 0 {
			if !memory.IsSet(fail, i) {
				t.Fatal("missing zero")
			}
			continue
		}
		q := int(math.Floor(float64(x) / float64(y)))
		r := int(x) - q*int(y)
		if div[i] != int8(q) || mod[i] != int8(r) {
			t.Fatalf("%d/%d got q=%d r=%d want q=%d r=%d", x, y, div[i], mod[i], int8(q), int8(r))
		}
	}
}
