package kernel

import (
	"gocolumn/memory"
	"math"
	"testing"
)

type reviewWidenNumber interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32
}

func reviewMixedAlias[T reviewWidenNumber](t *testing.T, values []T, cast func([]T, []float64, memory.Bitmap), mixed func([]float64, []T, []float64, memory.Bitmap, ArithmeticOp, bool)) {
	t.Helper()
	inAlloc := make([]T, 131)
	outAlloc := make([]float64, 131)
	validityAlloc := memory.Bitmap{^uint64(0), ^uint64(0), 7}
	for i := range inAlloc {
		inAlloc[i] = values[i%len(values)]
	}
	if allocs := testing.AllocsPerRun(20, func() {
		cast(inAlloc, outAlloc, validityAlloc)
		mixed(outAlloc, inAlloc, outAlloc, validityAlloc, ArithmeticDiv, true)
	}); allocs != 0 {
		t.Fatalf("widening kernels allocated %v objects", allocs)
	}

	for _, n := range []int{0, 1, 3, 4, 5, 63, 64, 65, 129} {
		in := make([]T, n+2)
		for i := range in {
			in[i] = values[i%len(values)]
		}
		in = in[1 : n+1] // unaligned loads and exact-length tails
		for _, nulls := range []bool{false, true} {
			var valid memory.Bitmap
			if nulls {
				valid = make(memory.Bitmap, memory.WordsFor(n))
				for i := 0; i < n; i++ {
					if i%9 != 0 {
						memory.Set(valid, i)
					}
				}
			}
			out := make([]float64, n+2)
			out[0] = 17
			out[n+1] = 19
			cast(in, out[1:n+1], valid)
			for i := 0; i < n; i++ {
				want := float64(in[i])
				if !validAt(valid, i) {
					want = 0
				}
				if !reviewWidenEqual(out[i+1], want) {
					t.Fatalf("cast n=%d i=%d got=%v want=%v", n, i, out[i+1], want)
				}
			}
			for _, reverse := range []bool{false, true} {
				for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
					left := out[1 : n+1]
					for i := range left {
						left[i] = float64(i%11) - 5.5
					}
					want := make([]float64, n)
					for i := range want {
						if !validAt(valid, i) {
							continue
						}
						a, b := left[i], float64(in[i])
						if reverse {
							a, b = b, a
						}
						switch op {
						case ArithmeticAdd:
							want[i] = a + b
						case ArithmeticSub:
							want[i] = a - b
						case ArithmeticMul:
							want[i] = a * b
						case ArithmeticDiv:
							want[i] = a / b
						}
					}
					mixed(left, in, left, valid, op, reverse)
					for i := range want {
						if !reviewWidenEqual(left[i], want[i]) {
							t.Fatalf("alias n=%d op=%v reverse=%v i=%d got=%v want=%v", n, op, reverse, i, left[i], want[i])
						}
					}
					if out[0] != 17 || out[n+1] != 19 {
						t.Fatal("write outside output")
					}
				}
			}
		}
	}
}
func reviewWidenEqual(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b) || (math.IsNaN(a) && math.IsNaN(b))
}
func TestWidenReviewMixedExactAlias(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewMixedAlias(t, []int8{0, -128, 127, -1}, s.WidenInt8ToFloat64, s.Float64Int8Arithmetic)
		reviewMixedAlias(t, []int16{0, -32768, 32767, -1}, s.WidenInt16ToFloat64, s.Float64Int16Arithmetic)
		reviewMixedAlias(t, []int32{0, math.MinInt32, math.MaxInt32, -1}, s.WidenInt32ToFloat64, s.Float64Int32Arithmetic)
		reviewMixedAlias(t, []int64{0, math.MinInt64, math.MaxInt64, 1<<53 + 1, -1}, s.WidenInt64ToFloat64, s.Float64Int64Arithmetic)
		reviewMixedAlias(t, []uint8{0, 128, 255, 1}, s.WidenUint8ToFloat64, s.Float64Uint8Arithmetic)
		reviewMixedAlias(t, []uint16{0, 32768, 65535, 1}, s.WidenUint16ToFloat64, s.Float64Uint16Arithmetic)
		reviewMixedAlias(t, []uint32{0, 1 << 31, math.MaxUint32, 1}, s.WidenUint32ToFloat64, s.Float64Uint32Arithmetic)
		reviewMixedAlias(t, []uint64{0, 1 << 63, math.MaxUint64, 1<<53 + 1, 1}, s.WidenUint64ToFloat64, s.Float64Uint64Arithmetic)
		reviewMixedAlias(t, []float32{0, float32(math.Copysign(0, -1)), float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), math.MaxFloat32, math.SmallestNonzeroFloat32, -1}, s.WidenFloat32ToFloat64, s.Float64Float32Arithmetic)
	}
}

// Exercise integer-to-Float64 ties and carries independently of the production
// scalar kernel, including values whose correct conversion rounds to 2^64.
func TestWidenReview64BitRounding(t *testing.T) {
	raw := []uint64{0, 1, math.MaxUint64, math.MaxInt64, 1 << 63, 1<<63 + 1}
	for exponent := 53; exponent <= 63; exponent++ {
		pivot := uint64(1) << exponent
		for delta := int64(-4097); delta <= 4097; delta++ {
			value := pivot + uint64(delta)
			raw = append(raw, value, -value)
		}
	}
	for delta := uint64(0); delta <= 4097; delta++ {
		raw = append(raw, math.MaxUint64-delta)
	}
	random := uint64(0x123456789abcdef)
	for i := 0; i < 65539; i++ {
		random ^= random << 13
		random ^= random >> 7
		random ^= random << 17
		raw = append(raw, random)
	}
	signed := make([]int64, len(raw))
	for i, v := range raw {
		signed[i] = int64(v)
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		reviewWideRounding(t, raw, s.WidenUint64ToFloat64, s.Float64Uint64Arithmetic)
		reviewWideRounding(t, signed, s.WidenInt64ToFloat64, s.Float64Int64Arithmetic)
	}
}
func reviewWideRounding[T interface{ ~uint64 | ~int64 }](t *testing.T, input []T, cast func([]T, []float64, memory.Bitmap), mixed func([]float64, []T, []float64, memory.Bitmap, ArithmeticOp, bool)) {
	t.Helper()
	out := make([]float64, len(input))
	left := make([]float64, len(input))
	for i := range left {
		left[i] = float64(i%117) - 58.25
	}
	for _, nullable := range []bool{false, true} {
		var v memory.Bitmap
		if nullable {
			v = make(memory.Bitmap, memory.WordsFor(len(input)))
			for i := range input {
				if i%19 != 0 {
					memory.Set(v, i)
				}
			}
		}
		cast(input, out, v)
		for i, x := range input {
			want := float64(x)
			if !validAt(v, i) {
				want = 0
			}
			if !reviewWidenEqual(out[i], want) {
				t.Fatalf("conversion index=%d input=%v got=%.17g want=%.17g", i, x, out[i], want)
			}
		}
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
			for _, reverse := range []bool{false, true} {
				mixed(left, input, out, v, op, reverse)
				for i, x := range input {
					want := 0.0
					if validAt(v, i) {
						a, b := left[i], float64(x)
						if reverse {
							a, b = b, a
						}
						switch op {
						case ArithmeticAdd:
							want = a + b
						case ArithmeticSub:
							want = a - b
						case ArithmeticMul:
							want = a * b
						case ArithmeticDiv:
							want = a / b
						}
					}
					if !reviewWidenEqual(out[i], want) {
						t.Fatalf("mixed index=%d input=%v op=%v reverse=%v got=%.17g want=%.17g", i, x, op, reverse, out[i], want)
					}
				}
			}
		}
	}
}
