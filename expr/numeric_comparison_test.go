package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"slices"
	"testing"
)

type comparisonNumber interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func numericComparisonRegister[T comparisonNumber](values []T) *scalarRegister {
	r := new(scalarRegister)
	switch v := any(values).(type) {
	case []int8:
		r.typ, r.i8 = memory.TypeInt8, v
	case []int16:
		r.typ, r.i16 = memory.TypeInt16, v
	case []int32:
		r.typ, r.i32 = memory.TypeInt32, v
	case []int64:
		r.typ, r.i64 = memory.TypeInt64, v
	case []uint8:
		r.typ, r.u8 = memory.TypeUint8, v
	case []uint16:
		r.typ, r.u16 = memory.TypeUint16, v
	case []uint32:
		r.typ, r.u32 = memory.TypeUint32, v
	case []uint64:
		r.typ, r.u64 = memory.TypeUint64, v
	case []float32:
		r.typ, r.f32 = memory.TypeFloat32, v
	case []float64:
		r.typ, r.f64 = memory.TypeFloat64, v
	}
	return r
}

func numericComparisonOracle[T comparisonNumber](a, b T, op Op) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	}
	return false
}

func checkNumericRegisterComparison[T comparisonNumber](t *testing.T, edges []T) {
	t.Helper()
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		kernels := kernel.New(mode)
		for rows := 0; rows <= 129; rows++ {
			// Unaligned input includes a physical suffix outside the logical batch.
			av, bv := make([]T, rows+2)[1:], make([]T, rows+2)[1:]
			for row := range av {
				av[row] = edges[row%len(edges)]
				bv[row] = edges[(row/len(edges)+3)%len(edges)]
			}
			a, b := numericComparisonRegister(av), numericComparisonRegister(bv)
			for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
				for _, profile := range []string{"valid", "mixed", "null"} {
					words := memory.WordsFor(rows)
					valid := make(memory.Bitmap, words)
					for row := 0; row < rows; row++ {
						if profile == "valid" || profile == "mixed" && row%7 != 0 && row%11 != 0 {
							memory.Set(valid, row)
						}
					}
					if words > 0 {
						valid[words-1] |= ^memory.TailMask(rows - (words-1)*64)
					}
					before := slices.Clone(valid)
					got, want := make(memory.Bitmap, words+1), make(memory.Bitmap, words+1)
					for i := range got {
						got[i] = ^uint64(0)
					}
					got[words], want[words] = 0x5eed, 0x5eed
					out := &scalarRegister{BoolVector: BoolVector{Truth: got}, validity: valid}
					if !evalNumericRegisterComparison(out, a, b, op, rows, kernels) {
						t.Fatal("numeric dense registers not dispatched")
					}
					for row := 0; row < rows; row++ {
						if memory.IsSet(valid, row) && numericComparisonOracle(av[row], bv[row], op) {
							memory.Set(want, row)
						}
					}
					if !slices.Equal(got, want) || !slices.Equal(valid, before) {
						t.Fatalf("type=%s mode=%s rows=%d op=%s profile=%s got=%x want=%x", a.typ, mode, rows, op, profile, got, want)
					}
				}
			}
		}
	}
}

func TestNumericRegisterComparisonAllTypes(t *testing.T) {
	t.Run("int8", func(t *testing.T) { checkNumericRegisterComparison(t, []int8{math.MinInt8, -1, 0, 1, math.MaxInt8}) })
	t.Run("int16", func(t *testing.T) { checkNumericRegisterComparison(t, []int16{math.MinInt16, -1, 0, 1, math.MaxInt16}) })
	t.Run("int32", func(t *testing.T) { checkNumericRegisterComparison(t, []int32{math.MinInt32, -1, 0, 1, math.MaxInt32}) })
	t.Run("int64", func(t *testing.T) {
		checkNumericRegisterComparison(t, []int64{math.MinInt64, -1, 0, 1, 1<<53 + 1, math.MaxInt64})
	})
	t.Run("uint8", func(t *testing.T) { checkNumericRegisterComparison(t, []uint8{0, 1, 1 << 7, math.MaxUint8}) })
	t.Run("uint16", func(t *testing.T) { checkNumericRegisterComparison(t, []uint16{0, 1, 1 << 15, math.MaxUint16}) })
	t.Run("uint32", func(t *testing.T) { checkNumericRegisterComparison(t, []uint32{0, 1, 1 << 31, math.MaxUint32}) })
	t.Run("uint64", func(t *testing.T) {
		checkNumericRegisterComparison(t, []uint64{0, 1, 1<<53 + 1, 1 << 63, math.MaxUint64})
	})
	t.Run("float32", func(t *testing.T) {
		checkNumericRegisterComparison(t, []float32{float32(math.NaN()), float32(math.Inf(-1)), float32(math.Inf(1)), float32(math.Copysign(0, -1)), 0, math.SmallestNonzeroFloat32, math.MaxFloat32, -1, 1})
	})
	t.Run("float64", func(t *testing.T) {
		checkNumericRegisterComparison(t, []float64{math.NaN(), math.Inf(-1), math.Inf(1), math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, math.MaxFloat64, -1, 1})
	})
}

func TestNumericRegisterComparisonCompactLiterals(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		k := kernel.New(mode)
		for _, floating := range []bool{false, true} {
			for _, reverse := range []bool{false, true} {
				for _, op := range []Op{OpGT, OpGE, OpLT, OpLE, OpEQ} {
					const rows = 129
					a := numericComparisonRegister(make([]int64, rows))
					b := &scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 1<<53 + 1}
					if floating {
						a = numericComparisonRegister(make([]float64, rows))
						b.typ, b.f64lit = memory.TypeFloat64, 0
					}
					valid := memory.EnsureBitmap(nil, rows)
					for row := 0; row < rows; row++ {
						if row%7 != 0 {
							memory.Set(valid, row)
						}
						if floating {
							a.f64[row] = []float64{math.NaN(), math.Inf(-1), math.Copysign(0, -1), 0, math.Inf(1)}[row%5]
						} else {
							a.i64[row] = b.i64lit + int64(row%3) - 1
						}
					}
					out := &scalarRegister{BoolVector: BoolVector{Truth: memory.EnsureBitmap(nil, rows)}, validity: valid}
					left, right := a, b
					if reverse {
						left, right = b, a
					}
					if !evalNumericRegisterComparison(out, left, right, op, rows, k) {
						t.Fatal("compact literal not dispatched")
					}
					for row := 0; row < rows; row++ {
						var want bool
						if floating {
							x, y := a.f64[row], b.f64lit
							if reverse {
								x, y = y, x
							}
							want = numericComparisonOracle(x, y, op)
						} else {
							x, y := a.i64[row], b.i64lit
							if reverse {
								x, y = y, x
							}
							want = numericComparisonOracle(x, y, op)
						}
						want = want && memory.IsSet(valid, row)
						if memory.IsSet(out.Truth, row) != want {
							t.Fatalf("mode=%s float=%t reverse=%t op=%s row=%d", mode, floating, reverse, op, row)
						}
					}
					if allocs := testing.AllocsPerRun(20, func() { evalNumericRegisterComparison(out, left, right, op, rows, k) }); allocs != 0 {
						t.Fatalf("allocated %g", allocs)
					}
				}
			}
		}
	}
}

func TestNumericRegisterComparisonKeepsOtherDomains(t *testing.T) {
	k := kernel.New(kernel.ModeAuto)
	tests := []struct {
		a, b                 memory.DataType
		constantA, constantB bool
		op                   Op
	}{
		{memory.TypeInt64, memory.TypeFloat64, false, false, OpGT},
		{memory.TypeInt64, memory.TypeUint64, false, false, OpGT},
		{memory.TypeInt32, memory.TypeInt64, false, false, OpEQ},
		{memory.TypeBool, memory.TypeBool, false, false, OpEQ},
		{memory.TypeDecimal, memory.TypeDecimal, false, false, OpGT},
		{memory.TypeDate32, memory.TypeDate32, false, false, OpEQ},
		{memory.TypeTimestamp, memory.TypeTimestamp, false, false, OpEQ},
		{memory.TypeString, memory.TypeString, false, false, OpEQ},
		{memory.TypeInt64, memory.TypeInt64, true, true, OpGT},
		{memory.TypeInt64, memory.TypeInt64, false, false, OpAdd},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			out := &scalarRegister{BoolVector: BoolVector{Truth: memory.Bitmap{0x1234}}, validity: memory.Bitmap{0x5678}}
			if evalNumericRegisterComparison(out, &scalarRegister{typ: tc.a, constant: tc.constantA}, &scalarRegister{typ: tc.b, constant: tc.constantB}, tc.op, 1, k) {
				t.Fatal("unexpected dispatch")
			}
			if out.Truth[0] != 0x1234 || out.validity[0] != 0x5678 {
				t.Fatal("fallback changed output")
			}
		})
	}
}
