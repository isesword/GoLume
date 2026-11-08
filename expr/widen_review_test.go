package expr

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func reviewWidenRegister(typ memory.DataType, n, shift int) scalarRegister {
	r := scalarRegister{typ: typ}
	signed := []int64{0, 1, -1, math.MinInt64, math.MaxInt64, 1<<53 + 1, -(1<<53 + 1), math.MinInt32, math.MaxInt32}
	unsigned := []uint64{0, 1, math.MaxUint64, 1<<63 + 1, 1<<53 + 1, math.MaxUint32, 1 << 31, 255, 65535}
	floats := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.NaN(), math.SmallestNonzeroFloat32, math.MaxFloat32, 1 + 1.0/(1<<23), -3.25}
	for i := 0; i < n; i++ {
		s, u, f := signed[(i+shift)%len(signed)], unsigned[(i+shift)%len(unsigned)], floats[(i+shift)%len(floats)]
		r.i8 = append(r.i8, int8(s))
		r.i16 = append(r.i16, int16(s))
		r.i32 = append(r.i32, int32(s))
		r.i64 = append(r.i64, s)
		r.u8 = append(r.u8, uint8(u))
		r.u16 = append(r.u16, uint16(u))
		r.u32 = append(r.u32, uint32(u))
		r.u64 = append(r.u64, u)
		r.f32 = append(r.f32, float32(f))
		r.f64 = append(r.f64, f)
	}
	return r
}

func reviewWidenSame(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b) || (math.IsNaN(a) && math.IsNaN(b))
}

// The oracle intentionally follows the pre-widening expression evaluator:
// convert each original operand separately, then perform Float64 arithmetic.
func TestWidenReviewAllTypesIEEEAndValidity(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, n := range []int{0, 1, 3, 4, 5, 63, 64, 65, 131} {
			for _, at := range types {
				for _, bt := range types {
					for _, op := range []Op{OpAdd, OpSub, OpMul, OpDiv} {
						for _, nullable := range []bool{false, true} {
							a, b := reviewWidenRegister(at, n, 0), reviewWidenRegister(bt, n, 4)
							if nullable {
								a.validity = make(memory.Bitmap, memory.WordsFor(n))
								b.validity = make(memory.Bitmap, memory.WordsFor(n))
								for i := 0; i < n; i++ {
									if i%5 != 0 {
										memory.Set(a.validity, i)
									}
									if i%7 != 0 {
										memory.Set(b.validity, i)
									}
								}
							}
							out := scalarRegister{typ: memory.TypeFloat64, f64: make([]float64, n)}
							out.ownedValidity = make(memory.Bitmap, memory.WordsFor(n))
							for i := range out.f64 {
								out.f64[i] = 12345
							}
							if err := evalBinary(&out, &a, &b, op, memory.TypeFloat64, n, kernel.New(mode)); err != nil {
								t.Fatal(err)
							}
							for i := 0; i < n; i++ {
								valid := isValidRegister(&a, i) && isValidRegister(&b, i)
								want := 0.0
								if valid {
									x, _ := registerFloat64(&a, i)
									y, _ := registerFloat64(&b, i)
									switch op {
									case OpAdd:
										want = x + y
									case OpSub:
										want = x - y
									case OpMul:
										want = x * y
									case OpDiv:
										want = x / y
									}
								}
								if isValidRegister(&out, i) != valid || !reviewWidenSame(out.f64[i], want) {
									t.Fatalf("mode=%v n=%d %s %v %s nullable=%v row=%d got=%v want=%v valid=%v", mode, n, at, op, bt, nullable, i, out.f64[i], want, valid)
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestWidenReviewExpressionChain(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64, memory.TypeInt32, memory.TypeUint64} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
			t.Run(fmt.Sprintf("%v/%v", typ, mode), func(t *testing.T) {
				var w evaluationState
				defer w.Close()
				for _, n := range []int{131, 1, 65, 0, 4, 63, 131} {
					schema, batch := wideningFixture(n, typ, true)
					expression := Col("a").Sub(Col("b")).Mul(Col("a")).Div(Col("b")).Add(F64(0.1))
					p, err := Compile(schema, expression)
					if err != nil {
						t.Fatal(err)
					}
					got, err := evaluateValueTest(p, batch, &w, kernel.New(mode))
					if err != nil {
						t.Fatal(err)
					}
					for i := 0; i < n; i++ {
						valid := i%17 != 0
						if got.Float64.IsValid(i) != valid {
							t.Fatalf("row %d validity", i)
						}
						if !valid {
							continue
						}
						a := float64(batch.Float32s["a"].Values[i])
						var b float64
						switch typ {
						case memory.TypeFloat32:
							b = float64(batch.Float32s["b"].Values[i])
						case memory.TypeFloat64:
							b = batch.Float64s["b"].Values[i]
						case memory.TypeInt32:
							b = float64(batch.Int32s["b"].Values[i])
						case memory.TypeUint64:
							b = float64(batch.Uint64s["b"].Values[i])
						}
						want := (a-b)*a/b + 0.1
						if !reviewWidenSame(got.Float64.Values[i], want) {
							t.Fatalf("row %d got %v want %v", i, got.Float64.Values[i], want)
						}
					}
				}
			})
		}
	}
}

// Public workspace reservations must stabilize after warming. Selected execution
// also checks row order and NULL preservation independently of dense evaluation.
func TestWidenReviewBudgetAndSelection(t *testing.T) {
	ctx := context.Background()
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64, memory.TypeInt32, memory.TypeUint64} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
			schema, batch := wideningFixture(131, typ, true)
			p, err := Compile(schema, Col("b").Sub(Col("a")).Mul(Col("a")).Div(Col("b")))
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(2 << 20)
			w, err := NewWorkspace(ctx, p, manager)
			if err != nil {
				t.Fatal(err)
			}
			set := kernel.New(mode)
			dense, err := w.Evaluate(ctx, batch, set)
			if err != nil {
				t.Fatal(err)
			}
			values := append([]float64(nil), dense[0].Float64.Values...)
			rows := []int{0, 1, 3, 17, 64, 65, 129, 130}
			selected, err := w.EvaluateSelected(ctx, batch, rows, set)
			if err != nil {
				t.Fatal(err)
			}
			for i, row := range rows {
				if selected[0].Float64.IsValid(i) != (row%17 != 0) {
					t.Fatalf("selection validity at %d", i)
				}
				if row%17 != 0 && !reviewWidenSame(selected[0].Float64.Values[i], values[row]) {
					t.Fatalf("selection value at %d", i)
				}
			}
			used, bytes := manager.Used(), w.MemoryBytes()
			allocs := testing.AllocsPerRun(20, func() {
				if _, err := w.Evaluate(ctx, batch, set); err != nil {
					t.Fatal(err)
				}
				if _, err := w.EvaluateSelected(ctx, batch, rows, set); err != nil {
					t.Fatal(err)
				}
			})
			// Each call exports a single vector header, not an unaccounted row array.
			if allocs > 2 {
				t.Fatalf("warmed expression allocated %v objects", allocs)
			}
			if manager.Used() != used || w.MemoryBytes() != bytes {
				t.Fatal("warmed workspace memory grew")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatal("workspace leaked its reservation")
			}
		}
	}
}
