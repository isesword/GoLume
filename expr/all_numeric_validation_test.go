package expr

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

var allNumericValidationTypes = []struct {
	typ   memory.DataType
	field string
}{
	{memory.TypeInt8, "Int8"}, {memory.TypeInt16, "Int16"}, {memory.TypeInt32, "Int32"}, {memory.TypeInt64, "Int64"},
	{memory.TypeUint8, "Uint8"}, {memory.TypeUint16, "Uint16"}, {memory.TypeUint32, "Uint32"}, {memory.TypeUint64, "Uint64"},
	{memory.TypeFloat32, "Float32"}, {memory.TypeFloat64, "Float64"},
}

// Reflection is limited to test fixture construction and observing public vectors;
// the oracle never calls the evaluator's conversion or arithmetic helpers.
func allNumericValidationPut(b *memory.Batch, field, name string, values []float64, valid memory.Bitmap) {
	m := reflect.ValueOf(b).Elem().FieldByName(field + "s")
	v := reflect.New(m.Type().Elem().Elem())
	dst := v.Elem().FieldByName("Values")
	dst.Set(reflect.MakeSlice(dst.Type(), len(values), len(values)))
	for i, x := range values {
		e := dst.Index(i)
		switch e.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			e.SetInt(int64(x))
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			e.SetUint(uint64(x))
		default:
			e.SetFloat(x)
		}
	}
	v.Elem().FieldByName("Validity").Set(reflect.ValueOf(valid))
	m.SetMapIndex(reflect.ValueOf(name), v)
}

func allNumericValidationRead(v ScalarValue, row int) (float64, bool) {
	field := "Uint32"
	for _, typ := range allNumericValidationTypes {
		if v.Type == typ.typ {
			field = typ.field
			break
		}
	}
	vec := reflect.ValueOf(v).FieldByName(field).Elem()
	valid := vec.FieldByName("Validity").Interface().(memory.Bitmap)
	x := vec.FieldByName("Values").Index(row)
	var n float64
	switch x.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n = float64(x.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n = float64(x.Uint())
	default:
		n = x.Float()
	}
	return n, (valid == nil || memory.IsSet(valid, row))
}

func TestAllNumericValidationCastsArithmeticComparisons(t *testing.T) {
	for _, src := range allNumericValidationTypes {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%v", src.typ, mode), func(t *testing.T) {
				schema := memory.NewSchema(memory.Field{Name: "a", Type: src.typ, Nullable: true}, memory.Field{Name: "b", Type: src.typ, Nullable: true})
				a, b := Col("a"), Col("b")
				shared := Add(a, b)
				named := []NamedExpr{Named(shared, "sum"), Named(Sub(shared, b), "subtract"), Named(Mul(a, b), "multiply"), Named(Div(a, b), "divide"), Named(shared, "duplicate"), Named(GT(a, b), "gt"), Named(GE(a, b), "ge"), Named(LT(a, b), "lt"), Named(LE(a, b), "le"), Named(EQ(a, b), "eq"), Named(Not(EQ(a, b)), "ne"), Named(IsNull(a), "null"), Named(IsNotNull(a), "notnull")}
				for _, dst := range allNumericValidationTypes {
					named = append(named, Named(CastTo(a, dst.typ), "cast_"+dst.field))
				}
				p, err := CompileExpressions(schema, named)
				if err != nil {
					t.Fatal(err)
				}
				for _, program := range []Program{p, withoutStoragePlan(p)} {
					w, err := NewWorkspace(context.Background(), program, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					for run, rows := range []int{0, 1, 7, 63, 64, 65, 129, 257, 3, 0, 65} {
						batch := memory.NewBatch()
						batch.Rows = rows
						av, bv := make([]float64, rows), make([]float64, rows)
						var va, vb memory.Bitmap
						if run%3 != 0 {
							va = make(memory.Bitmap, memory.WordsFor(rows))
							vb = make(memory.Bitmap, memory.WordsFor(rows))
						}
						for r := range rows {
							av[r] = float64((r*37 + run) % 101)
							bv[r] = float64((r*13 + run) % 11)
							if run%3 != 0 {
								if r%5 != 0 {
									memory.Set(va, r)
								}
								if r%7 != 0 {
									memory.Set(vb, r)
								}
							}
						}
						allNumericValidationPut(batch, src.field, "a", av, va)
						allNumericValidationPut(batch, src.field, "b", bv, vb)
						out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
						if err != nil {
							t.Fatalf("run=%d: %v", run, err)
						}
						for r := range rows {
							validA, validB := (va == nil || memory.IsSet(va, r)), (vb == nil || memory.IsSet(vb, r))
							boolnum := func(v bool) float64 {
								if v {
									return 1
								}
								return 0
							}
							want := []float64{av[r] + bv[r], av[r], av[r] * bv[r], av[r] / bv[r], av[r] + bv[r], boolnum(av[r] > bv[r]), boolnum(av[r] >= bv[r]), boolnum(av[r] < bv[r]), boolnum(av[r] <= bv[r]), boolnum(av[r] == bv[r]), boolnum(av[r] != bv[r]), boolnum(!validA), boolnum(validA)}
							for range allNumericValidationTypes {
								want = append(want, av[r])
							}
							for c, x := range want {
								valid := validA && validB
								if c == 11 || c == 12 {
									valid = true
								} else if c >= 13 {
									valid = validA
								}
								got, gotValid := allNumericValidationRead(out[c], r)
								if gotValid != valid || (valid && got != x && !(math.IsNaN(got) && math.IsNaN(x))) {
									t.Fatalf("run=%d row=%d output=%d got=%v/%v want=%v/%v", run, r, c, got, gotValid, x, valid)
								}
							}
						}
					}
				}
			})
		}
	}
}

func TestAllNumericValidationCheckedBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		e     Expr
		value int64
		fail  bool
	}{
		{"i8_max", CastTo(Col("x"), memory.TypeInt8), 127, false}, {"i8_min", CastTo(Col("x"), memory.TypeInt8), -128, false}, {"i8_over", CastTo(Col("x"), memory.TypeInt8), 128, true}, {"i8_under", CastTo(Col("x"), memory.TypeInt8), -129, true},
		{"i16_max", CastTo(Col("x"), memory.TypeInt16), 32767, false}, {"i16_min", CastTo(Col("x"), memory.TypeInt16), -32768, false}, {"i16_over", CastTo(Col("x"), memory.TypeInt16), 32768, true},
		{"i32_max", CastTo(Col("x"), memory.TypeInt32), math.MaxInt32, false}, {"i32_min", CastTo(Col("x"), memory.TypeInt32), math.MinInt32, false}, {"i32_over", CastTo(Col("x"), memory.TypeInt32), math.MaxInt32 + 1, true},
		{"u8_max", CastTo(Col("x"), memory.TypeUint8), 255, false}, {"u8_over", CastTo(Col("x"), memory.TypeUint8), 256, true},
		{"u16_max", CastTo(Col("x"), memory.TypeUint16), 65535, false}, {"u16_over", CastTo(Col("x"), memory.TypeUint16), 65536, true},
		{"u32_max", CastTo(Col("x"), memory.TypeUint32), math.MaxUint32, false}, {"u32_over", CastTo(Col("x"), memory.TypeUint32), math.MaxUint32 + 1, true},
		{"add_over", Add(Col("x"), I64(1)), math.MaxInt64, true}, {"sub_under", Sub(Col("x"), I64(1)), math.MinInt64, true}, {"mul_over", Mul(Col("x"), I64(2)), math.MaxInt64, true},
	}
	for _, tc := range cases {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%v", tc.name, mode), func(t *testing.T) {
				p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), tc.e)
				if err != nil {
					t.Fatal(err)
				}
				for _, program := range []Program{p, withoutStoragePlan(p)} {
					w, err := NewWorkspace(context.Background(), program, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					// Masked errors must not execute; a visible error closes the workspace.
					for _, masked := range []bool{true, true, false} {
						b := memory.NewBatch()
						b.Rows = 65
						v := &memory.Int64Vector{Values: make([]int64, 65)}
						for i := range v.Values {
							v.Values[i] = tc.value
						}
						if masked {
							v.Validity = make(memory.Bitmap, memory.WordsFor(65))
						}
						b.Int64s["x"] = v
						out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
						if tc.fail && !masked {
							if err == nil || !strings.Contains(err.Error(), "overflow") {
								t.Fatalf("wanted overflow, got %v", err)
							}
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						for r := range b.Rows {
							got, valid := allNumericValidationRead(out[0], r)
							if valid == masked || (!masked && got != float64(tc.value)) {
								t.Fatalf("row=%d got=%v/%v", r, got, valid)
							}
						}
					}
				}
			})
		}
	}
}

func TestAllNumericValidationBooleanTruthTables(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeBool, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeBool, Nullable: true})
	a, b := Col("a"), Col("b")
	and, or := And(a, b), Or(a, b)
	p, err := CompileExpressions(schema, []NamedExpr{Named(and, "and"), Named(or, "or"), Named(Not(a), "not"), Named(Not(and), "notand"), Named(Or(Not(a), Not(b)), "demorgan"), Named(and, "duplicate")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, program := range []Program{p, withoutStoragePlan(p)} {
			w, err := NewWorkspace(context.Background(), program, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for _, rows := range []int{0, 9, 63, 64, 65, 129, 1, 257} {
				batch := memory.NewBatch()
				batch.Rows = rows
				av, bv := &memory.Uint32Vector{Values: make([]uint32, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}, &memory.Uint32Vector{Values: make([]uint32, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
				for r := range rows {
					aa, bb := r%3, (r/3)%3
					if aa < 2 {
						memory.Set(av.Validity, r)
						av.Values[r] = uint32(aa)
					}
					if bb < 2 {
						memory.Set(bv.Validity, r)
						bv.Values[r] = uint32(bb)
					}
				}
				batch.Uint32s["a"] = av
				batch.Uint32s["b"] = bv
				out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for r := range rows {
					aa, bb := r%3, (r/3)%3
					andWant, orWant := 2, 2
					if aa == 0 || bb == 0 {
						andWant = 0
					} else if aa == 1 && bb == 1 {
						andWant = 1
					}
					if aa == 1 || bb == 1 {
						orWant = 1
					} else if aa == 0 && bb == 0 {
						orWant = 0
					}
					neg := func(x int) int {
						if x == 2 {
							return 2
						}
						return 1 - x
					}
					for c, want := range []int{andWant, orWant, neg(aa), neg(andWant), neg(andWant), andWant} {
						got, valid := allNumericValidationRead(out[c], r)
						if valid != (want != 2) || (valid && got != float64(want)) {
							t.Fatalf("mode=%v rows=%d row=%d col=%d got=%v/%v want=%d", mode, rows, r, c, got, valid, want)
						}
					}
				}
			}
		}
	}
}

func TestAllNumericValidationFloatSpecials(t *testing.T) {
	for _, typ := range allNumericValidationTypes[8:] {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "x", Type: typ.typ}), []NamedExpr{Named(Add(Col("x"), F64(0)), "sum"), Named(Mul(Col("x"), F64(2)), "product"), Named(Div(Col("x"), F64(0)), "quotient"), Named(EQ(Col("x"), Col("x")), "self"), Named(LT(Col("x"), F64(0)), "negative")})
			if err != nil {
				t.Fatal(err)
			}
			for _, program := range []Program{p, withoutStoragePlan(p)} {
				w, err := NewWorkspace(context.Background(), program, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				vals := []float64{0, math.Copysign(0, -1), 1, -1, math.Inf(1), math.Inf(-1), math.NaN(), math.SmallestNonzeroFloat32, math.MaxFloat32}
				b := memory.NewBatch()
				b.Rows = len(vals)
				allNumericValidationPut(b, typ.field, "x", vals, nil)
				out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for r, x := range vals {
					eq, lt := 0.0, 0.0
					if x == x {
						eq = 1
					}
					if x < 0 {
						lt = 1
					}
					zero := 0.0
					for c, want := range []float64{x + zero, x * 2, x / zero, eq, lt} {
						got, valid := allNumericValidationRead(out[c], r)
						if !valid || (!(math.IsNaN(want) && math.IsNaN(got)) && (got != want || (want == 0 && math.Signbit(got) != math.Signbit(want)))) {
							t.Fatalf("type=%s mode=%v row=%d column=%d got=%v want=%v", typ.typ, mode, r, c, got, want)
						}
					}
				}
			}
		}
	}
}

func TestAllNumericValidationUint64ExactBoundary(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, op := range []struct {
			name  string
			build func(Expr, Expr) Binary
		}{{"add", Add}, {"sub", Sub}, {"mul", Mul}} {
			p, err := Compile(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeUint64}, memory.Field{Name: "b", Type: memory.TypeUint64}), op.build(Col("a"), Col("b")))
			if err != nil {
				t.Fatal(err)
			}
			for _, program := range []Program{p, withoutStoragePlan(p)} {
				w, err := NewWorkspace(context.Background(), program, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				a, b, want := uint64(math.MaxUint64), uint64(0), uint64(math.MaxUint64)
				if op.name == "mul" {
					b = 1
				}
				batch := memory.NewBatch()
				batch.Rows = 1
				batch.Uint64s["a"] = &memory.Uint64Vector{Values: []uint64{a}}
				batch.Uint64s["b"] = &memory.Uint64Vector{Values: []uint64{b}}
				out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil || out[0].Uint64.Values[0] != want {
					t.Fatalf("%s exact max: out=%v err=%v", op.name, out, err)
				}
				if op.name == "sub" {
					batch.Uint64s["a"].Values[0] = 0
					batch.Uint64s["b"].Values[0] = 1
				} else {
					batch.Uint64s["b"].Values[0] = 2
				}
				if _, err = w.Evaluate(context.Background(), batch, kernel.New(mode)); err == nil || !strings.Contains(err.Error(), "overflow") {
					t.Fatalf("%s wanted overflow: %v", op.name, err)
				}
			}
		}
	}
}
