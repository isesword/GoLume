package expr_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// This matrix is a compatibility contract, not a proposed lossless promotion
// policy. Expected types are declared independently of CommonNumericType.
func TestNumericPromotionContractMatrix(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	const I, U, F = memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64
	arithmetic := [10][10]memory.DataType{
		{I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F},
		{I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {F, F, F, F, F, F, F, U, F, F},
		{F, F, F, F, F, F, F, F, F, F}, {F, F, F, F, F, F, F, F, F, F},
	}
	for a, at := range types {
		for b, bt := range types {
			t.Run(fmt.Sprintf("%s_%s", at, bt), func(t *testing.T) {
				left, right := expr.CastTo(expr.I64(3), at), expr.CastTo(expr.I64(2), bt)
				conditional := arithmetic[a][b]
				if a == b {
					conditional = at
				}
				cases := []struct {
					e   expr.Expr
					typ memory.DataType
				}{
					{expr.Add(left, right), arithmetic[a][b]}, {expr.Sub(left, right), arithmetic[a][b]}, {expr.Mul(left, right), arithmetic[a][b]}, {expr.Div(left, right), F},
					{expr.If(expr.EQ(expr.I64(1), expr.I64(1)), left, right), conditional},
					{expr.Case([]expr.WhenThen{{When: expr.EQ(expr.I64(1), expr.I64(1)), Then: left}}, right), conditional},
					{expr.Coalesce(left, right), conditional},
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					for index, tc := range cases {
						p, err := expr.Compile(memory.NewSchema(), tc.e)
						if err != nil {
							t.Fatal(err)
						}
						w, err := expr.NewWorkspace(context.Background(), p, nil)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = w.Close() })
						batch := memory.NewBatch()
						batch.Rows = 1
						out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						if out[0].Type != tc.typ {
							t.Fatalf("%T mode=%v: got %s want %s", tc.e, mode, out[0].Type, tc.typ)
						}
						want := []float64{5, 1, 6, 1.5, 3, 3, 3}[index]
						if got := numericContractNumber(out[0]); got != want {
							t.Fatalf("expression %d: got %v want %v", index, got, want)
						}
						w.Close()
					}
				}
			})
		}
	}
}

func TestNumericPromotionContractPrecisionAndNulls(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64, Nullable: true}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			p, err := expr.CompileExpressions(schema, []expr.NamedExpr{
				expr.Named(expr.Sub(expr.Col("u"), expr.Col("i")), "mixed_sub"),
				expr.Named(expr.EQ(expr.Col("u"), expr.Col("i")), "exact_eq"),
				expr.Named(expr.GT(expr.Col("u"), expr.Col("i")), "exact_gt"),
				expr.Named(expr.If(expr.GT(expr.Col("u"), expr.Col("i")), expr.Col("u"), expr.Col("i")), "if"),
				expr.Named(expr.Coalesce(expr.Col("u"), expr.Col("i")), "coalesce"),
				expr.Named(expr.Add(expr.Col("f"), expr.Col("f")), "float_sum"),
			})
			if err != nil {
				t.Fatal(err)
			}
			w, err := expr.NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			b := memory.NewBatch()
			b.Rows = 4
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993, math.MaxUint64, 0, math.MaxUint64}, Validity: memory.Bitmap{7}}
			b.Int64s["i"] = &memory.Int64Vector{Values: []int64{9007199254740992, math.MaxInt64, -1, -7}}
			b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1.5, 0.25, -2, 0}}
			out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			// Exact mathematical difference at row 0 is 1, but conversion occurs before
			// subtraction. IEEE binary64 rounds 2^53+1 to 2^53 (ties to even).
			wantSub := []float64{0, 9223372036854775808, 1}
			for row, want := range wantSub {
				if !out[0].Float64.IsValid(row) || out[0].Float64.Values[row] != want {
					t.Fatalf("sub row %d: %v", row, out[0].Float64)
				}
			}
			if out[0].Float64.IsValid(3) {
				t.Fatal("NULL arithmetic became valid")
			}
			for row := 0; row < 3; row++ {
				if out[1].Uint32.Values[row] != 0 || out[2].Uint32.Values[row] == 0 {
					t.Fatalf("integer comparison rounded at row %d", row)
				}
			}
			if out[1].Uint32.IsValid(3) || out[2].Uint32.IsValid(3) {
				t.Fatal("NULL comparison became valid")
			}
			wantIf := []float64{9007199254740992, 18446744073709551616, 0, -7}
			for row, want := range wantIf {
				if out[3].Float64.Values[row] != want || !out[3].Float64.IsValid(row) {
					t.Fatalf("if row %d", row)
				}
				if out[4].Float64.Values[row] != want || !out[4].Float64.IsValid(row) {
					t.Fatalf("coalesce row %d", row)
				}
			}
			for row, want := range []float64{3, 0.5, -4, 0} {
				if out[5].Type != memory.TypeFloat64 || out[5].Float64.Values[row] != want {
					t.Fatalf("float32 arithmetic row %d", row)
				}
			}
		})
	}
}

// numericContractNumber reads public output vectors only; test operands here
// are small exactly representable integers, so this observation loses no bits.
func numericContractNumber(v expr.ScalarValue) float64 { return numericContractNumberAt(v, 0) }

func numericContractNumberAt(v expr.ScalarValue, row int) float64 {
	switch v.Type {
	case memory.TypeInt8:
		return float64(v.Int8.Values[row])
	case memory.TypeInt16:
		return float64(v.Int16.Values[row])
	case memory.TypeInt32:
		return float64(v.Int32.Values[row])
	case memory.TypeInt64:
		return float64(v.Int64.Values[row])
	case memory.TypeUint8:
		return float64(v.Uint8.Values[row])
	case memory.TypeUint16:
		return float64(v.Uint16.Values[row])
	case memory.TypeUint32:
		return float64(v.Uint32.Values[row])
	case memory.TypeUint64:
		return float64(v.Uint64.Values[row])
	case memory.TypeFloat32:
		return float64(v.Float32.Values[row])
	case memory.TypeFloat64:
		return v.Float64.Values[row]
	default:
		panic("unexpected numeric contract output type")
	}
}

// TestNumericPromotionContractColumns covers actual typed column inputs and
// bitmap tails. Requested SIMD may use a scalar fallback on unsupported hosts.
func TestNumericPromotionContractColumns(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
	const I, U, F = memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64
	arithmetic := [10][10]memory.DataType{
		{I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F},
		{I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {I, I, I, I, I, I, I, F, F, F}, {F, F, F, F, F, F, F, U, F, F},
		{F, F, F, F, F, F, F, F, F, F}, {F, F, F, F, F, F, F, F, F, F},
	}
	const rows = 130
	for a, at := range types {
		for b, bt := range types {
			t.Run(fmt.Sprintf("%s_%s", at, bt), func(t *testing.T) {
				schema := memory.NewSchema(memory.Field{Name: "left", Type: at, Nullable: true}, memory.Field{Name: "right", Type: bt}, memory.Field{Name: "condition", Type: memory.TypeInt64, Nullable: true})
				batch := memory.NewBatch()
				batch.Rows = rows
				lv, rv, cv := make([]int64, rows), make([]int64, rows), make([]int64, rows)
				lvalid, cvalid := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
				for row := 0; row < rows; row++ {
					lv[row] = int64(12 + row%5)
					rv[row] = int64(1 + row%4)
					if row%5 != 0 {
						lvalid[row/64] |= uint64(1) << uint(row%64)
					}
					if row%3 != 2 {
						cvalid[row/64] |= uint64(1) << uint(row%64)
					}
					if row%3 == 0 {
						cv[row] = 1
					}
				}
				numericContractColumn(batch, "left", at, lv, lvalid)
				numericContractColumn(batch, "right", bt, rv, nil)
				batch.Int64s["condition"] = &memory.Int64Vector{Values: cv, Validity: cvalid}
				left, right := expr.Col("left"), expr.Col("right")
				condition := expr.EQ(expr.Col("condition"), expr.I64(1))
				branchType := arithmetic[a][b]
				if a == b {
					branchType = at
				}
				cases := []struct {
					name string
					e    expr.Expr
					typ  memory.DataType
				}{
					{"add", expr.Add(left, right), arithmetic[a][b]}, {"sub", expr.Sub(left, right), arithmetic[a][b]},
					{"mul", expr.Mul(left, right), arithmetic[a][b]}, {"div", expr.Div(left, right), F},
					{"if", expr.If(condition, left, right), branchType},
					{"case", expr.Case([]expr.WhenThen{{When: condition, Then: left}}, right), branchType},
					{"coalesce", expr.Coalesce(left, right), branchType},
				}
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					for index, tc := range cases {
						t.Run(fmt.Sprintf("%s_%v", tc.name, mode), func(t *testing.T) {
							p, err := expr.Compile(schema, tc.e)
							if err != nil {
								t.Fatal(err)
							}
							w, err := expr.NewWorkspace(context.Background(), p, nil)
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { _ = w.Close() })
							out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							if out[0].Type != tc.typ {
								t.Fatalf("type got %s want %s", out[0].Type, tc.typ)
							}
							for row := 0; row < rows; row++ {
								x, y := float64(lv[row]), float64(rv[row])
								valid := row%5 != 0
								var want float64
								switch index {
								case 0:
									want = x + y
								case 1:
									want = x - y
								case 2:
									want = x * y
								case 3:
									want = x / y
								case 4, 5:
									if row%3 == 0 {
										want = x
									} else {
										want = y
										valid = true
									}
								case 6:
									if valid {
										want = x
									} else {
										want = y
									}
									valid = true
								}
								if got := numericContractValidAt(out[0], row); got != valid {
									t.Fatalf("row %d validity got %v want %v", row, got, valid)
								}
								if valid {
									if got := numericContractNumberAt(out[0], row); got != want {
										t.Fatalf("row %d got %v want %v", row, got, want)
									}
								}
							}
						})
					}
				}
			})
		}
	}
}

// numericContractColumn builds typed data directly, without casts in the
// evaluated expression. Values are deliberately small and exactly representable.
func numericContractColumn(b *memory.Batch, name string, typ memory.DataType, values []int64, validity memory.Bitmap) {
	switch typ {
	case memory.TypeInt8:
		v := make([]int8, len(values))
		for i, x := range values {
			v[i] = int8(x)
		}
		b.Int8s[name] = &memory.Int8Vector{Values: v, Validity: validity}
	case memory.TypeInt16:
		v := make([]int16, len(values))
		for i, x := range values {
			v[i] = int16(x)
		}
		b.Int16s[name] = &memory.Int16Vector{Values: v, Validity: validity}
	case memory.TypeInt32:
		v := make([]int32, len(values))
		for i, x := range values {
			v[i] = int32(x)
		}
		b.Int32s[name] = &memory.Int32Vector{Values: v, Validity: validity}
	case memory.TypeInt64:
		v := make([]int64, len(values))
		for i, x := range values {
			v[i] = int64(x)
		}
		b.Int64s[name] = &memory.Int64Vector{Values: v, Validity: validity}
	case memory.TypeUint8:
		v := make([]uint8, len(values))
		for i, x := range values {
			v[i] = uint8(x)
		}
		b.Uint8s[name] = &memory.Uint8Vector{Values: v, Validity: validity}
	case memory.TypeUint16:
		v := make([]uint16, len(values))
		for i, x := range values {
			v[i] = uint16(x)
		}
		b.Uint16s[name] = &memory.Uint16Vector{Values: v, Validity: validity}
	case memory.TypeUint32:
		v := make([]uint32, len(values))
		for i, x := range values {
			v[i] = uint32(x)
		}
		b.Uint32s[name] = &memory.Uint32Vector{Values: v, Validity: validity}
	case memory.TypeUint64:
		v := make([]uint64, len(values))
		for i, x := range values {
			v[i] = uint64(x)
		}
		b.Uint64s[name] = &memory.Uint64Vector{Values: v, Validity: validity}
	case memory.TypeFloat32:
		v := make([]float32, len(values))
		for i, x := range values {
			v[i] = float32(x)
		}
		b.Float32s[name] = &memory.Float32Vector{Values: v, Validity: validity}
	case memory.TypeFloat64:
		v := make([]float64, len(values))
		for i, x := range values {
			v[i] = float64(x)
		}
		b.Float64s[name] = &memory.Float64Vector{Values: v, Validity: validity}
	default:
		panic("unsupported contract input type")
	}
}

func numericContractValidAt(v expr.ScalarValue, row int) bool {
	switch v.Type {
	case memory.TypeInt8:
		return v.Int8.IsValid(row)
	case memory.TypeInt16:
		return v.Int16.IsValid(row)
	case memory.TypeInt32:
		return v.Int32.IsValid(row)
	case memory.TypeInt64:
		return v.Int64.IsValid(row)
	case memory.TypeUint8:
		return v.Uint8.IsValid(row)
	case memory.TypeUint16:
		return v.Uint16.IsValid(row)
	case memory.TypeUint32:
		return v.Uint32.IsValid(row)
	case memory.TypeUint64:
		return v.Uint64.IsValid(row)
	case memory.TypeFloat32:
		return v.Float32.IsValid(row)
	case memory.TypeFloat64:
		return v.Float64.IsValid(row)
	default:
		panic("unsupported contract output type")
	}
}
