package expr_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestMixedInt128ExactOracle(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u", Type: memory.TypeUint64, Nullable: true},
	)
	b := memory.NewBatch()
	b.Rows = 130
	is := make([]int64, b.Rows)
	us := make([]uint64, b.Rows)
	iv, uv := make(memory.Bitmap, memory.WordsFor(b.Rows)), make(memory.Bitmap, memory.WordsFor(b.Rows))
	rng := rand.New(rand.NewPCG(17, 128))
	for r := range b.Rows {
		is[r], us[r] = int64(rng.Uint64()), rng.Uint64()
		if r%7 != 0 {
			iv[r/64] |= 1 << (r % 64)
		}
		if r%11 != 0 {
			uv[r/64] |= 1 << (r % 64)
		}
	}
	is[1], us[1] = math.MinInt64, math.MaxUint64
	is[2], us[2] = math.MaxInt64, math.MaxUint64
	is[3], us[3] = 1<<53, (1<<53)+1
	is[4], us[4] = math.MinInt64, 0
	is[5], us[5] = -1, 1
	is[6], us[6] = 0, math.MaxUint64
	b.Int64s["i"] = &memory.Int64Vector{Values: is, Validity: iv}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: us, Validity: uv}

	dense := make([]int, b.Rows)
	for i := range dense {
		dense[i] = i
	}
	selection := []int{0, 1, 2, 3, 4, 5, 6, 63, 64, 65, 66, 127, 128, 129}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, op := range []string{"add", "sub", "mul"} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/%s/reverse=%v", mode, op, reverse), func(t *testing.T) {
					left, right := expr.Col("i"), expr.Col("u")
					if reverse {
						left, right = right, left
					}
					e := mixedExactExpr(op, left, right)
					info, err := expr.InferScalarType(schema, e)
					if err != nil || info.Type != memory.TypeInt128 || !info.Nullable {
						t.Fatalf("type=%+v err=%v", info, err)
					}
					m := memory.NewBudgetManager(0)
					w := recipeWorkspace(t, schema, e, m)
					for _, rows := range [][]int{nil, selection, {}, nil} {
						out, err := w.EvaluateSelected(ctx, b, rows, kernel.New(mode))
						if err != nil {
							t.Fatal(err)
						}
						if m.Used() != w.MemoryBytes() {
							t.Fatalf("accounting used=%d retained=%d", m.Used(), w.MemoryBytes())
						}
						if rows == nil {
							rows = dense
						}
						d := out[0].Int128
						if out[0].Type != memory.TypeInt128 || d == nil || len(d.Values) != len(rows) {
							t.Fatalf("invalid result metadata: %+v", out[0])
						}
						for j, r := range rows {
							wantValid := r%7 != 0 && r%11 != 0
							if mixedExactValid(out[0], j) != wantValid {
								t.Fatalf("row %d valid=%v want %v", r, mixedExactValid(out[0], j), wantValid)
							}
							if !wantValid {
								continue
							}
							x, y := big.NewInt(is[r]), new(big.Int).SetUint64(us[r])
							if reverse {
								x, y = y, x
							}
							want := new(big.Int)
							switch op {
							case "add":
								want.Add(x, y)
							case "sub":
								want.Sub(x, y)
							case "mul":
								want.Mul(x, y)
							}
							if got := mixedExactBig(d.Values[j]); got.Cmp(want) != 0 {
								t.Fatalf("row %d: got %s, want %s", r, got, want)
							}
						}
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					if m.Used() != 0 {
						t.Fatalf("leaked %d bytes", m.Used())
					}
				})
			}
		}
	}
}

func TestMixedInt128ExactRejectedPairsAndCompatibility(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	i, u := expr.Col("i"), expr.Col("u")
	if expr.CanCast(memory.TypeInt128, memory.TypeInt128) || expr.IsNumeric(memory.TypeInt128) {
		t.Fatal("workspace-only result entered the ordinary cast or promotion surface")
	}
	for _, e := range []expr.Expr{mixedExactExpr("add", i, i), mixedExactExpr("sub", u, u), mixedExactExpr("mul", i, expr.CastTo(u, memory.TypeFloat64)), expr.Add(mixedExactExpr("add", i, u), i), expr.If(expr.EQ(i, i), i, mixedExactExpr("add", i, u))} {
		if _, err := expr.Compile(schema, e); err == nil {
			t.Fatalf("accepted unsupported %T", e)
		}
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{1 << 53}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{(1 << 53) + 1}}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, tc := range []struct {
			e   expr.Expr
			typ memory.DataType
		}{
			{expr.Sub(u, i), memory.TypeFloat64}, {mixedExactExpr("sub", u, i), memory.TypeInt128},
		} {
			w := recipeWorkspace(t, schema, tc.e, nil)
			out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			if out[0].Type != tc.typ {
				t.Fatalf("got %v, want %v", out[0].Type, tc.typ)
			}
			if tc.typ == memory.TypeFloat64 && out[0].Float64.Values[0] != 0 {
				t.Fatal("default changed")
			}
			if tc.typ == memory.TypeInt128 && mixedExactBig(out[0].Int128.Values[0]).Cmp(big.NewInt(1)) != 0 {
				t.Fatal("inexact opt-in result")
			}
		}
	}
}

func TestMixedInt128ExactBudget(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 130
	b.Int64s["i"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: make([]uint64, b.Rows)}
	e := mixedExactExpr("mul", expr.Col("i"), expr.Col("u"))
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		m := memory.NewBudgetManager(1)
		w := recipeWorkspace(t, schema, e, m)
		if _, err := w.Evaluate(ctx, b, kernel.New(mode)); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("budget error: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if m.Used() != 0 {
			t.Fatalf("budget leak: %d", m.Used())
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		m = memory.NewBudgetManager(0)
		w = recipeWorkspace(t, schema, e, m)
		if _, err := w.Evaluate(canceled, b, kernel.New(mode)); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if m.Used() != 0 {
			t.Fatalf("cancel leak: %d", m.Used())
		}
	}
}

func mixedExactExpr(op string, left, right expr.Expr) expr.Expr {
	switch op {
	case "add":
		return expr.AddInt128(left, right)
	case "sub":
		return expr.SubInt128(left, right)
	case "mul":
		return expr.MulInt128(left, right)
	default:
		panic(op)
	}
}

func mixedExactValid(v expr.ScalarValue, row int) bool {
	return v.Int128 != nil && v.Int128.IsValid(row)
}

func mixedExactBig(v memory.Int128) *big.Int {
	h := new(big.Int).Lsh(big.NewInt(v.High), 64)
	return h.Add(h, new(big.Int).SetUint64(v.Low))
}

func TestMixedInt128ExactConditionalDemand(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64},
		memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "c", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u", Type: memory.TypeUint64},
	)
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{math.MaxInt64, 2, math.MaxInt64, math.MaxInt64}}
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1}, Validity: memory.Bitmap{7}}
	b.Int64s["c"] = &memory.Int64Vector{Values: []int64{0, 1, 1, 0}, Validity: memory.Bitmap{11}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{2, 2, 2, 2}}
	cond := expr.EQ(expr.Col("c"), expr.I64(1))
	maybeBad := expr.MulInt128(expr.Add(expr.Col("x"), expr.I64(1)), expr.Col("u"))
	ok := expr.AddInt128(expr.Col("i"), expr.Col("u"))
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, e := range []expr.Expr{
			expr.If(cond, maybeBad, ok),
			expr.Case([]expr.WhenThen{{When: cond, Then: maybeBad}}, ok),
		} {
			w := recipeWorkspace(t, schema, e, nil)
			for _, rows := range [][]int{nil, {0, 1, 2, 3}} {
				out, err := w.EvaluateSelected(ctx, b, rows, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for row, want := range []int64{3, 6, 3, 0} {
					if mixedExactValid(out[0], row) != (row != 3) {
						t.Fatalf("row %d: validity mismatch", row)
					}
					if row != 3 && mixedExactBig(out[0].Int128.Values[row]).Cmp(big.NewInt(want)) != 0 {
						t.Fatalf("row %d: want %d", row, want)
					}
				}
			}
		}
		w := recipeWorkspace(t, schema, expr.Coalesce(ok, maybeBad), nil)
		if _, err := w.EvaluateSelected(ctx, b, []int{0, 2}, kernel.New(mode)); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Evaluate(ctx, b, kernel.New(mode)); err == nil {
			t.Fatal("demanded child error disappeared")
		}
	}
}

func TestMixedInt128ExactNamedOutputs(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{1 << 53, math.MinInt64}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{(1 << 53) + 1, math.MaxUint64}}
	i, u := expr.Col("i"), expr.Col("u")
	a := expr.SubInt128(u, i)
	p, err := expr.CompileExpressions(schema, []expr.NamedExpr{expr.Named(a, "exact"), expr.Named(a, "alias"), expr.Named(expr.Sub(u, i), "legacy"), expr.Named(expr.IsNull(a), "null")})
	if err != nil {
		t.Fatal(err)
	}
	w, err := expr.NewWorkspace(ctx, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	out, err := w.Evaluate(ctx, b, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 4 || out[0].Type != memory.TypeInt128 || out[1].Type != memory.TypeInt128 || out[2].Type != memory.TypeFloat64 || out[3].Type != memory.TypeBool {
		t.Fatalf("named result types: %+v", out)
	}
	if mixedExactBig(out[0].Int128.Values[0]).Int64() != 1 || mixedExactBig(out[1].Int128.Values[0]).Int64() != 1 || out[2].Float64.Values[0] != 0 || out[3].Uint32.Values[0] != 0 {
		t.Fatalf("named results: %+v", out)
	}
}

// Adding an opt-in result must not make the existing public value type
// unusable as a map key or in equality comparisons.
var _ map[expr.ScalarValue]struct{}
