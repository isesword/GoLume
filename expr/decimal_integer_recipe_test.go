package expr_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// ExampleCastDecimal128_mixedIntegerArithmetic converts inputs before arithmetic.
func ExampleCastDecimal128_mixedIntegerArithmetic() {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{9007199254740992}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993}}
	exact := expr.Sub(expr.CastDecimal128(expr.Col("u"), 20, 0), expr.CastDecimal128(expr.Col("i"), 20, 0))
	p, err := expr.Compile(schema, exact)
	if err != nil {
		panic(err)
	}
	w, err := expr.NewWorkspace(context.Background(), p, nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()
	out, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if err != nil {
		panic(err)
	}
	fmt.Printf("coefficient=%s precision=%d scale=%d\n", out[0].Decimal.Values[0].BigInt(), out[0].Decimal.Precision, out[0].Decimal.Scale)
	// Output: coefficient=1 precision=21 scale=0
}

func recipeWorkspace(t *testing.T, s memory.Schema, e expr.Expr, m memory.Manager) *expr.Workspace {
	t.Helper()
	p, err := expr.Compile(s, e)
	if err != nil {
		t.Fatal(err)
	}
	w, err := expr.NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestExplicitDecimalIntegerRecipeDenseSelectedOracle(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 130
	iv := []int64{math.MinInt64, math.MaxInt64, -1, 0, 1, 9007199254740993}
	uv := []uint64{math.MaxUint64, 0, 1, 9007199254740993, 9007199254740992}
	is, us := make([]int64, b.Rows), make([]uint64, b.Rows)
	valid := make(memory.Bitmap, memory.WordsFor(b.Rows))
	for r := range is {
		is[r] = iv[r%len(iv)]
		us[r] = uv[r%len(uv)]
		if r%7 != 0 {
			valid[r/64] |= 1 << uint(r%64)
		}
	}
	b.Int64s["i"] = &memory.Int64Vector{Values: is, Validity: valid}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: us}
	a, c := expr.CastDecimal128(expr.Col("i"), 20, 0), expr.CastDecimal128(expr.Col("u"), 20, 0)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, op := range []string{"add", "sub", "reverse_sub"} {
			t.Run(fmt.Sprintf("%s_%v", op, mode), func(t *testing.T) {
				e := expr.Add(a, c)
				if op == "sub" {
					e = expr.Sub(a, c)
				}
				if op == "reverse_sub" {
					e = expr.Sub(c, a)
				}
				m := memory.NewBudgetManager(0)
				w := recipeWorkspace(t, schema, e, m)
				for _, selection := range [][]int{nil, {0, 1, 63, 64, 65, 127, 128, 129}, {}} {
					out, err := w.EvaluateSelected(context.Background(), b, selection, kernel.New(mode))
					if err != nil {
						t.Fatal(err)
					}
					d := out[0].Decimal
					if out[0].Type != memory.TypeDecimal || d.Precision != 21 || d.Scale != 0 {
						t.Fatalf("metadata: %+v", d)
					}
					rows := selection
					if rows == nil {
						rows = make([]int, b.Rows)
						for i := range rows {
							rows[i] = i
						}
					}
					if len(d.Values) != len(rows) {
						t.Fatal("wrong selected length")
					}
					for j, r := range rows {
						wantValid := r%7 != 0
						if d.IsValid(j) != wantValid {
							t.Fatalf("validity %d", r)
						}
						if !wantValid {
							continue
						}
						x, y := big.NewInt(is[r]), new(big.Int).SetUint64(us[r])
						want := new(big.Int)
						switch op {
						case "add":
							want.Add(x, y)
						case "sub":
							want.Sub(x, y)
						default:
							want.Sub(y, x)
						}
						if d.Values[j].BigInt().Cmp(want) != 0 {
							t.Fatalf("row %d: got %s want %s", r, d.Values[j].BigInt(), want)
						}
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if m.Used() != 0 {
					t.Fatalf("leaked %d", m.Used())
				}
			})
		}
	}
}

func TestExplicitDecimalIntegerRecipeProductsAndDivision(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "v", Type: memory.TypeUint64})
	cases := []struct {
		name             string
		i                int64
		u, v             uint64
		op               string
		want             string
		precision, scale int32
		errText          string
	}{
		{"small_product", -3, 7, 0, "mixed", "-21", 38, 0, ""},
		{"mixed_positive_overflow", math.MaxInt64, math.MaxUint64, 0, "mixed", "", 38, 0, "overflows"},
		{"mixed_negative_overflow", math.MinInt64, math.MaxUint64, 0, "mixed", "", 38, 0, "overflows"},
		{"unsigned_square_overflow", 0, math.MaxUint64, math.MaxUint64, "unsigned", "", 38, 0, "overflows"},
		{"half", 1, 2, 0, "divide", "5", 20, 1, ""},
		{"third", 1, 3, 0, "divide", "", 20, 1, "loses fractional digits"},
		{"zero_divisor", 1, 0, 0, "divide", "", 20, 1, "by zero"},
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s_%v", tc.name, mode), func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = 1
				b.Int64s["i"] = &memory.Int64Vector{Values: []int64{tc.i}}
				b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{tc.u}}
				b.Uint64s["v"] = &memory.Uint64Vector{Values: []uint64{tc.v}}
				i, u, v := expr.CastDecimal128(expr.Col("i"), 20, 0), expr.CastDecimal128(expr.Col("u"), 20, 0), expr.CastDecimal128(expr.Col("v"), 20, 0)
				var e expr.Expr = expr.Mul(i, u)
				if tc.op == "unsigned" {
					e = expr.Mul(u, v)
				}
				if tc.op == "divide" {
					e = expr.DivideDecimal128(i, u, tc.precision, tc.scale)
				}
				if tc.op != "divide" {
					x := big.NewInt(tc.i)
					if tc.op == "unsigned" {
						x = new(big.Int).SetUint64(tc.v)
					}
					product := new(big.Int).Mul(x, new(big.Int).SetUint64(tc.u))
					fits := new(big.Int).Abs(new(big.Int).Set(product)).Cmp(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)) < 0
					if fits != (tc.errText == "") {
						t.Fatal("invalid independent product range oracle")
					}
					if fits && product.String() != tc.want {
						t.Fatal("invalid product value oracle")
					}
				}
				m := memory.NewBudgetManager(0)
				w := recipeWorkspace(t, schema, e, m)
				out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
				if tc.errText != "" {
					if err == nil || !strings.Contains(err.Error(), tc.errText) {
						t.Fatalf("want %q got %v", tc.errText, err)
					}
					if m.Used() != 0 {
						t.Fatal("error leaked budget")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				d := out[0].Decimal
				if d.Values[0].BigInt().String() != tc.want || d.Precision != tc.precision || d.Scale != tc.scale || !d.IsValid(0) {
					t.Fatalf("result %+v", d)
				}
			})
		}
	}
}

func TestExplicitDecimalIntegerRecipeBranchesAndGuards(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "c", Type: memory.TypeInt64, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64, 1, math.MaxInt64}, Validity: memory.Bitmap{7}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64, 2, math.MaxUint64}}
	b.Int64s["c"] = &memory.Int64Vector{Values: []int64{1, 0, 1, 0}, Validity: memory.Bitmap{11}}
	i, u := expr.CastDecimal128(expr.Col("i"), 20, 0), expr.CastDecimal128(expr.Col("u"), 20, 0)
	cond := expr.EQ(expr.Col("c"), expr.I64(1))
	sum := expr.Add(i, u)
	for _, bad := range []expr.Expr{expr.If(cond, sum, u), expr.Case([]expr.WhenThen{{When: cond, Then: sum}}, u), expr.Coalesce(sum, u), expr.If(expr.EQ(expr.I64(0), expr.I64(1)), sum, u)} {
		if _, err := expr.Compile(schema, bad); err == nil || !strings.Contains(err.Error(), "identical precision and scale") {
			t.Fatalf("metadata mismatch accepted: %v", err)
		}
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, tc := range []struct {
			name string
			e    expr.Expr
			p    int32
			want []string
		}{
			{"if", expr.If(cond, i, u), 20, []string{"-9223372036854775808", "18446744073709551615", "2", "18446744073709551615"}},
			{"case", expr.Case([]expr.WhenThen{{When: cond, Then: i}}, u), 20, []string{"-9223372036854775808", "18446744073709551615", "2", "18446744073709551615"}},
			{"coalesce", expr.Coalesce(i, u), 20, []string{"-9223372036854775808", "9223372036854775807", "1", "18446744073709551615"}},
			{"normalized", expr.If(cond, sum, expr.CastDecimal128(u, 21, 0)), 21, []string{"9223372036854775807", "18446744073709551615", "2", "18446744073709551615"}},
		} {
			t.Run(fmt.Sprintf("%s_%v", tc.name, mode), func(t *testing.T) {
				w := recipeWorkspace(t, schema, tc.e, nil)
				out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				d := out[0].Decimal
				for r, want := range tc.want {
					if !d.IsValid(r) || d.Values[r].BigInt().String() != want || d.Precision != tc.p || d.Scale != 0 {
						t.Fatalf("row %d got %v", r, d)
					}
				}
			})
		}
		// A NULL input suppresses its overflowing product; selected rows avoid all
		// other overflowing rows. A false guard does not demand the bad product.
		prod := expr.Mul(i, u)
		w := recipeWorkspace(t, schema, prod, nil)
		out, err := w.EvaluateSelected(context.Background(), b, []int{2, 3}, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Type != memory.TypeDecimal || out[0].Decimal.Precision != 38 || out[0].Decimal.Scale != 0 || len(out[0].Decimal.Values) != 2 {
			t.Fatal("selected product metadata/length")
		}
		if !out[0].Decimal.IsValid(0) || out[0].Decimal.Values[0].BigInt().String() != "2" || out[0].Decimal.IsValid(1) {
			t.Fatal("NULL/selected product")
		}
		guarded := expr.If(expr.EQ(expr.I64(0), expr.I64(1)), prod, expr.CastDecimal128(expr.I64(7), 38, 0))
		w = recipeWorkspace(t, schema, guarded, nil)
		out, err = w.Evaluate(context.Background(), b, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Type != memory.TypeDecimal || out[0].Decimal.Precision != 38 || out[0].Decimal.Scale != 0 || len(out[0].Decimal.Values) != b.Rows {
			t.Fatal("guard metadata/length")
		}
		for r := 0; r < b.Rows; r++ {
			if !out[0].Decimal.IsValid(r) || out[0].Decimal.Values[r].BigInt().String() != "7" {
				t.Fatal("guard result")
			}
		}

		// A data-dependent guard demands the safe product and a NULL product,
		// but suppresses overflow behind both false and NULL conditions.
		dynamic := memory.NewBatch()
		dynamic.Rows = b.Rows
		dynamic.Int64s["i"] = b.Int64s["i"]
		dynamic.Uint64s["u"] = b.Uint64s["u"]
		dynamic.Int64s["c"] = &memory.Int64Vector{Values: []int64{0, 1, 1, 1}, Validity: memory.Bitmap{13}}
		w = recipeWorkspace(t, schema, expr.If(cond, prod, expr.CastDecimal128(expr.I64(7), 38, 0)), nil)
		out, err = w.Evaluate(context.Background(), dynamic, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		d := out[0].Decimal
		if out[0].Type != memory.TypeDecimal || d.Precision != 38 || d.Scale != 0 || len(d.Values) != 4 {
			t.Fatal("dynamic guard metadata/length")
		}
		for r, want := range []string{"7", "7", "2", ""} {
			if d.IsValid(r) != (r != 3) {
				t.Fatalf("dynamic guard validity row %d", r)
			}
			if r != 3 && d.Values[r].BigInt().String() != want {
				t.Fatalf("dynamic guard value row %d", r)
			}
		}
	}
}

func TestExplicitDecimalIntegerRecipeBudgetAndCancellation(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 130
	b.Int64s["i"] = &memory.Int64Vector{Values: make([]int64, 130)}
	e := expr.Add(expr.CastDecimal128(expr.Col("i"), 20, 0), expr.CastDecimal128(expr.I64(1), 20, 0))
	m := memory.NewBudgetManager(1)
	w := recipeWorkspace(t, s, e, m)
	_, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget error: %v", err)
	}
	_ = w.Close()
	if m.Used() != 0 {
		t.Fatal("budget leak")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m = memory.NewBudgetManager(0)
	w = recipeWorkspace(t, s, e, m)
	_, err = w.Evaluate(ctx, b, kernel.New(kernel.ModeScalar))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error: %v", err)
	}
	_ = w.Close()
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
}

func TestExplicitDecimalIntegerRecipeDoesNotAlterDefault(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{9007199254740992}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{9007199254740993}}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		w := recipeWorkspace(t, s, expr.Sub(expr.Col("u"), expr.Col("i")), nil)
		out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Type != memory.TypeFloat64 || out[0].Float64.Values[0] != 0 {
			t.Fatal("default promotion changed")
		}
	}
}
