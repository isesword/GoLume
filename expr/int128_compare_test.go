package expr_test

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestInt128CompareOracle(t *testing.T) {
	ctx := context.Background()
	s := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u", Type: memory.TypeUint64, Nullable: true},
	)
	const n = 130
	b := memory.NewBatch()
	b.Rows = n
	is := make([]int64, n)
	us := make([]uint64, n)
	iv, uv := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
	rng := rand.New(rand.NewPCG(128, 17))
	for j := range n {
		is[j], us[j] = int64(rng.Uint64()), rng.Uint64()
		if j%7 != 0 {
			memory.Set(iv, j)
		}
		if j%11 != 0 {
			memory.Set(uv, j)
		}
	}
	is[1], us[1] = math.MinInt64, math.MaxUint64
	is[2], us[2] = math.MaxInt64, math.MaxUint64
	is[3], us[3] = 1, math.MaxUint64
	is[4], us[4] = -1, math.MaxUint64
	is[5], us[5] = 0, math.MaxUint64
	is[6], us[6] = 1, 1<<63
	b.Int64s["i"] = &memory.Int64Vector{Values: is, Validity: iv}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: us, Validity: uv}
	left := expr.MulInt128(expr.Col("i"), expr.Col("u"))
	dense := make([]int, n)
	for j := range dense {
		dense[j] = j
	}
	selected := []int{0, 1, 2, 3, 4, 5, 6, 7, 63, 64, 65, 66, 127, 128, 129}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, op := range []struct {
			name string
			make func(expr.Expr, expr.Expr) expr.Binary
			want func(int) bool
		}{
			{"eq", expr.EQ, func(c int) bool { return c == 0 }},
			{"lt", expr.LT, func(c int) bool { return c < 0 }},
			{"le", expr.LE, func(c int) bool { return c <= 0 }},
			{"gt", expr.GT, func(c int) bool { return c > 0 }},
			{"ge", expr.GE, func(c int) bool { return c >= 0 }},
		} {
			for _, bound := range []memory.Int128{{High: math.MinInt64}, {High: -1, Low: math.MaxUint64}, {}, {Low: 1 << 63}, {Low: math.MaxUint64}, {High: 1}, {High: math.MaxInt64, Low: math.MaxUint64}} {
				t.Run(fmt.Sprintf("%v/%s/%d_%d", mode, op.name, bound.High, bound.Low), func(t *testing.T) {
					right := expr.I128(bound.High, bound.Low)
					for _, reverse := range []bool{false, true} {
						a, z := expr.Expr(left), expr.Expr(right)
						if reverse {
							a, z = z, a
						}
						m := memory.NewBudgetManager(0)
						w := recipeWorkspace(t, s, op.make(a, z), m)
						for _, rows := range [][]int{nil, selected, {}, nil} {
							out, err := w.EvaluateSelected(ctx, b, rows, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							if rows == nil {
								rows = dense
							}
							v := out[0].Uint32
							if out[0].Type != memory.TypeBool || v == nil || len(v.Values) != len(rows) {
								t.Fatalf("bad Boolean result: %+v", out[0])
							}
							limit := mixedExactBig(bound)
							for j, row := range rows {
								ok := row%7 != 0 && row%11 != 0
								if v.IsValid(j) != ok {
									t.Fatalf("row %d validity mismatch", row)
								}
								if !ok {
									continue
								}
								p := new(big.Int).Mul(big.NewInt(is[row]), new(big.Int).SetUint64(us[row]))
								cmp := p.Cmp(limit)
								if reverse {
									cmp = -cmp
								}
								if (v.Values[j] != 0) != op.want(cmp) {
									t.Fatalf("row %d: compare %s with %s produced %d", row, p, limit, v.Values[j])
								}
							}
							if w.MemoryBytes() != m.Used() {
								t.Fatalf("used %d, retained %d", m.Used(), w.MemoryBytes())
							}
						}
						if err := w.Close(); err != nil || m.Used() != 0 {
							t.Fatalf("close %v, used %d", err, m.Used())
						}
					}
				})
			}
		}
	}
}

func TestInt128CompareBindingAndNull(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	wide := expr.AddInt128(expr.Col("i"), expr.Col("u"))
	for _, e := range []expr.Expr{expr.EQ(wide, expr.Col("i")), expr.LT(expr.Col("u"), wide), expr.GT(wide, expr.F64(1)), expr.EQ(wide, expr.Str("1"))} {
		if _, err := expr.Compile(s, e); err == nil {
			t.Fatalf("accepted mixed comparison: %v", e)
		}
	}
	for _, tc := range []struct {
		a, b         expr.Expr
		valid, truth bool
	}{
		{expr.I128(-1, math.MaxUint64), expr.I128(-1, math.MaxUint64), true, true},
		{expr.I128(-1, math.MaxUint64), expr.I128(0, math.MaxUint64), true, false},
		{expr.Null(memory.TypeInt128), expr.I128(0, 0), false, false},
	} {
		w := recipeWorkspace(t, s, expr.EQ(tc.a, tc.b), nil)
		b := memory.NewBatch()
		b.Rows = 1
		out, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		v := out[0].Uint32
		if v.IsValid(0) != tc.valid || tc.valid && (v.Values[0] != 0) != tc.truth {
			t.Fatalf("unexpected bool %+v", v)
		}
	}
}

func TestInt128CompareGuardedRange(t *testing.T) {
	ctx := context.Background()
	s := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{math.MaxInt64, 1}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64}}
	i, u := expr.Col("i"), expr.Col("u")
	bad := expr.GT(expr.AddInt128(expr.Add(i, expr.I64(1)), u), expr.I128(1, 0))
	guard := expr.If(expr.EQ(i, expr.I64(1)), bad, expr.Bool(false))
	if _, fast, err := expr.CompilePredicate(s, bad); err != nil || fast != nil {
		t.Fatalf("Int128 must remain residual: fast=%v err=%v", fast, err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		w := recipeWorkspace(t, s, guard, nil)
		out, err := w.Evaluate(ctx, b, kernel.New(mode))
		if err != nil {
			t.Fatal(err)
		}
		v := out[0].Uint32
		if !v.IsValid(0) || !v.IsValid(1) || v.Values[0] != 0 || v.Values[1] != 1 {
			t.Fatalf("guard result=%+v", v)
		}
		w = recipeWorkspace(t, s, bad, nil)
		if _, err := w.Evaluate(ctx, b, kernel.New(mode)); err == nil {
			t.Fatal("demanded child overflow was swallowed")
		}
	}
}
