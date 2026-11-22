package expr_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestInt128ToStringWorkspace(t *testing.T) {
	ctx := context.Background()
	schema := memory.NewSchema(
		memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "u", Type: memory.TypeUint64},
		memory.Field{Name: "x", Type: memory.TypeInt64},
	)
	i, u := expr.Col("i"), expr.Col("u")
	exact := expr.MulInt128(i, u)
	text := expr.Int128ToString(exact)
	if info, err := expr.InferScalarType(schema, text); err != nil || info.Type != memory.TypeString || !info.Nullable {
		t.Fatalf("text binding: %+v %v", info, err)
	}
	for _, bad := range []expr.Expr{expr.Int128ToString(i), expr.Int128ToString(expr.Sub(u, i)), expr.CastTo(exact, memory.TypeString)} {
		if _, err := expr.Compile(schema, bad); err == nil {
			t.Fatalf("unsupported conversion accepted: %s", bad)
		}
	}
	b := memory.NewBatch()
	b.Rows = 5
	b.Int64s["i"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64, 0, -1, math.MinInt64}, Validity: memory.Bitmap{15}}
	b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, math.MaxUint64, math.MaxUint64, 42, math.MaxUint64}}
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{math.MaxInt64, 2, math.MaxInt64, math.MaxInt64, math.MaxInt64}}
	want := []string{"-170141183460469231722463931679029329920", "170141183460469231704017187605319778305", "0", "-42", ""}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			m := memory.NewBudgetManager(0)
			w := recipeWorkspace(t, schema, text, m)
			for _, rows := range []int{5, 1, 0, 5} {
				b.Rows = rows
				b.Int64s["i"].Values = b.Int64s["i"].Values[:rows]
				b.Uint64s["u"].Values = b.Uint64s["u"].Values[:rows]
				if rows == 0 {
					b.Int64s["i"].Validity = nil
				} else {
					b.Int64s["i"].Validity = memory.Bitmap{15 & ((1 << rows) - 1)}
				}
				out, err := w.Evaluate(ctx, b, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				v := out[0].String
				if out[0].Type != memory.TypeString || v == nil || v.Len() != rows {
					t.Fatalf("incorrect string metadata: %+v", out[0])
				}
				for row := range rows {
					if v.IsValid(row) != (row != 4) || string(v.ValueBytes(row)) != want[row] {
						t.Fatalf("row %d got valid=%v value=%q, want %q", row, v.IsValid(row), v.ValueBytes(row), want[row])
					}
				}
				if m.Used() != w.MemoryBytes() {
					t.Fatalf("used=%d retained=%d", m.Used(), w.MemoryBytes())
				}
			}
			if err := w.Close(); err != nil || m.Used() != 0 {
				t.Fatalf("close=%v used=%d", err, m.Used())
			}
			w = recipeWorkspace(t, schema, text, memory.NewBudgetManager(1))
			if _, err := w.Evaluate(ctx, b, kernel.New(mode)); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("budget=%v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			w = recipeWorkspace(t, schema, text, nil)
			if _, err := w.Evaluate(cancelled, b, kernel.New(mode)); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}

			// Overflow in an ordinary Int64 child must be masked by the selected branch.
			bad := expr.Int128ToString(expr.MulInt128(expr.Add(expr.Col("x"), expr.I64(1)), u))
			w = recipeWorkspace(t, schema, expr.If(expr.EQ(expr.Col("x"), expr.I64(2)), bad, text), nil)
			out, err := w.Evaluate(ctx, b, kernel.New(mode))
			if err != nil || string(out[0].String.ValueBytes(1)) != "55340232221128654845" {
				t.Fatalf("guard result=%v err=%v", out, err)
			}
			w = recipeWorkspace(t, schema, bad, nil)
			if _, err := w.Evaluate(ctx, b, kernel.New(mode)); err == nil {
				t.Fatal("demanded child overflow was swallowed")
			}
		})
	}
}
