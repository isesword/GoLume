package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
)

func TestMathMaskedWorkspaceReuse(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		schema := memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}, memory.Field{Name: "gate", Type: memory.TypeInt64})
		a := Col("x").Abs()
		p, err := CompileExpressions(schema, []NamedExpr{Named(If(GE(Col("gate"), I64(50)), a.Sqrt(), Col("x")), "selected"), Named(a, "abs"), Named(Col("x").Sqrt(), "sqrt"), Named(Col("x"), "original")})
		if err != nil {
			t.Fatal(err)
		}
		var referenceCharge []int64
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			mgr := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, mgr)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				t.Fatal(err)
			}
			for step, n := range []int{8193, 1, 63, 64, 65, 4095, 4096, 4097, 0, 8193} {
				batch := memory.NewBatch()
				batch.Reset(n)
				f32, f64, g := make([]float32, n), make([]float64, n), make([]int64, n)
				valid := make(memory.Bitmap, memory.WordsFor(n))
				for i := range f64 {
					g[i] = int64((i * 37) % 100)
					f32[i] = math.Float32frombits(uint32(i) * 2654435761)
					f64[i] = math.Float64frombits(uint64(i) * 11400714819323198485)
					if step%3 == 0 && i%17 != 0 || step%3 == 1 {
						memory.Set(valid, i)
					}
				}
				if n > 0 {
					valid[len(valid)-1] |= ^memory.TailMask(n)
				}
				if typ == memory.TypeFloat32 {
					batch.Float32s["x"] = &memory.Float32Vector{Values: f32, Validity: valid}
				} else {
					batch.Float64s["x"] = &memory.Float64Vector{Values: f64, Validity: valid}
				}
				batch.Int64s["gate"] = &memory.Int64Vector{Values: g}
				got, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				want, err := ref.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar))
				if err != nil {
					t.Fatal(err)
				}
				if mode == kernel.ModeScalar {
					referenceCharge = append(referenceCharge, mgr.Used())
				} else if mgr.Used() != referenceCharge[step] {
					t.Fatalf("%s/%s rows%d charged=%d scalar=%d", typ, mode, n, mgr.Used(), referenceCharge[step])
				}
				for j := range want {
					for i := 0; i < n; i++ {
						var x, y float64
						var xb, yb uint64
						var xv, yv bool
						if typ == memory.TypeFloat32 {
							x = float64(want[j].Float32.Values[i])
							y = float64(got[j].Float32.Values[i])
							xb = uint64(math.Float32bits(want[j].Float32.Values[i]))
							yb = uint64(math.Float32bits(got[j].Float32.Values[i]))
							xv = want[j].Float32.IsValid(i)
							yv = got[j].Float32.IsValid(i)
						} else {
							x = want[j].Float64.Values[i]
							y = got[j].Float64.Values[i]
							xb = math.Float64bits(x)
							yb = math.Float64bits(y)
							xv = want[j].Float64.IsValid(i)
							yv = got[j].Float64.IsValid(i)
						}
						if xv != yv || xv && xb != yb && !(math.IsNaN(x) && math.IsNaN(y)) {
							t.Fatalf("%s/%s rows%d output%d row%d got%x valid%t want%x valid%t", typ, mode, n, j, i, yb, yv, xb, xv)
						}
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := w.Evaluate(ctx, memory.NewBatch(), kernel.New(mode)); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			ref.Close()
			if mgr.Used() != 0 {
				t.Fatal("workspace leak", mgr.Used())
			}
		}
	}
}

func TestMathMaskedPreservesEvaluationErrors(t *testing.T) {
	const rows = 8193
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "e", Type: memory.TypeInt64}, memory.Field{Name: "gate", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Reset(rows)
	a, e, g := make([]int64, rows), make([]int64, rows), make([]int64, rows)
	for i := range a {
		a[i] = 4
		g[i] = int64(i % 100)
		e[i] = 2
		if g[i] >= 50 {
			e[i] = -1
		}
	}
	batch.Int64s["a"] = &memory.Int64Vector{Values: a}
	batch.Int64s["e"] = &memory.Int64Vector{Values: e}
	batch.Int64s["gate"] = &memory.Int64Vector{Values: g}
	for _, op := range []string{"abs", "sqrt"} {
		var value Expr = Col("a").Pow(Col("e")).Sqrt()
		if op == "abs" {
			value = Col("a").Pow(Col("e")).Cast(memory.TypeFloat64).Abs()
		}
		p, err := Compile(schema, If(LT(Col("gate"), I64(50)), value, F64(0)))
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/%s", op, mode), func(t *testing.T) {
				w, err := NewWorkspace(context.Background(), p, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				if _, err := w.Evaluate(context.Background(), batch, kernel.New(mode)); err != nil {
					t.Fatalf("inactive power errors evaluated: %v", err)
				}
				e[100] = -1
				_, err = w.Evaluate(context.Background(), batch, kernel.New(mode))
				e[100] = 2
				if err == nil || !strings.Contains(err.Error(), "row 100") {
					t.Fatalf("active error lost: %v", err)
				}
			})
		}
	}
}
