package optimizer

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

// This isolates the rewrite contract. Current public InMemory execution retains
// a residual predicate and may not execute pushed annotations, so its passing
// query regression alone cannot establish that this internal rewrite is sound.
func TestStructFromImplicitNamesSurviveColumnRewrite(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "y", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{-10, 20, -30}}
	b.Int64s["y"] = &memory.Int64Vector{Values: []int64{1, -2, 3}}
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit", true: "explicit"}[explicit], func(t *testing.T) {
			ax, ay := "", ""
			if explicit {
				ax, ay = "x", "y"
			}
			original := expr.StructGet(expr.StructFrom(expr.Named(expr.Col("x"), ax), expr.Named(expr.Col("y"), ay)), "x")
			before := original.String()
			renamed, ok := renamePredicateColumns(original, map[string]string{"x": "y", "y": "x"})
			if !ok {
				t.Fatal("rewrite refused")
			}
			if original.String() != before {
				t.Fatal("original AST mutated")
			}
			p, e := expr.CompileExpressions(schema, []expr.NamedExpr{expr.Named(renamed, "picked")})
			if e != nil {
				t.Fatal(e)
			}
			manager := memory.NewBudgetManager(0)
			w, e := expr.NewWorkspace(context.Background(), p, manager)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if e != nil {
				w.Close()
				t.Fatal(e)
			}
			got := append([]int64(nil), out[0].Int64.Values...)
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("leak")
			}
			want := []int64{1, -2, 3}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("renamed=%s got=%v want=%v", renamed.String(), got, want)
			}
		})
	}
}
