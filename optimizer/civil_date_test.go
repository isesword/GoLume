package optimizer

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestCivilDateRewritePreservesResolver(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "renamed", Type: memory.TypeDate32, Nullable: true})
	b := memory.NewBatch()
	b.Reset(1)
	b.Int32s["renamed"] = &memory.Int32Vector{Values: []int32{0}}
	for _, offset := range []int32{0, 3600} {
		for _, tolerant := range []bool{false, true} {
			r := civilRewriteResolver(t, offset)
			var original expr.Expr = expr.DateAtMidnight(expr.Col("original"), r)
			if tolerant {
				original = expr.TryDateAtMidnight(expr.Col("original"), r)
			}
			before := original.String()
			rewritten, ok := renamePredicateColumns(original, map[string]string{"original": "renamed"})
			if !ok {
				t.Fatal("rename refused")
			}
			if original.String() != before || !reflect.DeepEqual(expr.Columns(original), []string{"original"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed"}) {
				t.Fatal("dependency/mutation")
			}
			if _, ok := renamePredicateColumns(original, map[string]string{}); ok {
				t.Fatal("missing rename admitted")
			}
			p, e := expr.CompileExpressions(schema, []expr.NamedExpr{expr.Named(rewritten, "out")})
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			w, e := expr.NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if e != nil || out[0].Int64.Values[0] != -int64(offset)*1e9 {
				t.Fatal(offset, e, out)
			}
			b.Int32s["renamed"] = &memory.Int32Vector{Values: []int32{2147483647}}
			out, e = w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if tolerant {
				if e != nil || out[0].Int64.IsValid(0) {
					t.Fatal("try bit lost", e)
				}
			} else if !errors.Is(e, expr.ErrCivilRange) {
				t.Fatal("strict bit lost", e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
			b.Int32s["renamed"] = &memory.Int32Vector{Values: []int32{0}}
		}
	}
}

func TestCivilDateMayErrorContract(t *testing.T) {
	r := civilRewriteResolver(t, 0)
	s := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeDate32})
	for _, tc := range []struct {
		e    expr.Expr
		want bool
	}{{expr.DateAtMidnight(expr.Col("x"), r), true}, {expr.TryDateAtMidnight(expr.Col("x"), r), false}, {expr.TryDateAtMidnight(expr.CastTo(expr.CastTo(expr.Col("x"), memory.TypeTimestamp), memory.TypeDate32), r), true}} {
		b, e := expr.Bind(s, tc.e)
		if e != nil {
			t.Fatal(e)
		}
		if b.MayError != tc.want {
			t.Fatalf("%s MayError=%v want %v", tc.e, b.MayError, tc.want)
		}
	}
}
