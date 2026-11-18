package optimizer

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestCivilComposeRewriteAndMayError(t *testing.T) {
	sc := memory.NewSchema(memory.Field{Name: "renamed_date", Type: memory.TypeDate32, Nullable: true}, memory.Field{Name: "renamed_clock", Type: memory.TypeTime64, Nullable: true})
	for _, offset := range []int32{-3600, 3600} {
		r := civilRewriteResolver(t, offset)
		for _, try := range []bool{false, true} {
			var original expr.Expr = expr.ComposeCivilTimestamp(expr.Col("date"), expr.Col("clock"), r)
			if try {
				original = expr.TryComposeCivilTimestamp(expr.Col("date"), expr.Col("clock"), r)
			}
			before := original.String()
			rewritten, ok := renamePredicateColumns(original, map[string]string{"date": "renamed_date", "clock": "renamed_clock"})
			if !ok {
				t.Fatal("rename")
			}
			if original.String() != before || !reflect.DeepEqual(expr.Columns(original), []string{"clock", "date"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed_clock", "renamed_date"}) {
				t.Fatal("mutation/dependencies", expr.Columns(original), expr.Columns(rewritten))
			}
			if _, ok := renamePredicateColumns(original, map[string]string{"date": "renamed_date"}); ok {
				t.Fatal("missing clock mapping")
			}
			bound, e := expr.Bind(sc, rewritten)
			if e != nil || bound.MayError == try || bound.Field.Type != memory.TypeTimestamp || !bound.Field.Nullable {
				t.Fatal("metadata", e)
			}
			p, e := expr.Compile(sc, rewritten)
			if e != nil {
				t.Fatal(e)
			}
			b := memory.NewBatch()
			b.Reset(2)
			b.Int32s["renamed_date"] = &memory.Int32Vector{Values: []int32{0, 0}, Validity: memory.Bitmap{1}}
			b.Int64s["renamed_clock"] = &memory.Int64Vector{Values: []int64{1, 86400e9}, Validity: memory.Bitmap{3}}
			m := memory.NewBudgetManager(0)
			w, e := expr.NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if e != nil || out[0].Int64.Values[0] != 1-int64(offset)*1e9 || out[0].Int64.IsValid(1) {
				t.Fatal("rules/NULL", e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
	r := civilRewriteResolver(t, 0)
	schema := memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString}, memory.Field{Name: "clock", Type: memory.TypeTime64})
	bound, e := expr.Bind(schema, expr.TryComposeCivilTimestamp(expr.CastTo(expr.Col("text"), memory.TypeDate32), expr.Col("clock"), r))
	if e != nil || !bound.MayError {
		t.Fatal("Try child error domain lost", e)
	}
}
