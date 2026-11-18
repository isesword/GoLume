package optimizer

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestCivilFormatRewritePreservesZone(t *testing.T) {
	sc := memory.NewSchema(memory.Field{Name: "renamed", Type: memory.TypeTimestamp, Nullable: true})
	for _, offset := range []int32{-3600, 3600} {
		z := isoRewriteZone(t, offset)
		original := expr.FormatCivilTimestamp(expr.Col("original"), z)
		before := original.String()
		rewritten, ok := renamePredicateColumns(original, map[string]string{"original": "renamed"})
		if !ok {
			t.Fatal("rename")
		}
		if original.String() != before || !reflect.DeepEqual(expr.Columns(original), []string{"original"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed"}) {
			t.Fatal("mutation/dependencies")
		}
		if _, ok := renamePredicateColumns(original, map[string]string{}); ok {
			t.Fatal("missing mapping")
		}
		bound, e := expr.Bind(sc, rewritten)
		if e != nil || bound.MayError || bound.Field.Type != memory.TypeString || !bound.Field.Nullable {
			t.Fatal("metadata", e)
		}
		p, e := expr.Compile(sc, rewritten)
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(2)
		b.Int64s["renamed"] = &memory.Int64Vector{Values: []int64{0, 0}, Validity: memory.Bitmap{1}}
		m := memory.NewBudgetManager(0)
		w, e := expr.NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		want := "1969-12-31T23:00:00.000000000"
		if offset > 0 {
			want = "1970-01-01T01:00:00.000000000"
		}
		if string(out[0].String.ValueBytes(0)) != want || out[0].String.IsValid(1) {
			t.Fatal("rules/null")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	// Total formatting still inherits a fallible child's error domain.
	z := isoRewriteZone(t, 0)
	schema := memory.NewSchema(memory.Field{Name: "text", Type: memory.TypeString})
	b, e := expr.Bind(schema, expr.FormatCivilTimestamp(expr.CastTo(expr.Col("text"), memory.TypeTimestamp), z))
	if e != nil || !b.MayError {
		t.Fatal("child MayError lost", e)
	}
}
