package optimizer

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestTimeOfDayRewriteAndErrorDomain(t *testing.T) {
	sc := memory.NewSchema(memory.Field{Name: "renamed", Type: memory.TypeTimestamp, Nullable: true})
	for _, offset := range []int32{-3600, 3600} {
		z := isoRewriteZone(t, offset)
		original := expr.TimeOfDayInZone(expr.Col("original"), z)
		before := original.String()
		rewritten, ok := renamePredicateColumns(original, map[string]string{"original": "renamed"})
		if !ok {
			t.Fatal("rename")
		}
		if original.String() != before || !reflect.DeepEqual(expr.Columns(original), []string{"original"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed"}) {
			t.Fatal("mutation/dependencies")
		}
		if _, ok := renamePredicateColumns(original, map[string]string{}); ok {
			t.Fatal("missing map")
		}
		bound, e := expr.Bind(sc, rewritten)
		if e != nil || bound.MayError || bound.Field.Type != memory.TypeTime64 || !bound.Field.Nullable {
			t.Fatal("typed metadata", e)
		}
		p, e := expr.Compile(sc, rewritten)
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(2)
		b.Int64s["renamed"] = &memory.Int64Vector{Values: []int64{1, 0}, Validity: memory.Bitmap{1}}
		m := memory.NewBudgetManager(0)
		w, e := expr.NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		want := int64(23*3600e9 + 1)
		if offset > 0 {
			want = 3600e9 + 1
		}
		if out[0].Type != memory.TypeTime64 || out[0].Int64.Values[0] != want || out[0].Int64.IsValid(1) {
			t.Fatal("rules/typed value/null")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	z := isoRewriteZone(t, 0)
	b, e := expr.Bind(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), expr.TimeOfDayInZone(expr.CastTo(expr.Col("s"), memory.TypeTimestamp), z))
	if e != nil || !b.MayError {
		t.Fatal("child error lost", e)
	}
}
