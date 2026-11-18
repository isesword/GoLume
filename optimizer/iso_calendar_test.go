package optimizer

import (
	"context"
	"encoding/binary"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func isoRewriteZone(t *testing.T, offset int32) expr.TimeZone {
	t.Helper()
	b := make([]byte, 44)
	copy(b, "TZif")
	binary.BigEndian.PutUint32(b[36:], 1)
	binary.BigEndian.PutUint32(b[40:], 4)
	b = binary.BigEndian.AppendUint32(b, uint32(offset))
	b = append(b, 0, 0, 'F', 'I', 'X', 0)
	z, e := expr.TimeZoneFromTZData("same-label", b)
	if e != nil {
		t.Fatal(e)
	}
	return z
}
func TestISOCalendarRewritePreservesRulesAndPart(t *testing.T) {
	parts := []func(expr.Expr, expr.TimeZone) expr.ZonedExtractExpr{expr.ISOWeekYearInZone, expr.ISOWeekInZone, expr.ISOWeekdayInZone, expr.UTCOffsetInZone}
	schema := memory.NewSchema(memory.Field{Name: "renamed", Type: memory.TypeTimestamp, Nullable: true})
	for _, offset := range []int32{-3600, 3600} {
		z := isoRewriteZone(t, offset)
		want := []int64{2020, 53, 7, -3600}
		if offset > 0 {
			want = []int64{2021, 1, 1, 3600}
		}
		for part, fn := range parts {
			original := fn(expr.Col("original"), z)
			before := original.String()
			rewritten, ok := renamePredicateColumns(original, map[string]string{"original": "renamed"})
			if !ok {
				t.Fatal("rename refused")
			}
			if before != original.String() || !reflect.DeepEqual(expr.Columns(original), []string{"original"}) || !reflect.DeepEqual(expr.Columns(rewritten), []string{"renamed"}) {
				t.Fatal("mutation/dependency")
			}
			if _, ok := renamePredicateColumns(original, map[string]string{}); ok {
				t.Fatal("missing rename")
			}
			bound, e := expr.Bind(schema, rewritten)
			if e != nil || bound.MayError || bound.Field.Type != memory.TypeInt64 || !bound.Field.Nullable {
				t.Fatal("bound", e)
			}
			p, e := expr.Compile(schema, rewritten)
			if e != nil {
				t.Fatal(e)
			}
			b := memory.NewBatch()
			b.Reset(2)
			b.Int64s["renamed"] = &memory.Int64Vector{Values: []int64{1609718400000000000, 0}, Validity: memory.Bitmap{1}}
			m := memory.NewBudgetManager(0)
			w, e := expr.NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
			if e != nil || out[0].Int64.Values[0] != want[part] || out[0].Int64.IsValid(1) {
				t.Fatal("rewritten rules/part/null", part, offset, e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
}
