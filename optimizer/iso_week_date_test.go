package optimizer

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestISOWeekDateRewriteAndErrorDomain(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "year", Type: memory.TypeInt64}, memory.Field{Name: "week", Type: memory.TypeInt64}, memory.Field{Name: "weekday", Type: memory.TypeInt64})
	for _, try := range []bool{false, true} {
		var original expr.Expr = expr.DateFromISOWeek(expr.Col("y"), expr.Col("w"), expr.Col("d"))
		if try {
			original = expr.TryDateFromISOWeek(expr.Col("y"), expr.Col("w"), expr.Col("d"))
		}
		before := original.String()
		rewritten, ok := renamePredicateColumns(original, map[string]string{"y": "year", "w": "week", "d": "weekday"})
		if !ok || original.String() != before || !reflect.DeepEqual(expr.Columns(rewritten), []string{"week", "weekday", "year"}) {
			t.Fatal("rewrite", ok)
		}
		for _, missing := range []string{"y", "w", "d"} {
			mapping := map[string]string{"y": "year", "w": "week", "d": "weekday"}
			delete(mapping, missing)
			if _, ok := renamePredicateColumns(original, mapping); ok {
				t.Fatal("missing dependency", missing)
			}
		}
		b, e := expr.Bind(schema, rewritten)
		if e != nil || b.Field.Type != memory.TypeDate32 || b.Field.Nullable != try || b.MayError == try {
			t.Fatal("bound metadata", e)
		}
	}
	for slot := 0; slot < 3; slot++ {
		args := []expr.Expr{expr.I64(2020), expr.I64(53), expr.I64(5)}
		args[slot] = expr.CastTo(expr.Str("invalid"), memory.TypeInt64)
		b, e := expr.Bind(memory.NewSchema(), expr.TryDateFromISOWeek(args[0], args[1], args[2]))
		if e != nil || !b.MayError {
			t.Fatal("Try swallowed child MayError", slot, e)
		}
	}
}
