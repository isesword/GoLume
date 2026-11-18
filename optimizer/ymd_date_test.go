package optimizer

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestYMDDateRewriteAndErrorDomain(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "year", Type: memory.TypeInt64}, memory.Field{Name: "month", Type: memory.TypeInt64}, memory.Field{Name: "day", Type: memory.TypeInt64})
	for _, try := range []bool{false, true} {
		var original expr.Expr = expr.DateFromYMD(expr.Col("y"), expr.Col("m"), expr.Col("d"))
		if try {
			original = expr.TryDateFromYMD(expr.Col("y"), expr.Col("m"), expr.Col("d"))
		}
		before := original.String()
		rewritten, ok := renamePredicateColumns(original, map[string]string{"y": "year", "m": "month", "d": "day"})
		if !ok || original.String() != before || !reflect.DeepEqual(expr.Columns(rewritten), []string{"day", "month", "year"}) {
			t.Fatal("rewrite", ok)
		}
		for _, missing := range []string{"y", "m", "d"} {
			mapping := map[string]string{"y": "year", "m": "month", "d": "day"}
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
		args := []expr.Expr{expr.I64(2020), expr.I64(2), expr.I64(29)}
		args[slot] = expr.CastTo(expr.Str("invalid"), memory.TypeInt64)
		b, e := expr.Bind(memory.NewSchema(), expr.TryDateFromYMD(args[0], args[1], args[2]))
		if e != nil || !b.MayError {
			t.Fatal("Try swallowed child MayError", slot, e)
		}
	}
}
