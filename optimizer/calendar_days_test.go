package optimizer

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestCalendarDaysRenameErrorDomain(t *testing.T) {
	r := civilRewriteResolver(t, 0)
	for _, try := range []bool{false, true} {
		var ex expr.Expr = expr.AddCalendarDays(expr.Col("ts"), expr.Col("days"), r)
		if try {
			ex = expr.TryAddCalendarDays(expr.Col("ts"), expr.Col("days"), r)
		}
		before := ex.String()
		renamed, ok := renamePredicateColumns(ex, map[string]string{"ts": "x", "days": "y"})
		if !ok || ex.String() != before || !reflect.DeepEqual(expr.Columns(renamed), []string{"x", "y"}) {
			t.Fatal("rename")
		}
		b, e := expr.Bind(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeTimestamp}, memory.Field{Name: "y", Type: memory.TypeInt64}), renamed)
		if e != nil || b.MayError == try || b.Field.Nullable != try {
			t.Fatal("error domain", e)
		}
	}
}
