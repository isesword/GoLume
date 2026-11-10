package optimizer

import (
	"gocolumn/expr"
	"testing"
)

func TestDecimalDivisionPredicateRenameKeepsOrderedInputs(t *testing.T) {
	original := expr.DivideDecimal128(expr.Col("left"), expr.Col("right"), 19, 7)
	rewritten, ok := renamePredicateColumns(original, map[string]string{"left": "a", "right": "b"})
	if !ok {
		t.Fatal("division rename failed")
	}
	got, ok := rewritten.(expr.Decimal128DivideExpr)
	if !ok || got.Left != expr.Col("a") || got.Right != expr.Col("b") || got.Precision != 19 || got.Scale != 7 {
		t.Fatalf("changed division identity: %#v", rewritten)
	}
	if original.Left != expr.Col("left") || original.Right != expr.Col("right") {
		t.Fatal("mutated caller expression")
	}
	if _, ok := renamePredicateColumns(original, map[string]string{"left": "a"}); ok {
		t.Fatal("accepted unknown right input")
	}
}
