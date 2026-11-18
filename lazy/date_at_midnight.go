package lazy

import "gocolumn/expr"

// DateAtMidnight resolves a Date32 calendar date at local midnight using explicit captured rules.
func DateAtMidnight(value expr.Expr, resolver expr.CivilTimeResolver) expr.DateAtMidnightExpr {
	return expr.DateAtMidnight(value, resolver)
}

// TryDateAtMidnight returns NULL for civil row errors, not configuration, storage, budget or cancellation errors.
func TryDateAtMidnight(value expr.Expr, resolver expr.CivilTimeResolver) expr.DateAtMidnightExpr {
	return expr.TryDateAtMidnight(value, resolver)
}
