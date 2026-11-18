package lazy

import "gocolumn/expr"

// ComposeCivilTimestamp resolves a Date32 date and Time64 nanosecond clock using explicit civil policies.
func ComposeCivilTimestamp(date, clock expr.Expr, resolver expr.CivilTimeResolver) expr.CivilComposeExpr {
	return expr.ComposeCivilTimestamp(date, clock, resolver)
}

// TryComposeCivilTimestamp converts only invalid clock, gap, fold and Timestamp-range row errors to NULL.
// Configuration, storage, child, budget and cancellation errors remain errors.
func TryComposeCivilTimestamp(date, clock expr.Expr, resolver expr.CivilTimeResolver) expr.CivilComposeExpr {
	return expr.TryComposeCivilTimestamp(date, clock, resolver)
}
