package lazy

import "gocolumn/expr"

// DateFromYMD constructs a proleptic Gregorian Date32 from three Int64 calendar components.
// Invalid dates are rejected rather than normalized.
func DateFromYMD(year, month, day expr.Expr) expr.YMDDateExpr {
	return expr.DateFromYMD(year, month, day)
}

// TryDateFromYMD converts invalid calendar dates and Date32 range errors to NULL.
// Child, storage, budget and cancellation errors remain errors.
func TryDateFromYMD(year, month, day expr.Expr) expr.YMDDateExpr {
	return expr.TryDateFromYMD(year, month, day)
}
