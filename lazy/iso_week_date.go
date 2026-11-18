package lazy

import "gocolumn/expr"

// DateFromISOWeek constructs a Date32 from an ISO week-year, week and Monday-based weekday.
func DateFromISOWeek(year, week, weekday expr.Expr) expr.ISOWeekDateExpr {
	return expr.DateFromISOWeek(year, week, weekday)
}

// TryDateFromISOWeek converts invalid ISO week dates and Date32 range errors to NULL.
// Child, storage, budget and cancellation errors remain errors.
func TryDateFromISOWeek(year, week, weekday expr.Expr) expr.ISOWeekDateExpr {
	return expr.TryDateFromISOWeek(year, week, weekday)
}
