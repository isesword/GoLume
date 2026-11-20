package lazy

import "gocolumn/expr"

// AddCalendarDays preserves the captured local clock while shifting its calendar date.
func AddCalendarDays(timestamp, days expr.Expr, resolver expr.CivilTimeResolver) expr.CalendarDaysExpr {
	return expr.AddCalendarDays(timestamp, days, resolver)
}

// TryAddCalendarDays converts only civil row errors to NULL.
func TryAddCalendarDays(timestamp, days expr.Expr, resolver expr.CivilTimeResolver) expr.CalendarDaysExpr {
	return expr.TryAddCalendarDays(timestamp, days, resolver)
}
