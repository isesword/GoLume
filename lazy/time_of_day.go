package lazy

import "gocolumn/expr"

// TimeOfDayInZone extracts local wall-clock time as Time64 nanoseconds since midnight.
// It carries no date or zone, and distinct instants during a fold can have the same value.
func TimeOfDayInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.TimeOfDayInZone(value, zone)
}
