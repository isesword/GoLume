package lazy

import "gocolumn/expr"

// FormatCivilTimestamp formats a Timestamp as local wall-clock text with nine fractional digits.
// The text carries no offset or zone and is not a unique instant during a fold.
func FormatCivilTimestamp(value expr.Expr, zone expr.TimeZone) expr.CivilFormatExpr {
	return expr.FormatCivilTimestamp(value, zone)
}
