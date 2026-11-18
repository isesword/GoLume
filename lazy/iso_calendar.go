package lazy

import "gocolumn/expr"

// ISOWeekYearInZone returns the local ISO week-numbering year, to pair with ISOWeekInZone.
func ISOWeekYearInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.ISOWeekYearInZone(value, zone)
}

// ISOWeekInZone returns the local ISO week number (1 through 53).
func ISOWeekInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.ISOWeekInZone(value, zone)
}

// ISOWeekdayInZone returns the local ISO weekday (Monday 1 through Sunday 7).
func ISOWeekdayInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.ISOWeekdayInZone(value, zone)
}

// UTCOffsetInZone returns signed seconds east of UTC, including historical second offsets.
func UTCOffsetInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.UTCOffsetInZone(value, zone)
}
