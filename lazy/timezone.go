package lazy

import "gocolumn/expr"

// LoadTimeZone captures standard-library time-zone rules once for explicit extraction.
func LoadTimeZone(name string) (expr.TimeZone, error) { return expr.LoadTimeZone(name) }

// TimeZoneFromTZData constructs captured rules from bounded explicit TZif bytes.
func TimeZoneFromTZData(name string, data []byte) (expr.TimeZone, error) {
	return expr.TimeZoneFromTZData(name, data)
}

// YearInZone extracts a Timestamp's year using explicit captured rules.
func YearInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.YearInZone(value, zone)
}

// MonthInZone extracts a Timestamp's month using explicit captured rules.
func MonthInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.MonthInZone(value, zone)
}

// DayInZone extracts a Timestamp's day using explicit captured rules.
func DayInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.DayInZone(value, zone)
}

// HourInZone extracts a Timestamp's hour using explicit captured rules.
func HourInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.HourInZone(value, zone)
}

// MinuteInZone extracts a Timestamp's minute using explicit captured rules.
func MinuteInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.MinuteInZone(value, zone)
}

// SecondInZone extracts a Timestamp's second using explicit captured rules.
func SecondInZone(value expr.Expr, zone expr.TimeZone) expr.ZonedExtractExpr {
	return expr.SecondInZone(value, zone)
}

// DateInZone returns the Timestamp instant's local calendar date as Date32.
func DateInZone(value expr.Expr, zone expr.TimeZone) expr.DateInZoneExpr {
	return expr.DateInZone(value, zone)
}

// ParseCivilTimestamp resolves exact civil text using explicit captured rules.
func ParseCivilTimestamp(value expr.Expr, resolver expr.CivilTimeResolver) expr.CivilTimestampExpr {
	return expr.ParseCivilTimestamp(value, resolver)
}

// TryParseCivilTimestamp returns NULL only for civil row-value errors.
func TryParseCivilTimestamp(value expr.Expr, resolver expr.CivilTimeResolver) expr.CivilTimestampExpr {
	return expr.TryParseCivilTimestamp(value, resolver)
}
