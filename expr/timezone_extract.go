package expr

import (
	"fmt"
	"gocolumn/memory"
	"time"
)

// ZonedExtractExpr extracts one calendar component from a UTC nanosecond
// Timestamp using captured rules. Construct it using the explicit InZone functions.
type ZonedExtractExpr struct {
	Value Expr
	zone  TimeZone
	part  Op
}

func (ZonedExtractExpr) exprNode() {}

// String describes the component and zone provenance, not a cache identity.
func (e ZonedExtractExpr) String() string {
	if e.zone.state == nil {
		return fmt.Sprintf("%s_IN_ZONE(%s,invalid)", e.part, e.Value)
	}
	state := e.zone.state
	if state.source == "TZif" {
		return fmt.Sprintf("%s_IN_ZONE(%s,%q,TZif:%x)", e.part, e.Value, state.name, state.digest)
	}
	return fmt.Sprintf("%s_IN_ZONE(%s,%q,%s)", e.part, e.Value, state.name, state.source)
}

// Alias names an extracted calendar component.
func (e ZonedExtractExpr) Alias(name string) NamedExpr { return Named(e, name) }

// YearInZone extracts the local calendar year from a Timestamp.
func YearInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpYear}
}

// MonthInZone extracts the local calendar month, numbered 1 through 12.
func MonthInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpMonth}
}

// DayInZone extracts the local day of month from a Timestamp.
func DayInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpDay}
}

// HourInZone extracts the local hour from a Timestamp.
func HourInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpHour}
}

// MinuteInZone extracts the local minute from a Timestamp.
func MinuteInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpMinute}
}

// SecondInZone extracts the local second, numbered 0 through 59, without leap seconds.
func SecondInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpSecond}
}

// ISOWeekYearInZone extracts the ISO week-numbering year in captured local rules.
// Use it with ISOWeekInZone: near New Year it may differ from YearInZone.
func ISOWeekYearInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpISOWeekYear}
}

// ISOWeekInZone extracts the ISO week number (1 through 53) in captured local rules.
// Pair it with ISOWeekYearInZone rather than the Gregorian calendar year.
func ISOWeekInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpISOWeek}
}

// ISOWeekdayInZone extracts the local ISO weekday, Monday=1 through Sunday=7.
func ISOWeekdayInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpISOWeekday}
}

// UTCOffsetInZone extracts signed seconds east of UTC at the Timestamp instant.
// Historical second-level offsets and offset changes within a local day are retained.
func UTCOffsetInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpUTCOffset}
}

// TimeOfDayInZone extracts the local wall-clock h:m:s.n as Time64 nanoseconds.
// The result is in [0, 86400e9), not an elapsed duration since local midnight.
// It carries neither the calendar date nor a timezone or offset.
func TimeOfDayInZone(value Expr, zone TimeZone) ZonedExtractExpr {
	return ZonedExtractExpr{value, zone, OpTimeOfDay}
}

func validZonedPart(part Op) bool {
	switch part {
	case OpYear, OpMonth, OpDay, OpHour, OpMinute, OpSecond, OpISOWeekYear, OpISOWeek, OpISOWeekday, OpUTCOffset, OpTimeOfDay:
		return true
	}
	return false
}

func evalZonedExtract(out, in *scalarRegister, zone *timeZoneState, part Op, rows int) error {
	copyRegisterValidity(out, in, rows)
	for row := 0; row < rows; row++ {
		if row&63 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(out, row) {
			out.i64[row] = 0
			continue
		}
		ns, err := registerInt64(in, row)
		if err != nil {
			return err
		}
		// Do not add an offset to ns: that could overflow at timestamp extremes.
		instant := time.Unix(ns/int64(time.Second), ns%int64(time.Second)).In(zone.location)
		var value int
		switch part {
		case OpTimeOfDay:
			hour, minute, second := instant.Clock()
			// Clock components, not Sub(localMidnight) or ns+offset modulo day.
			// int64 is required even on 32-bit targets.
			out.i64[row] = ((int64(hour)*60+int64(minute))*60+int64(second))*int64(time.Second) + int64(instant.Nanosecond())
			continue
		case OpYear:
			value = instant.Year()
		case OpMonth:
			value = int(instant.Month())
		case OpDay:
			value = instant.Day()
		case OpHour:
			value = instant.Hour()
		case OpMinute:
			value = instant.Minute()
		case OpSecond:
			value = instant.Second()
		case OpISOWeekYear:
			value, _ = instant.ISOWeek()
		case OpISOWeek:
			_, value = instant.ISOWeek()
		case OpISOWeekday:
			value = (int(instant.Weekday())+6)%7 + 1
		case OpUTCOffset:
			_, value = instant.Zone()
		}
		out.i64[row] = int64(value)
	}
	if out.checkContext != nil {
		return out.checkContext()
	}
	return nil
}

func bindZonedExtract(bound *BoundExpr, node ZonedExtractExpr) error {
	if bound.Children[0].Field.Type != memory.TypeTimestamp {
		return fmt.Errorf("calendar extraction in a time zone requires Timestamp input")
	}
	if node.zone.state == nil || node.zone.state.location == nil {
		return fmt.Errorf("calendar extraction requires a valid TimeZone handle")
	}
	if !validZonedPart(node.part) {
		return fmt.Errorf("invalid zoned calendar component")
	}
	resultType := memory.TypeInt64
	if node.part == OpTimeOfDay {
		resultType = memory.TypeTime64
	}
	bound.Field = computedField(resultType, bound.Children[0].Field.Nullable)
	return nil
}
