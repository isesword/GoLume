package expr

import (
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// ErrISOWeekDate identifies a nonexistent ISO week-date (including invalid week
// or weekday). No week or weekday is silently normalized.
var ErrISOWeekDate = errors.New("invalid ISO week-date")

// ErrISOWeekDateRange identifies a date outside signed 32-bit epoch days.
var ErrISOWeekDateRange = errors.New("ISO week-date outside Date32 range")

// ISOWeekDateExpr constructs a calendar date, not an instant. Use the constructors
// to select strict or tolerant row-error handling.
type ISOWeekDateExpr struct {
	Year     Expr
	Week     Expr
	Weekday  Expr
	tolerant bool
}

func (ISOWeekDateExpr) exprNode() {}

// DateFromISOWeek constructs Date32 from Int64 ISO year, week, and weekday.
// Monday is 1 and Sunday is 7. The proleptic Gregorian calendar includes year 0
// and negative years. No timezone or implicit numeric conversion is involved.
func DateFromISOWeek(year, week, weekday Expr) ISOWeekDateExpr {
	return ISOWeekDateExpr{year, week, weekday, false}
}

// TryDateFromISOWeek turns only invalid ISO dates and Date32 range errors into
// NULL. Child evaluation, storage, cancellation and budget failures propagate.
func TryDateFromISOWeek(year, week, weekday Expr) ISOWeekDateExpr {
	return ISOWeekDateExpr{year, week, weekday, true}
}

// Alias names the resulting Date32 column.
func (e ISOWeekDateExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the ordered ISO calendar inputs and error-handling mode.
func (e ISOWeekDateExpr) String() string {
	name := "DATE_FROM_ISO_WEEK"
	if e.tolerant {
		name = "TRY_DATE_FROM_ISO_WEEK"
	}
	return fmt.Sprintf("%s(%s,%s,%s)", name, e.Year, e.Week, e.Weekday)
}
func bindISOWeekDate(b *BoundExpr, e ISOWeekDateExpr) error {
	nullable := e.tolerant
	for _, c := range b.Children {
		if c.Field.Type != memory.TypeInt64 {
			return fmt.Errorf("ISO week-date requires three Int64 inputs")
		}
		nullable = nullable || c.Field.Nullable
	}
	b.Field = computedField(memory.TypeDate32, nullable)
	b.MayError = !e.tolerant
	return nil
}

// isoWeekDateDays checks week/weekday domains first, then the safe year envelope,
// then actual week existence, then Date32 range. Arithmetic uses seconds/days,
// never Timestamp nanoseconds. The envelope covers all Date32 ISO years; see
// docs/ISO_WEEK_DATE.md for the bound proof.
func isoWeekDateDays(year, week, weekday int64) (int32, error) {
	if week < 1 || week > 53 || weekday < 1 || weekday > 7 {
		return 0, ErrISOWeekDate
	}
	if year < -6000000 || year > 6000000 {
		return 0, ErrISOWeekDateRange
	}
	jan4 := time.Date(int(year), time.January, 4, 0, 0, 0, 0, time.UTC)
	monday := jan4.Unix()/86400 - int64((int(jan4.Weekday())+6)%7)
	days := monday + (week-1)*7 + weekday - 1
	candidate := time.Unix(days*86400, 0).UTC()
	y, w := candidate.ISOWeek()
	if int64(y) != year || int64(w) != week || int64((int(candidate.Weekday())+6)%7+1) != weekday {
		return 0, ErrISOWeekDate
	}
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, ErrISOWeekDateRange
	}
	return int32(days), nil
}

func evalISOWeekDate(out, year, week, weekday *scalarRegister, tolerant bool, rows int, active memory.Bitmap) error {
	if rows < 0 {
		return fmt.Errorf("invalid ISO week-date row count")
	}
	words := memory.WordsFor(rows)
	for _, in := range []*scalarRegister{year, week, weekday} {
		if in.typ != memory.TypeInt64 || in.validity != nil && len(in.validity) < words {
			return fmt.Errorf("invalid ISO week-date input storage")
		}
	}
	if active != nil && len(active) < words {
		return fmt.Errorf("invalid ISO week-date active bitmap")
	}
	if out.typ != memory.TypeDate32 || len(out.i32) < rows || len(out.ownedValidity) < words {
		return fmt.Errorf("ISO week-date output buffers not prepared")
	}
	valid := ensureValidity(out, rows)
	clear(valid)
	out.validity = valid
	ctx := dateEvaluationContext{out.checkContext}
	for row := 0; row < rows; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		out.i32[row] = 0
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(year, row) || !isValidRegister(week, row) || !isValidRegister(weekday, row) {
			continue
		}
		for _, in := range []*scalarRegister{year, week, weekday} {
			if !in.constant && row >= len(in.i64) {
				return fmt.Errorf("short demanded ISO week-date storage")
			}
		}
		y, err := registerInt64(year, row)
		if err != nil {
			return err
		}
		w, err := registerInt64(week, row)
		if err != nil {
			return err
		}
		d, err := registerInt64(weekday, row)
		if err != nil {
			return err
		}
		value, err := isoWeekDateDays(y, w, d)
		if err != nil {
			if tolerant && (errors.Is(err, ErrISOWeekDate) || errors.Is(err, ErrISOWeekDateRange)) {
				continue
			}
			return fmt.Errorf("ISO week-date at row %d: %w", row, err)
		}
		out.i32[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
