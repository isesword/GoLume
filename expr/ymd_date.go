package expr

import (
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// ErrYMDDate identifies a nonexistent Gregorian date (including invalid month
// or day). No month or day is silently normalized.
var ErrYMDDate = errors.New("invalid Gregorian date")

// ErrYMDDateRange identifies a date outside signed 32-bit epoch days.
var ErrYMDDateRange = errors.New("Gregorian date outside Date32 range")

// YMDDateExpr constructs a calendar date, not an instant. Use the constructors
// to select strict or tolerant row-error handling.
type YMDDateExpr struct {
	Year     Expr
	Month    Expr
	Day      Expr
	tolerant bool
}

func (YMDDateExpr) exprNode() {}

// DateFromYMD constructs Date32 from Int64 year, month, and day.
// Months are 1 through 12. The proleptic Gregorian calendar includes year 0
// and negative years. No timezone or implicit numeric conversion is involved.
func DateFromYMD(year, month, day Expr) YMDDateExpr {
	return YMDDateExpr{year, month, day, false}
}

// TryDateFromYMD turns only invalid Gregorian dates and Date32 range errors into
// NULL. Child evaluation, storage, cancellation and budget failures propagate.
func TryDateFromYMD(year, month, day Expr) YMDDateExpr {
	return YMDDateExpr{year, month, day, true}
}

// Alias names the resulting Date32 column.
func (e YMDDateExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the ordered Gregorian calendar inputs and error-handling mode.
func (e YMDDateExpr) String() string {
	name := "DATE_FROM_YMD"
	if e.tolerant {
		name = "TRY_DATE_FROM_YMD"
	}
	return fmt.Sprintf("%s(%s,%s,%s)", name, e.Year, e.Month, e.Day)
}
func bindYMDDate(b *BoundExpr, e YMDDateExpr) error {
	nullable := e.tolerant
	for _, c := range b.Children {
		if c.Field.Type != memory.TypeInt64 {
			return fmt.Errorf("Gregorian date requires three Int64 inputs")
		}
		nullable = nullable || c.Field.Nullable
	}
	b.Field = computedField(memory.TypeDate32, nullable)
	b.MayError = !e.tolerant
	return nil
}

// ymdDateDays validates month/day coarse domains before the safe year envelope,
// rejects normalized calendar dates, and finally enforces the Date32 range.
func ymdDateDays(year, month, day int64) (int32, error) {
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return 0, ErrYMDDate
	}
	if year < -6000000 || year > 6000000 {
		return 0, ErrYMDDateRange
	}
	candidate := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
	y, m, d := candidate.Date()
	if int64(y) != year || int64(m) != month || int64(d) != day {
		return 0, ErrYMDDate
	}
	days := candidate.Unix() / 86400
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, ErrYMDDateRange
	}
	return int32(days), nil
}

func evalYMDDate(out, year, month, day *scalarRegister, tolerant bool, rows int, active memory.Bitmap) error {
	if rows < 0 {
		return fmt.Errorf("invalid Gregorian date row count")
	}
	words := memory.WordsFor(rows)
	for _, in := range []*scalarRegister{year, month, day} {
		if in.typ != memory.TypeInt64 || in.validity != nil && len(in.validity) < words {
			return fmt.Errorf("invalid Gregorian date input storage")
		}
	}
	if active != nil && len(active) < words {
		return fmt.Errorf("invalid Gregorian date active bitmap")
	}
	if out.typ != memory.TypeDate32 || len(out.i32) < rows || len(out.ownedValidity) < words {
		return fmt.Errorf("Gregorian date output buffers not prepared")
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
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(year, row) || !isValidRegister(month, row) || !isValidRegister(day, row) {
			continue
		}
		for _, in := range []*scalarRegister{year, month, day} {
			if !in.constant && row >= len(in.i64) {
				return fmt.Errorf("short demanded Gregorian date storage")
			}
		}
		y, err := registerInt64(year, row)
		if err != nil {
			return err
		}
		w, err := registerInt64(month, row)
		if err != nil {
			return err
		}
		d, err := registerInt64(day, row)
		if err != nil {
			return err
		}
		value, err := ymdDateDays(y, w, d)
		if err != nil {
			if tolerant && (errors.Is(err, ErrYMDDate) || errors.Is(err, ErrYMDDateRange)) {
				continue
			}
			return fmt.Errorf("Gregorian date at row %d: %w", row, err)
		}
		out.i32[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
