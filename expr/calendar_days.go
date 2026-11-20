package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// AddCalendarDays shifts the local Gregorian date while preserving the clock and
// nanoseconds under captured rules. Zero days is exact instant identity. Nonzero
// shifts resolve the target using the configured fold and gap policies.
func (r CivilTimeResolver) AddCalendarDays(ctx context.Context, instant, days int64) (int64, error) {
	if ctx == nil {
		return 0, ErrCivilConfiguration
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.state == nil {
		return 0, ErrCivilConfiguration
	}
	if days == 0 {
		return instant, nil
	}
	local := time.Unix(0, instant).In(r.state.zone.location)
	year, month, day := local.Date()
	ordinal := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / 86400
	// Every Timestamp local date fits Date32 even with the captured int32 offsets.
	// Check before addition to avoid overflow for arbitrary Int64 offsets.
	if days < int64(math.MinInt32)-ordinal || days > int64(math.MaxInt32)-ordinal {
		return 0, ErrCivilRange
	}
	target := time.Unix((ordinal+days)*86400, 0).UTC()
	y, m, d := target.Date()
	return r.resolveFields(ctx, civilFields{year: y, month: int(m), day: d, hour: local.Hour(), minute: local.Minute(), second: local.Second(), nano: local.Nanosecond()})
}

// CalendarDaysExpr offsets a Timestamp by local calendar days.
// Construct it with AddCalendarDays or TryAddCalendarDays.
type CalendarDaysExpr struct {
	Timestamp Expr
	Days      Expr
	resolver  CivilTimeResolver
	tolerant  bool
}

func (CalendarDaysExpr) exprNode() {}

// AddCalendarDays preserves local clock fields under captured rules, not elapsed 24-hour periods.
// Zero days preserves the exact input instant, including within a fold.
func AddCalendarDays(timestamp, days Expr, resolver CivilTimeResolver) CalendarDaysExpr {
	return CalendarDaysExpr{timestamp, days, resolver, false}
}

// TryAddCalendarDays turns calendar range, gap, fold and Timestamp range into
// NULL. Child, configuration, storage, budget and cancellation failures propagate.
func TryAddCalendarDays(timestamp, days Expr, resolver CivilTimeResolver) CalendarDaysExpr {
	return CalendarDaysExpr{timestamp, days, resolver, true}
}

// Alias names the resulting UTC Timestamp column.
func (e CalendarDaysExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the operation; strong captured resolver identity is separate.
func (e CalendarDaysExpr) String() string {
	name := "ADD_CALENDAR_DAYS"
	if e.tolerant {
		name = "TRY_ADD_CALENDAR_DAYS"
	}
	if e.resolver.state == nil {
		return fmt.Sprintf("%s(%s,%s,invalid)", name, e.Timestamp, e.Days)
	}
	s := e.resolver.state
	return fmt.Sprintf("%s(%s,%s,%q,TZif:%x,fold:%d)", name, e.Timestamp, e.Days, s.zone.name, s.zone.digest, s.options.Fold)
}
func bindCalendarDays(bound *BoundExpr, n CalendarDaysExpr) error {
	if n.resolver.state == nil {
		return ErrCivilConfiguration
	}
	if bound.Children[0].Field.Type != memory.TypeTimestamp || bound.Children[1].Field.Type != memory.TypeInt64 {
		return fmt.Errorf("calendar day offset requires Timestamp and Int64 days")
	}
	bound.Field = computedField(memory.TypeTimestamp, n.tolerant || bound.Children[0].Field.Nullable || bound.Children[1].Field.Nullable)
	bound.MayError = !n.tolerant
	return nil
}
func evalCalendarDays(out, timestamp, offset *scalarRegister, state *civilTimeResolverState, tolerant bool, rows int, active memory.Bitmap) error {
	if state == nil {
		return ErrCivilConfiguration
	}
	if rows < 0 || timestamp.typ != memory.TypeTimestamp || offset.typ != memory.TypeInt64 || timestamp.validity != nil && len(timestamp.validity) < memory.WordsFor(rows) || offset.validity != nil && len(offset.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("invalid calendar day offset input storage")
	}
	if out.typ != memory.TypeTimestamp || len(out.i64) < rows || len(out.ownedValidity) < memory.WordsFor(rows) {
		return fmt.Errorf("calendar day offset output buffers not prepared")
	}
	valid := ensureValidity(out, rows)
	clear(valid)
	out.validity = valid
	ctx := dateEvaluationContext{out.checkContext}
	r := CivilTimeResolver{state}
	for row := 0; row < rows; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		out.i64[row] = 0
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(timestamp, row) || !isValidRegister(offset, row) {
			continue
		}
		if !timestamp.constant && row >= len(timestamp.i64) || !offset.constant && row >= len(offset.i64) {
			return fmt.Errorf("short demanded calendar day offset storage")
		}
		instant, err := registerInt64(timestamp, row)
		if err != nil {
			return err
		}
		days, err := registerInt64(offset, row)
		if err != nil {
			return err
		}
		value, err := r.AddCalendarDays(ctx, instant, days)
		if err != nil {
			if tolerant && (errors.Is(err, ErrCivilRange) || errors.Is(err, ErrCivilGap) || errors.Is(err, ErrCivilFold)) {
				continue
			}
			return fmt.Errorf("calendar day offset at row %d: %w", row, err)
		}
		out.i64[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
