package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// ErrCivilClock identifies a Time64 wall-clock coefficient outside [0, 86400e9).
// It is local to explicit civil composition, not a new global Time64 restriction.
var ErrCivilClock = errors.New("civil clock outside one day")

// ResolveDateTime resolves a Date32 date and Time64 nanosecond wall clock using
// captured civil policies. The clock is not elapsed time since local midnight.
// A fold policy selects the instant before checking Timestamp nanosecond range.
func (r CivilTimeResolver) ResolveDateTime(ctx context.Context, days int32, clockNanos int64) (int64, error) {
	if ctx == nil {
		return 0, ErrCivilConfiguration
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.state == nil {
		return 0, ErrCivilConfiguration
	}
	if clockNanos < 0 || clockNanos >= 86400e9 {
		return 0, ErrCivilClock
	}
	// Int32 epoch days safely fit seconds; no date coefficient is converted to
	// nanoseconds before the matcher selects and range-checks the actual instant.
	y, m, d := time.Unix(int64(days)*86400, 0).UTC().Date()
	seconds := clockNanos / int64(time.Second)
	return r.resolveFields(ctx, civilFields{year: y, month: int(m), day: d, hour: int(seconds / 3600), minute: int(seconds / 60 % 60), second: int(seconds % 60), nano: int(clockNanos % int64(time.Second))})
}

// CivilComposeExpr resolves a typed local date and clock to a UTC Timestamp.
// Construct it with ComposeCivilTimestamp or TryComposeCivilTimestamp.
type CivilComposeExpr struct {
	Date     Expr
	Clock    Expr
	resolver CivilTimeResolver
	tolerant bool
}

func (CivilComposeExpr) exprNode() {}

// ComposeCivilTimestamp combines Date32 and Time64(ns) using explicit civil rules.
// No implicit integer units, UTC default, day rollover or gap shifting is applied.
func ComposeCivilTimestamp(date, clock Expr, resolver CivilTimeResolver) CivilComposeExpr {
	return CivilComposeExpr{date, clock, resolver, false}
}

// TryComposeCivilTimestamp turns invalid clock, gap, fold and Timestamp range into
// NULL. Child, configuration, storage, budget and cancellation failures propagate.
func TryComposeCivilTimestamp(date, clock Expr, resolver CivilTimeResolver) CivilComposeExpr {
	return CivilComposeExpr{date, clock, resolver, true}
}

// Alias names the resulting UTC Timestamp column.
func (e CivilComposeExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the operation; strong captured resolver identity is separate.
func (e CivilComposeExpr) String() string {
	name := "COMPOSE_CIVIL_TIMESTAMP"
	if e.tolerant {
		name = "TRY_COMPOSE_CIVIL_TIMESTAMP"
	}
	if e.resolver.state == nil {
		return fmt.Sprintf("%s(%s,%s,invalid)", name, e.Date, e.Clock)
	}
	s := e.resolver.state
	return fmt.Sprintf("%s(%s,%s,%q,TZif:%x,fold:%d)", name, e.Date, e.Clock, s.zone.name, s.zone.digest, s.options.Fold)
}
func bindCivilCompose(bound *BoundExpr, n CivilComposeExpr) error {
	if n.resolver.state == nil {
		return ErrCivilConfiguration
	}
	if bound.Children[0].Field.Type != memory.TypeDate32 || bound.Children[1].Field.Type != memory.TypeTime64 {
		return fmt.Errorf("civil composition requires Date32 date and Time64 nanosecond clock")
	}
	bound.Field = computedField(memory.TypeTimestamp, n.tolerant || bound.Children[0].Field.Nullable || bound.Children[1].Field.Nullable)
	bound.MayError = !n.tolerant
	return nil
}
func evalCivilCompose(out, date, clock *scalarRegister, state *civilTimeResolverState, tolerant bool, rows int, active memory.Bitmap) error {
	if state == nil {
		return ErrCivilConfiguration
	}
	if rows < 0 || date.typ != memory.TypeDate32 || clock.typ != memory.TypeTime64 || date.validity != nil && len(date.validity) < memory.WordsFor(rows) || clock.validity != nil && len(clock.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("invalid civil composition input storage")
	}
	if out.typ != memory.TypeTimestamp || len(out.i64) < rows || len(out.ownedValidity) < memory.WordsFor(rows) {
		return fmt.Errorf("civil composition output buffers not prepared")
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
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(date, row) || !isValidRegister(clock, row) {
			continue
		}
		if !date.constant && row >= len(date.i32) || !clock.constant && row >= len(clock.i64) {
			return fmt.Errorf("short demanded civil composition storage")
		}
		if date.constant && (date.i64lit < math.MinInt32 || date.i64lit > math.MaxInt32) {
			return fmt.Errorf("civil composition Date32 constant outside storage range")
		}
		days, err := registerInt64(date, row)
		if err != nil {
			return err
		}
		ns, err := registerInt64(clock, row)
		if err != nil {
			return err
		}
		value, err := r.ResolveDateTime(ctx, int32(days), ns)
		if err != nil {
			if tolerant && (errors.Is(err, ErrCivilClock) || civilRowError(err)) {
				continue
			}
			return fmt.Errorf("civil composition at row %d: %w", row, err)
		}
		out.i64[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
