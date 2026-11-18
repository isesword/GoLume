package expr

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// ResolveDate resolves a Date32 epoch-day ordinal to local midnight using the
// captured rules and explicit fold/gap policies. It is not a UTC midnight cast.
// All int32 day values are accepted as dates; selected instants outside Timestamp
// nanoseconds return ErrCivilRange. Configuration and context errors are preserved.
func (r CivilTimeResolver) ResolveDate(ctx context.Context, days int32) (int64, error) {
	if ctx == nil {
		return 0, ErrCivilConfiguration
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.state == nil {
		return 0, ErrCivilConfiguration
	}
	// This multiplication encodes a timezone-free date only. Int32 days safely
	// fit Unix seconds; the UTC instant is determined by resolveFields, not here.
	y, m, d := time.Unix(int64(days)*86400, 0).UTC().Date()
	return r.resolveFields(ctx, civilFields{year: y, month: int(m), day: d})
}

// DateAtMidnightExpr resolves Date32 values to local midnight UTC instants.
// Construct it with DateAtMidnight or TryDateAtMidnight.
type DateAtMidnightExpr struct {
	Value    Expr
	resolver CivilTimeResolver
	tolerant bool
}

func (DateAtMidnightExpr) exprNode() {}

// DateAtMidnight resolves Date32 input using explicit captured midnight policies.
func DateAtMidnight(value Expr, resolver CivilTimeResolver) DateAtMidnightExpr {
	return DateAtMidnightExpr{value, resolver, false}
}

// TryDateAtMidnight converts midnight gap, fold and Timestamp-range errors to NULL.
// Configuration, storage, budget, cancellation and child errors remain errors.
func TryDateAtMidnight(value Expr, resolver CivilTimeResolver) DateAtMidnightExpr {
	return DateAtMidnightExpr{value, resolver, true}
}

// Alias names the resulting UTC Timestamp column.
func (e DateAtMidnightExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the expression; captured resolver identity is kept separately.
func (e DateAtMidnightExpr) String() string {
	name := "DATE_AT_MIDNIGHT"
	if e.tolerant {
		name = "TRY_DATE_AT_MIDNIGHT"
	}
	if e.resolver.state == nil {
		return fmt.Sprintf("%s(%s,invalid)", name, e.Value)
	}
	s := e.resolver.state
	return fmt.Sprintf("%s(%s,%q,TZif:%x,fold:%d)", name, e.Value, s.zone.name, s.zone.digest, s.options.Fold)
}
func bindDateAtMidnight(bound *BoundExpr, n DateAtMidnightExpr) error {
	if n.resolver.state == nil {
		return ErrCivilConfiguration
	}
	if bound.Children[0].Field.Type != memory.TypeDate32 {
		return fmt.Errorf("date at midnight requires Date32 input")
	}
	bound.Field = computedField(memory.TypeTimestamp, n.tolerant || bound.Children[0].Field.Nullable)
	bound.MayError = !n.tolerant
	return nil
}
func evalDateAtMidnight(out, in *scalarRegister, state *civilTimeResolverState, tolerant bool, rows int, active memory.Bitmap) error {
	if state == nil {
		return ErrCivilConfiguration
	}
	if rows < 0 || in.validity != nil && len(in.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("short midnight validity")
	}
	if in.typ != memory.TypeDate32 {
		return fmt.Errorf("invalid midnight Date32 storage")
	}
	valid := ensureValidity(out, rows)
	clear(valid)
	out.validity = valid
	ctx := dateEvaluationContext{out.checkContext}
	resolver := CivilTimeResolver{state}
	for row := 0; row < rows; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		out.i64[row] = 0
		if active != nil && !memory.IsSet(active, row) || !isValidRegister(in, row) {
			continue
		}
		if !in.constant && row >= len(in.i32) {
			return fmt.Errorf("short demanded midnight Date32 storage")
		}
		// A malformed physical constant is a storage failure, never a Try row error.
		if in.constant && (in.i64lit < math.MinInt32 || in.i64lit > math.MaxInt32) {
			return fmt.Errorf("midnight Date32 constant outside int32 storage")
		}
		days, err := registerInt64(in, row)
		if err != nil {
			return err
		}
		value, err := resolver.ResolveDate(ctx, int32(days))
		if err != nil {
			if tolerant && civilRowError(err) {
				continue
			}
			return fmt.Errorf("date at midnight at row %d: %w", row, err)
		}
		out.i64[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
