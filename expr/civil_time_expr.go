package expr

import (
	"errors"
	"fmt"
	"gocolumn/memory"
)

// CivilTimestampExpr resolves local text using a captured raw-rule resolver.
type CivilTimestampExpr struct {
	Value    Expr
	resolver CivilTimeResolver
	tolerant bool
}

func (CivilTimestampExpr) exprNode() {}

// ParseCivilTimestamp parses exact civil text and reports invalid row values.
func ParseCivilTimestamp(value Expr, resolver CivilTimeResolver) CivilTimestampExpr {
	return CivilTimestampExpr{value, resolver, false}
}

// TryParseCivilTimestamp converts only syntax, gap, fold and range errors to NULL.
// Configuration, storage, budget and cancellation failures remain errors.
func TryParseCivilTimestamp(value Expr, resolver CivilTimeResolver) CivilTimestampExpr {
	return CivilTimestampExpr{value, resolver, true}
}

// Alias names the resulting UTC Timestamp column.
func (e CivilTimestampExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the expression; display text is not its cache identity.
func (e CivilTimestampExpr) String() string {
	name := "PARSE_CIVIL_TIMESTAMP"
	if e.tolerant {
		name = "TRY_PARSE_CIVIL_TIMESTAMP"
	}
	if e.resolver.state == nil {
		return fmt.Sprintf("%s(%s,invalid)", name, e.Value)
	}
	s := e.resolver.state
	return fmt.Sprintf("%s(%s,%q,TZif:%x,fold:%d)", name, e.Value, s.zone.name, s.zone.digest, s.options.Fold)
}
func bindCivilTimestamp(bound *BoundExpr, n CivilTimestampExpr) error {
	if n.resolver.state == nil {
		return ErrCivilConfiguration
	}
	if bound.Children[0].Field.Type != memory.TypeString {
		return fmt.Errorf("civil timestamp requires String input")
	}
	bound.Field = computedField(memory.TypeTimestamp, n.tolerant || bound.Children[0].Field.Nullable)
	// Bind combines this intrinsic row-error domain with child errors.
	bound.MayError = !n.tolerant
	return nil
}
func civilRowError(err error) bool {
	return errors.Is(err, ErrCivilSyntax) || errors.Is(err, ErrCivilGap) || errors.Is(err, ErrCivilFold) || errors.Is(err, ErrCivilRange)
}
func evalCivilTimestamp(out, in *scalarRegister, state *civilTimeResolverState, tolerant bool, rows int, active memory.Bitmap) error {
	if state == nil {
		return ErrCivilConfiguration
	}
	if rows < 0 || in.validity != nil && len(in.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("short civil timestamp validity")
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
		text, err := castStringBytes(in, row)
		if err != nil {
			return err
		}
		var value int64
		if len(text) < 19 || len(text) > 29 {
			err = ErrCivilSyntax
		} else {
			value, err = resolver.Parse(ctx, string(text))
		}
		if err != nil {
			if tolerant && civilRowError(err) {
				continue
			}
			return fmt.Errorf("civil timestamp at row %d: %w", row, err)
		}
		out.i64[row] = value
		memory.Set(valid, row)
	}
	return ctx.Err()
}
