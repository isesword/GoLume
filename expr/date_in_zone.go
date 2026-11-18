package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"time"
)

// DateInZoneExpr converts a Timestamp instant to its captured-zone calendar date.
type DateInZoneExpr struct {
	Value Expr
	zone  TimeZone
}

func (DateInZoneExpr) exprNode() {}

// DateInZone returns the local calendar date as epoch days, not a midnight instant.
// The input must be Timestamp and zone must be a valid captured TimeZone.
func DateInZone(value Expr, zone TimeZone) DateInZoneExpr { return DateInZoneExpr{value, zone} }

// Alias names the resulting Date32 column.
func (e DateInZoneExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the captured rules without using the label as cache identity.
func (e DateInZoneExpr) String() string {
	if e.zone.state == nil {
		return fmt.Sprintf("DATE_IN_ZONE(%s,invalid)", e.Value)
	}
	s := e.zone.state
	if s.source == "TZif" {
		return fmt.Sprintf("DATE_IN_ZONE(%s,%q,TZif:%x)", e.Value, s.name, s.digest)
	}
	return fmt.Sprintf("DATE_IN_ZONE(%s,%q,%s)", e.Value, s.name, s.source)
}

func bindDateInZone(bound *BoundExpr, node DateInZoneExpr) error {
	if bound.Children[0].Field.Type != memory.TypeTimestamp {
		return fmt.Errorf("DATE_IN_ZONE requires Timestamp input")
	}
	if node.zone.state == nil || node.zone.state.location == nil {
		return fmt.Errorf("DATE_IN_ZONE requires a valid TimeZone handle")
	}
	bound.Field = computedField(memory.TypeDate32, bound.Children[0].Field.Nullable)
	return nil
}

func evalDateInZone(out, in *scalarRegister, zone *timeZoneState, rows int) error {
	copyRegisterValidity(out, in, rows)
	for row := 0; row < rows; row++ {
		if row&63 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(out, row) {
			out.i32[row] = 0
			continue
		}
		ns, err := registerInt64(in, row)
		if err != nil {
			return err
		}
		instant := time.Unix(ns/int64(time.Second), ns%int64(time.Second)).In(zone.location)
		y, m, d := instant.Date()
		// UTC midnight only encodes the already selected civil date as an ordinal.
		// It does not resolve a local midnight, which could be absent or ambiguous.
		// Unix seconds remain safe even when that midnight is outside int64 nanos.
		days := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400
		// Accepted TZif offsets are signed 32-bit seconds; POSIX footer offsets
		// are smaller. Combined with int64 nanosecond instants this fits Date32.
		// This is an invariant defense, not an extra error domain for valid inputs.
		if days < math.MinInt32 || days > math.MaxInt32 {
			return fmt.Errorf("DATE_IN_ZONE internal date range invariant at row %d", row)
		}
		out.i32[row] = int32(days)
	}
	if out.checkContext != nil {
		return out.checkContext()
	}
	return nil
}
