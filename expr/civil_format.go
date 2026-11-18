package expr

import (
	"fmt"
	"gocolumn/memory"
	"time"
)

// CivilFormatExpr formats a Timestamp as fixed local civil wall-clock text.
// Construct it with FormatCivilTimestamp; the text is not instant serialization.
type CivilFormatExpr struct {
	Value Expr
	zone  TimeZone
}

func (CivilFormatExpr) exprNode() {}

// FormatCivilTimestamp formats Timestamp input in explicit captured zone rules.
// Every valid result has exactly 29 ASCII bytes: YYYY-MM-DDTHH:MM:SS.fffffffff.
// It contains no offset, Z or zone label, so a fold can map two instants to the
// same text. Both existing loader and raw-TZif TimeZone handles are supported.
func FormatCivilTimestamp(value Expr, zone TimeZone) CivilFormatExpr {
	return CivilFormatExpr{value, zone}
}

// Alias names the resulting plain String column.
func (e CivilFormatExpr) Alias(name string) NamedExpr { return Named(e, name) }

// String describes the expression; captured state, not its label, defines identity.
func (e CivilFormatExpr) String() string {
	if e.zone.state == nil {
		return fmt.Sprintf("FORMAT_CIVIL_TIMESTAMP(%s,invalid)", e.Value)
	}
	s := e.zone.state
	if s.source == "TZif" {
		return fmt.Sprintf("FORMAT_CIVIL_TIMESTAMP(%s,%q,TZif:%x)", e.Value, s.name, s.digest)
	}
	return fmt.Sprintf("FORMAT_CIVIL_TIMESTAMP(%s,%q,%s)", e.Value, s.name, s.source)
}
func bindCivilFormat(bound *BoundExpr, n CivilFormatExpr) error {
	if n.zone.state == nil || n.zone.state.location == nil {
		return fmt.Errorf("civil formatting requires a valid TimeZone handle")
	}
	if bound.Children[0].Field.Type != memory.TypeTimestamp {
		return fmt.Errorf("civil formatting requires Timestamp input")
	}
	bound.Field = computedField(memory.TypeString, bound.Children[0].Field.Nullable)
	return nil
}

// writeCivilFormat fills stack scratch without producing an uncharged heap string.
func writeCivilFormat(dst *[29]byte, instant time.Time) error {
	year, month, day := instant.Date()
	hour, minute, second := instant.Clock()
	// Nanosecond Timestamp endpoints plus accepted signed-int32 TZif offsets
	// remain within four-digit years. Keep an invariant defense, never truncate.
	if year < 0 || year > 9999 {
		return fmt.Errorf("civil format year outside four-digit invariant")
	}
	put := func(start, width, value int) {
		for i := start + width - 1; i >= start; i-- {
			dst[i] = byte(value%10) + '0'
			value /= 10
		}
	}
	put(0, 4, year)
	put(5, 2, int(month))
	put(8, 2, day)
	put(11, 2, hour)
	put(14, 2, minute)
	put(17, 2, second)
	put(20, 9, instant.Nanosecond())
	dst[4] = '-'
	dst[7] = '-'
	dst[10] = 'T'
	dst[13] = ':'
	dst[16] = ':'
	dst[19] = '.'
	return nil
}
func evalCivilFormat(out, in *scalarRegister, zone *timeZoneState, rows int, active memory.Bitmap) error {
	if zone == nil || zone.location == nil {
		return fmt.Errorf("invalid civil formatting TimeZone")
	}
	if rows < 0 || rows == int(^uint(0)>>1) || in.typ != memory.TypeTimestamp || in.validity != nil && len(in.validity) < memory.WordsFor(rows) || active != nil && len(active) < memory.WordsFor(rows) {
		return fmt.Errorf("invalid civil formatting input storage")
	}
	// All retained fixed buffers are prepared and charged by the Workspace.
	// Never use an uncharged make fallback on a malformed internal register.
	if out.typ != memory.TypeString || len(out.offsets) < rows+1 || len(out.ownedValidity) < memory.WordsFor(rows) || out.checkContext == nil {
		return fmt.Errorf("civil formatting output buffers not prepared")
	}
	valid := ensureValidity(out, rows)
	clear(valid)
	out.validity = valid
	offsets := out.offsets[:rows+1]
	offsets[0] = 0
	out.data = out.data[:0]
	var scratch [29]byte
	for row := 0; row < rows; row++ {
		if err := out.checkContext(); err != nil {
			return err
		}
		if (active == nil || memory.IsSet(active, row)) && isValidRegister(in, row) {
			if !in.constant && row >= len(in.i64) {
				return fmt.Errorf("short demanded civil formatting Timestamp storage")
			}
			ns, err := registerInt64(in, row)
			if err != nil {
				return err
			}
			instant := time.Unix(ns/int64(time.Second), ns%int64(time.Second)).In(zone.location)
			if err = writeCivilFormat(&scratch, instant); err != nil {
				return err
			}
			if err = appendScalarBytes(out, scratch[:]); err != nil {
				return err
			}
			memory.Set(valid, row)
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return out.checkContext()
}
