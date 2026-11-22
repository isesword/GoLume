package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"
)

// Int128ToString formats a signed binary Int128 expression as canonical base
// ten text. Zero is "0"; negative values have one leading minus; NULL remains
// NULL. No Decimal or floating conversion occurs. The result is an ordinary
// String column; this operation does not enable general casts from Int128.
func Int128ToString(value Expr) Unary { return Unary{Op: OpInt128ToString, Value: value} }

// formatInt128Decimal writes into caller-owned stack scratch and returns the
// occupied suffix. The widest value needs 39 digits and one sign byte.
func formatInt128Decimal(dst *[40]byte, value memory.Int128) []byte {
	hi, lo := uint64(value.High), value.Low
	negative := value.High < 0
	if negative {
		// Negate as unsigned limbs, so even Int128 minimum has a valid
		// magnitude (2^127) with no signed overflow.
		var carry uint64
		lo, carry = bits.Add64(^lo, 1, 0)
		hi = ^hi + carry
	}
	const chunk = 10_000_000_000_000_000_000 // 10^19 fits uint64.
	i := len(dst)
	if hi != 0 {
		// Magnitude is at most 2^127, so hi <= 2^63 < 10^19;
		// 128/64 division gives one uint64 quotient and 19 low digits.
		q, rem := bits.Div64(hi, lo, chunk)
		for end := i - 19; i > end; {
			i--
			dst[i] = '0' + byte(rem%10)
			rem /= 10
		}
		lo = q
	}
	for {
		i--
		dst[i] = '0' + byte(lo%10)
		lo /= 10
		if lo == 0 {
			break
		}
	}
	if negative {
		i--
		dst[i] = '-'
	}
	return dst[i:]
}

func evalInt128ToString(out, in *scalarRegister, rows int, active memory.Bitmap) error {
	if in.typ != memory.TypeInt128 || out.typ != memory.TypeString ||
		rows < 0 || len(in.i128) < rows || len(out.offsets) <= rows ||
		len(out.ownedValidity) < memory.WordsFor(rows) ||
		(in.validity != nil && len(in.validity) < memory.WordsFor(rows)) ||
		(active != nil && len(active) < memory.WordsFor(rows)) || out.checkContext == nil {
		return fmt.Errorf("invalid INT128_TO_STRING buffers")
	}
	// Offsets and validity belong to the admitted fixed workspace. Retained
	// payload capacity grows only through appendScalarResult admission.
	valid := ensureValidity(out, rows)
	clear(valid)
	offsets := out.offsets[:rows+1]
	offsets[0] = 0
	out.data = out.data[:0]
	var scratch [40]byte
	for row := range rows {
		if row&4095 == 0 {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if (active == nil || memory.IsSet(active, row)) && isValidRegister(in, row) {
			if err := appendScalarResult(out, formatInt128Decimal(&scratch, in.i128[row]), OpInt128ToString, row); err != nil {
				return err
			}
			memory.Set(valid, row)
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return out.checkContext()
}
