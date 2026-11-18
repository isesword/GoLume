package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// DecimalRoundingMode explicitly selects how discarded decimal digits round.
// The zero value is invalid; no rounding policy is implicit.
type DecimalRoundingMode uint8

const (
	// DecimalRoundTowardZero discards fractional digits toward zero.
	DecimalRoundTowardZero DecimalRoundingMode = iota + 1
	// DecimalRoundAwayFromZero increments magnitude whenever digits are discarded.
	DecimalRoundAwayFromZero
	// DecimalRoundFloor rounds toward negative infinity.
	DecimalRoundFloor
	// DecimalRoundCeil rounds toward positive infinity.
	DecimalRoundCeil
	// DecimalRoundHalfEven rounds to nearest, resolving ties to an even coefficient.
	DecimalRoundHalfEven
	// DecimalRoundHalfAwayFromZero rounds to nearest, resolving ties away from zero.
	DecimalRoundHalfAwayFromZero
)

// String returns a stable readable mode name, or identifies an invalid value.
func (mode DecimalRoundingMode) String() string {
	switch mode {
	case DecimalRoundTowardZero:
		return "toward-zero"
	case DecimalRoundAwayFromZero:
		return "away-from-zero"
	case DecimalRoundFloor:
		return "floor"
	case DecimalRoundCeil:
		return "ceil"
	case DecimalRoundHalfEven:
		return "half-even"
	case DecimalRoundHalfAwayFromZero:
		return "half-away-from-zero"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(mode))
	}
}

// Decimal128RoundExpr rounds a native decimal to explicit result metadata.
type Decimal128RoundExpr struct {
	Value            Expr
	Precision, Scale int32
	Mode             DecimalRoundingMode
}

func (Decimal128RoundExpr) exprNode() {}

// String includes the target precision, scale, and explicit rounding mode.
func (v Decimal128RoundExpr) String() string {
	return fmt.Sprintf("ROUND_DECIMAL128(%s,%d,%d,%s)", v.Value, v.Precision, v.Scale, v.Mode)
}

// RoundDecimal128 rounds native decimal values once at the target scale using
// mode. Precision must be 1..38, scale 0..precision, and mode must be explicit
// and valid, including in dead branches. NULLs remain NULL. Increasing scale
// is exact. The final rounded coefficient must fit precision or evaluation
// fails at the first demanded non-NULL row; no saturation or float conversion
// occurs. Existing exact decimal operations are unaffected.
func RoundDecimal128(value Expr, precision, scale int32, mode DecimalRoundingMode) Decimal128RoundExpr {
	return Decimal128RoundExpr{Value: value, Precision: precision, Scale: scale, Mode: mode}
}

// Alias names a rounded decimal output.
func (v Decimal128RoundExpr) Alias(name string) NamedExpr { return Named(v, name) }

func validDecimalRoundingMode(mode DecimalRoundingMode) bool {
	return mode >= DecimalRoundTowardZero && mode <= DecimalRoundHalfAwayFromZero
}

func evalDecimalRound(out, in *scalarRegister, mode DecimalRoundingMode, rows int) error {
	copyRegisterValidity(out, in, rows)
	precision, scale := out.decimalType.DecimalPrecision, out.decimalType.DecimalScale
	for row := 0; row < rows; row++ {
		if row&63 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(out, row) {
			out.decimal[row] = memory.Decimal128{}
			continue
		}
		value, status := decimalRoundValue(in.decimal[row], in.decimalType.DecimalScale, precision, scale, mode)
		if status != decimalRescaleOK {
			return fmt.Errorf("round to decimal128(%d,%d) overflows at row %d (mode %s)", precision, scale, row, mode)
		}
		out.decimal[row] = value
	}
	return nil
}

// decimalRoundValue requires validated metadata, mode, and coefficients. For
// scale reduction, at most two two-limb divisions retain the complete remainder:
// r = r0 + r1*10^19. It is less than 10^38, so every intermediate fits signed
// 128 bits. Rounding occurs only after both divisions, never per chunk. There
// is no per-row heap allocation or floating-point operation.
func decimalRoundValue(value memory.Decimal128, fromScale, precision, scale int32, mode DecimalRoundingMode) (memory.Decimal128, decimalRescaleStatus) {
	if scale >= fromScale {
		return decimalRescaleValue(value, fromScale, precision, scale)
	}
	negative := value.Sign() < 0
	if negative {
		value = value.Negate()
	}
	hi, lo := uint64(value.HighBits()), value.LowBits()
	remainder := memory.Decimal128{}
	digitsRemoved := int32(0)
	for remaining := fromScale - scale; remaining > 0; {
		digits := min(remaining, 19)
		divisor := decimal128.GetScaleMultiplier(int(digits)).LowBits()
		qhi, rhi := hi/divisor, hi%divisor
		qlo, rem := bits.Div64(rhi, lo, divisor)
		part := decimal128.New(0, rem)
		if digitsRemoved != 0 {
			part = part.Mul(decimal128.GetScaleMultiplier(int(digitsRemoved)))
		}
		remainder = remainder.Add(part)
		hi, lo = qhi, qlo
		digitsRemoved += digits
		remaining -= digits
	}
	quotient := decimal128.New(int64(hi), lo)
	increment := false
	if remainder != (memory.Decimal128{}) {
		switch mode {
		case DecimalRoundAwayFromZero:
			increment = true
		case DecimalRoundFloor:
			increment = negative
		case DecimalRoundCeil:
			increment = !negative
		case DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero:
			// 10^k is even for k >= 1. Computing half avoids doubling the remainder.
			half := decimal128.GetScaleMultiplier(int(fromScale - scale - 1)).Mul(decimal128.FromI64(5))
			comparison := remainder.Cmp(half)
			increment = comparison > 0 || comparison == 0 && (mode == DecimalRoundHalfAwayFromZero || lo&1 != 0)
		}
	}
	if increment {
		quotient = quotient.Add(decimal128.FromI64(1))
	}
	if negative {
		quotient = quotient.Negate()
	}
	if !memory.Decimal128FitsPrecision(quotient, precision) {
		return memory.Decimal128{}, decimalRescaleOverflow
	}
	return quotient, decimalRescaleOK
}
