package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Decimal128DivideRoundExpr divides native decimals and rounds once to explicit
// target metadata using an explicit mode. Existing exact division is unchanged.
type Decimal128DivideRoundExpr struct {
	Left, Right      Expr
	Precision, Scale int32
	Mode             DecimalRoundingMode
}

func (Decimal128DivideRoundExpr) exprNode() {}

// String includes both operands, target metadata, and the explicit mode.
func (e Decimal128DivideRoundExpr) String() string {
	return fmt.Sprintf("DIVIDE_ROUND_DECIMAL128(%s,%s,%d,%d,%s)", e.Left, e.Right, e.Precision, e.Scale, e.Mode)
}

// DivideRoundDecimal128 rounds the exact decimal quotient once at the target
// scale. Precision must be 1..38, scale 0..precision, and mode must be one of
// the six explicit DecimalRoundingMode values. Binding validates both native
// inputs, target metadata, and mode even in dead branches. A NULL operand
// produces NULL. Otherwise zero divisors (including 0/0) fail before arithmetic;
// the final rounded coefficient must fit precision or evaluation fails. There
// is no intermediate rounding, float conversion, saturation, or inexact error.
func DivideRoundDecimal128(left, right Expr, precision, scale int32, mode DecimalRoundingMode) Decimal128DivideRoundExpr {
	return Decimal128DivideRoundExpr{Left: left, Right: right, Precision: precision, Scale: scale, Mode: mode}
}

// Alias names a rounded quotient output.
func (e Decimal128DivideRoundExpr) Alias(name string) NamedExpr { return Named(e, name) }

func evalDecimalDivideRound(out, a, b *scalarRegister, mode DecimalRoundingMode, rows int) error {
	combineValidity(out, a, b, rows)
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
		value, status := decimalDivideRoundValue(a.decimal[row], a.decimalType.DecimalScale, b.decimal[row], b.decimalType.DecimalScale, precision, scale, mode)
		switch status {
		case decimalDivideZero:
			return fmt.Errorf("rounded divide to decimal128(%d,%d) by zero at row %d (mode %s)", precision, scale, row, mode)
		case decimalDivideOverflow:
			return fmt.Errorf("rounded divide to decimal128(%d,%d) overflows at row %d (mode %s)", precision, scale, row, mode)
		}
		out.decimal[row] = value
	}
	if out.checkContext != nil {
		return out.checkContext()
	}
	return nil
}

// decimalDivideRoundValue requires validated native coefficients, metadata,
// and mode. The exact target coefficient is a*10^(scale+bs-as)/b. The exponent
// is -38..76, so numerator<10^114 and denominator<10^76 fit the existing 384-bit
// magnitude. Full quotient and remainder survive until the sole final rounding
// decision; no per-row heap allocation or floating-point operation is needed.
func decimalDivideRoundValue(a memory.Decimal128, as int32, b memory.Decimal128, bs, precision, scale int32, mode DecimalRoundingMode) (memory.Decimal128, decimalDivideStatus) {
	if b == (memory.Decimal128{}) {
		return memory.Decimal128{}, decimalDivideZero
	}
	if a == (memory.Decimal128{}) {
		return memory.Decimal128{}, decimalDivideOK
	}
	negative := (a.Sign() < 0) != (b.Sign() < 0)
	if a.Sign() < 0 {
		a = a.Negate()
	}
	if b.Sign() < 0 {
		b = b.Negate()
	}
	numerator := decimalDivideMagnitude{a.LowBits(), uint64(a.HighBits())}
	denominator := decimalDivideMagnitude{b.LowBits(), uint64(b.HighBits())}
	if exponent := scale + bs - as; exponent >= 0 {
		numerator.scaleUp(exponent)
	} else {
		denominator.scaleUp(-exponent)
	}
	quotient, remainder := decimalDivideQuoRem(numerator, denominator)
	increment := false
	if remainder != (decimalDivideMagnitude{}) {
		switch mode {
		case DecimalRoundAwayFromZero:
			increment = true
		case DecimalRoundFloor:
			increment = negative
		case DecimalRoundCeil:
			increment = !negative
		case DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero:
			// r compared with D-r is exactly 2r compared with D, including odd D,
			// without doubling a wide remainder or truncating half the denominator.
			complement := denominator
			var borrow uint64
			for i := range complement {
				complement[i], borrow = bits.Sub64(complement[i], remainder[i], borrow)
			}
			comparison := remainder.compare(complement)
			increment = comparison > 0 || comparison == 0 && (mode == DecimalRoundHalfAwayFromZero || quotient[0]&1 != 0)
		}
	}
	if increment {
		carry := uint64(1)
		for i := range quotient {
			quotient[i], carry = bits.Add64(quotient[i], 0, carry)
		}
		if carry != 0 {
			return memory.Decimal128{}, decimalDivideOverflow
		}
	}
	if quotient[5]|quotient[4]|quotient[3]|quotient[2]|(quotient[1]>>63) != 0 {
		return memory.Decimal128{}, decimalDivideOverflow
	}
	value := decimal128.New(int64(quotient[1]), quotient[0])
	if negative {
		value = value.Negate()
	}
	if !memory.Decimal128FitsPrecision(value, precision) {
		return memory.Decimal128{}, decimalDivideOverflow
	}
	return value, decimalDivideOK
}
