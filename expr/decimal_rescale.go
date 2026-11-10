package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Decimal128RescaleExpr converts a native decimal to explicit precision/scale
// without changing its numerical value. Metadata is validated at bind time;
// coefficient overflow and any loss of fractional digits are execution errors.
type Decimal128RescaleExpr struct {
	Value            Expr
	Precision, Scale int32
}

func (Decimal128RescaleExpr) exprNode() {}

// String returns the explicit target precision and scale.
func (v Decimal128RescaleExpr) String() string {
	return fmt.Sprintf("RESCALE_DECIMAL128(%s,%d,%d)", v.Value, v.Precision, v.Scale)
}

// RescaleDecimal128 converts only native Decimal128 values. Raising scale
// appends coefficient zeros; lowering scale requires exact division by the
// corresponding power of ten. A nonzero remainder or a coefficient outside
// the target precision fails at the first demanded non-NULL row. NULLs remain
// NULL. This operation never rounds or converts through floating point.
//
// Precision must be 1..38 and scale 0..precision, including for dead branches.
// Explicitly rescaling both branches to the same precision/scale permits them
// to participate in IF, CASE, and COALESCE without implicit type coercion.
func RescaleDecimal128(value Expr, precision, scale int32) Decimal128RescaleExpr {
	return Decimal128RescaleExpr{Value: value, Precision: precision, Scale: scale}
}

// Alias names a rescaled decimal output.
func (v Decimal128RescaleExpr) Alias(name string) NamedExpr { return Named(v, name) }

type decimalRescaleStatus uint8

const (
	decimalRescaleOK decimalRescaleStatus = iota
	decimalRescaleOverflow
	decimalRescaleInexact
)

func evalDecimalRescale(out, in *scalarRegister, rows int) error {
	copyRegisterValidity(out, in, rows)
	fromScale := in.decimalType.DecimalScale
	precision, scale := out.decimalType.DecimalPrecision, out.decimalType.DecimalScale
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			out.decimal[row] = memory.Decimal128{}
			continue
		}
		value, status := decimalRescaleValue(in.decimal[row], fromScale, precision, scale)
		switch status {
		case decimalRescaleOverflow:
			return fmt.Errorf("rescale to decimal128(%d,%d) overflows at row %d", precision, scale, row)
		case decimalRescaleInexact:
			return fmt.Errorf("rescale to decimal128(%d,%d) loses fractional digits at row %d", precision, scale, row)
		}
		out.decimal[row] = value
	}
	return nil
}

// decimalRescaleValue requires validated native coefficients and metadata.
// Upscaling checks the exact decimal bound before multiplying, so no wrapped
// product can be accepted. Downscaling uses at most two unsigned two-limb
// divisions, each by a power of ten that fits uint64. The remainder must be
// zero at every step. There is no per-row allocation or floating point.
func decimalRescaleValue(value memory.Decimal128, fromScale, precision, scale int32) (memory.Decimal128, decimalRescaleStatus) {
	if value == (memory.Decimal128{}) {
		return value, decimalRescaleOK
	}
	if delta := scale - fromScale; delta > 0 {
		if !memory.Decimal128FitsPrecision(value, precision-delta) {
			return memory.Decimal128{}, decimalRescaleOverflow
		}
		return value.Mul(decimal128.GetScaleMultiplier(int(delta))), decimalRescaleOK
	} else if delta < 0 {
		negative := value.Sign() < 0
		if negative {
			value = value.Negate() // Valid coefficients exclude the signed minimum.
		}
		hi, lo := uint64(value.HighBits()), value.LowBits()
		for remaining := -delta; remaining > 0; {
			digits := min(remaining, 19)
			divisor := decimal128.GetScaleMultiplier(int(digits)).LowBits()
			quotientHi, remainderHi := hi/divisor, hi%divisor
			quotientLo, remainder := bits.Div64(remainderHi, lo, divisor)
			if remainder != 0 {
				return memory.Decimal128{}, decimalRescaleInexact
			}
			hi, lo = quotientHi, quotientLo
			remaining -= digits
		}
		value = decimal128.New(int64(hi), lo)
		if negative {
			value = value.Negate()
		}
	}
	if !memory.Decimal128FitsPrecision(value, precision) {
		return memory.Decimal128{}, decimalRescaleOverflow
	}
	return value, decimalRescaleOK
}
