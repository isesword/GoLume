package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// bindDecimalArithmetic preserves all fractional digits. The natural result
// precision is capped at 38, not the scale: a demanded non-NULL coefficient
// outside that declared precision is a runtime error, never a rounded value.
func bindDecimalArithmetic(bound *BoundExpr, op Op) error {
	a, b := bound.Children[0].Field, bound.Children[1].Field
	for _, field := range []memory.Field{a, b} {
		if err := field.ValidateDecimal128(); err != nil {
			return fmt.Errorf("operator %s requires two native decimal128 operands: %w", op, err)
		}
	}
	var precision, scale int32
	if op == OpMul {
		precision, scale = a.DecimalPrecision+b.DecimalPrecision, a.DecimalScale+b.DecimalScale
		if scale > 38 {
			return fmt.Errorf("decimal multiplication result scale %d exceeds 38; implicit rounding is not supported", scale)
		}
	} else {
		scale = max(a.DecimalScale, b.DecimalScale)
		precision = max(a.DecimalPrecision-a.DecimalScale, b.DecimalPrecision-b.DecimalScale) + scale + 1
	}
	field, err := memory.Decimal128Field("", a.Nullable || b.Nullable, min(precision, 38), scale)
	if err != nil {
		return err
	}
	bound.Field, bound.MayError = field, true
	return nil
}

func evalDecimalBinary(out, a, b *scalarRegister, op Op, rows int) error {
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			out.decimal[row] = memory.Decimal128{}
			continue
		}
		value, ok := decimalArithmeticValue(a.decimal[row], a.decimalType.DecimalScale, b.decimal[row], b.decimalType.DecimalScale, op, out.decimalType.DecimalPrecision)
		if !ok {
			return fmt.Errorf("checked %s arithmetic for decimal128(%d,%d) overflows at row %d", op, out.decimalType.DecimalPrecision, out.decimalType.DecimalScale, row)
		}
		out.decimal[row] = value
	}
	return nil
}

// decimalArithmeticValue operates only on validated coefficients and metadata.
// Alignment and multiplication need at most 76 digits; addition may add one
// digit, still below 2^256. A wide intermediate permits cancellation before
// checking the result bound. No heap-allocated big integers or floats occur.
func decimalArithmeticValue(a memory.Decimal128, as int32, b memory.Decimal128, bs int32, op Op, precision int32) (memory.Decimal128, bool) {
	if as == bs && (op == OpAdd || op == OpSub) {
		// The common path needs just two limbs. Signed overflow must be checked
		// before the decimal precision bound because Arrow arithmetic wraps.
		if op == OpSub {
			b = b.Negate() // Valid coefficients cannot be the signed minimum.
		}
		value := a.Add(b)
		if (a.HighBits()^value.HighBits())&(b.HighBits()^value.HighBits()) < 0 {
			return memory.Decimal128{}, false
		}
		return value, memory.Decimal128FitsPrecision(value, precision)
	}
	negativeA, negativeB := a.Sign() < 0, b.Sign() < 0
	if negativeA {
		a = a.Negate()
	}
	if negativeB {
		b = b.Negate()
	}
	var value [4]uint64
	negative := negativeA
	if op == OpMul {
		value, negative = decimalMagnitudeProduct(a, b), negativeA != negativeB
	} else {
		av, bv := [4]uint64{a.LowBits(), uint64(a.HighBits())}, [4]uint64{b.LowBits(), uint64(b.HighBits())}
		if as < bs {
			av = decimalMagnitudeProduct(a, decimal128.GetScaleMultiplier(int(bs-as)))
		} else if bs < as {
			bv = decimalMagnitudeProduct(b, decimal128.GetScaleMultiplier(int(as-bs)))
		}
		if op == OpSub {
			negativeB = !negativeB
		}
		if negativeA == negativeB {
			var carry uint64
			for i := range value {
				value[i], carry = bits.Add64(av[i], bv[i], carry)
			}
		} else {
			if decimalMagnitudeCompare(av, bv) < 0 {
				av, bv, negative = bv, av, negativeB
			}
			var borrow uint64
			for i := range value {
				value[i], borrow = bits.Sub64(av[i], bv[i], borrow)
			}
		}
	}
	if value[3] != 0 || value[2] != 0 || value[1]>>63 != 0 {
		return memory.Decimal128{}, false
	}
	result := decimal128.New(int64(value[1]), value[0])
	if negative {
		result = result.Negate()
	}
	return result, memory.Decimal128FitsPrecision(result, precision)
}

func decimalMagnitudeCompare(a, b [4]uint64) int {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
