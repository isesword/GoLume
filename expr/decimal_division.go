package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Decimal128DivideExpr divides native decimals with explicit result metadata.
// Binding validates both operands and the target even in dead branches. Value
// errors are deferred until a non-NULL result is demanded.
type Decimal128DivideExpr struct {
	Left, Right      Expr
	Precision, Scale int32
}

func (Decimal128DivideExpr) exprNode() {}

// String includes both operands and the target precision/scale.
func (v Decimal128DivideExpr) String() string {
	return fmt.Sprintf("DIVIDE_DECIMAL128(%s,%s,%d,%d)", v.Left, v.Right, v.Precision, v.Scale)
}

// DivideDecimal128 divides two native decimals exactly at precision 1..38 and
// scale 0..precision. A NULL operand makes the result NULL. A non-NULL zero
// divisor (including 0/0), nonzero quotient remainder at the target scale, or
// coefficient outside the target precision is an execution error. Remainder
// is checked before precision. No rounding or floating-point conversion occurs.
// Ordinary Div still rejects decimals because it has no explicit target type.
func DivideDecimal128(left, right Expr, precision, scale int32) Decimal128DivideExpr {
	return Decimal128DivideExpr{Left: left, Right: right, Precision: precision, Scale: scale}
}

// Alias names a decimal quotient output.
func (v Decimal128DivideExpr) Alias(name string) NamedExpr { return Named(v, name) }

type decimalDivideStatus uint8

const (
	decimalDivideOK decimalDivideStatus = iota
	decimalDivideZero
	decimalDivideInexact
	decimalDivideOverflow
)

func evalDecimalDivide(out, a, b *scalarRegister, rows int) error {
	combineValidity(out, a, b, rows)
	precision, scale := out.decimalType.DecimalPrecision, out.decimalType.DecimalScale
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			out.decimal[row] = memory.Decimal128{}
			continue
		}
		value, status := decimalDivideValue(a.decimal[row], a.decimalType.DecimalScale, b.decimal[row], b.decimalType.DecimalScale, precision, scale)
		switch status {
		case decimalDivideZero:
			return fmt.Errorf("divide to decimal128(%d,%d) by zero at row %d", precision, scale, row)
		case decimalDivideInexact:
			return fmt.Errorf("divide to decimal128(%d,%d) loses fractional digits at row %d", precision, scale, row)
		case decimalDivideOverflow:
			return fmt.Errorf("divide to decimal128(%d,%d) overflows at row %d", precision, scale, row)
		}
		out.decimal[row] = value
	}
	return nil
}

// decimalDivideValue requires validated native coefficients and metadata.
// The desired coefficient is a*10^(scale+bs-as)/b. The exponent is -38..76.
// For nonnegative exponents the numerator has at most 114 decimal digits;
// for negative exponents the denominator has at most 76. Both fit 384 bits.
// Keeping the complete quotient and remainder avoids premature overflow and
// establishes exactness before narrowing. All work is fixed-width stack data.
func decimalDivideValue(a memory.Decimal128, as int32, b memory.Decimal128, bs, precision, scale int32) (memory.Decimal128, decimalDivideStatus) {
	if b == (memory.Decimal128{}) {
		return memory.Decimal128{}, decimalDivideZero
	}
	if a == (memory.Decimal128{}) {
		return memory.Decimal128{}, decimalDivideOK
	}
	negative := (a.Sign() < 0) != (b.Sign() < 0)
	if a.Sign() < 0 {
		a = a.Negate() // Valid decimal coefficients exclude the signed minimum.
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
	if remainder != (decimalDivideMagnitude{}) {
		return memory.Decimal128{}, decimalDivideInexact
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

// decimalDivideMagnitude is a little-endian unsigned 384-bit integer. It is
// private to exact division; it adds no per-register or per-row heap storage.
type decimalDivideMagnitude [6]uint64

func (v *decimalDivideMagnitude) scaleUp(digits int32) {
	for digits > 0 {
		chunk := min(digits, 19)
		factor := decimal128.GetScaleMultiplier(int(chunk)).LowBits()
		var carry uint64
		for i := range v {
			hi, lo := bits.Mul64(v[i], factor)
			var overflow uint64
			v[i], overflow = bits.Add64(lo, carry, 0)
			carry = hi + overflow
		}
		// Callers' proven 114-digit bound makes the final carry zero.
		digits -= chunk
	}
}

func (v decimalDivideMagnitude) bitLen() int {
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] != 0 {
			return i*64 + bits.Len64(v[i])
		}
	}
	return 0
}

func (v decimalDivideMagnitude) compare(other decimalDivideMagnitude) int {
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] < other[i] {
			return -1
		}
		if v[i] > other[i] {
			return 1
		}
	}
	return 0
}

func decimalDivideQuoRem(numerator, denominator decimalDivideMagnitude) (quotient, remainder decimalDivideMagnitude) {
	// Common small divisors use one hardware division per numerator limb.
	if denominator[5]|denominator[4]|denominator[3]|denominator[2]|denominator[1] == 0 {
		var rem uint64
		for i := len(numerator) - 1; i >= 0; i-- {
			quotient[i], rem = bits.Div64(rem, numerator[i], denominator[0])
		}
		remainder[0] = rem
		return
	}
	remainder = numerator
	shift := numerator.bitLen() - denominator.bitLen()
	if shift < 0 {
		return
	}
	// The aligned divisor has the numerator's bit length, at most 379.
	var aligned decimalDivideMagnitude
	word, offset := shift/64, uint(shift%64)
	for i := len(aligned) - 1; i >= word; i-- {
		aligned[i] = denominator[i-word] << offset
		if offset != 0 && i > word {
			aligned[i] |= denominator[i-word-1] >> (64 - offset)
		}
	}
	for bit := shift; bit >= 0; bit-- {
		if remainder.compare(aligned) >= 0 {
			var borrow uint64
			for i := range remainder {
				remainder[i], borrow = bits.Sub64(remainder[i], aligned[i], borrow)
			}
			quotient[bit/64] |= uint64(1) << uint(bit%64)
		}
		for i := 0; i < len(aligned)-1; i++ {
			aligned[i] = aligned[i]>>1 | aligned[i+1]<<63
		}
		aligned[len(aligned)-1] >>= 1
	}
	return
}
