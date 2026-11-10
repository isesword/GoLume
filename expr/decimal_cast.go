package expr

import (
	"fmt"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

// Decimal128CastExpr converts an integer, string, or native Decimal128 to an
// explicit decimal type. Tolerant replaces only conversion failures with NULL.
type Decimal128CastExpr struct {
	Value            Expr
	Precision, Scale int32
	Tolerant         bool
}

func (Decimal128CastExpr) exprNode() {}

// String returns the explicit conversion kind and target precision/scale.
func (v Decimal128CastExpr) String() string {
	name := "CAST_DECIMAL128"
	if v.Tolerant {
		name = "TRY_CAST_DECIMAL128"
	}
	return fmt.Sprintf("%s(%s,%d,%d)", name, v.Value, v.Precision, v.Scale)
}

// CastDecimal128 converts exactly to decimal128(precision, scale). Supported
// inputs are native integers, strings, and Decimal128. Strings accept an optional
// sign and a decimal point with at least one ASCII digit; whitespace, exponents,
// separators and special floating values are rejected. Excess fractional digits
// must be zero. Overflow and inexact conversion fail on demanded non-NULL rows.
func CastDecimal128(value Expr, precision, scale int32) Decimal128CastExpr {
	return Decimal128CastExpr{Value: value, Precision: precision, Scale: scale}
}

// TryCastDecimal128 is CastDecimal128 with conversion failures replaced by NULL.
// It does not suppress invalid type metadata, child errors, storage failures,
// cancellation, or memory-admission errors.
func TryCastDecimal128(value Expr, precision, scale int32) Decimal128CastExpr {
	return Decimal128CastExpr{Value: value, Precision: precision, Scale: scale, Tolerant: true}
}

// Alias names the converted output.
func (v Decimal128CastExpr) Alias(name string) NamedExpr { return Named(v, name) }

// Cast converts the decimal result to an integer or string target.
func (v Decimal128CastExpr) Cast(target memory.DataType) Cast { return CastTo(v, target) }

// TryCast converts the decimal result to an integer, replacing conversion failures with NULL.
func (v Decimal128CastExpr) TryCast(target memory.DataType) TryCastExpr { return TryCastTo(v, target) }

const decimalCastSyntax decimalRescaleStatus = 3

// parseDecimal128Text keeps fixed-width coefficient scratch even for arbitrarily
// long input. It validates the entire token before inexactness, then precision.
// A context failure is separate from a conversion status and must never become NULL.
func parseDecimal128Text(text []byte, precision, scale int32, check func() error) (memory.Decimal128, decimalRescaleStatus, error) {
	negative := len(text) > 0 && text[0] == '-'
	if len(text) > 0 && (text[0] == '+' || text[0] == '-') {
		text = text[1:]
	}
	var value memory.Decimal128
	var digits, fractional, significant int
	point, inexact, syntax := false, false, false
	for i, c := range text {
		if i&4095 == 0 && check != nil {
			if err := check(); err != nil {
				return memory.Decimal128{}, decimalRescaleOK, err
			}
		}
		if c == '.' && !point {
			point = true
			continue
		}
		if c < '0' || c > '9' {
			syntax = true
			continue
		}
		digits++
		if point {
			fractional++
			if fractional > int(scale) {
				inexact = inexact || c != '0'
				continue
			}
		}
		if significant == 0 && c == '0' {
			continue
		}
		significant++
		if significant <= 38 {
			value = value.Mul(decimal128.FromI64(10)).Add(decimal128.FromI64(int64(c - '0')))
		}
	}
	if syntax || digits == 0 {
		return memory.Decimal128{}, decimalCastSyntax, nil
	}
	if inexact {
		return memory.Decimal128{}, decimalRescaleInexact, nil
	}
	if significant == 0 {
		return memory.Decimal128{}, decimalRescaleOK, nil
	}
	padding := max(0, int(scale)-fractional)
	if significant+padding > int(precision) {
		return memory.Decimal128{}, decimalRescaleOverflow, nil
	}
	if padding > 0 {
		value = value.Mul(decimal128.GetScaleMultiplier(padding))
	}
	if negative {
		value = value.Negate()
	}
	return value, decimalRescaleOK, nil
}

func decimalCastError(from, to memory.DataType, precision, scale int32, row int, status decimalRescaleStatus) error {
	reason := "overflows"
	if status == decimalRescaleInexact {
		reason = "loses fractional digits"
	}
	if status == decimalCastSyntax {
		reason = "has invalid decimal syntax"
	}
	if to == memory.TypeDecimal {
		return fmt.Errorf("cast %s to decimal128(%d,%d) %s at row %d", from, precision, scale, reason, row)
	}
	return fmt.Errorf("cast %s to %s %s at row %d", from, to, reason, row)
}

func evalDecimalCast(out, in *scalarRegister, from, to memory.DataType, rows int, tolerant bool) error {
	copyRegisterValidity(out, in, rows)
	for row := 0; row < rows; row++ {
		if row&255 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		var value memory.Decimal128
		status := decimalRescaleOK
		var si int64
		var ui uint64
		if isValidRegister(out, row) {
			if to == memory.TypeDecimal {
				p, s := out.decimalType.DecimalPrecision, out.decimalType.DecimalScale
				switch {
				case from == memory.TypeString:
					text, err := castStringBytes(in, row)
					if err != nil {
						return err
					}
					value, status, err = parseDecimal128Text(text, p, s, out.checkContext)
					if err != nil {
						return err
					}
				case from == memory.TypeDecimal:
					value, status = decimalRescaleValue(in.decimal[row], in.decimalType.DecimalScale, p, s)
				case isSignedInteger(from):
					n, err := registerInt64(in, row)
					if err != nil {
						return err
					}
					value, status = decimalRescaleValue(decimal128.FromI64(n), 0, p, s)
				case unsignedInteger(from):
					n, err := registerUint64(in, row)
					if err != nil {
						return err
					}
					value, status = decimalRescaleValue(decimal128.New(0, n), 0, p, s)
				default:
					return fmt.Errorf("unsupported decimal cast input %s", from)
				}
			} else {
				// Exactness takes precedence over the destination range check.
				value, status = decimalRescaleValue(in.decimal[row], in.decimalType.DecimalScale, 38, 0)
				if status == decimalRescaleOK {
					negative := value.Sign() < 0
					if negative {
						value = value.Negate()
					}
					limit := ^uint64(0) >> (64 - integerWidth(to))
					if isSignedInteger(to) {
						limit >>= 1
						if negative {
							limit++
						}
					}
					if value.HighBits() != 0 || value.LowBits() > limit || (negative && unsignedInteger(to)) {
						status = decimalRescaleOverflow
					} else {
						ui = value.LowBits()
						si = int64(ui)
						if negative {
							si = -si
						}
					}
				}
			}
			if status != decimalRescaleOK {
				if !tolerant {
					var p, s int32
					if to == memory.TypeDecimal {
						p, s = out.decimalType.DecimalPrecision, out.decimalType.DecimalScale
					}
					return decimalCastError(from, to, p, s, row, status)
				}
				invalidateCastRow(out, rows, row)
				value = memory.Decimal128{}
			}
		}
		switch to {
		case memory.TypeDecimal:
			out.decimal[row] = value
		case memory.TypeInt8:
			out.i8[row] = int8(si)
		case memory.TypeInt16:
			out.i16[row] = int16(si)
		case memory.TypeInt32:
			out.i32[row] = int32(si)
		case memory.TypeInt64:
			out.i64[row] = si
		case memory.TypeUint8:
			out.u8[row] = uint8(ui)
		case memory.TypeUint16:
			out.u16[row] = uint16(ui)
		case memory.TypeUint32:
			out.u32[row] = uint32(ui)
		case memory.TypeUint64:
			out.u64[row] = ui
		default:
			return fmt.Errorf("unsupported decimal cast target %s", to)
		}
	}
	return nil
}

// appendDecimal128Text formats the full declared scale, with no exponent,
// floating point, big integer, or heap scratch. Zero has no negative sign.
func appendDecimal128Text(dst []byte, value memory.Decimal128, scale int32) []byte {
	if value.Sign() < 0 {
		dst = append(dst, '-')
		value = value.Negate()
	}
	var digits [38]byte
	n := len(digits)
	hi, lo := uint64(value.HighBits()), value.LowBits()
	for {
		qhi, rem := hi/10, hi%10
		qlo, r := bits.Div64(rem, lo, 10)
		n--
		digits[n] = byte(r) + '0'
		hi, lo = qhi, qlo
		if hi == 0 && lo == 0 {
			break
		}
	}
	count := len(digits) - n
	if int(scale) >= count {
		dst = append(dst, '0')
		if scale > 0 {
			dst = append(dst, '.')
		}
		for i := count; i < int(scale); i++ {
			dst = append(dst, '0')
		}
		return append(dst, digits[n:]...)
	}
	integer := len(digits) - int(scale)
	dst = append(dst, digits[n:integer]...)
	if scale > 0 {
		dst = append(dst, '.')
		dst = append(dst, digits[integer:]...)
	}
	return dst
}
