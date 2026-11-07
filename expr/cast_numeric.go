package expr

import (
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"strconv"
	"unsafe"
)

var errNumericCast = errors.New("invalid numeric syntax or out-of-range value")

// evalNumericCast keeps parsing and conversion in native physical vectors. All
// buffers (including nullable output) have already been admitted by the scalar
// workspace. No intermediate widened vector or per-row register is allocated.
func evalNumericCast(out, in *scalarRegister, from, to memory.DataType, rows int, tolerant bool) error {
	copyRegisterValidity(out, in, rows)
	for row := 0; row < rows; row++ {
		if row&255 == 0 && out.checkContext != nil {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		var si int64
		var ui uint64
		var fv float64
		var err error
		if isValidRegister(out, row) {
			var text []byte
			if from == memory.TypeString {
				data, readErr := registerString(in, row)
				if readErr != nil {
					return readErr // Storage errors are not conversion errors.
				}
				text = data
			}
			switch {
			case isSignedInteger(to):
				if from == memory.TypeString {
					si, err = parseSignedDecimal(text, integerWidth(to))
				} else {
					si, err = registerInt64(in, row)
					bits := integerWidth(to)
					if bits < 64 && (si < -(int64(1)<<(bits-1)) || si > (int64(1)<<(bits-1))-1) {
						err = errNumericCast
					}
				}
			case unsignedInteger(to):
				if from == memory.TypeString {
					ui, err = parseUnsignedDecimal(text, integerWidth(to))
				} else {
					ui, err = registerUint64(in, row)
					bits := integerWidth(to)
					if bits < 64 && ui > (uint64(1)<<bits)-1 {
						err = errNumericCast
					}
				}
			default:
				if from == memory.TypeString {
					bits := 64
					if to == memory.TypeFloat32 {
						bits = 32
					}
					fv, err = parseDecimalFloat(text, bits)
				} else {
					fv, err = registerFloat64(in, row)
					// Match existing strict numeric casts, including infinity
					// rejection on a narrowing Float64 -> Float32 conversion.
					if to == memory.TypeFloat32 && from != memory.TypeFloat32 && (math.IsInf(fv, 0) || math.Abs(fv) > math.MaxFloat32) {
						err = errNumericCast
					}
				}
			}
			if err != nil {
				if !tolerant {
					return fmt.Errorf("cast %s to %s at row %d: %w", from, to, row, err)
				}
				if out.validity == nil {
					validity := ensureValidity(out, rows)
					for i := range validity {
						validity[i] = ^uint64(0)
					}
					if len(validity) > 0 {
						validity[len(validity)-1] &= memory.TailMask(rows)
					}
				}
				out.validity[row>>6] &^= uint64(1) << uint(row&63)
				si, ui, fv = 0, 0, 0
			}
		}
		switch to {
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
		case memory.TypeFloat32:
			out.f32[row] = float32(fv)
		case memory.TypeFloat64:
			out.f64[row] = fv
		default:
			return fmt.Errorf("unsupported numeric cast target %s", to)
		}
	}
	return nil
}

// Integer parsing avoids allocating strconv.NumError (which copies the token)
// for every rejected row in TryCast. Overflow is checked before multiplication.
func parseUnsignedDecimal(text []byte, bits int) (uint64, error) {
	if len(text) > 0 && text[0] == '+' {
		text = text[1:]
	}
	return parseDecimalMagnitude(text, ^uint64(0)>>(64-bits))
}

func parseSignedDecimal(text []byte, bits int) (int64, error) {
	negative := len(text) > 0 && text[0] == '-'
	if len(text) > 0 && (text[0] == '+' || text[0] == '-') {
		text = text[1:]
	}
	limit := (uint64(1) << (bits - 1)) - 1
	if negative {
		limit++
	}
	magnitude, err := parseDecimalMagnitude(text, limit)
	if negative {
		return -int64(magnitude), err
	}
	return int64(magnitude), err
}

func parseDecimalMagnitude(text []byte, limit uint64) (uint64, error) {
	if len(text) == 0 {
		return 0, errNumericCast
	}
	var value uint64
	for _, c := range text {
		if c < '0' || c > '9' {
			return 0, errNumericCast
		}
		digit := uint64(c - '0')
		if value > limit/10 || (value == limit/10 && digit > limit%10) {
			return 0, errNumericCast
		}
		value = value*10 + digit
	}
	return value, nil
}

// decimalFloatSyntax accepts decimal notation and case-insensitive IEEE special
// values, but not Go hexadecimal numbers, separators, or surrounding whitespace.
// Validate before strconv so invalid input never allocates a copied error token.
func decimalFloatSyntax(text []byte) bool {
	if len(text) == 0 {
		return false
	}
	if asciiEqualFold(text, "nan") || asciiEqualFold(text, "inf") || asciiEqualFold(text, "infinity") {
		return true
	}
	if text[0] == '+' || text[0] == '-' {
		text = text[1:]
		if asciiEqualFold(text, "nan") || asciiEqualFold(text, "inf") || asciiEqualFold(text, "infinity") {
			return true
		}
	}
	i, digits := 0, 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
		digits++
	}
	if i < len(text) && text[i] == '.' {
		i++
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if start == i {
			return false
		}
	}
	return i == len(text)
}

func asciiEqualFold(text []byte, lower string) bool {
	if len(text) != len(lower) {
		return false
	}
	for i, c := range text {
		if c|0x20 != lower[i] {
			return false
		}
	}
	return true
}

func parseDecimalFloat(text []byte, bits int) (float64, error) {
	if !decimalFloatSyntax(text) {
		return 0, errNumericCast
	}
	unsigned := text
	if len(unsigned) > 0 && (unsigned[0] == '+' || unsigned[0] == '-') {
		unsigned = unsigned[1:]
	}
	if asciiEqualFold(unsigned, "nan") {
		return math.NaN(), nil
	}
	// Check the exact round-to-nearest overflow boundary before strconv. It
	// reports overflow through NumError, which would copy the entire token even
	// though infinity is a valid result here.
	boundary := float64OverflowBoundary
	if bits == 32 {
		boundary = float32OverflowBoundary
	}
	if decimalAtLeastInteger(unsigned, boundary) {
		sign := 1
		if text[0] == '-' {
			sign = -1
		}
		return math.Inf(sign), nil
	}
	// The source is borrowed immutable input for the duration of evaluation.
	// ParseFloat consumes this string synchronously and does not retain it; any
	// NumError is discarded below. Avoid an unbudgeted copy of long valid input.
	var normalized [832]byte
	if len(text) > 768 {
		text = normalizeLongDecimal(text, normalized[:0])
	}
	view := unsafe.String(unsafe.SliceData(text), len(text))
	value, err := strconv.ParseFloat(view, bits)
	if errors.Is(err, strconv.ErrRange) && math.IsInf(value, 0) {
		return value, nil
	}
	if err != nil {
		return 0, errNumericCast
	}
	return value, nil
}

// IEEE round-to-nearest overflow boundaries: 2^128 - 2^103 and 2^1024 -
// 2^970 respectively. A magnitude at or above this exact integer rounds to Inf.
const float32OverflowBoundary = "340282356779733661637539395458142568448"
const float64OverflowBoundary = "179769313486231580793728971405303415079934132710037826936173778980444968292764750946649017977587207096330286416692887910946555547851940402630657488671505820681908902000708383676273854845817711531764475730270069855571366959622842914819860834936475292719074168444365510704342711559699508093042880177904174497792"

// decimalAtLeastInteger compares a validated, unsigned decimal token against
// a positive integer without materializing its digits or exponent. Exponents
// saturate well beyond the largest possible token's digit count.
func decimalAtLeastInteger(text []byte, boundary string) bool {
	mantissaEnd := len(text)
	decimalPoint := -1
	firstNonzero := -1
	digitsBefore := 0
	digitsSeen := 0
	for i, c := range text {
		if c == 'e' || c == 'E' {
			mantissaEnd = i
			break
		}
		if c == '.' {
			decimalPoint = i
			digitsBefore = digitsSeen
			continue
		}
		if c < '0' || c > '9' {
			return false
		} // Special NaN/Inf spellings.
		if firstNonzero < 0 && c != '0' {
			firstNonzero = digitsSeen
		}
		digitsSeen++
	}
	if firstNonzero < 0 {
		return false
	}
	if decimalPoint < 0 {
		digitsBefore = digitsSeen
	}
	var exponent int64
	if mantissaEnd < len(text) {
		exp := text[mantissaEnd+1:]
		negative := len(exp) > 0 && exp[0] == '-'
		if len(exp) > 0 && (exp[0] == '+' || exp[0] == '-') {
			exp = exp[1:]
		}
		limit := int64(len(text)) + 1024
		for _, c := range exp {
			if exponent > limit/10 {
				exponent = limit
				break
			}
			exponent = exponent*10 + int64(c-'0')
			if exponent > limit {
				exponent = limit
				break
			}
		}
		if negative {
			exponent = -exponent
		}
	}
	integerDigits := int64(digitsBefore-firstNonzero) + exponent
	if integerDigits != int64(len(boundary)) {
		return integerDigits > int64(len(boundary))
	}
	started, index := false, 0
	for _, c := range text[:mantissaEnd] {
		if c == '.' {
			continue
		}
		if !started && c == '0' {
			continue
		}
		started = true
		if index == len(boundary) {
			return true
		} // Equal prefix is already the boundary.
		if c != boundary[index] {
			return c > boundary[index]
		}
		index++
	}
	for ; index < len(boundary); index++ {
		if boundary[index] != '0' {
			return false
		}
	}
	return true
}

// Long mantissas are normalized before strconv's fixed-decimal slow path.
// 790 significant digits exceed the exact significant length of every binary32
// and binary64 rounding midpoint (at most 768 decimal digits). A nonzero sticky
// digit therefore preserves which side of a midpoint the original value lies on.
// This also avoids the Go 1.27 Float32 slow-path digit-count bug for long decimal
// coefficients followed by compensating negative exponents.
func normalizeLongDecimal(text, dst []byte) []byte {
	if text[0] == '-' {
		dst = append(dst, '-')
		text = text[1:]
	} else if text[0] == '+' {
		text = text[1:]
	}
	start := len(dst)
	digitsSeen, digitsBefore, firstNonzero := 0, -1, -1
	mantissaEnd := len(text)
	sticky := false
	for i, c := range text {
		if c == 'e' || c == 'E' {
			mantissaEnd = i
			break
		}
		if c == '.' {
			digitsBefore = digitsSeen
			continue
		}
		if firstNonzero < 0 && c != '0' {
			firstNonzero = digitsSeen
		}
		digitsSeen++
		if firstNonzero >= 0 {
			if len(dst)-start < 790 {
				dst = append(dst, c)
			} else if c != '0' {
				sticky = true
			}
		}
	}
	if firstNonzero < 0 {
		return append(dst, '0')
	}
	if digitsBefore < 0 {
		digitsBefore = digitsSeen
	}
	if sticky {
		dst = append(dst, '1')
	}
	var exponent int64
	if mantissaEnd < len(text) {
		exp := text[mantissaEnd+1:]
		negative := exp[0] == '-'
		if exp[0] == '+' || exp[0] == '-' {
			exp = exp[1:]
		}
		limit := int64(len(text)) + 1024
		for _, c := range exp {
			if exponent > limit/10 {
				exponent = limit
				break
			}
			exponent = exponent*10 + int64(c-'0')
			if exponent > limit {
				exponent = limit
				break
			}
		}
		if negative {
			exponent = -exponent
		}
	}
	exponent += int64(digitsBefore - firstNonzero - (len(dst) - start))
	dst = append(dst, 'e')
	return strconv.AppendInt(dst, exponent, 10)
}
