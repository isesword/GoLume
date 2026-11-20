package aggregate

import (
	"errors"
	"math"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

// DecimalAverageRoundingMode is an explicit final AVG rounding policy. Its
// vocabulary matches expression rounding, but this lower-level type is separate:
// aggregate must not depend on expr. Zero and unknown values are invalid.
type DecimalAverageRoundingMode uint8

const (
	// DecimalAverageTowardZero discards the fractional part toward zero.
	DecimalAverageTowardZero DecimalAverageRoundingMode = iota + 1
	// DecimalAverageAwayFromZero increases magnitude for any nonzero remainder.
	DecimalAverageAwayFromZero
	// DecimalAverageFloor rounds toward negative infinity.
	DecimalAverageFloor
	// DecimalAverageCeil rounds toward positive infinity.
	DecimalAverageCeil
	// DecimalAverageHalfEven rounds to nearest, with ties to an even coefficient.
	DecimalAverageHalfEven
	// DecimalAverageHalfAwayFromZero rounds to nearest, with ties away from zero.
	DecimalAverageHalfAwayFromZero
)

// DecimalAverageOptions describes the input coefficient unit and the explicit
// final result metadata. InputScale is 0..38; Precision is 1..38; Scale is
// 0..Precision. Mode has no implicit default. InputScale must be the immutable
// common scale used by every Add/Merge contributing to this state. The state
// itself does not retain or verify input metadata or rescale merged inputs.
type DecimalAverageOptions struct {
	InputScale int32
	Precision  int32
	Scale      int32
	Mode       DecimalAverageRoundingMode
}

var ErrDecimalAverageOptions = errors.New("decimal AVG requires valid explicit input scale, output precision/scale and rounding mode")
var ErrDecimalAverageOverflow = errors.New("decimal AVG rounded final coefficient exceeds output precision")

// FinalizeAverage computes the exact coefficient sum divided by its valid-value
// count, rescaled to the requested output scale and rounded exactly once. NULL
// inputs must have been skipped by the owner; zero count returns NULL. Options
// are validated even for empty and exact-zero states. Errors and success leave
// the original state unchanged, permitting repeated finalization with different
// output options. No intermediate SUM narrowing, floating point or heap-backed
// arithmetic is used, and this method never changes the existing SUM contract.
//
// As for Add/Merge, owners must preserve the state invariant: validated legal
// Decimal128 coefficients in one unit, and count <= MaxInt64. The sum magnitude
// is then < 2^190. Scaling it by at most 10^38 stays below 2^317; the scaled count
// stays below 2^190. Fixed 384-bit scratch suffices. Work is bounded by 38 scaling
// steps and 384 quotient bits, with no context/cancellation interface. Query
// owners remain responsible for cancellation checks between finalizations.
func (s DecimalSumState) FinalizeAverage(o DecimalAverageOptions) (memory.Decimal128, bool, error) {
	if o.InputScale < 0 || o.InputScale > 38 || o.Precision < 1 || o.Precision > 38 || o.Scale < 0 || o.Scale > o.Precision || o.Mode < DecimalAverageTowardZero || o.Mode > DecimalAverageHalfAwayFromZero {
		return memory.Decimal128{}, false, ErrDecimalAverageOptions
	}
	if s.count > math.MaxInt64 {
		return memory.Decimal128{}, false, ErrDecimalSumCount
	}
	if s.count == 0 {
		return memory.Decimal128{}, false, nil
	}
	negative := int64(s.limbs[3]) < 0
	numerator := decimalAverageMagnitude{s.limbs[0], s.limbs[1], s.limbs[2], s.limbs[3]}
	if negative {
		// Negate within the original 256-bit width, then zero-extend the magnitude.
		carry := uint64(1)
		for i := 0; i < 4; i++ {
			numerator[i], carry = bits.Add64(^numerator[i], 0, carry)
		}
	}
	denominator := decimalAverageMagnitude{s.count}
	if delta := o.Scale - o.InputScale; delta >= 0 {
		numerator.scaleUp(delta)
	} else {
		denominator.scaleUp(-delta)
	}
	quotient, remainder := decimalAverageQuoRem(numerator, denominator)
	increment := false
	if remainder != (decimalAverageMagnitude{}) {
		switch o.Mode {
		case DecimalAverageAwayFromZero:
			increment = true
		case DecimalAverageFloor:
			increment = negative
		case DecimalAverageCeil:
			increment = !negative
		case DecimalAverageHalfEven, DecimalAverageHalfAwayFromZero:
			// Twice a remainder is still < 2^191 under the denominator bound.
			remainder.shiftOne(0)
			comparison := remainder.compare(denominator)
			increment = comparison > 0 || comparison == 0 && (o.Mode == DecimalAverageHalfAwayFromZero || quotient[0]&1 != 0)
		}
	}
	if increment {
		carry := uint64(1)
		for i := range quotient {
			quotient[i], carry = bits.Add64(quotient[i], 0, carry)
		}
	}
	limit := decimalAverageMagnitude{1}
	limit.scaleUp(o.Precision)
	if quotient.compare(limit) >= 0 {
		return memory.Decimal128{}, false, ErrDecimalAverageOverflow
	}
	value := decimal128.New(int64(quotient[1]), quotient[0])
	if negative {
		value = value.Negate()
	}
	return value, true, nil
}

// decimalAverageMagnitude is private fixed-size stack scratch, not persistent
// per-row state. It deliberately does not couple SUM to expression internals.
type decimalAverageMagnitude [6]uint64

func (v *decimalAverageMagnitude) scaleUp(digits int32) {
	for ; digits > 0; digits-- {
		var carry uint64
		for i := range v {
			hi, lo := bits.Mul64(v[i], 10)
			var c uint64
			v[i], c = bits.Add64(lo, carry, 0)
			carry = hi + c
		}
		// The validated metadata and legal-state bounds prove final carry is zero.
	}
}
func (v decimalAverageMagnitude) compare(other decimalAverageMagnitude) int {
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
func (v *decimalAverageMagnitude) shiftOne(low uint64) {
	for i := range v {
		next := v[i] >> 63
		v[i] = v[i]<<1 | low
		low = next
	}
}
func decimalAverageQuoRem(n, d decimalAverageMagnitude) (q, r decimalAverageMagnitude) {
	// Single-word counts, including MaxInt64, avoid the bitwise long division.
	if d[1]|d[2]|d[3]|d[4]|d[5] == 0 {
		var rem uint64
		for i := len(n) - 1; i >= 0; i-- {
			q[i], rem = bits.Div64(rem, n[i], d[0])
		}
		r[0] = rem
		return
	}
	// Binary long division has bounded work and never allocates. The remainder
	// is < d before each step, so its doubled value fits the proven width.
	for bit := 383; bit >= 0; bit-- {
		r.shiftOne((n[bit/64] >> uint(bit%64)) & 1)
		if r.compare(d) >= 0 {
			var borrow uint64
			for i := range r {
				r[i], borrow = bits.Sub64(r[i], d[i], borrow)
			}
			q[bit/64] |= uint64(1) << uint(bit%64)
		}
	}
	return
}
