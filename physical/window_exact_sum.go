package physical

import (
	"gocolumn/memory"
	"math"
	"math/bits"
)

// A finite float64 is an integer multiple of 2^-1074. Its largest set bit in
// those units is 2097. At most MaxInt64 input rows add fewer than 63 more bits,
// so 34 words (2176 magnitude bits) hold every legal window total.
// The sign/magnitude representation makes adding and removing a value exact,
// even when a previous result rounded to infinity. It never allocates.
const windowExactWords = 34

// Charge the fixed accumulator before allocating it. This includes the three
// special-value counters and allocator rounding, on 32-bit and 64-bit targets.
const windowExactSumBytes = int64(320)

type windowExactSum struct {
	words                   [windowExactWords]uint64
	high                    int // Number of significant words; all words above it are zero.
	negativeFinite          bool
	nan, positive, negative int64
}

func usesExactWindowSum(function WindowFunction, input memory.DataType) bool {
	return function == WindowAvg || function == WindowSum && windowFloat(input)
}

func (s *windowExactSum) addFloat(value float64, direction int64) {
	encoded := math.Float64bits(value)
	exponent := int(encoded >> 52 & 0x7ff)
	mantissa := encoded & (uint64(1)<<52 - 1)
	if exponent == 0x7ff {
		if mantissa != 0 {
			s.nan += direction
		} else if encoded>>63 == 0 {
			s.positive += direction
		} else {
			s.negative += direction
		}
		return
	}
	shift := 0
	if exponent != 0 {
		mantissa |= uint64(1) << 52
		shift = exponent - 1
	}
	s.addMantissa(mantissa, shift, (encoded>>63 != 0) != (direction < 0))
}

func (s *windowExactSum) addSigned(value int64, direction int64) {
	magnitude := uint64(value)
	if value < 0 {
		magnitude = -magnitude // Also handles MinInt64, without signed overflow.
	}
	s.addMantissa(magnitude, 1074, (value < 0) != (direction < 0))
}

func (s *windowExactSum) addUnsigned(value uint64, direction int64) {
	s.addMantissa(value, 1074, direction < 0)
}

func (s *windowExactSum) addMantissa(value uint64, shift int, negative bool) {
	if value == 0 {
		return
	}
	word, offset := shift/64, uint(shift%64)
	low := value << offset
	high := uint64(0)
	if offset != 0 {
		high = value >> (64 - offset)
	}
	end := word + 1
	if high != 0 {
		end++
	}
	if s.high == 0 {
		s.words[word], s.words[word+1] = low, high
		s.high, s.negativeFinite = end, negative
		return
	}
	if s.negativeFinite == negative {
		var carry uint64
		s.words[word], carry = bits.Add64(s.words[word], low, 0)
		s.words[word+1], carry = bits.Add64(s.words[word+1], high, carry)
		i := word + 2
		for ; carry != 0; i++ {
			s.words[i], carry = bits.Add64(s.words[i], 0, carry)
		}
		s.high = max(s.high, i)
	} else if s.atLeast(word, low, high, end) {
		var borrow uint64
		s.words[word], borrow = bits.Sub64(s.words[word], low, 0)
		s.words[word+1], borrow = bits.Sub64(s.words[word+1], high, borrow)
		for i := word + 2; borrow != 0; i++ {
			s.words[i], borrow = bits.Sub64(s.words[i], 0, borrow)
		}
	} else {
		// The incoming magnitude is larger, so subtraction changes the sign.
		var borrow uint64
		for i := 0; i < end; i++ {
			part := uint64(0)
			if i == word {
				part = low
			} else if i == word+1 {
				part = high
			}
			s.words[i], borrow = bits.Sub64(part, s.words[i], borrow)
		}
		s.high, s.negativeFinite = end, negative
	}
	for s.high > 0 && s.words[s.high-1] == 0 {
		s.high--
	}
	if s.high == 0 {
		s.negativeFinite = false
	}
}

// atLeast compares the retained magnitude with one shifted mantissa. Equal
// leading words suffice: every retained lower word is nonnegative.
func (s *windowExactSum) atLeast(word int, low, high uint64, end int) bool {
	if s.high != end {
		return s.high > end
	}
	if high != 0 && s.words[word+1] != high {
		return s.words[word+1] > high
	}
	return s.words[word] >= low
}

// shiftedWord returns the low 64 bits of the exact magnitude shifted right.
func (s *windowExactSum) shiftedWord(shift int) uint64 {
	word, offset := shift/64, uint(shift%64)
	if word >= s.high {
		return 0
	}
	value := s.words[word] >> offset
	if offset != 0 && word+1 < s.high {
		value |= s.words[word+1] << (64 - offset)
	}
	return value
}

func (s *windowExactSum) anyBelow(bit int) bool {
	word, offset := bit/64, uint(bit%64)
	if s.words[word]&(uint64(1)<<offset-1) != 0 {
		return true
	}
	for i := 0; i < word; i++ {
		if s.words[i] != 0 {
			return true
		}
	}
	return false
}

// result divides the exact total by a positive count before rounding once to
// float64, nearest with ties to even. Sum supplies one. In particular, Avg does
// not round or overflow the sum before division, or round integer inputs first.
// Exact zero is +0, including all-negative-zero frames; a negative nonzero
// average that underflows is -0. NaN payloads are not part of the contract.
func (s *windowExactSum) result(count int64) float64 {
	if s.nan != 0 || s.positive != 0 && s.negative != 0 {
		return math.NaN()
	}
	if s.positive != 0 {
		return math.Inf(1)
	}
	if s.negative != 0 {
		return math.Inf(-1)
	}
	if s.high == 0 {
		return 0
	}
	sign := uint64(0)
	if s.negativeFinite {
		sign = uint64(1) << 63
	}
	divisor := uint64(count)
	numeratorBits := (s.high-1)*64 + bits.Len64(s.words[s.high-1])
	exponent := numeratorBits - bits.Len64(divisor)
	if exponent >= 0 && s.shiftedWord(exponent) < divisor {
		exponent--
	}
	shift := max(0, exponent-52)
	// Only the leading 53 quotient bits are needed. Divide at most 116 input
	// bits, then retain the omitted low words as an exact fractional remainder.
	// This avoids a full 34-word division on every output without rounding early.
	low, high := s.shiftedWord(shift), s.shiftedWord(shift+64)
	significand, remainder := low, uint64(0)
	if count != 1 {
		significand, remainder = bits.Div64(high, low, divisor)
	}
	twice := remainder * 2 // count <= MaxInt64, so this cannot overflow.
	roundUp := twice > divisor
	if twice == divisor {
		roundUp = significand&1 != 0 || s.anyBelow(shift)
	} else if twice < divisor && divisor-twice == 1 && shift > 0 {
		// Only this case can straddle half an ulp because of omitted bits.
		half := shift - 1
		if s.words[half/64]&(uint64(1)<<uint(half%64)) != 0 {
			roundUp = significand&1 != 0 || s.anyBelow(half)
		}
	}
	if roundUp {
		significand++
	}
	if significand < uint64(1)<<52 {
		return math.Float64frombits(sign | significand)
	}
	if significand == uint64(1)<<53 {
		significand >>= 1
		shift++
	}
	encodedExponent := uint64(shift + 1)
	if encodedExponent >= 0x7ff {
		return math.Float64frombits(sign | uint64(0x7ff)<<52)
	}
	return math.Float64frombits(sign | encodedExponent<<52 | significand&(uint64(1)<<52-1))
}
