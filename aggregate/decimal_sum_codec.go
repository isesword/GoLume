package aggregate

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// DecimalSumStateEncodedBytes is the fixed little-endian partial-state size:
// four two's-complement limbs followed by the non-NULL value count.
const DecimalSumStateEncodedBytes = 40

// ErrDecimalSumStateEncoding identifies invalid state, precision or wire shape.
var ErrDecimalSumStateEncoding = errors.New("invalid decimal SUM partial state encoding")

// validDecimalPartial checks the stronger input-precision invariant. Scale is
// deliberately external: a containing protocol must bind it before any Merge.
func validDecimalPartial(s DecimalSumState, precision int32) bool {
	if precision < 1 || precision > 38 || s.count > math.MaxInt64 {
		return false
	}
	// 10^precision-1 fits unsigned128. Build without heap allocation.
	var max [4]uint64
	max[0] = 1
	for i := int32(0); i < precision; i++ {
		carry := uint64(0)
		for j := 0; j < 4; j++ {
			hi, lo := bits.Mul64(max[j], 10)
			lo, c := bits.Add64(lo, carry, 0)
			max[j] = lo
			carry = hi + c
		}
	}
	var borrow uint64
	max[0], borrow = bits.Sub64(max[0], 1, 0)
	for j := 1; j < 4; j++ {
		max[j], borrow = bits.Sub64(max[j], 0, borrow)
	}
	// count <= MaxInt64 gives a bound strictly below 2^190.
	var bound [4]uint64
	carry := uint64(0)
	for j := 0; j < 4; j++ {
		hi, lo := bits.Mul64(max[j], s.count)
		lo, c := bits.Add64(lo, carry, 0)
		bound[j] = lo
		carry = hi + c
	}
	magnitude := s.limbs
	if int64(magnitude[3]) < 0 {
		carry = 1
		for j := 0; j < 4; j++ {
			magnitude[j], carry = bits.Add64(^magnitude[j], 0, carry)
		}
	}
	for j := 3; j >= 0; j-- {
		if magnitude[j] != bound[j] {
			return magnitude[j] < bound[j]
		}
	}
	return true
}

// EncodeChecked writes only on success. It preserves the complete wide partial
// sum, including values outside Decimal128; it never performs finalization.
func (s DecimalSumState) EncodeChecked(dst []byte, inputPrecision int32) error {
	if len(dst) != DecimalSumStateEncodedBytes || !validDecimalPartial(s, inputPrecision) {
		return ErrDecimalSumStateEncoding
	}
	for i, v := range s.limbs {
		binary.LittleEndian.PutUint64(dst[i*8:i*8+8], v)
	}
	binary.LittleEndian.PutUint64(dst[32:40], s.count)
	return nil
}

// DecodeChecked commits only a validated complete state. The destination is
// unchanged on all errors, including an invalid wire length or precision.
func (s *DecimalSumState) DecodeChecked(src []byte, inputPrecision int32) error {
	if s == nil || len(src) != DecimalSumStateEncodedBytes {
		return ErrDecimalSumStateEncoding
	}
	var candidate DecimalSumState
	for i := range candidate.limbs {
		candidate.limbs[i] = binary.LittleEndian.Uint64(src[i*8 : i*8+8])
	}
	candidate.count = binary.LittleEndian.Uint64(src[32:40])
	if !validDecimalPartial(candidate, inputPrecision) {
		return ErrDecimalSumStateEncoding
	}
	*s = candidate
	return nil
}
