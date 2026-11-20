package aggregate

import (
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
	"math"
	"math/bits"
)

var ErrDecimalSumCount = errors.New("decimal SUM valid-value count exceeds MaxInt64")
var ErrDecimalSumOverflow = errors.New("decimal SUM final coefficient exceeds precision 38")

// DecimalSumState is a fixed, allocation-free exact coefficient sum. Limbs are
// little endian two's-complement. Count <= MaxInt64 bounds its magnitude below
// 2^190 for legal Decimal128 inputs; intermediate sums need not fit Decimal128.
// Owners must preserve the state invariant and supply validated coefficients.
// Scale is external immutable lane metadata: Add/Merge require coefficients
// in the same unit. This state does not rescale or validate cross-lane metadata.
type DecimalSumState struct {
	limbs [4]uint64
	count uint64
}

func (s DecimalSumState) Count() uint64 { return s.count }
func (s *DecimalSumState) Add(v memory.Decimal128) error {
	if s.count >= math.MaxInt64 {
		return ErrDecimalSumCount
	}
	ext := uint64(0)
	if v.HighBits() < 0 {
		ext = math.MaxUint64
	}
	a := [4]uint64{v.LowBits(), uint64(v.HighBits()), ext, ext}
	c := uint64(0)
	for i := range a {
		s.limbs[i], c = bits.Add64(s.limbs[i], a[i], c)
	}
	s.count++
	return nil
}
func (s *DecimalSumState) Merge(other DecimalSumState) error {
	if other.count > math.MaxInt64 || s.count > math.MaxInt64-other.count {
		return ErrDecimalSumCount
	}
	c := uint64(0)
	for i := range s.limbs {
		s.limbs[i], c = bits.Add64(s.limbs[i], other.limbs[i], c)
	}
	s.count += other.count
	return nil
}
func (s DecimalSumState) Finalize() (memory.Decimal128, bool, error) {
	if s.count == 0 {
		return memory.Decimal128{}, false, nil
	}
	ext := uint64(0)
	if int64(s.limbs[1]) < 0 {
		ext = math.MaxUint64
	}
	if s.limbs[2] != ext || s.limbs[3] != ext {
		return memory.Decimal128{}, false, ErrDecimalSumOverflow
	}
	v := decimal128.New(int64(s.limbs[1]), s.limbs[0])
	if !v.FitsInPrecision(38) {
		return memory.Decimal128{}, false, ErrDecimalSumOverflow
	}
	return v, true, nil
}
