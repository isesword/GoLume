package physical

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/bits"
	"unsafe"
)

// A finite value is an integer x in units 2^-1074, |x|<2^2098.
// With n<=MaxInt64, S needs 2161 bits, Q needs 4259, and nQ/S²
// each need at most 4322. All multiplication/division scratch is owned here.
type windowExactMoments struct {
	sum       windowExactSum
	squares   [67]uint64
	numerator [68]uint64
	product   [68]uint64
	count     int64
}

// Includes fixed scratch and Go allocator size-class/page rounding slack.
const windowExactMomentsBytes = int64(unsafe.Sizeof(windowExactMoments{})) + (int64(unsafe.Sizeof(windowExactMoments{}))+3)/4 + 64

func (m *windowExactMoments) addCell(typ memory.DataType, cell windowNumericCell, direction int64) error {
	if direction != 1 && direction != -1 {
		return fmt.Errorf("variance direction must be +1 or -1")
	}
	if !cell.valid {
		return nil
	}
	if direction > 0 && m.count == math.MaxInt64 || direction < 0 && m.count == 0 {
		return fmt.Errorf("variance count overflow or underflow")
	}
	var magnitude uint64
	shift := 1074
	switch {
	case windowSigned(typ):
		magnitude = uint64(cell.i64)
		if cell.i64 < 0 {
			magnitude = -magnitude
		}
	case windowUnsigned(typ):
		magnitude = cell.u64
	case windowFloat(typ):
		encoded := math.Float64bits(cell.f64)
		exp := int(encoded >> 52 & 0x7ff)
		magnitude = encoded & ((uint64(1) << 52) - 1)
		if exp == 0x7ff {
			if direction < 0 {
				counter := m.sum.nan
				if magnitude == 0 {
					counter = m.sum.positive
					if encoded>>63 != 0 {
						counter = m.sum.negative
					}
				}
				if counter == 0 {
					return fmt.Errorf("variance special counter underflow")
				}
			}
			m.sum.addFloat(cell.f64, direction)
			m.count += direction
			return nil
		}
		shift = 0
		if exp != 0 {
			magnitude |= uint64(1) << 52
			shift = exp - 1
		}
	default:
		return fmt.Errorf("variance unsupported input type %s", typ)
	}
	hi, lo := bits.Mul64(magnitude, magnitude)
	word, offset := (2*shift)/64, uint((2*shift)%64)
	var carry uint64
	for i := word; i < len(m.squares); i++ {
		part := uint64(0)
		j := i - word
		if offset == 0 {
			if j == 0 {
				part = lo
			} else if j == 1 {
				part = hi
			}
		} else {
			switch j {
			case 0:
				part = lo << offset
			case 1:
				part = hi<<offset | lo>>(64-offset)
			case 2:
				part = hi >> (64 - offset)
			}
		}
		if direction > 0 {
			m.squares[i], carry = bits.Add64(m.squares[i], part, carry)
		} else {
			m.squares[i], carry = bits.Sub64(m.squares[i], part, carry)
		}
		if j >= 2 && carry == 0 {
			break
		}
	}
	if carry != 0 {
		return fmt.Errorf("variance square sum overflow or underflow")
	}
	addExactWindowCell(&m.sum, typ, cell, direction)
	m.count += direction
	return nil
}

// momentsAddProduct adds a 128-bit product without losing carry at either limb.
func momentsAddProduct(dst *[68]uint64, index int, hi, lo uint64) error {
	var carry uint64
	dst[index], carry = bits.Add64(dst[index], lo, 0)
	dst[index+1], carry = bits.Add64(dst[index+1], hi, carry)
	for i := index + 2; carry != 0; i++ {
		if i >= len(dst) {
			return fmt.Errorf("variance square product overflow")
		}
		dst[i], carry = bits.Add64(dst[i], 0, carry)
	}
	return nil
}
func momentsBitLen(a *[68]uint64) int {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] != 0 {
			return i*64 + bits.Len64(a[i])
		}
	}
	return 0
}
func momentsShiftedWord(a *[68]uint64, index, shift int) uint64 {
	word, off := shift/64, uint(shift%64)
	j := index - word
	var v uint64
	if j >= 0 && j < len(a) {
		v = a[j] << off
	}
	if off != 0 && j-1 >= 0 && j-1 < len(a) {
		v |= a[j-1] >> (64 - off)
	}
	return v
}
func momentsDenWord(lo, hi uint64, index, shift int) uint64 {
	word, off := shift/64, uint(shift%64)
	switch index - word {
	case 0:
		return lo << off
	case 1:
		if off == 0 {
			return hi
		}
		return hi<<off | lo>>(64-off)
	case 2:
		if off != 0 {
			return hi >> (64 - off)
		}
	}
	return 0
}
func momentsCompare(a *[68]uint64, aShift int, lo, hi uint64, dShift int) int {
	for i := 67; i >= 0; i-- {
		x, y := momentsShiftedWord(a, i, aShift), momentsDenWord(lo, hi, i, dShift)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
	}
	return 0
}
func momentsSubtract(a *[68]uint64, lo, hi uint64, shift int) error {
	var borrow uint64
	for i := range a {
		a[i], borrow = bits.Sub64(a[i], momentsDenWord(lo, hi, i, shift), borrow)
	}
	if borrow != 0 {
		return fmt.Errorf("variance division underflow")
	}
	return nil
}

func (m *windowExactMoments) result(sample bool) (float64, bool, error) {
	minimum := int64(1)
	if sample {
		minimum = 2
	}
	if m.count < minimum {
		return 0, false, nil
	}
	if m.sum.nan != 0 || m.sum.positive != 0 || m.sum.negative != 0 {
		return math.NaN(), true, nil
	}
	if err := m.prepareNumerator(); err != nil {
		return 0, false, err
	}
	n := uint64(m.count)
	length := momentsBitLen(&m.numerator)
	if length == 0 {
		return 0, true, nil
	}
	other := n
	if sample {
		other--
	}
	dhi, dlo := bits.Mul64(n, other)
	dbits := bits.Len64(dlo)
	if dhi != 0 {
		dbits = 64 + bits.Len64(dhi)
	}
	k := length - dbits
	cmp := 0
	if k >= 0 {
		cmp = momentsCompare(&m.numerator, 0, dlo, dhi, k)
	} else {
		cmp = momentsCompare(&m.numerator, -k, dlo, dhi, 0)
	}
	if cmp < 0 {
		k--
	}
	exponent := k - 2148
	if exponent >= 1024 {
		return math.Inf(1), true, nil
	}
	if exponent < -1075 {
		return 0, true, nil
	}
	shift := max(exponent-52, -1074) + 2148
	var quotient uint64
	for bit := 52; bit >= 0; bit-- {
		if momentsCompare(&m.numerator, 0, dlo, dhi, shift+bit) >= 0 {
			if err := momentsSubtract(&m.numerator, dlo, dhi, shift+bit); err != nil {
				return 0, false, err
			}
			quotient |= uint64(1) << uint(bit)
		}
	}
	cmp = momentsCompare(&m.numerator, 1, dlo, dhi, shift)
	if cmp > 0 || cmp == 0 && quotient&1 != 0 {
		quotient++
	}
	if exponent < -1022 {
		return math.Float64frombits(quotient), true, nil
	}
	if quotient == uint64(1)<<53 {
		quotient >>= 1
		exponent++
	}
	if exponent >= 1024 {
		return math.Inf(1), true, nil
	}
	return math.Float64frombits(uint64(exponent+1023)<<52 | (quotient - uint64(1)<<52)), true, nil
}

// prepareNumerator reconstructs exact nQ-S² without changing retained moments.
func (m *windowExactMoments) prepareNumerator() error {
	clear(m.numerator[:])
	clear(m.product[:])
	n := uint64(m.count)
	var carry uint64
	for i, v := range m.squares {
		hi, lo := bits.Mul64(v, n)
		lo, c := bits.Add64(lo, carry, 0)
		hi, c2 := bits.Add64(hi, 0, c)
		if c2 != 0 {
			return fmt.Errorf("variance nQ overflow")
		}
		m.numerator[i] = lo
		carry = hi
	}
	m.numerator[67] = carry
	// Skip zero low/high limbs, important for ordinary narrow exponent ranges.
	low := 0
	for low < m.sum.high && m.sum.words[low] == 0 {
		low++
	}
	for i := low; i < m.sum.high; i++ {
		for j := low; j < m.sum.high; j++ {
			hi, lo := bits.Mul64(m.sum.words[i], m.sum.words[j])
			if err := momentsAddProduct(&m.product, i+j, hi, lo); err != nil {
				return err
			}
		}
	}
	var borrow uint64
	for i := range m.numerator {
		m.numerator[i], borrow = bits.Sub64(m.numerator[i], m.product[i], borrow)
	}
	if borrow != 0 {
		return fmt.Errorf("variance negative exact numerator")
	}

	return nil
}

// rootProduct forms candidate²*D in the already charged scratch. candidate has
// at most 54 bits (including midpoint trials), so its product needs 234 bits.
func (m *windowExactMoments) rootProduct(candidate, dlo, dhi uint64) error {
	clear(m.product[:])
	hi, lo := bits.Mul64(candidate, candidate)
	a, b := bits.Mul64(lo, dlo)
	if err := momentsAddProduct(&m.product, 0, a, b); err != nil {
		return err
	}
	a, b = bits.Mul64(lo, dhi)
	if err := momentsAddProduct(&m.product, 1, a, b); err != nil {
		return err
	}
	a, b = bits.Mul64(hi, dlo)
	if err := momentsAddProduct(&m.product, 1, a, b); err != nil {
		return err
	}
	a, b = bits.Mul64(hi, dhi)
	return momentsAddProduct(&m.product, 2, a, b)
}
func (m *windowExactMoments) compareRootProduct(aShift, productShift int) int {
	for i := 67; i >= 0; i-- {
		a, b := momentsShiftedWord(&m.numerator, i, aShift), momentsShiftedWord(&m.product, i, productShift)
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
	}
	return 0
}

// stddev rounds sqrt(A/D)*2^-1074 directly. It never takes the square root of
// the rounded variance: that intermediate may already be zero or infinity.
func (m *windowExactMoments) stddev(sample bool) (float64, bool, error) {
	minimum := int64(1)
	if sample {
		minimum = 2
	}
	if m.count < minimum {
		return 0, false, nil
	}
	if m.sum.nan != 0 || m.sum.positive != 0 || m.sum.negative != 0 {
		return math.NaN(), true, nil
	}
	if err := m.prepareNumerator(); err != nil {
		return 0, false, err
	}
	length := momentsBitLen(&m.numerator)
	if length == 0 {
		return 0, true, nil
	}
	n := uint64(m.count)
	other := n
	if sample {
		other--
	}
	dhi, dlo := bits.Mul64(n, other)
	dbits := bits.Len64(dlo)
	if dhi != 0 {
		dbits = 64 + bits.Len64(dhi)
	}
	k := length - dbits
	cmp := 0
	if k >= 0 {
		cmp = momentsCompare(&m.numerator, 0, dlo, dhi, k)
	} else {
		cmp = momentsCompare(&m.numerator, -k, dlo, dhi, 0)
	}
	if cmp < 0 {
		k--
	}
	// Signed right shift is floor division, including negative odd exponents.
	exponent := (k >> 1) - 1074
	if exponent >= 1024 {
		return math.Inf(1), true, nil
	}
	if exponent < -1075 {
		return 0, true, nil
	}
	shift := max(2*exponent+2044, 0)
	var q uint64
	for bit := 52; bit >= 0; bit-- {
		candidate := q | uint64(1)<<uint(bit)
		if err := m.rootProduct(candidate, dlo, dhi); err != nil {
			return 0, false, err
		}
		if m.compareRootProduct(0, shift) >= 0 {
			q = candidate
		}
	}
	if err := m.rootProduct(2*q+1, dlo, dhi); err != nil {
		return 0, false, err
	}
	// 4A and midpoint²D need at most 4324 bits, within the same 68 words.
	cmp = m.compareRootProduct(2, shift)
	if cmp > 0 || cmp == 0 && q&1 != 0 {
		q++
	}
	if exponent < -1022 {
		return math.Float64frombits(q), true, nil
	}
	if q == uint64(1)<<53 {
		q >>= 1
		exponent++
	}
	if exponent >= 1024 {
		return math.Inf(1), true, nil
	}
	return math.Float64frombits(uint64(exponent+1023)<<52 | (q - (uint64(1) << 52))), true, nil
}
