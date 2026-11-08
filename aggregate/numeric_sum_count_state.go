package aggregate

import "gocolumn/memory"

// NumericSumCountState is a checked, non-owning view of one SUM/COUNT slot.
// It captures the current backing arrays and must be discarded before vector
// replacement, bundle growth or Close. It owns no buffers, does not extend
// their accounting lifetime, and requires the same exclusive access as its
// owner. Obtain it through BorrowSumCountState before a batch loop.
type NumericSumCountState struct {
	typ                    memory.DataType
	sum                    *uint64
	legacySum              *int64
	sumValid               *uint8
	countRows, countValues *uint64
}

// BorrowSumCountState binds an existing slot for layouts containing only SUM,
// COUNT(*) and/or COUNT(value). Invalid types, layouts or bounds return false
// and clear dst. dst must be non-nil. No memory is allocated.
func (v *NumericVector) BorrowSumCountState(slot int, dst *NumericSumCountState) bool {
	*dst = NumericSumCountState{}
	if v == nil || slot < 0 || slot >= v.slots || v.typ <= memory.TypeInvalid || v.typ > memory.TypeStruct {
		return false
	}
	l := v.layout
	if l.IsZero() || l.Min || l.Max || l.Avg || (l.Sum && !isAggregateNumeric(v.typ)) {
		return false
	}
	sums, valid, rows, values := v.sums, v.sumValid, v.countRows, v.countValues
	if v.typ == memory.TypeInt64 {
		if v.legacy.layout != l || (l.Sum && len(v.legacy.sums) <= slot) {
			return false
		}
		valid, rows, values = v.legacy.sumValid, v.legacy.countRows, v.legacy.countValues
	} else if l.Sum && len(sums) <= slot {
		return false
	}
	if (l.Sum && len(valid) <= slot) || (l.CountRows && len(rows) <= slot) || (l.CountValues && len(values) <= slot) {
		return false
	}
	dst.typ = v.typ
	if l.Sum {
		if v.typ == memory.TypeInt64 {
			dst.legacySum = &v.legacy.sums[slot]
		} else {
			dst.sum = &sums[slot]
		}
		dst.sumValid = &valid[slot]
	}
	if l.CountRows {
		dst.countRows = &rows[slot]
	}
	if l.CountValues {
		dst.countValues = &values[slot]
	}
	return true
}

// BorrowSumCountState binds one live, addressable group/column slot. It returns
// false and clears dst for closed bundles, invalid bounds or unsupported lanes.
// Discard the view before EnsureGroups replaces storage or Close releases it.
// dst must be non-nil. No memory is allocated.
func (b *NumericBundle) BorrowSumCountState(group, column int, dst *NumericSumCountState) bool {
	*dst = NumericSumCountState{}
	if b == nil || b.closed || group < 0 || group >= b.capacity || column < 0 || column >= len(b.lanes) {
		return false
	}
	return b.lanes[column].BorrowSumCountState(group, dst)
}

// SumBits returns the complete running sum in its output type's representation,
// or zero for count-only layouts. The view must still be valid.
func (s NumericSumCountState) SumBits() uint64 {
	if s.legacySum != nil {
		return uint64(*s.legacySum)
	}
	if s.sum != nil {
		return *s.sum
	}
	return 0
}

// Update stores a COMPLETE running sum, not a batch partial, and adds count
// deltas with wrapping uint64 arithmetic. A zero validValues leaves SUM bits
// and validity unchanged. Narrow integer SUMs remain widened to Int64; Int32
// sums are sign-extended, and Uint32/Float32 bits are truncated to 32 bits.
// Floating callers must preserve encounter order and Float32 step rounding
// when computing sumBits. The view must still be valid.
func (s NumericSumCountState) Update(sumBits, selectedRows, validValues uint64) {
	if s.countRows != nil {
		*s.countRows += selectedRows
	}
	if s.countValues != nil {
		*s.countValues += validValues
	}
	if validValues == 0 || s.sumValid == nil {
		return
	}
	if s.legacySum != nil {
		*s.legacySum = int64(sumBits)
	} else {
		switch s.typ {
		case memory.TypeInt32:
			sumBits = uint64(int64(int32(sumBits)))
		case memory.TypeUint32, memory.TypeFloat32:
			sumBits = uint64(uint32(sumBits))
		}
		*s.sum = sumBits
	}
	*s.sumValid = 1
}
