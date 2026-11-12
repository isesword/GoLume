package memory

import (
	"fmt"
	"math"
)

// Int64ListVector stores a nullable list<int64> column. Offsets index the
// child int64 rows, while Values owns the child values and optional
// element validity bitmap. Parent NULL and a valid empty list are distinct.
type Int64ListVector struct {
	Offsets  []uint32
	Values   *Int64Vector
	Validity Bitmap
	Encoding Encoding
}

// Len returns the number of logical list rows.
func (v *Int64ListVector) Len() int {
	if v == nil || len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

// IsValid reports whether row i is a non-NULL list.
func (v *Int64ListVector) IsValid(i int) bool {
	return v != nil && i >= 0 && i < v.Len() && (v.Validity == nil || IsSet(v.Validity, i))
}

// ValueRange returns the child row range for i. ok is false for a NULL or
// malformed row; a valid empty list returns equal start and end positions.
func (v *Int64ListVector) ValueRange(i int) (start, end int, ok bool) {
	if v == nil || !v.IsValid(i) || i < 0 || i+1 >= len(v.Offsets) || v.Values == nil {
		return 0, 0, false
	}
	lo, hi := v.Offsets[i], v.Offsets[i+1]
	if lo > hi || int(hi) > v.Values.Len() {
		return 0, 0, false
	}
	return int(lo), int(hi), true
}

// Validate checks the canonical list<int64> offsets, parent validity, child
// int64 vector, and element validity bounds.
func (v *Int64ListVector) Validate(rows int) error {
	if v == nil {
		return fmt.Errorf("list<int64> vector is nil")
	}
	if rows < 0 || rows == math.MaxInt {
		return fmt.Errorf("list<int64> invalid row count")
	}
	if len(v.Offsets) != rows+1 {
		return fmt.Errorf("list<int64> offsets=%d want=%d", len(v.Offsets), rows+1)
	}
	if len(v.Offsets) == 0 || v.Offsets[0] != 0 {
		return fmt.Errorf("list<int64> first offset must be zero")
	}
	if v.Values == nil {
		return fmt.Errorf("list<int64> child vector is nil")
	}
	for i := 1; i < len(v.Offsets); i++ {
		if v.Offsets[i] < v.Offsets[i-1] {
			return fmt.Errorf("list<int64> offset %d decreases from %d to %d", i, v.Offsets[i-1], v.Offsets[i])
		}
	}
	children := int(v.Offsets[len(v.Offsets)-1])
	if children != v.Values.Len() {
		return fmt.Errorf("list<int64> terminal offset=%d child rows=%d", children, v.Values.Len())
	}

	if v.Validity != nil && len(v.Validity) < WordsFor(rows) {
		return fmt.Errorf("list<int64> validity words=%d want>=%d", len(v.Validity), WordsFor(rows))
	}
	if v.Values.Validity != nil && len(v.Values.Validity) < WordsFor(children) {
		return fmt.Errorf("list<int64> child validity words=%d want>=%d", len(v.Values.Validity), WordsFor(children))
	}
	return nil
}

// AppendInt64ListRow copies a logical row, suppressing hidden NULL payload.
func AppendInt64ListRow(dst, src *Int64ListVector, row int) error {
	if dst == nil || src == nil || row < 0 || row >= src.Len() {
		return fmt.Errorf("list<int64> append row out of range")
	}
	if err := rangeValidityBounds(src.Validity, row, 1); err != nil {
		return err
	}
	lo, hi, err := int64ListRangeBounds(src, row, 1)
	if err != nil {
		return err
	}
	if dst.Values == nil {
		dst.Values = &Int64Vector{Encoding: EncodingPlain}
	}
	if len(dst.Offsets) == 0 {
		dst.Offsets = []uint32{0}
	}
	if err := dst.validateAppendTarget(); err != nil {
		return err
	}
	valid := src.IsValid(row)
	if valid && uint64(dst.Values.Len())+uint64(hi-lo) > math.MaxUint32 {
		return fmt.Errorf("list<int64> child count exceeds uint32")
	}
	dst.Validity = appendValidity(dst.Validity, valid, dst.Len())
	if valid {
		for i := int(lo); i < int(hi); i++ {
			validChild := src.Values.IsValid(i)
			dst.Values.Validity = appendValidity(dst.Values.Validity, validChild, dst.Values.Len())
			value := int64(0)
			if validChild {
				value = src.Values.Values[i]
			}
			dst.Values.Values = append(dst.Values.Values, value)
		}
	}
	dst.Offsets = append(dst.Offsets, uint32(dst.Values.Len()))
	dst.Encoding = EncodingView
	return nil
}

// AppendNullInt64List appends a NULL parent without child values.
func AppendNullInt64List(dst *Int64ListVector) error {
	if dst == nil {
		return fmt.Errorf("list<int64> destination nil")
	}
	if dst.Values == nil {
		dst.Values = &Int64Vector{Encoding: EncodingPlain}
	}
	if len(dst.Offsets) == 0 {
		dst.Offsets = []uint32{0}
	}
	if err := dst.validateAppendTarget(); err != nil {
		return err
	}
	dst.Validity = appendValidity(dst.Validity, false, dst.Len())
	dst.Offsets = append(dst.Offsets, uint32(dst.Values.Len()))
	dst.Encoding = EncodingView
	return nil
}
func int64ListRangeBounds(v *Int64ListVector, offset, length int) (uint32, uint32, error) {
	if v == nil || v.Values == nil {
		return 0, 0, fmt.Errorf("list<int64> vector or child nil")
	}
	lo, hi, err := rangeOffsetBounds(v.Offsets, offset, length, v.Values.Len(), false)
	if err != nil {
		return 0, 0, err
	}
	if err = rangeValidityBounds(v.Validity, offset, length); err != nil {
		return 0, 0, err
	}
	err = rangeValidityBounds(v.Values.Validity, int(lo), int(hi-lo))
	return lo, hi, err
}

// validateAppendTarget checks destination boundary state without rescanning all
// prior rows for every append. Existing interior offsets remain caller-owned.
func (v *Int64ListVector) validateAppendTarget() error {
	if v.Offsets[0] != 0 || uint64(v.Values.Len()) > math.MaxUint32 || uint64(v.Offsets[len(v.Offsets)-1]) != uint64(v.Values.Len()) {
		return fmt.Errorf("invalid list<int64> append destination")
	}
	if err := rangeValidityBounds(v.Validity, 0, v.Len()); err != nil {
		return err
	}
	return rangeValidityBounds(v.Values.Validity, 0, v.Values.Len())
}
