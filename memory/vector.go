package memory

import (
	"fmt"
	"math"
)

// StringViewVector is a pointer-free UTF-8/binary physical representation.
// Offsets must contain Len()+1 entries. Values are slices of Data and no Go
// string headers are retained in the hot column buffer.
type StringViewVector struct {
	Offsets  []uint32
	Data     []byte
	Validity Bitmap
	Encoding Encoding
}

// Len returns the number of logical string rows.
func (v *StringViewVector) Len() int {
	if len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

// IsValid reports whether row i is non-NULL.
func (v *StringViewVector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

// ValueBytes returns a zero-copy view of row i or nil for an out-of-range or malformed offset.
func (v *StringViewVector) ValueBytes(i int) []byte {
	if i < 0 || i+1 >= len(v.Offsets) {
		return nil
	}
	lo, hi := v.Offsets[i], v.Offsets[i+1]
	if lo > hi || int(hi) > len(v.Data) {
		return nil
	}
	return v.Data[lo:hi]
}

// Validate checks row count and offset bounds for a string view vector.
func (v *StringViewVector) Validate(rows int) error {
	if v.Len() != rows {
		return fmt.Errorf("string view rows=%d want=%d", v.Len(), rows)
	}
	if len(v.Offsets) > 0 && int(v.Offsets[len(v.Offsets)-1]) > len(v.Data) {
		return fmt.Errorf("string view terminal offset exceeds data")
	}
	for i := 1; i < len(v.Offsets); i++ {
		if v.Offsets[i] < v.Offsets[i-1] {
			return fmt.Errorf("string view offset %d decreases from %d to %d", i, v.Offsets[i-1], v.Offsets[i])
		}
	}
	return nil
}

// StringListVector stores a nullable list<string> column. Offsets index the
// child string rows, while Values owns the child offsets, bytes, and optional
// element validity bitmap. Parent NULL and a valid empty list are distinct.
type StringListVector struct {
	Offsets  []uint32
	Values   *StringViewVector
	Validity Bitmap
	Encoding Encoding
}

// Len returns the number of logical list rows.
func (v *StringListVector) Len() int {
	if v == nil || len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

// IsValid reports whether row i is a non-NULL list.
func (v *StringListVector) IsValid(i int) bool {
	return v != nil && i >= 0 && i < v.Len() && (v.Validity == nil || IsSet(v.Validity, i))
}

// ValueRange returns the child row range for i. ok is false for a NULL or
// malformed row; a valid empty list returns equal start and end positions.
func (v *StringListVector) ValueRange(i int) (start, end int, ok bool) {
	if v == nil || !v.IsValid(i) || i < 0 || i+1 >= len(v.Offsets) || v.Values == nil {
		return 0, 0, false
	}
	lo, hi := v.Offsets[i], v.Offsets[i+1]
	if lo > hi || int(hi) > v.Values.Len() {
		return 0, 0, false
	}
	return int(lo), int(hi), true
}

// Validate checks the canonical list<string> offsets, parent validity, child
// string vector, and element validity bounds.
func (v *StringListVector) Validate(rows int) error {
	if v == nil {
		return fmt.Errorf("list<string> vector is nil")
	}
	if len(v.Offsets) != rows+1 {
		return fmt.Errorf("list<string> offsets=%d want=%d", len(v.Offsets), rows+1)
	}
	if len(v.Offsets) == 0 || v.Offsets[0] != 0 {
		return fmt.Errorf("list<string> first offset must be zero")
	}
	if v.Values == nil {
		return fmt.Errorf("list<string> child vector is nil")
	}
	for i := 1; i < len(v.Offsets); i++ {
		if v.Offsets[i] < v.Offsets[i-1] {
			return fmt.Errorf("list<string> offset %d decreases from %d to %d", i, v.Offsets[i-1], v.Offsets[i])
		}
	}
	children := int(v.Offsets[len(v.Offsets)-1])
	if children != v.Values.Len() {
		return fmt.Errorf("list<string> terminal offset=%d child rows=%d", children, v.Values.Len())
	}
	if err := v.Values.Validate(children); err != nil {
		return fmt.Errorf("list<string> child: %w", err)
	}
	if v.Validity != nil && len(v.Validity) < WordsFor(rows) {
		return fmt.Errorf("list<string> validity words=%d want>=%d", len(v.Validity), WordsFor(rows))
	}
	if v.Values.Validity != nil && len(v.Values.Validity) < WordsFor(children) {
		return fmt.Errorf("list<string> child validity words=%d want>=%d", len(v.Values.Validity), WordsFor(children))
	}
	return nil
}

// AppendStringListRow appends one canonical row from src to dst. NULL parents
// append no hidden child values; NULL elements append an empty byte span while
// retaining child validity.
func AppendStringListRow(dst, src *StringListVector, row int) error {
	if dst == nil || src == nil || row < 0 || row >= src.Len() {
		return fmt.Errorf("list<string> append row %d out of range", row)
	}
	if dst.Values == nil {
		dst.Values = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
	}
	if len(dst.Offsets) == 0 {
		dst.Offsets = []uint32{0}
	}
	dstRow := len(dst.Offsets) - 1
	valid := src.IsValid(row)
	dst.Validity = appendValidity(dst.Validity, valid, dstRow)
	if valid {
		start, end, ok := src.ValueRange(row)
		if !ok {
			return fmt.Errorf("list<string> row %d has malformed offsets", row)
		}
		if uint64(len(dst.Values.Offsets)-1)+uint64(end-start) > math.MaxUint32 {
			return fmt.Errorf("list<string> child row count exceeds uint32")
		}
		for child := start; child < end; child++ {
			childRow := len(dst.Values.Offsets) - 1
			childValid := src.Values.IsValid(child)
			dst.Values.Validity = appendValidity(dst.Values.Validity, childValid, childRow)
			if childValid {
				value := src.Values.ValueBytes(child)
				if uint64(len(dst.Values.Data))+uint64(len(value)) > math.MaxUint32 {
					return fmt.Errorf("list<string> child data exceeds 4 GiB")
				}
				dst.Values.Data = append(dst.Values.Data, value...)
			}
			dst.Values.Offsets = append(dst.Values.Offsets, uint32(len(dst.Values.Data)))
		}
	}
	dst.Offsets = append(dst.Offsets, uint32(dst.Values.Len()))
	dst.Encoding = EncodingView
	return nil
}

// AppendNullStringList appends one canonical NULL parent row to dst.
func AppendNullStringList(dst *StringListVector) error {
	if dst == nil {
		return fmt.Errorf("list<string> destination is nil")
	}
	if dst.Values == nil {
		dst.Values = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
	}
	if len(dst.Offsets) == 0 {
		dst.Offsets = []uint32{0}
	}
	row := len(dst.Offsets) - 1
	dst.Validity = appendValidity(dst.Validity, false, row)
	dst.Offsets = append(dst.Offsets, uint32(dst.Values.Len()))
	dst.Encoding = EncodingView
	return nil
}

func appendValidity(dst Bitmap, valid bool, row int) Bitmap {
	if dst == nil && valid {
		return nil
	}
	if dst == nil {
		dst = EnsureBitmap(nil, row+1)
		for i := 0; i < row; i++ {
			Set(dst, i)
		}
	} else if WordsFor(row+1) > len(dst) {
		grown := make(Bitmap, WordsFor(row+1))
		copy(grown, dst)
		dst = grown
	}
	if valid {
		Set(dst, row)
	}
	return dst
}

// DictionaryStringVector keeps row values as integer codes. DictionaryDomain
// identifies the semantic dictionary. Codes can only be compared/grouped
// across batches when the domain is identical and stable.
type DictionaryStringVector struct {
	Indices          []uint32
	Validity         Bitmap
	Offsets          []uint32 // dictionary offsets, len=dict cardinality+1
	Data             []byte
	DictionaryDomain string
	Encoding         Encoding
}

// Len returns the number of logical rows.
func (v *DictionaryStringVector) Len() int { return len(v.Indices) }

// IsValid reports whether row i is non-NULL.
func (v *DictionaryStringVector) IsValid(i int) bool {
	return v.Validity == nil || IsSet(v.Validity, i)
}

// DictionaryLen returns the number of values in the dictionary.
func (v *DictionaryStringVector) DictionaryLen() int {
	if len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

// DictionaryValue returns a zero-copy dictionary value for code or nil when code is invalid.
func (v *DictionaryStringVector) DictionaryValue(code uint32) []byte {
	i := int(code)
	if i < 0 || i+1 >= len(v.Offsets) {
		return nil
	}
	lo, hi := v.Offsets[i], v.Offsets[i+1]
	if lo > hi || int(hi) > len(v.Data) {
		return nil
	}
	return v.Data[lo:hi]
}

// Validate checks row count, dictionary domain, and dictionary offset bounds.
func (v *DictionaryStringVector) Validate(rows int) error {
	if len(v.Indices) != rows {
		return fmt.Errorf("dictionary rows=%d want=%d", len(v.Indices), rows)
	}
	if v.DictionaryDomain == "" {
		return fmt.Errorf("dictionary string vector requires DictionaryDomain")
	}
	if len(v.Offsets) > 0 && int(v.Offsets[len(v.Offsets)-1]) > len(v.Data) {
		return fmt.Errorf("dictionary terminal offset exceeds data")
	}
	return nil
}

// OpaqueVector preserves a source-native column that has no specialized engine
// representation yet. Value is source-owned and valid until the next read on
// the same reader unless the source documents a longer lifetime.
type OpaqueVector struct {
	Rows      int
	Type      DataType
	ArrowType string
	Encoding  Encoding
	Validity  Bitmap
	Value     any
	// Owned reports whether the batch owns one retain/reference on Value and
	// must release it when the binding is cleared or the materialized batch is
	// released. Source-native bindings normally leave Owned false.
	Owned bool
}

// Len returns the logical row count.
func (v *OpaqueVector) Len() int {
	if v == nil {
		return 0
	}
	return v.Rows
}
