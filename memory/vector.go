package memory

import "fmt"

// Int64Vector stores a nullable int64 column in a contiguous value buffer plus validity bitmap.
type Int64Vector struct {
	Values   []int64
	Validity Bitmap // nil means all valid
	Encoding Encoding
}

// Len returns the logical row count.
func (v *Int64Vector) Len() int { return len(v.Values) }

// IsValid reports whether row i is non-NULL.
func (v *Int64Vector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

// Int32Vector stores a nullable int32 column in a contiguous value buffer plus validity bitmap.
type Int32Vector struct {
	Values   []int32
	Validity Bitmap
	Encoding Encoding
}

// Len returns the logical row count.
func (v *Int32Vector) Len() int { return len(v.Values) }

// IsValid reports whether row i is non-NULL.
func (v *Int32Vector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

// Uint32Vector stores a nullable uint32 column in a contiguous value buffer plus validity bitmap.
type Uint32Vector struct {
	Values   []uint32
	Validity Bitmap
	Encoding Encoding
}

// Len returns the logical row count.
func (v *Uint32Vector) Len() int { return len(v.Values) }

// IsValid reports whether row i is non-NULL.
func (v *Uint32Vector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

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
	return nil
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

// Uint64Vector stores a nullable uint64 column in a contiguous value buffer.
type Uint64Vector struct {
	Values   []uint64
	Validity Bitmap
	Encoding Encoding
}

// Len returns the logical row count.
func (v *Uint64Vector) Len() int { return len(v.Values) }

// IsValid reports whether row i is non-NULL.
func (v *Uint64Vector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

// Float64Vector stores a nullable float64 column in a contiguous value buffer.
type Float64Vector struct {
	Values   []float64
	Validity Bitmap
	Encoding Encoding
}

// Len returns the logical row count.
func (v *Float64Vector) Len() int { return len(v.Values) }

// IsValid reports whether row i is non-NULL.
func (v *Float64Vector) IsValid(i int) bool { return v.Validity == nil || IsSet(v.Validity, i) }

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
