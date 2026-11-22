package memory

// Int128 stores a signed binary 128-bit integer in two's-complement form.
// High is the signed upper limb; Low holds the least significant 64 bits.
// Int128 values are currently only produced by bounded expression evaluation.
type Int128 struct {
	High int64
	Low  uint64
}

// Int128Vector is a typed view of expression results, not a Batch column.
type Int128Vector struct {
	Values   []Int128
	Validity Bitmap
}

// IsValid reports whether the indicated row is not NULL.
func (v *Int128Vector) IsValid(row int) bool {
	return v.Validity == nil || IsSet(v.Validity, row)
}
