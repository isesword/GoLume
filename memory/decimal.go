package memory

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Decimal128 is a signed 128-bit unscaled coefficient. Its numerical value is
// coefficient * 10^-scale; scale and precision belong to its vector and Field.
// Native execution never implicitly converts a coefficient to floating point.
type Decimal128 = decimal128.Num

// Decimal128Vector is plain, pointer-free decimal storage with explicit logical
// precision and scale. The caller owns Values and Validity. NULL coefficients
// are ignored, but every valid coefficient must fit Precision.
type Decimal128Vector struct {
	Values    []Decimal128
	Validity  Bitmap
	Precision int32
	Scale     int32
}

// Decimal128Field constructs a native decimal128 field or rejects unsupported
// precision/scale. Wider and negative-scale decimals remain opaque Arrow data.
func Decimal128Field(name string, nullable bool, precision, scale int32) (Field, error) {
	f := Field{Name: name, Type: TypeDecimal, Nullable: nullable, Encoding: EncodingPlain, DecimalPrecision: precision, DecimalScale: scale}
	if err := f.ValidateDecimal128(); err != nil {
		return Field{}, err
	}
	return f, nil
}

// ValidateDecimal128 validates the native decimal128 metadata contract. An
// optional Arrow spelling must agree with the explicit precision and scale.
func (f Field) ValidateDecimal128() error {
	if f.Type != TypeDecimal || f.DecimalPrecision < 1 || f.DecimalPrecision > 38 || f.DecimalScale < 0 || f.DecimalScale > f.DecimalPrecision {
		return fmt.Errorf("field %q requires native decimal128 precision 1..38 and scale 0..precision, got type=%s precision=%d scale=%d", f.Name, f.Type, f.DecimalPrecision, f.DecimalScale)
	}
	if f.Encoding != EncodingPlain || len(f.Children) != 0 || f.DictionaryDomain != "" {
		return fmt.Errorf("decimal field %q requires plain scalar encoding", f.Name)
	}
	if f.ArrowType != "" {
		expected := (&arrow.Decimal128Type{Precision: f.DecimalPrecision, Scale: f.DecimalScale}).String()
		if f.ArrowType != expected {
			return fmt.Errorf("decimal field %q Arrow type %q conflicts with %s", f.Name, f.ArrowType, expected)
		}
	}
	return nil
}

// Decimal128FitsPrecision checks the strict decimal coefficient bounds without
// negating the input, which also safely rejects the signed 128-bit minimum.
func Decimal128FitsPrecision(value Decimal128, precision int32) bool {
	if precision < 1 || precision > 38 {
		return false
	}
	limit := decimal128.GetScaleMultiplier(int(precision))
	return value.Cmp(limit) < 0 && value.Cmp(limit.Negate()) > 0
}

// Decimal128FromBigEndian decodes a Parquet two's-complement big-endian
// coefficient. Arbitrary sign extension is accepted, but overflow, empty
// encodings and coefficients outside the declared precision fail explicitly.
// No input byte slice is retained and no per-value heap buffer is allocated.
func Decimal128FromBigEndian(data []byte, precision int32) (Decimal128, error) {
	if len(data) == 0 {
		return Decimal128{}, fmt.Errorf("decimal coefficient is empty")
	}
	for len(data) > 16 {
		if data[0] == 0 && data[1]&0x80 == 0 || data[0] == 0xff && data[1]&0x80 != 0 {
			data = data[1:]
		} else {
			return Decimal128{}, fmt.Errorf("decimal coefficient exceeds signed 128-bit range")
		}
	}
	var buf [16]byte
	if data[0]&0x80 != 0 {
		for i := range buf {
			buf[i] = 0xff
		}
	}
	copy(buf[16-len(data):], data)
	value := decimal128.New(int64(binary.BigEndian.Uint64(buf[:8])), binary.BigEndian.Uint64(buf[8:]))
	if !Decimal128FitsPrecision(value, precision) {
		return Decimal128{}, fmt.Errorf("decimal coefficient exceeds precision %d", precision)
	}
	return value, nil
}

// Len returns the logical row count.
func (v *Decimal128Vector) Len() int {
	if v == nil {
		return 0
	}
	return len(v.Values)
}

// IsValid reports whether row is an existing, non-NULL value.
func (v *Decimal128Vector) IsValid(row int) bool {
	return v != nil && row >= 0 && row < len(v.Values) && (v.Validity == nil || row>>6 < len(v.Validity) && IsSet(v.Validity, row))
}

func (v *Decimal128Vector) validateRange(offset, length int) error {
	if v == nil || offset < 0 || length < 0 || offset > len(v.Values) || length > len(v.Values)-offset {
		return fmt.Errorf("decimal vector is shorter than range")
	}
	if _, err := Decimal128Field("", true, v.Precision, v.Scale); err != nil {
		return err
	}
	if v.Validity != nil && len(v.Validity) < WordsFor(offset+length) {
		return fmt.Errorf("decimal validity is shorter than range")
	}
	return nil
}

// Validate checks metadata, exact row count, validity storage and all valid
// coefficients. It does not require canonical zero payloads for NULL rows.
func (v *Decimal128Vector) Validate(rows int) error {
	if err := v.validateRange(0, rows); err != nil {
		return err
	}
	if len(v.Values) != rows {
		return fmt.Errorf("decimal vector rows=%d want=%d", len(v.Values), rows)
	}
	for row, value := range v.Values {
		if v.IsValid(row) && !Decimal128FitsPrecision(value, v.Precision) {
			return fmt.Errorf("decimal row %d exceeds precision %d", row, v.Precision)
		}
	}
	return nil
}

// ValidateField also checks vector/schema precision and scale agreement.
func (v *Decimal128Vector) ValidateField(field Field, rows int) error {
	if err := field.ValidateDecimal128(); err != nil {
		return err
	}
	if err := v.Validate(rows); err != nil {
		return err
	}
	if v.Precision != field.DecimalPrecision || v.Scale != field.DecimalScale {
		return fmt.Errorf("decimal field %q vector precision/scale mismatch", field.Name)
	}
	if !field.Nullable && HasNulls(v.Validity, rows) {
		return fmt.Errorf("non-nullable decimal field %q contains NULL", field.Name)
	}
	return nil
}

// Decimal returns the named native decimal128 vector or an explicit error.
func (b *Batch) Decimal(name string) (*Decimal128Vector, error) {
	v := b.Decimals[name]
	if v == nil {
		return nil, fmt.Errorf("decimal column %q not present in batch", name)
	}
	return v, nil
}
