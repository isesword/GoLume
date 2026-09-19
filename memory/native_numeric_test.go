package memory

import "testing"

func TestNativeNumericVectorsLenAndValidity(t *testing.T) {
	validity := Bitmap{0b101}
	tests := []struct {
		name    string
		len     int
		isValid func(int) bool
	}{
		{name: "int8", len: (&Int8Vector{Values: []int8{-128, 0, 127}, Validity: validity, Encoding: EncodingPlain}).Len(), isValid: (&Int8Vector{Values: []int8{-128, 0, 127}, Validity: validity, Encoding: EncodingPlain}).IsValid},
		{name: "int16", len: (&Int16Vector{Values: []int16{-32768, 0, 32767}, Validity: validity, Encoding: EncodingPlain}).Len(), isValid: (&Int16Vector{Values: []int16{-32768, 0, 32767}, Validity: validity, Encoding: EncodingPlain}).IsValid},
		{name: "uint8", len: (&Uint8Vector{Values: []uint8{0, 1, 255}, Validity: validity, Encoding: EncodingPlain}).Len(), isValid: (&Uint8Vector{Values: []uint8{0, 1, 255}, Validity: validity, Encoding: EncodingPlain}).IsValid},
		{name: "uint16", len: (&Uint16Vector{Values: []uint16{0, 1, 65535}, Validity: validity, Encoding: EncodingPlain}).Len(), isValid: (&Uint16Vector{Values: []uint16{0, 1, 65535}, Validity: validity, Encoding: EncodingPlain}).IsValid},
		{name: "float32", len: (&Float32Vector{Values: []float32{-1.5, 0, 3.25}, Validity: validity, Encoding: EncodingPlain}).Len(), isValid: (&Float32Vector{Values: []float32{-1.5, 0, 3.25}, Validity: validity, Encoding: EncodingPlain}).IsValid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.len != 3 {
				t.Fatalf("Len() = %d, want 3", tc.len)
			}
			for row, want := range []bool{true, false, true} {
				if got := tc.isValid(row); got != want {
					t.Errorf("IsValid(%d) = %v, want %v", row, got, want)
				}
			}
		})
	}
}

func TestNativeNumericVectorsNilValidityMeansAllValid(t *testing.T) {
	tests := []struct {
		name    string
		isValid func(int) bool
	}{
		{name: "int8", isValid: (&Int8Vector{Values: []int8{1, 2}}).IsValid},
		{name: "int16", isValid: (&Int16Vector{Values: []int16{1, 2}}).IsValid},
		{name: "uint8", isValid: (&Uint8Vector{Values: []uint8{1, 2}}).IsValid},
		{name: "uint16", isValid: (&Uint16Vector{Values: []uint16{1, 2}}).IsValid},
		{name: "float32", isValid: (&Float32Vector{Values: []float32{1, 2}}).IsValid},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for row := 0; row < 2; row++ {
				if !tc.isValid(row) {
					t.Errorf("IsValid(%d) = false with nil validity", row)
				}
			}
		})
	}
}

func TestBatchNativeNumericBindingsAndBytes(t *testing.T) {
	b := NewBatch()
	b.Int8s["i8"] = &Int8Vector{Values: []int8{1, 2}}
	b.Int16s["i16"] = &Int16Vector{Values: []int16{3}}
	b.Uint8s["u8"] = &Uint8Vector{Values: []uint8{4, 5, 6}}
	b.Uint16s["u16"] = &Uint16Vector{Values: []uint16{7}}
	b.Float32s["f32"] = &Float32Vector{Values: []float32{1.5, 2.5}}
	want := int64(2 + 2 + 3 + 2 + 8)
	if got := BatchBytes(b); got != want {
		t.Fatalf("BatchBytes=%d, want %d", got, want)
	}
	if got, err := b.Float32("f32"); err != nil || got.Len() != 2 {
		t.Fatalf("Float32 lookup=(%v,%v)", got, err)
	}
	b.ClearColumns()
	if len(b.Int8s) != 0 || len(b.Float32s) != 0 {
		t.Fatalf("native bindings were not cleared")
	}
}
