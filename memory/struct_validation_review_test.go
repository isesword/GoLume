package memory

import (
	"math"
	"testing"
)

// Independently exercise an empty dictionary: it is valid only when no visible
// row references it, including at the bitmap word boundary.
func TestStructValidationReviewEmptyDictionaryVisibility(t *testing.T) {
	for _, visible := range []bool{false, true} {
		f := StructField("record", true, Field{Name: "s", Type: TypeString, Nullable: true})
		b := NewBatch()
		b.Rows = 65
		d := &DictionaryStringVector{Indices: make([]uint32, 65), Offsets: []uint32{0}, Validity: Bitmap{0, 0, math.MaxUint64}}
		for i := range d.Indices {
			d.Indices[i] = math.MaxUint32
		}
		if visible {
			d.Validity[1] = 1
		}
		b.DictStrings["s"] = d
		v := &StructVector{Rows: 65, Fields: f.Children, Values: b}
		err := v.Validate(f, 65)
		if visible && err == nil {
			t.Fatal("visible row 64 with an empty dictionary was accepted")
		}
		if !visible && err != nil {
			t.Fatalf("all-NULL dictionary with extra bitmap word rejected: %v", err)
		}
		v.Validity = Bitmap{math.MaxUint64, 0}
		if err := v.Validate(f, 65); err != nil {
			t.Fatalf("parent-hidden code rejected: %v", err)
		}
	}
}

func TestStructValidationReviewNonNullableParentTailBits(t *testing.T) {
	f := StructField("record", false, Field{Name: "x", Type: TypeInt64, Nullable: true})
	b := NewBatch()
	b.Rows = 65
	b.Int64s["x"] = &Int64Vector{Values: make([]int64, 65), Validity: Bitmap{0, 0, 0}}
	v := &StructVector{Rows: 65, Fields: f.Children, Values: b, Validity: Bitmap{math.MaxUint64, 1, 0}}
	if err := v.Validate(f, 65); err != nil {
		t.Fatalf("unused zero tail bits/extra word rejected: %v", err)
	}
	v.Validity[1] = 0
	if err := v.Validate(f, 65); err == nil {
		t.Fatal("NULL parent at row 64 accepted")
	}
}

func TestStructValidationReviewShortDictionaryBitmap(t *testing.T) {
	f := StructField("record", true, Field{Name: "s", Type: TypeString, Nullable: true})
	b := NewBatch()
	b.Rows = 65
	b.DictStrings["s"] = &DictionaryStringVector{Indices: make([]uint32, 65), Offsets: []uint32{0, 1}, Data: []byte("a"), Validity: Bitmap{math.MaxUint64}}
	v := &StructVector{Rows: 65, Fields: f.Children, Values: b, Validity: Bitmap{0, 0}}
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("Validate panicked: %v", p)
		}
	}()
	if err := v.Validate(f, 65); err == nil {
		t.Fatal("short dictionary bitmap accepted even though parent hides its rows")
	}
}
