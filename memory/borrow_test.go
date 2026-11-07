package memory

import (
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func TestBorrowBatchRangePayloadAndMetadataOwnership(t *testing.T) {
	src := NewBatch()
	src.Rows = 4
	valid := Bitmap{0b1101}
	src.Int8s["i"] = &Int8Vector{Values: []int8{-128, 1, 2, 127}, Validity: valid}
	src.Float32s["f"] = &Float32Vector{Values: []float32{1, 2, 3, 4}, Validity: valid}
	src.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 3, 6, 10}, Data: []byte("abbcccdddd"), Validity: valid}
	src.DictStrings["d"] = &DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("xy"), Validity: valid, DictionaryDomain: "domain", Encoding: EncodingDictionary}
	src.StringLists["l"] = testStringListVector()
	children := NewBatch()
	children.Rows = 4
	children.Int8s["i"] = src.Int8s["i"]
	src.Structs["n"] = &StructVector{Rows: 4, Fields: []Field{{Name: "i", Type: TypeInt8, Nullable: true}}, Values: children, Validity: valid}
	metadata, err := BorrowedRangeMetadataBytes(src, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	view, err := BorrowBatchRange(src, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if &view.Int8s["i"].Values[0] != &src.Int8s["i"].Values[1] || &view.Float32s["f"].Values[0] != &src.Float32s["f"].Values[1] {
		t.Fatal("numeric payload copied")
	}
	if &view.Strings["s"].Data[0] != &src.Strings["s"].Data[1] || &view.DictStrings["d"].Indices[0] != &src.DictStrings["d"].Indices[1] {
		t.Fatal("string/dictionary payload copied")
	}
	if view.Strings["s"].Offsets[0] != 0 || string(view.Strings["s"].ValueBytes(0)) != "bb" || view.DictStrings["d"].DictionaryDomain != "domain" {
		t.Fatal("borrowed string metadata changed")
	}
	if err := view.StringLists["l"].Validate(3); err != nil {
		t.Fatal(err)
	}
	if err := view.Structs["n"].Validate(StructField("n", true, Field{Name: "i", Type: TypeInt8, Nullable: true}), 3); err != nil {
		t.Fatal(err)
	}
	// Exactly the misaligned bitmaps and rebased offset buffers are owned.
	actual := int64(len(view.Int8s["i"].Validity)+len(view.Float32s["f"].Validity)+len(view.Strings["s"].Validity)+len(view.DictStrings["d"].Validity)+len(view.StringLists["l"].Validity)+len(view.Structs["n"].Validity)+len(view.Structs["n"].Values.Int8s["i"].Validity)) * 8
	actual += int64(len(view.Strings["s"].Offsets)) * 4
	list := src.StringLists["l"]
	lo := list.Offsets[1]
	if lo != 0 {
		actual += int64(len(view.StringLists["l"].Offsets)) * 4
	}
	if list.Values.Offsets[lo] != 0 {
		actual += int64(len(view.StringLists["l"].Values.Offsets)) * 4
	}
	if lo&63 != 0 {
		actual += int64(len(view.StringLists["l"].Values.Validity)) * 8
	}
	if metadata != actual {
		t.Fatalf("metadata=%d actual allocated=%d", metadata, actual)
	}
	// Mutating owned view metadata cannot alter source metadata.
	before := valid[0]
	view.Int8s["i"].Validity[0] ^= 1
	if valid[0] != before {
		t.Fatal("misaligned bitmap aliases source")
	}
	view.Strings["s"].Offsets[0] = 99
	if src.Strings["s"].Offsets[1] != 1 {
		t.Fatal("rebased offsets alias source")
	}
	view.ClearColumns()
	if src.Rows != 4 || len(src.Int8s) != 1 || len(children.Int8s) != 1 || src.Int8s["i"].Values[0] != -128 {
		t.Fatal("clearing view destroyed source bindings")
	}
}

func TestBorrowBatchRangeOpaqueReleaseIsIndependent(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{10, 20, 30}, nil)
	values := builder.NewInt64Array()
	builder.Release()
	src := NewBatch()
	src.Rows = 3
	src.Opaque["x"] = &OpaqueVector{Rows: 3, Type: TypeInt64, Value: values, Owned: true}
	full, err := BorrowBatchRange(src, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	full.ClearColumns()
	if !src.Opaque["x"].Owned || values.Value(0) != 10 {
		t.Fatal("full borrow released source-owned Arrow array")
	}
	partial, err := BorrowBatchRange(src, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !partial.Opaque["x"].Owned || partial.Opaque["x"].Value.(*array.Int64).Value(0) != 20 {
		t.Fatal("partial Arrow view is invalid")
	}
	partial.ClearColumns()
	partial.ClearColumns()
	if !src.Opaque["x"].Owned || values.Value(2) != 30 {
		t.Fatal("partial release destroyed source")
	}
	src.ClearColumns()
}

func TestBorrowBatchRangeAlignedValidityAndInvalidMetadata(t *testing.T) {
	src := NewBatch()
	src.Rows = 70
	bits := Bitmap{^uint64(0), 0b101101}
	src.Uint16s["x"] = &Uint16Vector{Values: make([]uint16, 70), Validity: bits}
	view, err := BorrowBatchRange(src, 64, 6)
	if err != nil {
		t.Fatal(err)
	}
	if &view.Uint16s["x"].Validity[0] != &bits[1] {
		t.Fatal("aligned bitmap was copied")
	}
	if n, err := BorrowedRangeMetadataBytes(src, 64, 6); err != nil || n != 0 {
		t.Fatalf("aligned metadata=%d err=%v", n, err)
	}
	view.ClearColumns()
	src.Uint16s["x"].Validity = Bitmap{1}
	if _, err := BorrowBatchRange(src, 63, 3); err == nil {
		t.Fatal("accepted short validity")
	}
	if _, err := BorrowedRangeMetadataBytes(src, 63, 3); err == nil {
		t.Fatal("estimated short validity")
	}
}

type borrowOpaqueValue struct{ releases int }

func (v *borrowOpaqueValue) Release() { v.releases++ }
func (v *borrowOpaqueValue) Retain()  {}
func TestBorrowWholeOpaqueDoesNotRequireCloneSizing(t *testing.T) {
	value := &borrowOpaqueValue{}
	src := NewBatch()
	src.Rows = 2
	src.Opaque["x"] = &OpaqueVector{Rows: 2, Type: TypeArrow, Value: value, Owned: true}
	view, err := BorrowBatchRange(src, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if view.Opaque["x"].Value != value || view.Opaque["x"].Owned {
		t.Fatal("whole opaque borrow changed ownership")
	}
	view.ClearColumns()
	if value.releases != 0 || !src.Opaque["x"].Owned {
		t.Fatal("view released source-owned opaque value")
	}
	if _, err := BorrowBatchRange(src, 0, 1); err == nil {
		t.Fatal("unknown opaque partial range accepted")
	}
	if _, err := EstimateBatchRangeBytes(src, 0, 2); err == nil {
		t.Fatal("unknown opaque payload acquired a fake sizing contract")
	}
	src.ClearColumns()
	if value.releases != 1 {
		t.Fatalf("source releases=%d", value.releases)
	}
}
