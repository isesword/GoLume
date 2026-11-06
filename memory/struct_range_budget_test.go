package memory

import (
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func TestStructRangeEstimateIncludesInheritedNullBitmaps(t *testing.T) {
	children := NewBatch()
	children.Rows = 2
	children.Int8s["i8"] = &Int8Vector{Values: []int8{1, 2}}
	children.Int16s["i16"] = &Int16Vector{Values: []int16{1, 2}}
	children.Uint8s["u8"] = &Uint8Vector{Values: []uint8{1, 2}}
	children.Uint16s["u16"] = &Uint16Vector{Values: []uint16{1, 2}}
	children.Float32s["f32"] = &Float32Vector{Values: []float32{1, 2}}
	fields := []Field{{Name: "i8", Type: TypeInt8}, {Name: "i16", Type: TypeInt16}, {Name: "u8", Type: TypeUint8}, {Name: "u16", Type: TypeUint16}, {Name: "f32", Type: TypeFloat32}}
	src := NewBatch()
	src.Rows = 2
	src.Structs["s"] = &StructVector{Rows: 2, Fields: fields, Values: children, Validity: Bitmap{1}}
	estimated, err := EstimateBatchRangeBytes(src, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := CloneBatchRange(src, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	if actual := BatchBytes(clone); actual != estimated || actual != 68 {
		t.Fatalf("estimated=%d actual=%d want=68", estimated, actual)
	}
	if clone.Structs["s"].Values.Int8s["i8"].IsValid(1) {
		t.Fatal("parent NULL did not propagate to child")
	}
	// Estimation also propagates an ancestor NULL through nested storage. This
	// does not add nested struct expression support or invoke unsupported clone.
	inner := NewBatch()
	inner.Rows = 2
	inner.Structs["inner"] = &StructVector{Rows: 2, Fields: fields, Values: children}
	nested := NewBatch()
	nested.Rows = 2
	nested.Structs["outer"] = &StructVector{Rows: 2, Values: inner, Validity: Bitmap{1}}
	if got, err := EstimateBatchRangeBytes(nested, 0, 2); err != nil || got != 76 {
		t.Fatalf("nested estimate=%d err=%v want=76", got, err)
	}
}

func TestStructDictionaryCloneTakeAndArrowOwnership(t *testing.T) {
	child := NewBatch()
	child.Rows = 4
	dictionary := &DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Validity: Bitmap{0b1101}, DictionaryDomain: "shared-domain", Encoding: EncodingDictionary}
	child.DictStrings["text"] = dictionary
	field := Field{Name: "text", Type: TypeString, Nullable: true, Encoding: EncodingView}
	src := &StructVector{Rows: 4, Fields: []Field{field}, Values: child, Validity: Bitmap{0b1011}, Encoding: EncodingView}
	if err := src.Validate(StructField("payload", true, field), 4); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		rows  []int
		clone func() (*StructVector, error)
	}{
		{"clone", []int{0, 1, 2, 3}, func() (*StructVector, error) { return CloneStructRange(src, 0, 4) }},
		{"take", []int{3, 0, 1, 2}, func() (*StructVector, error) { return TakeStructRows(src, []int{3, 0, 1, 2}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.clone()
			if err != nil {
				t.Fatal(err)
			}
			defer got.Values.ReleaseOwned()
			d := got.Values.DictStrings["text"]
			if d == nil || d.DictionaryDomain != "shared-domain" || d.Encoding != EncodingDictionary {
				t.Fatal("dictionary representation/domain lost")
			}
			for i, row := range tc.rows {
				valid := row == 0 || row == 3
				if d.IsValid(i) != valid || got.IsValid(i) != (row != 2) {
					t.Fatalf("row %d validity", i)
				}
				if valid && string(d.DictionaryValue(d.Indices[i])) != string(dictionary.DictionaryValue(dictionary.Indices[row])) {
					t.Fatalf("row %d value", i)
				}
			}
			batch := NewBatch()
			batch.Rows = 4
			batch.Structs["payload"] = got
			alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			record, err := ArrowRecord(alloc, NewSchema(StructField("payload", true, field)), batch)
			if err != nil {
				t.Fatal(err)
			}
			defer record.Release()
			values := record.Column(0).(*array.Struct).Field(0).(*array.String)
			for i, row := range tc.rows {
				valid := row == 0 || row == 3
				if values.IsNull(i) == valid {
					t.Fatalf("Arrow row %d validity", i)
				}
				if valid && values.Value(i) != string(dictionary.DictionaryValue(dictionary.Indices[row])) {
					t.Fatalf("Arrow row %d value", i)
				}
			}
			dictionary.Data[0] = 'z'
			dictionary.Indices[0] = 1
			dictionary.Offsets[1] = 0
			dictionary.Validity[0] ^= 1
			if string(d.Data) != "ab" || d.Offsets[1] != 1 {
				t.Fatal("dictionary clone aliases source payload")
			}
			dictionary.Data[0] = 'a'
			dictionary.Indices[0] = 0
			dictionary.Offsets[1] = 1
			dictionary.Validity[0] ^= 1
		})
	}
}
