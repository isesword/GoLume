package memory

import (
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func TestCloneBatchRangePreservesNestedValuesAndNulls(t *testing.T) {
	valid := EnsureBitmap(nil, 4)
	Set(valid, 0)
	Set(valid, 2)
	Set(valid, 3)
	children := NewBatch()
	children.Int64s["n"] = &Int64Vector{Values: []int64{10, 20, 30, 40}, Validity: valid}
	children.Reset(4)
	batch := NewBatch()
	batch.Int64s["i"] = &Int64Vector{Values: []int64{1, 2, 3, 4}, Validity: valid}
	batch.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 3, 6, 10}, Data: []byte("abbcccdddd"), Validity: valid, Encoding: EncodingView}
	batch.StringLists["list"] = testStringListVector()
	batch.Structs["struct"] = &StructVector{
		Rows: 4, Fields: []Field{{Name: "n", Type: TypeInt64, Nullable: true}},
		Values: children, Validity: valid, Encoding: EncodingView,
	}
	batch.Reset(4)

	estimated, err := EstimateBatchRangeBytes(batch, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := CloneBatchRange(batch, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer got.ReleaseOwned()
	if actual := BatchBytes(got); actual != estimated {
		t.Fatalf("range bytes actual=%d estimated=%d", actual, estimated)
	}
	if got.Rows != 2 || got.Int64s["i"].IsValid(0) || !got.Int64s["i"].IsValid(1) {
		t.Fatalf("int range rows=%d validity=%v", got.Rows, got.Int64s["i"].Validity)
	}
	if string(got.Strings["s"].ValueBytes(0)) != "bb" || string(got.Strings["s"].ValueBytes(1)) != "ccc" {
		t.Fatalf("string offsets=%v data=%q", got.Strings["s"].Offsets, got.Strings["s"].Data)
	}
	requireStringListRow(t, got.StringLists["list"], 0, false, nil, nil)
	requireStringListRow(t, got.StringLists["list"], 1, true, nil, nil)
	structField := StructField("struct", true, Field{Name: "n", Type: TypeInt64, Nullable: true})
	if err := got.Structs["struct"].Validate(structField, 2); err != nil {
		t.Fatal(err)
	}
	if got.Structs["struct"].IsValid(0) || !got.Structs["struct"].IsValid(1) || got.Structs["struct"].Values.Int64s["n"].Values[1] != 30 {
		t.Fatalf("struct range=%+v", got.Structs["struct"])
	}
}

func TestCloneBatchRangeSlicesOpaqueArrow(t *testing.T) {
	builder := array.NewInt64Builder(arrowmem.NewGoAllocator())
	builder.AppendValues([]int64{10, 20, 30}, nil)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	batch := NewBatch()
	batch.Opaque["arrow"] = &OpaqueVector{Rows: 3, Type: TypeInt64, ArrowType: "int64", Value: values}
	batch.Reset(3)
	got, err := CloneBatchRange(batch, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer got.ReleaseOwned()
	vector, ok := got.Opaque["arrow"].Value.(*array.Int64)
	if !ok || vector.Len() != 1 || vector.Value(0) != 20 || !got.Opaque["arrow"].Owned {
		t.Fatalf("opaque range=%T %+v", got.Opaque["arrow"].Value, got.Opaque["arrow"])
	}
}

func TestCloneBatchRangeRejectsInvalidRange(t *testing.T) {
	batch := NewBatch()
	batch.Reset(2)
	for _, test := range [][2]int{{-1, 1}, {0, -1}, {2, 1}, {1, 2}} {
		if _, err := CloneBatchRange(batch, test[0], test[1]); err == nil {
			t.Fatalf("range %v accepted", test)
		}
	}
}
