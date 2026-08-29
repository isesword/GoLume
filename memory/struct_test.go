package memory

import "testing"

func TestStructCloneMillionRowsExtremeNullDensity(t *testing.T) {
	const rows = 1_000_000
	field := StructField("payload", true, Field{Name: "value", Type: TypeInt64})
	parentValidity := EnsureBitmap(nil, rows)
	Set(parentValidity, 0)
	Set(parentValidity, rows-1)
	children := NewBatch()
	children.Int64s["value"] = &Int64Vector{Values: make([]int64, rows)}
	children.Int64s["value"].Values[0] = 11
	children.Int64s["value"].Values[rows-1] = 99
	children.Reset(rows)
	batch := NewBatch()
	batch.Structs["payload"] = &StructVector{
		Rows: rows, Fields: append([]Field(nil), field.Children...), Values: children,
		Validity: parentValidity, Encoding: EncodingView,
	}
	batch.Reset(rows)
	if err := batch.Structs["payload"].Validate(field, rows); err != nil {
		t.Fatal(err)
	}

	clone, err := CloneBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	got := clone.Structs["payload"]
	if err := got.Validate(field, rows); err != nil {
		t.Fatal(err)
	}
	if !got.IsValid(0) || got.IsValid(1) || !got.IsValid(rows-1) {
		t.Fatalf("cloned parent validity is incorrect")
	}
	if got.Values.Int64s["value"].Values[0] != 11 || got.Values.Int64s["value"].Values[rows-1] != 99 {
		t.Fatalf("cloned child endpoints are incorrect")
	}
	children.Int64s["value"].Values[0] = -1
	if got.Values.Int64s["value"].Values[0] != 11 {
		t.Fatal("Struct clone aliases child storage")
	}
}
