package lazy

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestSeriesNativeNumericMaterializationAndStruct(t *testing.T) {
	fields := []memory.Field{
		{Name: "i8", Type: memory.TypeInt8, Nullable: true},
		{Name: "i16", Type: memory.TypeInt16, Nullable: true},
		{Name: "u8", Type: memory.TypeUint8, Nullable: true},
		{Name: "u16", Type: memory.TypeUint16, Nullable: true},
		{Name: "f32", Type: memory.TypeFloat32, Nullable: true},
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Int8s["i8"] = &memory.Int8Vector{Values: []int8{-1, 99, 3}, Validity: memory.Bitmap{5}}
	b.Int16s["i16"] = &memory.Int16Vector{Values: []int16{-1, 99, 3}, Validity: memory.Bitmap{5}}
	b.Uint8s["u8"] = &memory.Uint8Vector{Values: []uint8{1, 99, 3}, Validity: memory.Bitmap{5}}
	b.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{1, 99, 3}, Validity: memory.Bitmap{5}}
	b.Float32s["f32"] = &memory.Float32Vector{Values: []float32{1.25, 99, 3.5}, Validity: memory.Bitmap{5}}
	// Keep the child batch separate from its parent to avoid recursive ownership.
	parent := memory.NewBatch()
	parent.Rows = 3
	parent.Structs["record"] = &memory.StructVector{Rows: 3, Fields: fields, Values: b, Validity: memory.Bitmap{3}}
	src, err := inmemory.New(memory.NewSchema(fields...), b)
	if err != nil {
		t.Fatal(err)
	}
	structSrc, err := inmemory.New(memory.NewSchema(memory.StructField("record", true, fields...)), parent)
	if err != nil {
		t.Fatal(err)
	}
	first := []any{int8(-1), int16(-1), uint8(1), uint16(1), float32(1.25)}
	last := []any{int8(3), int16(3), uint8(3), uint16(3), float32(3.5)}
	session := NewSession(decimalExpressionOptions(false))
	defer session.Close()
	frame, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, field := range fields {
		s, err := frame.Column(field.Name)
		if err != nil {
			t.Fatal(err)
		}
		values, err := s.ToAnySlice()
		if err != nil {
			t.Fatal(err)
		}
		if len(values) != 3 || values[0] != first[i] || values[1] != nil || values[2] != last[i] || s.NullCount() != 1 {
			t.Fatalf("%s values=%v nulls=%d", field.Name, values, s.NullCount())
		}
	}
	frame.Close()
	frame, err = session.FromSource(structSrc).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s, err := frame.Column("record")
	if err != nil {
		t.Fatal(err)
	}
	values, err := s.ToAnySlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[2] != nil || s.NullCount() != 1 {
		t.Fatal("parent validity", values, s.NullCount())
	}
	for i, field := range fields {
		if values[0].(map[string]any)[field.Name] != first[i] || values[1].(map[string]any)[field.Name] != nil {
			t.Fatal("child value/validity", field.Name, values)
		}
	}
	chunks, err := s.StructChunks()
	if err != nil {
		t.Fatal(err)
	}
	value, valid, err := chunks[0].Value(0)
	if err != nil || !valid {
		t.Fatal(valid, err)
	}
	frame.Close()
	if _, _, err := value.Field("i8"); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatal("closed struct child", err)
	}
	if session.Engine().Memory().Used() != 0 {
		t.Fatal("leaked")
	}
}
