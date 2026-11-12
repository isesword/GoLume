package expr

import (
	"gocolumn/memory"
	"testing"
)

func TestInt64ListColumnExportPrefixAndNull(t *testing.T) {
	schema := memory.NewSchema(memory.ListInt64Field("xs", true, true))
	b := memory.NewBatch()
	b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 2, 2, 3}, Values: &memory.Int64Vector{Values: []int64{42, 7, 9}, Validity: memory.Bitmap{5}}, Validity: memory.Bitmap{5}}
	b.Reset(2) // Export only the logical prefix, not all physical rows.
	p, err := Compile(schema, Col("xs"))
	if err != nil {
		t.Fatal(err)
	}
	w := &evaluationState{}
	defer w.Close()
	got, err := evaluateValueTest(p, b, w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	v := got.Int64List
	if v == nil || v.Len() != 2 || v.Values.Len() != 2 || !v.IsValid(0) || v.IsValid(1) || v.Values.IsValid(1) {
		t.Fatalf("wrong prefix export: %+v", v)
	}
	if b.Int64Lists["xs"].Len() != 3 || b.Int64Lists["xs"].Values.Len() != 3 {
		t.Fatal("input modified")
	}
}

func TestJSONDecodeInt64Lists(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString})
	target := memory.NewSchema(memory.ListInt64Field("xs", true, true))
	b := memory.NewBatch()
	b.Strings["payload"] = testStringVector([]string{`{"xs":[-9223372036854775808,null,9223372036854775807]}`, `{"xs":[]}`, `{"xs":null}`, `{}`}, nil)
	b.Reset(4)
	p, err := Compile(schema, JSONDecode(Col("payload"), target))
	if err != nil {
		t.Fatal(err)
	}
	w := &evaluationState{}
	defer w.Close()
	got, err := evaluateValueTest(p, b, w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	v := got.Struct.Values.Int64Lists["xs"]
	if v == nil {
		t.Fatal("missing typed list")
	}
	if err := v.Validate(4); err != nil {
		t.Fatal(err)
	}
	if v.Values.Values[0] != -9223372036854775808 || v.Values.Values[2] != 9223372036854775807 || v.Values.IsValid(1) || !v.IsValid(1) || v.IsValid(2) || v.IsValid(3) {
		t.Fatalf("wrong values: %+v", v)
	}
	for _, raw := range []string{`{"xs":[9223372036854775808]}`, `{"xs":[1.5]}`, `{"xs":["1"]}`} {
		b.Strings["payload"] = testStringVector([]string{raw}, nil)
		b.Reset(1)
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestInt64ListNullExpression(t *testing.T) {
	schema := memory.NewSchema(memory.ListInt64Field("xs", true, true))
	b := memory.NewBatch()
	b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 0, 0}, Values: &memory.Int64Vector{}, Validity: memory.Bitmap{1}}
	b.Reset(2)
	p, err := Compile(schema, IsNull(Col("xs")))
	if err != nil {
		t.Fatal(err)
	}
	w := &evaluationState{}
	defer w.Close()
	v, err := evaluateValueTest(p, b, w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if v.Uint32 == nil || v.Uint32.Values[0] != 0 || v.Uint32.Values[1] != 1 {
		t.Fatalf("null result %+v", v)
	}
}

func TestJSONDecodeEmptyInt64ListSchema(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString})
	p, err := Compile(schema, JSONDecode(Col("payload"), memory.NewSchema(memory.ListInt64Field("xs", false, false))))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Strings["payload"] = testStringVector(nil, nil)
	b.Reset(0)
	w := &evaluationState{}
	defer w.Close()
	v, err := evaluateValueTest(p, b, w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if v.Struct.Values.Int64Lists["xs"] == nil {
		t.Fatal("missing empty list")
	}
	if err := v.Struct.Values.Int64Lists["xs"].Validate(0); err != nil {
		t.Fatal(err)
	}
}
