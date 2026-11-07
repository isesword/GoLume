package expr

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"math"
	"strings"
	"testing"

	"gocolumn/memory"
)

func TestJSONDecodeExplicitStructSemantics(t *testing.T) {
	target := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt8},
		memory.Field{Name: "name", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "active", Type: memory.TypeBool},
		memory.ListStringField("tags", true, true),
	)
	schema := memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["payload"] = testStringVector([]string{
		`{"id":7,"name":"中","active":true,"tags":["go",null,""],"ignored":1}`,
		`null`,
		`{"id":-8,"active":false}`,
		`ignored`,
	}, []bool{true, true, true, false})
	batch.Reset(4)
	program, err := Compile(schema, JSONDecode(Col("payload"), target))
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluateValueTest(program, batch, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	field := memory.StructField("decoded", true, target.Fields...)
	if err := got.Struct.Validate(field, 4); err != nil {
		t.Fatal(err)
	}
	if !got.Struct.IsValid(0) || got.Struct.IsValid(1) || !got.Struct.IsValid(2) || got.Struct.IsValid(3) {
		t.Fatalf("parent validity=%v", got.Struct.Validity)
	}
	if got.Struct.Values.Int8s["id"].Values[0] != 7 || got.Struct.Values.Uint32s["active"].Values[0] != 1 {
		t.Fatalf("decoded scalar children=%+v", got.Struct.Values)
	}
	tags := got.Struct.Values.StringLists["tags"]
	if start, end, ok := tags.ValueRange(0); !ok || end-start != 3 || tags.Values.IsValid(start+1) || string(tags.Values.ValueBytes(start+2)) != "" {
		t.Fatalf("decoded tags=%+v range=(%d,%d,%v)", tags, start, end, ok)
	}
	if tags.IsValid(2) {
		t.Fatal("missing nullable tags must be NULL")
	}
}

func TestJSONDecodeRejectsInvalidRowsAndOverflow(t *testing.T) {
	tests := []struct {
		name, json, want string
		field            memory.Field
	}{
		{"required", `{}`, `row 0 field "x"`, memory.Field{Name: "x", Type: memory.TypeInt64}},
		{"type", `{"x":"no"}`, "expected integer", memory.Field{Name: "x", Type: memory.TypeInt64}},
		{"overflow", `{"x":128}`, "overflows int8", memory.Field{Name: "x", Type: memory.TypeInt8}},
		{"malformed", `{`, "row 0", memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}},
		{"trailing", `{} {}`, "trailing JSON value", memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}},
		{"list-child", `{"x":[null]}`, `field "x"[0]`, memory.ListStringField("x", false, false)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView})
			batch := memory.NewBatch()
			batch.Strings["payload"] = testStringVector([]string{tc.json}, nil)
			batch.Reset(1)
			program, err := Compile(schema, JSONDecode(Col("payload"), memory.NewSchema(tc.field)))
			if err != nil {
				t.Fatal(err)
			}
			_, err = evaluateValueTest(program, batch, &evaluationState{}, testKernelSet())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestJSONDecodeBudgetFailureReleasesWorkspaceReservation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "payload", Type: memory.TypeString, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["payload"] = testStringVector([]string{`{"value":"a long value"}`}, nil)
	batch.Reset(1)
	program, err := Compile(schema, JSONDecode(Col("payload"), memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingView})))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(4)
	workspace := &evaluationState{}
	workspace.SetMemoryManager(context.Background(), manager)
	if _, err := evaluateValueTest(program, batch, workspace, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("workspace leaked %d bytes", manager.Used())
	}
}

func TestJSONDecodeNativeChildrenOwnedRoundTrip(t *testing.T) {
	fields := []memory.Field{
		{Name: "i8", Type: memory.TypeInt8, Nullable: true},
		{Name: "i16", Type: memory.TypeInt16, Nullable: true},
		{Name: "u8", Type: memory.TypeUint8, Nullable: true},
		{Name: "u16", Type: memory.TypeUint16, Nullable: true},
		{Name: "f32", Type: memory.TypeFloat32, Nullable: true},
	}
	payloads := []string{
		`{"i8":-128,"i16":-32768,"u8":0,"u16":0,"f32":1.0000000596046447753906250000000001}`,
		`{"i8":127,"i16":32767,"u8":255,"u16":65535,"f32":3.40282346638528859811704183484516925440e38}`,
		`{"i8":null,"i16":null,"u8":null,"u16":null,"f32":null}`,
		`null`, `{}`, `not JSON but the input row is NULL`,
	}
	input := scalarRegister{typ: memory.TypeString, strView: testStringVector(payloads, []bool{true, true, true, true, true, false})}
	input.validity = input.strView.Validity
	manager := memory.NewBudgetManager(1 << 20)
	out := scalarRegister{acquire: func(n int64) (memory.Reservation, error) { return manager.Reserve(context.Background(), n) }}
	defer func() {
		if out.jsonReservation != nil {
			out.jsonReservation.Release()
		}
	}()
	outputBytes, _, err := jsonAllocationSizes(&out, &input, len(fields), len(payloads))
	if err != nil {
		t.Fatal(err)
	}
	if err := evalJSONDecode(&out, &input, fields, len(payloads)); err != nil {
		t.Fatal(err)
	}
	field := memory.StructField("decoded", true, fields...)
	if err := out.strct.Validate(field, len(payloads)); err != nil {
		t.Fatal(err)
	}
	values := out.strct.Values
	if len(values.Int32s) != 0 || len(values.Uint32s) != 0 || len(values.Float64s) != 0 {
		t.Fatal("JSON output contains widened compatibility vectors")
	}
	if values.Int8s["i8"].Values[0] != math.MinInt8 || values.Int8s["i8"].Values[1] != math.MaxInt8 || values.Int16s["i16"].Values[0] != math.MinInt16 || values.Int16s["i16"].Values[1] != math.MaxInt16 || values.Uint8s["u8"].Values[1] != math.MaxUint8 || values.Uint16s["u16"].Values[1] != math.MaxUint16 {
		t.Fatal("native integer boundaries lost")
	}
	if values.Float32s["f32"].Values[0] != math.Nextafter32(1, 2) || values.Float32s["f32"].Values[1] != math.MaxFloat32 {
		t.Fatalf("float32 incorrectly rounded: %v", values.Float32s["f32"].Values)
	}
	for row := 2; row < len(payloads); row++ {
		if values.Int8s["i8"].IsValid(row) || values.Int16s["i16"].IsValid(row) || values.Uint8s["u8"].IsValid(row) || values.Uint16s["u16"].IsValid(row) || values.Float32s["f32"].IsValid(row) {
			t.Fatalf("NULL child became valid at row %d", row)
		}
	}
	if out.strct.IsValid(3) || out.strct.IsValid(5) || !out.strct.IsValid(2) || !out.strct.IsValid(4) {
		t.Fatal("parent NULL semantics changed")
	}
	actual := jsonRetainedBufferCapacity(out.strct)
	expected := int64(cap(out.strct.Validity))*8 + int64(cap(values.Int8s["i8"].Values)) + int64(cap(values.Int16s["i16"].Values))*2 + int64(cap(values.Uint8s["u8"].Values)) + int64(cap(values.Uint16s["u16"].Values))*2 + int64(cap(values.Float32s["f32"].Values))*4
	for _, bits := range []memory.Bitmap{values.Int8s["i8"].Validity, values.Int16s["i16"].Validity, values.Uint8s["u8"].Validity, values.Uint16s["u16"].Validity, values.Float32s["f32"].Validity} {
		expected += int64(cap(bits)) * 8
	}
	if actual != expected || actual > outputBytes || manager.Used() != outputBytes {
		t.Fatalf("capacities=%d expected=%d allowance=%d used=%d", actual, expected, outputBytes, manager.Used())
	}
	clone, err := memory.CloneStructRange(out.strct, 0, len(payloads))
	if err != nil {
		t.Fatal(err)
	}
	if err := clone.Validate(field, len(payloads)); err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = len(payloads)
	batch.Structs["decoded"] = out.strct
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	record, err := memory.ArrowRecord(allocator, memory.NewSchema(field), batch)
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	arrowStruct := record.Column(0).(*array.Struct)
	if arrowStruct.Field(0).(*array.Int8).Value(0) != math.MinInt8 || arrowStruct.Field(1).(*array.Int16).Value(1) != math.MaxInt16 || arrowStruct.Field(2).(*array.Uint8).Value(1) != math.MaxUint8 || arrowStruct.Field(3).(*array.Uint16).Value(1) != math.MaxUint16 || arrowStruct.Field(4).(*array.Float32).Value(0) != math.Nextafter32(1, 2) {
		t.Fatal("native Arrow round trip lost values")
	}
	for i := range fields {
		if !arrowStruct.Field(i).IsNull(2) || !arrowStruct.Field(i).IsNull(4) {
			t.Fatal("Arrow lost child NULL")
		}
	}
	if !arrowStruct.IsNull(3) || !arrowStruct.IsNull(5) {
		t.Fatal("Arrow lost parent NULL")
	}
	values.Int8s["i8"].Values[0] = 0
	values.Float32s["f32"].Values[0] = 0
	if clone.Values.Int8s["i8"].Values[0] != math.MinInt8 || clone.Values.Float32s["f32"].Values[0] != math.Nextafter32(1, 2) || arrowStruct.Field(0).(*array.Int8).Value(0) != math.MinInt8 || arrowStruct.Field(4).(*array.Float32).Value(0) != math.Nextafter32(1, 2) {
		t.Fatal("derived owned results alias JSON output")
	}
	out.jsonReservation.Release()
	out.jsonReservation = nil
	out.strct = nil
	if manager.Used() != 0 {
		t.Fatalf("JSON output leaked %d budget bytes", manager.Used())
	}
}

func TestJSONDecodeNativeRangeAndFloat32Semantics(t *testing.T) {
	for _, tc := range []struct {
		typ   memory.DataType
		value string
	}{
		{memory.TypeInt8, "-129"}, {memory.TypeInt8, "128"},
		{memory.TypeInt16, "-32769"}, {memory.TypeInt16, "32768"},
		{memory.TypeUint8, "-1"}, {memory.TypeUint8, "256"},
		{memory.TypeUint16, "-1"}, {memory.TypeUint16, "65536"},
		{memory.TypeFloat32, "3.5e38"}, {memory.TypeFloat32, "-3.5e38"},
	} {
		t.Run(fmt.Sprint(tc.typ, "/", tc.value), func(t *testing.T) {
			input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"x":` + tc.value + `}`}, nil)}
			var out scalarRegister
			if err := evalJSONDecode(&out, &input, []memory.Field{{Name: "x", Type: tc.typ}}, 1); err == nil {
				t.Fatal("out-of-range JSON accepted")
			}
			if out.strct != nil {
				t.Fatal("failed decode published partial result")
			}
		})
	}
	for _, tc := range []struct {
		value string
		bits  uint32
	}{
		{"-0", 0x80000000}, {"1e-100", 0}, {"1.401298464324817070923729583289916131280e-45", 1},
		{"1.000000059604644775390625", 0x3f800000},
		{"1.0000000596046447753906250000000001", 0x3f800001},
	} {
		t.Run(tc.value, func(t *testing.T) {
			input := scalarRegister{typ: memory.TypeString, strView: testStringVector([]string{`{"x":` + tc.value + `}`}, nil)}
			var out scalarRegister
			if err := evalJSONDecode(&out, &input, []memory.Field{{Name: "x", Type: memory.TypeFloat32}}, 1); err != nil {
				t.Fatal(err)
			}
			if got := math.Float32bits(out.strct.Values.Float32s["x"].Values[0]); got != tc.bits {
				t.Fatalf("bits=%08x want=%08x", got, tc.bits)
			}
		})
	}
}
