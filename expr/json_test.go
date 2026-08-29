package expr

import (
	"context"
	"errors"
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
	program, err := CompileScalar(schema, JSONDecode(Col("payload"), target))
	if err != nil {
		t.Fatal(err)
	}
	got, err := program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
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
	if got.Struct.Values.Int32s["id"].Values[0] != 7 || got.Struct.Values.Uint32s["active"].Values[0] != 1 {
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
			program, err := CompileScalar(schema, JSONDecode(Col("payload"), memory.NewSchema(tc.field)))
			if err != nil {
				t.Fatal(err)
			}
			_, err = program.Evaluate(batch, &ScalarWorkspace{}, testKernelSet())
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
	program, err := CompileScalar(schema, JSONDecode(Col("payload"), memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingView})))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(4)
	workspace := &ScalarWorkspace{}
	workspace.SetMemoryManager(context.Background(), manager)
	if _, err := program.Evaluate(batch, workspace, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("workspace leaked %d bytes", manager.Used())
	}
}
