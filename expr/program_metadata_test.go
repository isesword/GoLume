package expr

import (
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestProgramResultMetadataRemainsIndependent(t *testing.T) {
	if got := (Program{}).ResultField(); !reflect.DeepEqual(got, memory.Field{}) {
		t.Fatalf("zero program metadata=%+v", got)
	}
	field := memory.StructField("record", true,
		memory.Field{Name: "count", Type: memory.TypeInt64},
		memory.ListStringField("tags", true, true))
	schema := memory.NewSchema(field)
	program, err := Compile(schema, Col("record"))
	if err != nil {
		t.Fatal(err)
	}
	want := program.ResultField()
	copyOfProgram := program
	result := program.ResultField()
	result.Name = "changed"
	result.Children[0].Type = memory.TypeFloat64
	result.Children[1].Children[0].Nullable = false
	schema.Fields[0].Children[0].Name = "changed source"
	for _, p := range []Program{program, copyOfProgram} {
		if got := p.ResultField(); !reflect.DeepEqual(got, want) {
			t.Fatalf("shared immutable result changed: got=%+v want=%+v", got, want)
		}
	}
}
