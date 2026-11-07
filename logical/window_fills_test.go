package logical

import (
	"gocolumn/memory"
	"testing"
)

func TestDirectionalFillSchemaValidation(t *testing.T) {
	frame := RowsFrame{Start: RowBound{Kind: RowCurrent}, End: RowBound{Kind: RowCurrent}}
	for _, function := range []WindowFunction{WindowForwardFill, WindowBackwardFill} {
		expression := WindowExpr{Function: function, Column: "v", Alias: "f", Frame: frame}
		for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary, memory.TypeStruct, memory.TypeList} {
			if _, err := inferWindowField(memory.NewSchema(memory.Field{Name: "v", Type: typ}), expression); err == nil {
				t.Fatalf("accepted %s", typ)
			}
		}
		schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8})
		field, err := inferWindowField(schema, expression)
		if err != nil || field.Type != memory.TypeInt8 || !field.Nullable {
			t.Fatal(field, err)
		}
		expression.Column = "missing"
		if _, err := inferWindowField(schema, expression); err == nil {
			t.Fatal("missing column accepted")
		}
		expression.Column = "v"
		expression.Frame.Start = RowBound{Kind: RowPreceding, Offset: 1}
		if _, err := inferWindowField(schema, expression); err == nil {
			t.Fatal("custom frame accepted")
		}
	}
}
