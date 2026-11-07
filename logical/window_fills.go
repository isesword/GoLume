package logical

import (
	"fmt"
	"gocolumn/memory"
)

func inferFillWindowField(schema memory.Schema, window WindowExpr) (memory.Field, error) {
	field, ok := schema.Field(window.Column)
	if !ok {
		return memory.Field{}, fmt.Errorf("fill window column %q not found", window.Column)
	}
	if !windowNumeric(field.Type) {
		return memory.Field{}, fmt.Errorf("fill window requires numeric input, got %s", field.Type)
	}
	if window.Frame.Start.Kind != RowCurrent || window.Frame.End.Kind != RowCurrent || window.Frame.Start.Offset != 0 || window.Frame.End.Offset != 0 {
		return memory.Field{}, fmt.Errorf("fill window uses a canonical CURRENT ROW frame; direction is determined by the function")
	}
	return memory.Field{Name: window.Alias, Type: field.Type, Nullable: true, Encoding: memory.EncodingPlain}, nil
}
