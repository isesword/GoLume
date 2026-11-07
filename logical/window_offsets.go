package logical

import (
	"fmt"
	"gocolumn/memory"
)

func inferOffsetWindowField(schema memory.Schema, window WindowExpr) (memory.Field, error) {
	input, ok := schema.Field(window.Column)
	if !ok {
		return memory.Field{}, fmt.Errorf("window column %q not found", window.Column)
	}
	if !windowNumeric(input.Type) {
		return memory.Field{}, fmt.Errorf("offset window column %q must be numeric, got %s", window.Column, input.Type)
	}
	if window.Frame.Start != window.Frame.End {
		return memory.Field{}, fmt.Errorf("offset window requires a single row bound")
	}
	switch window.Frame.Start.Kind {
	case RowPreceding, RowCurrent, RowFollowing:
	default:
		return memory.Field{}, fmt.Errorf("offset window requires a finite bound")
	}
	if err := validateWindowFrame(window.Frame); err != nil {
		return memory.Field{}, err
	}
	typ := input.Type
	if window.Function == WindowDiff {
		switch typ {
		case memory.TypeUint8:
			typ = memory.TypeInt16
		case memory.TypeUint16:
			typ = memory.TypeInt32
		case memory.TypeUint32, memory.TypeUint64:
			typ = memory.TypeInt64
		}
	}
	return memory.Field{Name: window.Alias, Type: typ, Nullable: true, Encoding: memory.EncodingPlain}, nil
}
