package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

func offsetOutputType(typ memory.DataType, function WindowFunction) memory.DataType {
	if function == WindowDiff {
		switch typ {
		case memory.TypeUint8:
			return memory.TypeInt16
		case memory.TypeUint16:
			return memory.TypeInt32
		case memory.TypeUint32, memory.TypeUint64:
			return memory.TypeInt64
		}
	}
	return typ
}

func offsetDifference(current, previous windowNumericCell, typ memory.DataType) windowNumericCell {
	if !current.valid || !previous.valid {
		return windowNumericCell{}
	}
	result := windowNumericCell{valid: true}
	switch {
	case windowSigned(typ):
		result.i64 = current.i64 - previous.i64
	case windowUnsigned(typ):
		// Polars converts UInt64 endpoints to Int64 before subtracting. Values
		// outside that domain become NULL, even if their difference would fit.
		if typ == memory.TypeUint64 && (current.u64 > math.MaxInt64 || previous.u64 > math.MaxInt64) {
			return windowNumericCell{}
		}
		result.i64 = int64(current.u64) - int64(previous.u64)
	default:
		result.f64 = current.f64 - previous.f64
	}
	return result
}

func evaluateOffsetWindow(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, window WindowExpression) error {
	field, ok := schema.Field(window.Column)
	if !ok {
		return fmt.Errorf("window column %q not found", window.Column)
	}
	typ := offsetOutputType(field.Type, window.Function)
	initializeOffsetOutput(output, window.Alias, typ, input.Rows)
	bound := window.Frame.Start
	for pos, row := range rows {
		if err := everyWindowRows(ctx, pos); err != nil {
			return err
		}
		target := int64(pos)
		switch bound.Kind {
		case WindowPreceding:
			if bound.Offset > target {
				continue
			}
			target -= bound.Offset
		case WindowFollowing:
			if bound.Offset >= int64(len(rows))-target {
				continue
			}
			target += bound.Offset
		case WindowCurrentRow:
		default:
			return fmt.Errorf("offset window requires finite single row frame")
		}
		value, err := readWindowNumericCell(input, schema, window, rows[int(target)])
		if err != nil {
			return err
		}
		if window.Function == WindowDiff {
			current, err := readWindowNumericCell(input, schema, window, row)
			if err != nil {
				return err
			}
			value = offsetDifference(current, value, field.Type)
		}
		setOffsetOutput(output, window.Alias, typ, row, value)
	}
	return nil
}

func initializeOffsetOutput(batch *memory.Batch, name string, typ memory.DataType, rows int) {
	switch typ {
	case memory.TypeInt8:
		if _, ok := batch.Int8s[name]; !ok {
			batch.Int8s[name] = &memory.Int8Vector{Values: make([]int8, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeInt16:
		if _, ok := batch.Int16s[name]; !ok {
			batch.Int16s[name] = &memory.Int16Vector{Values: make([]int16, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeInt32:
		if _, ok := batch.Int32s[name]; !ok {
			batch.Int32s[name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeInt64:
		if _, ok := batch.Int64s[name]; !ok {
			batch.Int64s[name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeUint8:
		if _, ok := batch.Uint8s[name]; !ok {
			batch.Uint8s[name] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeUint16:
		if _, ok := batch.Uint16s[name]; !ok {
			batch.Uint16s[name] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeUint32:
		if _, ok := batch.Uint32s[name]; !ok {
			batch.Uint32s[name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeUint64:
		if _, ok := batch.Uint64s[name]; !ok {
			batch.Uint64s[name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeFloat32:
		if _, ok := batch.Float32s[name]; !ok {
			batch.Float32s[name] = &memory.Float32Vector{Values: make([]float32, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	case memory.TypeFloat64:
		if _, ok := batch.Float64s[name]; !ok {
			batch.Float64s[name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: memory.EnsureBitmap(nil, rows)}
		}
	}
}

func setOffsetOutput(batch *memory.Batch, name string, typ memory.DataType, row int, value windowNumericCell) {
	if !value.valid {
		return
	}
	switch typ {
	case memory.TypeInt8:
		v := batch.Int8s[name]
		v.Values[row] = int8(value.i64)
		memory.Set(v.Validity, row)
	case memory.TypeInt16:
		v := batch.Int16s[name]
		v.Values[row] = int16(value.i64)
		memory.Set(v.Validity, row)
	case memory.TypeInt32:
		v := batch.Int32s[name]
		v.Values[row] = int32(value.i64)
		memory.Set(v.Validity, row)
	case memory.TypeInt64:
		v := batch.Int64s[name]
		v.Values[row] = int64(value.i64)
		memory.Set(v.Validity, row)
	case memory.TypeUint8:
		v := batch.Uint8s[name]
		v.Values[row] = uint8(value.u64)
		memory.Set(v.Validity, row)
	case memory.TypeUint16:
		v := batch.Uint16s[name]
		v.Values[row] = uint16(value.u64)
		memory.Set(v.Validity, row)
	case memory.TypeUint32:
		v := batch.Uint32s[name]
		v.Values[row] = uint32(value.u64)
		memory.Set(v.Validity, row)
	case memory.TypeUint64:
		v := batch.Uint64s[name]
		v.Values[row] = uint64(value.u64)
		memory.Set(v.Validity, row)
	case memory.TypeFloat32:
		v := batch.Float32s[name]
		v.Values[row] = float32(value.f64)
		memory.Set(v.Validity, row)
	case memory.TypeFloat64:
		v := batch.Float64s[name]
		v.Values[row] = float64(value.f64)
		memory.Set(v.Validity, row)
	}
}

func validateOffsetWindow(schema memory.Schema, e WindowExpression, output memory.Field) error {
	field, ok := schema.Field(e.Column)
	if !ok {
		return fmt.Errorf("offset window column %q not found", e.Column)
	}
	if !windowSigned(field.Type) && !windowUnsigned(field.Type) && field.Type != memory.TypeFloat32 && field.Type != memory.TypeFloat64 {
		return fmt.Errorf("offset window requires numeric input")
	}
	b := e.Frame.Start
	if b != e.Frame.End || b.Offset < 0 || (b.Kind != WindowCurrentRow && b.Kind != WindowPreceding && b.Kind != WindowFollowing) || (b.Kind == WindowCurrentRow && b.Offset != 0) {
		return fmt.Errorf("offset window requires a valid finite single row bound")
	}
	if output.Name != e.Alias || output.Type != offsetOutputType(field.Type, e.Function) || !output.Nullable {
		return fmt.Errorf("offset window output field disagrees with expression")
	}
	return nil
}
