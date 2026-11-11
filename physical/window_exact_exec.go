package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

// An immutable plan groups exact outputs by their original input and frame.
// It belongs to the source, not an execution: concurrent streams share only
// these descriptors, never accumulators, rings or borrowed vector bindings.
type windowExactPlan struct {
	leader    int
	indices   []int
	column    string
	frame     WindowFrame
	inputType memory.DataType
	single    bool
}

func canonicalExactFrame(frame WindowFrame) WindowFrame {
	canonical := func(bound WindowBound) WindowBound {
		if bound.Kind == WindowCurrentRow || (bound.Offset == 0 && (bound.Kind == WindowPreceding || bound.Kind == WindowFollowing)) {
			return WindowBound{Kind: WindowCurrentRow}
		}
		return bound
	}
	return WindowFrame{Start: canonical(frame.Start), End: canonical(frame.End)}
}

func planExactWindows(schema memory.Schema, expressions []WindowExpression) []*windowExactPlan {
	type key struct {
		column string
		frame  WindowFrame
		typ    memory.DataType
	}
	var plans []*windowExactPlan
	var groups map[key]*windowExactPlan
	for index, expression := range expressions {
		typ, _ := fieldType(schema, expression.Column)
		if !usesExactWindowSum(expression.Function, typ) {
			continue
		}
		if plans == nil {
			plans = make([]*windowExactPlan, len(expressions))
			groups = make(map[key]*windowExactPlan)
		}
		frame := canonicalExactFrame(expression.Frame)
		k := key{expression.Column, frame, typ}
		plan := groups[k]
		if plan == nil {
			single := frame.Start == frame.End && (frame.Start.Kind == WindowCurrentRow || (frame.Start.Offset >= 0 && (frame.Start.Kind == WindowPreceding || frame.Start.Kind == WindowFollowing)))
			plan = &windowExactPlan{leader: index, column: expression.Column, frame: frame, inputType: typ, single: single}
			groups[k] = plan
		}
		plan.indices = append(plan.indices, index)
		plans[index] = plan
	}
	return plans
}

// Bind once per input batch. Values are borrowed only during the caller's
// evaluation and must be cleared before it returns or asks its child for more.
// The common Float64 path avoids a type switch and map/schema lookup per row.
type windowExactInput struct {
	f64   *memory.Float64Vector
	other any
}

func bindWindowExactInput(batch *memory.Batch, column string, typ memory.DataType) (windowExactInput, error) {
	var binding windowExactInput
	var err error
	switch typ {
	case memory.TypeFloat64:
		binding.f64, err = batch.Float64(column)
	case memory.TypeFloat32:
		binding.other, err = batch.Float32(column)
	case memory.TypeInt8:
		binding.other, err = batch.Int8(column)
	case memory.TypeInt16:
		binding.other, err = batch.Int16(column)
	case memory.TypeInt32:
		binding.other, err = batch.Int32(column)
	case memory.TypeInt64:
		binding.other, err = batch.Int64(column)
	case memory.TypeUint8:
		binding.other, err = batch.Uint8(column)
	case memory.TypeUint16:
		binding.other, err = batch.Uint16(column)
	case memory.TypeUint32:
		binding.other, err = batch.Uint32(column)
	case memory.TypeUint64:
		binding.other, err = batch.Uint64(column)
	default:
		err = fmt.Errorf("window column %q type %s is not numeric", column, typ)
	}
	return binding, err
}

func (b windowExactInput) cell(row int) windowNumericCell {
	if b.f64 != nil {
		return windowNumericCell{valid: b.f64.IsValid(row), f64: b.f64.Values[row]}
	}
	switch v := b.other.(type) {
	case *memory.Float32Vector:
		return windowNumericCell{valid: v.IsValid(row), f64: float64(v.Values[row])}
	case *memory.Int8Vector:
		return windowNumericCell{valid: v.IsValid(row), i64: int64(v.Values[row])}
	case *memory.Int16Vector:
		return windowNumericCell{valid: v.IsValid(row), i64: int64(v.Values[row])}
	case *memory.Int32Vector:
		return windowNumericCell{valid: v.IsValid(row), i64: int64(v.Values[row])}
	case *memory.Int64Vector:
		return windowNumericCell{valid: v.IsValid(row), i64: v.Values[row]}
	case *memory.Uint8Vector:
		return windowNumericCell{valid: v.IsValid(row), u64: uint64(v.Values[row])}
	case *memory.Uint16Vector:
		return windowNumericCell{valid: v.IsValid(row), u64: uint64(v.Values[row])}
	case *memory.Uint32Vector:
		return windowNumericCell{valid: v.IsValid(row), u64: uint64(v.Values[row])}
	case *memory.Uint64Vector:
		return windowNumericCell{valid: v.IsValid(row), u64: v.Values[row]}
	default:
		panic("unbound exact window input")
	}
}

// A frame containing one value needs no addition or division. Native integer
// conversion rounds that exact integer once. Exact zero is always positive,
// including a floating input of -0; NaN payload preservation is unspecified.
func singleExactWindowValue(cell windowNumericCell, typ memory.DataType) float64 {
	var value float64
	switch {
	case windowSigned(typ):
		value = float64(cell.i64)
	case windowUnsigned(typ):
		value = float64(cell.u64)
	default:
		value = cell.f64
	}
	if value == 0 {
		return 0
	}
	return value
}

func addExactWindowCell(total *windowExactSum, typ memory.DataType, cell windowNumericCell, direction int64) {
	switch {
	case windowSigned(typ):
		total.addSigned(cell.i64, direction)
	case windowUnsigned(typ):
		total.addUnsigned(cell.u64, direction)
	default:
		total.addFloat(cell.f64, direction)
	}
}

type residentExactTarget struct {
	vector  *memory.Float64Vector
	average bool
}

type residentExactWindow struct {
	input   windowExactInput
	plan    *windowExactPlan
	targets []residentExactTarget
	needSum bool
	needAvg bool
}

func bindResidentExactWindow(input *memory.Batch, output *memory.Batch, plan *windowExactPlan, expressions []WindowExpression) (*residentExactWindow, error) {
	state := &residentExactWindow{plan: plan}
	if input.Rows != 0 {
		binding, err := bindWindowExactInput(input, plan.column, plan.inputType)
		if err != nil {
			return nil, err
		}
		state.input = binding
	}
	state.targets = make([]residentExactTarget, len(plan.indices))
	for index, expressionIndex := range plan.indices {
		expression := expressions[expressionIndex]
		values, validity := windowNullableF64(output, expression.Alias, input.Rows)
		vector := &memory.Float64Vector{Values: values, Validity: validity}
		output.Float64s[expression.Alias] = vector
		average := expression.Function == WindowAvg
		state.targets[index] = residentExactTarget{vector: vector, average: average}
		state.needSum = state.needSum || !average
		state.needAvg = state.needAvg || average
	}
	return state, nil
}

func (s *residentExactWindow) write(row int, sum, average float64) {
	for _, target := range s.targets {
		value := sum
		if target.average {
			value = average
		}
		target.vector.Values[row] = value
		memory.Set(target.vector.Validity, row)
	}
}

func (s *residentExactWindow) evaluate(ctx context.Context, rows []int) error {
	if s.plan.single {
		for position, row := range rows {
			if err := everyWindowRows(ctx, position); err != nil {
				return err
			}
			lo, hi := windowFrameRange(position, len(rows), s.plan.frame)
			if lo > hi {
				continue
			}
			cell := s.input.cell(rows[lo])
			if cell.valid {
				value := singleExactWindowValue(cell, s.plan.inputType)
				s.write(row, value, value)
			}
		}
		return nil
	}
	var total windowExactSum
	var count int64
	left, right := 0, 0
	for position, row := range rows {
		if err := everyWindowRows(ctx, position); err != nil {
			return err
		}
		lo, hi := windowFrameRange(position, len(rows), s.plan.frame)
		end := hi + 1
		if lo >= end {
			total, count = windowExactSum{}, 0
			left, right = lo, lo
			continue
		}
		for left < lo && left < right {
			if err := everyWindowRows(ctx, left); err != nil {
				return err
			}
			cell := s.input.cell(rows[left])
			if cell.valid {
				addExactWindowCell(&total, s.plan.inputType, cell, -1)
				count--
			}
			left++
		}
		if right < lo {
			left, right = lo, lo
		}
		for right < end {
			if err := everyWindowRows(ctx, right); err != nil {
				return err
			}
			cell := s.input.cell(rows[right])
			if cell.valid {
				addExactWindowCell(&total, s.plan.inputType, cell, 1)
				count++
			}
			right++
		}
		if count != 0 {
			var sum, average float64
			if s.needSum {
				sum = total.result(1)
			}
			if s.needAvg {
				average = total.result(count)
			}
			s.write(row, sum, average)
		}
	}
	return nil
}
