package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

func isVarianceWindow(f WindowFunction) bool { return f == WindowVarPop || f == WindowVarSamp }
func windowsHaveVariance(expressions []WindowExpression) bool {
	for _, e := range expressions {
		if isVarianceWindow(e.Function) {
			return true
		}
	}
	return false
}

func validateVarianceWindow(schema memory.Schema, e WindowExpression, out memory.Field) error {
	f, ok := schema.Field(e.Column)
	if e.Column == "" || !ok || !(windowSigned(f.Type) || windowUnsigned(f.Type) || windowFloat(f.Type)) {
		return fmt.Errorf("variance requires a native numeric input column")
	}
	if e.Alias == "" || out.Name != e.Alias || out.Type != memory.TypeFloat64 || !out.Nullable || !canonicalNTileField(out) {
		return fmt.Errorf("variance requires matching canonical nullable Float64 output")
	}
	start, err := countWindowBoundOrder(e.Frame.Start)
	if err != nil {
		return err
	}
	end, err := countWindowBoundOrder(e.Frame.End)
	if err != nil {
		return err
	}
	if start > end {
		return fmt.Errorf("variance frame start is after end")
	}
	if e.Frame.Start.Kind == WindowUnboundedPreceding && e.Frame.End.Kind == WindowCurrentRow {
		return nil
	}
	finite := func(b WindowBound) bool {
		return b.Kind == WindowPreceding || b.Kind == WindowCurrentRow || b.Kind == WindowFollowing
	}
	if !finite(e.Frame.Start) || !finite(e.Frame.End) {
		return fmt.Errorf("variance requires finite ROWS or UNBOUNDED PRECEDING through CURRENT ROW")
	}
	// Match the bounded execution ring's capacity arithmetic at every API mode.
	if start < 0 && end > math.MaxInt64+start-1 {
		return fmt.Errorf("variance frame width overflows int64")
	}
	if end-start >= int64(platformMaxInt()) {
		return fmt.Errorf("variance frame exceeds platform capacity")
	}
	return nil
}

// The caller has admitted one moment object and all result scratch. Partition
// bounds are monotone; every row enters and leaves the moments at most once.
func evaluateResidentVarianceState(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, e WindowExpression, state *windowExactMoments) error {
	*state = windowExactMoments{}
	typ, _ := fieldType(schema, e.Column)
	values, validity := windowNullableF64(output, e.Alias, input.Rows)
	left, right := 0, 0
	update := func(pos int, direction int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		cell, err := readWindowNumericCell(input, schema, e, rows[pos])
		if err != nil {
			return err
		}
		return state.addCell(typ, cell, direction)
	}
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		lo, hi := windowFrameRange(i, len(rows), e.Frame)
		end := hi + 1
		if end < lo {
			end = lo
		}
		for left < lo && left < right {
			if err := update(left, -1); err != nil {
				return err
			}
			left++
		}
		if right < lo {
			left, right = lo, lo
		}
		for right < end {
			if err := update(right, 1); err != nil {
				return err
			}
			right++
		}
		v, valid, err := momentResult(state, e.Function)
		if err != nil {
			return err
		}
		values[row] = v
		if valid {
			memory.Set(validity, row)
		}
	}
	output.Float64s[e.Alias] = &memory.Float64Vector{Values: values, Validity: validity}
	return ctx.Err()
}

// isMomentWindow broadens execution only; isVarianceWindow retains its original meaning.
func isMomentWindow(f WindowFunction) bool {
	return isVarianceWindow(f) || f == WindowStdDevPop || f == WindowStdDevSamp
}
func windowsHaveMoments(expressions []WindowExpression) bool {
	for _, e := range expressions {
		if isMomentWindow(e.Function) {
			return true
		}
	}
	return false
}

func momentResult(state *windowExactMoments, function WindowFunction) (float64, bool, error) {
	switch function {
	case WindowVarPop:
		return state.result(false)
	case WindowVarSamp:
		return state.result(true)
	case WindowStdDevPop:
		return state.stddev(false)
	case WindowStdDevSamp:
		return state.stddev(true)
	default:
		return 0, false, fmt.Errorf("function %d does not use exact moments", function)
	}
}
