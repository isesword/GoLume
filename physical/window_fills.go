package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

func isFillWindow(f WindowFunction) bool { return f == WindowForwardFill || f == WindowBackwardFill }

func needsFutureWindow(e WindowExpression) bool {
	return e.Function == WindowBackwardFill || ((e.Function == WindowShift || e.Function == WindowDiff) && e.Frame.Start.Kind == WindowFollowing)
}

func validateFillWindow(schema memory.Schema, e WindowExpression, output memory.Field) error {
	// The zero-offset contract also validates numeric input and output width.
	if e.Frame.Start.Kind != WindowCurrentRow || e.Frame.End.Kind != WindowCurrentRow {
		return fmt.Errorf("fill window uses a canonical CURRENT ROW frame")
	}
	return validateOffsetWindow(schema, e, output)
}

func evaluateFillWindow(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, e WindowExpression) error {
	field, _ := schema.Field(e.Column)
	initializeOffsetOutput(output, e.Alias, field.Type, input.Rows)
	value := windowNumericCell{}
	for position := range rows {
		if err := everyWindowRows(ctx, position); err != nil {
			return err
		}
		index := position
		if e.Function == WindowBackwardFill {
			index = len(rows) - 1 - position
		}
		row := rows[index]
		cell, err := readWindowNumericCell(input, schema, e, row)
		if err != nil {
			return err
		}
		if cell.valid {
			value = cell
		}
		setOffsetOutput(output, e.Alias, field.Type, row, value)
	}
	return nil
}

// backwardSeed finds the first valid value at/after this batch's last row, or
// its partition boundary. Search ordinals only advance, so long NULL runs are
// scanned once per expression rather than once per pending output batch.
func (s *futureWindowStream) backwardSeed(ctx context.Context, node *futureWindowBatch, index int) (windowNumericCell, error) {
	e := s.owner.expressions[index]
	target := node.end - 1
	position := s.fillPositions[index]
	if position < target {
		position = target
	}
	for {
		if err := ctx.Err(); err != nil {
			return windowNumericCell{}, err
		}
		for position >= s.end && !s.done {
			if err := s.load(ctx); err != nil {
				return windowNumericCell{}, err
			}
		}
		s.fillPositions[index] = position
		batch, row, exists := s.locate(index, position)
		if !exists {
			return windowNumericCell{}, nil
		}
		same, err := s.samePartition(node.batch, node.batch.Rows-1, batch, row)
		if err != nil || !same {
			return windowNumericCell{}, err
		}
		value, err := readWindowNumericCell(batch, s.owner.input.Schema(), e, row)
		if err != nil || value.valid {
			return value, err
		}
		position++ // position < s.end, which is checked against MaxInt64 by load.
	}
}

func (s *futureWindowStream) fillBackwardBatch(ctx context.Context, node *futureWindowBatch, index int) error {
	value, err := s.backwardSeed(ctx, node, index)
	if err != nil {
		return err
	}
	e := s.owner.expressions[index]
	for row := node.batch.Rows - 1; row >= 0; row-- {
		if err := everyWindowRows(ctx, row); err != nil {
			return err
		}
		if row+1 < node.batch.Rows {
			same, err := s.samePartition(node.batch, row, node.batch, row+1)
			if err != nil {
				return err
			}
			if !same {
				value = windowNumericCell{}
			}
		}
		current, err := readWindowNumericCell(node.batch, s.owner.input.Schema(), e, row)
		if err != nil {
			return err
		}
		if current.valid {
			value = current
		}
		setOffsetOutput(node.batch, s.owner.names[index], s.owner.fields[index].Type, row, value)
	}
	return nil
}
