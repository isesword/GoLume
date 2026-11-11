package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

// finiteRowsAggregate is deliberately narrower than all window aggregates:
// extending floating Sum/Avg requires a separate accumulation contract.
func finiteRowsAggregate(e WindowExpression) bool {
	switch e.Function {
	case WindowCountRows, WindowCountValues, WindowMin, WindowMax:
	default:
		return false
	}
	finite := func(b WindowBound) bool {
		return b.Kind == WindowPreceding || b.Kind == WindowCurrentRow || b.Kind == WindowFollowing
	}
	return finite(e.Frame.Start) && finite(e.Frame.End)
}

func needsFiniteRowsWindow(e WindowExpression) bool {
	return finiteRowsAggregate(e) && (e.Frame.End.Kind != WindowCurrentRow || e.Frame.Start.Kind == WindowFollowing)
}

// A trailing ring computes a finite frame at its right endpoint. For a past
// endpoint, delay owns numeric cells until they enter the ring. For a future
// endpoint, the existing owned lookahead supplies cells. Missing partition
// rows enter as invalid cells, so clipping also expires values at the tail.
// No borrowed vector or row reference survives a child Next call.
type finiteRowsState struct {
	aggregate    rollingWindowState
	delay        windowCellRing
	start, end   int64
	keys         []SortKey
	previous     rowCandidate
	havePrevious bool
	keyBytes     int64
	keyLease     memory.Reservation
}

func newFiniteRowsState(e WindowExpression, input, output memory.DataType, partitionBy []string) (*finiteRowsState, int64, error) {
	start, err := countWindowBoundOrder(e.Frame.Start)
	if err != nil {
		return nil, 0, err
	}
	end, err := countWindowBoundOrder(e.Frame.End)
	if err != nil {
		return nil, 0, err
	}
	// Subtraction is safe only after checking a frame that straddles zero.
	if start > end || (start < 0 && end > math.MaxInt64+start-1) {
		return nil, 0, fmt.Errorf("window %q finite frame width overflows int64", e.Alias)
	}
	width := end - start
	if width >= int64(platformMaxInt()) {
		return nil, 0, fmt.Errorf("window %q finite frame exceeds platform capacity", e.Alias)
	}
	width++
	delay := int64(0)
	if end < 0 {
		delay = -end
	}
	if delay > int64(platformMaxInt()) {
		return nil, 0, fmt.Errorf("window %q preceding endpoint exceeds platform capacity", e.Alias)
	}
	// CountRows uses the same validity ring as CountValues: a real row is valid,
	// while a synthetic row outside the current partition is invalid.
	function := e.Function
	if function == WindowCountRows {
		function = WindowCountValues
	}
	aggregate := rollingWindowState{expression: WindowExpression{Function: function}, inputType: input, outputType: output, width: int(width)}
	cells := width
	bytes, err := futureBudgetAdd(1024, cells, windowStateCellBytes)
	if err != nil {
		return nil, 0, err
	}
	bytes, err = futureBudgetAdd(bytes, delay, windowStateCellBytes)
	if err != nil {
		return nil, 0, err
	}
	bytes, err = futureBudgetAdd(bytes, int64(len(partitionBy)), 256)
	if err != nil {
		return nil, 0, err
	}
	// Ring and key allocations happen only after the caller admits all state.
	return &finiteRowsState{aggregate: aggregate, start: start, end: end}, bytes, nil
}

func (s *finiteRowsState) initialize(partitionBy []string) {
	if s.aggregate.expression.Function == WindowMin || s.aggregate.expression.Function == WindowMax {
		s.aggregate.extrema = newWindowCellRing(s.aggregate.width)
	} else {
		s.aggregate.queue = newWindowCellRing(s.aggregate.width)
	}
	if s.end < 0 {
		s.delay = newWindowCellRing(int(-s.end))
	}
	for _, name := range partitionBy {
		s.keys = append(s.keys, SortKey{Column: name})
	}
}

// advance skips absent partition rows in one step. Only valid numeric cells
// occupy the ring/deque, so short partitions do not do O(frame-width) work.
func (s *finiteRowsState) advance(cell windowNumericCell, steps int64) error {
	a := &s.aggregate
	if steps <= 0 || a.position > math.MaxInt64-steps {
		return fmt.Errorf("window finite frame position overflows int64")
	}
	a.position += steps
	cell.seq = a.position - 1
	cutoff := cell.seq - int64(a.width)
	if a.expression.Function == WindowCountValues {
		for a.queue.len > 0 && a.queue.front().seq <= cutoff {
			a.queue.popFront()
		}
		if cell.valid {
			a.queue.pushBack(cell)
		}
		a.validCount = int64(a.queue.len)
		return nil
	}
	for a.extrema.len > 0 && a.extrema.front().seq <= cutoff {
		a.extrema.popFront()
	}
	if cell.valid {
		minimum := a.expression.Function == WindowMin
		for a.extrema.len > 0 && windowCellBetter(cell, a.extrema.back(), a.inputType, minimum) {
			a.extrema.popBack()
		}
		a.extrema.pushBack(cell)
	}
	a.hasBest = a.extrema.len > 0
	if a.hasBest {
		a.best = a.extrema.front()
	}
	return nil
}

func (s *finiteRowsState) reset() { s.aggregate.reset(); s.delay.reset() }
func (s *finiteRowsState) close() {
	if s.keyLease != nil {
		s.keyLease.Release()
	}
	*s = finiteRowsState{}
}

func (s *finiteRowsState) partition(ctx context.Context, owner *futureWindowSource, batch *memory.Batch, row int) (bool, error) {
	schema := owner.input.Schema()
	changed := !s.havePrevious
	if s.havePrevious {
		same, err := batchRowEqualsCandidate(batch, schema, s.keys, row, s.previous)
		if err != nil {
			return false, err
		}
		changed = !same
	}
	// Charge retained capacity per key, not just the largest sum of current
	// lengths: different columns can reach their high-water marks on different rows.
	var bytes int64
	for index, key := range s.keys {
		cell, err := snapshotCell(batch, schema, key.Column, row)
		if err != nil {
			return false, err
		}
		retained := int64(0)
		if index < len(s.previous.cells) {
			retained = int64(cap(s.previous.cells[index].text))
		}
		growth, err := futureBudgetAdd(0, int64(len(cell.text)), 4)
		if err != nil {
			return false, err
		}
		bytes, err = futureBudgetAdd(bytes, max(retained, growth), 1)
		if err != nil {
			return false, err
		}
	}
	if bytes > s.keyBytes {
		if owner.manager != nil {
			lease, err := owner.manager.Reserve(ctx, bytes)
			if err != nil {
				return false, fmt.Errorf("window finite frame partition keys: %w", err)
			}
			if s.keyLease != nil {
				s.keyLease.Release()
			}
			s.keyLease = lease
		}
		s.keyBytes = bytes
	}
	if err := snapshotCandidateInto(&s.previous, batch, schema, s.keys, row, 0); err != nil {
		return false, err
	}
	s.havePrevious = true
	return changed, nil
}

func (s *futureWindowStream) finiteRowsCell(node *futureWindowBatch, index, row int, offset int64) (windowNumericCell, bool, error) {
	ordinal := node.start + int64(row)
	if offset > math.MaxInt64-ordinal {
		return windowNumericCell{}, false, nil
	}
	target, tr, found := s.locate(index, ordinal+offset)
	if !found {
		return windowNumericCell{}, false, nil
	}
	same, err := s.samePartition(node.batch, row, target, tr)
	if err != nil || !same {
		return windowNumericCell{}, false, err
	}
	cell, err := readWindowNumericCell(target, s.owner.input.Schema(), s.owner.expressions[index], tr)
	return cell, true, err
}

func (s *futureWindowStream) finiteRowsBatch(ctx context.Context, node *futureWindowBatch, index int) error {
	state := s.finiteRows[index]
	e := s.owner.expressions[index]
	name := s.owner.names[index]
	typ := s.owner.fields[index].Type
	for row := 0; row < node.batch.Rows; row++ {
		if err := everyWindowRows(ctx, row); err != nil {
			return err
		}
		changed, err := state.partition(ctx, s.owner, node.batch, row)
		if err != nil {
			return err
		}
		if changed {
			state.reset()
		}
		if state.end < 0 {
			cell, err := readWindowNumericCell(node.batch, s.owner.input.Schema(), e, row)
			if err != nil {
				return err
			}
			old, full := state.delay.push(cell)
			if !full {
				old = windowNumericCell{}
			}
			if err := state.advance(old, 1); err != nil {
				return err
			}
		} else if changed {
			// Only seed the initial clipped frame, never scan a skipped positive prefix.
			offset := max(int64(0), state.start)
			for ; offset <= state.end; offset++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				cell, exists, err := s.finiteRowsCell(node, index, row, offset)
				if err != nil {
					return err
				}
				if !exists {
					if err := state.advance(windowNumericCell{}, state.end-offset+1); err != nil {
						return err
					}
					break
				}
				if err := state.advance(cell, 1); err != nil {
					return err
				}
			}
		} else {
			cell, _, err := s.finiteRowsCell(node, index, row, state.end)
			if err != nil {
				return err
			}
			if err := state.advance(cell, 1); err != nil {
				return err
			}
		}
		value := windowNumericCell{}
		if e.Function == WindowCountRows || e.Function == WindowCountValues {
			value = windowNumericCell{valid: true, i64: state.aggregate.validCount}
		} else if state.aggregate.hasBest {
			value = state.aggregate.best
		}
		setOffsetOutput(node.batch, name, typ, row, value)
	}
	return nil
}
