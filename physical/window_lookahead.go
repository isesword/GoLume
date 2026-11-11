package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
)

// futureWindowSource owns a bounded delayed input, not a replay cursor. Its
// private columns carry already-computed future results to the ordinary window
// evaluator, which reads them as zero-offset shifts and removes their names.
type futureWindowSource struct {
	input       source.Source
	schema      memory.Schema
	expressions []WindowExpression
	fields      []memory.Field
	names       []string
	partitionBy []string
	maxLead     int64
	manager     memory.Manager
}

// Schema returns the input and private numeric result fields.
func (s *futureWindowSource) Schema() memory.Schema { return s.schema }

// Stats forwards input row counts.
func (s *futureWindowSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

// Partitions forwards input partition metadata.
func (s *futureWindowSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// StreamPlan reports bounded owned lookahead.
func (s *futureWindowSource) StreamPlan() source.StreamPlanNode {
	detail := fmt.Sprintf("max-future-offset=%d", s.maxLead)
	for _, e := range s.expressions {
		if e.Function == WindowBackwardFill {
			detail += "; backward-fill-retention-limited-by-memory-budget"
			break
		}
	}
	return source.StreamPlanNode{Operator: "WindowLookahead", Class: source.StreamBounded, Detail: detail, Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)}}
}

// NewBatchStream opens one input stream and its admitted lookahead state.
func (s *futureWindowSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		return nil, err
	}
	st := &futureWindowStream{owner: s, child: child, scratch: memory.NewBatch()}
	bytes := int64(2048) + int64(len(s.expressions))*1024
	// Describe and admit rings before allocating their backing storage.
	st.finiteRows = make([]*finiteRowsState, len(s.expressions))
	for index, e := range s.expressions {
		if !needsFiniteRowsWindow(e) {
			continue
		}
		typ, _ := fieldType(s.input.Schema(), e.Column)
		state, stateBytes, stateErr := newFiniteRowsState(e, typ, s.fields[index].Type, s.partitionBy)
		if stateErr != nil {
			child.Close()
			return nil, stateErr
		}
		bytes, stateErr = futureBudgetAdd(bytes, stateBytes, 1)
		if stateErr != nil {
			child.Close()
			return nil, stateErr
		}
		st.finiteRows[index] = state
	}
	if s.manager != nil {
		st.base, err = s.manager.Reserve(ctx, bytes)
		if err != nil {
			child.Close()
			return nil, err
		}
	}
	for _, state := range st.finiteRows {
		if state != nil {
			state.initialize(s.partitionBy)
		}
	}
	st.cursors = make([]*futureWindowBatch, len(s.expressions))
	st.fillPositions = make([]int64, len(s.expressions))
	return st, nil
}

type futureWindowBatch struct {
	batch      *memory.Batch
	lease      memory.Reservation
	start, end int64
	next       *futureWindowBatch
}
type futureWindowStream struct {
	owner               *futureWindowSource
	child               source.BatchStream
	scratch             *memory.Batch
	head, tail, current *futureWindowBatch
	cursors             []*futureWindowBatch
	fillPositions       []int64
	finiteRows          []*finiteRowsState
	base, output        memory.Reservation
	end                 int64
	done, closed        bool
}

func (s *futureWindowStream) releaseCurrent() {
	if s.current != nil {
		for i, c := range s.cursors {
			if c == s.current {
				s.cursors[i] = s.current.next
			}
		}
		s.current.batch.ReleaseOwned()
		if s.current.lease != nil {
			s.current.lease.Release()
		}
		s.current = nil
	}
	if s.output != nil {
		s.output.Release()
		s.output = nil
	}
}
func (s *futureWindowStream) load(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ok, err := s.child.Next(ctx, s.scratch)
	if err != nil {
		return err
	}
	if !ok {
		s.done = true
		return nil
	}
	if s.scratch.Rows == 0 {
		return nil
	}
	if int64(s.scratch.Rows) > math.MaxInt64-s.end {
		return fmt.Errorf("window lookahead row ordinal overflow")
	}
	bytes, err := futureCloneAdmissionBytes(s.scratch)
	if err != nil {
		return err
	}
	var lease memory.Reservation
	if s.owner.manager != nil {
		lease, err = s.owner.manager.Reserve(ctx, bytes)
		if err != nil {
			return fmt.Errorf("window lookahead retained input: %w", err)
		}
	}
	cloned, err := memory.CloneBatchRange(s.scratch, 0, s.scratch.Rows)
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return err
	}
	node := &futureWindowBatch{batch: cloned, lease: lease, start: s.end, end: s.end + int64(cloned.Rows)}
	s.end = node.end
	if s.tail == nil {
		s.head = node
	} else {
		s.tail.next = node
	}
	s.tail = node
	return nil
}
func (s *futureWindowStream) locate(index int, target int64) (*memory.Batch, int, bool) {
	node := s.cursors[index]
	if node == nil || target < node.start {
		node = s.head
	}
	for node != nil && target >= node.end {
		node = node.next
	}
	s.cursors[index] = node
	if node == nil || target < node.start {
		return nil, 0, false
	}
	return node.batch, int(target - node.start), true
}
func (s *futureWindowStream) samePartition(a *memory.Batch, ar int, b *memory.Batch, br int) (bool, error) {
	schema := s.owner.input.Schema()
	for _, name := range s.owner.partitionBy {
		x, err := snapshotCell(a, schema, name, ar)
		if err != nil {
			return false, err
		}
		y, err := snapshotCell(b, schema, name, br)
		if err != nil {
			return false, err
		}
		if compareSortCells(x, y, SortKey{Column: name}) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Next emits an owned delayed batch with private future results.
func (s *futureWindowStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	dst.ClearColumns()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	s.releaseCurrent()
	defer func() {
		if err != nil {
			_ = s.Close()
			dst.ClearColumns()
		}
	}()
	for !s.done && (s.head == nil || s.end-s.head.end < s.owner.maxLead) {
		if err = s.load(ctx); err != nil {
			return false, err
		}
	}
	if s.head == nil {
		return false, nil
	}
	node := s.head
	bytes, err := estimateWindowOutputBytes(s.owner.schema, s.owner.fields, node.batch.Rows)
	if err != nil {
		return false, err
	}
	bytes, err = futureBudgetAdd(bytes, int64(len(s.owner.fields)), 512)
	if err != nil {
		return false, err
	}
	if s.owner.manager != nil {
		s.output, err = s.owner.manager.Reserve(ctx, bytes)
		if err != nil {
			return false, fmt.Errorf("window lookahead output: %w", err)
		}
	}
	for i, e := range s.owner.expressions {
		name := s.owner.names[i]
		typ := s.owner.fields[i].Type
		initializeOffsetOutput(node.batch, name, typ, node.batch.Rows)
		if needsFiniteRowsWindow(e) {
			if err = s.finiteRowsBatch(ctx, node, i); err != nil {
				return false, err
			}
			continue
		}
		if e.Function == WindowBackwardFill {
			if err = s.fillBackwardBatch(ctx, node, i); err != nil {
				return false, err
			}
			continue
		}
		inputType, _ := fieldType(s.owner.input.Schema(), e.Column)
		for row := 0; row < node.batch.Rows; row++ {
			if row&1023 == 0 {
				if err = ctx.Err(); err != nil {
					return false, err
				}
			}
			ordinal := node.start + int64(row)
			if e.Frame.Start.Offset > math.MaxInt64-ordinal {
				continue
			}
			future, fr, exists := s.locate(i, ordinal+e.Frame.Start.Offset)
			if !exists {
				continue
			}
			same, equalErr := s.samePartition(node.batch, row, future, fr)
			if equalErr != nil {
				return false, equalErr
			}
			if !same {
				continue
			}
			value, readErr := readWindowNumericCell(future, s.owner.input.Schema(), e, fr)
			if readErr != nil {
				return false, readErr
			}
			if e.Function == WindowDiff {
				current, readErr := readWindowNumericCell(node.batch, s.owner.input.Schema(), e, row)
				if readErr != nil {
					return false, readErr
				}
				value = offsetDifference(current, value, inputType)
			}
			setOffsetOutput(node.batch, name, typ, row, value)
		}
	}
	s.head = node.next
	if s.head == nil {
		s.tail = nil
	}
	s.current = node
	bindAll(dst, node.batch)
	dst.Reset(node.batch.Rows)
	return true, nil
}

// Close releases every retained batch, reservation and child stream.
func (s *futureWindowStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.releaseCurrent()
	for node := s.head; node != nil; node = node.next {
		node.batch.ReleaseOwned()
		if node.lease != nil {
			node.lease.Release()
		}
	}
	s.head, s.tail = nil, nil
	s.cursors = nil
	s.fillPositions = nil
	for _, state := range s.finiteRows {
		if state != nil {
			state.close()
		}
	}
	s.finiteRows = nil
	s.scratch.ReleaseOwned()
	if s.base != nil {
		s.base.Release()
		s.base = nil
	}
	err := s.child.Close()
	s.owner = nil
	s.child = nil
	s.scratch = nil
	return err
}

func prepareFutureWindow(owner *WindowSource, input source.Source) (*WindowSource, source.Source, []string) {
	hasFuture := false
	for _, expression := range owner.expressions {
		if needsFutureWindow(expression) {
			hasFuture = true
			break
		}
	}
	if !hasFuture {
		return owner, input, nil
	}
	fields := append([]memory.Field(nil), input.Schema().Fields...)
	copied := *owner
	copied.expressions = append([]WindowExpression(nil), owner.expressions...)
	copied.outputFields = append([]memory.Field(nil), owner.outputFields...)
	future := &futureWindowSource{input: input, partitionBy: owner.partitionBy, manager: owner.manager}
	names := make(map[string]bool, len(fields)+len(owner.outputFields))
	for _, f := range fields {
		names[f.Name] = true
	}
	for _, f := range owner.outputFields {
		names[f.Name] = true
	}
	for index, e := range owner.expressions {
		if !needsFutureWindow(e) {
			continue
		}
		name := fmt.Sprintf("__golume_window_future_%d", index)
		for names[name] {
			name += "_"
		}
		names[name] = true
		typ, _ := fieldType(input.Schema(), e.Column)
		field := memory.Field{Name: name, Type: offsetOutputType(typ, e.Function), Nullable: true, Encoding: memory.EncodingPlain}
		if needsFiniteRowsWindow(e) {
			field = owner.outputFields[index]
			field.Name = name
			// The private zero-offset shift owns a validity bitmap even for
			// Count. Charge it in both stages; the public output schema stays
			// non-nullable and every count result is explicitly valid.
			field.Nullable = true
			copied.outputFields[index].Nullable = true
		}
		fields = append(fields, field)
		future.fields = append(future.fields, field)
		future.names = append(future.names, name)
		future.expressions = append(future.expressions, e)
		lead := e.Frame.Start.Offset
		if needsFiniteRowsWindow(e) {
			lead = 0
			if e.Frame.End.Kind == WindowFollowing {
				lead = e.Frame.End.Offset
			}
		}
		if lead > future.maxLead {
			future.maxLead = lead
		}
		copied.expressions[index] = WindowExpression{Function: WindowShift, Column: name, Alias: e.Alias, Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}
	}
	if len(future.expressions) == 0 {
		return owner, input, nil
	}
	future.schema = memory.NewSchema(fields...)
	return &copied, future, future.names
}
