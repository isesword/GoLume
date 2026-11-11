package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"math/big"
)

const windowStateCellBytes = int64(64)

// validateBoundedWindowExpressions defines the first complete bounded ROWS
// slice. Ranking functions are constant-state. Aggregates support frames that
// end at CURRENT ROW and start at CURRENT ROW, N PRECEDING, or UNBOUNDED
// PRECEDING. FOLLOWING needs look-ahead and remains an explicit capability
// error instead of silently retaining an unbounded partition.
func validateBoundedWindowExpressions(expressions []WindowExpression) error {
	for _, expression := range expressions {
		switch expression.Function {
		case WindowRowNumber, WindowRank, WindowDenseRank, WindowForwardFill, WindowBackwardFill:
			continue
		case WindowShift, WindowDiff:
			if expression.Frame.Start != expression.Frame.End || (expression.Frame.Start.Kind != WindowCurrentRow && expression.Frame.Start.Kind != WindowPreceding && expression.Frame.Start.Kind != WindowFollowing) || expression.Frame.Start.Offset < 0 {
				return fmt.Errorf("bounded offset window %q requires a finite offset", expression.Alias)
			}
			continue
		}
		if expression.Frame.End.Kind != WindowCurrentRow {
			return fmt.Errorf("bounded window %q requires a frame ending at CURRENT ROW", expression.Alias)
		}
		switch expression.Frame.Start.Kind {
		case WindowUnboundedPreceding, WindowPreceding, WindowCurrentRow:
		default:
			return fmt.Errorf("bounded window %q does not support FOLLOWING frame state", expression.Alias)
		}
	}
	return nil
}

// NewBatchStream opens the bounded ordered-state or external-sort window.
func (s *WindowSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	switch s.mode {
	case WindowExecutionResident:
		return newResidentWindowStream(ctx, s, opts)
	case WindowExecutionOrdered:
		return newOrderedWindowStream(ctx, s, s.input, s.output, opts)
	case WindowExecutionExternalSort:
		return newExternalWindowStream(ctx, s, opts)
	default:
		return nil, fmt.Errorf("invalid window execution mode %d", s.mode)
	}
}

type residentWindowStream struct {
	reader *windowReader
	done   bool
}

func newResidentWindowStream(ctx context.Context, owner *WindowSource, opts source.ReadOptions) (source.BatchStream, error) {
	child, err := source.OpenBatchStream(ctx, owner.input, opts)
	if err != nil {
		return nil, err
	}
	return &residentWindowStream{reader: &windowReader{owner: owner, child: child, scratch: memory.NewBatch()}}, nil
}

// Next emits the resident window result once.
func (s *residentWindowStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.done {
		dst.ClearColumns()
		return false, nil
	}
	s.done = true
	return s.reader.Next(ctx, dst)
}

// Close releases the resident window reader.
func (s *residentWindowStream) Close() error { return s.reader.Close() }

type ordinalSource struct {
	input   source.Source
	schema  memory.Schema
	column  string
	manager memory.Manager
}

func newOrdinalSource(input source.Source, manager memory.Manager) *ordinalSource {
	column := "__golume_window_ordinal"
	for suffix := 0; ; suffix++ {
		if _, exists := input.Schema().Field(column); !exists {
			break
		}
		column = fmt.Sprintf("__golume_window_ordinal_%d", suffix+1)
	}
	fields := append([]memory.Field(nil), input.Schema().Fields...)
	fields = append(fields, memory.Field{Name: column, Type: memory.TypeUint64})
	return &ordinalSource{input: input, schema: memory.NewSchema(fields...), column: column, manager: manager}
}

// Schema returns the input schema plus the private ordinal.
func (s *ordinalSource) Schema() memory.Schema { return s.schema }

// Stats forwards child statistics.
func (s *ordinalSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

// Partitions forwards child partitions.
func (s *ordinalSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewBatchStream opens the ordinal-appending stream.
func (s *ordinalSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		return nil, err
	}
	return &ordinalBatchStream{child: child, column: s.column, manager: s.manager}, nil
}

type ordinalBatchStream struct {
	child       source.BatchStream
	column      string
	manager     memory.Manager
	reservation memory.Reservation
	next        uint64
	closed      bool
}

// Next appends globally increasing row ordinals to one child batch.
func (s *ordinalBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.reservation != nil {
		s.reservation.Release()
		s.reservation = nil
	}
	ok, err := s.child.Next(ctx, dst)
	if err != nil || !ok {
		return ok, err
	}
	if uint64(dst.Rows) > math.MaxUint64-s.next {
		return false, fmt.Errorf("window input row ordinal overflows uint64")
	}
	bytes := int64(dst.Rows) * 8
	if s.manager != nil {
		s.reservation, err = s.manager.Reserve(ctx, bytes)
		if err != nil {
			return false, fmt.Errorf("window ordinal output memory: %w", err)
		}
	}
	values := make([]uint64, dst.Rows)
	for row := range values {
		values[row] = s.next + uint64(row)
	}
	s.next += uint64(dst.Rows)
	dst.Uint64s[s.column] = &memory.Uint64Vector{Values: values}
	return true, nil
}

// Close releases the ordinal vector reservation and child stream.
func (s *ordinalBatchStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.reservation != nil {
		s.reservation.Release()
		s.reservation = nil
	}
	return s.child.Close()
}

type orderedWindowSource struct {
	owner  *WindowSource
	input  source.Source
	schema memory.Schema
}

// Schema returns input and window output fields.
func (s *orderedWindowSource) Schema() memory.Schema { return s.schema }

// Stats forwards ordered-input statistics.
func (s *orderedWindowSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

// Partitions forwards ordered-input partitions.
func (s *orderedWindowSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewBatchStream opens the bounded ordered-state evaluator.
func (s *orderedWindowSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return newOrderedWindowStream(ctx, s.owner, s.input, s.schema, opts)
}

type externalWindowStream struct {
	child  source.BatchStream
	query  *memory.QueryBudget
	rows   int64
	closed bool
}

func newExternalWindowStream(ctx context.Context, owner *WindowSource, opts source.ReadOptions) (source.BatchStream, error) {
	query := memory.QueryBudgetFromContext(ctx)
	if query == nil || query.Limit() <= 0 || !query.SpillConfigured() {
		return nil, fmt.Errorf("external window requires finite memory and spill quotas")
	}
	ordinal := newOrdinalSource(owner.input, owner.manager)
	keys := make([]SortKey, 0, len(owner.partitionBy)+len(owner.orderBy))
	for _, column := range owner.partitionBy {
		keys = append(keys, SortKey{Column: column})
	}
	keys = append(keys, owner.orderBy...)
	ordered, err := NewSortSourceWithOptions(ordinal, keys, owner.manager, SortSourceOptions{Spillable: true})
	if err != nil {
		return nil, err
	}
	windowSchema, err := windowOutputSchema(ordinal.Schema(), owner.outputFields)
	if err != nil {
		return nil, err
	}
	windowed := &orderedWindowSource{owner: owner, input: ordered, schema: windowSchema}
	restored, err := NewSortSourceWithOptions(windowed, []SortKey{{Column: ordinal.column}}, owner.manager, SortSourceOptions{Spillable: true})
	if err != nil {
		return nil, err
	}
	hidden, err := NewDropColumnsSource(restored, []string{ordinal.column})
	if err != nil {
		return nil, err
	}
	child, err := source.OpenBatchStream(ctx, hidden, opts)
	if err != nil {
		return nil, err
	}
	return &externalWindowStream{child: child, query: query}, nil
}

// Next emits one restored original-order window batch.
func (s *externalWindowStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	ok, err := s.child.Next(ctx, dst)
	if ok {
		s.rows += int64(dst.Rows)
	}
	return ok, err
}

// Close releases both external sort chains and their spill files.
func (s *externalWindowStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.child.Close()
}

// StreamDiagnostics reports the composite external-window resource totals.
func (s *externalWindowStream) StreamDiagnostics() map[string]string {
	snapshot := s.query.Snapshot()
	return map[string]string{
		"strategy": "external-sort-restore", "output_rows": fmt.Sprint(s.rows),
		"spill_bytes": fmt.Sprint(snapshot.SpilledBytes), "peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(snapshot.PeakBytes),
	}
}

type windowNumericCell struct {
	valid bool
	i64   int64
	u64   uint64
	f64   float64
	seq   int64
}

type windowCellRing struct {
	values []windowNumericCell
	head   int
	len    int
}

func newWindowCellRing(capacity int) windowCellRing {
	if capacity <= 0 {
		return windowCellRing{}
	}
	return windowCellRing{values: make([]windowNumericCell, capacity)}
}

func (r *windowCellRing) reset() { r.head, r.len = 0, 0 }

func (r *windowCellRing) push(value windowNumericCell) (windowNumericCell, bool) {
	if len(r.values) == 0 {
		return windowNumericCell{}, false
	}
	var removed windowNumericCell
	full := r.len == len(r.values)
	if full {
		removed = r.values[r.head]
		r.head = (r.head + 1) % len(r.values)
		r.len--
	}
	index := (r.head + r.len) % len(r.values)
	r.values[index] = value
	r.len++
	return removed, full
}

func (r *windowCellRing) at(index int) windowNumericCell {
	return r.values[(r.head+index)%len(r.values)]
}

func (r *windowCellRing) front() windowNumericCell { return r.at(0) }
func (r *windowCellRing) back() windowNumericCell  { return r.at(r.len - 1) }
func (r *windowCellRing) popFront() {
	if r.len == 0 {
		return
	}
	r.head = (r.head + 1) % len(r.values)
	r.len--
}
func (r *windowCellRing) popBack() {
	if r.len > 0 {
		r.len--
	}
}
func (r *windowCellRing) pushBack(value windowNumericCell) {
	if r.len == len(r.values) {
		panic("window deque capacity exceeded")
	}
	index := (r.head + r.len) % len(r.values)
	r.values[index] = value
	r.len++
}

type rollingWindowState struct {
	expression  WindowExpression
	inputType   memory.DataType
	outputType  memory.DataType
	unbounded   bool
	width       int
	position    int64
	queue       windowCellRing
	extrema     windowCellRing
	validCount  int64
	signedSum   big.Int
	unsignedSum big.Int
	finiteSum   float64
	nanCount    int64
	posInf      int64
	negInf      int64
	best        windowNumericCell
	hasBest     bool
}

func newRollingWindowState(expression WindowExpression, input, output memory.DataType) (*rollingWindowState, error) {
	state := &rollingWindowState{expression: expression, inputType: input, outputType: output}
	switch expression.Function {
	case WindowRowNumber, WindowRank, WindowDenseRank, WindowForwardFill:
		return state, nil
	}
	switch expression.Frame.Start.Kind {
	case WindowUnboundedPreceding:
		state.unbounded = true
	case WindowCurrentRow:
		state.width = 1
	case WindowPreceding:
		if expression.Frame.Start.Offset >= int64(platformMaxInt()) {
			return nil, fmt.Errorf("window %q preceding offset exceeds platform capacity", expression.Alias)
		}
		state.width = int(expression.Frame.Start.Offset) + 1
	}
	return state, nil
}

func (s *rollingWindowState) initialize() {
	if s.unbounded || s.expression.Function == WindowCountRows || s.width <= 0 {
		return
	}
	s.queue = newWindowCellRing(s.width)
	if s.expression.Function == WindowMin || s.expression.Function == WindowMax {
		s.extrema = newWindowCellRing(s.width)
	}
}

func platformMaxInt() int { return int(^uint(0) >> 1) }

func (s *rollingWindowState) reset() {
	s.position = 0
	s.queue.reset()
	s.extrema.reset()
	s.validCount = 0
	s.signedSum.SetInt64(0)
	s.unsignedSum.SetInt64(0)
	s.finiteSum = 0
	s.nanCount, s.posInf, s.negInf = 0, 0, 0
	s.best, s.hasBest = windowNumericCell{}, false
}

func (s *rollingWindowState) advance(cell windowNumericCell) error {
	if s.expression.Function == WindowForwardFill {
		if cell.valid {
			s.best = cell
		}
		return nil
	}
	cell.seq = s.position
	s.position++
	if s.expression.Function == WindowShift || s.expression.Function == WindowDiff {
		s.queue.push(cell)
		return nil
	}
	if s.expression.Function == WindowCountRows {
		return nil
	}
	if !s.unbounded {
		removed, ok := s.queue.push(cell)
		if ok {
			s.remove(removed)
		}
	}
	s.add(cell)
	if s.expression.Function == WindowMin || s.expression.Function == WindowMax {
		if s.unbounded {
			if cell.valid && (!s.hasBest || windowCellBetter(cell, s.best, s.inputType, s.expression.Function == WindowMin)) {
				s.best, s.hasBest = cell, true
			}
		} else {
			minimum := s.expression.Function == WindowMin
			for s.extrema.len > 0 && s.extrema.front().seq <= cell.seq-int64(s.width) {
				s.extrema.popFront()
			}
			if cell.valid {
				for s.extrema.len > 0 && windowCellBetter(cell, s.extrema.back(), s.inputType, minimum) {
					s.extrema.popBack()
				}
				s.extrema.pushBack(cell)
			}
			s.hasBest = s.extrema.len > 0
			if s.hasBest {
				s.best = s.extrema.front()
			}
		}
	}
	return nil
}

func (s *rollingWindowState) add(cell windowNumericCell) {
	if !cell.valid {
		return
	}
	s.validCount++
	switch s.expression.Function {
	case WindowSum:
		s.addSum(cell, 1)
	case WindowAvg:
		s.addFloat(cell, 1)
	}
}

func (s *rollingWindowState) remove(cell windowNumericCell) {
	if !cell.valid {
		return
	}
	s.validCount--
	switch s.expression.Function {
	case WindowSum:
		s.addSum(cell, -1)
	case WindowAvg:
		s.addFloat(cell, -1)
	}
}

func (s *rollingWindowState) addSum(cell windowNumericCell, direction int64) {
	switch {
	case windowSigned(s.inputType):
		value := big.NewInt(cell.i64)
		if direction < 0 {
			value.Neg(value)
		}
		s.signedSum.Add(&s.signedSum, value)
	case windowUnsigned(s.inputType):
		value := new(big.Int).SetUint64(cell.u64)
		if direction < 0 {
			value.Neg(value)
		}
		s.unsignedSum.Add(&s.unsignedSum, value)
	default:
		s.addFloat(cell, direction)
	}
}

func (s *rollingWindowState) addFloat(cell windowNumericCell, direction int64) {
	value := cell.f64
	if windowSigned(s.inputType) {
		value = float64(cell.i64)
	}
	if windowUnsigned(s.inputType) {
		value = float64(cell.u64)
	}
	switch {
	case math.IsNaN(value):
		s.nanCount += direction
	case math.IsInf(value, 1):
		s.posInf += direction
	case math.IsInf(value, -1):
		s.negInf += direction
	default:
		s.finiteSum += float64(direction) * value
	}
}

func (s *rollingWindowState) floatTotal() float64 {
	if s.nanCount > 0 || s.posInf > 0 && s.negInf > 0 {
		return math.NaN()
	}
	if s.posInf > 0 {
		return math.Inf(1)
	}
	if s.negInf > 0 {
		return math.Inf(-1)
	}
	return s.finiteSum
}

func windowCellBetter(value, best windowNumericCell, typ memory.DataType, minimum bool) bool {
	switch {
	case windowSigned(typ):
		return windowPickInt(value.i64, best.i64, minimum)
	case windowUnsigned(typ):
		return windowPickUint(value.u64, best.u64, minimum)
	default:
		return windowPickFloat(value.f64, best.f64, minimum)
	}
}

type orderedWindowStream struct {
	hiddenColumns          []string
	preparationReservation memory.Reservation
	owner                  *WindowSource
	inputSchema            memory.Schema
	output                 memory.Schema
	child                  source.BatchStream
	states                 []*rollingWindowState
	input                  *memory.Batch
	partitionKeys          []SortKey
	previousPartition      rowCandidate
	previousOrder          rowCandidate
	havePrevious           bool
	rowNumber              int64
	rank                   int64
	denseRank              int64
	manager                memory.Manager
	baseReservation        memory.Reservation
	extraReservations      []memory.Reservation
	baseBytes              int64
	keyBytes               int64
	reservedBytes          int64
	outputReservation      memory.Reservation
	closed                 bool
}

func newOrderedWindowStream(ctx context.Context, owner *WindowSource, input source.Source, output memory.Schema, opts source.ReadOptions) (source.BatchStream, error) {
	var preparation memory.Reservation
	preparationBytes, err := futurePreparationAdmissionBytes(owner, input)
	if err != nil {
		return nil, err
	}
	if preparationBytes != 0 && owner.manager != nil {
		preparation, err = owner.manager.Reserve(ctx, preparationBytes)
		if err != nil {
			return nil, fmt.Errorf("window lookahead descriptors: %w", err)
		}
	}
	keepPreparation := false
	defer func() {
		if !keepPreparation && preparation != nil {
			preparation.Release()
		}
	}()
	owner, input, hidden := prepareFutureWindow(owner, input)
	child, err := source.OpenBatchStream(ctx, input, opts)
	if err != nil {
		return nil, err
	}
	stream := &orderedWindowStream{owner: owner, inputSchema: input.Schema(), output: output, child: child, input: memory.NewBatch(), manager: owner.manager}
	stream.hiddenColumns = hidden
	stream.preparationReservation = preparation
	for _, column := range owner.partitionBy {
		stream.partitionKeys = append(stream.partitionKeys, SortKey{Column: column})
	}
	var stateBytes int64 = 512
	for _, expression := range owner.expressions {
		inputType := memory.TypeInt64
		if expression.Column != "" {
			inputType, _ = fieldType(input.Schema(), expression.Column)
		}
		outputType, _ := fieldType(output, expression.Alias)
		state, stateErr := newRollingWindowState(expression, inputType, outputType)
		if stateErr != nil {
			_ = child.Close()
			return nil, stateErr
		}
		stream.states = append(stream.states, state)
		cells := int64(1)
		if !state.unbounded && state.width > 0 {
			cells = int64(state.width)
			if expression.Function == WindowMin || expression.Function == WindowMax {
				if cells > math.MaxInt64/2 {
					_ = child.Close()
					return nil, fmt.Errorf("window extrema state estimate overflows int64")
				}
				cells *= 2
			}
		}
		if cells > (math.MaxInt64-stateBytes)/windowStateCellBytes {
			_ = child.Close()
			return nil, fmt.Errorf("window retained-state estimate overflows int64")
		}
		stateBytes += cells * windowStateCellBytes
	}
	if stream.manager != nil {
		stream.baseReservation, err = stream.manager.Reserve(ctx, stateBytes)
		if err != nil {
			_ = child.Close()
			return nil, fmt.Errorf("window retained state: %w", err)
		}
		stream.baseBytes = stream.baseReservation.Bytes()
		stream.reservedBytes = stream.baseReservation.Bytes()
	}
	for _, state := range stream.states {
		state.initialize()
	}
	keepPreparation = true
	return stream, nil
}

func fieldType(schema memory.Schema, name string) (memory.DataType, bool) {
	field, ok := schema.Field(name)
	return field.Type, ok
}

func (s *orderedWindowStream) growKeyReservation(ctx context.Context, bytes int64) error {
	if s.manager == nil || bytes <= s.reservedBytes {
		return nil
	}
	reservation, err := s.manager.Reserve(ctx, bytes-s.reservedBytes)
	if err != nil {
		return err
	}
	s.extraReservations = append(s.extraReservations, reservation)
	s.reservedBytes = bytes
	return nil
}

func batchRowEqualsCandidate(batch *memory.Batch, schema memory.Schema, keys []SortKey, row int, candidate rowCandidate) (bool, error) {
	if len(keys) != len(candidate.cells) {
		return false, nil
	}
	for index, key := range keys {
		cell, err := snapshotCell(batch, schema, key.Column, row)
		if err != nil {
			return false, err
		}
		if compareSortCells(cell, candidate.cells[index], key) != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *orderedWindowStream) resetPartition() {
	s.rowNumber, s.rank, s.denseRank = 0, 0, 0
	for _, state := range s.states {
		state.reset()
	}
}

// Next evaluates one ordered child batch while carrying partition state.
func (s *orderedWindowStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.outputReservation != nil {
		s.outputReservation.Release()
		s.outputReservation = nil
	}
	ok, err := s.child.Next(ctx, s.input)
	if err != nil || !ok {
		dst.ClearColumns()
		return ok, err
	}
	outputBytes, err := estimateWindowOutputBytes(s.output, s.owner.outputFields, s.input.Rows)
	if err != nil {
		return false, err
	}
	if s.manager != nil {
		s.outputReservation, err = s.manager.Reserve(ctx, outputBytes)
		if err != nil {
			return false, fmt.Errorf("window output workspace: %w", err)
		}
	}
	dst.ClearColumns()
	bindAll(dst, s.input)
	dst.Reset(s.input.Rows)
	initializeWindowOutputs(dst, s.owner.outputFields, s.input.Rows, s.owner.expressions)
	for row := 0; row < s.input.Rows; row++ {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return false, err
			}
		}
		newPartition := !s.havePrevious
		if s.havePrevious && len(s.partitionKeys) > 0 {
			equal, equalErr := batchRowEqualsCandidate(s.input, s.inputSchema, s.partitionKeys, row, s.previousPartition)
			if equalErr != nil {
				return false, equalErr
			}
			newPartition = !equal
		}
		peer := false
		if !newPartition && s.havePrevious {
			peer, err = batchRowEqualsCandidate(s.input, s.inputSchema, s.owner.orderBy, row, s.previousOrder)
			if err != nil {
				return false, err
			}
		}
		if newPartition {
			s.resetPartition()
		}
		s.rowNumber++
		if s.rowNumber == 1 || !peer {
			s.rank = s.rowNumber
			s.denseRank++
		}
		for index, state := range s.states {
			expression := state.expression
			switch expression.Function {
			case WindowRowNumber:
				setWindowInt64(dst, expression.Alias, row, s.rowNumber, true)
			case WindowRank:
				setWindowInt64(dst, expression.Alias, row, s.rank, true)
			case WindowDenseRank:
				setWindowInt64(dst, expression.Alias, row, s.denseRank, true)
			default:
				cell, cellErr := readWindowNumericCell(s.input, s.inputSchema, expression, row)
				if cellErr != nil {
					return false, cellErr
				}
				if advanceErr := state.advance(cell); advanceErr != nil {
					return false, advanceErr
				}
				if outputErr := state.write(dst, row); outputErr != nil {
					return false, outputErr
				}
			}
			_ = index
		}
		partitionText, textErr := sortKeyVariableBytes(s.input, s.inputSchema, s.partitionKeys, row)
		if textErr != nil {
			return false, textErr
		}
		orderText, textErr := sortKeyVariableBytes(s.input, s.inputSchema, s.owner.orderBy, row)
		if textErr != nil {
			return false, textErr
		}
		if orderText > math.MaxInt64-partitionText {
			return false, fmt.Errorf("window key state byte estimate overflows int64")
		}
		textBytes := partitionText + orderText
		if textBytes > s.keyBytes {
			s.keyBytes = textBytes
		}
		if s.keyBytes > math.MaxInt64-s.baseBytes {
			return false, fmt.Errorf("window key state byte estimate overflows int64")
		}
		if err := s.growKeyReservation(ctx, s.baseBytes+s.keyBytes); err != nil {
			return false, fmt.Errorf("window key state: %w", err)
		}
		if err := snapshotCandidateInto(&s.previousPartition, s.input, s.inputSchema, s.partitionKeys, row, 0); err != nil {
			return false, err
		}
		if err := snapshotCandidateInto(&s.previousOrder, s.input, s.inputSchema, s.owner.orderBy, row, 0); err != nil {
			return false, err
		}
		s.havePrevious = true
	}
	for _, name := range s.hiddenColumns {
		clearWindowOutput(dst, name)
	}
	return true, nil
}

func estimateWindowOutputBytes(_ memory.Schema, fields []memory.Field, rows int) (int64, error) {
	var width int64
	for _, field := range fields {
		switch field.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
			width += 4
		default:
			width += 8
		}
		if field.Nullable {
			width++
		}
	}
	if rows > 0 && width > math.MaxInt64/int64(rows) {
		return 0, fmt.Errorf("window output byte estimate overflows int64")
	}
	return width*int64(rows) + 256, nil
}

func initializeWindowOutputs(batch *memory.Batch, fields []memory.Field, rows int, expressions []WindowExpression) {
	for index, field := range fields {
		if expressions[index].Function == WindowShift || expressions[index].Function == WindowDiff || isFillWindow(expressions[index].Function) || expressions[index].Function == WindowMin || expressions[index].Function == WindowMax {
			clearWindowOutput(batch, field.Name)
			initializeOffsetOutput(batch, field.Name, field.Type, rows)
			continue
		}
		validity := memory.Bitmap(nil)
		if field.Nullable {
			validity = memory.EnsureBitmap(nil, rows)
		}
		switch field.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32:
			batch.Int32s[field.Name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: validity}
		case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
			batch.Uint32s[field.Name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: validity}
		case memory.TypeUint64:
			batch.Uint64s[field.Name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: validity}
		case memory.TypeFloat32, memory.TypeFloat64:
			batch.Float64s[field.Name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: validity}
		default:
			batch.Int64s[field.Name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: validity}
		}
	}
}

func readWindowNumericCell(batch *memory.Batch, schema memory.Schema, expression WindowExpression, row int) (windowNumericCell, error) {
	if expression.Function == WindowCountRows {
		return windowNumericCell{valid: true}, nil
	}
	if expression.Function == WindowCountValues {
		valid, err := windowValueValid(batch, schema, expression.Column, row)
		return windowNumericCell{valid: valid}, err
	}
	field, _ := schema.Field(expression.Column)
	cell := windowNumericCell{}
	var err error
	switch {
	case windowSigned(field.Type):
		cell.i64, cell.valid, err = windowInt64(batch, schema, expression.Column, row)
	case windowUnsigned(field.Type):
		cell.u64, cell.valid, err = windowUint64(batch, schema, expression.Column, row)
	default:
		cell.f64, cell.valid, err = windowFloat64(batch, schema, expression.Column, row)
	}
	return cell, err
}

func setWindowInt64(batch *memory.Batch, name string, row int, value int64, valid bool) {
	vector := batch.Int64s[name]
	vector.Values[row] = value
	if valid && vector.Validity != nil {
		memory.Set(vector.Validity, row)
	}
}

func (s *rollingWindowState) write(batch *memory.Batch, row int) error {
	expression := s.expression
	if expression.Function == WindowForwardFill {
		setOffsetOutput(batch, expression.Alias, s.outputType, row, s.best)
		return nil
	}
	if expression.Function == WindowShift || expression.Function == WindowDiff {
		if s.queue.len < s.width {
			return nil
		}
		value := s.queue.front()
		if expression.Function == WindowDiff {
			value = offsetDifference(s.queue.back(), value, s.inputType)
		}
		setOffsetOutput(batch, expression.Alias, s.outputType, row, value)
		return nil
	}
	if expression.Function == WindowCountRows {
		value := s.position
		if !s.unbounded && value > int64(s.width) {
			value = int64(s.width)
		}
		setWindowInt64(batch, expression.Alias, row, value, true)
		return nil
	}
	if expression.Function == WindowCountValues {
		setWindowInt64(batch, expression.Alias, row, s.validCount, true)
		return nil
	}
	if s.validCount == 0 {
		return nil
	}
	switch expression.Function {
	case WindowSum:
		switch {
		case windowSigned(s.inputType):
			if !s.signedSum.IsInt64() {
				return fmt.Errorf("window sum %q overflows int64 at ordered row %d", expression.Alias, s.position-1)
			}
			setWindowInt64(batch, expression.Alias, row, s.signedSum.Int64(), true)
		case windowUnsigned(s.inputType):
			if s.unsignedSum.Sign() < 0 || s.unsignedSum.BitLen() > 64 {
				return fmt.Errorf("window sum %q overflows uint64 at ordered row %d", expression.Alias, s.position-1)
			}
			vector := batch.Uint64s[expression.Alias]
			vector.Values[row] = s.unsignedSum.Uint64()
			memory.Set(vector.Validity, row)
		default:
			vector := batch.Float64s[expression.Alias]
			vector.Values[row] = s.floatTotal()
			memory.Set(vector.Validity, row)
		}
	case WindowAvg:
		vector := batch.Float64s[expression.Alias]
		vector.Values[row] = s.floatTotal() / float64(s.validCount)
		memory.Set(vector.Validity, row)
	case WindowMin, WindowMax:
		if s.hasBest {
			setOffsetOutput(batch, expression.Alias, s.outputType, row, s.best)
		}
	}
	return nil
}

// Close releases rolling state, output reservations, and the child stream.
func (s *orderedWindowStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.outputReservation != nil {
		s.outputReservation.Release()
	}
	if s.baseReservation != nil {
		s.baseReservation.Release()
	}
	for _, reservation := range s.extraReservations {
		reservation.Release()
	}
	s.input.ReleaseOwned()
	err := s.child.Close()
	if s.preparationReservation != nil {
		s.preparationReservation.Release()
		s.preparationReservation = nil
	}
	// Closed streams may themselves remain reachable. Drop their execution-owned
	// rings and derived future descriptors when their reservations are released.
	s.owner = nil
	s.child = nil
	s.input = nil
	s.states = nil
	s.hiddenColumns = nil
	s.partitionKeys = nil
	s.inputSchema = memory.Schema{}
	s.output = memory.Schema{}
	s.previousPartition = rowCandidate{}
	s.previousOrder = rowCandidate{}
	s.extraReservations = nil
	s.outputReservation = nil
	s.baseReservation = nil
	return err
}

var _ source.BatchStreamDiagnostics = (*externalWindowStream)(nil)
