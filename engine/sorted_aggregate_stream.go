package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/planner"
	"gocolumn/source"
	"time"
)

// IsSortedAggregate reports whether preparation selected adjacent-key state.
func (p *PreparedQuery) IsSortedAggregate() bool {
	return p != nil && p.strategy.Kind == planner.StrategySorted
}

// NewSortedAggregateStream creates a one-group-state stream over proven
// globally ordered input. It does not require a spill store.
func (p *PreparedQuery) NewSortedAggregateStream(ctx context.Context, schema memory.Schema, outputRows int) (source.BatchStream, error) {
	if p == nil || p.e == nil || !p.IsSortedAggregate() {
		return nil, fmt.Errorf("sorted aggregate stream requires a prepared sorted strategy")
	}
	if ctx == nil {
		return nil, fmt.Errorf("sorted aggregate stream context is nil")
	}
	budget := memory.QueryBudgetFromContext(ctx)
	if budget == nil {
		return nil, fmt.Errorf("sorted aggregate stream requires a query memory budget")
	}
	if err := p.begin(); err != nil {
		return nil, err
	}
	active := true
	defer func() {
		if active {
			p.end()
		}
	}()
	if outputRows < 1 {
		outputRows = p.e.opts.Pipeline.MaxMorselRows
	}
	rowBytes := pipeline.EstimateSchemaRowBytes(schema) + 64
	budgetRows := budget.Limit() / 8 / rowBytes
	if budgetRows < 1 {
		budgetRows = 1
	}
	if budget.Limit() > 0 && int64(outputRows) > budgetRows {
		outputRows = int(budgetRows)
	}
	exchange := budget.Operator("SortedAggregateExchange", memory.MemoryInFlightBatch)
	input, err := pipeline.OpenBatchStream(ctx, p.src, source.ReadOptions{Kernels: p.e.kernels}, p.parts, exchange, p.e.opts.Pipeline, p.e.opts.Observer)
	if err != nil {
		return nil, err
	}
	stateBudget := budget.Operator("SortedAggregateState", memory.MemoryRetainedState)
	stateBytes := aggregate.Int64VectorBytesForSlots(1, p.cq.AggregateLayout) + 128
	stateLease, err := stateBudget.ReserveGrowth(ctx, stateBytes)
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	stream := &sortedAggregateStream{
		query: p, schema: schema, budget: budget, input: input,
		stateBudget: stateBudget, outputBudget: budget.Operator("SortedAggregateOutput", memory.MemoryWorkspace),
		stateLease: stateLease, outputRows: outputRows, inputBatch: memory.NewBatch(),
		state: aggregate.NewInt64Vector(1, p.cq.AggregateLayout), started: time.Now(), active: true,
	}
	stream.workspace.SetReserveHook(func(n int64) error {
		r, err := budget.Reserve(context.Background(), n)
		if err == nil {
			stream.dynamicReservations = append(stream.dynamicReservations, r)
		}
		return err
	})
	active = false
	return stream, nil
}

type sortedAggregateStream struct {
	query        *PreparedQuery
	schema       memory.Schema
	budget       *memory.QueryBudget
	input        source.BatchStream
	stateBudget  *memory.OperatorBudget
	outputBudget *memory.OperatorBudget
	stateLease   memory.Reservation
	keyLease     memory.Reservation
	outputLeases []memory.Reservation
	outputRows   int

	inputBatch          *memory.Batch
	binding             aggregateBatchBinding
	selection           memory.Bitmap
	nextRow             int
	workspace           expr.EvalWorkspace
	dynamicReservations []memory.Reservation
	state               aggregate.Int64Vector
	current             aggregateRecord
	haveGroup           bool
	inputDone           bool

	selectedRows int
	groups       int
	checksum     int64
	started      time.Time
	active       bool
	closed       bool
	exhausted    bool
	terminalErr  error
	closeErr     error
}

func (s *sortedAggregateStream) releaseOutput() {
	for index := len(s.outputLeases) - 1; index >= 0; index-- {
		s.outputLeases[index].Release()
	}
	s.outputLeases = nil
}

func (s *sortedAggregateStream) releaseKey() {
	if s.keyLease != nil {
		s.keyLease.Release()
		s.keyLease = nil
	}
}

func (s *sortedAggregateStream) startGroup(ctx context.Context, key aggregateRecord, value int64, valid bool) error {
	s.releaseKey()
	s.current = aggregateRecord{a: key.a, b: key.b, validMask: key.validMask}
	if s.query.cq.GroupShape == GroupShapeStringView && key.validMask&1 != 0 {
		lease, err := s.stateBudget.ReserveGrowth(ctx, int64(len(key.stringKey)))
		if err != nil {
			return fmt.Errorf("reserve sorted aggregate string key: %w", err)
		}
		s.keyLease = lease
		s.current.stringKey = append([]byte(nil), key.stringKey...)
	}
	s.state.InitValue(0, value, valid)
	s.haveGroup = true
	return nil
}

func (s *sortedAggregateStream) ensureOutputString(ctx context.Context, dst *memory.Batch, record aggregateRecord) error {
	if s.query.cq.GroupShape != GroupShapeStringView || record.validMask&1 == 0 {
		return nil
	}
	name := s.query.cq.GroupBy[0].Name
	vector := dst.Strings[name]
	need := len(vector.Data) + len(record.stringKey)
	if need <= cap(vector.Data) {
		return nil
	}
	newCapacity := cap(vector.Data) * 2
	if newCapacity < 64 {
		newCapacity = 64
	}
	if newCapacity < need {
		newCapacity = need
	}
	lease, err := s.outputBudget.ReserveGrowth(ctx, int64(newCapacity-cap(vector.Data)))
	if err != nil {
		return fmt.Errorf("reserve sorted aggregate output strings: %w", err)
	}
	data := make([]byte, len(vector.Data), newCapacity)
	copy(data, vector.Data)
	vector.Data = data
	s.outputLeases = append(s.outputLeases, lease)
	return nil
}

func (s *sortedAggregateStream) flushGroup(ctx context.Context, dst *memory.Batch, row int, formatter *spillableAggregateStream) error {
	if !s.haveGroup {
		return nil
	}
	s.current.state = s.state.Result(0)
	if err := s.ensureOutputString(ctx, dst, s.current); err != nil {
		return err
	}
	if err := formatter.setOutputRecord(dst, row, s.current); err != nil {
		return err
	}
	s.groups++
	s.haveGroup = false
	s.releaseKey()
	return nil
}

func (s *sortedAggregateStream) loadInput(ctx context.Context) error {
	for !s.inputDone {
		ok, err := s.input.Next(ctx, s.inputBatch)
		if err != nil {
			return err
		}
		if !ok {
			s.inputDone = true
			return nil
		}
		selection, err := s.query.cq.Predicate.Evaluate(s.inputBatch, &s.workspace, s.query.e.kernels)
		if err != nil {
			return err
		}
		binding, err := s.query.bindSortedBatch(s.inputBatch)
		if err != nil {
			return err
		}
		s.selection = selection.Truth
		s.binding = binding
		s.nextRow = 0
		for row := 0; row < s.inputBatch.Rows; row++ {
			if memory.IsSet(s.selection, row) {
				return nil
			}
		}
	}
	return nil
}

func (s *sortedAggregateStream) nextSelected(ctx context.Context) (aggregateRecord, int64, bool, bool, error) {
	for {
		for s.nextRow < s.inputBatch.Rows {
			row := s.nextRow
			s.nextRow++
			if !memory.IsSet(s.selection, row) {
				continue
			}
			key := s.binding.record(s.query.cq.GroupShape, row)
			value, valid := s.binding.value(row)
			return key, value, valid, true, nil
		}
		if s.inputDone {
			return aggregateRecord{}, 0, false, false, nil
		}
		if err := s.loadInput(ctx); err != nil {
			return aggregateRecord{}, 0, false, false, err
		}
	}
}

func shrinkAggregateOutput(dst *memory.Batch, schema memory.Schema, rows int) {
	for _, field := range schema.Fields {
		switch field.Type {
		case memory.TypeInt32:
			dst.Int32s[field.Name].Values = dst.Int32s[field.Name].Values[:rows]
		case memory.TypeInt64:
			dst.Int64s[field.Name].Values = dst.Int64s[field.Name].Values[:rows]
		case memory.TypeUint32:
			dst.Uint32s[field.Name].Values = dst.Uint32s[field.Name].Values[:rows]
		case memory.TypeUint64:
			dst.Uint64s[field.Name].Values = dst.Uint64s[field.Name].Values[:rows]
		case memory.TypeFloat64:
			dst.Float64s[field.Name].Values = dst.Float64s[field.Name].Values[:rows]
		case memory.TypeString:
			if field.Encoding == memory.EncodingDictionary {
				dst.DictStrings[field.Name].Indices = dst.DictStrings[field.Name].Indices[:rows]
			} else {
				dst.Strings[field.Name].Offsets = dst.Strings[field.Name].Offsets[:rows+1]
			}
		}
	}
	dst.Reset(rows)
}

func (s *sortedAggregateStream) finishActive(success bool) {
	if !s.active {
		return
	}
	s.active = false
	if success {
		elapsed := time.Since(s.started)
		if s.query.feedbackKey != "" {
			s.query.e.feedback.observe(s.query.feedbackKey, s.query.stats.Rows, s.selectedRows, s.groups)
		}
		s.query.e.opts.Observer.Observe(observer.Event{Stage: "sorted-aggregate", Duration: elapsed, Rows: s.query.stats.Rows, Attributes: map[string]string{"groups": fmt.Sprint(s.groups)}})
	}
	s.query.end()
}

func (s *sortedAggregateStream) fail(err error) error {
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	s.releaseOutput()
	s.releaseKey()
	if s.input != nil {
		err = errors.Join(err, s.input.Close())
		s.input = nil
	}
	s.finishActive(false)
	return err
}

// Next emits up to outputRows completed adjacent-key groups.
func (s *sortedAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("sorted aggregate stream destination is nil")
	}
	dst.ClearColumns()
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, nil
	}
	if ctx == nil {
		return false, s.fail(fmt.Errorf("sorted aggregate stream context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return false, s.fail(err)
	}
	baseBytes, err := aggregateOutputBytes(s.schema, s.outputRows, 0)
	if err != nil {
		return false, s.fail(err)
	}
	lease, err := s.outputBudget.ReserveGrowth(ctx, baseBytes)
	if err != nil {
		return false, s.fail(err)
	}
	s.outputLeases = append(s.outputLeases, lease)
	formatter := &spillableAggregateStream{query: s.query, schema: s.schema}
	if err := formatter.allocateOutputBatch(dst, s.outputRows, 0); err != nil {
		return false, s.fail(err)
	}
	output := 0
	for output < s.outputRows {
		key, value, valid, ok, err := s.nextSelected(ctx)
		if err != nil {
			dst.ClearColumns()
			return false, s.fail(err)
		}
		if !ok {
			if s.haveGroup {
				if err := s.flushGroup(ctx, dst, output, formatter); err != nil {
					dst.ClearColumns()
					return false, s.fail(err)
				}
				output++
			}
			s.exhausted = true
			if s.input != nil {
				if err := s.input.Close(); err != nil {
					dst.ClearColumns()
					return false, s.fail(err)
				}
				s.input = nil
			}
			s.finishActive(true)
			break
		}
		if !s.haveGroup {
			if err := s.startGroup(ctx, key, value, valid); err != nil {
				dst.ClearColumns()
				return false, s.fail(err)
			}
		} else if aggregateKeysEqual(s.query.cq.GroupShape, s.current, key) {
			s.state.AddValue(0, value, valid)
		} else {
			if err := s.flushGroup(ctx, dst, output, formatter); err != nil {
				dst.ClearColumns()
				return false, s.fail(err)
			}
			output++
			if err := s.startGroup(ctx, key, value, valid); err != nil {
				dst.ClearColumns()
				return false, s.fail(err)
			}
		}
		s.selectedRows++
		if valid {
			s.checksum += value
		}
	}
	if output == 0 {
		dst.ClearColumns()
		s.releaseOutput()
		return false, nil
	}
	shrinkAggregateOutput(dst, s.schema, output)
	return true, nil
}

// Close releases input, one-group state and output reservations exactly once.
func (s *sortedAggregateStream) Close() error {
	if s == nil || s.closed {
		return s.closeErr
	}
	s.closed = true
	for _, r := range s.dynamicReservations {
		r.Release()
	}
	s.dynamicReservations = nil
	s.releaseOutput()
	s.releaseKey()
	if s.stateLease != nil {
		s.stateLease.Release()
		s.stateLease = nil
	}
	if s.input != nil {
		s.closeErr = s.input.Close()
		s.input = nil
	}
	s.inputBatch.ReleaseOwned()
	s.finishActive(s.exhausted && s.terminalErr == nil)
	return s.closeErr
}
