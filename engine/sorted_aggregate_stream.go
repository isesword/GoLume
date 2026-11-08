package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/planner"
	"gocolumn/source"
	"time"
	"unsafe"
)

// IsSortedAggregate reports whether preparation selected adjacent-key state.
func (p *PreparedQuery) IsSortedAggregate() bool {
	return p != nil && p.strategy.Kind == planner.StrategySorted
}

// NewSortedAggregateStream creates a one-group-state stream over proven
// globally ordered input. It does not require a spill store.
func (p *PreparedQuery) NewSortedAggregateStream(ctx context.Context, schema memory.Schema, outputRows int) (source.BatchStream, error) {
	var readOpts source.ReadOptions
	if p != nil && p.e != nil {
		readOpts.Kernels = p.e.kernels
		readOpts.BatchRows = p.e.opts.Pipeline.MaxMorselRows
		readOpts.BatchBytes = p.e.opts.Pipeline.MaxMorselBytes
	}
	return p.NewSortedAggregateStreamWithReadOptions(ctx, schema, outputRows, readOpts)
}

// NewSortedAggregateStreamWithReadOptions preserves scan limits and memory ownership
// through the aggregate child graph. Kernels must match the prepared query.
func (p *PreparedQuery) NewSortedAggregateStreamWithReadOptions(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions) (source.BatchStream, error) {
	return p.newSortedAggregateStream(ctx, schema, outputRows, readOpts, true)
}

// acquirePrepared is false only when Execute already owns the plan lifecycle.
func (p *PreparedQuery) newSortedAggregateStream(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions, acquirePrepared bool) (source.BatchStream, error) {
	if p != nil && p.distinct() != nil {
		return p.newDistinctAggregateStream(ctx, schema, outputRows, readOpts, acquirePrepared)
	}
	if p == nil || p.e == nil || !p.IsSortedAggregate() {
		return nil, fmt.Errorf("sorted aggregate stream requires a prepared sorted strategy")
	}
	if err := readOpts.Validate(); err != nil {
		return nil, err
	}
	if readOpts.Kernels != p.e.kernels {
		return nil, fmt.Errorf("aggregate stream kernel set differs from prepared query")
	}
	if ctx == nil {
		return nil, fmt.Errorf("sorted aggregate stream context is nil")
	}
	budget := memory.QueryBudgetFromContext(ctx)
	if budget == nil {
		return nil, fmt.Errorf("sorted aggregate stream requires a query memory budget")
	}
	if p.executionPolicy == ExecutionBounded && budget.Limit() <= 0 {
		return nil, ErrBoundedMemoryLimitRequired
	}
	if acquirePrepared {
		if err := p.begin(); err != nil {
			return nil, err
		}
	}
	active := acquirePrepared
	defer func() {
		if active {
			p.end()
		}
	}()
	if outputRows < 1 {
		outputRows = p.e.opts.Pipeline.MaxMorselRows
	}
	if p.cq.GroupShape == GroupShapeGlobal {
		outputRows = 1
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
	pipelineOpts := p.e.opts.Pipeline
	pipelineOpts.Ordering = pipeline.OrderingStable
	input, err := pipeline.OpenBatchStream(ctx, p.src, readOpts, p.parts, exchange, pipelineOpts, p.e.opts.Observer)
	if err != nil {
		return nil, err
	}
	stateBudget := budget.Operator("SortedAggregateState", memory.MemoryRetainedState)
	stateBytes := int64(128)
	if p.hasByteStatistics() {
		stateBytes += int64(len(p.cq.Aggregates)) * int64(unsafe.Sizeof(memory.Reservation(nil)))
	}
	if p.cq.MultiAggregate == nil {
		stateBytes += aggregate.Int64VectorBytesForSlots(1, p.cq.AggregateLayout)
	}
	stateLease, err := stateBudget.ReserveGrowth(ctx, stateBytes)
	if err != nil {
		return nil, errors.Join(err, input.Close())
	}
	stream := &sortedAggregateStream{
		query: p, schema: schema, budget: budget, input: input,
		stateBudget: stateBudget, outputBudget: budget.Operator("SortedAggregateOutput", memory.MemoryWorkspace),
		stateLease: stateLease, outputRows: outputRows, inputBatch: memory.NewBatch(),
		started: time.Now(), active: acquirePrepared,
	}
	if p.nativeAggregate() && len(p.cq.MultiAggregate.Columns) > 0 {
		stream.numeric, err = aggregate.NewNumericBundle(p.cq.MultiAggregate.Types, p.multiLayouts(), func(bytes int64) (memory.Reservation, error) { return stateBudget.ReserveGrowth(ctx, bytes) })
		if err == nil {
			err = stream.numeric.EnsureGroups(1)
		}
		if err != nil {
			if stream.numeric != nil {
				stream.numeric.Close()
			}
			stateLease.Release()
			return nil, errors.Join(err, input.Close())
		}
	} else if p.cq.MultiAggregate != nil && !p.nativeAggregate() {
		stream.bundle, err = aggregate.NewInt64Bundle(p.multiLayouts(), func(bytes int64) (memory.Reservation, error) { return stateBudget.ReserveGrowth(ctx, bytes) })
		if err == nil {
			err = stream.bundle.EnsureGroups(1)
		}
		if err != nil {
			if stream.bundle != nil {
				stream.bundle.Close()
			}
			stateLease.Release()
			return nil, errors.Join(err, input.Close())
		}
	} else if p.cq.MultiAggregate == nil {
		stream.state = aggregate.NewInt64Vector(1, p.cq.AggregateLayout)
	}
	if st := p.statistics(); st != nil {
		stream.statistics, err = aggregate.NewStatisticsBundle(st.Types, st.Layouts, func(bytes int64) (memory.Reservation, error) { return stateBudget.ReserveGrowth(ctx, bytes) })
		if err == nil {
			err = stream.statistics.EnsureGroups(1)
		}
		if err != nil {
			if stream.statistics != nil {
				stream.statistics.Close()
			}
			if stream.numeric != nil {
				stream.numeric.Close()
			}
			stateLease.Release()
			return nil, errors.Join(err, input.Close())
		}
	}
	if p.hasByteStatistics() {
		stream.endpointOutputLeases = make([]memory.Reservation, len(p.cq.Aggregates))
	}
	if err = stream.byteWorkspace.init(p.statistics(), func(n int64) (memory.Reservation, error) { return stateBudget.ReserveGrowth(ctx, n) }); err != nil {
		if stream.statistics != nil {
			stream.statistics.Close()
		}
		if stream.numeric != nil {
			stream.numeric.Close()
		}
		stateLease.Release()
		return nil, errors.Join(err, input.Close())
	}
	if err = stream.initGlobalAggregateBindings(ctx); err != nil {
		// The constructor's defer still owns the prepared-query reference.
		stream.active = false
		return nil, errors.Join(err, stream.cleanup())
	}
	// Seed the empty global group without adding a row: COUNT(*) must remain
	// zero and SUM/MIN/MAX/AVG retain their SQL NULL identity.
	stream.haveGroup = p.cq.GroupShape == GroupShapeGlobal
	stream.predicate.manager = budget.Operator("SortedAggregatePredicate", memory.MemoryWorkspace)
	stream.predicate.operator = "SortedAggregatePredicate"

	active = false
	return stream, nil
}

type sortedAggregateStream struct {
	byteWorkspace        byteStatisticsWorkspace
	endpointOutputLeases []memory.Reservation
	canonicalScratch     []byte
	canonicalLease       memory.Reservation

	outputDst         *memory.Batch
	cleaned           bool
	statsRecorded     bool
	recordSink        func(aggregateRecord) error
	query             *PreparedQuery
	schema            memory.Schema
	budget            *memory.QueryBudget
	input             source.BatchStream
	stateBudget       *memory.OperatorBudget
	outputBudget      *memory.OperatorBudget
	stateLease        memory.Reservation
	keyLease          memory.Reservation
	outputLease       memory.Reservation
	outputStringLease memory.Reservation
	outputRows        int

	globalBindings     []globalAggregateLaneBinding
	globalBindingLease memory.Reservation

	inputBatch *memory.Batch
	binding    aggregateBatchBinding
	selection  memory.Bitmap
	nextRow    int
	predicate  aggregatePredicateWorkspace
	state      aggregate.Int64Vector
	bundle     *aggregate.Int64Bundle
	numeric    *aggregate.NumericBundle
	statistics *aggregate.StatisticsBundle
	current    aggregateRecord
	haveGroup  bool
	inputDone  bool

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
	for i, lease := range s.endpointOutputLeases {
		if lease != nil {
			lease.Release()
			s.endpointOutputLeases[i] = nil
		}
	}
	if s.outputDst != nil {
		s.outputDst.ClearColumns()
		s.outputDst = nil
	}
	if s.outputStringLease != nil {
		s.outputStringLease.Release()
		s.outputStringLease = nil
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
}

func (s *sortedAggregateStream) releaseKey() {
	if s.keyLease != nil {
		s.keyLease.Release()
		s.keyLease = nil
	}
}

func (s *sortedAggregateStream) startGroup(ctx context.Context, key aggregateKey, value int64, valid bool) error {
	s.releaseKey()
	s.current = aggregateRecord{a: key.a, b: key.b, validMask: key.validMask}
	if (s.query.cq.GroupShape == GroupShapeStringView || s.query.cq.GroupShape == GroupShapeGeneric) && key.validMask&1 != 0 {
		lease, err := s.stateBudget.ReserveGrowth(ctx, int64(len(key.stringKey)))
		if err != nil {
			return fmt.Errorf("reserve sorted aggregate string key: %w", err)
		}
		s.keyLease = lease
		s.current.stringKey = append([]byte(nil), key.stringKey...)
	}
	if s.numeric != nil {
		if err := s.numeric.ClearGroup(0); err != nil {
			return err
		}
	}
	if s.bundle != nil {
		s.bundle.ClearGroup(0)
	}
	if s.statistics != nil {
		if err := s.statistics.ClearGroup(0); err != nil {
			return err
		}
	}
	if s.query.cq.MultiAggregate != nil {
		if err := s.addBundleRow(); err != nil {
			return err
		}
	} else {
		s.state.InitValue(0, value, valid)
	}
	s.haveGroup = true
	return nil
}

func (s *sortedAggregateStream) ensureOutputString(ctx context.Context, dst *memory.Batch, record aggregateRecord) error {
	if s.query.cq.GroupShape == GroupShapeGeneric {
		return s.ensureGenericOutputStrings(ctx, dst, record.stringKey)
	}
	if s.query.cq.GroupShape != GroupShapeStringView || record.validMask&1 == 0 {
		return nil
	}
	name := s.query.cq.GroupBy[0].Name
	vector := dst.Strings[name]
	need := len(vector.Data) + len(record.stringKey)
	if need <= cap(vector.Data) {
		return nil
	}
	if cap(vector.Data) > int(^uint(0)>>1)/2 {
		return fmt.Errorf("aggregate output string capacity exceeds addressable memory")
	}
	newCapacity := cap(vector.Data) * 2
	if newCapacity < 64 {
		newCapacity = 64
	}
	if newCapacity < need {
		newCapacity = need
	}
	lease, err := s.outputBudget.ReserveGrowth(ctx, int64(newCapacity))
	if err != nil {
		return fmt.Errorf("reserve sorted aggregate output strings: %w", err)
	}
	data := make([]byte, len(vector.Data), newCapacity)
	copy(data, vector.Data)
	vector.Data = data
	if s.outputStringLease != nil {
		s.outputStringLease.Release()
	}
	s.outputStringLease = lease
	return nil
}

func (s *sortedAggregateStream) flushGroup(ctx context.Context, dst *memory.Batch, row int, formatter *spillableAggregateStream) error {
	if !s.haveGroup {
		return nil
	}
	if s.numeric != nil {
		s.current.numericBundle = s.numeric
		s.current.groupID = 0
	} else if s.bundle != nil {
		s.current.state = s.bundle.Result(0, 0)
		s.current.bundle = s.bundle
		s.current.groupID = 0
	} else if s.statistics == nil {
		s.current.state = s.state.Result(0)
	}
	s.current.statisticsBundle = s.statistics
	if dst == nil {
		if s.recordSink != nil {
			if err := s.recordSink(s.current); err != nil {
				return err
			}
		}
	} else {
		if err := s.ensureEndpointOutput(ctx, dst, s.current); err != nil {
			return err
		}
		if err := s.ensureOutputString(ctx, dst, s.current); err != nil {
			return err
		}
		if err := formatter.setOutputRecord(dst, row, s.current); err != nil {
			return err
		}
	}
	s.groups++
	s.haveGroup = false
	s.releaseKey()
	return nil
}

func (s *sortedAggregateStream) loadInput(ctx context.Context) error {
	for !s.inputDone {
		// The producer may recycle the previous borrowed batch on Next.
		clear(s.globalBindings)
		s.predicate.keys.clear()
		s.binding = aggregateBatchBinding{}
		s.selection = nil
		ok, err := s.input.Next(ctx, s.inputBatch)
		if err != nil {
			s.inputBatch.ClearColumns()
			return err
		}
		if !ok {
			s.inputDone = true
			s.inputBatch.ClearColumns()
			return nil
		}
		selection, err := s.predicate.evaluate(ctx, s.query.cq.Predicate, s.inputBatch, s.query.e.kernels)
		if err != nil {
			return err
		}
		binding, err := s.query.bindSortedBatch(ctx, s.inputBatch, &s.predicate)
		if err != nil {
			return err
		}
		binding.ordinalBase, err = s.predicate.nextOrdinal(s.inputBatch.Rows)
		if err != nil {
			return err
		}
		s.selection = selection.Truth
		s.binding = binding
		s.bindGlobalAggregateInputs()
		s.nextRow = 0
		for row := 0; row < s.inputBatch.Rows; row++ {
			if memory.IsSet(s.selection, row) {
				return nil
			}
		}
	}
	return nil
}

func (s *sortedAggregateStream) nextSelected(ctx context.Context) (aggregateKey, int64, bool, bool, error) {
	for {
		for s.nextRow < s.inputBatch.Rows {
			row := s.nextRow
			s.nextRow++
			if !memory.IsSet(s.selection, row) {
				continue
			}
			key := aggregateKey{}
			if s.query.cq.GroupShape == GroupShapeGeneric {
				encoded, err := s.canonicalRowKey(ctx, row)
				if err != nil {
					return aggregateKey{}, 0, false, false, err
				}
				key = aggregateKey{validMask: 1, stringKey: encoded}
			} else {
				key = s.binding.key(s.query.cq.GroupShape, row)
			}
			value, valid := s.binding.value(row)
			return key, value, valid, true, nil
		}
		if s.inputDone {
			return aggregateKey{}, 0, false, false, nil
		}
		if err := s.loadInput(ctx); err != nil {
			return aggregateKey{}, 0, false, false, err
		}
	}
}

func shrinkAggregateOutput(dst *memory.Batch, schema memory.Schema, rows int) {
	for _, field := range schema.Fields {
		switch field.Type {
		case memory.TypeInt8:
			dst.Int8s[field.Name].Values = dst.Int8s[field.Name].Values[:rows]
		case memory.TypeInt16:
			dst.Int16s[field.Name].Values = dst.Int16s[field.Name].Values[:rows]
		case memory.TypeUint8:
			dst.Uint8s[field.Name].Values = dst.Uint8s[field.Name].Values[:rows]
		case memory.TypeUint16:
			dst.Uint16s[field.Name].Values = dst.Uint16s[field.Name].Values[:rows]
		case memory.TypeFloat32:
			dst.Float32s[field.Name].Values = dst.Float32s[field.Name].Values[:rows]
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			dst.Int32s[field.Name].Values = dst.Int32s[field.Name].Values[:rows]
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			dst.Int64s[field.Name].Values = dst.Int64s[field.Name].Values[:rows]
		case memory.TypeUint32, memory.TypeBool:
			dst.Uint32s[field.Name].Values = dst.Uint32s[field.Name].Values[:rows]
		case memory.TypeUint64:
			dst.Uint64s[field.Name].Values = dst.Uint64s[field.Name].Values[:rows]
		case memory.TypeFloat64:
			dst.Float64s[field.Name].Values = dst.Float64s[field.Name].Values[:rows]
		case memory.TypeString, memory.TypeBinary:
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
	if success && !s.statsRecorded {
		s.statsRecorded = true
		elapsed := time.Since(s.started)
		if s.query.feedbackKey != "" {
			s.query.e.feedback.observe(s.query.feedbackKey, s.query.stats.Rows, s.selectedRows, s.groups)
		}
		s.query.e.opts.Observer.Observe(observer.Event{Stage: "sorted-aggregate", Duration: elapsed, Rows: s.query.stats.Rows, Workers: 1, Attributes: s.StreamDiagnostics()})
	}
	if s.active {
		s.active = false
		s.query.end()
	}
}

func (s *sortedAggregateStream) fail(err error) error {
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	s.terminalErr = errors.Join(s.terminalErr, s.cleanup())
	return s.terminalErr
}

// Next emits up to outputRows completed adjacent-key groups.
func (s *sortedAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("sorted aggregate stream destination is nil")
	}
	return s.next(ctx, dst)
}

// next advances the same one-group-state machine for columnar and record sinks.
// A nil destination selects the terminal record consumer without output vectors.
func (s *sortedAggregateStream) next(ctx context.Context, dst *memory.Batch) (bool, error) {
	clearAggregateOutput(dst)
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, s.cleanup()
	}
	if ctx == nil {
		return false, s.fail(fmt.Errorf("sorted aggregate stream context is nil"))
	}
	var ownershipErr error
	ctx, ownershipErr = aggregateStreamContext(ctx, s.budget)
	if ownershipErr != nil {
		return false, s.fail(ownershipErr)
	}
	if err := ctx.Err(); err != nil {
		return false, s.fail(err)
	}
	formatter := &spillableAggregateStream{query: s.query, schema: s.schema}
	if dst != nil {
		baseBytes, err := aggregateOutputBytes(s.schema, s.outputRows, 0)
		if err != nil {
			return false, s.fail(err)
		}
		lease, err := s.outputBudget.ReserveGrowth(ctx, baseBytes)
		if err != nil {
			return false, s.fail(err)
		}
		s.outputLease = lease
		s.outputDst = dst
		if err := formatter.allocateOutputBatch(dst, s.outputRows, 0); err != nil {
			return false, s.fail(err)
		}
	}
	output := 0
	for output < s.outputRows {
		key, value, valid, ok, err := s.nextSelected(ctx)
		if err != nil {
			clearAggregateOutput(dst)
			return false, s.fail(err)
		}
		if !ok {
			if s.haveGroup {
				if err := s.flushGroup(ctx, dst, output, formatter); err != nil {
					clearAggregateOutput(dst)
					return false, s.fail(err)
				}
				output++
			}
			s.exhausted = true
			if s.input != nil {
				if err := s.input.Close(); err != nil {
					clearAggregateOutput(dst)
					return false, s.fail(err)
				}
				s.input = nil
			}
			break
		}
		if !s.haveGroup {
			if err := s.startGroup(ctx, key, value, valid); err != nil {
				clearAggregateOutput(dst)
				return false, s.fail(err)
			}
		} else if aggregateKeyMatches(s.query.cq.GroupShape, &s.current, key) {
			if s.query.cq.MultiAggregate != nil {
				if err := s.addBundleRow(); err != nil {
					clearAggregateOutput(dst)
					return false, s.fail(err)
				}
			} else {
				s.state.AddValue(0, value, valid)
			}
		} else {
			if err := s.flushGroup(ctx, dst, output, formatter); err != nil {
				clearAggregateOutput(dst)
				return false, s.fail(err)
			}
			output++
			if err := s.startGroup(ctx, key, value, valid); err != nil {
				clearAggregateOutput(dst)
				return false, s.fail(err)
			}
		}
		s.selectedRows++
		if valid {
			s.checksum += value
		}
	}
	if output == 0 {
		clearAggregateOutput(dst)
		s.releaseOutput()
		return false, s.cleanup()
	}
	if dst != nil {
		shrinkAggregateOutput(dst, s.schema, output)
	}
	return true, nil
}

// Close releases input, one-group state and output reservations exactly once.
func (s *sortedAggregateStream) Close() error {
	if s == nil {
		return nil
	}
	s.closed = true
	return s.cleanup()
}

func (s *sortedAggregateStream) cleanup() error {
	if s.canonicalLease != nil {
		s.canonicalLease.Release()
		s.canonicalLease = nil
	}
	s.canonicalScratch = nil

	if s.cleaned {
		return s.closeErr
	}
	s.cleaned = true
	s.binding = aggregateBatchBinding{}
	clear(s.globalBindings)
	s.globalBindings = nil
	if s.globalBindingLease != nil {
		s.globalBindingLease.Release()
		s.globalBindingLease = nil
	}
	s.selection = nil
	s.predicate.close()
	s.current = aggregateRecord{}
	s.state = aggregate.Int64Vector{}
	s.byteWorkspace.close()
	if s.statistics != nil {
		s.statistics.Close()
		s.statistics = nil
	}
	if s.numeric != nil {
		s.numeric.Close()
		s.numeric = nil
	}
	if s.bundle != nil {
		s.bundle.Close()
		s.bundle = nil
	}
	s.recordSink = nil
	s.releaseOutput()
	s.releaseKey()
	if s.stateLease != nil {
		s.stateLease.Release()
		s.stateLease = nil
	}
	if s.inputBatch != nil {
		s.inputBatch.ClearColumns()
		s.inputBatch = nil
	}
	if s.input != nil {
		s.closeErr = s.input.Close()
		s.input = nil
	}
	s.finishActive(s.exhausted && s.terminalErr == nil)
	return s.closeErr
}

// Stats returns execution diagnostics for the shared aggregate consumer.
func (s *sortedAggregateStream) Stats() AggregateStreamStats {
	return AggregateStreamStats{SelectedRows: s.selectedRows, Groups: s.groups, Checksum: s.checksum, Elapsed: time.Since(s.started)}
}

// StreamDiagnostics reports the actual adjacent-key state operator.
func (s *sortedAggregateStream) StreamDiagnostics() map[string]string {
	peak := int64(0)
	if s.budget != nil {
		peak = s.budget.Snapshot().PeakBytes
	}
	return map[string]string{
		"strategy": planner.StrategySorted.String(), "workers": "1", "groups": fmt.Sprint(s.groups),
		"selected_rows": fmt.Sprint(s.selectedRows), "checksum": fmt.Sprint(s.checksum), "peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(peak), "spill_runs": "0",
		"kernel_mode": s.query.e.kernels.Capabilities().Mode().String(), "kernel_arch": s.query.e.kernels.Capabilities().Arch(),
	}
}

var _ source.BatchStreamDiagnostics = (*sortedAggregateStream)(nil)
