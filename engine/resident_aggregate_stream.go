package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/planner"
	"gocolumn/source"
	"math"
	"time"
	"unsafe"
)

// NewResidentAggregateStream creates the explicit MemoryOnly/Auto resident
// hash strategy on the same physical BatchStream graph as bounded execution.
// Retained group state may grow with cardinality, but input and output remain
// incremental. Result and DataFrame consumers use this same physical operator.
func (p *PreparedQuery) NewResidentAggregateStream(ctx context.Context, schema memory.Schema, outputRows int) (source.BatchStream, error) {
	var readOpts source.ReadOptions
	if p != nil && p.e != nil {
		readOpts.Kernels = p.e.kernels
		readOpts.BatchRows = p.e.opts.Pipeline.MaxMorselRows
		readOpts.BatchBytes = p.e.opts.Pipeline.MaxMorselBytes
	}
	return p.NewResidentAggregateStreamWithReadOptions(ctx, schema, outputRows, readOpts)
}

// NewResidentAggregateStreamWithReadOptions preserves scan limits and memory ownership
// through the aggregate child graph. Kernels must match the prepared query.
func (p *PreparedQuery) NewResidentAggregateStreamWithReadOptions(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions) (source.BatchStream, error) {
	return p.newResidentAggregateStream(ctx, schema, outputRows, readOpts, true, false)
}

// acquirePrepared is false only when Execute already owns the plan lifecycle.
func (p *PreparedQuery) newResidentAggregateStream(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions, acquirePrepared, reuse bool) (source.BatchStream, error) {
	if p != nil && p.distinct() != nil {
		return p.newDistinctAggregateStream(ctx, schema, outputRows, readOpts, acquirePrepared)
	}
	if p == nil || p.e == nil {
		return nil, fmt.Errorf("nil prepared aggregate query")
	}
	if err := readOpts.Validate(); err != nil {
		return nil, err
	}
	if readOpts.Kernels != p.e.kernels {
		return nil, fmt.Errorf("aggregate stream kernel set differs from prepared query")
	}
	if ctx == nil {
		return nil, fmt.Errorf("resident aggregate stream context is nil")
	}
	budget := memory.QueryBudgetFromContext(ctx)
	if budget == nil {
		return nil, fmt.Errorf("resident aggregate stream requires a query memory budget")
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
	if outputRows < 1 {
		outputRows = 4096
	}
	exchange := budget.Operator("ResidentAggregateExchange", memory.MemoryInFlightBatch)
	pipelineOpts := p.e.opts.Pipeline
	if p.strategy.Kind == planner.StrategyPartitionedHash {
		// All supported hash states are commutative. Relaxing scan order lets
		// native producers decode concurrently while borrowed batches remain
		// pinned until this operator joins every reader of the current batch.
		pipelineOpts.Workers = p.strategy.Workers
		if !p.orderedStatistics() {
			pipelineOpts.Ordering = pipeline.OrderingRelaxed
		} else {
			pipelineOpts.Ordering = pipeline.OrderingStable
		}
	}
	if p.orderedStatistics() {
		pipelineOpts.Ordering = pipeline.OrderingStable
	}
	input, err := pipeline.OpenBatchStream(ctx, p.src, readOpts, p.parts, exchange, pipelineOpts, p.e.opts.Observer)
	if err != nil {
		return nil, err
	}
	stream := &residentAggregateStream{
		query: p, schema: schema, budget: budget, input: input,
		output:     budget.Operator("ResidentAggregateOutput", memory.MemoryWorkspace),
		outputRows: outputRows, inputBatch: memory.NewBatch(), started: time.Now(), active: acquirePrepared,
	}
	if reuse && p.reuseEligible() {
		stream.reuse = p.e.cache.take(ctx, p, budget)
		if stream.reuse == nil {
			stream.reuse, err = newResidentReuseUnit(ctx, p, budget)
		}
		if err == nil {
			stream.table, stream.partitioned = stream.reuse.table, stream.reuse.partitioned
		}
	} else if p.strategy.Kind == planner.StrategyPartitionedHash {
		stream.partitioned, err = newResidentPartitionedState(ctx, p, budget)
	} else {
		state := budget.Operator("ResidentHashAggregate", memory.MemoryRetainedState)
		stream.table, err = newResidentTableOwner(ctx, p, state, p.cq.CardinalityHint)
	}
	if err != nil {
		return nil, errors.Join(err, input.Close())
	}
	stream.physicalKind, stream.physicalWorkers = planner.StrategyBitmapDirect, 1
	if stream.partitioned != nil {
		stream.partitioned.prepareWave(ctx, readOpts)
		stream.physicalKind, stream.physicalWorkers = planner.StrategyPartitionedHash, stream.partitioned.workers
		stream.statePartitions = len(stream.partitioned.owners)
		if wave := stream.partitioned.wave; wave != nil {
			stream.waveRows, stream.waveBytes = wave.capacity, wave.lease.Bytes()
		}
	} else if p.strategy.Kind == planner.StrategyDirectFused && p.cq.SimpleComparison != nil {
		stream.physicalKind = planner.StrategyDirectFused
	}
	stream.formatter = spillableAggregateStream{query: p, schema: schema}
	stream.predicate.manager = budget.Operator("ResidentAggregatePredicate", memory.MemoryWorkspace)
	stream.predicate.operator = "ResidentAggregatePredicate"

	active = false
	return stream, nil
}

type residentAggregateStream struct {
	physicalKind              planner.Kind
	physicalWorkers           int
	statePartitions, waveRows int
	waveBytes                 int64
	query                     *PreparedQuery
	schema                    memory.Schema
	budget                    *memory.QueryBudget
	input                     source.BatchStream
	reuse                     *residentReuseUnit
	table                     *aggregateTableOwner
	partitioned               *residentPartitionedState
	output                    *memory.OperatorBudget
	outputRows                int
	inputBatch                *memory.Batch
	formatter                 spillableAggregateStream
	predicate                 aggregatePredicateWorkspace
	cursor                    int
	outputDst                 *memory.Batch
	outputLease               memory.Reservation
	recordLease               memory.Reservation

	selectedRows int
	checksum     int64
	groups       int
	started      time.Time
	prepared     bool
	active       bool
	closed       bool
	cleaned      bool
	exhausted    bool
	terminalErr  error
	closeErr     error
}

func (s *residentAggregateStream) consume(ctx context.Context) (err error) {
	defer func() {
		if s.input != nil {
			err = errors.Join(err, s.input.Close())
			s.input = nil
		}
	}()
	for {
		ok, err := s.input.Next(ctx, s.inputBatch)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if s.partitioned == nil && s.query.strategy.Kind == planner.StrategyDirectFused && s.query.cq.SimpleComparison != nil {
			if err := s.predicate.ensureFused(ctx); err != nil {
				return err
			}
		}
		var selected int
		var checksum int64
		if s.partitioned != nil {
			binding, bindErr := s.formatter.bindBatch(s.inputBatch)
			if bindErr != nil {
				return bindErr
			}
			binding.ordinalBase, err = s.predicate.nextOrdinal(s.inputBatch.Rows)
			if err != nil {
				return err
			}
			selection, evaluateErr := s.predicate.evaluate(ctx, s.query.cq.Predicate, s.inputBatch, s.query.e.kernels)
			if evaluateErr != nil {
				return evaluateErr
			}
			selected, checksum, err = s.partitioned.aggregateBatch(ctx, binding, selection.Truth, s.inputBatch.Rows)
		} else {
			selected, _, checksum, err = s.query.aggregateResidentBatch(ctx, s.inputBatch, s.table.table, &s.predicate)
		}
		if err != nil {
			return err
		}

		s.selectedRows += selected
		s.checksum += checksum

	}
	if s.partitioned != nil {
		checksum, err := s.partitioned.finishInput(ctx)
		if err != nil {
			return err
		}
		s.checksum += checksum
		s.groups = s.partitioned.len()
	} else {
		s.groups = s.table.table.len()
	}
	s.prepared = true
	return nil
}

func (s *residentAggregateStream) releaseOutput() {
	if s.outputDst != nil {
		s.outputDst.ClearColumns()
		s.outputDst = nil
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
	if s.recordLease != nil {
		s.recordLease.Release()
		s.recordLease = nil
	}
}

func (s *residentAggregateStream) finishActive() {
	if s.active {
		s.active = false
		s.query.end()
	}
}

func (s *residentAggregateStream) abort(err error) error {
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	s.terminalErr = errors.Join(s.terminalErr, s.cleanup())
	return s.terminalErr
}

// Next emits at most outputRows groups by advancing the hash-table cursor.
func (s *residentAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("resident aggregate destination is nil")
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
		return false, s.cleanup()
	}
	if ctx == nil {
		return false, s.abort(fmt.Errorf("resident aggregate context is nil"))
	}
	var ownershipErr error
	ctx, ownershipErr = aggregateStreamContext(ctx, s.budget)
	if ownershipErr != nil {
		return false, s.abort(ownershipErr)
	}
	if err := ctx.Err(); err != nil {
		return false, s.abort(err)
	}
	if !s.prepared {
		if err := s.consume(ctx); err != nil {
			return false, s.abort(err)
		}
	}
	recordCapacity := minInt(s.outputRows, s.groups)
	recordBytes := int64(recordCapacity) * int64(unsafe.Sizeof(aggregateRecord{}))
	recordLease, err := s.output.ReserveGrowth(ctx, recordBytes)
	if err != nil {
		return false, s.abort(fmt.Errorf("reserve resident aggregate record cursor: %w", err))
	}
	s.recordLease = recordLease
	records := make([]aggregateRecord, 0, recordCapacity)
	done, err := s.visitChunk(s.outputRows, func(record aggregateRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		return false, s.abort(err)
	}
	if len(records) == 0 {
		s.releaseOutput()
		s.exhausted = true
		s.recordStats()
		return false, s.cleanup()
	}
	plainStringBytes := int64(0)
	for _, record := range records {
		if s.query.cq.GroupShape == GroupShapeGeneric {
			size, err := genericGroupPayloadSize(record.stringKey, s.query.cq.GenericFields, -1)
			if err != nil {
				return false, s.abort(err)
			}
			if size > math.MaxInt64-plainStringBytes {
				return false, s.abort(fmt.Errorf("generic output size overflow"))
			}
			plainStringBytes += size
			continue
		}
		if int64(len(record.stringKey)) > math.MaxInt64-plainStringBytes {
			return false, s.abort(fmt.Errorf("aggregate output string bytes overflow"))
		}
		plainStringBytes += int64(len(record.stringKey))
	}
	stringCapacity := plainStringBytes
	if s.query.cq.GroupShape == GroupShapeGeneric {
		stringCapacity = 0
	}
	bytes, err := aggregateOutputBytes(s.schema, len(records), stringCapacity)
	if err == nil && s.query.cq.GroupShape == GroupShapeGeneric {
		if plainStringBytes > math.MaxInt64-bytes {
			return false, s.abort(fmt.Errorf("generic output size overflow"))
		}
		bytes += plainStringBytes
	}
	if err != nil {
		return false, s.abort(err)
	}
	lease, err := s.output.ReserveGrowth(ctx, bytes)
	if err != nil {
		return false, s.abort(fmt.Errorf("reserve resident aggregate output: %w", err))
	}
	if plainStringBytes > int64(math.MaxInt) {
		lease.Release()
		return false, s.abort(fmt.Errorf("aggregate output strings exceed addressable memory"))
	}
	s.outputLease, s.outputDst = lease, dst
	if err := s.formatter.allocateOutputBatch(dst, len(records), int(stringCapacity)); err != nil {
		return false, s.abort(err)
	}
	if s.query.cq.GroupShape == GroupShapeGeneric {
		for index, field := range s.query.cq.GenericFields {
			if field.Type != memory.TypeString && field.Type != memory.TypeBinary {
				continue
			}
			size := int64(0)
			for _, record := range records {
				n, err := genericGroupPayloadSize(record.stringKey, s.query.cq.GenericFields, index)
				if err != nil {
					return false, s.abort(err)
				}
				if n > int64(math.MaxInt)-size {
					return false, s.abort(fmt.Errorf("generic output capacity overflow"))
				}
				size += n
			}
			dst.Strings[field.Name].Data = make([]byte, 0, int(size))
		}
	}
	for row, record := range records {
		if err := s.formatter.setOutputRecord(dst, row, record); err != nil {
			return false, s.abort(err)
		}
	}
	s.outputLease = lease
	if done {
		s.exhausted = true
		s.recordStats()
	}
	return true, nil
}

func (s *residentAggregateStream) recordStats() {
	if s == nil || s.query == nil {
		return
	}
	if s.query.feedbackKey != "" {
		s.query.e.feedback.observe(s.query.feedbackKey, s.query.stats.Rows, s.selectedRows, s.groups)
	}
	_, workers := s.physicalStrategy()
	s.query.e.opts.Observer.Observe(observer.Event{
		Stage: "resident-stream-aggregate", Duration: time.Since(s.started), Rows: s.query.stats.Rows,
		Workers: workers, Attributes: s.StreamDiagnostics(),
	})
}

func (s *residentAggregateStream) physicalStrategy() (planner.Kind, int) {
	return s.physicalKind, s.physicalWorkers
}

// StreamDiagnostics reports resident aggregate strategy and runtime counters.
func (s *residentAggregateStream) StreamDiagnostics() map[string]string {
	peak := int64(0)
	if s.budget != nil {
		peak = s.budget.Snapshot().PeakBytes
	}
	strategy, workers := s.physicalStrategy()
	attributes := map[string]string{
		"strategy": strategy.String(), "workers": fmt.Sprint(workers), "groups": fmt.Sprint(s.groups),
		"selected_rows": fmt.Sprint(s.selectedRows), "checksum": fmt.Sprint(s.checksum), "peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(peak), "spill_runs": "0",
		"kernel_mode": s.query.e.kernels.Capabilities().Mode().String(), "kernel_arch": s.query.e.kernels.Capabilities().Arch(),
	}
	if s.physicalKind == planner.StrategyPartitionedHash {
		attributes["state_partitions"] = fmt.Sprint(s.statePartitions)
		attributes["aggregate_wave_rows"] = fmt.Sprint(s.waveRows)
		attributes["aggregate_wave_bytes"] = fmt.Sprint(s.waveBytes)
	}
	return attributes
}

// Close releases input, hash state, output reservations, and prepared-query
// ownership exactly once.
func (s *residentAggregateStream) Close() error {
	if s == nil {
		return nil
	}
	s.closed = true
	return s.cleanup()
}

// cleanup releases ownership after either EOF/error or explicit Close without
// changing the public terminal state. Repeated errors stay observable until Close.
func (s *residentAggregateStream) cleanup() error {
	if s == nil || s.cleaned {
		if s == nil {
			return nil
		}
		return s.closeErr
	}
	s.cleaned = true
	s.predicate.close()

	var err error
	if s.inputBatch != nil {
		s.inputBatch.ClearColumns()
		s.inputBatch = nil
	}
	if s.input != nil {
		err = errors.Join(err, s.input.Close())
		s.input = nil
	}
	s.releaseOutput()
	if s.reuse != nil {
		u := s.reuse
		s.reuse, s.partitioned, s.table = nil, nil, nil
		if s.exhausted && s.terminalErr == nil && err == nil && u.state.ctx != nil && u.state.ctx.Err() == nil {
			s.query.e.cache.put(s.query, u)
		} else {
			u.destroy()
		}
	} else {
		if s.partitioned != nil {
			s.partitioned.close()
			s.partitioned = nil
		}
		if s.table != nil {
			s.table.close()
			s.table = nil
		}
	}
	s.finishActive()
	s.closeErr = err
	return err
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

var _ source.BatchStream = (*residentAggregateStream)(nil)
var _ source.BatchStreamDiagnostics = (*residentAggregateStream)(nil)

// Stats returns execution diagnostics for the shared aggregate consumer.
func (s *residentAggregateStream) Stats() AggregateStreamStats {
	return AggregateStreamStats{SelectedRows: s.selectedRows, Groups: s.groups, Checksum: s.checksum, Elapsed: time.Since(s.started)}
}

func (s *residentAggregateStream) visit(fn func(aggregateRecord) error) error {
	if s.partitioned != nil {
		return s.partitioned.visit(fn)
	}
	return s.table.table.visit(fn)
}
func (s *residentAggregateStream) visitChunk(limit int, fn func(aggregateRecord) error) (bool, error) {
	if s.partitioned != nil {
		return s.partitioned.visitChunk(limit, fn)
	}
	return s.table.table.visitChunk(&s.cursor, limit, fn)
}
