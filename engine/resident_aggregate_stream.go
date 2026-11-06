package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source"
	"math"
	"time"
	"unsafe"
)

// NewResidentAggregateStream creates the explicit MemoryOnly/Auto resident
// hash strategy on the same physical BatchStream graph as bounded execution.
// Retained group state may grow with cardinality, but input and output remain
// incremental and no engine.Result compatibility materialization is used.
func (p *PreparedQuery) NewResidentAggregateStream(ctx context.Context, schema memory.Schema, outputRows int) (source.BatchStream, error) {
	var readOpts source.ReadOptions
	if p != nil && p.e != nil {
		readOpts.Kernels = p.e.kernels
	}
	return p.NewResidentAggregateStreamWithReadOptions(ctx, schema, outputRows, readOpts)
}

// NewResidentAggregateStreamWithReadOptions preserves scan limits and memory ownership
// through the aggregate child graph. Kernels must match the prepared query.
func (p *PreparedQuery) NewResidentAggregateStreamWithReadOptions(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions) (source.BatchStream, error) {
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
	if outputRows < 1 {
		outputRows = 4096
	}
	exchange := budget.Operator("ResidentAggregateExchange", memory.MemoryInFlightBatch)
	input, err := pipeline.OpenBatchStream(ctx, p.src, readOpts, p.parts, exchange, p.e.opts.Pipeline, p.e.opts.Observer)
	if err != nil {
		return nil, err
	}
	state := budget.Operator("ResidentHashAggregate", memory.MemoryRetainedState)
	owner := &aggregateTableOwner{}
	reserve := func(bytes int64) error {
		lease, reserveErr := state.ReserveGrowth(ctx, bytes)
		if reserveErr != nil {
			return reserveErr
		}
		owner.reservations = append(owner.reservations, lease)
		owner.retained += bytes
		return nil
	}
	table, err := newAggregateStateTable(p.cq.GroupShape, p.cq.AggregateLayout, p.cq.CardinalityHint, p.cq.AverageKeyBytes, 0, reserve)
	if err != nil {
		_ = input.Close()
		owner.close()
		return nil, err
	}
	owner.table = table
	stream := &residentAggregateStream{
		query: p, schema: schema, budget: budget, input: input, table: owner,
		output:     budget.Operator("ResidentAggregateOutput", memory.MemoryWorkspace),
		outputRows: outputRows, inputBatch: memory.NewBatch(), started: time.Now(), active: true,
	}
	stream.formatter = spillableAggregateStream{query: p, schema: schema}
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

type residentAggregateStream struct {
	query               *PreparedQuery
	schema              memory.Schema
	budget              *memory.QueryBudget
	input               source.BatchStream
	table               *aggregateTableOwner
	output              *memory.OperatorBudget
	outputRows          int
	inputBatch          *memory.Batch
	formatter           spillableAggregateStream
	workspace           expr.EvalWorkspace
	dynamicReservations []memory.Reservation
	cursor              int
	outputLease         memory.Reservation
	recordLease         memory.Reservation

	selectedRows int
	checksum     int64
	groups       int
	started      time.Time
	prepared     bool
	active       bool
	closed       bool
	exhausted    bool
	terminalErr  error
	closeErr     error
}

func (s *residentAggregateStream) consume(ctx context.Context) error {
	defer func() {
		if s.input != nil {
			_ = s.input.Close()
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
		binding, err := s.formatter.bindBatch(s.inputBatch)
		if err != nil {
			return err
		}
		selection, err := s.query.cq.Predicate.Evaluate(s.inputBatch, &s.workspace, s.query.e.kernels)
		if err != nil {
			return err
		}
		err = forSelectedFrom(selection.Truth, s.inputBatch.Rows, 0, func(row int) error {
			record := binding.record(s.query.cq.GroupShape, row)
			value, valid := binding.value(row)
			if _, addErr := s.table.table.add(record, value, valid); addErr != nil {
				return addErr
			}
			s.selectedRows++
			if valid {
				s.checksum += value
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	s.groups = s.table.table.len()
	s.prepared = true
	return nil
}

func (s *residentAggregateStream) releaseOutput() {
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
	_ = s.Close()
	return err
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
		return false, nil
	}
	if ctx == nil {
		return false, s.abort(fmt.Errorf("resident aggregate context is nil"))
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
	done, err := s.table.table.visitChunk(&s.cursor, s.outputRows, func(record aggregateRecord) error {
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
		s.finishActive()
		return false, nil
	}
	plainStringBytes := int64(0)
	for _, record := range records {
		if int64(len(record.stringKey)) > math.MaxInt64-plainStringBytes {
			return false, s.abort(fmt.Errorf("aggregate output string bytes overflow"))
		}
		plainStringBytes += int64(len(record.stringKey))
	}
	bytes, err := aggregateOutputBytes(s.schema, len(records), plainStringBytes)
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
	if err := s.formatter.allocateOutputBatch(dst, len(records), int(plainStringBytes)); err != nil {
		lease.Release()
		return false, s.abort(err)
	}
	for row, record := range records {
		if err := s.formatter.setOutputRecord(dst, row, record); err != nil {
			dst.ClearColumns()
			lease.Release()
			return false, s.abort(err)
		}
	}
	s.outputLease = lease
	if done {
		s.exhausted = true
		s.recordStats()
		s.finishActive()
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
	s.query.e.opts.Observer.Observe(observer.Event{
		Stage: "resident-stream-aggregate", Duration: time.Since(s.started), Rows: s.query.stats.Rows,
		Attributes: s.StreamDiagnostics(),
	})
}

// StreamDiagnostics reports resident aggregate strategy and runtime counters.
func (s *residentAggregateStream) StreamDiagnostics() map[string]string {
	peak := int64(0)
	if s.budget != nil {
		peak = s.budget.Snapshot().PeakBytes
	}
	return map[string]string{
		"strategy": "resident-stream", "groups": fmt.Sprint(s.groups),
		"selected_rows": fmt.Sprint(s.selectedRows), "checksum": fmt.Sprint(s.checksum), "peak_memory_bytes": fmt.Sprint(peak), "spill_runs": "0",
		"kernel_mode": s.query.e.kernels.Capabilities().Mode().String(), "kernel_arch": s.query.e.kernels.Capabilities().Arch(),
	}
}

// Close releases input, hash state, output reservations, and prepared-query
// ownership exactly once.
func (s *residentAggregateStream) Close() error {
	if s == nil || s.closed {
		if s == nil {
			return nil
		}
		return s.closeErr
	}
	s.closed = true
	for _, r := range s.dynamicReservations {
		r.Release()
	}
	s.dynamicReservations = nil
	var err error
	if s.input != nil {
		err = errors.Join(err, s.input.Close())
		s.input = nil
	}
	if s.inputBatch != nil {
		s.inputBatch.ReleaseOwned()
		s.inputBatch = nil
	}
	s.releaseOutput()
	if s.table != nil {
		s.table.close()
		s.table = nil
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
