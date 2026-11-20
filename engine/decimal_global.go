package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"math"
	"unsafe"
)

func compileDecimalGlobal(schema memory.Schema, q expr.Query) ([]memory.Field, error) {
	native := false
	for _, f := range schema.Fields {
		if f.Type == memory.TypeDecimal && f.DecimalPrecision != 0 {
			native = true
		}
	}
	if !native {
		return nil, nil
	}
	if len(q.GroupBy) != 0 || len(q.Aggregates) == 0 {
		return nil, fmt.Errorf("native Decimal aggregation requires global SUM only")
	}
	fields := make([]memory.Field, len(q.Aggregates))
	sums := 0
	for i, a := range q.Aggregates {
		if a.Kind == expr.AggCountRows {
			continue
		}
		f, ok := schema.Field(a.Column)
		if !ok || (a.Kind != expr.AggSum && a.Kind != expr.AggCountValues) || f.Type != memory.TypeDecimal || f.DecimalPrecision == 0 {
			return nil, fmt.Errorf("native Decimal global SUM cannot mix other aggregates")
		}
		if e := f.ValidateDecimal128(); e != nil {
			return nil, e
		}
		fields[i] = f
		if a.Kind == expr.AggSum {
			sums++
		}
	}
	if sums == 0 {
		return nil, fmt.Errorf("native Decimal mixed aggregate requires at least one SUM")
	}
	return fields, nil
}
func (p *PreparedQuery) decimalGlobal() bool {
	return p != nil && p.cq.MultiAggregate != nil && p.cq.MultiAggregate.DecimalFields != nil
}
func (p *PreparedQuery) decimalResultSchema() memory.Schema {
	fields := make([]memory.Field, len(p.cq.Aggregates))
	for i, a := range p.cq.Aggregates {
		name := a.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", i)
		}
		if a.Kind == expr.AggCountRows || a.Kind == expr.AggCountValues {
			fields[i] = memory.Field{Name: name, Type: memory.TypeUint64}
			continue
		}
		precision, scale := int32(38), p.decimalLaneFields()[i].DecimalScale
		if a.Kind == expr.AggDecimalAverage {
			o := p.cq.MultiAggregate.DecimalAverages[i]
			precision, scale = o.Precision, o.Scale
		}
		fields[i], _ = memory.Decimal128Field(name, true, precision, scale)
	}
	if p.decimalGrouped() {
		fields = append(append([]memory.Field(nil), p.cq.GenericFields...), fields...)
	}
	return memory.NewSchema(fields...)
}

type decimalGlobalStream struct {
	p                       *PreparedQuery
	budget                  *memory.QueryBudget
	input                   source.BatchStream
	states, scratch         []aggregate.DecimalSumState
	counts, countScratch    []uint64
	inputBatch              memory.Batch
	predicate               aggregatePredicateWorkspace
	lease, outputLease      memory.Reservation
	outputManager           memory.Manager
	output                  *memory.Batch
	saved                   memory.Batch
	active, closed, emitted bool
	terminal                error
	selected                int
}

func (p *PreparedQuery) newDecimalGlobalStream(ctx context.Context, schema memory.Schema, opts source.ReadOptions, acquire bool) (source.BatchStream, error) {
	return p.newDecimalGlobalStreamWithManagers(ctx, schema, opts, acquire, nil)
}

// The optional wrapper instruments the exact same admission boundaries in tests.
// Production supplies nil; it cannot weaken a query's underlying hard limit.
func (p *PreparedQuery) newDecimalGlobalStreamWithManagers(ctx context.Context, schema memory.Schema, opts source.ReadOptions, acquire bool, wrap func(memory.Manager) memory.Manager) (source.BatchStream, error) {
	manager := func(m memory.Manager) memory.Manager {
		if wrap != nil {
			return wrap(m)
		}
		return m
	}

	if ctx == nil || p.e == nil || (!p.IsSortedAggregate() && !p.decimalResident()) {
		return nil, fmt.Errorf("Decimal SUM requires prepared global sorted stream")
	}
	if e := opts.Validate(); e != nil {
		return nil, e
	}
	if opts.Kernels != p.e.kernels {
		return nil, fmt.Errorf("Decimal SUM kernel mismatch")
	}
	if memory.SchemaFingerprint(schema) != memory.SchemaFingerprint(p.executionSchema) {
		return nil, fmt.Errorf("Decimal SUM output schema mismatch")
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q == nil {
		return nil, fmt.Errorf("Decimal SUM requires query budget")
	}
	if p.executionPolicy == ExecutionBounded && q.Limit() <= 0 {
		return nil, ErrBoundedMemoryLimitRequired
	}
	if acquire {
		if e := p.begin(); e != nil {
			return nil, e
		}
	}
	active := acquire
	defer func() {
		if active {
			p.end()
		}
	}()
	n := len(p.decimalLaneFields())
	per := int64(2 * unsafe.Sizeof(aggregate.DecimalSumState{}))
	mixed := false
	for i, a := range p.cq.Aggregates {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return nil, e
			}
		}
		if a.Kind == expr.AggCountRows || a.Kind == expr.AggCountValues {
			mixed = true
		}
	}
	if mixed {
		per += 16
	}
	if int64(n) > (math.MaxInt64-16384)/per {
		return nil, memory.ErrBudgetExceeded
	}
	lease, e := manager(spillmeta.GrowthManager(q.Operator("DecimalSumState", memory.MemoryRetainedState))).Reserve(ctx, 16384+int64(n)*per)
	if e != nil {
		return nil, e
	}
	s := &decimalGlobalStream{p: p, budget: q, lease: lease, active: acquire, states: make([]aggregate.DecimalSumState, n), scratch: make([]aggregate.DecimalSumState, n), inputBatch: *memory.NewBatch(), outputManager: manager(q.Operator("DecimalSumOutput", memory.MemoryWorkspace))}
	if mixed {
		s.counts = make([]uint64, n)
		s.countScratch = make([]uint64, n)
	}
	s.predicate.manager = manager(q.Operator("DecimalSumPredicate", memory.MemoryWorkspace))
	s.predicate.operator = "DecimalSumPredicate"
	po := p.e.opts.Pipeline
	po.Ordering = pipeline.OrderingStable
	if p.decimalGrouped() {
		po.Workers = 1
	}
	s.input, e = pipeline.OpenBatchStream(ctx, p.src, opts, p.parts, manager(q.Operator("DecimalSumExchange", memory.MemoryInFlightBatch)), po, p.e.opts.Observer)
	if e != nil {
		s.active = false
		_ = s.cleanup()
		return nil, e
	}
	active = false
	return s, nil
}
func (s *decimalGlobalStream) releaseOutput() {
	if s.output != nil {
		for _, v := range s.output.Int64s {
			*v = memory.Int64Vector{}
		}
		for _, v := range s.output.Uint64s {
			*v = memory.Uint64Vector{}
		}
		for _, v := range s.output.Decimals {
			*v = memory.Decimal128Vector{}
		}
		*s.output = s.saved
		s.output = nil
		s.saved = memory.Batch{}
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
}
func (s *decimalGlobalStream) cleanup() error {
	s.releaseOutput()
	var e error
	if s.input != nil {
		e = s.input.Close()
		s.input = nil
	}
	s.inputBatch.ClearColumns()
	s.predicate.close()
	clear(s.counts)
	clear(s.countScratch)
	s.counts = nil
	s.countScratch = nil
	clear(s.states)
	clear(s.scratch)
	s.states = nil
	s.scratch = nil
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
	if s.active {
		s.p.end()
		s.active = false
	}
	return e
}
func (s *decimalGlobalStream) fail(e error) (bool, error) {
	if s.terminal == nil {
		s.terminal = errors.Join(e, s.cleanup())
	}
	return false, s.terminal
}
func (s *decimalGlobalStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.cleanup()
}
func (s *decimalGlobalStream) consume(ctx context.Context) error {
	for {
		s.inputBatch.ClearColumns()
		ok, e := s.input.Next(ctx, &s.inputBatch)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		b := &s.inputBatch
		if b.Rows < 0 {
			return fmt.Errorf("Decimal SUM negative rows")
		}
		copy(s.scratch, s.states)
		copy(s.countScratch, s.counts)
		for lane, f := range s.p.cq.MultiAggregate.DecimalFields {
			if e := ctx.Err(); e != nil {
				return e
			}
			if s.p.cq.Aggregates[lane].Kind == expr.AggCountRows {
				continue
			}
			v := b.Decimals[f.Name]
			if v == nil || v.Precision != f.DecimalPrecision || v.Scale != f.DecimalScale || len(v.Values) != b.Rows || v.Validity != nil && len(v.Validity) < (b.Rows+63)/64 {
				return fmt.Errorf("Decimal SUM input binding mismatch %q", f.Name)
			}
			for row := 0; row < b.Rows; row++ {
				if row%64 == 0 {
					if e := ctx.Err(); e != nil {
						return e
					}
				}
				if !v.IsValid(row) {
					if !f.Nullable {
						return fmt.Errorf("Decimal SUM nonnullable input NULL")
					}
					continue
				}
				if !v.Values[row].FitsInPrecision(f.DecimalPrecision) {
					return fmt.Errorf("Decimal SUM input coefficient overflow")
				}
			}
		}
		selection, e := s.predicate.evaluate(ctx, s.p.cq.Predicate, b, s.p.e.kernels)
		if e != nil {
			return e
		}
		selected := 0
		for row := 0; row < b.Rows; row++ {
			if row%64 == 0 {
				if e := ctx.Err(); e != nil {
					return e
				}
			}
			if !memory.IsSet(selection.Truth, row) {
				continue
			}
			selected++
			for lane, f := range s.p.cq.MultiAggregate.DecimalFields {
				if lane%64 == 0 {
					if e := ctx.Err(); e != nil {
						return e
					}
				}
				kind := s.p.cq.Aggregates[lane].Kind
				if kind == expr.AggCountRows {
					if e := decimalCountIncrement(&s.countScratch[lane]); e != nil {
						return e
					}
					continue
				}
				v := b.Decimals[f.Name]
				if kind == expr.AggCountValues {
					if v.IsValid(row) {
						if e := decimalCountIncrement(&s.countScratch[lane]); e != nil {
							return e
						}
					}
					continue
				}
				if v.IsValid(row) {
					if e := s.scratch[lane].Add(v.Values[row]); e != nil {
						return e
					}
				}
			}
		}
		if selected > math.MaxInt-s.selected {
			return fmt.Errorf("Decimal SUM selected-row statistics exceed MaxInt")
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		copy(s.states, s.scratch)
		copy(s.counts, s.countScratch)
		s.selected += selected
	}
	s.inputBatch.ClearColumns()
	e := s.input.Close()
	s.input = nil
	return e
}
func (s *decimalGlobalStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if s.emitted {
		return false, s.cleanup()
	}
	if ctx == nil || dst == nil {
		return s.fail(fmt.Errorf("Decimal SUM requires context and destination"))
	}
	var e error
	ctx, e = aggregateStreamContext(ctx, s.budget)
	if e != nil {
		return s.fail(e)
	}
	if e = ctx.Err(); e != nil {
		return s.fail(e)
	}
	if e = s.consume(ctx); e != nil {
		return s.fail(e)
	}
	for i, state := range s.states {
		if i%64 == 0 {
			if e = ctx.Err(); e != nil {
				return s.fail(e)
			}
		}
		if !decimalValueKind(s.p.cq.Aggregates[i].Kind) {
			continue
		}
		if _, _, e = s.p.finalizeDecimal(i, state); e != nil {
			return s.fail(e)
		}
	}
	n := len(s.states)
	if int64(n) > (math.MaxInt64-8192)/1024 {
		return s.fail(memory.ErrBudgetExceeded)
	}
	s.outputLease, e = s.outputManager.Reserve(ctx, 8192+int64(n)*1024)
	if e != nil {
		return s.fail(e)
	}
	sumSlots := 0
	for i, a := range s.p.cq.Aggregates {
		if i%64 == 0 {
			if e = ctx.Err(); e != nil {
				return s.fail(e)
			}
		}
		if decimalValueKind(a.Kind) {
			sumSlots++
		}
	}
	s.saved = *dst
	s.output = dst
	*dst = memory.Batch{Rows: 1, Decimals: make(map[string]*memory.Decimal128Vector, sumSlots)}
	if s.counts != nil {
		dst.Uint64s = make(map[string]*memory.Uint64Vector, n-sumSlots)
	}
	for i, state := range s.states {
		if i%64 == 0 {
			if e = ctx.Err(); e != nil {
				return s.fail(e)
			}
		}
		f := s.p.executionSchema.Fields[i]
		if !decimalValueKind(s.p.cq.Aggregates[i].Kind) {
			dst.Uint64s[f.Name] = &memory.Uint64Vector{Values: []uint64{s.counts[i]}}
			continue
		}
		v, valid, _ := s.p.finalizeDecimal(i, state)
		vec := &memory.Decimal128Vector{Precision: f.DecimalPrecision, Scale: f.DecimalScale, Values: []memory.Decimal128{v}, Validity: memory.Bitmap{0}}
		if valid {
			memory.Set(vec.Validity, 0)
		}
		dst.Decimals[f.Name] = vec
	}
	if e = ctx.Err(); e != nil {
		return s.fail(e)
	}
	s.emitted = true
	return true, nil
}
func (s *decimalGlobalStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	if sink != nil {
		return AggregateStreamStats{}, fmt.Errorf("Decimal SUM record materialization unsupported; use BatchStream or lazy Collect")
	}
	b := &memory.Batch{}
	for {
		ok, e := s.Next(ctx, b)
		if e != nil {
			return AggregateStreamStats{}, e
		}
		if !ok {
			break
		}
	}
	return AggregateStreamStats{SelectedRows: s.selected, Groups: 1}, nil
}

func decimalCountIncrement(count *uint64) error {
	if *count == math.MaxUint64 {
		return fmt.Errorf("Decimal mixed COUNT exceeds Uint64")
	}
	*count++
	return nil
}
