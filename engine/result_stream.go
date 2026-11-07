package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"gocolumn/source"
	"time"
	"unsafe"
)

// Execute is a terminal consumer of the same physical aggregate streams used
// by DataFrames. Scan and operator scratch are morsel-bounded in every policy.
// Materialize retains caller-owned result rows; while collecting, their memory
// is charged to this execution and must fit its budget.
func (p *PreparedQuery) Execute(ctx context.Context, opts ExecuteOptions) (Result, error) {
	return p.execute(ctx, opts, true)
}
func (p *PreparedQuery) execute(ctx context.Context, opts ExecuteOptions, allowReuse bool) (Result, error) {
	return p.executeTyped(ctx, opts, allowReuse, nil)
}
func (p *PreparedQuery) executeTyped(ctx context.Context, opts ExecuteOptions, allowReuse bool, typed *TypedResult) (result Result, err error) {
	if p == nil || ctx == nil {
		return Result{}, fmt.Errorf("prepared execution requires a query and context")
	}
	if typed == nil && opts.Materialize && (p.nativeAggregate() || p.cq.GroupShape == GroupShapeGeneric) {
		return Result{}, fmt.Errorf("native aggregate materialization requires ExecuteTyped or a lazy DataFrame")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := p.begin(); err != nil {
		return Result{}, err
	}
	defer p.end()
	select {
	case p.executions <- struct{}{}:
		defer func() { <-p.executions }()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	start := time.Now()
	budget := memory.QueryBudgetFromContext(ctx)
	internalBudget := budget == nil
	if budget == nil {
		budget, err = p.e.beginQueryMemory()
		if err != nil {
			return Result{}, err
		}
		defer func() {
			err = errors.Join(err, budget.Close())
			if err != nil {
				result = Result{}
			}
		}()
		ctx = memory.WithQueryBudget(ctx, budget)
	}
	if p.executionPolicy == ExecutionBounded && budget.Limit() <= 0 {
		return Result{}, ErrBoundedMemoryLimitRequired
	}
	collector := resultCollector{query: p, result: &result, budget: budget.Operator("CollectResult", memory.MemoryRetainedState)}
	defer collector.close()
	var typedCollector *typedResultCollector
	if typed != nil {
		typedCollector = &typedResultCollector{query: p, result: typed, account: resultCollector{budget: budget.Operator("CollectTypedResult", memory.MemoryRetainedState)}}
		defer typedCollector.account.close()
	}
	schema := p.resultSchema()
	var stream source.BatchStream
	readOpts := source.ReadOptions{Kernels: p.e.kernels, BatchRows: p.e.opts.Pipeline.MaxMorselRows, BatchBytes: p.e.opts.Pipeline.MaxMorselBytes}
	switch {
	case p.IsSortedAggregate():
		stream, err = p.newSortedAggregateStream(ctx, schema, 0, readOpts, false)
	case p.strategy.Kind == planner.StrategySpillHash:
		stream, err = p.newSpillableAggregateStream(ctx, schema, 0, readOpts, false)
	default:
		stream, err = p.newResidentAggregateStream(ctx, schema, 0, readOpts, false, allowReuse && internalBudget)
	}
	if err != nil {
		return Result{}, err
	}
	defer func() {
		err = errors.Join(err, stream.Close())
		if err != nil {
			result = Result{}
		}
	}()
	var sink func(aggregateRecord) error
	if opts.Materialize {
		if typed != nil {
			sink = func(record aggregateRecord) error { return typedCollector.append(ctx, record) }
		} else {
			sink = func(record aggregateRecord) error { return collector.append(ctx, record) }
		}
	}
	stats, err := stream.(aggregateRecordConsumer).consumeRecords(ctx, sink)
	if err != nil {
		return Result{}, err
	}
	result.GroupCount, result.SelectedRows, result.Checksum = stats.Groups, stats.SelectedRows, stats.Checksum
	result.Strategy, result.Elapsed = p.strategy, time.Since(start)
	p.e.opts.Observer.Observe(observer.Event{Stage: "prepared-query", Duration: result.Elapsed, Rows: p.stats.Rows, Bytes: p.stats.Bytes, Workers: p.strategy.Workers, Attributes: map[string]string{"strategy": p.strategy.Kind.String(), "reason": p.strategy.Reason}})
	if trace := observer.TraceFromContext(ctx); trace != nil {
		children := []string(nil)
		if profiled, ok := p.src.(profileOperatorIDProvider); ok && profiled.OperatorID() != "" {
			children = append(children, profiled.OperatorID())
		}
		node := trace.NewNode("aggregate", p.strategy.Kind.String(), children...)
		trace.Record(node.ID, observer.PhaseFromContext(ctx), p.stats.Rows, int64(result.GroupCount), result.Elapsed)
	}
	return result, nil
}

func (p *PreparedQuery) resultSchema() memory.Schema {
	fields := make([]memory.Field, 0, len(p.cq.GroupBy)+len(p.cq.Aggregates))
	if p.cq.GroupShape == GroupShapeGeneric {
		for _, field := range p.cq.GenericFields {
			if field.Type == memory.TypeString || field.Type == memory.TypeBinary {
				field.Encoding = memory.EncodingView
				field.DictionaryDomain = ""
			}
			fields = append(fields, field)
		}
	} else {
		for _, group := range p.cq.GroupBy {
			fields = append(fields, memory.Field{Name: group.Name, Type: group.Type, Encoding: group.Encoding, DictionaryDomain: group.DictionaryDomain, Nullable: group.Nullable})
		}
	}
	for index, agg := range p.cq.Aggregates {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", index)
		}
		typ, nullable := memory.TypeInt64, true
		switch agg.Kind {
		case expr.AggCountRows, expr.AggCountValues, expr.AggNUnique:
			typ, nullable = memory.TypeUint64, false
		case expr.AggAvg:
			typ = memory.TypeFloat64
		}
		if agg.Kind == expr.AggQuantile {
			q := p.quantiles()
			o := q.Outputs[q.OutputSlot[index]]
			typ, _ = aggregate.QuantileOutputType(q.Fields[o.Lane].Type)
		} else if agg.Kind == expr.AggNUnique {
			typ, nullable = memory.TypeUint64, false
		} else if expr.IsStatistics(agg.Kind) {
			s := p.statistics()
			typ, _ = expr.StatisticsType(s.Types[s.OutputLane[index]], agg.Kind)
		} else if p.nativeAggregate() {
			lane := p.cq.MultiAggregate.OutputLane[index]
			typ, _ = aggregate.NumericAggregateType(p.cq.MultiAggregate.Types[lane], aggregateStateKind(agg.Kind))
		}
		fields = append(fields, memory.Field{Name: name, Type: typ, Nullable: nullable})
	}
	return memory.NewSchema(fields...)
}

type resultCollector struct {
	query              *PreparedQuery
	result             *Result
	budget             memory.Manager
	leases             []memory.Reservation
	rowsLease          memory.Reservation
	leasesBackingLease memory.Reservation
}

func (c *resultCollector) reserve(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	if len(c.leases) == cap(c.leases) {
		capacity := cap(c.leases)
		maxInt := int(^uint(0) >> 1)
		if capacity > maxInt/2 {
			return fmt.Errorf("result reservation metadata exceeds addressable memory")
		}
		capacity = max(8, capacity*2)
		width := int64(unsafe.Sizeof(memory.Reservation(nil)))
		if int64(capacity) > int64(^uint64(0)>>1)/width {
			return fmt.Errorf("result reservation metadata size overflows")
		}
		backing, err := c.budget.Reserve(ctx, int64(capacity)*width)
		if err != nil {
			return fmt.Errorf("collect result reservation metadata: %w", err)
		}
		next := make([]memory.Reservation, len(c.leases), capacity)
		copy(next, c.leases)
		previous := c.leasesBackingLease
		c.leases, c.leasesBackingLease = next, backing
		if previous != nil {
			previous.Release()
		}
	}
	lease, err := c.budget.Reserve(ctx, bytes)
	if err != nil {
		return fmt.Errorf("collect aggregate result: %w", err)
	}
	c.leases = append(c.leases, lease)
	return nil
}

func (c *resultCollector) append(ctx context.Context, record aggregateRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rows := c.result.Rows
	if len(rows) == cap(rows) {
		if cap(rows) > int(^uint(0)>>1)/2 {
			return fmt.Errorf("aggregate result capacity exceeds addressable memory")
		}
		capacity := max(16, cap(rows)*2)
		if uint64(capacity) > uint64(^uint(0)>>1)/uint64(unsafe.Sizeof(AggregateRow{})) {
			return fmt.Errorf("aggregate result byte size exceeds addressable memory")
		}
		// Reserve both old and new backing arrays during their copy.
		lease, err := c.budget.Reserve(ctx, int64(capacity)*int64(unsafe.Sizeof(AggregateRow{})))
		if err != nil {
			return fmt.Errorf("collect aggregate result: %w", err)
		}
		grown := make([]AggregateRow, len(rows), capacity)
		copy(grown, rows)
		rows, c.result.Rows = grown, grown
		if c.rowsLease != nil {
			c.rowsLease.Release()
		}
		c.rowsLease = lease
	}
	bytes := int64(len(c.query.cq.GroupBy))*int64(unsafe.Sizeof(KeyPart{})) + int64(len(c.query.cq.Aggregates))*int64(unsafe.Sizeof(AggregateValue{}))
	if c.query.cq.GroupShape == GroupShapeStringView {
		bytes += int64(len(record.stringKey))
	}
	for index, group := range c.query.cq.GroupBy {
		if group.Type == memory.TypeString && group.Encoding == memory.EncodingDictionary {
			raw := record.a
			if index == 1 {
				raw = record.b
			}
			if dictionary, ok := c.query.dicts[group.Name]; ok && record.validMask&(1<<index) != 0 {
				bytes += int64(len(dictionary.Value(uint32(raw))))
			}
		}
	}
	if err := c.reserve(ctx, bytes); err != nil {
		return err
	}
	c.result.Rows = append(rows, c.query.materializeRecord(record))
	return nil
}

func (c *resultCollector) close() {
	if c.rowsLease != nil {
		c.rowsLease.Release()
		c.rowsLease = nil
	}
	for _, lease := range c.leases {
		lease.Release()
	}
	c.leases = nil
	if c.leasesBackingLease != nil {
		c.leasesBackingLease.Release()
		c.leasesBackingLease = nil
	}
}
