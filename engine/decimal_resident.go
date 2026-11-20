package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source"
	"math"
	"unsafe"
)

func (p *PreparedQuery) decimalResident() bool {
	return p.decimalGrouped() && len(p.cq.GroupBy) >= 1 && len(p.cq.GroupBy) <= 2 && p.executionPolicy == ExecutionMemoryOnly && p.strategy.Kind == planner.StrategyBitmapDirect
}

type decimalResidentStream struct {
	*decimalGroupedStream
	index            *hashagg.GroupIndex
	values           []aggregate.DecimalSumState
	countsByGroup    []uint64
	ownerLease       memory.Reservation
	ownerManager     memory.Manager
	operationContext context.Context
	capacity, cursor int
	loaded           bool
}

func (p *PreparedQuery) newDecimalResidentStream(ctx context.Context, schema memory.Schema, outputRows int, opts source.ReadOptions, acquire bool, wrap func(memory.Manager) memory.Manager) (source.BatchStream, error) {
	if !p.decimalResident() {
		return nil, fmt.Errorf("Decimal resident requires MemoryOnly unordered grouped plan")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Decimal resident requires context")
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q == nil || q.Limit() <= 0 {
		return nil, fmt.Errorf("Decimal resident requires finite query budget")
	}
	base, e := p.newDecimalGlobalStreamWithManagers(ctx, schema, opts, acquire, wrap)
	if e != nil {
		return nil, e
	}
	rows := outputRows
	if rows < 1 {
		rows = opts.BatchRows
	}
	if rows < 1 || rows > 64 {
		rows = 64
	}
	if opts.BatchRows > 0 && rows > opts.BatchRows {
		rows = opts.BatchRows
	}
	nKeys := len(p.cq.GroupBy)
	rowBytes := int64(16*nKeys + 24*len(p.decimalLaneFields()))
	if opts.BatchBytes > 0 && int64(rows) > opts.BatchBytes/rowBytes {
		rows = max(1, int(opts.BatchBytes/rowBytes))
	}
	s := &decimalResidentStream{decimalGroupedStream: &decimalGroupedStream{decimalGlobalStream: base.(*decimalGlobalStream), numKeys: nKeys, pageRows: rows}, operationContext: ctx}
	manager := func(m memory.Manager) memory.Manager {
		if wrap != nil {
			return wrap(m)
		}
		return m
	}
	s.ownerManager = manager(spillmeta.GrowthManager(q.Operator("DecimalResidentState", memory.MemoryRetainedState)))
	indexManager := manager(spillmeta.GrowthManager(q.Operator("DecimalResidentIndex", memory.MemoryRetainedState)))
	s.index, e = hashagg.NewGroupIndex(1, func(n int64) (memory.Reservation, error) { return indexManager.Reserve(s.operationContext, n) })
	if e != nil {
		_, e = s.fail(e)
		return nil, e
	}
	return s, nil
}
func (s *decimalResidentStream) releaseOwner() {
	if s.index != nil {
		s.index.Close()
		s.index = nil
	}
	clear(s.values)
	clear(s.countsByGroup)
	s.values = nil
	s.countsByGroup = nil
	if s.ownerLease != nil {
		s.ownerLease.Release()
		s.ownerLease = nil
	}
	s.operationContext = nil
}
func (s *decimalResidentStream) fail(e error) (bool, error) {
	s.releaseOwner()
	return s.decimalGroupedStream.fail(e)
}
func (s *decimalResidentStream) Close() error {
	s.releaseOwner()
	return s.decimalGroupedStream.Close()
}
func (s *decimalResidentStream) grow(ctx context.Context, need int) error {
	if need <= s.capacity {
		return nil
	}
	cap := max(16, s.capacity*2)
	if cap < need {
		cap = need
	}
	n := len(s.states)
	per := int64(unsafe.Sizeof(aggregate.DecimalSumState{}))
	if s.counts != nil {
		per += 8
	}
	if s.capacity > math.MaxInt/2 || n > math.MaxInt/cap || int64(n) > math.MaxInt64/int64(cap)/per {
		return memory.ErrBudgetExceeded
	}
	lease, e := s.ownerManager.Reserve(ctx, int64(cap)*int64(n)*per)
	if e != nil {
		return e
	}
	committed := false
	defer func() {
		if !committed {
			lease.Release()
		}
	}()
	values := make([]aggregate.DecimalSumState, cap*n)
	var counts []uint64
	if s.counts != nil {
		counts = make([]uint64, cap*n)
	}
	for i := 0; i < len(s.values); i += 64 {
		if e := ctx.Err(); e != nil {
			return e
		}
		end := min(i+64, len(s.values))
		copy(values[i:end], s.values[i:end])
		if counts != nil {
			copy(counts[i:end], s.countsByGroup[i:end])
		}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	old := s.ownerLease
	s.values, s.countsByGroup, s.ownerLease, s.capacity = values, counts, lease, cap
	committed = true
	if old != nil {
		old.Release()
	}
	return nil
}
func (s *decimalResidentStream) loadGroup(ctx context.Context, id int) error {
	n := len(s.states)
	for i := 0; i < n; i++ {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return e
			}
		}
		s.states[i] = s.values[id*n+i]
		if s.counts != nil {
			s.counts[i] = s.countsByGroup[id*n+i]
		}
	}
	return nil
}
func (s *decimalResidentStream) consumeAll(ctx context.Context) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		s.truth = nil
		s.inputBatch.ClearColumns()
		ok, e := s.input.Next(ctx, &s.inputBatch)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		if e = s.validateBatch(ctx); e != nil {
			return e
		}
		var keys [2]*memory.Int64Vector
		for i, f := range s.p.cq.GenericFields {
			keys[i] = s.inputBatch.Int64s[f.Name]
		}
		for row := 0; row < s.inputBatch.Rows; row++ {
			if row%64 == 0 {
				if e := ctx.Err(); e != nil {
					return e
				}
			}
			if !memory.IsSet(s.truth, row) {
				continue
			}
			// Each key part owns one validity byte and an eight-byte value.
			// Null payloads stay zero, preserving one canonical tuple and the
			// legacy one-key byte representation.
			var encoded [18]byte
			for i := 0; i < s.numKeys; i++ {
				offset := 9 * i
				if keys[i].IsValid(row) {
					encoded[offset] = 1
					binary.LittleEndian.PutUint64(encoded[offset+1:offset+9], uint64(keys[i].Values[row]))
				}
			}
			key := encoded[:9*s.numKeys]
			id, found, e := s.index.LookupWithCheck(key, ctx.Err)
			if e != nil {
				return e
			}
			if !found {
				if s.index.Len() == math.MaxInt {
					return fmt.Errorf("Decimal resident group count exceeds MaxInt")
				}
				if e = s.grow(ctx, s.index.Len()+1); e != nil {
					return e
				}
				id, _, e = s.index.LookupOrInsertWithCheck(key, ctx.Err)
				if e != nil {
					return e
				}
			}
			if e = s.loadGroup(ctx, id); e != nil {
				return e
			}
			if e = s.addRow(ctx, row); e != nil {
				return e
			}
			n := len(s.states)
			// No failure point while committing candidate state. Input values and all
			// lane counters were validated before this commit; later errors are sticky.
			copy(s.values[id*n:(id+1)*n], s.states)
			if s.counts != nil {
				copy(s.countsByGroup[id*n:(id+1)*n], s.counts)
			}
		}
	}
	s.truth = nil
	s.inputBatch.ClearColumns()
	e := s.input.Close()
	s.input = nil
	if e != nil {
		return e
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	for id := 0; id < s.index.Len(); id++ {
		for lane := range s.states {
			if lane%64 == 0 {
				if e := ctx.Err(); e != nil {
					return e
				}
			}
			// Preflight all exact results before yielding the first page.
			// emit recomputes AVG rather than retaining a second result table.
			if decimalValueKind(s.p.cq.Aggregates[lane].Kind) {
				if _, _, e := s.p.finalizeDecimal(lane, s.values[id*len(s.states)+lane]); e != nil {
					return e
				}
			}
		}
	}
	s.loaded = true
	return nil
}
func (s *decimalResidentStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.releaseOutput()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if ctx == nil || dst == nil {
		return s.fail(fmt.Errorf("Decimal resident requires context and destination"))
	}
	var e error
	ctx, e = aggregateStreamContext(ctx, s.budget)
	if e != nil {
		return s.fail(e)
	}
	if e = ctx.Err(); e != nil {
		return s.fail(e)
	}
	s.operationContext = ctx
	if !s.loaded {
		if e = s.consumeAll(ctx); e != nil {
			return s.fail(e)
		}
	}
	if s.index == nil {
		return false, nil
	}
	for s.cursor < s.index.Len() {
		if e = s.loadGroup(ctx, s.cursor); e != nil {
			return s.fail(e)
		}
		key := s.index.Key(s.cursor)
		for i := 0; i < s.numKeys; i++ {
			offset := 9 * i
			s.key.valid[i] = key[offset] != 0
			s.key.values[i] = int64(binary.LittleEndian.Uint64(key[offset+1 : offset+9]))
		}
		full, e := s.emit(ctx, dst)
		if e != nil {
			return s.fail(e)
		}
		s.cursor++
		if full {
			ok, e := s.finishPage(ctx, dst)
			if e != nil {
				return s.fail(e)
			}
			return ok, nil
		}
	}
	if s.output != nil && dst.Rows > 0 {
		ok, e := s.finishPage(ctx, dst)
		if e != nil {
			return s.fail(e)
		}
		return ok, nil
	}
	if e := ctx.Err(); e != nil {
		return s.fail(e)
	}
	s.releaseOwner()
	return false, s.cleanup()
}
func (s *decimalResidentStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	if sink != nil {
		return AggregateStreamStats{}, fmt.Errorf("Decimal resident record materialization unsupported")
	}
	b := memory.NewBatch()
	for {
		ok, e := s.Next(ctx, b)
		if e != nil {
			return AggregateStreamStats{}, e
		}
		if !ok {
			break
		}
	}
	return AggregateStreamStats{SelectedRows: s.selected, Groups: s.groups}, nil
}
