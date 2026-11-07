package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/tuplesort"
)

// distinctAggregateStream sorts two families together by canonical group. Raw
// numeric inputs carry a unique source ordinal; distinct values do not. Global
// tuple dedup therefore removes only repeated values, never ordinary input rows.
// A single group accumulator suffices regardless of group/value cardinality.
type distinctAggregateStream struct {
	p                                   *PreparedQuery
	budget                              *memory.QueryBudget
	readOpts                            source.ReadOptions
	schema                              memory.Schema
	sorter                              *tuplesort.Sorter
	manager                             memory.Manager
	helper                              spillableAggregateStream
	numeric                             *aggregate.NumericBundle
	statistics                          *aggregate.StatisticsBundle
	counts                              []byte
	metadata                            memory.Reservation
	scratch, key                        []byte
	scratchLease, keyLease              memory.Reservation
	pending                             []byte
	output                              *memory.Batch
	outputLease                         memory.Reservation
	stats                               AggregateStreamStats
	started                             time.Time
	prepared, exhausted, closed, active bool
	eofObserved                         bool
	terminal                            error
}

func (p *PreparedQuery) newDistinctAggregateStream(ctx context.Context, schema memory.Schema, _ int, readOpts source.ReadOptions, acquire bool) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("distinct aggregate requires context")
	}
	if err := readOpts.Validate(); err != nil {
		return nil, err
	}
	if readOpts.Kernels != p.e.kernels {
		return nil, fmt.Errorf("distinct kernel set differs from prepared query")
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q == nil {
		return nil, fmt.Errorf("distinct aggregate requires query budget")
	}
	if p.executionPolicy == ExecutionBounded && q.Limit() <= 0 {
		return nil, ErrBoundedMemoryLimitRequired
	}
	if acquire {
		if err := p.begin(); err != nil {
			return nil, err
		}
	}
	s := &distinctAggregateStream{p: p, budget: q, readOpts: readOpts, schema: schema, active: acquire, started: time.Now()}
	fail := func(err error) (source.BatchStream, error) { return nil, errors.Join(err, s.Close()) }
	s.manager = q.Operator("ExactDistinct", memory.MemoryRetainedState)
	s.helper = spillableAggregateStream{query: p, schema: schema}
	var store memory.SpillManager
	var err error
	if p.executionPolicy != ExecutionMemoryOnly && q.SpillConfigured() {
		store, err = q.SpillManager()
		if err != nil {
			return fail(err)
		}
	}
	block := 64 * 1024
	if q.Limit() > 0 {
		block = int(min(int64(block), max(int64(256), q.Limit()/32)))
	}
	s.sorter, err = tuplesort.New(tuplesort.Config{Manager: s.manager, Spill: store, BlockBytes: block})
	if err != nil {
		return fail(err)
	}
	m := p.cq.MultiAggregate
	s.metadata, err = s.manager.Reserve(ctx, int64(len(m.Distinct.Fields))*8+1024)
	if err != nil {
		return fail(err)
	}
	s.counts = make([]byte, 8*len(m.Distinct.Fields))
	reserve := func(n int64) (memory.Reservation, error) { return s.manager.Reserve(ctx, n) }
	if len(m.Columns) > 0 {
		s.numeric, err = aggregate.NewNumericBundle(m.Types, m.Layouts, reserve)
		if err == nil {
			err = s.numeric.EnsureGroups(1)
		}
		if err != nil {
			return fail(err)
		}
	}
	if m.Statistics != nil {
		st := m.Statistics
		s.statistics, err = aggregate.NewStatisticsBundle(st.Types, st.Layouts, reserve)
		if err == nil {
			err = s.statistics.EnsureGroups(1)
		}
		if err != nil {
			return fail(err)
		}
	}
	return s, nil
}

func (s *distinctAggregateStream) grow(ctx context.Context, dst *[]byte, lease *memory.Reservation, size int) error {
	if size < 0 {
		return fmt.Errorf("distinct buffer length overflow")
	}
	if size <= cap(*dst) {
		*dst = (*dst)[:size]
		return nil
	}
	n := max(64, size)
	r, err := s.manager.Reserve(ctx, int64(n))
	if err != nil {
		return err
	}
	b := make([]byte, n)
	copy(b, *dst)
	*dst = b[:size]
	if *lease != nil {
		(*lease).Release()
	}
	*lease = r
	return nil
}

func (s *distinctAggregateStream) ingest(ctx context.Context) (err error) {
	if s.prepared {
		return nil
	}
	s.prepared = true
	opts := s.p.e.opts.Pipeline
	opts.Ordering = pipeline.OrderingStable
	input, err := pipeline.OpenBatchStream(ctx, s.p.src, s.readOpts, s.p.parts, s.budget.Operator("DistinctExchange", memory.MemoryInFlightBatch), opts, s.p.e.opts.Observer)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, input.Close()) }()
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	workspace := aggregatePredicateWorkspace{manager: s.budget.Operator("DistinctPredicate", memory.MemoryWorkspace), operator: "DistinctPredicate"}
	defer workspace.close()
	m := s.p.cq.MultiAggregate
	for {
		ok, e := input.Next(ctx, batch)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		base, e := workspace.nextOrdinal(batch.Rows)
		if e != nil {
			return e
		}
		selection, e := workspace.evaluate(ctx, s.p.cq.Predicate, batch, s.p.e.kernels)
		if e != nil {
			return e
		}
		groups, e := bindCanonicalGroupKeys(s.p.cq.GenericFields, batch)
		if e != nil {
			return e
		}
		// Validate every input vector before indexing or accepting spill records.
		for i := range m.Distinct.Fields {
			if _, e = bindCanonicalGroupKeys(m.Distinct.Fields[i:i+1], batch); e != nil {
				return e
			}
		}
		for lane, name := range m.Columns {
			if name != "" {
				if e = validateDistinctInput(batch, name, m.Types[lane], aggregateNeedsValue(m.Layouts[lane])); e != nil {
					return e
				}
			}
		}
		if st := m.Statistics; st != nil {
			for lane, name := range st.Columns {
				if e = validateDistinctInput(batch, name, st.Types[lane], true); e != nil {
					return e
				}
			}
		}
		e = forSelectedFrom(selection.Truth, batch.Rows, 0, func(row int) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			keySize, err := groups.KeySize(row)
			if err != nil {
				return err
			}
			if keySize > math.MaxUint32 || keySize > math.MaxInt-32 {
				return fmt.Errorf("distinct group key too large")
			}
			prefix := 4 + keySize
			lanes := len(m.Columns)
			if m.Statistics != nil {
				lanes += len(m.Statistics.Columns)
			}
			if lanes > (math.MaxInt-prefix-17)/9 {
				return fmt.Errorf("distinct lane size overflow")
			}
			size := prefix + 1 + 16 + 9*lanes
			if err = s.grow(ctx, &s.scratch, &s.scratchLease, size); err != nil {
				return err
			}
			binary.BigEndian.PutUint32(s.scratch[:4], uint32(keySize))
			encoded, err := groups.AppendKey(s.scratch[:4], row)
			if err != nil {
				return err
			}
			if len(encoded) != prefix {
				return fmt.Errorf("distinct key size mismatch")
			}
			s.scratch[prefix] = 0
			ordinal := aggregate.SourceOrdinal{Row: base + uint64(row)}
			binary.BigEndian.PutUint64(s.scratch[prefix+1:prefix+9], ordinal.Partition)
			binary.BigEndian.PutUint64(s.scratch[prefix+9:prefix+17], ordinal.Row)
			pos := prefix + 17
			write := func(bits uint64, valid bool) {
				s.scratch[pos] = 0
				if valid {
					s.scratch[pos] = 1
				}
				binary.LittleEndian.PutUint64(s.scratch[pos+1:pos+9], bits)
				pos += 9
			}
			for lane, name := range m.Columns {
				bits, valid := uint64(0), false
				if name != "" {
					bits, valid = nativeAggregateBits(batch, name, m.Types[lane], m.Layouts[lane], row)
				}
				write(bits, valid)
			}
			if st := m.Statistics; st != nil {
				for lane, name := range st.Columns {
					bits, valid := uint64(0), false
					if !byteStatisticsType(st.Types[lane]) {
						bits, valid = statisticsInputBits(batch, name, st.Types[lane], row)
					}
					write(bits, valid)
				}
			}
			// Even distinct-only queries keep one ordinal record per input row so
			// selected-row diagnostics and empty-group presence remain exact.
			if err = s.sorter.Add(ctx, s.scratch[:size]); err != nil {
				return err
			}
			if st := m.Statistics; st != nil {
				for lane, name := range st.Columns {
					if !byteStatisticsType(st.Types[lane]) {
						continue
					}
					data, valid, e := statisticsInputBytes(batch, name, row)
					if e != nil {
						return e
					}
					if len(data) > math.MaxUint32 || len(data) > math.MaxInt-prefix-26 {
						return fmt.Errorf("endpoint tuple too large")
					}
					n := prefix + 26 + len(data)
					if e = s.grow(ctx, &s.scratch, &s.scratchLease, n); e != nil {
						return e
					}
					body := s.scratch[prefix:n]
					body[0] = 2
					binary.BigEndian.PutUint32(body[1:5], uint32(lane))
					binary.BigEndian.PutUint64(body[5:13], ordinal.Partition)
					binary.BigEndian.PutUint64(body[13:21], ordinal.Row)
					body[21] = 0
					if valid {
						body[21] = 1
					}
					binary.BigEndian.PutUint32(body[22:26], uint32(len(data)))
					copy(body[26:], data)
					if e = s.sorter.Add(ctx, s.scratch[:n]); e != nil {
						return e
					}
				}
			}
			for lane := range m.Distinct.Fields {
				values := canonicalGroupBinding{fields: m.Distinct.Fields[lane : lane+1], batch: batch}
				n, err := values.KeySize(row)
				if err != nil {
					return err
				}
				if n > math.MaxInt-prefix-5 {
					return fmt.Errorf("distinct value too large")
				}
				if err = s.grow(ctx, &s.scratch, &s.scratchLease, prefix+5+n); err != nil {
					return err
				}
				s.scratch[prefix] = 1
				binary.BigEndian.PutUint32(s.scratch[prefix+1:prefix+5], uint32(lane))
				tuple, err := values.AppendKey(s.scratch[:prefix+5], row)
				if err != nil {
					return err
				}
				if err = s.sorter.Add(ctx, tuple); err != nil {
					return err
				}
			}
			s.stats.SelectedRows++
			if s.p.cq.ValueColumn != "" {
				v := batch.Int64s[s.p.cq.ValueColumn]
				if v != nil && v.IsValid(row) {
					s.stats.Checksum += v.Values[row]
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	// Scratch is not needed during merging; release before the merge peak.
	s.scratch = nil
	if s.scratchLease != nil {
		s.scratchLease.Release()
		s.scratchLease = nil
	}
	return s.sorter.Finish(ctx)
}

func splitDistinctTuple(tuple []byte) (key, body []byte, err error) {
	if len(tuple) < 5 {
		return nil, nil, fmt.Errorf("%w: short distinct tuple", memory.ErrSpillCorrupt)
	}
	n := uint64(binary.BigEndian.Uint32(tuple[:4]))
	if n > uint64(len(tuple)-5) {
		return nil, nil, fmt.Errorf("%w: invalid distinct key length", memory.ErrSpillCorrupt)
	}
	return tuple[4 : 4+int(n)], tuple[4+int(n):], nil
}

func (s *distinctAggregateStream) nextRecord(ctx context.Context) (aggregateRecord, bool, error) {
	if ctx == nil {
		return aggregateRecord{}, false, fmt.Errorf("distinct aggregate context is nil")
	}
	if s.closed {
		if s.terminal != nil {
			return aggregateRecord{}, false, s.terminal
		}
		if s.eofObserved {
			return aggregateRecord{}, false, nil
		}
		return aggregateRecord{}, false, source.ErrBatchStreamClosed
	}
	if s.exhausted {
		return aggregateRecord{}, false, nil
	}
	if err := s.ingest(ctx); err != nil {
		return aggregateRecord{}, false, err
	}
	tuple := s.pending
	s.pending = nil
	if tuple == nil {
		var ok bool
		var err error
		tuple, ok, err = s.sorter.Next(ctx)
		if err != nil {
			return aggregateRecord{}, false, err
		}
		if !ok {
			if s.stats.Groups == 0 && len(s.p.cq.GroupBy) == 0 {
				s.stats.Groups++
				s.exhausted = true
				return s.record(), true, nil
			}
			s.exhausted = true
			return aggregateRecord{}, false, nil
		}
	}
	key, _, err := splitDistinctTuple(tuple)
	if err != nil {
		return aggregateRecord{}, false, err
	}
	if err = s.grow(ctx, &s.key, &s.keyLease, len(key)); err != nil {
		return aggregateRecord{}, false, err
	}
	copy(s.key, key)
	if err = visitCanonicalGroupKey(s.key, s.p.cq.GenericFields, func(int, memory.DataType, bool, uint64, []byte) error { return nil }); err != nil {
		return aggregateRecord{}, false, errors.Join(memory.ErrSpillCorrupt, err)
	}
	clear(s.counts)
	if s.numeric != nil {
		if err = s.numeric.ClearGroup(0); err != nil {
			return aggregateRecord{}, false, err
		}
	}
	if s.statistics != nil {
		if err = s.statistics.ClearGroup(0); err != nil {
			return aggregateRecord{}, false, err
		}
	}
	for {
		k, body, e := splitDistinctTuple(tuple)
		if e != nil {
			return aggregateRecord{}, false, e
		}
		if !bytes.Equal(k, s.key) {
			s.pending = tuple
			break
		}
		if e = s.apply(body); e != nil {
			return aggregateRecord{}, false, e
		}
		var ok bool
		tuple, ok, e = s.sorter.Next(ctx)
		if e != nil {
			return aggregateRecord{}, false, e
		}
		if !ok {
			s.exhausted = true
			break
		}
	}
	s.stats.Groups++
	return s.record(), true, nil
}

func (s *distinctAggregateStream) record() aggregateRecord {
	return aggregateRecord{stringKey: s.key, numericBundle: s.numeric, statisticsBundle: s.statistics, groupID: 0, rawStates: s.counts}
}
func (s *distinctAggregateStream) apply(body []byte) error {
	bad := func() error { return fmt.Errorf("%w: invalid distinct record", memory.ErrSpillCorrupt) }
	if len(body) == 0 {
		return bad()
	}
	m := s.p.cq.MultiAggregate
	if body[0] == 1 {
		if len(body) < 5 {
			return bad()
		}
		lane := uint64(binary.BigEndian.Uint32(body[1:5]))
		if lane >= uint64(len(s.counts)/8) {
			return bad()
		}
		if err := visitCanonicalGroupKey(body[5:], m.Distinct.Fields[lane:lane+1], func(int, memory.DataType, bool, uint64, []byte) error { return nil }); err != nil {
			return errors.Join(memory.ErrSpillCorrupt, err)
		}
		count := binary.LittleEndian.Uint64(s.counts[lane*8 : lane*8+8])
		if count == math.MaxUint64 {
			return fmt.Errorf("distinct count overflows UInt64")
		}
		binary.LittleEndian.PutUint64(s.counts[lane*8:lane*8+8], count+1)
		return nil
	}
	if body[0] == 2 {
		if len(body) < 26 || m.Statistics == nil {
			return bad()
		}
		lane := uint64(binary.BigEndian.Uint32(body[1:5]))
		if lane >= uint64(len(m.Statistics.Types)) || !byteStatisticsType(m.Statistics.Types[lane]) || body[21] > 1 || uint64(binary.BigEndian.Uint32(body[22:26])) != uint64(len(body)-26) || body[21] == 0 && len(body) != 26 {
			return bad()
		}
		ordinal := aggregate.SourceOrdinal{Partition: binary.BigEndian.Uint64(body[5:13]), Row: binary.BigEndian.Uint64(body[13:21])}
		return s.statistics.AddBytes(0, int(lane), body[26:], body[21] == 1, ordinal)
	}
	lanes := len(m.Columns)
	if m.Statistics != nil {
		lanes += len(m.Statistics.Columns)
	}
	if body[0] != 0 || len(body) != 17+9*lanes {
		return bad()
	}
	ordinal := aggregate.SourceOrdinal{Partition: binary.BigEndian.Uint64(body[1:9]), Row: binary.BigEndian.Uint64(body[9:17])}
	pos := 17
	for lane := range m.Columns {
		if body[pos] > 1 {
			return bad()
		}
		s.numeric.AddBits(0, lane, binary.LittleEndian.Uint64(body[pos+1:pos+9]), body[pos] == 1)
		pos += 9
	}
	if m.Statistics != nil {
		for lane := range m.Statistics.Columns {
			if body[pos] > 1 {
				return bad()
			}
			if byteStatisticsType(m.Statistics.Types[lane]) {
				if body[pos] != 0 || binary.LittleEndian.Uint64(body[pos+1:pos+9]) != 0 {
					return bad()
				}
				pos += 9
				continue
			}
			if err := s.statistics.AddBits(0, lane, binary.LittleEndian.Uint64(body[pos+1:pos+9]), body[pos] == 1, ordinal); err != nil {
				return err
			}
			pos += 9
		}
	}
	return nil
}

// Next emits one budgeted group row, borrowing state until the next call.
func (s *distinctAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.output != nil {
		s.output.ClearColumns()
		s.output = nil
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
	if dst == nil {
		return false, s.fail(fmt.Errorf("nil distinct output batch"))
	}
	dst.ClearColumns()
	record, ok, err := s.nextRecord(ctx)
	if err != nil {
		return false, s.fail(err)
	}
	if !ok {
		s.eofObserved = true
		return false, s.Close()
	}
	// Conservative per-string capacity: every string column gets total key bytes.
	endpointBytes, err := s.p.byteStatisticsPayloadSize(record)
	if err != nil {
		return false, s.fail(err)
	}
	if endpointBytes > int64(math.MaxInt-len(s.key)) {
		return false, s.fail(fmt.Errorf("endpoint output size overflow"))
	}
	stringCapacity := len(s.key) + int(endpointBytes)
	size, err := aggregateOutputBytes(s.schema, 1, int64(stringCapacity))
	if err != nil {
		return false, s.fail(err)
	}
	s.outputLease, err = s.manager.Reserve(ctx, size)
	if err != nil {
		return false, s.fail(err)
	}
	s.output = dst
	if err = s.helper.allocateOutputBatch(dst, 1, stringCapacity); err != nil {
		return false, s.fail(err)
	}
	if err = s.helper.setOutputRecord(dst, 0, record); err != nil {
		return false, s.fail(err)
	}
	dst.Rows = 1
	return true, nil
}
func (s *distinctAggregateStream) consumeRecords(ctx context.Context, sink func(aggregateRecord) error) (AggregateStreamStats, error) {
	for {
		record, ok, err := s.nextRecord(ctx)
		if err != nil {
			return AggregateStreamStats{}, s.fail(err)
		}
		if !ok {
			break
		}
		if sink != nil {
			if err = sink(record); err != nil {
				return AggregateStreamStats{}, s.fail(err)
			}
		}
	}
	stats := s.Stats()
	return stats, s.Close()
}

// Stats returns cumulative exact aggregation and spill diagnostics.
func (s *distinctAggregateStream) Stats() AggregateStreamStats {
	r := s.stats
	if s.sorter != nil {
		st := s.sorter.Stats()
		r.SpillRuns = st.SpillRuns
		r.SpillBytes = st.SpillBytes
	}
	r.Elapsed = time.Since(s.started)
	return r
}

// StreamDiagnostics describes the effective exact distinct fallback.
func (s *distinctAggregateStream) StreamDiagnostics() map[string]string {
	return map[string]string{"distinct_algorithm": "exact-global-tuple-sort", "distinct_policy": "shared bounded fallback; no parallel aggregate states"}
}
func (s *distinctAggregateStream) fail(err error) error {
	s.terminal = errors.Join(err, s.Close())
	return s.terminal
}

// Close releases owned buffers and retries any failed spill deletions.
func (s *distinctAggregateStream) Close() error {
	if s.closed {
		if s.sorter != nil {
			return s.sorter.Close()
		}
		return nil
	}
	s.closed = true
	var err error
	if s.sorter != nil {
		err = s.sorter.Close()
	}
	if s.numeric != nil {
		s.numeric.Close()
	}
	if s.statistics != nil {
		s.statistics.Close()
	}
	if s.output != nil {
		s.output.ClearColumns()
		s.output = nil
	}
	for _, r := range []memory.Reservation{s.metadata, s.scratchLease, s.keyLease, s.outputLease} {
		if r != nil {
			r.Release()
		}
	}
	s.metadata = nil
	s.scratchLease = nil
	s.keyLease = nil
	s.outputLease = nil
	s.scratch = nil
	s.key = nil
	s.counts = nil
	s.pending = nil
	if s.active {
		s.p.end()
		s.active = false
	}
	return err
}

func validateDistinctInput(b *memory.Batch, name string, typ memory.DataType, values bool) error {
	rows, validity, err := aggregateColumnInfo(b, name, typ, values)
	if err != nil {
		return err
	}
	if rows != b.Rows || (validity != nil && len(validity) < memory.WordsFor(rows)) {
		return fmt.Errorf("distinct aggregate input %q has invalid row count or validity", name)
	}
	return nil
}
