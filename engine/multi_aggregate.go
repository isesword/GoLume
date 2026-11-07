package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/hashagg"
	"gocolumn/memory"
)

// CompiledMultiAggregate binds distinct int64 input columns to shared group IDs.
// Columns and layouts are immutable preparation metadata. OutputLane maps each
// logical aggregate to its column's state; COUNT(*) uses lane zero.
type CompiledMultiAggregate struct {
	Columns    []string
	Types      []memory.DataType
	Native     bool
	Layouts    []aggregate.Layout
	OutputLane []int
	Statistics *CompiledStatistics
	Distinct   *CompiledDistinct
	Quantiles  *CompiledQuantiles
}

func compileMultiAggregate(schema memory.Schema, aggs []expr.Aggregate, force ...bool) (*CompiledMultiAggregate, error) {
	m := &CompiledMultiAggregate{OutputLane: make([]int, len(aggs))}
	stats, err := compileStatistics(schema, aggs)
	if err != nil {
		return nil, err
	}
	d, err := compileDistinct(schema, aggs)
	if err != nil {
		return nil, err
	}
	m.Distinct = d
	m.Statistics = stats
	m.Native = stats != nil || d != nil
	rows := false
	for i, a := range aggs {
		if expr.IsStatistics(a.Kind) || a.Kind == expr.AggNUnique {
			continue
		}
		if a.Kind == expr.AggCountRows {
			rows = true
			continue
		}
		field, ok := schema.Field(a.Column)
		if !ok {
			return nil, fmt.Errorf("aggregate column %q not found", a.Column)
		}
		if _, err := aggregate.NumericAggregateType(field.Type, aggregateStateKind(a.Kind)); err != nil {
			return nil, err
		}
		lane := -1
		for j, column := range m.Columns {
			if column == a.Column {
				lane = j
				break
			}
		}
		if lane < 0 {
			lane = len(m.Columns)
			m.Columns = append(m.Columns, a.Column)
			m.Types = append(m.Types, field.Type)
			if field.Type != memory.TypeInt64 {
				m.Native = true
			}
			m.Layouts = append(m.Layouts, aggregate.Layout{})
		}
		m.OutputLane[i] = lane
		l := &m.Layouts[lane]
		switch a.Kind {
		case expr.AggSum:
			l.Sum = true
		case expr.AggCountValues:
			l.CountValues = true
		case expr.AggMin:
			l.Min = true
		case expr.AggMax:
			l.Max = true
		case expr.AggAvg:
			l.Avg = true
		}
	}
	forced := len(force) > 0 && force[0]
	if len(m.Columns) == 0 && (rows || (forced && stats == nil && d == nil)) {
		m.Columns = []string{""}
		m.Types = []memory.DataType{memory.TypeInt64}
		m.Layouts = []aggregate.Layout{{CountRows: true}}
	}
	if !forced && stats == nil && d == nil && (len(m.Columns) == 0 || (len(m.Columns) == 1 && !m.Native)) {
		return nil, nil
	}
	if len(m.Layouts) > 0 {
		m.Layouts[0].CountRows = rows
	}
	return m, nil
}

func (p *PreparedQuery) multiLayouts() []aggregate.Layout {
	if p.cq.MultiAggregate == nil {
		return nil
	}
	return p.cq.MultiAggregate.Layouts
}

type multiAggregateState struct {
	byteWorkspace byteStatisticsWorkspace
	index         *hashagg.GroupIndex
	bundle        *aggregate.Int64Bundle
	numeric       *aggregate.NumericBundle
	statistics    *aggregate.StatisticsBundle
	compiled      *CompiledMultiAggregate
	scratch       []byte
	scratchLease  memory.Reservation
	table         *aggregateStateTable
}

func newMultiAggregateState(t *aggregateStateTable, compiled *CompiledMultiAggregate, hint int) (*multiAggregateState, error) {
	m := &multiAggregateState{compiled: compiled, table: t}
	var err error
	m.index, err = hashagg.NewGroupIndex(min(hint, 64), t.reserveGrowth)
	if err != nil {
		return nil, err
	}
	if compiled.Native && len(compiled.Columns) > 0 {
		m.numeric, err = aggregate.NewNumericBundle(compiled.Types, compiled.Layouts, t.reserveGrowth)
	} else if !compiled.Native {
		m.bundle, err = aggregate.NewInt64Bundle(compiled.Layouts, t.reserveGrowth)
	}
	if err != nil {
		m.index.Close()
		return nil, err
	}
	if compiled.Statistics != nil {
		s := compiled.Statistics
		m.statistics, err = aggregate.NewStatisticsBundle(s.Types, s.Layouts, t.reserveGrowth)
		if err != nil {
			m.close()
			return nil, err
		}
	}
	if err = m.byteWorkspace.init(compiled.Statistics, t.reserveGrowth); err != nil {
		m.close()
		return nil, err
	}
	return m, nil
}

func (m *multiAggregateState) key(record aggregateRecord) ([]byte, error) {
	if m.table.shape == GroupShapeGeneric {
		return m.canonicalKey(record)
	}
	n := 9
	switch m.table.shape {
	case GroupShapePairFixed:
		n = 17
	case GroupShapeStringView:
		n = 1 + len(record.stringKey)
	}
	if n > cap(m.scratch) {
		size := max(n, max(64, cap(m.scratch)*2))
		lease, err := m.table.reserveGrowth(int64(size))
		if err != nil {
			return nil, err
		}
		m.scratch = make([]byte, size)
		if m.scratchLease != nil {
			m.scratchLease.Release()
		}
		m.scratchLease = lease
	}
	key := m.scratch[:n]
	key[0] = record.validMask
	switch m.table.shape {
	case GroupShapePairFixed:
		binary.LittleEndian.PutUint64(key[1:9], record.a)
		binary.LittleEndian.PutUint64(key[9:17], record.b)
	case GroupShapeStringView:
		copy(key[1:], record.stringKey)
	default:
		binary.LittleEndian.PutUint64(key[1:], record.a)
	}
	return key, nil
}

func (m *multiAggregateState) group(record aggregateRecord) (int, bool, error) {
	key, err := m.key(record)
	if err != nil {
		return 0, false, err
	}
	if id, ok := m.index.Lookup(key); ok {
		return id, false, nil
	}
	// Ensure state first. A rejected index insertion must never leave a key
	// pointing to nonexistent state. Extra unused capacity is safe on retry.
	var growErr error
	if m.numeric != nil {
		growErr = m.numeric.EnsureGroups(m.index.Len() + 1)
	} else if m.bundle != nil {
		growErr = m.bundle.EnsureGroups(m.index.Len() + 1)
	}
	if err := growErr; err != nil {
		return 0, false, err
	}
	if m.statistics != nil {
		if err := m.statistics.EnsureGroups(m.index.Len() + 1); err != nil {
			return 0, false, err
		}
	}
	return m.index.LookupOrInsert(key)
}

func (m *multiAggregateState) add(record aggregateRecord, batch *memory.Batch, row int) (bool, error) {
	id, created, err := m.group(record)
	if err != nil {
		return false, err
	}
	if m.compiled.Statistics != nil {
		if err := m.byteWorkspace.prepare(m.statistics, m.compiled.Statistics, id, batch, row, record.ordinal); err != nil {
			return false, err
		}
		defer m.byteWorkspace.abort()
	}
	for lane, name := range m.compiled.Columns {
		if name == "" {
			if m.numeric != nil {
				m.numeric.AddBits(id, lane, 0, false)
			} else {
				m.bundle.AddValue(id, lane, 0, false)
			}
			continue
		}
		if m.numeric != nil {
			bits, valid := nativeAggregateBits(batch, name, m.compiled.Types[lane], m.compiled.Layouts[lane], row)
			m.numeric.AddBits(id, lane, bits, valid)
			continue
		}
		v := batch.Int64s[name]
		m.bundle.AddValue(id, lane, v.Values[row], v.IsValid(row))
	}
	if m.statistics != nil {
		for lane, name := range m.compiled.Statistics.Columns {
			if byteStatisticsType(m.compiled.Statistics.Types[lane]) {
				continue
			}
			bits, valid := statisticsInputBits(batch, name, m.compiled.Statistics.Types[lane], row)
			if err := m.statistics.AddBits(id, lane, bits, valid, record.ordinal); err != nil {
				return false, err
			}
		}
	}
	m.byteWorkspace.commit()
	return created, nil
}

func (m *multiAggregateState) merge(record aggregateRecord) (bool, error) {
	id, created, err := m.group(record)
	if err != nil {
		return false, err
	}
	if m.compiled.Statistics != nil {
		if err := m.byteWorkspace.prepareMerge(m.statistics, m.compiled.Statistics, id, record); err != nil {
			return false, err
		}
		defer m.byteWorkspace.abort()
	}
	for lane := range m.compiled.Columns {
		if m.numeric != nil {
			if err := m.numeric.Merge(id, lane, numericAggregateRecordState(record, lane, m.compiled.Types[lane], m.compiled.Layouts[lane])); err != nil {
				return false, err
			}
		} else {
			m.bundle.Merge(id, lane, aggregateRecordState(record, lane))
		}
	}
	if m.statistics != nil {
		s := m.compiled.Statistics
		for lane := range s.Columns {
			if byteStatisticsType(s.Types[lane]) {
				continue
			}
			if err := m.statistics.Merge(id, lane, statisticsAggregateRecordState(record, lane, s.Types[lane], s.Layouts[lane])); err != nil {
				return false, err
			}
		}
	}
	m.byteWorkspace.commit()
	return created, nil
}

func (m *multiAggregateState) record(id int) aggregateRecord {
	key := m.index.Key(id)
	r := aggregateRecord{validMask: key[0], bundle: m.bundle, numericBundle: m.numeric, statisticsBundle: m.statistics, groupID: id}
	if m.bundle != nil {
		r.state = m.bundle.Result(id, 0)
	}
	switch m.table.shape {
	case GroupShapePairFixed:
		r.a = binary.LittleEndian.Uint64(key[1:9])
		r.b = binary.LittleEndian.Uint64(key[9:17])
	case GroupShapeStringView:
		r.stringKey = key[1:]
	case GroupShapeGeneric:
		r.validMask = 1
		r.stringKey = key
	default:
		r.a = binary.LittleEndian.Uint64(key[1:])
	}
	return r
}

func (m *multiAggregateState) visitChunk(cursor *int, limit int, fn func(aggregateRecord) error) (bool, error) {
	end := min(m.index.Len(), *cursor+limit)
	for *cursor < end {
		if err := fn(m.record(*cursor)); err != nil {
			return false, err
		}
		*cursor++
	}
	return *cursor == m.index.Len(), nil
}

func (m *multiAggregateState) close() {
	m.byteWorkspace.close()
	m.index.Close()
	if m.statistics != nil {
		m.statistics.Close()
	}
	if m.bundle != nil {
		m.bundle.Close()
	}
	if m.numeric != nil {
		m.numeric.Close()
	}
	m.scratch = nil
	if m.scratchLease != nil {
		m.scratchLease.Release()
		m.scratchLease = nil
	}
}

func (p *PreparedQuery) bindMultiValues(batch *memory.Batch) error {
	if p.cq.MultiAggregate == nil {
		return nil
	}
	for lane, name := range p.cq.MultiAggregate.Columns {
		if name == "" {
			continue
		}
		rows, validity, err := aggregateColumnInfo(batch, name, p.cq.MultiAggregate.Types[lane], aggregateNeedsValue(p.cq.MultiAggregate.Layouts[lane]))
		if err != nil {
			return err
		}
		if rows < batch.Rows || (validity != nil && len(validity) < memory.WordsFor(batch.Rows)) {
			return fmt.Errorf("aggregate column %q shorter than batch", name)
		}
	}
	if s := p.statistics(); s != nil {
		for lane, name := range s.Columns {
			rows, validity, err := aggregateColumnInfo(batch, name, s.Types[lane], true)
			if err != nil {
				return err
			}
			if rows < batch.Rows || (validity != nil && len(validity) < memory.WordsFor(batch.Rows)) {
				return fmt.Errorf("statistics column %q shorter than batch", name)
			}
		}
	}
	return nil
}

func (p *PreparedQuery) aggregateMultiResidentBatch(ctx context.Context, batch *memory.Batch, table *aggregateStateTable, workspace *aggregatePredicateWorkspace) (selected, groups int, checksum int64, err error) {
	binding, err := p.bindSortedBatch(batch)
	if err != nil {
		return 0, 0, 0, err
	}
	binding.ordinalBase, err = workspace.nextOrdinal(batch.Rows)
	if err != nil {
		return 0, 0, 0, err
	}
	selection, err := workspace.evaluate(ctx, p.cq.Predicate, batch, p.e.kernels)
	if err != nil {
		return 0, 0, 0, err
	}
	err = forSelectedFrom(selection.Truth, batch.Rows, 0, func(row int) error {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		created, err := table.multi.add(binding.record(table.shape, row), batch, row)
		if err != nil {
			return err
		}
		selected++
		if created {
			groups++
		}
		v, valid := binding.value(row)
		if valid {
			checksum += v
		}
		return nil
	})
	return
}

func (p *PreparedQuery) aggregateRunCodec() string {
	if p.hasByteStatistics() {
		return bytesStatisticsAggregateRunCodec
	}
	if p.statistics() != nil {
		return "aggregate-statistics-state-v4"
	}
	if p.nativeAggregate() {
		return "aggregate-native-state-v3"
	}
	if p.cq.MultiAggregate != nil {
		return "aggregate-int64-bundle-v2"
	}
	return aggregateRunCodec
}

func (p *PreparedQuery) encodeAggregateRun(ctx context.Context, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if s := p.statistics(); s != nil {
		if p.hasByteStatistics() {
			return encodeBytesStatisticsAggregateRun(ctx, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts(), s.Types, s.Layouts, visit, filter, workspace)
		}
		return encodeStatisticsAggregateRun(ctx, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts(), s.Types, s.Layouts, visit, filter, workspace)
	}
	if p.nativeAggregate() {
		return encodeNumericAggregateRun(ctx, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts(), visit, filter, workspace)
	}
	if p.cq.MultiAggregate != nil {
		return encodeMultiAggregateRun(ctx, p.cq.GroupShape, p.multiLayouts(), visit, filter, workspace)
	}
	return encodeAggregateRun(ctx, p.cq.GroupShape, p.cq.AggregateLayout, visit, filter, workspace)
}

func (p *PreparedQuery) newAggregateRunReader(payload []byte) (*aggregateRunReader, error) {
	if s := p.statistics(); s != nil {
		if p.hasByteStatistics() {
			reader, err := newBytesStatisticsAggregateRunReader(payload, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts(), s.Types, s.Layouts)
			if err != nil {
				return nil, err
			}
			return &aggregateRunReader{bytesStatistics: reader, groups: reader.groups}, nil
		}
		reader, err := newStatisticsAggregateRunReader(payload, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts(), s.Types, s.Layouts)
		if err != nil {
			return nil, err
		}
		return &aggregateRunReader{statistics: reader, groups: reader.groups}, nil
	}
	if p.nativeAggregate() {
		reader, err := newNumericAggregateRunReader(payload, p.cq.GroupShape, p.cq.MultiAggregate.Types, p.multiLayouts())
		if err != nil {
			return nil, err
		}
		return &aggregateRunReader{numeric: reader, groups: reader.groups}, nil
	}
	if p.cq.MultiAggregate != nil {
		reader, err := newMultiAggregateRunReader(payload, p.cq.GroupShape, p.multiLayouts())
		if err != nil {
			return nil, err
		}
		return &aggregateRunReader{multi: reader, groups: reader.groups}, nil
	}
	return newAggregateRunReader(payload, p.cq.GroupShape, p.cq.AggregateLayout)
}

func (s *sortedAggregateStream) addBundleRow() error {
	row := s.nextRow - 1
	if st := s.query.statistics(); st != nil {
		if err := s.byteWorkspace.prepare(s.statistics, st, 0, s.binding.batch, row, aggregate.SourceOrdinal{Row: s.binding.ordinalBase + uint64(row)}); err != nil {
			return err
		}
		defer s.byteWorkspace.abort()
	}
	for lane, name := range s.query.cq.MultiAggregate.Columns {
		if name == "" {
			if s.numeric != nil {
				s.numeric.AddBits(0, lane, 0, false)
			} else {
				s.bundle.AddValue(0, lane, 0, false)
			}
			continue
		}
		if s.numeric != nil {
			bits, valid := nativeAggregateBits(s.binding.batch, name, s.query.cq.MultiAggregate.Types[lane], s.query.cq.MultiAggregate.Layouts[lane], row)
			s.numeric.AddBits(0, lane, bits, valid)
			continue
		}
		v := s.binding.batch.Int64s[name]
		s.bundle.AddValue(0, lane, v.Values[row], v.IsValid(row))
	}
	if s.statistics != nil {
		st := s.query.statistics()
		for lane, name := range st.Columns {
			if byteStatisticsType(st.Types[lane]) {
				continue
			}
			bits, valid := statisticsInputBits(s.binding.batch, name, st.Types[lane], row)
			if err := s.statistics.AddBits(0, lane, bits, valid, aggregate.SourceOrdinal{Row: s.binding.ordinalBase + uint64(row)}); err != nil {
				return err
			}
		}
	}
	s.byteWorkspace.commit()
	return nil
}
