package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"math"
	"math/bits"
	"sync"
	"time"
)

const (
	aggregateRunVersion   = uint16(1)
	aggregateRunHeaderLen = 16
	aggregateStateBytes   = 43
	aggregateRadixBits    = uint(4)
	maxAggregateDepth     = 16
	aggregateRunCodec     = "aggregate-int64-state-v1"
)

var aggregateRunMagic = [8]byte{'G', 'L', 'A', 'G', 'G', '0', '0', '1'}

var errAggregateStateTarget = errors.New("aggregate retained-state target reached")

type aggregateRecord struct {
	ordinal   aggregate.SourceOrdinal
	canonical *canonicalGroupBinding

	a, b             uint64
	validMask        uint8
	stringKey        []byte
	state            aggregate.Int64Result
	states           []aggregate.Int64Result
	rawStates        []byte
	bundle           *aggregate.Int64Bundle
	groupID          int
	numericBundle    *aggregate.NumericBundle
	numericRaw       []byte
	statisticsRaw    []byte
	statisticsBundle *aggregate.StatisticsBundle
}

type aggregateRunRef struct {
	handle    memory.SpillHandle
	groups    int
	partition int
}

type aggregateStateTable struct {
	shape       GroupShape
	layout      aggregate.Layout
	maxRetained int64
	retained    int64
	reserve     hashagg.ReserveFunc

	i32   *hashagg.I32Table
	u64   *hashagg.U64Table
	u32   *hashagg.U32Table
	pair  *hashagg.PairTable
	str   *hashagg.StringTable
	multi *multiAggregateState
}

func newAggregateStateTable(shape GroupShape, layout aggregate.Layout, cardinalityHint, averageKeyBytes int, maxRetained int64, reserve hashagg.ReserveFunc, compiled ...*CompiledMultiAggregate) (*aggregateStateTable, error) {
	if reserve == nil {
		return nil, fmt.Errorf("aggregate state table requires a reservation callback")
	}
	t := &aggregateStateTable{shape: shape, layout: layout, maxRetained: maxRetained, reserve: reserve}
	if len(compiled) > 0 && compiled[0] != nil {
		var err error
		t.multi, err = newMultiAggregateState(t, compiled[0], cardinalityHint)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	arenaHint := 64
	if averageKeyBytes > 0 {
		if averageKeyBytes > 4096 || cardinalityHint > 4096/averageKeyBytes {
			arenaHint = 4096
		} else {
			arenaHint = max(64, averageKeyBytes*cardinalityHint)
		}
	}
	opts := hashagg.TableOptions{Aggregates: layout, Reserve: t.reserveGrowth}
	var err error
	switch shape {
	case GroupShapeI32:
		t.i32, err = hashagg.NewI32TableWithOptions(cardinalityHint, opts)
		if err == nil {
			t.i32.Reset()
		}
	case GroupShapeI64:
		t.u64, err = hashagg.NewU64TableWithOptions(cardinalityHint, opts)
		if err == nil {
			t.u64.Reset()
		}
	case GroupShapeU32, GroupShapeDictionaryString:
		t.u32, err = hashagg.NewU32TableWithOptions(cardinalityHint, opts)
		if err == nil {
			t.u32.Reset()
		}
	case GroupShapePairFixed:
		t.pair, err = hashagg.NewPairTableWithOptions(cardinalityHint, opts)
		if err == nil {
			t.pair.Reset()
		}
	case GroupShapeStringView:
		t.str, err = hashagg.NewStringTableWithOptions(cardinalityHint, arenaHint, opts)
		if err == nil {
			t.str.Reset()
		}
	default:
		return nil, fmt.Errorf("spillable aggregate does not support group shape %d", shape)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Each physical component owns its reservation. Replacement is charged at its
// full new size while the old component remains live; releasing the old token
// after replacement reduces the tracked state exactly once.
type aggregateStateReservation struct {
	lease memory.Reservation
	table *aggregateStateTable
	bytes int64
}

// Bytes returns the physical component capacity charged by this token.
func (r *aggregateStateReservation) Bytes() int64 { return r.bytes }

// Release relinquishes this component exactly once and updates live state bytes.
func (r *aggregateStateReservation) Release() {
	if r.lease == nil {
		return
	}
	r.lease.Release()
	r.lease = nil
	r.table.retained -= r.bytes
}

func (t *aggregateStateTable) reserveGrowth(bytes int64) (memory.Reservation, error) {
	if t.maxRetained > 0 && (bytes > t.maxRetained || t.retained > t.maxRetained-bytes) {
		return nil, errAggregateStateTarget
	}
	lease, err := t.reserve(bytes)
	if err != nil {
		if lease != nil {
			lease.Release()
		}
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("aggregate state reservation is nil for %d bytes", bytes)
	}
	if lease.Bytes() != bytes {
		lease.Release()
		return nil, fmt.Errorf("aggregate state reservation size differs from %d bytes", bytes)
	}
	t.retained += bytes
	return &aggregateStateReservation{lease: lease, table: t, bytes: bytes}, nil
}

func (t *aggregateStateTable) close() {
	if t.multi != nil {
		t.multi.close()
		t.multi = nil
	}
	if t.i32 != nil {
		t.i32.Close()
		t.i32 = nil
	}
	if t.u64 != nil {
		t.u64.Close()
		t.u64 = nil
	}
	if t.u32 != nil {
		t.u32.Close()
		t.u32 = nil
	}
	if t.pair != nil {
		t.pair.Close()
		t.pair = nil
	}
	if t.str != nil {
		t.str.Close()
		t.str = nil
	}
}

func (t *aggregateStateTable) len() int {
	if t.multi != nil {
		return t.multi.index.Len()
	}
	switch t.shape {
	case GroupShapeI32:
		return t.i32.Len()
	case GroupShapeI64:
		return t.u64.Len()
	case GroupShapeU32, GroupShapeDictionaryString:
		return t.u32.Len()
	case GroupShapePairFixed:
		return t.pair.Len()
	case GroupShapeStringView:
		return t.str.Len()
	default:
		return 0
	}
}

func (t *aggregateStateTable) visit(fn func(aggregateRecord) error) error {
	if t.multi != nil {
		cursor := 0
		_, err := t.multi.visitChunk(&cursor, t.multi.index.Len(), fn)
		return err
	}
	switch t.shape {
	case GroupShapeI32:
		return t.i32.Visit(func(key int32, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: uint64(uint32(key)), validMask: mask, state: state})
		})
	case GroupShapeI64:
		return t.u64.Visit(func(key uint64, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: key, validMask: mask, state: state})
		})
	case GroupShapeU32, GroupShapeDictionaryString:
		return t.u32.Visit(func(key uint32, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: uint64(key), validMask: mask, state: state})
		})
	case GroupShapePairFixed:
		return t.pair.Visit(func(key hashagg.Key128, validMask uint8, state aggregate.Int64Result) error {
			return fn(aggregateRecord{a: key.A, b: key.B, validMask: validMask, state: state})
		})
	case GroupShapeStringView:
		return t.str.Visit(func(key []byte, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{stringKey: key, validMask: mask, state: state})
		})
	default:
		return fmt.Errorf("unsupported aggregate table shape %d", t.shape)
	}
}

// visitChunk advances a stable table-generation cursor and emits at most
// limit records. It lets resident aggregation produce bounded output batches
// without first materializing an engine.Result or a group slice.
func (t *aggregateStateTable) visitChunk(cursor *int, limit int, fn func(aggregateRecord) error) (bool, error) {
	if t.multi != nil {
		return t.multi.visitChunk(cursor, limit, fn)
	}
	switch t.shape {
	case GroupShapeI32:
		return t.i32.VisitChunk(cursor, limit, func(key int32, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: uint64(uint32(key)), validMask: mask, state: state})
		})
	case GroupShapeI64:
		return t.u64.VisitChunk(cursor, limit, func(key uint64, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: key, validMask: mask, state: state})
		})
	case GroupShapeU32, GroupShapeDictionaryString:
		return t.u32.VisitChunk(cursor, limit, func(key uint32, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{a: uint64(key), validMask: mask, state: state})
		})
	case GroupShapePairFixed:
		return t.pair.VisitChunk(cursor, limit, func(key hashagg.Key128, validMask uint8, state aggregate.Int64Result) error {
			return fn(aggregateRecord{a: key.A, b: key.B, validMask: validMask, state: state})
		})
	case GroupShapeStringView:
		return t.str.VisitChunk(cursor, limit, func(key []byte, valid bool, state aggregate.Int64Result) error {
			mask := uint8(0)
			if valid {
				mask = 1
			}
			return fn(aggregateRecord{stringKey: key, validMask: mask, state: state})
		})
	default:
		return false, fmt.Errorf("unsupported aggregate table shape %d", t.shape)
	}
}

func (t *aggregateStateTable) merge(record aggregateRecord) (bool, error) {
	if t.multi != nil {
		return t.multi.merge(record)
	}
	switch t.shape {
	case GroupShapeI32:
		return t.i32.TryMergeNullable(int32(uint32(record.a)), record.validMask&1 != 0, record.state)
	case GroupShapeI64:
		return t.u64.TryMergeNullable(record.a, record.validMask&1 != 0, record.state)
	case GroupShapeU32, GroupShapeDictionaryString:
		return t.u32.TryMergeNullable(uint32(record.a), record.validMask&1 != 0, record.state)
	case GroupShapePairFixed:
		return t.pair.TryMergeNullable(hashagg.Key128{A: record.a, B: record.b}, record.validMask, record.state)
	case GroupShapeStringView:
		return t.str.TryMergeNullable(record.stringKey, record.validMask&1 != 0, record.state)
	default:
		return false, fmt.Errorf("unsupported aggregate table shape %d", t.shape)
	}
}

func (t *aggregateStateTable) add(record aggregateRecord, value int64, valueValid bool) (bool, error) {
	switch t.shape {
	case GroupShapeI32:
		return t.i32.TryAddNullableValue(int32(uint32(record.a)), record.validMask&1 != 0, value, valueValid)
	case GroupShapeI64:
		return t.u64.TryAddNullableValue(record.a, record.validMask&1 != 0, value, valueValid)
	case GroupShapeU32, GroupShapeDictionaryString:
		return t.u32.TryAddNullableValue(uint32(record.a), record.validMask&1 != 0, value, valueValid)
	case GroupShapePairFixed:
		return t.pair.TryAddNullableValue(hashagg.Key128{A: record.a, B: record.b}, record.validMask, value, valueValid)
	case GroupShapeStringView:
		return t.str.AddNullableValue(record.stringKey, record.validMask&1 != 0, value, valueValid)
	default:
		return false, fmt.Errorf("unsupported aggregate table shape %d", t.shape)
	}
}

func aggregateRecordHash(shape GroupShape, record aggregateRecord) uint64 {
	if shape == GroupShapeGeneric {
		if record.canonical != nil {
			hash, _ := record.canonical.HashRow(int(record.a))
			return hash
		}
		return canonicalGroupKeyHash(record.stringKey)
	}
	if record.validMask == 0 {
		return hashagg.Hash64(0x9e3779b97f4a7c15)
	}
	switch shape {
	case GroupShapeI32:
		return hashagg.Hash64(uint64(uint32(record.a)))
	case GroupShapeI64:
		return hashagg.Hash64(record.a)
	case GroupShapeU32, GroupShapeDictionaryString:
		return hashagg.Hash64(uint64(uint32(record.a)))
	case GroupShapePairFixed:
		return hashagg.Hash128Nullable(hashagg.Key128{A: record.a, B: record.b}, record.validMask)
	case GroupShapeStringView:
		return hashagg.HashBytes(record.stringKey)
	default:
		return 0
	}
}

func aggregateLayoutMask(layout aggregate.Layout) byte {
	var mask byte
	if layout.Sum {
		mask |= 1 << 0
	}
	if layout.CountRows {
		mask |= 1 << 1
	}
	if layout.CountValues {
		mask |= 1 << 2
	}
	if layout.Min {
		mask |= 1 << 3
	}
	if layout.Max {
		mask |= 1 << 4
	}
	if layout.Avg {
		mask |= 1 << 5
	}
	return mask
}

func aggregateRecordSize(shape GroupShape, record aggregateRecord) (int64, error) {
	size := int64(aggregateStateBytes)
	switch shape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		size += 1 + 8
	case GroupShapePairFixed:
		size += 1 + 16
	case GroupShapeStringView:
		if len(record.stringKey) > math.MaxUint32 {
			return 0, fmt.Errorf("aggregate string key exceeds uint32")
		}
		size += 1 + 4 + int64(len(record.stringKey))
	default:
		return 0, fmt.Errorf("unsupported aggregate run shape %d", shape)
	}
	return size, nil
}

type aggregateRunWriter struct {
	buf []byte
	pos int
	err error
}

func (w *aggregateRunWriter) bytes(value []byte) {
	if w.err != nil || len(value) > len(w.buf)-w.pos {
		w.err = fmt.Errorf("aggregate run encoder overflow")
		return
	}
	copy(w.buf[w.pos:], value)
	w.pos += len(value)
}
func (w *aggregateRunWriter) u8(value byte) { w.bytes([]byte{value}) }
func (w *aggregateRunWriter) u16(value uint16) {
	var data [2]byte
	binary.LittleEndian.PutUint16(data[:], value)
	w.bytes(data[:])
}
func (w *aggregateRunWriter) u32(value uint32) {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	w.bytes(data[:])
}
func (w *aggregateRunWriter) u64(value uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	w.bytes(data[:])
}

func encodeAggregateState(w *aggregateRunWriter, state aggregate.Int64Result) {
	w.u64(uint64(state.Sum))
	if state.SumValid {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.u64(state.CountRows)
	w.u64(state.CountValues)
	w.u64(uint64(state.Min))
	if state.MinValid {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.u64(uint64(state.Max))
	if state.MaxValid {
		w.u8(1)
	} else {
		w.u8(0)
	}
}

func encodeAggregateRecord(w *aggregateRunWriter, shape GroupShape, record aggregateRecord) {
	w.u8(record.validMask)
	switch shape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		w.u64(record.a)
	case GroupShapePairFixed:
		w.u64(record.a)
		w.u64(record.b)
	case GroupShapeStringView:
		w.u32(uint32(len(record.stringKey)))
		w.bytes(record.stringKey)
	}
	encodeAggregateState(w, record.state)
}

type aggregateRunReader struct {
	multi      *multiAggregateRunReader
	numeric    *numericAggregateRunReader
	statistics *statisticsAggregateRunReader
	buf        []byte
	pos        int
	err        error
	shape      GroupShape
	layout     aggregate.Layout
	groups     int
}

func newAggregateRunReader(payload []byte, shape GroupShape, layout aggregate.Layout) (*aggregateRunReader, error) {
	r := &aggregateRunReader{buf: payload, shape: shape, layout: layout}
	if string(r.bytes(8)) != string(aggregateRunMagic[:]) || r.u16() != aggregateRunVersion {
		return nil, fmt.Errorf("%w: aggregate run magic or version mismatch", memory.ErrSpillCorrupt)
	}
	if GroupShape(r.u8()) != shape || r.u8() != aggregateLayoutMask(layout) {
		return nil, fmt.Errorf("%w: aggregate run shape or layout mismatch", memory.ErrSpillCorrupt)
	}
	groups := r.u32()
	if uint64(groups) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: aggregate group count exceeds addressable memory", memory.ErrSpillCorrupt)
	}
	r.groups = int(groups)
	if r.err != nil {
		return nil, r.err
	}
	return r, nil
}

func (r *aggregateRunReader) bytes(length int) []byte {
	if r.err != nil || length < 0 || length > len(r.buf)-r.pos {
		if r.err == nil {
			r.err = fmt.Errorf("%w: aggregate run truncated", memory.ErrSpillCorrupt)
		}
		return nil
	}
	value := r.buf[r.pos : r.pos+length]
	r.pos += length
	return value
}
func (r *aggregateRunReader) u8() byte {
	value := r.bytes(1)
	if len(value) != 1 {
		return 0
	}
	return value[0]
}
func (r *aggregateRunReader) u16() uint16 {
	value := r.bytes(2)
	if len(value) != 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(value)
}
func (r *aggregateRunReader) u32() uint32 {
	value := r.bytes(4)
	if len(value) != 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(value)
}
func (r *aggregateRunReader) u64() uint64 {
	value := r.bytes(8)
	if len(value) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(value)
}

func (r *aggregateRunReader) next() (aggregateRecord, bool, error) {
	if r.statistics != nil {
		return r.statistics.next()
	}
	if r.numeric != nil {
		return r.numeric.next()
	}
	if r.multi != nil {
		return r.multi.next()
	}
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if r.groups == 0 {
		if r.pos != len(r.buf) {
			return aggregateRecord{}, false, fmt.Errorf("%w: aggregate run has trailing bytes", memory.ErrSpillCorrupt)
		}
		return aggregateRecord{}, false, nil
	}
	record := aggregateRecord{validMask: r.u8()}
	switch r.shape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		if record.validMask > 1 {
			r.err = fmt.Errorf("%w: aggregate single-key validity mask=%d", memory.ErrSpillCorrupt, record.validMask)
		}
		record.a = r.u64()
	case GroupShapePairFixed:
		if record.validMask > 3 {
			r.err = fmt.Errorf("%w: aggregate pair validity mask=%d", memory.ErrSpillCorrupt, record.validMask)
		}
		record.a, record.b = r.u64(), r.u64()
	case GroupShapeStringView:
		if record.validMask > 1 {
			r.err = fmt.Errorf("%w: aggregate string validity mask=%d", memory.ErrSpillCorrupt, record.validMask)
		}
		length := r.u32()
		if uint64(length) > uint64(math.MaxInt) {
			r.err = fmt.Errorf("%w: aggregate string key exceeds addressable memory", memory.ErrSpillCorrupt)
		} else {
			record.stringKey = r.bytes(int(length))
		}
	default:
		r.err = fmt.Errorf("%w: unsupported aggregate run shape %d", memory.ErrSpillCorrupt, r.shape)
	}
	sum := int64(r.u64())
	sumValid := r.u8()
	countRows := r.u64()
	countValues := r.u64()
	minValue := int64(r.u64())
	minValid := r.u8()
	maxValue := int64(r.u64())
	maxValid := r.u8()
	if sumValid > 1 || minValid > 1 || maxValid > 1 {
		r.err = fmt.Errorf("%w: aggregate state validity byte is not boolean", memory.ErrSpillCorrupt)
	}
	record.state = aggregate.Int64Result{
		Sum:         sum,
		SumValid:    sumValid == 1,
		CountRows:   countRows,
		CountValues: countValues,
		Min:         minValue,
		MinValid:    minValid == 1,
		Max:         maxValue,
		MaxValid:    maxValid == 1,
	}
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	r.groups--
	return record, true, nil
}

func (r *aggregateRunReader) visit(fn func(aggregateRecord) error) error {
	for {
		record, ok, err := r.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := fn(record); err != nil {
			return err
		}
	}
}

type aggregateRecordVisitor func(func(aggregateRecord) error) error
type aggregateRecordFilter func(index int, record aggregateRecord) bool

type encodedAggregateRun struct {
	payload     []byte
	groups      int
	reservation memory.Reservation
}

func (r *encodedAggregateRun) close() {
	if r == nil {
		return
	}
	r.payload = nil
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
}

func encodeAggregateRun(ctx context.Context, shape GroupShape, layout aggregate.Layout, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if visit == nil || workspace == nil {
		return nil, fmt.Errorf("aggregate run encode requires visitor and workspace")
	}
	size := int64(aggregateRunHeaderLen)
	groups := 0
	index := 0
	err := visit(func(record aggregateRecord) error {
		selected := filter == nil || filter(index, record)
		index++
		if !selected {
			return nil
		}
		recordBytes, err := aggregateRecordSize(shape, record)
		if err != nil {
			return err
		}
		if size > math.MaxInt64-recordBytes || uint64(groups) == uint64(math.MaxUint32) {
			return fmt.Errorf("aggregate run size or group count overflow")
		}
		size += recordBytes
		groups++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if groups == 0 {
		return &encodedAggregateRun{}, nil
	}
	if size > int64(math.MaxInt) {
		return nil, fmt.Errorf("aggregate run exceeds addressable memory")
	}
	reservation, err := workspace.Reserve(ctx, size)
	if err != nil {
		return nil, fmt.Errorf("reserve aggregate run encode buffer: %w", err)
	}
	payload := make([]byte, int(size))
	w := &aggregateRunWriter{buf: payload}
	w.bytes(aggregateRunMagic[:])
	w.u16(aggregateRunVersion)
	w.u8(byte(shape))
	w.u8(aggregateLayoutMask(layout))
	w.u32(uint32(groups))
	index = 0
	err = visit(func(record aggregateRecord) error {
		selected := filter == nil || filter(index, record)
		index++
		if selected {
			encodeAggregateRecord(w, shape, record)
		}
		return w.err
	})
	if err != nil || w.err != nil || w.pos != len(payload) {
		reservation.Release()
		if err != nil {
			return nil, err
		}
		if w.err != nil {
			return nil, w.err
		}
		return nil, fmt.Errorf("aggregate run encoded %d bytes, expected %d", w.pos, len(payload))
	}
	return &encodedAggregateRun{payload: payload, groups: groups, reservation: reservation}, nil
}

type aggregateTableOwner struct {
	table *aggregateStateTable
}

func newAggregateTableOwner(ctx context.Context, p *PreparedQuery, operator *memory.OperatorBudget, maxRetained int64) (*aggregateTableOwner, error) {
	table, err := newAggregateStateTable(p.cq.GroupShape, p.cq.AggregateLayout, 8, p.cq.AverageKeyBytes, maxRetained, func(bytes int64) (memory.Reservation, error) {
		return operator.ReserveGrowth(ctx, bytes)
	}, p.cq.MultiAggregate)
	if err != nil {
		return nil, err
	}
	return &aggregateTableOwner{table: table}, nil
}

func (o *aggregateTableOwner) retainedBytes() int64 {
	if o == nil || o.table == nil {
		return 0
	}
	return o.table.retained
}

func (o *aggregateTableOwner) close() int64 {
	if o == nil || o.table == nil {
		return 0
	}
	released := o.table.retained
	o.table.close()
	o.table = nil
	return released
}

// NewSpillableAggregateStream creates one execution-scoped partitioned hash
// aggregation stream. The caller must bind a QueryBudget to ctx and configure
// a finite memory limit and spill quota.
func (p *PreparedQuery) NewSpillableAggregateStream(ctx context.Context, schema memory.Schema, outputRows int) (source.BatchStream, error) {
	var readOpts source.ReadOptions
	if p != nil && p.e != nil {
		readOpts.Kernels = p.e.kernels
		readOpts.BatchRows = p.e.opts.Pipeline.MaxMorselRows
		readOpts.BatchBytes = p.e.opts.Pipeline.MaxMorselBytes
	}
	return p.NewSpillableAggregateStreamWithReadOptions(ctx, schema, outputRows, readOpts)
}

// NewSpillableAggregateStreamWithReadOptions preserves scan limits and memory ownership
// through the aggregate child graph. Kernels must match the prepared query.
func (p *PreparedQuery) NewSpillableAggregateStreamWithReadOptions(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions) (source.BatchStream, error) {
	return p.newSpillableAggregateStream(ctx, schema, outputRows, readOpts, true)
}

// acquirePrepared is false only when Execute already owns the plan lifecycle.
func (p *PreparedQuery) newSpillableAggregateStream(ctx context.Context, schema memory.Schema, outputRows int, readOpts source.ReadOptions, acquirePrepared bool) (source.BatchStream, error) {
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
		return nil, fmt.Errorf("aggregate stream context is nil")
	}
	query := memory.QueryBudgetFromContext(ctx)
	if query == nil {
		return nil, fmt.Errorf("spillable aggregate requires a query memory budget")
	}
	if query.Limit() <= 0 {
		return nil, ErrBoundedMemoryLimitRequired
	}
	if !query.SpillConfigured() {
		return nil, fmt.Errorf("spillable aggregate requires an explicit spill quota")
	}
	if acquirePrepared {
		if err := p.begin(); err != nil {
			return nil, err
		}
	}
	begun := acquirePrepared
	defer func() {
		if begun {
			p.end()
		}
	}()
	store, err := query.SpillManager()
	if err != nil {
		return nil, err
	}
	if outputRows < 1 {
		outputRows = p.e.opts.Pipeline.MaxMorselRows
	}
	rowBytes := pipeline.EstimateSchemaRowBytes(schema)
	budgetRows := query.Limit() / 8 / rowBytes
	if budgetRows < 1 {
		budgetRows = 1
	}
	if int64(outputRows) > budgetRows {
		outputRows = int(budgetRows)
	}
	// A merge may temporarily hold one restored run, one destination table and
	// one encode/output buffer. Keeping retained state below 3/8 of the hard
	// limit prevents spill itself from creating an unaccounted peak.
	maxState := query.Limit() * 3 / 8
	if maxState < 1 {
		maxState = 1
	}
	s := &spillableAggregateStream{
		query:       p,
		readOpts:    readOpts,
		schema:      schema,
		budget:      query,
		store:       store,
		fingerprint: memory.SchemaFingerprint(schema),
		state:       query.Operator("HashAggregate", memory.MemoryRetainedState),
		encoder:     query.Operator("HashAggregateEncode", memory.MemorySpillBuffer),
		restorer:    query.Operator("HashAggregateRestore", memory.MemorySpillBuffer),
		output:      query.Operator("HashAggregateOutput", memory.MemoryWorkspace),
		maxState:    maxState,
		outputRows:  outputRows,
		partitions:  make([][]aggregateRunRef, 1<<aggregateRadixBits),
		started:     time.Now(),
	}
	unregister, err := s.state.RegisterSpiller(s)
	if err != nil {
		return nil, err
	}
	s.unregister = unregister
	s.active = acquirePrepared
	begun = false
	return s, nil
}

type spillableAggregateStream struct {
	query       *PreparedQuery
	readOpts    source.ReadOptions
	schema      memory.Schema
	budget      *memory.QueryBudget
	store       memory.SpillManager
	fingerprint [32]byte
	state       *memory.OperatorBudget
	encoder     *memory.OperatorBudget
	restorer    *memory.OperatorBudget
	output      *memory.OperatorBudget
	maxState    int64
	outputRows  int

	mu          sync.Mutex
	table       *aggregateTableOwner
	partitions  [][]aggregateRunRef
	outputRuns  []aggregateRunRef
	unregister  func()
	outputDst   *memory.Batch
	outputLease memory.Reservation

	selectedRows       int
	checksum           int64
	groupCount         int
	spillRuns          int64
	spillBytes         int64
	maxDepth           int
	maxPartitionGroups int
	started            time.Time
	statsRecorded      bool

	prepared    bool
	active      bool
	closed      bool
	exhausted   bool
	terminalErr error
	closeErr    error
}

// SpillableBytes reports retained aggregate-state bytes eligible for release.
func (s *spillableAggregateStream) SpillableBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.table == nil {
		return 0
	}
	return s.table.retainedBytes()
}

// Spill partitions and persists the current aggregate state under memory pressure.
func (s *spillableAggregateStream) Spill(ctx context.Context, _ memory.SpillRequest) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spillTableLocked(ctx, 0, s.partitions)
}

func (s *spillableAggregateStream) partitionHashes(ctx context.Context, table *aggregateStateTable) ([]uint64, memory.Reservation, error) {
	groups := table.len()
	if groups == 0 {
		return nil, nil, nil
	}
	if groups > math.MaxInt/8 {
		return nil, nil, fmt.Errorf("aggregate partition hash workspace exceeds addressable memory")
	}
	lease, err := s.encoder.ReserveGrowth(ctx, int64(groups)*8)
	if err != nil {
		return nil, nil, fmt.Errorf("reserve aggregate partition hashes: %w", err)
	}
	hashes := make([]uint64, groups)
	index := 0
	batchHash64 := s.query.cq.GroupShape == GroupShapeI32 || s.query.cq.GroupShape == GroupShapeI64 || s.query.cq.GroupShape == GroupShapeU32 || s.query.cq.GroupShape == GroupShapeDictionaryString
	err = table.visit(func(record aggregateRecord) error {
		if index >= len(hashes) {
			return fmt.Errorf("aggregate table visit exceeded group count")
		}
		if batchHash64 {
			if record.validMask == 0 {
				hashes[index] = 0x9e3779b97f4a7c15
			} else {
				hashes[index] = record.a
				if s.query.cq.GroupShape == GroupShapeI32 || s.query.cq.GroupShape == GroupShapeU32 || s.query.cq.GroupShape == GroupShapeDictionaryString {
					hashes[index] = uint64(uint32(record.a))
				}
			}
		} else {
			hashes[index] = aggregateRecordHash(s.query.cq.GroupShape, record)
		}
		index++
		return nil
	})
	if err != nil || index != groups {
		lease.Release()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("aggregate table visit returned %d groups, want %d", index, groups)
	}
	if batchHash64 {
		// Hash64Batch is an operation-specific kernel dispatch. Auto uses it only
		// on architectures with committed benchmark evidence; forced SIMD may
		// correctly fall back, and string/pair keys remain scalar.
		s.query.e.kernels.Hash64Batch(hashes, hashes)
	}
	return hashes, lease, nil
}

func (s *spillableAggregateStream) spillTableLocked(ctx context.Context, bitOffset uint, destinations [][]aggregateRunRef) (int64, error) {
	if s.table == nil || s.table.table == nil || s.table.table.len() == 0 {
		return 0, nil
	}
	owner := s.table
	hashes, hashLease, err := s.partitionHashes(ctx, owner.table)
	if err != nil {
		return 0, err
	}
	if hashLease != nil {
		defer hashLease.Release()
	}
	created := make([]aggregateRunRef, 0, len(destinations))
	for partition := range destinations {
		part := uint64(partition)
		mask := uint64(len(destinations) - 1)
		encoded, err := s.query.encodeAggregateRun(ctx, owner.table.visit, func(index int, _ aggregateRecord) bool {
			return hashes[index]>>bitOffset&mask == part
		}, s.encoder)
		if err != nil {
			s.deleteRunRefs(ctx, created)
			return 0, err
		}
		if encoded.groups == 0 {
			encoded.close()
			continue
		}
		handle, err := s.store.Spill(ctx, memory.SpillObject{
			Kind: memory.SpillAggregateState, Partition: int32(partition), Codec: s.query.aggregateRunCodec(),
			SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
		})
		bytes := handleBytes(handle)
		groups := encoded.groups
		encoded.close()
		if err != nil {
			s.deleteRunRefs(ctx, created)
			return 0, err
		}
		created = append(created, aggregateRunRef{handle: handle, groups: groups, partition: partition})
		s.spillRuns++
		s.spillBytes += bytes
	}
	for _, run := range created {
		destinations[run.partition] = append(destinations[run.partition], run)
		partitionGroups := 0
		for _, existing := range destinations[run.partition] {
			partitionGroups += existing.groups
		}
		if partitionGroups > s.maxPartitionGroups {
			s.maxPartitionGroups = partitionGroups
		}
	}
	released := owner.close()
	s.table = nil
	return released, nil
}

func handleBytes(handle memory.SpillHandle) int64 {
	if handle == nil {
		return 0
	}
	return handle.Bytes()
}

func (s *spillableAggregateStream) deleteRunRefs(ctx context.Context, refs []aggregateRunRef) {
	for _, ref := range refs {
		_ = s.store.Delete(ctx, ref.handle)
	}
}

type aggregateBatchBinding struct {
	ordinalBase uint64
	canonical   *canonicalGroupBinding

	batch   *memory.Batch
	first   fixedColumn
	second  fixedColumn
	strings *memory.StringViewVector
	values  *memory.Int64Vector
}

func (s *spillableAggregateStream) bindBatch(batch *memory.Batch) (aggregateBatchBinding, error) {
	binding := aggregateBatchBinding{batch: batch}
	if err := s.query.bindMultiValues(batch); err != nil {
		return binding, err
	}
	var err error
	if s.query.cq.ValueColumn != "" && (!s.query.nativeAggregate() || s.query.cq.MultiAggregate.Types[0] == memory.TypeInt64) {
		binding.values, err = batch.Int64(s.query.cq.ValueColumn)
		if err != nil {
			return binding, err
		}
	}
	if s.query.cq.GroupShape == GroupShapeGeneric {
		canonical, bindErr := bindCanonicalGroupKeys(s.query.cq.GenericFields, batch)
		binding.canonical = &canonical
		return binding, bindErr
	}
	if s.query.cq.GroupShape == GroupShapeStringView {
		binding.strings, err = batch.String(s.query.cq.GroupBy[0].Name)
		return binding, err
	}
	binding.first, err = fixedFromBatch(batch, s.query.cq.GroupBy[0])
	if err != nil {
		return binding, err
	}
	if s.query.cq.GroupShape == GroupShapePairFixed {
		binding.second, err = fixedFromBatch(batch, s.query.cq.GroupBy[1])
	}
	return binding, err
}

type aggregateKey struct {
	a, b      uint64
	validMask uint8
	stringKey []byte
}

func (b aggregateBatchBinding) record(shape GroupShape, row int) aggregateRecord {
	if shape == GroupShapeGeneric {
		return aggregateRecord{a: uint64(row), validMask: 1, canonical: b.canonical, ordinal: aggregate.SourceOrdinal{Row: b.ordinalBase + uint64(row)}}
	}
	key := b.key(shape, row)
	return aggregateRecord{a: key.a, b: key.b, validMask: key.validMask, stringKey: key.stringKey, ordinal: aggregate.SourceOrdinal{Row: b.ordinalBase + uint64(row)}}
}

func (b aggregateBatchBinding) key(shape GroupShape, row int) aggregateKey {
	switch shape {
	case GroupShapeGlobal:
		return aggregateKey{}
	case GroupShapeStringView:
		if !b.strings.IsValid(row) {
			return aggregateKey{}
		}
		return aggregateKey{stringKey: b.strings.ValueBytes(row), validMask: 1}
	case GroupShapePairFixed:
		mask := uint8(0)
		record := aggregateKey{}
		if b.first.valid(row) {
			mask |= 1
			record.a = b.first.at(row)
		}
		if b.second.valid(row) {
			mask |= 2
			record.b = b.second.at(row)
		}
		record.validMask = mask
		return record
	default:
		if !b.first.valid(row) {
			return aggregateKey{}
		}
		return aggregateKey{a: b.first.at(row), validMask: 1}
	}
}

func (b aggregateBatchBinding) value(row int) (int64, bool) {
	if b.values == nil {
		return 0, false
	}
	return b.values.Values[row], b.values.IsValid(row)
}

func (s *spillableAggregateStream) ensureTableLocked(ctx context.Context) error {
	if s.table != nil {
		return nil
	}
	owner, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		return err
	}
	s.table = owner
	return nil
}

func forSelectedFrom(selection memory.Bitmap, rows, start int, fn func(int) error) error {
	for wi, word := range selection {
		base := wi << 6
		for word != 0 {
			bit := bitsTrailingZeros64(word)
			row := base + bit
			if row >= rows {
				return nil
			}
			word &= word - 1
			if row < start {
				continue
			}
			if err := fn(row); err != nil {
				return err
			}
		}
	}
	return nil
}

func bitsTrailingZeros64(value uint64) int {
	if value == 0 {
		return 64
	}
	return bits.TrailingZeros64(value)
}

func (s *spillableAggregateStream) aggregateBatch(ctx context.Context, batch *memory.Batch, workspace *aggregatePredicateWorkspace) error {
	binding, err := s.bindBatch(batch)
	if err != nil {
		return err
	}
	binding.ordinalBase, err = workspace.nextOrdinal(batch.Rows)
	if err != nil {
		return err
	}
	selection, err := workspace.evaluate(ctx, s.query.cq.Predicate, batch, s.query.e.kernels)
	if err != nil {
		return err
	}
	resume := 0
	for {
		failedRow := -1
		var pressureErr error
		s.mu.Lock()
		err = s.ensureTableLocked(ctx)
		if err == nil {
			err = forSelectedFrom(selection.Truth, batch.Rows, resume, func(row int) error {
				record := binding.record(s.query.cq.GroupShape, row)
				value, valueValid := binding.value(row)
				var addErr error
				if s.table.table.multi != nil {
					_, addErr = s.table.table.multi.add(record, batch, row)
				} else {
					_, addErr = s.table.table.add(record, value, valueValid)
				}
				if addErr != nil {
					failedRow = row
					return addErr
				}
				s.selectedRows++
				if valueValid {
					s.checksum += value
				}
				resume = row + 1
				return nil
			})
		}
		if err != nil && (errors.Is(err, memory.ErrBudgetExceeded) || errors.Is(err, errAggregateStateTarget)) {
			pressureErr = err
		}
		retained := int64(0)
		if s.table != nil {
			retained = s.table.retainedBytes()
		}
		s.mu.Unlock()
		if err == nil {
			break
		}
		if pressureErr == nil || failedRow < 0 || retained == 0 {
			if errors.Is(err, errAggregateStateTarget) {
				return errors.Join(memory.ErrBudgetExceeded, err)
			}
			return err
		}
		released, spillErr := s.state.RequestHardSpill(ctx, retained)
		if spillErr != nil {
			return errors.Join(pressureErr, spillErr)
		}
		if released == 0 {
			return fmt.Errorf("aggregate cannot release state for row %d: %w", failedRow, errors.Join(memory.ErrBudgetExceeded, pressureErr))
		}
		resume = failedRow
	}
	_, err = s.state.RequestSoftSpill(ctx)
	return err
}

func (s *spillableAggregateStream) consumeInput(ctx context.Context) error {
	exchange := s.budget.Operator("AggregateExchange", memory.MemoryInFlightBatch)
	opts := s.query.e.opts.Pipeline
	if s.query.orderedStatistics() {
		opts.Ordering = pipeline.OrderingStable
	}
	stream, err := pipeline.OpenBatchStream(ctx, s.query.src, s.readOpts, s.query.parts, exchange, opts, s.query.e.opts.Observer)
	if err != nil {
		return err
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	workspace := aggregatePredicateWorkspace{manager: s.budget.Operator("SpillAggregatePredicate", memory.MemoryWorkspace), operator: "SpillAggregatePredicate"}
	defer workspace.close()

	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			return errors.Join(nextErr, stream.Close())
		}
		if !ok {
			if closeErr := stream.Close(); closeErr != nil {
				return closeErr
			}
			break
		}
		if err := s.aggregateBatch(ctx, batch, &workspace); err != nil {
			return errors.Join(err, stream.Close())
		}
	}
	s.mu.Lock()
	retained := int64(0)
	if s.table != nil {
		retained = s.table.retainedBytes()
	}
	s.mu.Unlock()
	if retained > 0 {
		released, err := s.state.RequestHardSpill(ctx, retained)
		if err != nil {
			return err
		}
		if released == 0 {
			return fmt.Errorf("aggregate final state could not be spilled")
		}
	}
	return nil
}
