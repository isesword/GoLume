package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math"
)

const (
	statisticsAggregateRunVersion   = uint16(4)
	statisticsAggregateRunHeaderLen = 24
	statisticsAggregateRunCodec     = "aggregate-statistics-state-v4"
	statisticsAggregateStateBytes   = 92
)

var statisticsAggregateRunMagic = [8]byte{'G', 'L', 'A', 'G', 'G', '0', '0', '4'}

// encodeStatisticsAggregateRun visits immutable logical records twice, first to
// reserve the complete payload and then to encode it. Record lane slices and
// borrowed key/state views need remain valid only for their callback duration.
func encodeStatisticsAggregateRun(ctx context.Context, shape GroupShape, numericTypes []memory.DataType, numericLayouts []aggregate.Layout, types []memory.DataType, layouts []aggregate.StatisticsLayout, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if ctx == nil || visit == nil || workspace == nil {
		return nil, fmt.Errorf("statistics-aggregate encode requires context, visitor and workspace")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStatisticsAggregateDescriptors(numericTypes, numericLayouts, types, layouts); err != nil {
		return nil, err
	}
	if _, err := multiAggregateKeyBytes(shape, aggregateRecord{}); err != nil {
		return nil, err
	}
	lanes := len(layouts)
	stateBytes := int64(lanes)*statisticsAggregateStateBytes + int64(len(numericTypes))*numericAggregateStateBytes
	size := int64(statisticsAggregateRunHeaderLen) + 2*int64(lanes+len(numericTypes))
	groups, index := 0, 0
	err := visit(func(record aggregateRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		selected := filter == nil || filter(index, record)
		index++
		if !selected {
			return nil
		}
		if err := validateStatisticsAggregateRecord(record, numericTypes, numericLayouts, types, layouts); err != nil {
			return err
		}
		keyBytes, err := multiAggregateKeyBytes(shape, record)
		if err != nil {
			return err
		}
		if keyBytes > math.MaxInt64-stateBytes || size > math.MaxInt64-stateBytes-keyBytes || uint64(groups) == math.MaxUint32 {
			return fmt.Errorf("statistics-aggregate run size or group count overflow")
		}
		size += keyBytes + stateBytes
		groups++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if groups == 0 {
		return &encodedAggregateRun{}, nil
	}
	if size > int64(math.MaxInt) {
		return nil, fmt.Errorf("statistics-aggregate run exceeds addressable memory")
	}
	reservation, err := workspace.Reserve(ctx, size)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return nil, fmt.Errorf("reserve statistics-aggregate run buffer: %w", err)
	}
	if reservation == nil {
		return nil, fmt.Errorf("statistics-aggregate run buffer reservation is nil")
	}
	if reservation.Bytes() != size {
		reservation.Release()
		return nil, fmt.Errorf("statistics-aggregate run buffer reservation size mismatch")
	}
	committed := false
	defer func() {
		if !committed {
			reservation.Release()
		}
	}()
	payload := make([]byte, int(size))
	w := aggregateRunWriter{buf: payload}
	w.bytes(statisticsAggregateRunMagic[:])
	w.u16(statisticsAggregateRunVersion)
	w.u8(byte(shape))
	w.u8(0)
	w.u32(uint32(len(numericTypes)))
	w.u32(uint32(lanes))
	w.u32(uint32(groups))
	for lane, layout := range numericLayouts {
		w.u8(byte(numericTypes[lane]))
		w.u8(aggregateLayoutMask(layout))
	}
	for lane, layout := range layouts {
		w.u8(byte(types[lane]))
		w.u8(statisticsLayoutMask(layout))
	}
	index = 0
	written := 0
	err = visit(func(record aggregateRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		selected := filter == nil || filter(index, record)
		index++
		if !selected {
			return nil
		}
		if err := validateStatisticsAggregateRecord(record, numericTypes, numericLayouts, types, layouts); err != nil {
			return err
		}
		if _, err := multiAggregateKeyBytes(shape, record); err != nil {
			return err
		}
		if written >= groups {
			return fmt.Errorf("statistics-aggregate visitor changed group count")
		}
		w.u8(record.validMask)
		switch shape {
		case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
			w.u64(record.a)
		case GroupShapePairFixed:
			w.u64(record.a)
			w.u64(record.b)
		case GroupShapeStringView, GroupShapeGeneric:
			w.u32(uint32(len(record.stringKey)))
			w.bytes(record.stringKey)
		}
		for lane := range numericTypes {
			encodeNumericAggregateState(&w, numericAggregateRecordState(record, lane, numericTypes[lane], numericLayouts[lane]))
		}
		for lane := 0; lane < lanes; lane++ {
			encodeStatisticsAggregateState(&w, statisticsAggregateRecordState(record, lane, types[lane], layouts[lane]))
		}
		written++
		return w.err
	})
	if err != nil {
		return nil, err
	}
	if w.err != nil {
		return nil, w.err
	}
	if written != groups || w.pos != len(payload) {
		return nil, fmt.Errorf("statistics-aggregate visitor changed encoded size or group count")
	}
	committed = true
	return &encodedAggregateRun{payload: payload, groups: groups, reservation: reservation}, nil
}

type statisticsAggregateRunReader struct {
	aggregateRunReader
	lanes        int
	numericLanes int
	types        []memory.DataType
	layouts      []aggregate.StatisticsLayout
}

// newStatisticsAggregateRunReader borrows payload and descriptor slices. Their
// owner must keep them immutable and live until iteration finishes.
func newStatisticsAggregateRunReader(payload []byte, shape GroupShape, numericTypes []memory.DataType, numericLayouts []aggregate.Layout, types []memory.DataType, layouts []aggregate.StatisticsLayout) (*statisticsAggregateRunReader, error) {
	if err := validateStatisticsAggregateDescriptors(numericTypes, numericLayouts, types, layouts); err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	keyBytes, err := multiAggregateKeyBytes(shape, aggregateRecord{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	reader := &statisticsAggregateRunReader{aggregateRunReader: aggregateRunReader{buf: payload, shape: shape}, lanes: len(layouts), numericLanes: len(numericTypes), types: types, layouts: layouts}
	r := &reader.aggregateRunReader
	if string(r.bytes(8)) != string(statisticsAggregateRunMagic[:]) || r.u16() != statisticsAggregateRunVersion {
		return nil, fmt.Errorf("%w: statistics-aggregate magic or version mismatch", memory.ErrSpillCorrupt)
	}
	if GroupShape(r.u8()) != shape || r.u8() != 0 {
		return nil, fmt.Errorf("%w: statistics-aggregate shape or reserved header mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(numericTypes)) {
		return nil, fmt.Errorf("%w: numeric lane count mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(layouts)) {
		return nil, fmt.Errorf("%w: statistics-aggregate lane count mismatch", memory.ErrSpillCorrupt)
	}
	groups := r.u32()
	if uint64(groups) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: statistics-aggregate group count exceeds addressable memory", memory.ErrSpillCorrupt)
	}
	r.groups = int(groups)
	for lane, layout := range numericLayouts {
		if memory.DataType(r.u8()) != numericTypes[lane] || r.u8() != aggregateLayoutMask(layout) {
			return nil, fmt.Errorf("%w: numeric descriptor mismatch", memory.ErrSpillCorrupt)
		}
	}
	for lane, layout := range layouts {
		if memory.DataType(r.u8()) != types[lane] || r.u8() != statisticsLayoutMask(layout) {
			return nil, fmt.Errorf("%w: statistics-aggregate lane layout mismatch", memory.ErrSpillCorrupt)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	minimum := keyBytes + int64(len(layouts))*statisticsAggregateStateBytes + int64(len(numericTypes))*numericAggregateStateBytes
	if int64(r.groups) > int64(len(payload)-r.pos)/minimum {
		return nil, fmt.Errorf("%w: statistics-aggregate group count exceeds payload", memory.ErrSpillCorrupt)
	}
	if r.groups == 0 && r.pos != len(payload) {
		return nil, fmt.Errorf("%w: statistics-aggregate empty run has trailing bytes", memory.ErrSpillCorrupt)
	}
	return reader, nil
}

func (reader *statisticsAggregateRunReader) next() (aggregateRecord, bool, error) {
	r := &reader.aggregateRunReader
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if r.groups == 0 {
		if r.pos != len(r.buf) {
			r.err = fmt.Errorf("%w: statistics-aggregate run has trailing bytes", memory.ErrSpillCorrupt)
		}
		return aggregateRecord{}, false, r.err
	}
	record := aggregateRecord{validMask: r.u8()}
	switch r.shape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		record.a = r.u64()
	case GroupShapePairFixed:
		record.a, record.b = r.u64(), r.u64()
	case GroupShapeStringView, GroupShapeGeneric:
		length := r.u32()
		if uint64(length) > uint64(math.MaxInt) {
			r.err = fmt.Errorf("%w: statistics-aggregate string exceeds addressable memory", memory.ErrSpillCorrupt)
		} else {
			record.stringKey = r.bytes(int(length))
		}
	}
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if _, err := multiAggregateKeyBytes(r.shape, record); err != nil {
		r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
		return aggregateRecord{}, false, r.err
	}
	record.numericRaw = r.bytes(reader.numericLanes * numericAggregateStateBytes)
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if err := validateNumericAggregateRaw(record.numericRaw, reader.numericLanes); err != nil {
		r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
		return aggregateRecord{}, false, r.err
	}
	record.statisticsRaw = r.bytes(reader.lanes * statisticsAggregateStateBytes)
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if err := validateStatisticsAggregateRaw(record.statisticsRaw, reader.types, reader.layouts); err != nil {
		r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
		return aggregateRecord{}, false, r.err
	}
	r.groups--
	return record, true, nil
}

func (reader *statisticsAggregateRunReader) visit(fn func(aggregateRecord) error) error {
	if fn == nil {
		return fmt.Errorf("statistics-aggregate run visitor is nil")
	}
	for {
		record, ok, err := reader.next()
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

// Statistics records retain both endpoint ordinals and raw native bits. All
// floating moment fields travel as IEEE bits; finalization never touches spill.
func statisticsLayoutMask(l aggregate.StatisticsLayout) byte {
	var b byte
	if l.Moments {
		b |= 1
	}
	if l.First {
		b |= 2
	}
	if l.Last {
		b |= 4
	}
	return b
}
func statisticsAggregateRecordState(record aggregateRecord, lane int, typ memory.DataType, layout aggregate.StatisticsLayout) aggregate.StatisticsResult {
	if record.statisticsBundle != nil {
		return record.statisticsBundle.Result(record.groupID, lane)
	}
	d := record.statisticsRaw[lane*statisticsAggregateStateBytes : (lane+1)*statisticsAggregateStateBytes]
	u := func(i int) uint64 { return binary.LittleEndian.Uint64(d[i : i+8]) }
	m := aggregate.MomentsResult{Count: u(0), AnchorBits: u(8), Mean: math.Float64frombits(u(16)), Scale: math.Float64frombits(u(24)), ScaledM2: math.Float64frombits(u(32)), Uncentered: d[40]&1 != 0, NonFinite: d[40]&2 != 0}
	e := aggregate.EndpointResult{Seen: d[41] == 1, FirstOrdinal: aggregate.SourceOrdinal{Partition: u(42), Row: u(50)}, LastOrdinal: aggregate.SourceOrdinal{Partition: u(58), Row: u(66)}, FirstBits: u(74), LastBits: u(82), FirstValid: d[90] == 1, LastValid: d[91] == 1}
	if layout.Moments {
		m.Type = typ
	}
	if layout.First || layout.Last {
		e.Type = typ
	}
	return aggregate.StatisticsResult{Type: typ, Layout: layout, Moments: m, Endpoints: e}
}
func statisticsBool(b bool) byte {
	if b {
		return 1
	}
	return 0
}
func encodeStatisticsAggregateState(w *aggregateRunWriter, s aggregate.StatisticsResult) {
	m, e := s.Moments, s.Endpoints
	w.u64(m.Count)
	w.u64(m.AnchorBits)
	w.u64(math.Float64bits(m.Mean))
	w.u64(math.Float64bits(m.Scale))
	w.u64(math.Float64bits(m.ScaledM2))
	w.u8(statisticsBool(m.Uncentered) | statisticsBool(m.NonFinite)<<1)
	w.u8(statisticsBool(e.Seen))
	w.u64(e.FirstOrdinal.Partition)
	w.u64(e.FirstOrdinal.Row)
	w.u64(e.LastOrdinal.Partition)
	w.u64(e.LastOrdinal.Row)
	w.u64(e.FirstBits)
	w.u64(e.LastBits)
	w.u8(statisticsBool(e.FirstValid))
	w.u8(statisticsBool(e.LastValid))
}
func validateStatisticsAggregateDescriptors(nt []memory.DataType, nl []aggregate.Layout, st []memory.DataType, sl []aggregate.StatisticsLayout) error {
	if len(nt) != len(nl) {
		return fmt.Errorf("statistics-aggregate numeric descriptors mismatch")
	}
	if len(nt) > 0 {
		if err := validateNumericAggregateDescriptors(nt, nl); err != nil {
			return err
		}
	}
	if _, err := aggregate.StatisticsBundleBytes(0, st, sl); err != nil {
		return err
	}
	if len(st) > math.MaxInt/statisticsAggregateStateBytes || uint64(len(st)) > math.MaxUint32 || uint64(len(nt)) > math.MaxUint32 || len(st) > (math.MaxInt-statisticsAggregateRunHeaderLen)/2-len(nt) {
		return fmt.Errorf("statistics-aggregate descriptor size overflow")
	}
	return nil
}
func validateStatisticsAggregateState(s aggregate.StatisticsResult) error {
	if s.Layout.Moments {
		if s.Moments.Type != s.Type {
			return fmt.Errorf("statistics-aggregate moments type mismatch")
		}
		if _, err := aggregate.RestoreMoments(s.Moments); err != nil {
			return err
		}
	} else if s.Moments != (aggregate.MomentsResult{}) {
		return fmt.Errorf("statistics-aggregate undeclared moments")
	}
	if s.Layout.First || s.Layout.Last {
		if s.Endpoints.Type != s.Type {
			return fmt.Errorf("statistics-aggregate endpoint type mismatch")
		}
		if _, err := aggregate.RestoreEndpoints(s.Endpoints); err != nil {
			return err
		}
	} else if s.Endpoints != (aggregate.EndpointResult{}) {
		return fmt.Errorf("statistics-aggregate undeclared endpoints")
	}
	return nil
}
func validateStatisticsAggregateRaw(raw []byte, types []memory.DataType, layouts []aggregate.StatisticsLayout) error {
	if len(raw) != len(types)*statisticsAggregateStateBytes {
		return fmt.Errorf("statistics-aggregate raw length mismatch")
	}
	for i, t := range types {
		d := raw[i*statisticsAggregateStateBytes : (i+1)*statisticsAggregateStateBytes]
		if d[40] > 3 || d[41] > 1 || d[90] > 1 || d[91] > 1 {
			return fmt.Errorf("statistics-aggregate invalid state flags")
		}
		if !layouts[i].Moments {
			for _, v := range d[:41] {
				if v != 0 {
					return fmt.Errorf("statistics-aggregate undeclared moments bytes")
				}
			}
		}
		if !layouts[i].First && !layouts[i].Last {
			for _, v := range d[41:] {
				if v != 0 {
					return fmt.Errorf("statistics-aggregate undeclared endpoint bytes")
				}
			}
		}
		if err := validateStatisticsAggregateState(statisticsAggregateRecordState(aggregateRecord{statisticsRaw: raw}, i, t, layouts[i])); err != nil {
			return err
		}
	}
	return nil
}
func validateStatisticsAggregateRecord(record aggregateRecord, nt []memory.DataType, nl []aggregate.Layout, st []memory.DataType, sl []aggregate.StatisticsLayout) error {
	if len(nt) > 0 {
		if err := validateNumericAggregateRecord(record, nt, nl); err != nil {
			return err
		}
	}
	if record.statisticsBundle == nil {
		return validateStatisticsAggregateRaw(record.statisticsRaw, st, sl)
	}
	b := record.statisticsBundle
	if b.Columns() != len(st) || record.groupID < 0 || record.groupID >= b.Capacity() {
		return fmt.Errorf("statistics-aggregate bundle lane or group mismatch")
	}
	for i, t := range st {
		s := b.Result(record.groupID, i)
		if s.Type != t || s.Layout != sl[i] {
			return fmt.Errorf("statistics-aggregate descriptor mismatch")
		}
		if err := validateStatisticsAggregateState(s); err != nil {
			return err
		}
	}
	return nil
}
