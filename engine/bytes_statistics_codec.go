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
	bytesStatisticsAggregateRunVersion   = uint16(5)
	bytesStatisticsAggregateRunHeaderLen = 24
	bytesStatisticsAggregateRunCodec     = "aggregate-statistics-state-v5"
)

var bytesStatisticsAggregateRunMagic = [8]byte{'G', 'L', 'A', 'G', 'G', '0', '0', '5'}

// encodeBytesStatisticsAggregateRun visits immutable logical records twice, first to
// reserve the complete payload and then to encode it. Record lane slices and
// borrowed key/state views need remain valid only for their callback duration.
func encodeBytesStatisticsAggregateRun(ctx context.Context, shape GroupShape, numericTypes []memory.DataType, numericLayouts []aggregate.Layout, types []memory.DataType, layouts []aggregate.StatisticsLayout, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if ctx == nil || visit == nil || workspace == nil {
		return nil, fmt.Errorf("bytes-statistics-aggregate encode requires context, visitor and workspace")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBytesStatisticsAggregateDescriptors(numericTypes, numericLayouts, types, layouts); err != nil {
		return nil, err
	}
	if _, err := multiAggregateKeyBytes(shape, aggregateRecord{}); err != nil {
		return nil, err
	}
	lanes := len(layouts)
	numericBytes := int64(len(numericTypes)) * numericAggregateStateBytes
	size := int64(bytesStatisticsAggregateRunHeaderLen) + 2*int64(lanes+len(numericTypes))
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
		if err := validateBytesStatisticsAggregateRecord(record, numericTypes, numericLayouts, types, layouts); err != nil {
			return err
		}
		stateBytes, err := bytesStatisticsRecordSize(record, types, layouts)
		if err != nil {
			return err
		}
		if stateBytes > math.MaxInt64-numericBytes {
			return fmt.Errorf("bytes-statistics state size overflow")
		}
		stateBytes += numericBytes
		keyBytes, err := multiAggregateKeyBytes(shape, record)
		if err != nil {
			return err
		}
		if keyBytes > math.MaxInt64-stateBytes || size > math.MaxInt64-stateBytes-keyBytes || uint64(groups) == math.MaxUint32 {
			return fmt.Errorf("bytes-statistics-aggregate run size or group count overflow")
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
		return nil, fmt.Errorf("bytes-statistics-aggregate run exceeds addressable memory")
	}
	reservation, err := workspace.Reserve(ctx, size)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return nil, fmt.Errorf("reserve bytes-statistics-aggregate run buffer: %w", err)
	}
	if reservation == nil {
		return nil, fmt.Errorf("bytes-statistics-aggregate run buffer reservation is nil")
	}
	if reservation.Bytes() != size {
		reservation.Release()
		return nil, fmt.Errorf("bytes-statistics-aggregate run buffer reservation size mismatch")
	}
	committed := false
	defer func() {
		if !committed {
			reservation.Release()
		}
	}()
	payload := make([]byte, int(size))
	w := aggregateRunWriter{buf: payload}
	w.bytes(bytesStatisticsAggregateRunMagic[:])
	w.u16(bytesStatisticsAggregateRunVersion)
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
		if err := validateBytesStatisticsAggregateRecord(record, numericTypes, numericLayouts, types, layouts); err != nil {
			return err
		}
		if _, err := multiAggregateKeyBytes(shape, record); err != nil {
			return err
		}
		if written >= groups {
			return fmt.Errorf("bytes-statistics-aggregate visitor changed group count")
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
			if byteStatisticsType(types[lane]) {
				encodeBytesStatisticsState(&w, bytesAggregateRecordState(record, lane, types, layouts))
			} else {
				encodeStatisticsAggregateState(&w, statisticsAggregateRecordState(record, lane, types[lane], layouts[lane]))
			}
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
		return nil, fmt.Errorf("bytes-statistics-aggregate visitor changed encoded size or group count")
	}
	committed = true
	return &encodedAggregateRun{payload: payload, groups: groups, reservation: reservation}, nil
}

type bytesStatisticsAggregateRunReader struct {
	aggregateRunReader
	lanes        int
	numericLanes int
	types        []memory.DataType
	layouts      []aggregate.StatisticsLayout
}

// newBytesStatisticsAggregateRunReader borrows payload and descriptor slices. Their
// owner must keep them immutable and live until iteration finishes.
func newBytesStatisticsAggregateRunReader(payload []byte, shape GroupShape, numericTypes []memory.DataType, numericLayouts []aggregate.Layout, types []memory.DataType, layouts []aggregate.StatisticsLayout) (*bytesStatisticsAggregateRunReader, error) {
	if err := validateBytesStatisticsAggregateDescriptors(numericTypes, numericLayouts, types, layouts); err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	keyBytes, err := multiAggregateKeyBytes(shape, aggregateRecord{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	reader := &bytesStatisticsAggregateRunReader{aggregateRunReader: aggregateRunReader{buf: payload, shape: shape}, lanes: len(layouts), numericLanes: len(numericTypes), types: types, layouts: layouts}
	r := &reader.aggregateRunReader
	if string(r.bytes(8)) != string(bytesStatisticsAggregateRunMagic[:]) || r.u16() != bytesStatisticsAggregateRunVersion {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate magic or version mismatch", memory.ErrSpillCorrupt)
	}
	if GroupShape(r.u8()) != shape || r.u8() != 0 {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate shape or reserved header mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(numericTypes)) {
		return nil, fmt.Errorf("%w: numeric lane count mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(layouts)) {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate lane count mismatch", memory.ErrSpillCorrupt)
	}
	groups := r.u32()
	if uint64(groups) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate group count exceeds addressable memory", memory.ErrSpillCorrupt)
	}
	r.groups = int(groups)
	for lane, layout := range numericLayouts {
		if memory.DataType(r.u8()) != numericTypes[lane] || r.u8() != aggregateLayoutMask(layout) {
			return nil, fmt.Errorf("%w: numeric descriptor mismatch", memory.ErrSpillCorrupt)
		}
	}
	for lane, layout := range layouts {
		if memory.DataType(r.u8()) != types[lane] || r.u8() != statisticsLayoutMask(layout) {
			return nil, fmt.Errorf("%w: bytes-statistics-aggregate lane layout mismatch", memory.ErrSpillCorrupt)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	minimum := keyBytes + int64(len(numericTypes))*numericAggregateStateBytes
	for _, t := range types {
		if byteStatisticsType(t) {
			minimum += bytesStatisticsStateHeaderBytes
		} else {
			minimum += statisticsAggregateStateBytes
		}
	}
	if int64(r.groups) > int64(len(payload)-r.pos)/minimum {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate group count exceeds payload", memory.ErrSpillCorrupt)
	}
	if r.groups == 0 && r.pos != len(payload) {
		return nil, fmt.Errorf("%w: bytes-statistics-aggregate empty run has trailing bytes", memory.ErrSpillCorrupt)
	}
	return reader, nil
}

func (reader *bytesStatisticsAggregateRunReader) next() (aggregateRecord, bool, error) {
	r := &reader.aggregateRunReader
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if r.groups == 0 {
		if r.pos != len(r.buf) {
			r.err = fmt.Errorf("%w: bytes-statistics-aggregate run has trailing bytes", memory.ErrSpillCorrupt)
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
			r.err = fmt.Errorf("%w: bytes-statistics-aggregate string exceeds addressable memory", memory.ErrSpillCorrupt)
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
	start := r.pos
	for i, t := range reader.types {
		if byteStatisticsType(t) {
			frameStart := r.pos
			h := r.bytes(bytesStatisticsStateHeaderBytes)
			if r.err != nil {
				return aggregateRecord{}, false, r.err
			}
			first, last := uint64(binary.LittleEndian.Uint32(h[35:39])), uint64(binary.LittleEndian.Uint32(h[39:43]))
			if first+last > uint64(len(r.buf)-r.pos) {
				r.err = fmt.Errorf("%w: byte endpoint lengths exceed payload", memory.ErrSpillCorrupt)
				return aggregateRecord{}, false, r.err
			}
			r.bytes(int(first + last))
			if err := validateBytesStatisticsFrame(r.buf[frameStart:r.pos], t); err != nil {
				r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
				return aggregateRecord{}, false, r.err
			}
		} else {
			d := r.bytes(statisticsAggregateStateBytes)
			if r.err != nil {
				return aggregateRecord{}, false, r.err
			}
			if err := validateStatisticsAggregateRaw(d, reader.types[i:i+1], reader.layouts[i:i+1]); err != nil {
				r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
				return aggregateRecord{}, false, r.err
			}
		}
	}
	record.bytesStatisticsRaw = r.buf[start:r.pos]
	record.bytesStatisticsTypes = reader.types
	r.groups--
	return record, true, nil
}

func (reader *bytesStatisticsAggregateRunReader) visit(fn func(aggregateRecord) error) error {
	if fn == nil {
		return fmt.Errorf("bytes-statistics-aggregate run visitor is nil")
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

const bytesStatisticsStateHeaderBytes = 43

// bytesStatisticsFrame scans validated frames without allocating an offset table.
func bytesStatisticsFrame(raw []byte, lane int, types []memory.DataType) []byte {
	for i, t := range types {
		n := statisticsAggregateStateBytes
		if byteStatisticsType(t) {
			n = bytesStatisticsStateHeaderBytes + int(binary.LittleEndian.Uint32(raw[35:39])) + int(binary.LittleEndian.Uint32(raw[39:43]))
		}
		if i == lane {
			return raw[:n]
		}
		raw = raw[n:]
	}
	panic("statistics lane out of range")
}

func decodeBytesStatisticsFrame(d []byte, t memory.DataType) aggregate.BytesEndpointResult {
	u := func(i int) uint64 { return binary.LittleEndian.Uint64(d[i : i+8]) }
	first := int(binary.LittleEndian.Uint32(d[35:39]))
	return aggregate.BytesEndpointResult{Type: t, Seen: d[0] == 1, FirstValid: d[1] == 1, LastValid: d[2] == 1,
		FirstOrdinal: aggregate.SourceOrdinal{Partition: u(3), Row: u(11)}, LastOrdinal: aggregate.SourceOrdinal{Partition: u(19), Row: u(27)},
		First: d[43 : 43+first], Last: d[43+first:]}
}
func validateBytesStatisticsFrame(d []byte, t memory.DataType) error {
	if len(d) < bytesStatisticsStateHeaderBytes {
		return fmt.Errorf("short byte endpoint header")
	}
	if d[0] > 1 || d[1] > 1 || d[2] > 1 {
		return fmt.Errorf("invalid byte endpoint flags")
	}
	first, last := uint64(binary.LittleEndian.Uint32(d[35:39])), uint64(binary.LittleEndian.Uint32(d[39:43]))
	if first+last != uint64(len(d)-bytesStatisticsStateHeaderBytes) {
		return fmt.Errorf("byte endpoint lengths mismatch")
	}
	return aggregate.ValidateBytesEndpoints(decodeBytesStatisticsFrame(d, t))
}
func encodeBytesStatisticsState(w *aggregateRunWriter, s aggregate.BytesEndpointResult) {
	w.u8(statisticsBool(s.Seen))
	w.u8(statisticsBool(s.FirstValid))
	w.u8(statisticsBool(s.LastValid))
	w.u64(s.FirstOrdinal.Partition)
	w.u64(s.FirstOrdinal.Row)
	w.u64(s.LastOrdinal.Partition)
	w.u64(s.LastOrdinal.Row)
	w.u32(uint32(len(s.First)))
	w.u32(uint32(len(s.Last)))
	w.bytes(s.First)
	w.bytes(s.Last)
}
func bytesStatisticsRecordSize(record aggregateRecord, types []memory.DataType, layouts []aggregate.StatisticsLayout) (int64, error) {
	var size int64
	for i, t := range types {
		n := int64(statisticsAggregateStateBytes)
		if byteStatisticsType(t) {
			s := bytesAggregateRecordState(record, i, types, layouts)
			if uint64(len(s.First)) > math.MaxUint32 || uint64(len(s.Last)) > math.MaxUint32 {
				return 0, fmt.Errorf("byte endpoint exceeds wire length")
			}
			n = bytesStatisticsStateHeaderBytes + int64(len(s.First)) + int64(len(s.Last))
		}
		if n > math.MaxInt64-size {
			return 0, fmt.Errorf("byte statistics size overflow")
		}
		size += n
	}
	return size, nil
}
func validateBytesStatisticsAggregateRecord(record aggregateRecord, nt []memory.DataType, nl []aggregate.Layout, st []memory.DataType, sl []aggregate.StatisticsLayout) error {
	if len(nt) > 0 {
		if err := validateNumericAggregateRecord(record, nt, nl); err != nil {
			return err
		}
	}
	if record.statisticsBundle != nil {
		b := record.statisticsBundle
		if b.Columns() != len(st) || record.groupID < 0 || record.groupID >= b.Capacity() {
			return fmt.Errorf("byte statistics bundle lane/group mismatch")
		}
		for i, t := range st {
			s := b.Result(record.groupID, i)
			if s.Type != t || s.Layout != sl[i] {
				return fmt.Errorf("byte statistics descriptor mismatch")
			}
			if byteStatisticsType(t) {
				if err := aggregate.ValidateBytesEndpoints(b.BytesResult(record.groupID, i)); err != nil {
					return err
				}
			} else if err := validateStatisticsAggregateState(s); err != nil {
				return err
			}
		}
		return nil
	}
	raw := record.bytesStatisticsRaw
	// Native accessors depend on the borrowed descriptor slice as well as raw data.
	if len(record.bytesStatisticsTypes) != len(st) {
		return fmt.Errorf("byte statistics raw descriptor count mismatch")
	}
	for i, t := range st {
		if record.bytesStatisticsTypes[i] != t {
			return fmt.Errorf("byte statistics raw type mismatch")
		}
		n := uint64(statisticsAggregateStateBytes)
		if byteStatisticsType(t) {
			if len(raw) < bytesStatisticsStateHeaderBytes {
				return fmt.Errorf("short byte statistics frame")
			}
			n = bytesStatisticsStateHeaderBytes + uint64(binary.LittleEndian.Uint32(raw[35:39])) + uint64(binary.LittleEndian.Uint32(raw[39:43]))
		}
		if n > uint64(len(raw)) {
			return fmt.Errorf("short byte statistics payload")
		}
		d := raw[:int(n)]
		raw = raw[int(n):]
		if byteStatisticsType(t) {
			if err := validateBytesStatisticsFrame(d, t); err != nil {
				return err
			}
		} else if err := validateStatisticsAggregateRaw(d, st[i:i+1], sl[i:i+1]); err != nil {
			return err
		}
	}
	if len(raw) != 0 {
		return fmt.Errorf("trailing byte statistics payload")
	}
	return nil
}
