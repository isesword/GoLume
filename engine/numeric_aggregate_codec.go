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
	numericAggregateRunVersion   = uint16(3)
	numericAggregateRunHeaderLen = 20
	numericAggregateRunCodec     = "aggregate-native-state-v3"
	numericAggregateStateBytes   = aggregateStateBytes + 8
)

var numericAggregateRunMagic = [8]byte{'G', 'L', 'A', 'G', 'G', '0', '0', '3'}

// numericAggregateRecordState borrows a native lane, decodes validated raw
// bytes, or losslessly adapts legacy Int64 state. The caller supplies the
// already validated descriptor and keeps the backing owner alive.
func numericAggregateRecordState(record aggregateRecord, lane int, typ memory.DataType, layout aggregate.Layout) aggregate.NumericResult {
	if record.numericBundle != nil {
		return record.numericBundle.Result(record.groupID, lane)
	}
	if record.numericRaw != nil {
		data := record.numericRaw[lane*numericAggregateStateBytes : (lane+1)*numericAggregateStateBytes]
		return aggregate.NumericResult{
			Type: typ, Layout: layout,
			SumBits: binary.LittleEndian.Uint64(data[0:8]), SumValid: data[8] == 1,
			CountRows: binary.LittleEndian.Uint64(data[9:17]), CountValues: binary.LittleEndian.Uint64(data[17:25]),
			MinBits: binary.LittleEndian.Uint64(data[25:33]), MinValid: data[33] == 1,
			MaxBits: binary.LittleEndian.Uint64(data[34:42]), MaxValid: data[42] == 1,
			AvgSum: math.Float64frombits(binary.LittleEndian.Uint64(data[43:51])),
		}
	}
	old := aggregateRecordState(record, lane)
	return aggregate.NumericResult{Type: typ, Layout: layout, SumBits: uint64(old.Sum), SumValid: old.SumValid, MinBits: uint64(old.Min), MinValid: old.MinValid, MaxBits: uint64(old.Max), MaxValid: old.MaxValid, CountRows: old.CountRows, CountValues: old.CountValues}
}

func encodeNumericAggregateState(w *aggregateRunWriter, state aggregate.NumericResult) {
	// Reuse the established bits/counts/validity order; no floating conversion
	// touches Sum/Min/Max, preserving Uint64 precision and IEEE payloads.
	encodeAggregateState(w, aggregate.Int64Result{Sum: int64(state.SumBits), SumValid: state.SumValid, Min: int64(state.MinBits), MinValid: state.MinValid, Max: int64(state.MaxBits), MaxValid: state.MaxValid, CountRows: state.CountRows, CountValues: state.CountValues})
	w.u64(math.Float64bits(state.AvgSum))
}

func validateNumericAggregateDescriptors(types []memory.DataType, layouts []aggregate.Layout) error {
	if err := validateMultiAggregateLayouts(layouts); err != nil {
		return err
	}
	if len(types) != len(layouts) || len(types) > (math.MaxInt-numericAggregateRunHeaderLen)/2 || len(types) > math.MaxInt/numericAggregateStateBytes {
		return fmt.Errorf("numeric-aggregate descriptor count or size mismatch")
	}
	for lane, typ := range types {
		if _, err := aggregate.NumericVectorBytesForSlots(0, typ, layouts[lane]); err != nil {
			return err
		}
	}
	return nil
}

func validateNumericAggregateRaw(raw []byte, lanes int) error {
	if len(raw) != lanes*numericAggregateStateBytes {
		return fmt.Errorf("numeric-aggregate raw state length mismatch")
	}
	for lane := 0; lane < lanes; lane++ {
		data := raw[lane*numericAggregateStateBytes:]
		if data[8] > 1 || data[33] > 1 || data[42] > 1 {
			return fmt.Errorf("numeric-aggregate state validity byte is not boolean")
		}
	}
	return nil
}

func validateNumericAggregateRecord(record aggregateRecord, types []memory.DataType, layouts []aggregate.Layout) error {
	lanes := len(types)
	if record.numericBundle != nil {
		if record.numericBundle.Columns() != lanes || record.groupID < 0 || record.groupID >= record.numericBundle.Capacity() {
			return fmt.Errorf("numeric-aggregate bundle lane count or group ID mismatch")
		}
		for lane, typ := range types {
			state := record.numericBundle.Result(record.groupID, lane)
			if state.Type != typ || state.Layout != layouts[lane] {
				return fmt.Errorf("numeric-aggregate bundle type or layout mismatch")
			}
		}
		return nil
	}
	if record.numericRaw != nil {
		return validateNumericAggregateRaw(record.numericRaw, lanes)
	}
	// Legacy states may only be interpreted as Int64, never as float bits or
	// unsigned/narrow native values merely because their byte widths happen to fit.
	for _, typ := range types {
		if typ != memory.TypeInt64 {
			return fmt.Errorf("numeric-aggregate native record is missing native lane states")
		}
	}
	return validateMultiAggregateRecord(record, lanes)
}

// encodeNumericAggregateRun visits immutable logical records twice, first to
// reserve the complete payload and then to encode it. Record lane slices and
// borrowed key/state views need remain valid only for their callback duration.
func encodeNumericAggregateRun(ctx context.Context, shape GroupShape, types []memory.DataType, layouts []aggregate.Layout, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if ctx == nil || visit == nil || workspace == nil {
		return nil, fmt.Errorf("numeric-aggregate encode requires context, visitor and workspace")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateNumericAggregateDescriptors(types, layouts); err != nil {
		return nil, err
	}
	if _, err := multiAggregateKeyBytes(shape, aggregateRecord{}); err != nil {
		return nil, err
	}
	lanes := len(layouts)
	stateBytes := int64(lanes) * numericAggregateStateBytes
	size := int64(numericAggregateRunHeaderLen) + 2*int64(lanes)
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
		if err := validateNumericAggregateRecord(record, types, layouts); err != nil {
			return err
		}
		keyBytes, err := multiAggregateKeyBytes(shape, record)
		if err != nil {
			return err
		}
		if keyBytes > math.MaxInt64-stateBytes || size > math.MaxInt64-stateBytes-keyBytes || uint64(groups) == math.MaxUint32 {
			return fmt.Errorf("numeric-aggregate run size or group count overflow")
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
		return nil, fmt.Errorf("numeric-aggregate run exceeds addressable memory")
	}
	reservation, err := workspace.Reserve(ctx, size)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return nil, fmt.Errorf("reserve numeric-aggregate run buffer: %w", err)
	}
	if reservation == nil {
		return nil, fmt.Errorf("numeric-aggregate run buffer reservation is nil")
	}
	if reservation.Bytes() != size {
		reservation.Release()
		return nil, fmt.Errorf("numeric-aggregate run buffer reservation size mismatch")
	}
	committed := false
	defer func() {
		if !committed {
			reservation.Release()
		}
	}()
	payload := make([]byte, int(size))
	w := aggregateRunWriter{buf: payload}
	w.bytes(numericAggregateRunMagic[:])
	w.u16(numericAggregateRunVersion)
	w.u8(byte(shape))
	w.u8(0)
	w.u32(uint32(lanes))
	w.u32(uint32(groups))
	for lane, layout := range layouts {
		w.u8(byte(types[lane]))
		w.u8(aggregateLayoutMask(layout))
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
		if err := validateNumericAggregateRecord(record, types, layouts); err != nil {
			return err
		}
		if _, err := multiAggregateKeyBytes(shape, record); err != nil {
			return err
		}
		if written >= groups {
			return fmt.Errorf("numeric-aggregate visitor changed group count")
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
		for lane := 0; lane < lanes; lane++ {
			encodeNumericAggregateState(&w, numericAggregateRecordState(record, lane, types[lane], layouts[lane]))
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
		return nil, fmt.Errorf("numeric-aggregate visitor changed encoded size or group count")
	}
	committed = true
	return &encodedAggregateRun{payload: payload, groups: groups, reservation: reservation}, nil
}

type numericAggregateRunReader struct {
	aggregateRunReader
	lanes int
}

func newNumericAggregateRunReader(payload []byte, shape GroupShape, types []memory.DataType, layouts []aggregate.Layout) (*numericAggregateRunReader, error) {
	if err := validateNumericAggregateDescriptors(types, layouts); err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	keyBytes, err := multiAggregateKeyBytes(shape, aggregateRecord{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	reader := &numericAggregateRunReader{aggregateRunReader: aggregateRunReader{buf: payload, shape: shape}, lanes: len(layouts)}
	r := &reader.aggregateRunReader
	if string(r.bytes(8)) != string(numericAggregateRunMagic[:]) || r.u16() != numericAggregateRunVersion {
		return nil, fmt.Errorf("%w: numeric-aggregate magic or version mismatch", memory.ErrSpillCorrupt)
	}
	if GroupShape(r.u8()) != shape || r.u8() != 0 {
		return nil, fmt.Errorf("%w: numeric-aggregate shape or reserved header mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(layouts)) {
		return nil, fmt.Errorf("%w: numeric-aggregate lane count mismatch", memory.ErrSpillCorrupt)
	}
	groups := r.u32()
	if uint64(groups) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: numeric-aggregate group count exceeds addressable memory", memory.ErrSpillCorrupt)
	}
	r.groups = int(groups)
	for lane, layout := range layouts {
		if memory.DataType(r.u8()) != types[lane] || r.u8() != aggregateLayoutMask(layout) {
			return nil, fmt.Errorf("%w: numeric-aggregate lane layout mismatch", memory.ErrSpillCorrupt)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	minimum := keyBytes + int64(len(layouts))*numericAggregateStateBytes
	if int64(r.groups) > int64(len(payload)-r.pos)/minimum {
		return nil, fmt.Errorf("%w: numeric-aggregate group count exceeds payload", memory.ErrSpillCorrupt)
	}
	if r.groups == 0 && r.pos != len(payload) {
		return nil, fmt.Errorf("%w: numeric-aggregate empty run has trailing bytes", memory.ErrSpillCorrupt)
	}
	return reader, nil
}

func (reader *numericAggregateRunReader) next() (aggregateRecord, bool, error) {
	r := &reader.aggregateRunReader
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if r.groups == 0 {
		if r.pos != len(r.buf) {
			r.err = fmt.Errorf("%w: numeric-aggregate run has trailing bytes", memory.ErrSpillCorrupt)
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
			r.err = fmt.Errorf("%w: numeric-aggregate string exceeds addressable memory", memory.ErrSpillCorrupt)
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
	record.numericRaw = r.bytes(reader.lanes * numericAggregateStateBytes)
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if err := validateNumericAggregateRaw(record.numericRaw, reader.lanes); err != nil {
		r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
		return aggregateRecord{}, false, r.err
	}
	r.groups--
	return record, true, nil
}

func (reader *numericAggregateRunReader) visit(fn func(aggregateRecord) error) error {
	if fn == nil {
		return fmt.Errorf("numeric-aggregate run visitor is nil")
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
