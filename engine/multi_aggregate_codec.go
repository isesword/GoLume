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
	multiAggregateRunVersion   = uint16(2)
	multiAggregateRunHeaderLen = 20
	multiAggregateRunCodec     = "aggregate-int64-bundle-v2"
)

var multiAggregateRunMagic = [8]byte{'G', 'L', 'A', 'G', 'G', '0', '0', '2'}

// aggregateRecordState borrows one lane from a live table or decodes one lane
// from a previously validated run. Its caller must keep the owner alive and
// supply an in-range lane. Raw AVG sums/counts remain the mergeable state.
func aggregateRecordState(record aggregateRecord, lane int) aggregate.Int64Result {
	if record.bundle != nil {
		return record.bundle.Result(record.groupID, lane)
	}
	if record.states != nil {
		return record.states[lane]
	}
	if record.rawStates != nil {
		data := record.rawStates[lane*aggregateStateBytes : (lane+1)*aggregateStateBytes]
		state := aggregate.Int64Result{
			Sum: int64(binary.LittleEndian.Uint64(data[0:8])), SumValid: data[8] == 1,
			CountRows: binary.LittleEndian.Uint64(data[9:17]), CountValues: binary.LittleEndian.Uint64(data[17:25]),
			Min: int64(binary.LittleEndian.Uint64(data[25:33])), MinValid: data[33] == 1,
			Max: int64(binary.LittleEndian.Uint64(data[34:42])), MaxValid: data[42] == 1,
		}
		if state.CountValues != 0 {
			state.Avg = float64(state.Sum) / float64(state.CountValues)
			state.AvgValid = true
		}
		return state
	}
	if lane == 0 {
		return record.state
	}
	return aggregate.Int64Result{}
}

func multiAggregateKeyBytes(shape GroupShape, record aggregateRecord) (int64, error) {
	switch shape {
	case GroupShapeGlobal:
		if record.validMask != 0 {
			return 0, fmt.Errorf("global aggregate key validity mask must be zero")
		}
		return 1, nil
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapeDictionaryString:
		if record.validMask > 1 {
			return 0, fmt.Errorf("aggregate single-key validity mask=%d", record.validMask)
		}
		return 9, nil
	case GroupShapePairFixed:
		if record.validMask > 3 {
			return 0, fmt.Errorf("aggregate pair validity mask=%d", record.validMask)
		}
		return 17, nil
	case GroupShapeStringView, GroupShapeGeneric:
		if record.validMask > 1 {
			return 0, fmt.Errorf("aggregate string validity mask=%d", record.validMask)
		}
		if uint64(len(record.stringKey)) > math.MaxUint32 {
			return 0, fmt.Errorf("aggregate string key exceeds uint32")
		}
		return 5 + int64(len(record.stringKey)), nil
	default:
		return 0, fmt.Errorf("unsupported multi-aggregate run shape %d", shape)
	}
}

func validateMultiAggregateLayouts(layouts []aggregate.Layout) error {
	if len(layouts) == 0 || uint64(len(layouts)) > math.MaxUint32 || len(layouts) > math.MaxInt-multiAggregateRunHeaderLen {
		return fmt.Errorf("multi-aggregate run requires an addressable nonempty lane layout")
	}
	if len(layouts) > math.MaxInt/aggregateStateBytes {
		return fmt.Errorf("multi-aggregate state size overflow")
	}
	for _, layout := range layouts {
		if layout.IsZero() {
			return fmt.Errorf("multi-aggregate lane layout must not be empty")
		}
	}
	return nil
}

func validateMultiAggregateRecord(record aggregateRecord, lanes int) error {
	switch {
	case record.bundle != nil:
		if record.bundle.Columns() != lanes || record.groupID < 0 || record.groupID >= record.bundle.Capacity() {
			return fmt.Errorf("multi-aggregate record bundle lane count or group ID mismatch")
		}
	case record.states != nil:
		if len(record.states) != lanes {
			return fmt.Errorf("multi-aggregate record lane count mismatch")
		}
	case record.rawStates != nil:
		if len(record.rawStates) != lanes*aggregateStateBytes {
			return fmt.Errorf("multi-aggregate raw state length mismatch")
		}
		for lane := 0; lane < lanes; lane++ {
			data := record.rawStates[lane*aggregateStateBytes:]
			if data[8] > 1 || data[33] > 1 || data[42] > 1 {
				return fmt.Errorf("multi-aggregate state validity byte is not boolean")
			}
		}
	default:
		if lanes != 1 {
			return fmt.Errorf("multi-aggregate record is missing lane states")
		}
	}
	return nil
}

// encodeMultiAggregateRun visits immutable logical records twice, first to
// reserve the complete payload and then to encode it. Record lane slices and
// borrowed key/state views need remain valid only for their callback duration.
func encodeMultiAggregateRun(ctx context.Context, shape GroupShape, layouts []aggregate.Layout, visit aggregateRecordVisitor, filter aggregateRecordFilter, workspace memory.Manager) (*encodedAggregateRun, error) {
	if ctx == nil || visit == nil || workspace == nil {
		return nil, fmt.Errorf("multi-aggregate encode requires context, visitor and workspace")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateMultiAggregateLayouts(layouts); err != nil {
		return nil, err
	}
	if _, err := multiAggregateKeyBytes(shape, aggregateRecord{}); err != nil {
		return nil, err
	}
	lanes := len(layouts)
	stateBytes := int64(lanes) * aggregateStateBytes
	size := int64(multiAggregateRunHeaderLen) + int64(lanes)
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
		if err := validateMultiAggregateRecord(record, lanes); err != nil {
			return err
		}
		keyBytes, err := multiAggregateKeyBytes(shape, record)
		if err != nil {
			return err
		}
		if keyBytes > math.MaxInt64-stateBytes || size > math.MaxInt64-stateBytes-keyBytes || uint64(groups) == math.MaxUint32 {
			return fmt.Errorf("multi-aggregate run size or group count overflow")
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
		return nil, fmt.Errorf("multi-aggregate run exceeds addressable memory")
	}
	reservation, err := workspace.Reserve(ctx, size)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return nil, fmt.Errorf("reserve multi-aggregate run buffer: %w", err)
	}
	if reservation == nil {
		return nil, fmt.Errorf("multi-aggregate run buffer reservation is nil")
	}
	if reservation.Bytes() != size {
		reservation.Release()
		return nil, fmt.Errorf("multi-aggregate run buffer reservation size mismatch")
	}
	committed := false
	defer func() {
		if !committed {
			reservation.Release()
		}
	}()
	payload := make([]byte, int(size))
	w := aggregateRunWriter{buf: payload}
	w.bytes(multiAggregateRunMagic[:])
	w.u16(multiAggregateRunVersion)
	w.u8(byte(shape))
	w.u8(0)
	w.u32(uint32(lanes))
	w.u32(uint32(groups))
	for _, layout := range layouts {
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
		if err := validateMultiAggregateRecord(record, lanes); err != nil {
			return err
		}
		if _, err := multiAggregateKeyBytes(shape, record); err != nil {
			return err
		}
		if written >= groups {
			return fmt.Errorf("multi-aggregate visitor changed group count")
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
			encodeAggregateState(&w, aggregateRecordState(record, lane))
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
		return nil, fmt.Errorf("multi-aggregate visitor changed encoded size or group count")
	}
	committed = true
	return &encodedAggregateRun{payload: payload, groups: groups, reservation: reservation}, nil
}

type multiAggregateRunReader struct {
	aggregateRunReader
	lanes int
}

func newMultiAggregateRunReader(payload []byte, shape GroupShape, layouts []aggregate.Layout) (*multiAggregateRunReader, error) {
	if err := validateMultiAggregateLayouts(layouts); err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	keyBytes, err := multiAggregateKeyBytes(shape, aggregateRecord{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
	}
	reader := &multiAggregateRunReader{aggregateRunReader: aggregateRunReader{buf: payload, shape: shape}, lanes: len(layouts)}
	r := &reader.aggregateRunReader
	if string(r.bytes(8)) != string(multiAggregateRunMagic[:]) || r.u16() != multiAggregateRunVersion {
		return nil, fmt.Errorf("%w: multi-aggregate magic or version mismatch", memory.ErrSpillCorrupt)
	}
	if GroupShape(r.u8()) != shape || r.u8() != 0 {
		return nil, fmt.Errorf("%w: multi-aggregate shape or reserved header mismatch", memory.ErrSpillCorrupt)
	}
	if uint64(r.u32()) != uint64(len(layouts)) {
		return nil, fmt.Errorf("%w: multi-aggregate lane count mismatch", memory.ErrSpillCorrupt)
	}
	groups := r.u32()
	if uint64(groups) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: multi-aggregate group count exceeds addressable memory", memory.ErrSpillCorrupt)
	}
	r.groups = int(groups)
	for _, layout := range layouts {
		if r.u8() != aggregateLayoutMask(layout) {
			return nil, fmt.Errorf("%w: multi-aggregate lane layout mismatch", memory.ErrSpillCorrupt)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	minimum := keyBytes + int64(len(layouts))*aggregateStateBytes
	if int64(r.groups) > int64(len(payload)-r.pos)/minimum {
		return nil, fmt.Errorf("%w: multi-aggregate group count exceeds payload", memory.ErrSpillCorrupt)
	}
	if r.groups == 0 && r.pos != len(payload) {
		return nil, fmt.Errorf("%w: multi-aggregate empty run has trailing bytes", memory.ErrSpillCorrupt)
	}
	return reader, nil
}

func (reader *multiAggregateRunReader) next() (aggregateRecord, bool, error) {
	r := &reader.aggregateRunReader
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if r.groups == 0 {
		if r.pos != len(r.buf) {
			r.err = fmt.Errorf("%w: multi-aggregate run has trailing bytes", memory.ErrSpillCorrupt)
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
			r.err = fmt.Errorf("%w: multi-aggregate string exceeds addressable memory", memory.ErrSpillCorrupt)
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
	record.rawStates = r.bytes(reader.lanes * aggregateStateBytes)
	if r.err != nil {
		return aggregateRecord{}, false, r.err
	}
	if err := validateMultiAggregateRecord(record, reader.lanes); err != nil {
		r.err = fmt.Errorf("%w: %v", memory.ErrSpillCorrupt, err)
		return aggregateRecord{}, false, r.err
	}
	record.state = aggregateRecordState(record, 0)
	r.groups--
	return record, true, nil
}

func (reader *multiAggregateRunReader) visit(fn func(aggregateRecord) error) error {
	if fn == nil {
		return fmt.Errorf("multi-aggregate run visitor is nil")
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
