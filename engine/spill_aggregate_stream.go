package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"gocolumn/source"
	"math"
	"time"
	"unsafe"
)

func isAggregatePressure(err error) bool {
	return errors.Is(err, memory.ErrBudgetExceeded) || errors.Is(err, errAggregateStateTarget)
}

func (s *spillableAggregateStream) restoreRun(ctx context.Context, ref aggregateRunRef, visit func(aggregateRecord) error) error {
	return s.restoreRunWithManager(ctx, ref, s.restorer, visit)
}

// restoreRunWithManager lets mutation paths use a non-reentrant growth manager
// while preserving the same validation and restored-payload lifetime.
func (s *spillableAggregateStream) restoreRunWithManager(ctx context.Context, ref aggregateRunRef, manager memory.Manager, visit func(aggregateRecord) error) (resultErr error) {
	restored, err := s.store.Restore(ctx, ref.handle, s.fingerprint, manager)
	if err != nil {
		return err
	}
	var reader *aggregateRunReader
	defer func() {
		// Readers borrow the restored payload, including on validation errors.
		// End that reference before its owner returns the payload reservation.
		reader = nil
		restored.Object = memory.SpillObject{}
		resultErr = errors.Join(resultErr, restored.Close())
		restored = nil
	}()
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != s.query.aggregateRunCodec() {
		return fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	reader, err = s.query.newAggregateRunReader(restored.Object.Payload)
	if err != nil {
		return err
	}
	if reader.groups != ref.groups {
		return fmt.Errorf("%w: aggregate spill group count=%d want=%d", memory.ErrSpillCorrupt, reader.groups, ref.groups)
	}
	return reader.visit(visit)
}

func (s *spillableAggregateStream) deleteRunRefsStrict(ctx context.Context, refs []aggregateRunRef) error {
	var out error
	for _, ref := range refs {
		if err := s.store.Delete(ctx, ref.handle); err != nil && out == nil {
			out = err
		}
	}
	return out
}

type aggregateRunPartitions struct {
	runs  [1 << aggregateRadixBits]spillmeta.Slice[aggregateRunRef]
	lease memory.Reservation
}

func (p *aggregateRunPartitions) release() {
	for i := range p.runs {
		p.runs[i].Release()
	}
	if p.lease != nil {
		p.lease.Release()
		p.lease = nil
	}
}

// finishRunRefs drops the unique array owner on every exit. Failed deletes
// remain owned and admitted by the private store until stream/query cleanup.
// Once a merge/repartition consumes its caller, errors cannot be retried by Next.
func (s *spillableAggregateStream) finishRunRefs(refs *spillmeta.Slice[aggregateRunRef], err error) {
	refs.Release()
	if err != nil {
		s.mu.Lock()
		// An outer recursive frame may add context to the same failure.
		// Keep the error returned by the outermost consuming operation.
		s.terminalErr = err
		s.mu.Unlock()
	}
}

// repartitionRuns consumes refs, including on failure. Each caller must pass
// its actual owner, never a copy of Slice or a borrowed Values view.
func (s *spillableAggregateStream) repartitionRuns(ctx context.Context, refs *spillmeta.Slice[aggregateRunRef], depth int) (_ *aggregateRunPartitions, resultErr error) {
	sources := *refs
	*refs = spillmeta.Slice[aggregateRunRef]{}
	defer func() { s.finishRunRefs(&sources, resultErr) }()
	if depth >= maxAggregateDepth {
		return nil, fmt.Errorf("aggregate radix repartition exhausted %d hash bits; a colliding partition still exceeds the memory budget", maxAggregateDepth*int(aggregateRadixBits))
	}
	lease, err := s.metadata.Reserve(ctx, 128+2*int64(unsafe.Sizeof(aggregateRunPartitions{})))
	if err != nil {
		return nil, err
	}
	children := &aggregateRunPartitions{lease: lease}
	success := false
	var created spillmeta.Slice[aggregateRunRef]
	defer func() {
		if !success {
			s.deleteRunRefs(context.Background(), created.Values)
			children.release()
		}
		created.Release()
	}()
	for i := range sources.Values {
		// No restored payload, parent handle or encoding closure escapes this
		// helper, so releasing sources below cannot leave a borrowed alias.
		if err := s.repartitionRun(ctx, sources.Values[i], depth, children, &created); err != nil {
			return nil, err
		}
	}
	if err := s.deleteRunRefsStrict(ctx, sources.Values); err != nil {
		return nil, err
	}
	// Commit all children and remove all parent files before dropping the
	// parent array. The caller can recurse without retaining this generation.
	sources.Release()
	created.Release()
	s.mu.Lock()
	if depth+1 > s.maxDepth {
		s.maxDepth = depth + 1
	}
	for i := range children.runs {
		groups := 0
		for _, ref := range children.runs[i].Values {
			groups += ref.groups
		}
		if groups > s.maxPartitionGroups {
			s.maxPartitionGroups = groups
		}
	}
	s.mu.Unlock()
	success = true
	return children, nil
}

func (s *spillableAggregateStream) repartitionRun(ctx context.Context, parent aggregateRunRef, depth int, children *aggregateRunPartitions, created *spillmeta.Slice[aggregateRunRef]) error {
	restored, err := s.store.Restore(ctx, parent.handle, s.fingerprint, s.restorer)
	if err != nil {
		return err
	}
	defer restored.Close()
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != s.query.aggregateRunCodec() {
		return fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	bitOffset := uint(depth) * aggregateRadixBits
	for child := range children.runs {
		visit := func(fn func(aggregateRecord) error) error {
			reader, readerErr := s.query.newAggregateRunReader(restored.Object.Payload)
			if readerErr != nil {
				return readerErr
			}
			if reader.groups != parent.groups {
				return fmt.Errorf("%w: aggregate spill group count=%d want=%d", memory.ErrSpillCorrupt, reader.groups, parent.groups)
			}
			return reader.visit(fn)
		}
		encoded, err := s.query.encodeAggregateRun(ctx, visit, func(_ int, record aggregateRecord) bool {
			return int(aggregateRecordHash(s.query.cq.GroupShape, record)>>bitOffset&((1<<aggregateRadixBits)-1)) == child
		}, s.encoder)
		if err != nil {
			return err
		}
		if encoded.groups == 0 {
			encoded.close()
			continue
		}
		handle, err := s.store.Spill(ctx, memory.SpillObject{
			Kind: memory.SpillAggregateState, Partition: int32(child), Codec: s.query.aggregateRunCodec(),
			SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
		})
		groups := encoded.groups
		encoded.close()
		if err != nil {
			return err
		}
		ref := aggregateRunRef{handle: handle, groups: groups, partition: child}
		if err := created.Append(ctx, s.metadata, ref); err != nil {
			_ = s.store.Delete(context.Background(), handle)
			return err
		}
		if err := children.runs[child].Append(ctx, s.metadata, ref); err != nil {
			return err
		}
		s.mu.Lock()
		s.spillRuns++
		s.spillBytes += handle.Bytes()
		s.mu.Unlock()
	}
	return restored.Close()
}

// mergePartition consumes the unique source owner. Array leases and all
// borrowed views must be gone before output admission or recursive merging.
func (s *spillableAggregateStream) mergePartition(ctx context.Context, refs *spillmeta.Slice[aggregateRunRef], depth int) (resultErr error) {
	sources := *refs
	*refs = spillmeta.Slice[aggregateRunRef]{}
	defer func() { s.finishRunRefs(&sources, resultErr) }()
	if len(sources.Values) == 0 {
		return nil
	}
	owner, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		return fmt.Errorf("create aggregate merge table: %w", err)
	}
	mergeErr := error(nil)
	for i := range sources.Values {
		mergeErr = s.restoreRun(ctx, sources.Values[i], func(record aggregateRecord) error {
			_, err := owner.table.merge(record)
			return err
		})
		if mergeErr != nil {
			break
		}
	}
	if mergeErr != nil {
		owner.close()
		if !isAggregatePressure(mergeErr) {
			return mergeErr
		}
		children, repartitionErr := s.repartitionRuns(ctx, &sources, depth)
		if repartitionErr != nil {
			return errors.Join(mergeErr, repartitionErr)
		}
		defer children.release()
		for i := range children.runs {
			if err := s.mergePartition(ctx, &children.runs[i], depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	groups := owner.table.len()
	// The merged table is authoritative now. Delete the files while their
	// store/path charges are live, then drop the source array before encoding
	// any output. Neither the caller nor a recursive frame retains an alias.
	if err := s.deleteRunRefsStrict(ctx, sources.Values); err != nil {
		owner.close()
		return err
	}
	sources.Release()
	s.mu.Lock()
	err = s.writeOutputRunsLocked(ctx, owner.table, 0, 0)
	s.mu.Unlock()
	if err != nil {
		owner.close()
		return err
	}
	owner.close()
	s.mu.Lock()
	s.groupCount += groups
	if depth > s.maxDepth {
		s.maxDepth = depth
	}
	s.mu.Unlock()
	return nil
}

// writeOutputRunsLocked persists only the unconsumed suffix. Every encoder
// pass resets to the same slot cursor, never rescanning earlier output chunks.
// Publication is atomic: on failure the original table and cursor stay valid.
func (s *spillableAggregateStream) writeOutputRunsLocked(ctx context.Context, table *aggregateStateTable, cursor, emitted int) error {
	groups := table.len()
	var created spillmeta.Slice[aggregateRunRef]
	defer created.Release()
	for start := emitted; start < groups; start += s.outputRows {
		if err := ctx.Err(); err != nil {
			s.deleteRunRefs(context.Background(), created.Values)
			return err
		}
		chunkStart := cursor
		visit := func(fn func(aggregateRecord) error) error {
			next := chunkStart
			_, err := table.visitChunk(&next, s.outputRows, fn)
			cursor = next
			return err
		}
		encoded, err := s.query.encodeAggregateRun(ctx, visit, nil, spillmeta.GrowthManager(s.encoder))
		if err != nil {
			s.deleteRunRefs(context.Background(), created.Values)
			return err
		}
		if encoded.groups == 0 {
			encoded.close()
			continue
		}
		partition := len(s.outputRuns) + len(created.Values)
		if partition > math.MaxInt32 {
			encoded.close()
			s.deleteRunRefs(context.Background(), created.Values)
			return fmt.Errorf("aggregate output run count exceeds int32")
		}
		handle, err := s.store.Spill(ctx, memory.SpillObject{
			Kind: memory.SpillAggregateState, Partition: int32(partition), Codec: s.query.aggregateRunCodec(),
			SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
		})
		count := encoded.groups
		encoded.close()
		if err != nil {
			s.deleteRunRefs(context.Background(), created.Values)
			return err
		}
		if err := created.Append(ctx, s.metadata, aggregateRunRef{handle: handle, groups: count, partition: partition}); err != nil {
			_ = s.store.Delete(context.Background(), handle)
			s.deleteRunRefs(context.Background(), created.Values)
			return err
		}
		s.spillRuns++
		s.spillBytes += handle.Bytes()
	}
	// Drop the borrowed view before replacement releases the old array.
	s.outputRuns = nil
	if err := s.outputRefs.Append(ctx, s.metadata, created.Values...); err != nil {
		s.outputRuns = s.outputRefs.Values
		s.deleteRunRefs(context.Background(), created.Values)
		return err
	}
	s.outputRuns = s.outputRefs.Values
	return nil
}

func (s *spillableAggregateStream) finishPartitions(ctx context.Context) error {
	for partition := range s.partitions {
		if err := s.mergePartition(ctx, &s.partitions[partition], 1); err != nil {
			err = fmt.Errorf("merge aggregate partition %d: %w", partition, err)
			s.mu.Lock()
			s.terminalErr = err
			s.mu.Unlock()
			return err
		}
	}
	return nil
}

func checkedOutputBytes(total *int64, count int, width int64) error {
	if count < 0 || width < 0 || int64(count) > math.MaxInt64/width {
		return fmt.Errorf("aggregate output buffer size overflow")
	}
	bytes := int64(count) * width
	if *total > math.MaxInt64-bytes {
		return fmt.Errorf("aggregate output buffer size overflow")
	}
	*total += bytes
	return nil
}

func aggregateOutputBytes(schema memory.Schema, rows int, plainStringBytes int64) (int64, error) {
	total := int64(0)
	// Output owns a vector descriptor and a map entry per column in addition
	// to backing buffers. Charge the largest supported descriptor and a
	// conservative map slot/growth-overlap allowance; arbitrarily wide key
	// schemas must not bypass admission merely by emitting one narrow row.
	const mapEntryAndGrowthBytes = int64(128)
	metadataPerColumn := int64(unsafe.Sizeof(memory.DictionaryStringVector{})) + mapEntryAndGrowthBytes
	if err := checkedOutputBytes(&total, len(schema.Fields), metadataPerColumn); err != nil {
		return 0, err
	}

	for _, field := range schema.Fields {
		width := int64(0)
		switch field.Type {
		case memory.TypeInt8, memory.TypeUint8:
			width = 1
		case memory.TypeInt16, memory.TypeUint16:
			width = 2
		case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeBool, memory.TypeDate32, memory.TypeTime32:
			width = 4
		case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			width = 8
		case memory.TypeString, memory.TypeBinary:
			if field.Encoding == memory.EncodingDictionary {
				width = 4
			} else {
				if err := checkedOutputBytes(&total, rows+1, 4); err != nil {
					return 0, err
				}
				if plainStringBytes < 0 || total > math.MaxInt64-plainStringBytes {
					return 0, fmt.Errorf("aggregate output string bytes overflow")
				}
				total += plainStringBytes
			}
		default:
			return 0, fmt.Errorf("aggregate output type %s for %q is unsupported", field.Type, field.Name)
		}
		if width != 0 {
			if err := checkedOutputBytes(&total, rows, width); err != nil {
				return 0, err
			}
		}
		if field.Nullable {
			if err := checkedOutputBytes(&total, memory.WordsFor(rows), 8); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func (s *spillableAggregateStream) allocateOutputBatch(dst *memory.Batch, rows int, plainStringBytes int) error {
	dst.ClearColumns()
	for _, group := range s.query.cq.GroupBy {
		field, ok := s.schema.Field(group.Name)
		if !ok {
			return fmt.Errorf("aggregate output schema is missing group %q", group.Name)
		}
		if s.query.cq.GroupShape == GroupShapeGeneric {
			if err := allocateGenericGroupColumn(dst, field, rows, plainStringBytes); err != nil {
				return err
			}
			continue
		}
		var validity memory.Bitmap
		if field.Nullable {
			validity = memory.EnsureBitmap(nil, rows)
		}
		switch field.Type {
		case memory.TypeInt32:
			dst.Int32s[field.Name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: validity, Encoding: field.Encoding}
		case memory.TypeInt64:
			dst.Int64s[field.Name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: validity, Encoding: field.Encoding}
		case memory.TypeUint32:
			dst.Uint32s[field.Name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: validity, Encoding: field.Encoding}
		case memory.TypeString, memory.TypeBinary:
			if field.Encoding == memory.EncodingDictionary {
				dict := s.query.dicts[field.Name]
				dst.DictStrings[field.Name] = &memory.DictionaryStringVector{Indices: make([]uint32, rows), Offsets: dict.Offsets, Data: dict.Data, Validity: validity, DictionaryDomain: field.DictionaryDomain, Encoding: memory.EncodingDictionary}
			} else {
				dst.Strings[field.Name] = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, 0, plainStringBytes), Validity: validity, Encoding: memory.EncodingView}
			}
		default:
			return fmt.Errorf("aggregate group output type %s for %q is unsupported", field.Type, field.Name)
		}
	}
	for index, agg := range s.query.cq.Aggregates {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", index)
		}
		field, ok := s.schema.Field(name)
		if !ok {
			return fmt.Errorf("aggregate output schema is missing value %q", name)
		}
		var validity memory.Bitmap
		if field.Nullable {
			validity = memory.EnsureBitmap(nil, rows)
		}
		switch field.Type {
		case memory.TypeString, memory.TypeBinary:
			dst.Strings[name] = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, 0, plainStringBytes), Validity: validity, Encoding: memory.EncodingView}
		case memory.TypeInt8:
			dst.Int8s[name] = &memory.Int8Vector{Values: make([]int8, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeInt16:
			dst.Int16s[name] = &memory.Int16Vector{Values: make([]int16, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			dst.Int32s[name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeUint8:
			dst.Uint8s[name] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeUint16:
			dst.Uint16s[name] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeUint32, memory.TypeBool:
			dst.Uint32s[name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeFloat32:
			dst.Float32s[name] = &memory.Float32Vector{Values: make([]float32, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			dst.Int64s[name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeUint64:
			dst.Uint64s[name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: validity, Encoding: memory.EncodingPlain}
		case memory.TypeFloat64:
			dst.Float64s[name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: validity, Encoding: memory.EncodingPlain}
		default:
			return fmt.Errorf("aggregate output type %s for %q is unsupported", field.Type, name)
		}
	}
	dst.Reset(rows)
	return nil
}

func (s *spillableAggregateStream) setOutputKey(dst *memory.Batch, row, groupIndex int, raw uint64, stringKey []byte, valid bool) error {
	group := s.query.cq.GroupBy[groupIndex]
	field, _ := s.schema.Field(group.Name)
	if !valid && !field.Nullable {
		return fmt.Errorf("%w: NULL aggregate key for non-nullable field %q", memory.ErrSpillCorrupt, field.Name)
	}
	switch field.Type {
	case memory.TypeInt32:
		v := dst.Int32s[field.Name]
		v.Values[row] = int32(uint32(raw))
		if valid && v.Validity != nil {
			memory.Set(v.Validity, row)
		}
	case memory.TypeInt64:
		v := dst.Int64s[field.Name]
		v.Values[row] = int64(raw)
		if valid && v.Validity != nil {
			memory.Set(v.Validity, row)
		}
	case memory.TypeUint32:
		v := dst.Uint32s[field.Name]
		v.Values[row] = uint32(raw)
		if valid && v.Validity != nil {
			memory.Set(v.Validity, row)
		}
	case memory.TypeString:
		if field.Encoding == memory.EncodingDictionary {
			v := dst.DictStrings[field.Name]
			v.Indices[row] = uint32(raw)
			if valid && v.Validity != nil {
				memory.Set(v.Validity, row)
			}
		} else {
			v := dst.Strings[field.Name]
			if valid {
				if len(v.Data) > math.MaxUint32-len(stringKey) {
					return fmt.Errorf("aggregate output string offsets exceed uint32")
				}
				v.Data = append(v.Data, stringKey...)
				if v.Validity != nil {
					memory.Set(v.Validity, row)
				}
			}
			v.Offsets[row+1] = uint32(len(v.Data))
		}
	default:
		return fmt.Errorf("unsupported aggregate key output type %s", field.Type)
	}
	return nil
}

func aggregateValue(state aggregateRecord, kind expr.AggregateKind) (int64, float64, bool) {
	switch kind {
	case expr.AggSum:
		return state.state.Sum, 0, state.state.SumValid
	case expr.AggCountRows:
		return int64(state.state.CountRows), 0, true
	case expr.AggCountValues:
		return int64(state.state.CountValues), 0, true
	case expr.AggMin:
		return state.state.Min, 0, state.state.MinValid
	case expr.AggMax:
		return state.state.Max, 0, state.state.MaxValid
	case expr.AggAvg:
		if state.state.CountValues == 0 {
			return 0, 0, false
		}
		return 0, float64(state.state.Sum) / float64(state.state.CountValues), true
	default:
		return 0, 0, false
	}
}

func (s *spillableAggregateStream) setOutputRecord(dst *memory.Batch, row int, record aggregateRecord) error {
	switch s.query.cq.GroupShape {
	case GroupShapeGeneric:
		if err := s.setGenericOutputKeys(dst, row, record.stringKey); err != nil {
			return err
		}
	case GroupShapeGlobal:
		// No key columns are emitted.
	case GroupShapePairFixed:
		if err := s.setOutputKey(dst, row, 0, record.a, nil, record.validMask&1 != 0); err != nil {
			return err
		}
		if err := s.setOutputKey(dst, row, 1, record.b, nil, record.validMask&2 != 0); err != nil {
			return err
		}
	case GroupShapeStringView:
		if err := s.setOutputKey(dst, row, 0, 0, record.stringKey, record.validMask&1 != 0); err != nil {
			return err
		}
	default:
		if err := s.setOutputKey(dst, row, 0, record.a, nil, record.validMask&1 != 0); err != nil {
			return err
		}
	}
	for index, agg := range s.query.cq.Aggregates {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", index)
		}
		if s.query.byteStatisticsOutput(index) {
			data, valid := s.query.byteStatisticsValue(record, index)
			v := dst.Strings[name]
			if v == nil {
				return fmt.Errorf("endpoint output %q missing", name)
			}
			if len(data) > math.MaxUint32-len(v.Data) || len(data) > cap(v.Data)-len(v.Data) {
				return fmt.Errorf("endpoint output %q payload exceeds admitted capacity", name)
			}
			if valid {
				v.Data = append(v.Data, data...)
				if v.Validity != nil {
					memory.Set(v.Validity, row)
				}
			}
			v.Offsets[row+1] = uint32(len(v.Data))
			continue
		}
		if s.query.nativeAggregate() {
			typ, bits, valid := s.query.nativeRecordValue(record, index)
			field, _ := s.schema.Field(name)
			if typ != field.Type || (!valid && !field.Nullable) {
				return fmt.Errorf("aggregate typed output mismatch for %q", name)
			}
			if err := setNativeAggregateOutput(dst, name, row, typ, bits, valid); err != nil {
				return err
			}
			continue
		}
		lane := 0
		if s.query.cq.MultiAggregate != nil {
			lane = s.query.cq.MultiAggregate.OutputLane[index]
		}
		valueRecord := record
		valueRecord.state = aggregateRecordState(record, lane)
		integer, floating, valid := aggregateValue(valueRecord, agg.Kind)
		field, _ := s.schema.Field(name)
		if !valid && !field.Nullable {
			return fmt.Errorf("%w: NULL aggregate value for non-nullable field %q", memory.ErrSpillCorrupt, name)
		}
		switch field.Type {
		case memory.TypeInt64:
			v := dst.Int64s[name]
			v.Values[row] = integer
			if valid && v.Validity != nil {
				memory.Set(v.Validity, row)
			}
		case memory.TypeUint64:
			v := dst.Uint64s[name]
			v.Values[row] = uint64(integer)
			if valid && v.Validity != nil {
				memory.Set(v.Validity, row)
			}
		case memory.TypeFloat64:
			v := dst.Float64s[name]
			v.Values[row] = floating
			if valid && v.Validity != nil {
				memory.Set(v.Validity, row)
			}
		}
	}
	return nil
}

func (s *spillableAggregateStream) materializeOutputRun(ctx context.Context, ref aggregateRunRef, dst *memory.Batch) (memory.Reservation, error) {
	restored, err := s.store.Restore(ctx, ref.handle, s.fingerprint, s.restorer)
	if err != nil {
		return nil, err
	}
	defer restored.Close()
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != s.query.aggregateRunCodec() {
		return nil, fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	preflight, err := s.query.newAggregateRunReader(restored.Object.Payload)
	if err != nil {
		return nil, err
	}
	if preflight.groups != ref.groups || preflight.groups > s.outputRows {
		return nil, fmt.Errorf("%w: aggregate output groups=%d want=%d max=%d", memory.ErrSpillCorrupt, preflight.groups, ref.groups, s.outputRows)
	}
	visit := func(fn func(aggregateRecord) error) error {
		reader, err := s.query.newAggregateRunReader(restored.Object.Payload)
		if err != nil {
			return err
		}
		return reader.visit(fn)
	}
	return s.materializeOutputRecords(ctx, visit, ref.groups, dst)
}

// materializeOutputRecords requires a replayable visitor over stable records.
// All payload and descriptor storage is admitted before allocation. No record
// slice is retained, so output scratch remains bounded by the emitted columns.
func (s *spillableAggregateStream) materializeOutputRecords(ctx context.Context, visit aggregateRecordVisitor, groups int, dst *memory.Batch) (memory.Reservation, error) {
	plainStringBytes := int64(0)
	endpointBytes := int64(0)
	if s.query.hasByteStatistics() || s.query.cq.GroupShape == GroupShapeStringView || s.query.cq.GroupShape == GroupShapeGeneric {
		err := visit(func(record aggregateRecord) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, e := s.query.byteStatisticsPayloadSize(record)
			if e != nil {
				return e
			}
			if n > math.MaxInt64-endpointBytes {
				return fmt.Errorf("endpoint payload overflow")
			}
			endpointBytes += n
			if s.query.cq.GroupShape == GroupShapeGeneric {
				return visitCanonicalGroupKey(record.stringKey, s.query.cq.GenericFields, func(_ int, typ memory.DataType, valid bool, _ uint64, data []byte) error {
					if valid && (typ == memory.TypeString || typ == memory.TypeBinary) {
						if int64(len(data)) > math.MaxInt64-plainStringBytes {
							return fmt.Errorf("generic output payload overflow")
						}
						plainStringBytes += int64(len(data))
					}
					return nil
				})
			}
			if int64(len(record.stringKey)) > math.MaxInt64-plainStringBytes {
				return fmt.Errorf("aggregate output string bytes overflow")
			}
			plainStringBytes += int64(len(record.stringKey))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if endpointBytes > math.MaxInt64-plainStringBytes {
		return nil, fmt.Errorf("endpoint payload overflow")
	}
	stringCapacity := plainStringBytes + endpointBytes
	if s.query.cq.GroupShape == GroupShapeGeneric {
		stringCapacity = endpointBytes
	}
	bytes, err := aggregateOutputBytes(s.schema, groups, stringCapacity)
	if err == nil && s.query.cq.GroupShape == GroupShapeGeneric {
		if plainStringBytes > math.MaxInt64-bytes {
			return nil, fmt.Errorf("generic output size overflow")
		}
		bytes += plainStringBytes
	}
	if err != nil {
		return nil, err
	}
	lease, err := s.output.ReserveGrowth(ctx, bytes)
	if err != nil {
		return nil, fmt.Errorf("reserve aggregate output batch: %w", err)
	}
	if stringCapacity > int64(math.MaxInt) {
		lease.Release()
		return nil, fmt.Errorf("aggregate output strings exceed addressable memory")
	}
	if err := s.allocateOutputBatch(dst, groups, int(stringCapacity)); err != nil {
		dst.ClearColumns()
		lease.Release()
		return nil, err
	}
	if s.query.cq.GroupShape == GroupShapeGeneric {
		// Count each string independently without unbudgeted per-column scratch.
		for index, field := range s.query.cq.GenericFields {
			if field.Type != memory.TypeString && field.Type != memory.TypeBinary {
				continue
			}
			size := 0
			countErr := visit(func(record aggregateRecord) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return visitCanonicalGroupKey(record.stringKey, s.query.cq.GenericFields, func(i int, _ memory.DataType, valid bool, _ uint64, data []byte) error {
					if i == index && valid {
						if len(data) > math.MaxInt-size {
							return fmt.Errorf("generic payload size overflow")
						}
						size += len(data)
					}
					return nil
				})
			})
			if countErr != nil {
				dst.ClearColumns()
				lease.Release()
				return nil, countErr
			}
			dst.Strings[field.Name].Data = make([]byte, 0, size)
		}
	}
	row := 0
	err = visit(func(record aggregateRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if row >= groups {
			return fmt.Errorf("aggregate output exceeded %d groups", groups)
		}
		if err := s.setOutputRecord(dst, row, record); err != nil {
			return err
		}
		row++
		return nil
	})
	if err != nil || row != groups {
		dst.ClearColumns()
		lease.Release()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: aggregate output decoded %d groups, want %d", memory.ErrSpillCorrupt, row, groups)
	}
	return lease, nil
}

// AggregateStreamStats exposes execution-level diagnostics without exposing
// hash-table or spill-file internals.
type AggregateStreamStats struct {
	SelectedRows       int
	Groups             int
	Checksum           int64
	SpillRuns          int64
	SpillBytes         int64
	MaxRadixDepth      int
	MaxPartitionGroups int
	Elapsed            time.Duration
}

// Stats returns a stable snapshot of spillable aggregation diagnostics.
func (s *spillableAggregateStream) Stats() AggregateStreamStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return AggregateStreamStats{
		SelectedRows: s.selectedRows, Groups: s.groupCount, Checksum: s.checksum,
		SpillRuns: s.spillRuns, SpillBytes: s.spillBytes, MaxRadixDepth: s.maxDepth,
		MaxPartitionGroups: s.maxPartitionGroups, Elapsed: time.Since(s.started),
	}
}

// StreamDiagnostics exposes cold-path ExplainAnalyze attributes.
func (s *spillableAggregateStream) StreamDiagnostics() map[string]string {
	stats := s.Stats()
	peak := int64(0)
	if s.budget != nil {
		peak = s.budget.Snapshot().PeakBytes
	}
	return map[string]string{
		"strategy": planner.StrategySpillHash.String(), "workers": "1",
		"groups": fmt.Sprint(stats.Groups), "selected_rows": fmt.Sprint(stats.SelectedRows),
		"checksum":   fmt.Sprint(stats.Checksum),
		"spill_runs": fmt.Sprint(stats.SpillRuns), "spill_bytes": fmt.Sprint(stats.SpillBytes),
		"peak_memory_scope": "query", "peak_memory_bytes": fmt.Sprint(peak), "max_radix_depth": fmt.Sprint(stats.MaxRadixDepth),
		"max_partition_groups": fmt.Sprint(stats.MaxPartitionGroups),
		"kernel_mode":          s.query.e.kernels.Capabilities().Mode().String(), "kernel_arch": s.query.e.kernels.Capabilities().Arch(),
	}
}

func (s *spillableAggregateStream) recordStats() {
	s.mu.Lock()
	if s.statsRecorded {
		s.mu.Unlock()
		return
	}
	s.statsRecorded = true
	stats := AggregateStreamStats{
		SelectedRows: s.selectedRows, Groups: s.groupCount, Checksum: s.checksum,
		SpillRuns: s.spillRuns, SpillBytes: s.spillBytes, MaxRadixDepth: s.maxDepth,
		MaxPartitionGroups: s.maxPartitionGroups, Elapsed: time.Since(s.started),
	}
	s.mu.Unlock()
	if s.query.feedbackKey != "" {
		s.query.e.feedback.observe(s.query.feedbackKey, s.query.stats.Rows, stats.SelectedRows, stats.Groups)
	}
	s.query.e.opts.Observer.Observe(observer.Event{
		Stage: "spillable-aggregate", Duration: stats.Elapsed, Rows: s.query.stats.Rows, Bytes: stats.SpillBytes,
		Workers: 1, Attributes: s.StreamDiagnostics(),
	})
}

func (s *spillableAggregateStream) finishActive() {
	s.mu.Lock()
	if s.table != nil {
		s.table.close()
		s.table = nil
	}
	s.outputRuns = nil
	s.outputRefs.Release()
	for i := range s.partitions {
		s.partitions[i].Release()
	}
	s.partitions = nil
	// Owned references must be dropped before the registry releases handle metadata.
	cleanupErr := s.store.Close()
	if s.closeErr == nil {
		s.closeErr = cleanupErr
	}
	if s.metadataLease != nil {
		s.metadataLease.Release()
		s.metadataLease = nil
	}
	active := s.active
	s.active = false
	unregister := s.unregister
	s.unregister = nil
	s.mu.Unlock()
	if unregister != nil {
		unregister()
	}
	if active {
		s.query.end()
	}
}

func (s *spillableAggregateStream) abort(err error) error {
	s.mu.Lock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	owner := s.table
	s.table = nil
	partitions := s.partitions
	s.partitions = nil
	outputs := s.outputRuns
	s.outputRuns = nil
	lease, outputDst := s.outputLease, s.outputDst
	s.outputLease, s.outputDst = nil, nil
	s.mu.Unlock()
	if outputDst != nil {
		outputDst.ClearColumns()
	}
	if owner != nil {
		owner.close()
	}
	if lease != nil {
		lease.Release()
	}
	cleanupCtx := context.Background()
	for i := range partitions {
		s.deleteRunRefs(cleanupCtx, partitions[i].Values)
		partitions[i].Release()
	}
	s.deleteRunRefs(cleanupCtx, outputs)
	outputs = nil
	partitions = nil
	s.finishActive()
	return err
}

// Next emits one bounded aggregate output batch. It consumes and partitions
// the input only on first demand, then visits resident state or restores one run.
func (s *spillableAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("aggregate stream destination is nil")
	}
	dst.ClearColumns()
	s.mu.Lock()
	if s.outputDst != nil {
		s.outputDst.ClearColumns()
		s.outputDst = nil
	}
	if s.outputLease != nil {
		s.outputLease.Release()
		s.outputLease = nil
	}
	if s.closed {
		s.mu.Unlock()
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.Unlock()
		return false, err
	}
	if s.exhausted {
		s.mu.Unlock()
		return false, nil
	}
	prepared := s.prepared
	s.mu.Unlock()
	if ctx == nil {
		return false, s.abort(fmt.Errorf("aggregate stream context is nil"))
	}
	var ownershipErr error
	ctx, ownershipErr = aggregateStreamContext(ctx, s.budget)
	if ownershipErr != nil {
		return false, s.abort(ownershipErr)
	}
	if err := ctx.Err(); err != nil {
		return false, s.abort(err)
	}
	if !prepared {
		if err := s.prepareInput(ctx); err != nil {
			return false, s.abort(err)
		}
	}

	s.mu.Lock()
	if s.residentOutput && s.table != nil && s.outputEmitted < s.groupCount {
		start := s.outputCursor
		end := start
		rows := min(s.outputRows, s.groupCount-s.outputEmitted)
		visit := func(fn func(aggregateRecord) error) error {
			cursor := start
			_, err := s.table.table.visitChunk(&cursor, rows, fn)
			end = cursor
			return err
		}
		lease, err := s.materializeOutputRecords(ctx, visit, rows, dst)
		if err == nil {
			s.outputCursor, s.outputEmitted = end, s.outputEmitted+rows
			s.outputLease, s.outputDst = lease, dst
			if s.outputEmitted == s.groupCount {
				s.table.close()
				s.table = nil
			}
			s.mu.Unlock()
			return true, nil
		}
		retained := s.table.retainedBytes()
		s.mu.Unlock()
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			return false, s.abort(err)
		}
		// Failed output admission leaves the cursor untouched. Reclaim state
		// by writing the remaining output once, then retry through restore.
		if _, spillErr := s.state.RequestHardSpill(ctx, retained); spillErr != nil {
			return false, s.abort(errors.Join(err, spillErr))
		}
		s.mu.Lock()
		if s.table != nil {
			s.mu.Unlock()
			return false, s.abort(err)
		}
	}
	if len(s.outputRuns) == 0 {
		s.exhausted = true
		s.mu.Unlock()
		s.recordStats()
		s.finishActive()
		return false, nil
	}
	ref := s.outputRuns[0]
	s.mu.Unlock()
	lease, err := s.materializeOutputRun(ctx, ref, dst)
	if err != nil {
		return false, s.abort(err)
	}
	if err := s.store.Delete(ctx, ref.handle); err != nil {
		dst.ClearColumns()
		lease.Release()
		return false, s.abort(err)
	}
	s.mu.Lock()
	s.outputRuns[0] = aggregateRunRef{}
	s.outputRuns = s.outputRuns[1:]
	s.outputLease, s.outputDst = lease, dst
	s.mu.Unlock()
	return true, nil
}

// Close releases retained state, output buffers, prepared-query ownership and
// every still-live spill run. It is idempotent.
func (s *spillableAggregateStream) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	owner := s.table
	s.table = nil
	partitions := s.partitions
	s.partitions = nil
	outputs := s.outputRuns
	s.outputRuns = nil
	lease, outputDst := s.outputLease, s.outputDst
	s.outputLease, s.outputDst = nil, nil
	s.mu.Unlock()
	if outputDst != nil {
		outputDst.ClearColumns()
	}
	if owner != nil {
		owner.close()
	}
	if lease != nil {
		lease.Release()
	}
	cleanupCtx := context.Background()
	var cleanupErr error
	for i := range partitions {
		if err := s.deleteRunRefsStrict(cleanupCtx, partitions[i].Values); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
		partitions[i].Release()
	}
	if err := s.deleteRunRefsStrict(cleanupCtx, outputs); err != nil && cleanupErr == nil {
		cleanupErr = err
	}
	outputs = nil
	partitions = nil
	// Preserve the earliest error before the private store retries failed files.
	// Joining each run's result would retain an input-sized error tree even
	// when all later deletes succeed.
	s.mu.Lock()
	if s.closeErr == nil {
		s.closeErr = cleanupErr
	}
	s.mu.Unlock()
	s.finishActive()
	s.mu.Lock()
	out := s.closeErr
	s.mu.Unlock()
	return out
}
