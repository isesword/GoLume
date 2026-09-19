package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"math"
	"time"
)

func isAggregatePressure(err error) bool {
	return errors.Is(err, memory.ErrBudgetExceeded) || errors.Is(err, errAggregateStateTarget)
}

func (s *spillableAggregateStream) restoreRun(ctx context.Context, ref aggregateRunRef, visit func(aggregateRecord) error) error {
	restored, err := s.store.Restore(ctx, ref.handle, s.fingerprint, s.restorer)
	if err != nil {
		return err
	}
	defer restored.Close()
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != aggregateRunCodec {
		return fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	reader, err := newAggregateRunReader(restored.Object.Payload, s.query.cq.GroupShape, s.query.cq.AggregateLayout)
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
		if err := s.store.Delete(ctx, ref.handle); err != nil {
			out = errors.Join(out, err)
		}
	}
	return out
}

func (s *spillableAggregateStream) repartitionRuns(ctx context.Context, refs []aggregateRunRef, depth int) ([][]aggregateRunRef, error) {
	if depth >= maxAggregateDepth {
		return nil, fmt.Errorf("aggregate radix repartition exhausted %d hash bits; a colliding partition still exceeds the memory budget", maxAggregateDepth*int(aggregateRadixBits))
	}
	children := make([][]aggregateRunRef, 1<<aggregateRadixBits)
	created := make([]aggregateRunRef, 0, len(refs)*2)
	bitOffset := uint(depth) * aggregateRadixBits
	for _, parent := range refs {
		restored, err := s.store.Restore(ctx, parent.handle, s.fingerprint, s.restorer)
		if err != nil {
			s.deleteRunRefs(context.Background(), created)
			return nil, err
		}
		if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != aggregateRunCodec {
			_ = restored.Close()
			s.deleteRunRefs(context.Background(), created)
			return nil, fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
		}
		for child := range children {
			visit := func(fn func(aggregateRecord) error) error {
				reader, readerErr := newAggregateRunReader(restored.Object.Payload, s.query.cq.GroupShape, s.query.cq.AggregateLayout)
				if readerErr != nil {
					return readerErr
				}
				if reader.groups != parent.groups {
					return fmt.Errorf("%w: aggregate spill group count=%d want=%d", memory.ErrSpillCorrupt, reader.groups, parent.groups)
				}
				return reader.visit(fn)
			}
			encoded, encodeErr := encodeAggregateRun(ctx, s.query.cq.GroupShape, s.query.cq.AggregateLayout, visit, func(_ int, record aggregateRecord) bool {
				return int(aggregateRecordHash(s.query.cq.GroupShape, record)>>bitOffset&((1<<aggregateRadixBits)-1)) == child
			}, s.encoder)
			if encodeErr != nil {
				_ = restored.Close()
				s.deleteRunRefs(context.Background(), created)
				return nil, encodeErr
			}
			if encoded.groups == 0 {
				encoded.close()
				continue
			}
			handle, spillErr := s.store.Spill(ctx, memory.SpillObject{
				Kind: memory.SpillAggregateState, Partition: int32(child), Codec: aggregateRunCodec,
				SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
			})
			groups := encoded.groups
			encoded.close()
			if spillErr != nil {
				_ = restored.Close()
				s.deleteRunRefs(context.Background(), created)
				return nil, spillErr
			}
			ref := aggregateRunRef{handle: handle, groups: groups, partition: child}
			children[child] = append(children[child], ref)
			created = append(created, ref)
			s.mu.Lock()
			s.spillRuns++
			s.spillBytes += handle.Bytes()
			s.mu.Unlock()
		}
		if err := restored.Close(); err != nil {
			s.deleteRunRefs(context.Background(), created)
			return nil, err
		}
	}
	if err := s.deleteRunRefsStrict(ctx, refs); err != nil {
		s.deleteRunRefs(context.Background(), created)
		return nil, err
	}
	s.mu.Lock()
	if depth+1 > s.maxDepth {
		s.maxDepth = depth + 1
	}
	for _, child := range children {
		groups := 0
		for _, ref := range child {
			groups += ref.groups
		}
		if groups > s.maxPartitionGroups {
			s.maxPartitionGroups = groups
		}
	}
	s.mu.Unlock()
	return children, nil
}

func (s *spillableAggregateStream) mergePartition(ctx context.Context, refs []aggregateRunRef, depth int) error {
	if len(refs) == 0 {
		return nil
	}
	owner, err := newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		return fmt.Errorf("create aggregate merge table: %w", err)
	}
	mergeErr := error(nil)
	for _, ref := range refs {
		mergeErr = s.restoreRun(ctx, ref, func(record aggregateRecord) error {
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
		children, repartitionErr := s.repartitionRuns(ctx, refs, depth)
		if repartitionErr != nil {
			return errors.Join(mergeErr, repartitionErr)
		}
		for _, child := range children {
			if err := s.mergePartition(ctx, child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	groups := owner.table.len()
	// Once every partial state merged, the source runs are redundant. Delete
	// them before writing final output runs so disk usage does not silently
	// double at the merge boundary.
	if err := s.deleteRunRefsStrict(ctx, refs); err != nil {
		owner.close()
		return err
	}
	if err := s.writeOutputRuns(ctx, owner.table); err != nil {
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

func (s *spillableAggregateStream) writeOutputRuns(ctx context.Context, table *aggregateStateTable) error {
	groups := table.len()
	created := make([]aggregateRunRef, 0, (groups+s.outputRows-1)/s.outputRows)
	for start := 0; start < groups; start += s.outputRows {
		end := start + s.outputRows
		if end > groups {
			end = groups
		}
		encoded, err := encodeAggregateRun(ctx, s.query.cq.GroupShape, s.query.cq.AggregateLayout, table.visit, func(index int, _ aggregateRecord) bool {
			return index >= start && index < end
		}, s.encoder)
		if err != nil {
			s.deleteRunRefs(context.Background(), created)
			return err
		}
		if encoded.groups == 0 {
			encoded.close()
			continue
		}
		partition := len(s.outputRuns) + len(created)
		if partition > math.MaxInt32 {
			encoded.close()
			s.deleteRunRefs(context.Background(), created)
			return fmt.Errorf("aggregate output run count exceeds int32")
		}
		handle, err := s.store.Spill(ctx, memory.SpillObject{
			Kind: memory.SpillAggregateState, Partition: int32(partition), Codec: aggregateRunCodec,
			SchemaFingerprint: s.fingerprint, Payload: encoded.payload,
		})
		count := encoded.groups
		encoded.close()
		if err != nil {
			s.deleteRunRefs(context.Background(), created)
			return err
		}
		created = append(created, aggregateRunRef{handle: handle, groups: count, partition: partition})
		s.mu.Lock()
		s.spillRuns++
		s.spillBytes += handle.Bytes()
		s.mu.Unlock()
	}
	s.outputRuns = append(s.outputRuns, created...)
	return nil
}

func (s *spillableAggregateStream) finishPartitions(ctx context.Context) error {
	for partition := range s.partitions {
		refs := s.partitions[partition]
		if len(refs) == 0 {
			continue
		}
		if err := s.mergePartition(ctx, refs, 1); err != nil {
			return fmt.Errorf("merge aggregate partition %d: %w", partition, err)
		}
		s.partitions[partition] = nil
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
	for _, field := range schema.Fields {
		width := int64(0)
		switch field.Type {
		case memory.TypeInt32, memory.TypeUint32:
			width = 4
		case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64:
			width = 8
		case memory.TypeString:
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
		case memory.TypeString:
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
		case memory.TypeInt64:
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
		integer, floating, valid := aggregateValue(record, agg.Kind)
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
	if restored.Object.Kind != memory.SpillAggregateState || restored.Object.Codec != aggregateRunCodec {
		return nil, fmt.Errorf("%w: aggregate spill kind or codec mismatch", memory.ErrSpillCorrupt)
	}
	preflight, err := newAggregateRunReader(restored.Object.Payload, s.query.cq.GroupShape, s.query.cq.AggregateLayout)
	if err != nil {
		return nil, err
	}
	if preflight.groups != ref.groups || preflight.groups > s.outputRows {
		return nil, fmt.Errorf("%w: aggregate output groups=%d want=%d max=%d", memory.ErrSpillCorrupt, preflight.groups, ref.groups, s.outputRows)
	}
	plainStringBytes := int64(0)
	if s.query.cq.GroupShape == GroupShapeStringView {
		err = preflight.visit(func(record aggregateRecord) error {
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
	bytes, err := aggregateOutputBytes(s.schema, ref.groups, plainStringBytes)
	if err != nil {
		return nil, err
	}
	lease, err := s.output.ReserveGrowth(ctx, bytes)
	if err != nil {
		return nil, fmt.Errorf("reserve aggregate output batch: %w", err)
	}
	if plainStringBytes > int64(math.MaxInt) {
		lease.Release()
		return nil, fmt.Errorf("aggregate output strings exceed addressable memory")
	}
	if err := s.allocateOutputBatch(dst, ref.groups, int(plainStringBytes)); err != nil {
		lease.Release()
		return nil, err
	}
	reader, err := newAggregateRunReader(restored.Object.Payload, s.query.cq.GroupShape, s.query.cq.AggregateLayout)
	if err != nil {
		dst.ClearColumns()
		lease.Release()
		return nil, err
	}
	row := 0
	err = reader.visit(func(record aggregateRecord) error {
		if err := s.setOutputRecord(dst, row, record); err != nil {
			return err
		}
		row++
		return nil
	})
	if err != nil || row != ref.groups {
		dst.ClearColumns()
		lease.Release()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: aggregate output decoded %d groups, want %d", memory.ErrSpillCorrupt, row, ref.groups)
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
		"groups": fmt.Sprint(stats.Groups), "selected_rows": fmt.Sprint(stats.SelectedRows),
		"checksum":   fmt.Sprint(stats.Checksum),
		"spill_runs": fmt.Sprint(stats.SpillRuns), "spill_bytes": fmt.Sprint(stats.SpillBytes),
		"peak_memory_bytes": fmt.Sprint(peak), "max_radix_depth": fmt.Sprint(stats.MaxRadixDepth),
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
		Attributes: map[string]string{
			"groups": fmt.Sprint(stats.Groups), "spill_runs": fmt.Sprint(stats.SpillRuns),
			"max_radix_depth": fmt.Sprint(stats.MaxRadixDepth), "max_partition_groups": fmt.Sprint(stats.MaxPartitionGroups),
		},
	})
}

func (s *spillableAggregateStream) finishActive() {
	s.mu.Lock()
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
	lease := s.outputLease
	s.outputLease = nil
	s.mu.Unlock()
	if owner != nil {
		owner.close()
	}
	if lease != nil {
		lease.Release()
	}
	cleanupCtx := context.Background()
	for _, refs := range partitions {
		s.deleteRunRefs(cleanupCtx, refs)
	}
	s.deleteRunRefs(cleanupCtx, outputs)
	s.finishActive()
	return err
}

// Next emits one bounded aggregate output batch. It consumes and partitions
// the input only on the first demand, then restores one final run per call.
func (s *spillableAggregateStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("aggregate stream destination is nil")
	}
	dst.ClearColumns()
	s.mu.Lock()
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
	if err := ctx.Err(); err != nil {
		return false, s.abort(err)
	}
	if !prepared {
		if err := s.consumeInput(ctx); err != nil {
			return false, s.abort(err)
		}
		if err := s.finishPartitions(ctx); err != nil {
			return false, s.abort(err)
		}
		s.mu.Lock()
		s.prepared = true
		s.partitions = nil
		s.mu.Unlock()
		s.recordStats()
	}
	s.mu.Lock()
	if len(s.outputRuns) == 0 {
		s.exhausted = true
		s.mu.Unlock()
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
	s.outputRuns = s.outputRuns[1:]
	s.outputLease = lease
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
	lease := s.outputLease
	s.outputLease = nil
	s.mu.Unlock()
	if owner != nil {
		owner.close()
	}
	if lease != nil {
		lease.Release()
	}
	cleanupCtx := context.Background()
	var cleanupErr error
	for _, refs := range partitions {
		cleanupErr = errors.Join(cleanupErr, s.deleteRunRefsStrict(cleanupCtx, refs))
	}
	cleanupErr = errors.Join(cleanupErr, s.deleteRunRefsStrict(cleanupCtx, outputs))
	s.finishActive()
	s.mu.Lock()
	s.closeErr = cleanupErr
	s.mu.Unlock()
	return cleanupErr
}
