package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"unsafe"
)

// percentRankResidentStream owns actual captured capacities and separately
// admitted result-only outputs, merged only after all input reads finish. Source Stats
// is never an allocation plan. The legacy non-PercentRank reader is unchanged.
type percentRankResidentStream struct {
	owner             *WindowSource
	child             source.BatchStream
	scratch           *memory.Batch
	workspace         *sortWorkspace
	output            *residentSortAccumulator
	base, results     memory.Reservation
	prepared, closed  bool
	pageRows, emitted int
	emptyEmitted      bool
	terminal          error
}

func newPercentRankResidentStream(ctx context.Context, owner *WindowSource, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("percent rank resident requires context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	manager := owner.manager
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		manager = q.Operator("PercentRankResident", memory.MemoryRetainedState)
		opts.MemoryManager = q.Operator("SourceScan", memory.MemoryWorkspace)
	} else if manager == nil {
		manager = opts.MemoryManager
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	// Covers stream/workspace/batch descriptors and empty-input typed headers.
	bytes, err := futureBudgetAdd(8192, int64(len(owner.input.Schema().Fields)), 1024)
	if err != nil {
		return nil, err
	}
	// Retained execution access to positional bucket configuration is charged
	// in addition to the original stream descriptors. Plan objects themselves
	// follow the existing caller-owned constructor lifetime.
	bytes, err = futureBudgetAdd(bytes, int64(len(owner.buckets)), 8)
	if err != nil {
		return nil, err
	}
	base, err := manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	child, err := source.OpenBatchStream(ctx, owner.input, opts)
	if err != nil {
		base.Release()
		return nil, err
	}
	// PR-only output-page default; native scans retain their own 8192-row default.
	pageRows := opts.BatchRows
	if pageRows <= 0 {
		pageRows = 1024
	}
	return &percentRankResidentStream{owner: owner, child: child, scratch: memory.NewBatch(), workspace: &sortWorkspace{parent: manager, bulkCredits: true}, base: base, pageRows: pageRows}, nil
}

// percentRankResidentResultBytes covers every currently supported mixed output:
// all are numeric, including Shift/Fill/MinMax, so no variable-width result
// estimate is needed. Native input strings/dictionaries remain owned by the
// capture accumulator through output invalidation; no payload gather is made.
func percentRankResidentResultBytes(rows int, inputFields int, fields []memory.Field) (int64, error) {
	if rows < 0 {
		return 0, fmt.Errorf("negative resident rows")
	}
	// Adding a new alias may rehash a map holding every retained input field.
	// Keep original capture metadata charged and admit old/new map overlap.
	// This also covers the independent result-only maps before ownership merge.
	total, err := futureBudgetAdd(4096, int64(inputFields), 1024)
	if err != nil {
		return 0, err
	}
	// Integer SUM has |sum| < 2^127 (N<=MaxInt64, each value <=2^64-1).
	// nat growth may retain old/new limbs plus capacity spare; 4KiB covers active
	// total/operand/uint64 limit and fixed exactSum (320B) scratch, sequentially.
	// It is a live-allocation bound, not a claim about GC-delayed garbage or RSS.
	for _, f := range fields {
		width := int64(8)
		switch f.Type {
		case memory.TypeInt8, memory.TypeUint8:
			width = 1
		case memory.TypeInt16, memory.TypeUint16:
			width = 2
		case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32:
			width = 4
		}
		// All outputs reserve a bitmap even when non-null, plus another bitmap for
		// preserveWindowValidity's old/new overlap across partition evaluation.
		data, err := futureBudgetAdd(0, int64(rows), width)
		if err != nil {
			return 0, err
		}
		bits, err := futureBudgetAdd(0, int64(memory.WordsFor(rows)), 16)
		if err != nil {
			return 0, err
		}
		// 25% allocator headroom, vector/map descriptors, exact target/state bindings
		// and immutable alias storage. All is admitted before any output allocation.
		bytes, err := sortWorkspaceSize(data, bits, 1024, int64(len(f.Name))*2)
		if err != nil {
			return 0, err
		}
		bytes, err = sortWorkspaceSize(bytes, bytes/4+1)
		if err != nil {
			return 0, err
		}
		total, err = sortWorkspaceSize(total, bytes)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func (s *percentRankResidentStream) prepare(ctx context.Context) (err error) {
	var full *residentSortAccumulator
	defer func() {
		if full != nil {
			full.close()
		}
	}()
	schema := s.owner.input.Schema()
	for {
		ok, e := s.child.Next(ctx, s.scratch)
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if !ok {
			break
		}
		if s.scratch.Rows == 0 {
			continue
		}
		if full == nil {
			full, e = newResidentSortAccumulator(ctx, schema, s.workspace, s.scratch)
			if e != nil {
				return e
			}
		}
		for row := 0; row < s.scratch.Rows; row++ {
			if row&63 == 0 {
				if e = ctx.Err(); e != nil {
					return e
				}
			}
			if e = full.appendRow(ctx, s.scratch, row); e != nil {
				return e
			}
		}
	}
	child := s.child
	s.child = nil
	if err = child.Close(); err != nil {
		return err
	}
	s.scratch.ClearColumns()
	if full == nil {
		empty := newExternalSortAccumulatorBatch(schema.Fields)
		full, err = newResidentSortAccumulator(ctx, schema, s.workspace, empty)
		destroyExternalSortAccumulatorBatch(empty)
		if err != nil {
			return err
		}
	}
	if err = full.finish(ctx); err != nil {
		return err
	}
	if full.hasOpaque() {
		return fmt.Errorf("percent rank resident requires native payload")
	}
	n := full.batch.Rows
	bindingBytes, err := sortBindingBytes(len(s.owner.partitionBy) + len(s.owner.orderBy))
	if err != nil {
		return err
	}
	rowBytes, err := futureBudgetAdd(0, int64(n), int64(unsafe.Sizeof(int(0))))
	if err != nil {
		return err
	}
	keyBytes, err := futureBudgetAdd(2048, int64(len(s.owner.partitionBy)+len(s.owner.orderBy)), 2*int64(unsafe.Sizeof(SortKey{})))
	if err != nil {
		return err
	}
	bytes, err := sortWorkspaceSize(bindingBytes, rowBytes, keyBytes)
	if err != nil {
		return err
	}
	lease, err := s.workspace.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	defer lease.Release()
	partitions := make([]SortKey, len(s.owner.partitionBy))
	for i, name := range s.owner.partitionBy {
		partitions[i] = SortKey{Column: name}
	}
	keys := make([]SortKey, 0, len(partitions)+len(s.owner.orderBy))
	keys = append(keys, partitions...)
	keys = append(keys, s.owner.orderBy...)
	cmp, err := newBatchComparator(full.batch, schema, keys)
	if err != nil {
		return err
	}
	rows, err := sortRows(ctx, n, cmp, 1, nil)
	if err != nil {
		return err
	}
	bytes, err = percentRankResidentResultBytes(n, len(schema.Fields), s.owner.outputFields)
	if err != nil {
		return err
	}
	// Variance aliases are evaluated sequentially. One admitted moment object
	// and its full result scratch remain live at a time, never one per row.
	if windowsHaveMoments(s.owner.expressions) {
		bytes, err = futureBudgetAdd(bytes, 1, windowExactMomentsBytes)
		if err != nil {
			return err
		}
	}
	s.results, err = s.workspace.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	// Evaluate exclusively into independent result maps. Every original input
	// field (including aliases that will be replaced) remains immutable until
	// the final expression completes. The result lease covers NewBatch headers.
	dst := memory.NewBatch()
	dst.Reset(n)
	// Only drop map references here. Never destroy/zero vector descriptors:
	// after the merge those same vectors belong to full and its output lifetime.
	defer dst.ClearColumns()
	var moments *windowExactMoments
	if windowsHaveMoments(s.owner.expressions) {
		moments = new(windowExactMoments)
	}
	for index, window := range s.owner.expressions {
		if err = ctx.Err(); err != nil {
			return err
		}
		var exact *residentExactWindow
		if s.owner.exactPlans != nil && s.owner.exactPlans[index] != nil {
			plan := s.owner.exactPlans[index]
			if plan.leader != index {
				continue
			}
			for _, target := range plan.indices {
				clearWindowOutput(dst, s.owner.expressions[target].Alias)
			}
			exact, err = bindResidentExactWindow(full.batch, dst, plan, s.owner.expressions)
			if err != nil {
				return err
			}
		} else {
			clearWindowOutput(dst, window.Alias)
		}
		evaluate := func(partition []int) error {
			if exact != nil {
				return exact.evaluate(ctx, partition)
			}
			if isMomentWindow(window.Function) {
				return evaluateResidentVarianceState(ctx, full.batch, schema, dst, partition, window, moments)
			}
			return s.owner.evaluateExpression(ctx, full.batch, schema, dst, partition, index)
		}
		if n == 0 {
			if err = evaluate(nil); err != nil {
				return err
			}
			continue
		}
		for start := 0; start < len(rows); {
			if err = ctx.Err(); err != nil {
				return err
			}
			end := start + 1
			for end < len(rows) && sameRowsByKeys(full.batch, schema, partitions, rows[start], rows[end]) {
				if end&63 == 0 {
					if err = ctx.Err(); err != nil {
						return err
					}
				}
				end++
			}
			if err = evaluate(rows[start:end]); err != nil {
				return err
			}
			start = end
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// All reads of original inputs are complete. Detach aliases before binding
	// the independently computed vectors; never mutate any shared input vector.
	for _, field := range s.owner.outputFields {
		if err = ctx.Err(); err != nil {
			return err
		}
		clearWindowOutput(full.batch, field.Name)
	}
	// The accumulator created only original input map kinds. New result kinds
	// need their own maps; these additions and rehash overlap were pre-admitted.
	if full.batch.Int8s == nil {
		full.batch.Int8s = make(map[string]*memory.Int8Vector)
	}
	if full.batch.Int16s == nil {
		full.batch.Int16s = make(map[string]*memory.Int16Vector)
	}
	if full.batch.Int32s == nil {
		full.batch.Int32s = make(map[string]*memory.Int32Vector)
	}
	if full.batch.Int64s == nil {
		full.batch.Int64s = make(map[string]*memory.Int64Vector)
	}
	if full.batch.Uint8s == nil {
		full.batch.Uint8s = make(map[string]*memory.Uint8Vector)
	}
	if full.batch.Uint16s == nil {
		full.batch.Uint16s = make(map[string]*memory.Uint16Vector)
	}
	if full.batch.Uint32s == nil {
		full.batch.Uint32s = make(map[string]*memory.Uint32Vector)
	}
	if full.batch.Uint64s == nil {
		full.batch.Uint64s = make(map[string]*memory.Uint64Vector)
	}
	if full.batch.Float32s == nil {
		full.batch.Float32s = make(map[string]*memory.Float32Vector)
	}
	if full.batch.Float64s == nil {
		full.batch.Float64s = make(map[string]*memory.Float64Vector)
	}
	bindAll(full.batch, dst)
	if err = ctx.Err(); err != nil {
		return err
	}
	// Transfer the captured owner, including unchanged charged capacities.
	// No token shrinking or early release of replaced-column credit is done.
	s.output = full
	full = nil
	return nil
}

func (s *percentRankResidentStream) cleanup() error {
	if s.output != nil {
		s.output.close()
		s.output = nil
	}
	if s.results != nil {
		s.results.Release()
		s.results = nil
	}
	var err error
	if s.child != nil {
		err = s.child.Close()
		s.child = nil
	}
	if s.scratch != nil {
		s.scratch.ClearColumns()
		s.scratch = nil
	}
	s.owner = nil
	if s.workspace != nil {
		s.workspace.close()
		s.workspace = nil
	}
	if s.base != nil {
		s.base.Release()
		s.base = nil
	}
	return err
}

// bindPage admits the independent borrowed headers, offsets and misaligned
// validity for one output page. Full native payload remains owned until the
// final borrowed page is invalidated by Next or Close.
func (s *percentRankResidentStream) bindPage(ctx context.Context, dst *memory.Batch, count int) error {
	a := s.output
	bytes, err := memory.BorrowedRangeMetadataBytesWithCheck(a.batch, s.emitted, count, ctx.Err)
	if err != nil {
		return err
	}
	headers, err := topKSnapshotHeaders(a.batch)
	if err != nil {
		return err
	}
	total, err := sortWorkspaceSize(bytes, headers)
	if err != nil {
		return err
	}
	a.viewLease, err = a.workspace.Reserve(ctx, total)
	if err != nil {
		return err
	}
	a.view, err = memory.BorrowBatchRangeWithCheck(a.batch, s.emitted, count, ctx.Err)
	if err != nil {
		a.clearOutput()
		return err
	}
	if err = ctx.Err(); err != nil {
		a.clearOutput()
		return err
	}
	dst.ClearColumns()
	bindAll(dst, a.view)
	dst.Reset(count)
	a.borrowed = dst
	return nil
}

// Next emits original-order borrowed pages after evaluating the complete input.
// It honors ReadOptions.BatchRows, defaulting to 1024; it does not make resident
// computation streaming or change the legacy non-PercentRank batch contract.
func (s *percentRankResidentStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if s.output != nil {
		s.output.clearOutput()
	}
	dst.ClearColumns()
	if s.terminal != nil {
		return false, s.terminal
	}
	if ctx == nil {
		s.terminal = fmt.Errorf("percent rank resident requires context")
	} else {
		s.terminal = ctx.Err()
	}
	if s.terminal == nil && !s.prepared {
		s.terminal = s.prepare(ctx)
		if s.terminal == nil {
			s.prepared = true
		}
	}
	if s.terminal != nil {
		s.terminal = errors.Join(s.terminal, s.cleanup())
		return false, s.terminal
	}
	if s.output == nil {
		return false, nil
	}
	rows := s.output.batch.Rows
	if s.emitted >= rows && (rows != 0 || s.emptyEmitted) {
		return false, s.cleanup()
	}
	count := min(s.pageRows, rows-s.emitted)
	if err := s.bindPage(ctx, dst, count); err != nil {
		dst.ClearColumns()
		s.terminal = errors.Join(err, s.cleanup())
		return false, s.terminal
	}
	s.emitted += count
	if rows == 0 {
		s.emptyEmitted = true
	}
	return true, nil
}

// Close releases input, captured/output owners and every execution reservation.
func (s *percentRankResidentStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.cleanup()
}
