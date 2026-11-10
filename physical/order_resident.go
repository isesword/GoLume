package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"unsafe"
)

type residentSortStream struct {
	owner            *SortSource
	child            source.BatchStream
	scratch          *memory.Batch
	workspace        *sortWorkspace
	output           *residentSortAccumulator
	query            *memory.QueryBudget
	prepared, closed bool
	emitted          int
	terminal         error
	order            []int
	orderLease       memory.Reservation
}

func newResidentSortStream(ctx context.Context, owner *SortSource, opts source.ReadOptions) (*residentSortStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("sort context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	q := memory.QueryBudgetFromContext(ctx)
	manager := owner.manager
	if q != nil {
		manager = q.Operator("ResidentSort", memory.MemoryRetainedState)
		opts.MemoryManager = q.Operator("SourceScan", memory.MemoryWorkspace)
	} else if manager == nil {
		manager = opts.MemoryManager
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	child, err := source.OpenBatchStream(ctx, owner.input, opts)
	if err != nil {
		return nil, err
	}
	return &residentSortStream{owner: owner, child: child, scratch: memory.NewBatch(), workspace: &sortWorkspace{parent: manager, bulkCredits: true}, query: q}, nil
}

func (s *residentSortStream) prepare(ctx context.Context) (err error) {
	var input *residentSortAccumulator
	defer func() {
		if input != nil {
			input.close()
		}
	}()
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
		if input == nil {
			input, e = newResidentSortAccumulator(ctx, s.owner.Schema(), s.workspace, s.scratch)
			if e != nil {
				return e
			}
		}
		for row := 0; row < s.scratch.Rows; row++ {
			if e = input.appendRow(ctx, s.scratch, row); e != nil {
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
	if input == nil {
		s.prepared = true
		return nil
	}
	if err = input.finish(ctx); err != nil {
		return err
	}
	// Reserve comparator descriptors, exact ordinal arrays and the actual worker
	// run/heap arrays while the materialized values remain owned and charged.
	bindings, err := sortBindingBytes(len(s.owner.keys))
	if err != nil {
		return err
	}
	workers := 1
	if s.owner.usesParallelSort() {
		workers = min(s.owner.workers, s.owner.executor.MaxWorkers(), input.batch.Rows)
	}
	width := int64(unsafe.Sizeof(int(0)))
	if workers > 1 {
		width *= 2
	}
	var rowBytes, runBytes int64
	if err = addJoinGatherProduct(&rowBytes, uint64(input.batch.Rows), uint64(width)); err != nil {
		return err
	}
	if workers > 1 {
		if err = addJoinGatherProduct(&runBytes, uint64(workers), uint64(unsafe.Sizeof([]int(nil)))+uint64(unsafe.Sizeof(rowRun{}))); err != nil {
			return err
		}
	}
	bytes, err := sortWorkspaceSize(bindings, rowBytes, runBytes, 1024)
	if err != nil {
		return err
	}
	lease, err := s.workspace.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	defer func() {
		if lease != nil {
			lease.Release()
		}
	}()
	cmp, err := newBatchComparator(input.batch, s.owner.Schema(), s.owner.keys)
	if err != nil {
		return err
	}
	rows, err := sortRows(ctx, input.batch.Rows, cmp, s.owner.workers, s.owner.executor)
	if err != nil {
		return err
	}
	if input.hasOpaque() {
		s.output = input
		input = nil
		s.order = rows
		s.orderLease = lease
		lease = nil
		s.prepared = true
		return nil
	}
	output, err := newResidentSortedAccumulator(ctx, input, rows)
	if err != nil {
		return err
	}
	input.clearOutput()
	s.output = output
	s.prepared = true
	return nil
}

func (s *residentSortStream) cleanup() error {
	if s.output != nil {
		s.output.close()
		s.output = nil
	}
	var err error
	if s.child != nil {
		child := s.child
		s.child = nil
		err = child.Close()
	}
	if s.scratch != nil {
		s.scratch.ClearColumns()
	}
	s.order = nil
	if s.orderLease != nil {
		s.orderLease.Release()
		s.orderLease = nil
	}
	if s.workspace != nil {
		s.workspace.close()
		s.workspace = nil
	}
	return err
}

// Next returns borrowed typed output, or one-row views for opaque Arrow payloads.
// The previous output is invalidated before its owner or range metadata is freed.
func (s *residentSortStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.output != nil {
		s.output.clearOutput()
	}
	if dst == nil {
		s.terminal = errors.Join(fmt.Errorf("sort stream destination is nil"), s.cleanup())
		return false, s.terminal
	}
	if ctx == nil {
		s.terminal = errors.Join(fmt.Errorf("sort stream context is nil"), s.cleanup())
		return false, s.terminal
	}
	dst.ClearColumns()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminal != nil {
		return false, s.terminal
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil && q != s.query {
		s.terminal = fmt.Errorf("sort stream cannot change query ownership")
	} else if q == nil && s.query != nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
	}
	if s.terminal == nil {
		s.terminal = ctx.Err()
	}
	if s.terminal == nil && !s.prepared {
		s.terminal = s.prepare(ctx)
	}
	if s.terminal != nil {
		s.terminal = errors.Join(s.terminal, s.cleanup())
		return false, s.terminal
	}
	if s.output == nil || s.emitted >= s.output.batch.Rows {
		return false, s.cleanup()
	}
	row := -1
	if s.output.hasOpaque() {
		row = s.order[s.emitted]
	}
	if err := s.output.bindOutput(ctx, dst, row); err != nil {
		s.terminal = errors.Join(err, s.cleanup())
		return false, s.terminal
	}
	if row < 0 {
		s.emitted = s.output.batch.Rows
	} else {
		s.emitted++
	}
	return true, nil
}

// Close releases every owned value and the upstream stream exactly once.
func (s *residentSortStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return s.cleanup()
}

// Remapping changes code identity. Never forward a source-level dictionary or
// its domain after gathering selected values into a new table.
func residentOrderSchema(schema memory.Schema) memory.Schema {
	needsCopy := false
	for _, f := range schema.Fields {
		if f.Encoding == memory.EncodingDictionary && f.DictionaryDomain != "" {
			needsCopy = true
			break
		}
	}
	if !needsCopy {
		return schema
	}
	fields := make([]memory.Field, len(schema.Fields))
	copy(fields, schema.Fields)
	for i := range fields {
		if fields[i].Encoding == memory.EncodingDictionary {
			fields[i].DictionaryDomain = ""
		}
	}
	return memory.NewSchema(fields...)
}
