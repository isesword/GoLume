package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"math/bits"
	"unsafe"
)

// FilterSource evaluates a compiled SQL predicate and compacts selected rows
// into reusable reader-local column buffers. It is used at physical pipeline
// boundaries where a filter must execute before a computed projection or join.
type FilterSource struct {
	input   source.Source
	program expr.Program
	schema  memory.Schema
	columns []string
}

// NewFilterSource compiles predicate against input and returns a reusable
// compacting source. A nil predicate returns input unchanged.
func NewFilterSource(input source.Source, predicate expr.Expr) (source.Source, error) {
	return NewFilterProjectSource(input, predicate, nil)
}

// NewFilterProjectSource filters all input columns but emits only columns in
// the supplied order. Nil columns preserves the full schema; a nonnil empty
// slice emits row counts only. Output bindings are borrowed until Next/Close.
// A nil predicate selects every row. The source holds no execution workspace.
func NewFilterProjectSource(input source.Source, predicate expr.Expr, columns []string) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("filter input is nil")
	}
	if predicate == nil && columns == nil {
		return input, nil
	}
	program, _, err := expr.CompilePredicate(input.Schema(), predicate)
	if err != nil {
		return nil, err
	}
	schema := input.Schema()
	if columns != nil {
		fields := make([]memory.Field, 0, len(columns))
		seen := make(map[string]bool, len(columns))
		for _, name := range columns {
			field, ok := schema.Field(name)
			if !ok {
				return nil, fmt.Errorf("filter output column %q not found", name)
			}
			if seen[name] {
				return nil, fmt.Errorf("duplicate filter output column %q", name)
			}
			seen[name] = true
			fields = append(fields, field)
		}
		schema = memory.NewSchema(fields...)
	}
	names := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		names[i] = f.Name
	}
	return &FilterSource{input: input, program: program, schema: schema, columns: names}, nil
}

// Schema returns the immutable filtered output schema.
func (s *FilterSource) Schema() memory.Schema { return s.schema }

// Ordering retains the child's visible ordering prefix.
func (s *FilterSource) Ordering() []source.OrderingColumn {
	provider, ok := s.input.(source.OrderingProvider)
	if !ok {
		return nil
	}
	keys := provider.Ordering()
	for i, key := range keys {
		if _, ok := s.schema.Field(key.Name); !ok {
			return append([]source.OrderingColumn(nil), keys[:i]...)
		}
	}
	return append([]source.OrderingColumn(nil), keys...)
}

// Replayability forwards the child snapshot contract.
func (s *FilterSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// StreamPlan reports a batch-local pipeline node.
func (s *FilterSource) StreamPlan() source.StreamPlanNode {
	child := source.StreamPlanOf(s.input)
	return source.StreamPlanNode{
		Operator:          "Filter",
		Class:             source.StreamPipeline,
		PartitionParallel: child.PartitionParallel,
		Children:          []source.StreamPlanNode{child},
	}
}

// Stats returns the child statistics as a conservative upper bound because the
// exact selected row count is intentionally not computed during preparation.
func (s *FilterSource) Stats(ctx context.Context) (source.Stats, error) {
	stats, err := s.input.Stats(ctx)
	if err == nil {
		stats.RowsUpperBound = true
	}
	return stats, err
}

// Partitions preserves input partition boundaries and row counts as upper bounds.
func (s *FilterSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewBatchStream creates a native filter stream that may consume any number of
// input morsels before returning the next non-empty output Batch.
func (s *FilterSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, nil, false)
}

// NewBatchStreamWithPartitions propagates prepared partition metadata to the
// partition-capable child without copying it per execution.
func (s *FilterSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, parts, true)
}

func (s *FilterSource) newBatchStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("filter context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	manager := opts.MemoryManager
	bounded := manager != nil || memory.QueryBudgetFromContext(ctx) != nil
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		opts.MemoryManager = q.Operator("SourceScan", memory.MemoryWorkspace)
		manager = q.Operator("Filter", memory.MemoryWorkspace)
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	eval, err := expr.NewPredicateWorkspace(ctx, s.program, manager, "Filter")
	if err != nil {
		return nil, err
	}
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		eval.Release()
		return nil, err
	}
	stream := &filterBatchStream{owner: s, input: memory.NewBatch(), view: memory.NewBatch(), eval: eval, manager: manager, query: memory.QueryBudgetFromContext(ctx), kernels: opts.Kernels}
	stream.output.manager, stream.output.bounded = manager, bounded
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.clearBorrowed()
		stream.view.ClearColumns()
		// input is filled by the child and never owns its borrowed payload.
		stream.input = nil
		stream.eval.Release()
		stream.output.release()
		stream.rows = nil
		if stream.rowsLease != nil {
			stream.rowsLease.Release()
			stream.rowsLease = nil
		}
		stream.manager = nil
		stream.query = nil
		return nil
	}
	return stream, nil
}

type filterBatchStream struct {
	state                 unaryStreamState
	owner                 *FilterSource
	input, view, borrowed *memory.Batch
	output                compactWorkspace
	eval                  *expr.PredicateWorkspace
	manager               memory.Manager
	rowsLease             memory.Reservation
	rows                  []int
	kernels               kernel.Set
	query                 *memory.QueryBudget
}

func (s *filterBatchStream) clearBorrowed() {
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
	}
}

func (s *filterBatchStream) selectRows(ctx context.Context, bv expr.BoolVector) (int, error) {
	selected := 0
	words := memory.WordsFor(s.input.Rows)
	for i := 0; i < words; i++ {
		word := bv.Truth[i]
		if bv.Validity != nil {
			word &= bv.Validity[i]
		}
		if i == words-1 && s.input.Rows&63 != 0 {
			word &= uint64(1)<<uint(s.input.Rows&63) - 1
		}
		bv.Truth[i] = word
		selected += bits.OnesCount64(word)
	}
	if selected == 0 || selected == s.input.Rows {
		return selected, nil
	}
	if cap(s.rows) < selected {
		lease, err := s.manager.Reserve(ctx, int64(selected)*int64(unsafe.Sizeof(int(0))))
		if err != nil {
			return 0, fmt.Errorf("reserve filter selected rows: %w", err)
		}
		next := make([]int, selected)
		previous := s.rowsLease
		s.rows, s.rowsLease = next, lease
		if previous != nil {
			previous.Release()
		}
	} else {
		s.rows = s.rows[:selected]
	}
	offset := 0
	for i := 0; i < words; i++ {
		word := bv.Truth[i]
		for word != 0 {
			s.rows[offset] = i*64 + bits.TrailingZeros64(word)
			offset++
			word &= word - 1
		}
	}
	return selected, nil
}

// Next returns the next non-empty filtered output, clearing the previous
// destination before advancing the child, including when dst changes. A
// context without a QueryBudget keeps the opening owner; it cannot rebind to
// a different explicit owner.
func (s *filterBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.clearBorrowed()
	if stop, err := s.state.before(ctx, dst); stop {
		if err != nil && !s.state.closed && s.state.terminalErr == nil {
			err = s.state.fail(err)
		}
		return false, err
	}
	dst.ClearColumns()
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.query {
		return false, s.state.fail(fmt.Errorf("filter stream cannot change query ownership"))
	} else if query == nil && s.query != nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
	}
	s.view.ClearColumns()
	clearCompactBorrows(s.output.batch)
	for {
		ok, err := s.state.child.Next(ctx, s.input)
		if err != nil {
			return false, s.state.fail(err)
		}
		if !ok {
			return false, s.state.finish()
		}
		bv, err := s.eval.Evaluate(ctx, s.input, s.kernels)
		if err != nil {
			return false, s.state.fail(err)
		}
		selected, err := s.selectRows(ctx, bv)
		if err != nil {
			return false, s.state.fail(err)
		}
		if selected == 0 {
			continue
		}
		for _, name := range s.owner.columns {
			if err := bindNamed(s.view, name, s.input, name); err != nil {
				// These are shallow child bindings, not owned output.
				s.view = memory.NewBatch()
				return false, s.state.fail(err)
			}
		}
		s.view.Rows = s.input.Rows
		if _, err := memory.BorrowedRangeMetadataBytes(s.view, 0, s.view.Rows); err != nil {
			s.view = memory.NewBatch()
			return false, s.state.fail(err)
		}
		borrowFilterOwners(s.view)
		s.view.Rows = s.input.Rows
		out := s.view
		if selected != s.input.Rows {
			out, err = s.output.compact(ctx, s.view, s.rows, bv.Truth, s.kernels)
			if err != nil {
				return false, s.state.fail(err)
			}
		}
		bindAll(dst, out)
		borrowFilterOwners(dst)
		dst.Rows = selected
		s.borrowed = dst
		return true, nil
	}
}

// borrowFilterOwners creates independent non-owning headers for values whose
// ReleaseOwned would otherwise mutate or release the child's vector. Typed Go
// buffers are borrowed directly. This does not retain an Arrow reference.
func borrowFilterOwners(batch *memory.Batch) {
	for name, v := range batch.Opaque {
		if v != nil {
			copy := *v
			copy.Owned = false
			batch.Opaque[name] = &copy
		}
	}
	for name, v := range batch.Structs {
		if v == nil {
			continue
		}
		copy := *v
		copy.Values = memory.NewBatch()
		bindAll(copy.Values, v.Values)
		copy.Values.Rows = v.Values.Rows
		borrowFilterOwners(copy.Values)
		batch.Structs[name] = &copy
	}
}

// Close releases filter workspace and the child stream exactly once.
func (s *filterBatchStream) Close() error { return s.state.Close() }

// Dictionary forwards stable dictionary metadata from the filtered input.
func (s *FilterSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	if _, ok := s.schema.Field(column); !ok {
		return source.Dictionary{}, fmt.Errorf("filter output column %q has no dictionary", column)
	}
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("filter input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

var _ source.StreamPlanProvider = (*FilterSource)(nil)
var _ source.ReplayabilityProvider = (*FilterSource)(nil)
var _ source.BatchStreamProvider = (*FilterSource)(nil)
var _ source.PreparedBatchStreamProvider = (*FilterSource)(nil)
var _ source.BatchStream = (*filterBatchStream)(nil)
