package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

// FilterSource evaluates a compiled SQL predicate and compacts selected rows
// into reusable reader-local column buffers. It is used at physical pipeline
// boundaries where a filter must execute before a computed projection or join.
type FilterSource struct {
	input   source.Source
	program expr.Program
	kernel  kernel.Set
}

// NewFilterSource compiles predicate against input and returns a reusable
// compacting source. A nil predicate returns input unchanged.
func NewFilterSource(input source.Source, predicate expr.Expr, mode kernel.Mode) (source.Source, error) {
	if predicate == nil {
		return input, nil
	}
	program, _, err := expr.CompilePredicate(input.Schema(), predicate)
	if err != nil {
		return nil, err
	}
	return &FilterSource{input: input, program: program, kernel: kernel.New(mode)}, nil
}

// Schema preserves the input schema.
func (s *FilterSource) Schema() memory.Schema { return s.input.Schema() }

// Stats returns the child statistics as a conservative upper bound because the
// exact selected row count is intentionally not computed during preparation.
func (s *FilterSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Partitions preserves input partition boundaries and row counts as upper bounds.
func (s *FilterSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewReader creates an independent compacting filter reader.
func (s *FilterSource) NewReader(ctx context.Context) (source.Reader, error) {
	r, err := s.input.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return &filterReader{owner: s, child: r, input: memory.NewBatch(), output: memory.NewBatch()}, nil
}

type filterReader struct {
	owner  *FilterSource
	child  source.Reader
	input  *memory.Batch
	output *memory.Batch
	eval   expr.EvalWorkspace
	rows   []int
}

// Close releases the child reader.
func (r *filterReader) Close() error { return r.child.Close() }

// ReadPartition filters and compacts one input partition. Specialized vectors
// use typed gathers; opaque Arrow vectors use Arrow-Go's generic Take kernel.
func (r *filterReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if err := r.child.ReadPartition(ctx, p, r.input); err != nil {
		return err
	}
	bv, err := r.owner.program.Evaluate(r.input, &r.eval, expr.GTKernel(r.owner.kernel.Int64GT))
	if err != nil {
		return err
	}
	r.rows = r.rows[:0]
	for i := 0; i < r.input.Rows; i++ {
		if memory.IsSet(bv.Validity, i) && memory.IsSet(bv.Truth, i) {
			r.rows = append(r.rows, i)
		}
	}
	if len(r.rows) == r.input.Rows {
		dst.ClearColumns()
		bindAll(dst, r.input)
		dst.Reset(r.input.Rows)
		return nil
	}
	if err := compactSelected(ctx, r.output, r.input, r.rows); err != nil {
		return err
	}
	dst.ClearColumns()
	bindAll(dst, r.output)
	dst.Reset(len(r.rows))
	return nil
}

// Dictionary forwards stable dictionary metadata from the filtered input.
func (s *FilterSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("filter input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}
