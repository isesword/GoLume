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
}

// NewFilterSource compiles predicate against input and returns a reusable
// compacting source. A nil predicate returns input unchanged.
func NewFilterSource(input source.Source, predicate expr.Expr) (source.Source, error) {
	if predicate == nil {
		return input, nil
	}
	program, _, err := expr.CompilePredicate(input.Schema(), predicate)
	if err != nil {
		return nil, err
	}
	return &FilterSource{input: input, program: program}, nil
}

// Schema preserves the input schema.
func (s *FilterSource) Schema() memory.Schema { return s.input.Schema() }

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
func (s *FilterSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

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
// child Source adapter without copying it per execution.
func (s *FilterSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, parts, true)
}

func (s *FilterSource) newBatchStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		return nil, err
	}
	stream := &filterBatchStream{
		owner:   s,
		input:   memory.NewBatch(),
		output:  memory.NewBatch(),
		kernels: opts.Kernels,
	}
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.input.ReleaseOwned()
		stream.output.ReleaseOwned()
		return nil
	}
	return stream, nil
}

func applyFilterBatch(ctx context.Context, owner *FilterSource, input, output, dst *memory.Batch, eval *expr.EvalWorkspace, rows *[]int, kernels kernel.Set) error {
	bv, err := owner.program.Evaluate(input, eval, kernels)
	if err != nil {
		return err
	}
	*rows = (*rows)[:0]
	for i := 0; i < input.Rows; i++ {
		if memory.IsSet(bv.Validity, i) && memory.IsSet(bv.Truth, i) {
			*rows = append(*rows, i)
		}
	}
	if len(*rows) == input.Rows {
		dst.ClearColumns()
		bindAll(dst, input)
		dst.Reset(input.Rows)
		return nil
	}
	if err := compactSelected(ctx, output, input, *rows, bv.Truth, kernels); err != nil {
		return err
	}
	dst.ClearColumns()
	bindAll(dst, output)
	dst.Reset(len(*rows))
	return nil
}

type filterBatchStream struct {
	state   unaryStreamState
	owner   *FilterSource
	input   *memory.Batch
	output  *memory.Batch
	eval    expr.EvalWorkspace
	rows    []int
	kernels kernel.Set
}

// Next returns the next non-empty filtered output Batch.
func (s *filterBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if stop, err := s.state.before(ctx, dst); stop {
		return false, err
	}
	for {
		ok, err := s.state.child.Next(ctx, s.input)
		if err != nil {
			dst.ClearColumns()
			return false, s.state.fail(err)
		}
		if !ok {
			dst.ClearColumns()
			return false, s.state.finish()
		}
		if err := applyFilterBatch(ctx, s.owner, s.input, s.output, dst, &s.eval, &s.rows, s.kernels); err != nil {
			dst.ClearColumns()
			return false, s.state.fail(err)
		}
		if dst.Rows == 0 {
			continue
		}
		return true, nil
	}
}

// Close releases filter workspace and the child stream.
func (s *filterBatchStream) Close() error { return s.state.Close() }

// Dictionary forwards stable dictionary metadata from the filtered input.
func (s *FilterSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
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
