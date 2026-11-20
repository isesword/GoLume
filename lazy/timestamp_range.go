package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/source"
)

// WindowFrameKind selects ROWS (zero) or explicit timestamp RANGE semantics.
type WindowFrameKind = logical.WindowFrameKind

// TimestampRangeFrame specifies an inclusive trailing nanosecond interval.
type TimestampRangeFrame = logical.TimestampRangeFrame

// TimestampRangeSpec selects the two input columns and requested output aliases.
type TimestampRangeSpec = logical.TimestampRangeSpec

const (
	// WindowFrameRows retains the zero ROWS meaning; this dedicated API rejects it.
	WindowFrameRows = logical.WindowFrameRows
	// WindowFrameRange explicitly selects timestamp RANGE.
	WindowFrameRange = logical.WindowFrameRange
)

// WithTimestampRange adds the explicit finite-MemoryOnly peer-complete operator.
// It never inserts sorting, FILTER, partitioning, spill, or a legacy Window node.
func (f LazyFrame) WithTimestampRange(spec TimestampRangeSpec) LazyFrame {
	if f.err != nil {
		return f
	}
	f.plan = &logical.TimestampRange{Input: f.plan, Spec: spec}
	return f
}
func (f LazyFrame) validateTimestampRangeExecution() error {
	if !logical.ContainsTimestampRange(f.plan) {
		return nil
	}
	if _, err := logical.InferSchema(f.plan); err != nil {
		return err
	}
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return err
	}
	if policy != engine.ExecutionMemoryOnly || f.session == nil || f.session.engine == nil || f.session.engine.Memory().Limit() <= 0 {
		return fmt.Errorf("timestamp RANGE requires explicit finite MemoryOnly execution")
	}
	return nil
}

// lowerTimestampRangeInput retains concrete source identity. In particular it
// never wraps a child with a diagnostic source that fabricates an order witness.
func lowerTimestampRangeInput(plan logical.Plan, manager memory.Manager, depth int) (source.Source, error) {
	if depth > 16 {
		return nil, fmt.Errorf("timestamp RANGE lineage exceeds bound")
	}
	switch n := plan.(type) {
	case *logical.Scan:
		if n == nil || n.Source == nil || n.PushedPredicate != nil {
			return nil, fmt.Errorf("timestamp RANGE requires unfiltered original scan")
		}
		return projectScanSource(n.Source, n.Projection)
	case *logical.Projection:
		src, err := lowerTimestampRangeInput(n.Input, manager, depth+1)
		if err != nil {
			return nil, err
		}
		named := make([]expr.NamedExpr, 0, len(n.Columns))
		for _, name := range n.Columns {
			named = append(named, expr.Named(expr.Col(name), name))
		}
		return physical.NewProjectSource(src, named, false, manager)
	default:
		return nil, fmt.Errorf("timestamp RANGE supports only original Scan and direct Projection upstream")
	}
}

func lowerTimestampRange(ctx context.Context, n *logical.TimestampRange, manager memory.Manager, eng *engine.Engine) (source.Source, error) {
	if _, err := logical.InferSchema(n); err != nil {
		return nil, err
	}
	if eng == nil || engine.ExecutionPolicyFromContext(ctx, eng.ExecutionPolicy()) != engine.ExecutionMemoryOnly || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("timestamp RANGE requires explicit finite MemoryOnly execution")
	}
	// Profiling the existing generic lowering would replace the proven input
	// with wrappers. Reject that mode rather than inventing child identities.
	if observer.TraceFromContext(ctx) != nil {
		return nil, fmt.Errorf("timestamp RANGE does not support execution profiling")
	}
	src, err := lowerTimestampRangeInput(n.Input, manager, 0)
	if err != nil {
		return nil, err
	}
	sp := n.Spec
	return physical.NewTimestampRangeSource(src, physical.TimestampRangeSpec{
		OrderBy: sp.OrderBy, Column: sp.Column,
		Frame:          physical.TimestampRangeFrame{Kind: physical.WindowFrameRange, PrecedingNS: sp.Frame.PrecedingNS},
		CountRowsAlias: sp.CountRowsAlias, CountAlias: sp.CountAlias,
		SumAlias: sp.SumAlias, AvgAlias: sp.AvgAlias,
	})
}
