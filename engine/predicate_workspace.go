package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// aggregatePredicateWorkspace delegates normal predicate ownership to the same
// workspace used by physical filters, with one separately charged fused word.
type aggregatePredicateWorkspace struct {
	ordinal          uint64
	fusedMask        memory.Bitmap
	fusedReservation memory.Reservation
	workspace        *expr.Workspace
	manager          memory.Manager
	operator         string
}

func (w *aggregatePredicateWorkspace) evaluate(ctx context.Context, program expr.Program, batch *memory.Batch, kernels kernel.Set) (expr.BoolVector, error) {
	if w.workspace == nil {
		if w.manager == nil {
			return expr.BoolVector{}, fmt.Errorf("aggregate predicate workspace has no memory owner")
		}
		var err error
		w.workspace, err = expr.NewWorkspace(ctx, program, w.manager, w.operator)
		if err != nil {
			return expr.BoolVector{}, err
		}
	}
	return w.workspace.EvaluateBool(ctx, batch, kernels)
}

// ensureFused owns exactly one reusable mask word. It is borrowed by the
// aggregate consumer synchronously and overwritten only after the call returns.
func (w *aggregatePredicateWorkspace) ensureFused(ctx context.Context) error {
	if w.fusedMask != nil {
		return nil
	}
	if w.manager == nil {
		return fmt.Errorf("fused predicate workspace has no memory owner")
	}
	lease, err := w.manager.Reserve(ctx, 8)
	if err != nil {
		return fmt.Errorf("reserve fused predicate mask: %w", err)
	}
	w.fusedMask = make(memory.Bitmap, 1)
	w.fusedReservation = lease
	return nil
}

func (w *aggregatePredicateWorkspace) close() {
	w.fusedMask = nil
	if w.fusedReservation != nil {
		w.fusedReservation.Release()
		w.fusedReservation = nil
	}
	if w.workspace != nil {
		_ = w.workspace.Close()
		w.workspace = nil
	}
	w.manager = nil
}

func (w *aggregatePredicateWorkspace) memoryBytes() int64 {
	if w.workspace == nil {
		return 0
	}
	return w.workspace.MemoryBytes()
}

// aggregateStreamContext preserves the stream's bound owner while allowing a
// caller to supply a plain cancellation context or the same query budget.
func aggregateStreamContext(ctx context.Context, budget *memory.QueryBudget) (context.Context, error) {
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		if q != budget {
			return nil, fmt.Errorf("aggregate stream cannot change query ownership")
		}
		return ctx, nil
	}
	return memory.WithQueryBudget(ctx, budget), nil
}

func (w *aggregatePredicateWorkspace) nextOrdinal(rows int) (uint64, error) {
	if rows < 0 || uint64(rows) > ^uint64(0)-w.ordinal {
		return 0, fmt.Errorf("aggregate source ordinal overflow")
	}
	base := w.ordinal
	w.ordinal += uint64(rows)
	return base, nil
}
