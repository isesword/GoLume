package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// PredicateWorkspace owns the static registers and dynamic dictionary caches
// of one immutable predicate in one execution. It is not concurrency safe.
// Call Release on EOF, cancellation, error, or early termination. Failed growth
// leaves the old workspace and its reservations intact so admission can retry.
type PredicateWorkspace struct {
	program     Program
	manager     memory.Manager
	query       *memory.QueryBudget
	workspace   EvalWorkspace
	reservation memory.Reservation
	rows        int
	ctx         context.Context
	released    bool
}

// NewPredicateWorkspace binds a predicate to its execution's memory owner.
// An external QueryBudget takes precedence over manager. A nil manager creates
// an unlimited local owner; allocations still have the same accounting path.
// operatorNames optionally supplies a diagnostic operator name (default Predicate).
// The program must remain immutable for the workspace's lifetime.
func NewPredicateWorkspace(ctx context.Context, program Program, manager memory.Manager, operatorNames ...string) (*PredicateWorkspace, error) {
	if ctx == nil {
		return nil, fmt.Errorf("predicate workspace context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if program.Root < 0 || program.Root >= len(program.Instructions) {
		return nil, fmt.Errorf("invalid predicate program root")
	}
	operator := "Predicate"
	if len(operatorNames) > 0 && operatorNames[0] != "" {
		operator = operatorNames[0]
	}
	q := memory.QueryBudgetFromContext(ctx)
	if q != nil {
		manager = q.Operator(operator, memory.MemoryWorkspace)
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	return &PredicateWorkspace{program: program, manager: manager, query: q}, nil
}

// Evaluate reserves complete replacement storage before allocation, then runs
// the predicate. A context without a QueryBudget keeps the bound owner; an
// explicit different owner is rejected. The returned bitmaps are borrowed
// until Evaluate or Release.
func (w *PredicateWorkspace) Evaluate(ctx context.Context, batch *memory.Batch, kernels kernel.Set) (BoolVector, error) {
	if w == nil || w.released {
		return BoolVector{}, fmt.Errorf("predicate workspace is released")
	}
	if !kernels.Valid() {
		return BoolVector{}, fmt.Errorf("predicate evaluation requires valid kernels")
	}
	if ctx == nil || batch == nil {
		return BoolVector{}, fmt.Errorf("predicate evaluation requires context and batch")
	}
	if err := ctx.Err(); err != nil {
		return BoolVector{}, err
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != w.query {
		return BoolVector{}, fmt.Errorf("predicate workspace cannot change query ownership")
	} else if query == nil && w.query != nil {
		ctx = memory.WithQueryBudget(ctx, w.query)
	}
	w.ctx = ctx
	if w.reservation == nil || batch.Rows > w.rows {
		bytes, err := w.program.WorkspaceBytes(batch.Rows)
		if err != nil {
			return BoolVector{}, err
		}
		lease, err := w.manager.Reserve(ctx, bytes)
		if err != nil {
			return BoolVector{}, fmt.Errorf("reserve predicate workspace: %w", err)
		}
		next := EvalWorkspace{}
		next.SetReserveHook(func(n int64) (memory.Reservation, error) { return w.manager.Reserve(w.ctx, n) })
		next.Ensure(batch.Rows, len(w.program.Instructions))
		previous, previousLease := w.workspace, w.reservation
		w.workspace, w.reservation, w.rows = next, lease, batch.Rows
		previous.Release()
		if previousLease != nil {
			previousLease.Release()
		}
	}
	return w.program.Evaluate(batch, &w.workspace, kernels)
}

// MemoryBytes reports actual retained predicate buffer capacities.
func (w *PredicateWorkspace) MemoryBytes() int64 {
	if w == nil {
		return 0
	}
	return w.workspace.MemoryBytes()
}

// Release idempotently drops all buffers, tokens, and execution references.
func (w *PredicateWorkspace) Release() {
	if w == nil {
		return
	}
	w.workspace.Release()
	if w.reservation != nil {
		w.reservation.Release()
	}
	*w = PredicateWorkspace{released: true}
}
