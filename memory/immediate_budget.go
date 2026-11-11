package memory

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// ImmediateReservationManager optionally admits speculative allocations without
// spilling, reclaiming, or waiting for capacity or another query admission.
// A false result with no error leaves accounting unchanged and means admission
// is temporarily unavailable. Callers must fall back without invoking Reserve
// for the speculative allocation. Successful tokens must be released normally.
// Nil contexts, cancellation, invalid managers, and closed queries return errors.
// Non-positive sizes produce an empty successful token after these checks.
type ImmediateReservationManager interface {
	TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error)
}

// TryReserveImmediate atomically reserves currently available capacity without
// callbacks. It preserves BudgetManager's non-positive-size no-op semantics.
func (m *BudgetManager) TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error) {
	if m == nil {
		return nil, false, fmt.Errorf("memory budget is nil")
	}
	if err := immediateReservationContextError(ctx); err != nil {
		return nil, false, err
	}
	if bytes <= 0 {
		return &reservation{}, true, nil
	}
	if err := m.reserveBytes(ctx, bytes); err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &reservation{bytes: bytes, mgr: m}, true, nil
}

// TryReserveImmediate follows Reserve's same-engine context routing, but never
// invokes root reclamation. Other contexts reserve direct engine capacity.
func (e *EngineBudget) TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error) {
	if e == nil || e.root == nil {
		return nil, false, fmt.Errorf("engine memory budget is nil")
	}
	if q := QueryBudgetFromContext(ctx); q != nil && q.engine == e {
		return q.TryReserveImmediate(ctx, bytes)
	}
	return e.root.TryReserveImmediate(ctx, bytes)
}

// TryReserveImmediate charges the query's unclassified workspace without any
// spill/reclaim callbacks. Unlike Reserve, it conservatively refuses to exceed
// the soft limit, even from spill or growth contexts. An exact limit fit is
// allowed. Query admission lock contention also returns an unchanged miss.
func (q *QueryBudget) TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error) {
	return q.tryReserveImmediate(ctx, MemoryWorkspace, nil, bytes)
}

// TryReserveImmediate charges this operator and its default class without spill,
// reclaim, or waiting for query admission. The operator's own query is used,
// matching Reserve; callers selecting an active context query must do so first.
func (o *OperatorBudget) TryReserveImmediate(ctx context.Context, bytes int64) (Reservation, bool, error) {
	if o == nil || o.query == nil {
		return nil, false, fmt.Errorf("operator memory budget is nil")
	}
	return o.query.tryReserveImmediate(ctx, o.defaultClass, o, bytes)
}

func (q *QueryBudget) tryReserveImmediate(ctx context.Context, class MemoryClass, op *OperatorBudget, bytes int64) (Reservation, bool, error) {
	if q == nil || q.engine == nil || q.engine.root == nil {
		return nil, false, fmt.Errorf("query memory budget is nil")
	}
	if err := immediateReservationContextError(ctx); err != nil {
		return nil, false, err
	}
	if class >= memoryClassCount {
		return nil, false, fmt.Errorf("invalid memory class %d", class)
	}
	if q.closed.Load() {
		return nil, false, ErrQueryBudgetClosed
	}
	if !q.reserveMu.TryLock() {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if q.closed.Load() {
			return nil, false, ErrQueryBudgetClosed
		}
		return nil, false, nil
	}
	defer q.reserveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if q.closed.Load() {
		return nil, false, ErrQueryBudgetClosed
	}
	if bytes <= 0 {
		return &hierarchicalReservation{}, true, nil
	}
	used := q.used.Load()
	if bytes > math.MaxInt64-used || q.soft > 0 && bytes > q.soft-used || q.hard > 0 && bytes > q.hard-used {
		return nil, false, nil
	}
	root, ok, err := q.engine.root.TryReserveImmediate(ctx, bytes)
	if !ok || err != nil {
		return nil, false, err
	}
	return q.creditReservation(root, op, class, bytes), true, nil
}

func immediateReservationContextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("memory reservation context is nil")
	}
	return ctx.Err()
}

var (
	_ ImmediateReservationManager = (*BudgetManager)(nil)
	_ ImmediateReservationManager = (*EngineBudget)(nil)
	_ ImmediateReservationManager = (*QueryBudget)(nil)
	_ ImmediateReservationManager = (*OperatorBudget)(nil)
)
