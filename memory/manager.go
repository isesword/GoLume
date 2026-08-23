package memory

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrBudgetExceeded is returned when a reservation would exceed the configured query memory limit.
var ErrBudgetExceeded = errors.New("memory budget exceeded")

// Reservation represents memory charged to a Manager until Release is called.
type Reservation interface {
	Bytes() int64
	Release()
}

// Manager accounts query memory and rejects reservations that exceed its limit.
type Manager interface {
	Reserve(ctx context.Context, bytes int64) (Reservation, error)
	Used() int64
	Limit() int64
}

type reservation struct {
	bytes int64
	mgr   *BudgetManager
	done  atomic.Bool
}

// Bytes returns the number of bytes charged by the reservation.
func (r *reservation) Bytes() int64 { return r.bytes }

// Release returns the reservation to its manager exactly once.
func (r *reservation) Release() {
	if r != nil && r.mgr != nil && r.done.CompareAndSwap(false, true) {
		r.mgr.used.Add(-r.bytes)
	}
}

// BudgetManager is a lock-free atomic implementation of Manager with a fixed byte limit.
// A non-positive limit means unlimited memory.
type BudgetManager struct {
	limit int64
	used  atomic.Int64
}

// NewBudgetManager creates a memory manager with limit bytes; a non-positive limit is unlimited.
func NewBudgetManager(limit int64) *BudgetManager { return &BudgetManager{limit: limit} }

// Reserve atomically charges bytes to the manager or returns ErrBudgetExceeded.
func (m *BudgetManager) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if bytes <= 0 {
		return &reservation{}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	for {
		cur := m.used.Load()
		if m.limit > 0 && cur+bytes > m.limit {
			return nil, ErrBudgetExceeded
		}
		if m.used.CompareAndSwap(cur, cur+bytes) {
			return &reservation{bytes: bytes, mgr: m}, nil
		}
	}
}

// Used returns the number of currently reserved bytes.
func (m *BudgetManager) Used() int64 { return m.used.Load() }

// Limit returns the configured byte limit; non-positive means unlimited.
func (m *BudgetManager) Limit() int64 { return m.limit }
