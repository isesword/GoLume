package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"sync"
	"sync/atomic"
)

// Covers a credit node, the query and root reservation objects, and allocator
// size-class rounding on the supported 64-bit targets.
const sortWorkspaceCreditOverhead int64 = 256

var errSortWorkspaceClosed = errors.New("external sort workspace closed")

// sortWorkspace reserves phase headroom at the parent before buffers are made.
// Child tokens spend that credit without charging the query a second time. An
// unused credit remains charged: run metadata must not steal the next merge's
// required workspace. Close retains credit until the last live token is released.
type sortWorkspace struct {
	parent memory.Manager
	// Resident phases can return all credit together. One node per growth avoids
	// thousands of 4KiB bookkeeping allocations without weakening admission.
	bulkCredits    bool
	mu             sync.Mutex
	growMu         sync.Mutex
	capacity, used int64
	credits        *sortWorkspaceCredit
	closed         bool
}

type sortWorkspaceCredit struct {
	bytes       int64
	reservation memory.Reservation
	next        *sortWorkspaceCredit
}

type sortWorkspaceReservation struct {
	owner *sortWorkspace
	bytes atomic.Int64
	done  atomic.Bool
}

func releaseSortWorkspaceCredits(credits *sortWorkspaceCredit) {
	for credits != nil {
		next := credits.next
		credits.next = nil
		credits.reservation.Release()
		credits = next
	}
}

func (w *sortWorkspace) ensure(ctx context.Context, bytes int64) error {
	if ctx == nil {
		return fmt.Errorf("external sort workspace requires context")
	}
	if bytes < 0 {
		return memory.ErrBudgetExceeded
	}
	w.growMu.Lock()
	defer w.growMu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return errSortWorkspaceClosed
		}
		if bytes <= w.capacity {
			w.mu.Unlock()
			return nil
		}
		delta := bytes - w.capacity
		w.mu.Unlock()
		// Small independently owned credits can be returned at a yield boundary
		// without dropping admission for any live decoded or output buffer.
		var credits *sortWorkspaceCredit
		remaining := delta
		for remaining > 0 {
			n := min(remaining, int64(4096))
			if w.bulkCredits {
				n = remaining
			}
			r, err := w.parent.Reserve(ctx, n+sortWorkspaceCreditOverhead)
			if err != nil {
				releaseSortWorkspaceCredits(credits)
				return err
			}
			credits = &sortWorkspaceCredit{bytes: n, reservation: r, next: credits}
			remaining -= n
		}
		w.mu.Lock()
		if w.closed || ctx.Err() != nil || delta > math.MaxInt64-w.capacity {
			closed := w.closed
			w.mu.Unlock()
			releaseSortWorkspaceCredits(credits)
			if closed {
				return errSortWorkspaceClosed
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return memory.ErrBudgetExceeded
		}
		w.capacity += delta
		for credits != nil {
			next := credits.next
			credits.next = w.credits
			w.credits = credits
			credits = next
		}
		w.mu.Unlock()
	}
}

// trim returns only credit not needed by live child tokens. Reservations are
// released after removing every owner reference, outside the workspace mutex.
func (w *sortWorkspace) trim(target int64) {
	w.mu.Lock()
	target = max(target, w.used)
	var released *sortWorkspaceCredit
	for at := &w.credits; *at != nil; {
		credit := *at
		if credit.bytes <= w.capacity-target {
			*at = credit.next
			w.capacity -= credit.bytes
			credit.next = released
			released = credit
		} else {
			at = &credit.next
		}
	}
	w.mu.Unlock()
	releaseSortWorkspaceCredits(released)
}

// Reserve spends pre-admitted credit before allocation.
func (w *sortWorkspace) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("external sort workspace requires context")
	}
	if bytes < 0 {
		return nil, memory.ErrBudgetExceeded
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return nil, errSortWorkspaceClosed
		}
		if bytes <= w.capacity-w.used {
			w.used += bytes
			w.mu.Unlock()
			r := &sortWorkspaceReservation{owner: w}
			r.bytes.Store(bytes)
			return r, nil
		}
		needed, err := sortWorkspaceSize(w.used, bytes)
		w.mu.Unlock()
		if err != nil {
			return nil, err
		}
		// Exceptional wide rows may exceed the ordinary phase plan. Admit
		// their actual extra capacity at the parent before allocating it.
		if err = w.ensure(ctx, needed); err != nil {
			return nil, err
		}
	}
}

// Used reports bytes held by live child reservations.
func (w *sortWorkspace) Used() int64 { w.mu.Lock(); defer w.mu.Unlock(); return w.used }

// Limit reports currently admitted usable credit.
func (w *sortWorkspace) Limit() int64 { w.mu.Lock(); defer w.mu.Unlock(); return w.capacity }

// Bytes reports the live reservation size.
func (r *sortWorkspaceReservation) Bytes() int64 { return r.bytes.Load() }

// grow admits metadata newly retained by an output before the decoded input
// owner is closed. The same token continues to cover all live output storage.
func (r *sortWorkspaceReservation) grow(ctx context.Context, extra int64) error {
	if extra < 0 {
		return memory.ErrBudgetExceeded
	}
	w := r.owner
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.mu.Lock()
		if w.closed || r.done.Load() {
			w.mu.Unlock()
			return errSortWorkspaceClosed
		}
		if extra <= w.capacity-w.used {
			w.used += extra
			r.bytes.Add(extra)
			w.mu.Unlock()
			return nil
		}
		needed, err := sortWorkspaceSize(w.used, extra)
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if err = w.ensure(ctx, needed); err != nil {
			return err
		}
	}
}

// Release returns this child reservation exactly once.
func (r *sortWorkspaceReservation) Release() {
	if r == nil || !r.done.CompareAndSwap(false, true) {
		return
	}
	w := r.owner
	w.mu.Lock()
	w.used -= r.bytes.Load()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		w.trim(0)
	}
}
func (w *sortWorkspace) close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.trim(0)
}

func sortWorkspaceSize(values ...int64) (int64, error) {
	var total int64
	for _, n := range values {
		if n < 0 || n > math.MaxInt64-total {
			return 0, memory.ErrBudgetExceeded
		}
		total += n
	}
	return total, nil
}
