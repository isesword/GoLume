package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	// ErrReusableAccountClosed reports allocation or attachment after Close.
	ErrReusableAccountClosed = errors.New("reusable memory account closed")
	// ErrReusableAccountDetached reports allocation without a query owner.
	ErrReusableAccountDetached = errors.New("reusable memory account detached")
	// ErrReusableAccountAttached reports attachment to a different live owner.
	ErrReusableAccountAttached = errors.New("reusable memory account already attached")
	// ErrReusableAccountChanged reports an ownership change during reclamation.
	ErrReusableAccountChanged = errors.New("reusable memory account ownership changed")
)

// ReusableAccount owns one bounded collection of allocations across executions.
// Tokens refer to this stable account rather than to an execution's query.
// Detach retains every byte at the engine root while removing query ownership;
// Attach admits all retained bytes to one operator/class without reserving them
// again. The caller must bound retained capacities and make detached state idle
// and evictable. This account does not store, reset, or free backing buffers.
//
// Lock order is account.mu then destination query.reserveMu. No operation takes
// a source query lock, and no reclaim or spill callback runs under either lock.
// The owner must quiesce buffer access before Detach/Attach and drop every buffer
// reference before its token is released. Accounting synchronization does not
// itself synchronize access to the buffers.
type ReusableAccount struct {
	engine     *EngineBudget
	mu         sync.Mutex
	used       int64
	owner      *OperatorBudget
	class      MemoryClass
	generation uint64
	closed     bool
}

// NewReusableAccount creates an empty, detached account on this engine root.
func (e *EngineBudget) NewReusableAccount() (*ReusableAccount, error) {
	if e == nil || e.root == nil {
		return nil, fmt.Errorf("engine memory budget is nil")
	}
	return &ReusableAccount{engine: e}, nil
}

// Attach assigns every retained byte to destination and class. It requires the
// same engine root, an open destination query, a live context, and enough query
// hard-limit capacity. Failure changes no ownership or accounting. Attaching
// the identical owner/class is a validated no-op; a different live owner must
// first be detached. Attach does not spill, reclaim, or change root accounting.
func (a *ReusableAccount) Attach(ctx context.Context, destination *OperatorBudget, class MemoryClass) error {
	if a == nil || a.engine == nil {
		return fmt.Errorf("reusable memory account is nil")
	}
	if ctx == nil {
		return fmt.Errorf("memory attachment context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if destination == nil || destination.query == nil {
		return fmt.Errorf("destination operator budget is nil")
	}
	q := destination.query
	if q.engine != a.engine {
		return fmt.Errorf("reusable memory account belongs to a different engine root")
	}
	if class >= memoryClassCount {
		return fmt.Errorf("invalid memory class %d", class)
	}
	if bound := QueryBudgetFromContext(ctx); bound != nil && bound != q {
		return fmt.Errorf("reusable memory attachment context has a different query budget")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	q.reserveMu.Lock()
	defer q.reserveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.closed {
		return ErrReusableAccountClosed
	}
	if q.closed.Load() {
		return ErrQueryBudgetClosed
	}
	if a.owner != nil {
		if a.owner == destination && a.class == class {
			return nil
		}
		return ErrReusableAccountAttached
	}
	used := q.used.Load()
	if q.hard > 0 && a.used > q.hard-used {
		return newMemoryResourceError("query", destination.name, class, a.used, used, q.hard, nil)
	}
	a.owner, a.class = destination, class
	a.generation++
	a.creditOwner(a.used)
	return nil
}

// Detach removes query/operator/class ownership but keeps all bytes charged to
// the engine root. It clears references to the former query and is idempotent.
// Detach is permitted after either the source query or this account is closed;
// live tokens still release the exact root charge. It never invokes callbacks.
func (a *ReusableAccount) Detach() error {
	if a == nil || a.engine == nil {
		return fmt.Errorf("reusable memory account is nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.owner != nil {
		a.debitOwner(a.used)
		a.owner = nil
		a.class = MemoryRetainedState
		a.generation++
	}
	return nil
}

// Reserve admits a native retained allocation using growth-safe semantics: it
// never invokes query spill callbacks, which could reenter a mutating table.
// A root-pressure failure permits one idle-reclaim pass outside all admission
// locks and one retry. A change of ownership during that pass rejects the retry
// instead of charging a different query. Detached or closed accounts reject
// allocation. Tokens must be released only after their backing is unreachable.
func (a *ReusableAccount) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if a == nil || a.engine == nil {
		return nil, fmt.Errorf("reusable memory account is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("memory reservation context is nil")
	}
	var generation uint64
	for attempt := 0; attempt < 2; attempt++ {
		a.mu.Lock()
		if attempt == 0 {
			generation = a.generation
		}
		lease, err := a.reserveLocked(ctx, bytes, generation)
		a.mu.Unlock()
		var resourceErr *ResourceError
		if err == nil || attempt == 1 || !errors.As(err, &resourceErr) || resourceErr.Scope != "engine" || !errors.Is(err, ErrBudgetExceeded) {
			return lease, err
		}
		a.engine.reclaim(ctx, bytes)
	}
	panic("unreachable reusable reservation retry")
}

func (a *ReusableAccount) reserveLocked(ctx context.Context, bytes int64, generation uint64) (Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.closed {
		return nil, ErrReusableAccountClosed
	}
	if a.generation != generation {
		return nil, ErrReusableAccountChanged
	}
	if a.owner == nil {
		return nil, ErrReusableAccountDetached
	}
	op := a.owner
	q := op.query
	if bound := QueryBudgetFromContext(ctx); bound != nil && bound != q {
		return nil, fmt.Errorf("reusable memory reservation context has a different query budget")
	}
	q.reserveMu.Lock()
	defer q.reserveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q.closed.Load() {
		return nil, ErrQueryBudgetClosed
	}
	if bytes <= 0 {
		return &reusableReservation{}, nil
	}
	used := q.used.Load()
	if q.hard > 0 && bytes > q.hard-used {
		return nil, newMemoryResourceError("query", op.name, a.class, bytes, used, q.hard, nil)
	}
	if err := a.engine.root.reserveBytes(ctx, bytes); err != nil {
		return nil, newMemoryResourceError("engine", op.name, a.class, bytes, a.engine.Used(), a.engine.Limit(), err)
	}
	a.used += bytes
	a.creditOwner(bytes)
	return &reusableReservation{account: a, bytes: bytes}, nil
}

// Used returns the actual live byte capacity of this account in either state.
func (a *ReusableAccount) Used() int64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}

// Limit returns the attached query's hard limit, or the root limit while idle.
func (a *ReusableAccount) Limit() int64 {
	if a == nil || a.engine == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.owner != nil {
		return a.owner.Limit()
	}
	return a.engine.Limit()
}

// Close permanently prevents Reserve and Attach. It does not release live
// tokens or backing buffers: their charge remains with the current owner until
// Release or Detach. The final Release clears references to that owner. Close
// is idempotent, callback-free, and normally follows buffer/token cleanup.
func (a *ReusableAccount) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		a.closed = true
		a.generation++
	}
	if a.used == 0 {
		a.owner = nil
	}
	return nil
}

// creditOwner and debitOwner require account.mu. Credits also require the
// destination query's reserveMu, so hard-limit admission and Close are ordered.
func (a *ReusableAccount) creditOwner(bytes int64) {
	if a.owner == nil || bytes == 0 {
		return
	}
	q := a.owner.query
	used := q.used.Add(bytes)
	for peak := q.peak.Load(); used > peak && !q.peak.CompareAndSwap(peak, used); peak = q.peak.Load() {
	}
	q.classes[a.class].Add(bytes)
	a.owner.used.Add(bytes)
}

func (a *ReusableAccount) debitOwner(bytes int64) {
	if a.owner == nil || bytes == 0 {
		return
	}
	a.owner.used.Add(-bytes)
	q := a.owner.query
	q.classes[a.class].Add(-bytes)
	q.used.Add(-bytes)
}

type reusableReservation struct {
	account *ReusableAccount
	bytes   int64
	done    atomic.Bool
}

// Bytes returns the immutable capacity owned by this allocation token.
func (r *reusableReservation) Bytes() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

// Release returns this token's charge from its current owner exactly once.
func (r *reusableReservation) Release() {
	if r == nil || r.account == nil || !r.done.CompareAndSwap(false, true) {
		return
	}
	a := r.account
	a.mu.Lock()
	defer a.mu.Unlock()
	a.debitOwner(r.bytes)
	a.used -= r.bytes
	a.engine.root.used.Add(-r.bytes)
	if a.closed && a.used == 0 {
		a.owner = nil
	}
}

var _ Manager = (*ReusableAccount)(nil)
