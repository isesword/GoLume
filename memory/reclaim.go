package memory

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
)

type rootReclaimContextKey struct{}

type rootReclaimer struct {
	active  atomic.Bool
	reclaim func(context.Context, int64)
}

// RegisterReclaimer registers a callback that drops idle retained buffers under
// root pressure. Its argument is the current byte shortfall. The callback must
// drop every reference to the buffers before releasing their reservations; it
// must never reclaim active execution state. Actual root accounting, rather
// than a callback's estimate, determines whether a retry can fit.
//
// Callbacks run without budget or registry locks and may overlap. Reclamation
// is suppressed inside a spill callback because it holds the spill arbiter lock.
// Standalone growth reservations still permit idle reclamation. A callback
// must synchronize its own state and use the supplied context for any nested
// reservation, which suppresses recursive reclaim and spill. The returned
// unregister function is idempotent and does not wait for callbacks already
// running. A closed cache must therefore tolerate a late callback.
func (e *EngineBudget) RegisterReclaimer(reclaim func(context.Context, int64)) (func(), error) {
	if e == nil || e.root == nil {
		return nil, fmt.Errorf("engine memory budget is nil")
	}
	if reclaim == nil {
		return nil, fmt.Errorf("memory reclaimer is nil")
	}
	item := &rootReclaimer{reclaim: reclaim}
	item.active.Store(true)
	e.reclaimMu.Lock()
	if e.reclaimers == nil {
		e.reclaimers = make(map[*rootReclaimer]struct{})
	}
	e.reclaimers[item] = struct{}{}
	e.reclaimMu.Unlock()
	return func() {
		if item.active.CompareAndSwap(true, false) {
			e.reclaimMu.Lock()
			delete(e.reclaimers, item)
			e.reclaimMu.Unlock()
		}
	}, nil
}

func (e *EngineBudget) rootShortfall(bytes int64) int64 {
	limit := e.Limit()
	if limit <= 0 {
		limit = math.MaxInt64
	}
	free := limit - e.Used()
	if bytes <= free {
		return 0
	}
	return bytes - free
}

// reclaim performs one bounded callback pass. Its callers hold no account or
// query admission locks. Reentrant reservations receive at most a raw retry.
func (e *EngineBudget) reclaim(ctx context.Context, bytes int64) {
	if e == nil || e.root == nil || ctx == nil || ctx.Err() != nil || e.rootShortfall(bytes) == 0 {
		return
	}
	if active, _ := ctx.Value(spillCallbackContextKey{}).(bool); active {
		return
	}
	if active, _ := ctx.Value(rootReclaimContextKey{}).(bool); active {
		return
	}
	e.reclaimMu.Lock()
	items := make([]*rootReclaimer, 0, len(e.reclaimers))
	for item := range e.reclaimers {
		items = append(items, item)
	}
	e.reclaimMu.Unlock()
	ctx = withSpillArbitration(context.WithValue(ctx, rootReclaimContextKey{}, true))
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		shortfall := e.rootShortfall(bytes)
		if shortfall == 0 {
			return
		}
		if item.active.Load() {
			item.reclaim(ctx, shortfall)
		}
	}
}
