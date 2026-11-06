package engine

import (
	"context"
	"gocolumn/memory"
	"sync"
	"sync/atomic"
)

type preparedCacheHandle struct {
	id       uint64
	closed   atomic.Bool
	retained sync.WaitGroup
}

// PreparedCacheStats separates available idle bytes from admission credits
// held during a handoff. Both remain bounded by LimitBytes; engine Memory also
// includes all live cached buffers throughout every ownership transition.
type PreparedCacheStats struct {
	LimitBytes              int64
	ReservedBytes           int64
	IdleBytes               int64
	IdleEntries             int
	Hits, Misses, Evictions uint64
}

type residentExecutionCache struct {
	mu                        sync.Mutex
	drained                   *sync.Cond
	limit, credits, idleBytes int64
	idleCount                 int
	head, tail                *residentReuseUnit
	closed                    bool
	unregister                func()
	hits, misses, evictions   uint64
	nextID                    atomic.Uint64
}

func newResidentExecutionCache(root *memory.EngineBudget, limit int64) *residentExecutionCache {
	if root.Limit() > 0 && limit > root.Limit() {
		limit = root.Limit()
	}
	c := &residentExecutionCache{limit: max(int64(0), limit)}
	c.drained = sync.NewCond(&c.mu)
	if c.limit > 0 {
		var err error
		c.unregister, err = root.RegisterReclaimer(c.reclaim)
		if err != nil {
			panic(err)
		}
	}
	return c
}
func (c *residentExecutionCache) newHandle() *preparedCacheHandle {
	return &preparedCacheHandle{id: c.nextID.Add(1)}
}
func (c *residentExecutionCache) enabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && c.limit > 0
}
func (c *residentExecutionCache) unlink(u *residentReuseUnit) {
	if u.prev != nil {
		u.prev.next = u.next
	} else {
		c.head = u.next
	}
	if u.next != nil {
		u.next.prev = u.prev
	} else {
		c.tail = u.prev
	}
	u.prev, u.next = nil, nil
	c.idleBytes -= u.credit
	c.idleCount--
}
func (c *residentExecutionCache) releaseCredit(u *residentReuseUnit) {
	c.mu.Lock()
	tracked := u.credit != 0
	c.credits -= u.credit
	u.credit = 0
	c.drained.Broadcast()
	c.mu.Unlock()
	if tracked {
		u.handle.retained.Done()
	}
}
func (c *residentExecutionCache) discard(u *residentReuseUnit) { u.destroy(); c.releaseCredit(u) }
func (c *residentExecutionCache) take(ctx context.Context, p *PreparedQuery, q *memory.QueryBudget) *residentReuseUnit {
	c.mu.Lock()
	if c.closed || p.cacheHandle.closed.Load() {
		c.mu.Unlock()
		return nil
	}
	var found *residentReuseUnit
	for u := c.head; u != nil; u = u.next {
		if u.planID == p.cacheHandle.id {
			found = u
			break
		}
	}
	if found == nil {
		c.misses++
		c.mu.Unlock()
		return nil
	}
	c.unlink(found)
	c.mu.Unlock()
	if err := found.bind(ctx, p, q); err != nil {
		c.discard(found)
		return nil
	}
	c.mu.Lock()
	c.hits++
	c.mu.Unlock()
	c.releaseCredit(found)
	return found
}
func (c *residentExecutionCache) put(p *PreparedQuery, u *residentReuseUnit) {
	size := u.bytes()
	// Reserve cache admission before detaching from the completed query. The
	// credit includes entries being transferred or evicted until buffers drop.
	for {
		c.mu.Lock()
		if c.closed || p.cacheHandle.closed.Load() || size <= 0 || size > c.limit {
			c.mu.Unlock()
			u.destroy()
			return
		}
		if c.credits <= c.limit-size {
			c.credits += size
			u.handle.retained.Add(1)
			u.credit = size
			c.mu.Unlock()
			break
		}
		victim := c.tail
		if victim == nil {
			c.mu.Unlock()
			u.destroy()
			return
		}
		c.unlink(victim)
		c.evictions++
		c.mu.Unlock()
		c.discard(victim)
	}
	if err := u.detach(); err != nil {
		c.discard(u)
		return
	}
	c.mu.Lock()
	if c.closed || p.cacheHandle.closed.Load() {
		c.mu.Unlock()
		c.discard(u)
		return
	}
	u.next = c.head
	if c.head != nil {
		c.head.prev = u
	} else {
		c.tail = u
	}
	c.head = u
	c.idleBytes += u.credit
	c.idleCount++
	c.mu.Unlock()
}
func (c *residentExecutionCache) reclaim(ctx context.Context, requested int64) {
	var released int64
	for released < requested {
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		victim := c.tail
		if victim == nil {
			c.mu.Unlock()
			return
		}
		c.unlink(victim)
		c.evictions++
		size := victim.credit
		c.mu.Unlock()
		c.discard(victim)
		if size >= requested-released {
			return
		}
		released += size
	}
}
func (c *residentExecutionCache) purge(planID uint64) {
	for {
		c.mu.Lock()
		var victim *residentReuseUnit
		for u := c.head; u != nil; u = u.next {
			if u.planID == planID {
				victim = u
				break
			}
		}
		if victim == nil {
			c.mu.Unlock()
			return
		}
		c.unlink(victim)
		c.mu.Unlock()
		c.discard(victim)
	}
}
func (c *residentExecutionCache) close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.reclaim(context.Background(), int64(^uint64(0)>>1))
	c.mu.Lock()
	for c.credits != 0 {
		c.drained.Wait()
	}
	c.mu.Unlock()
	if c.unregister != nil {
		c.unregister()
		c.unregister = nil
	}
}
func (c *residentExecutionCache) snapshot() PreparedCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return PreparedCacheStats{LimitBytes: c.limit, ReservedBytes: c.credits, IdleBytes: c.idleBytes, IdleEntries: c.idleCount, Hits: c.hits, Misses: c.misses, Evictions: c.evictions}
}

// PreparedCacheStats reports the engine's continuously charged idle state cache.
func (e *Engine) PreparedCacheStats() PreparedCacheStats {
	if e == nil || e.cache == nil {
		return PreparedCacheStats{}
	}
	return e.cache.snapshot()
}
