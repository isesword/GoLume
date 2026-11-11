package pipeline

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"sync/atomic"
	"time"
)

// sharedNativeOwner holds no Batch, map, vector, or consumer destination. Its
// reservation covers shared backing and this stable lease until the last binding
// AND construction reference is gone.
type sharedNativeOwner struct {
	refs        atomic.Int64
	reservation memory.Reservation
}

func (o *sharedNativeOwner) release() {
	if o.refs.Add(-1) == 0 {
		r := o.reservation
		o.reservation = nil
		if r != nil {
			r.Release()
		}
	}
}

// Each binding owns its independent mutable binding graph, its private pending
// node, and a separate reservation. The producer's reference prevents an early
// consumer ACK from refunding metadata while publication still uses the node.
type preparedSharedItem struct {
	next        *preparedSharedItem
	branch      *fanOutBranch
	item        exchangeItem
	refs        atomic.Int64
	reservation memory.Reservation
	owner       *sharedNativeOwner
}

func (p *preparedSharedItem) release() {
	if p.refs.Add(-1) == 0 {
		r, o := p.reservation, p.owner
		p.reservation = nil
		p.owner = nil
		if r != nil {
			r.Release()
		}
		o.release()
	}
}

func sharedImmediateManager(ctx context.Context, manager memory.Manager) memory.ImmediateReservationManager {
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		return q.Operator("PipelineExchange", memory.MemoryInFlightBatch)
	}
	// Only built-in managers are enabled in this experiment. A custom wrapper
	// may inherit the new method accidentally while overriding Reserve.
	switch m := manager.(type) {
	case *memory.BudgetManager:
		return m
	case *memory.EngineBudget:
		return m
	case *memory.QueryBudget:
		return m
	case *memory.OperatorBudget:
		return m
	default:
		return nil
	}
}

// acquireSharedCredit accounts the same conservative pinned footprint as the
// guard path. Shared unique storage is charged separately once to the query.
func (e *Exchange) acquireSharedCredit(ctx context.Context, bytes int64) error {
	if bytes <= 0 || bytes > e.maxBytes || bytes > e.groupMax {
		return fmt.Errorf("invalid shared exchange charge %d", bytes)
	}
	start := time.Now()
	if err := e.groupBytes[0].Acquire(ctx, bytes); err != nil {
		return err
	}
	if err := e.bytes.Acquire(ctx, bytes); err != nil {
		e.groupBytes[0].Release(bytes)
		return err
	}
	e.blockedNanos.Add(int64(time.Since(start)))
	updateMax(&e.peakBytes, e.currentBytes.Add(bytes))
	select {
	case <-e.closed:
		e.releaseSharedCredit(bytes)
		return errExchangeClosed
	default:
		return nil
	}
}
func (e *Exchange) releaseSharedCredit(bytes int64) {
	e.currentBytes.Add(-bytes)
	e.bytes.Release(bytes)
	e.groupBytes[0].Release(bytes)
}

// trySharedRange preserves guard range boundaries. Any Opaque anywhere, unknown
// manager, or immediate admission miss uses the original guard path. Every view
// is prepared before any publication; rollback therefore never duplicates rows.
// No branch sendMu is held while waiting for a different branch.
func (m *boundedMultiplexer) trySharedRange(src *memory.Batch, r MorselRange, seq Sequence) (handled bool, active int, err error) {
	if m.sharedImmediate == nil || m.active.Load() < 2 {
		return false, 0, nil
	}
	cost, err := memory.EstimateNativeSharedBatchRange(src, r.Offset, r.Rows)
	if err != nil {
		return true, 0, err
	}
	if !cost.Supported {
		return false, 0, nil
	}
	if cost.CloneBytes <= 0 || cost.OwnerBytes < 0 || cost.BindingBytes < 0 || cost.OwnerBytes > cost.CloneBytes || cost.BindingBytes != cost.CloneBytes-cost.OwnerBytes {
		return true, 0, fmt.Errorf("invalid native shared cost decomposition")
	}
	if cost.CloneBytes > m.opts.MaxMorselBytes {
		return true, 0, &MorselTooLargeError{Row: r.Offset, Bytes: cost.CloneBytes, LimitBytes: m.opts.MaxMorselBytes}
	}
	n := int64(m.active.Load())
	if n < 2 {
		return false, 0, nil
	}
	if cost.CloneBytes > math.MaxInt64/n || cost.BindingBytes > (math.MaxInt64-cost.OwnerBytes)/n {
		return false, 0, nil
	}
	if cost.OwnerBytes+n*cost.BindingBytes > n*cost.CloneBytes {
		return false, 0, nil
	}
	var head, tail *preparedSharedItem
	var owner *sharedNativeOwner
	var cloned *memory.Batch
	// Clearing each graph and pending reference precedes its ACK. The owner does
	// not keep the first branch's shell, which Next swaps with the caller's shell.
	rollback := func() {
		cloned = nil
		for head != nil {
			p := head
			head = p.next
			p.next = nil
			b := p.branch
			p.branch = nil
			item := p.item
			p.item = exchangeItem{}
			b.exchange.release(item)
		}
		tail = nil
		if owner != nil {
			owner.release()
			owner = nil
		}
	}
	for _, branch := range m.branches {
		if !branch.active.Load() {
			continue
		}
		if e := branch.exchange.acquireSharedCredit(branch.ctx, cost.CloneBytes); e != nil {
			if !branch.active.Load() {
				continue
			}
			rollback()
			return true, 0, e
		}
		if owner == nil {
			res, ok, e := m.sharedImmediate.TryReserveImmediate(m.ctx, cost.OwnerBytes)
			if e != nil || !ok {
				branch.exchange.releaseSharedCredit(cost.CloneBytes)
				rollback()
				if e != nil {
					return true, 0, e
				}
				return false, 0, nil
			}
			owner = &sharedNativeOwner{reservation: res}
			owner.refs.Store(1)
		}
		binding, ok, e := m.sharedImmediate.TryReserveImmediate(m.ctx, cost.BindingBytes)
		if e != nil || !ok {
			branch.exchange.releaseSharedCredit(cost.CloneBytes)
			rollback()
			if e != nil {
				return true, 0, e
			}
			return false, 0, nil
		}
		p := &preparedSharedItem{branch: branch, reservation: binding, owner: owner}
		p.refs.Store(1)
		owner.refs.Add(1)
		p.item = exchangeItem{sequence: seq, bytes: cost.CloneBytes, shared: p}
		if head == nil {
			head = p
		} else {
			tail.next = p
		}
		tail = p
		if cloned == nil {
			cloned, e = memory.CloneBatchRange(src, r.Offset, r.Rows)
			p.item.batch = cloned
		} else {
			p.item.batch, e = memory.BorrowNativeWholeBatch(cloned)
		}
		if e != nil {
			rollback()
			return true, 0, e
		}
	}
	// All future views now own independent maps/header graphs. Drop the producer's
	// alias before a first consumer may ClearColumns or swap its destination.
	cloned = nil
	tail = nil
	for head != nil {
		p := head
		head = p.next
		p.next = nil
		branch := p.branch
		p.branch = nil
		branch.sendMu.Lock()
		if !branch.active.Load() {
			item := p.item
			p.item = exchangeItem{}
			branch.exchange.release(item)
			branch.sendMu.Unlock()
			continue
		}
		active++
		p.refs.Add(1) // delivery reference; producer reference stays until send finishes
		item := p.item
		p.item = exchangeItem{} // detach heap graph reference before publication
		select {
		case branch.exchange.items <- item:
			updateMax(&branch.exchange.queueHigh, int64(len(branch.exchange.items)))
			p.item = exchangeItem{}
			item = exchangeItem{}
			p.release()
		case <-branch.ctx.Done():
			p.item = exchangeItem{}
			branch.exchange.release(item)
			item = exchangeItem{}
			p.release()
			stillActive := branch.active.Load()
			branch.sendMu.Unlock()
			if stillActive {
				rollback()
				return true, active, branch.ctx.Err()
			}
			continue
		case <-branch.exchange.closed:
			p.item = exchangeItem{}
			branch.exchange.release(item)
			item = exchangeItem{}
			p.release()
			stillActive := branch.active.Load()
			branch.sendMu.Unlock()
			if stillActive {
				rollback()
				return true, active, errExchangeClosed
			}
			continue
		}
		branch.sendMu.Unlock()
	}
	if owner != nil {
		owner.release()
	}
	return true, active, nil
}
