package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/planner"
)

type residentGrowthManager interface {
	memory.Manager
	ReserveGrowth(context.Context, int64) (memory.Reservation, error)
}

// The lane is stable for the table's lifetime. No reservation callback closes
// over an execution context: bind replaces it, and detach clears it after join.
type residentAllocationLane struct {
	account *memory.ReusableAccount
	ctx     context.Context
}

// Reserve charges a new allocation to the currently attached account.
func (l *residentAllocationLane) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	return l.account.Reserve(ctx, n)
}

// ReserveGrowth uses callback-free account growth with idle-root reclamation.
func (l *residentAllocationLane) ReserveGrowth(ctx context.Context, n int64) (memory.Reservation, error) {
	return l.account.Reserve(ctx, n)
}

// Used reports the lane's retained capacity while active or idle.
func (l *residentAllocationLane) Used() int64 { return l.account.Used() }

// Limit reports the current query cap, or root cap while detached.
func (l *residentAllocationLane) Limit() int64 { return l.account.Limit() }
func (l *residentAllocationLane) reserveCurrent(n int64) (memory.Reservation, error) {
	if l.ctx == nil {
		return nil, fmt.Errorf("resident allocation lane is idle")
	}
	return l.account.Reserve(l.ctx, n)
}

type residentReuseUnit struct {
	planID           uint64
	handle           *preparedCacheHandle
	kind             planner.Kind
	state, workspace residentAllocationLane
	table            *aggregateTableOwner
	partitioned      *residentPartitionedState
	prev, next       *residentReuseUnit
	credit           int64
}

func newResidentReuseUnit(ctx context.Context, p *PreparedQuery, q *memory.QueryBudget) (u *residentReuseUnit, err error) {
	u = &residentReuseUnit{planID: p.cacheHandle.id, handle: p.cacheHandle, kind: p.strategy.Kind}
	defer func() {
		if err != nil {
			u.destroy()
		}
	}()
	u.state.account, err = p.e.memory.NewReusableAccount()
	if err != nil {
		return nil, err
	}
	u.workspace.account, err = p.e.memory.NewReusableAccount()
	if err != nil {
		return u, err
	}
	if err = u.bind(ctx, p, q); err != nil {
		return u, err
	}
	if p.strategy.Kind == planner.StrategyPartitionedHash {
		u.partitioned, err = newResidentPartitionedStateOwned(ctx, p, q, &u.state, &u.workspace)
	} else {
		u.table, err = newResidentTableOwner(ctx, p, &u.state, p.cq.CardinalityHint)
	}
	return u, err
}
func (u *residentReuseUnit) bytes() int64 {
	if u == nil {
		return 0
	}
	return u.state.Used() + u.workspace.Used()
}
func (u *residentReuseUnit) bind(ctx context.Context, p *PreparedQuery, q *memory.QueryBudget) error {
	stateName, workspaceName := "ResidentHashAggregate", "ResidentAggregateWorkspace"
	if u.kind == planner.StrategyPartitionedHash {
		stateName = "ResidentPartitionedHashAggregate"
		workspaceName = "ResidentAggregatePartitionWorkspace"
	}
	if err := u.state.account.Attach(ctx, q.Operator(stateName, memory.MemoryRetainedState), memory.MemoryRetainedState); err != nil {
		return err
	}
	if err := u.workspace.account.Attach(ctx, q.Operator(workspaceName, memory.MemoryWorkspace), memory.MemoryWorkspace); err != nil {
		return err
	}
	u.state.ctx, u.workspace.ctx = ctx, ctx
	if u.partitioned != nil {
		u.partitioned.query = p
		u.partitioned.budget = q
		u.partitioned.stateBudget = &u.state
		u.partitioned.workspace = &u.workspace
	}
	return nil
}
func (u *residentReuseUnit) detach() error {
	if u.partitioned != nil {
		u.partitioned.reset()
		// In particular, do not extend the source lifetime through PreparedQuery.
		u.partitioned.query = nil
		u.partitioned.budget = nil
	}
	if u.table != nil {
		u.table.table.reset()
	}
	u.state.ctx, u.workspace.ctx = nil, nil
	return errors.Join(u.state.account.Detach(), u.workspace.account.Detach())
}
func (u *residentReuseUnit) destroy() {
	if u == nil {
		return
	}
	if u.partitioned != nil {
		u.partitioned.close()
		u.partitioned = nil
	}
	if u.table != nil {
		u.table.close()
		u.table = nil
	}
	u.state.ctx, u.workspace.ctx = nil, nil
	if u.state.account != nil {
		_ = u.state.account.Close()
	}
	if u.workspace.account != nil {
		_ = u.workspace.account.Close()
	}
	u.prev, u.next = nil, nil
}

func (t *aggregateStateTable) reset() {
	switch t.shape {
	case GroupShapeI32:
		t.i32.Reset()
	case GroupShapeI64:
		t.u64.Reset()
	case GroupShapeU32, GroupShapeDictionaryString:
		t.u32.Reset()
	case GroupShapePairFixed:
		t.pair.Reset()
	case GroupShapeStringView:
		t.str.Reset()
	}
}
func (s *residentPartitionedState) reset() {
	for _, owner := range s.owners {
		if owner != nil {
			owner.table.reset()
		}
	}
	clear(s.counts)
	clear(s.offsets)
	clear(s.positions)
	clear(s.checksums)
	s.outputPartition, s.outputCursor = 0, 0
	s.parallelClassified = false
	if s.wave != nil {
		s.wave.rows = 0
		if s.wave.keyNulls {
			clear(s.wave.keyValidity)
		}
		if s.wave.valueNulls {
			clear(s.wave.valueValidity)
		}
		s.wave.keyNulls, s.wave.valueNulls = false, false
	}
	s.keyNulls, s.valueNulls = false, false
}

func (p *PreparedQuery) reuseEligible() bool {
	if p.cq.MultiAggregate != nil || p.cacheHandle == nil || !p.e.cache.enabled() {
		return false
	}
	// Only entirely owned, fixed-width group keys enter the initial cache.
	// Dictionary/string results and their domain/arena lifetimes stay execution-local.
	for _, field := range p.cq.GroupBy {
		if field.Type == memory.TypeString {
			return false
		}
	}
	switch p.cq.GroupShape {
	case GroupShapeI32, GroupShapeI64, GroupShapeU32, GroupShapePairFixed:
		return true
	}
	return false
}
