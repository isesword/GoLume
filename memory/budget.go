package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// MemoryClass identifies why bytes remain live in a query.
type MemoryClass uint8

const (
	// MemoryRetainedState is operator state retained across input batches.
	MemoryRetainedState MemoryClass = iota
	// MemoryInFlightBatch is an owned batch waiting at an exchange boundary.
	MemoryInFlightBatch
	// MemoryWorkspace is temporary computation scratch space.
	MemoryWorkspace
	// MemoryOutput is materialized result memory owned by the caller or sink.
	MemoryOutput
	// MemorySpillBuffer is temporary encoding or restore I/O memory.
	MemorySpillBuffer
	memoryClassCount
)

// String returns the stable diagnostics spelling for a memory class.
func (c MemoryClass) String() string {
	switch c {
	case MemoryRetainedState:
		return "retained-state"
	case MemoryInFlightBatch:
		return "in-flight-batch"
	case MemoryWorkspace:
		return "workspace"
	case MemoryOutput:
		return "output"
	case MemorySpillBuffer:
		return "spill-buffer"
	default:
		return fmt.Sprintf("memory-class-%d", c)
	}
}

// ResourceError reports a typed memory or spill resource failure.
type ResourceError struct {
	Scope          string
	Operator       string
	Class          MemoryClass
	RequestedBytes int64
	UsedBytes      int64
	LimitBytes     int64
	Hint           string
	Cause          error
}

// Error describes the resource scope, usage, request, limit, and action hint.
func (e *ResourceError) Error() string {
	if e == nil {
		return "resource limit exceeded"
	}
	cause := e.Cause
	if cause == nil {
		cause = ErrBudgetExceeded
	}
	return fmt.Sprintf("%s memory budget exceeded: operator=%s class=%s requested=%d used=%d limit=%d: %v; hint: %s", e.Scope, e.Operator, e.Class, e.RequestedBytes, e.UsedBytes, e.LimitBytes, cause, e.Hint)
}

// Unwrap preserves errors.Is(err, ErrBudgetExceeded) compatibility.
func (e *ResourceError) Unwrap() error {
	if e == nil || e.Cause == nil {
		return ErrBudgetExceeded
	}
	return e.Cause
}

// ErrQueryBudgetClosed reports a reservation attempted after query execution
// stopped accepting new work. Existing output reservations may still release.
var ErrQueryBudgetClosed = errors.New("query memory budget closed")

// BudgetOptions defines one query's advisory soft limit and mandatory hard
// limit. Zero means no limit; a non-zero soft limit must not exceed hard.
type BudgetOptions struct {
	SoftLimitBytes int64
	HardLimitBytes int64
	Spill          SpillOptions
}

// Validate rejects inconsistent or negative query limits.
func (o BudgetOptions) Validate() error {
	if o.SoftLimitBytes < 0 || o.HardLimitBytes < 0 {
		return fmt.Errorf("memory budget limits cannot be negative")
	}
	if o.HardLimitBytes > 0 && o.SoftLimitBytes > o.HardLimitBytes {
		return fmt.Errorf("soft memory limit %d exceeds hard limit %d", o.SoftLimitBytes, o.HardLimitBytes)
	}
	if err := o.Spill.ValidateOptional(); err != nil {
		return err
	}
	return nil
}

// BudgetSnapshot is an atomic diagnostic snapshot of a query budget.
type BudgetSnapshot struct {
	UsedBytes      int64
	PeakBytes      int64
	SoftLimitBytes int64
	HardLimitBytes int64
	ByClass        map[MemoryClass]int64
	Operators      map[string]int64
	SpillRequests  int64
	SpilledBytes   int64
	Closed         bool
}

// EngineBudget is the process/session-level root for query reservations.
// Its hard limit is shared by concurrent QueryBudget children.
type EngineBudget struct {
	root *BudgetManager

	reclaimMu  sync.Mutex
	reclaimers map[*rootReclaimer]struct{}
}

// NewEngineBudget creates a shared engine budget. A non-positive hard limit is
// unlimited, matching BudgetManager compatibility semantics.
func NewEngineBudget(hardLimitBytes int64) *EngineBudget {
	return &EngineBudget{root: NewBudgetManager(hardLimitBytes)}
}

// BeginQuery creates an independent child budget sharing the engine hard cap.
func (e *EngineBudget) BeginQuery(opts BudgetOptions) (*QueryBudget, error) {
	if e == nil || e.root == nil {
		return nil, fmt.Errorf("engine memory budget is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.HardLimitBytes == 0 || e.Limit() > 0 && opts.HardLimitBytes > e.Limit() {
		opts.HardLimitBytes = e.Limit()
	}
	if opts.SoftLimitBytes == 0 && opts.HardLimitBytes > 0 {
		opts.SoftLimitBytes = opts.HardLimitBytes/10*8 + opts.HardLimitBytes%10*8/10
	}
	if opts.SoftLimitBytes > 0 && opts.HardLimitBytes > 0 && opts.SoftLimitBytes > opts.HardLimitBytes {
		opts.SoftLimitBytes = opts.HardLimitBytes
	}
	q := &QueryBudget{engine: e, soft: opts.SoftLimitBytes, hard: opts.HardLimitBytes, spillOptions: opts.Spill, operators: make(map[string]*OperatorBudget)}
	q.arbiter = newSpillArbiter(q)
	return q, nil
}

// Reserve implements Manager. When ctx contains a child query budget from the
// same engine, the reservation is routed through that query; otherwise it is
// charged directly to retained engine state for compatibility.
func (e *EngineBudget) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if q := QueryBudgetFromContext(ctx); q != nil && q.engine == e {
		return q.reserve(ctx, "unclassified", MemoryWorkspace, nil, bytes)
	}
	reservation, err := e.reserveRoot(ctx, bytes)
	if errors.Is(err, ErrBudgetExceeded) {
		e.reclaim(ctx, bytes)
		return e.reserveRoot(ctx, bytes)
	}
	return reservation, err
}

func (e *EngineBudget) reserveRoot(ctx context.Context, bytes int64) (Reservation, error) {
	if e == nil || e.root == nil {
		return nil, fmt.Errorf("engine memory budget is nil")
	}
	return e.root.Reserve(ctx, bytes)
}

// Used returns bytes currently charged across the engine and all children.
func (e *EngineBudget) Used() int64 {
	if e == nil || e.root == nil {
		return 0
	}
	return e.root.Used()
}

// Limit returns the engine hard limit; non-positive means unlimited.
func (e *EngineBudget) Limit() int64 {
	if e == nil || e.root == nil {
		return 0
	}
	return e.root.Limit()
}

type queryBudgetContextKey struct{}
type spillArbitrationContextKey struct{}
type spillCallbackContextKey struct{}

// WithQueryBudget binds budget to ctx for existing Manager call sites.
func WithQueryBudget(ctx context.Context, budget *QueryBudget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, queryBudgetContextKey{}, budget)
}

// QueryBudgetFromContext returns the bound query budget, if any.
func QueryBudgetFromContext(ctx context.Context) *QueryBudget {
	if ctx == nil {
		return nil
	}
	budget, _ := ctx.Value(queryBudgetContextKey{}).(*QueryBudget)
	return budget
}

func withSpillArbitration(ctx context.Context) context.Context {
	return context.WithValue(ctx, spillArbitrationContextKey{}, true)
}

// withSpillCallback distinguishes callbacks holding arbiter.mu from standalone
// growth reservations, which suppress recursive spill but may reclaim idle root
// state. Root reclaim callbacks must never inherit a held spill arbiter lock.
func withSpillCallback(ctx context.Context) context.Context {
	return context.WithValue(withSpillArbitration(ctx), spillCallbackContextKey{}, true)
}

func spillArbitrationActive(ctx context.Context) bool {
	active, _ := ctx.Value(spillArbitrationContextKey{}).(bool)
	return active
}

// QueryBudget is one execution's hard/soft limit, class accounting, operator
// accounting, and serialized spill arbitration domain.
type QueryBudget struct {
	engine       *EngineBudget
	soft         int64
	hard         int64
	spillOptions SpillOptions

	reserveMu sync.Mutex
	used      atomic.Int64
	peak      atomic.Int64
	classes   [memoryClassCount]atomic.Int64
	closed    atomic.Bool

	operatorsMu sync.Mutex
	operators   map[string]*OperatorBudget
	arbiter     *spillArbiter

	spillMu    sync.Mutex
	spillStore SpillManager
	closeOnce  sync.Once
	closeErr   error
}

// RecordSpilledBytes adds bytes written by a physical algorithm that performs
// planned external partitioning rather than reacting to arbiter pressure.
// Negative and zero values are ignored.
func (q *QueryBudget) RecordSpilledBytes(bytes int64) {
	if q == nil || q.arbiter == nil || bytes <= 0 {
		return
	}
	q.arbiter.spilled.Add(bytes)
}

// Operator returns a stable per-query child budget. The same name/class pair
// reuses its counters and spill registration identity.
func (q *QueryBudget) Operator(name string, class MemoryClass) *OperatorBudget {
	if q == nil {
		return nil
	}
	if name == "" {
		name = "unclassified"
	}
	key := fmt.Sprintf("%s\x00%d", name, class)
	q.operatorsMu.Lock()
	defer q.operatorsMu.Unlock()
	if existing := q.operators[key]; existing != nil {
		return existing
	}
	op := &OperatorBudget{query: q, name: name, defaultClass: class}
	q.operators[key] = op
	return op
}

// Reserve implements Manager using the query's unclassified workspace child.
func (q *QueryBudget) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	return q.reserve(ctx, "unclassified", MemoryWorkspace, nil, bytes)
}

// Used returns bytes live in this query, including output surviving execution.
func (q *QueryBudget) Used() int64 {
	if q == nil {
		return 0
	}
	return q.used.Load()
}

// Limit returns this query's hard limit; zero means unlimited.
func (q *QueryBudget) Limit() int64 {
	if q == nil {
		return 0
	}
	return q.hard
}

// SoftLimit returns the advisory spill threshold; zero disables proactive spill.
func (q *QueryBudget) SoftLimit() int64 {
	if q == nil {
		return 0
	}
	return q.soft
}

// SpillConfigured reports whether this query has an explicit finite disk
// quota. It does not create the query spill directory.
func (q *QueryBudget) SpillConfigured() bool {
	return q != nil && q.spillOptions.Enabled()
}

// SpillManager returns the query-scoped spill store, creating its secure
// temporary directory lazily on first use. A disabled configuration returns a
// NoSpill manager without filesystem side effects.
func (q *QueryBudget) SpillManager() (SpillManager, error) {
	if q == nil {
		return nil, fmt.Errorf("query memory budget is nil")
	}
	if q.closed.Load() {
		return nil, ErrQueryBudgetClosed
	}
	if !q.spillOptions.Enabled() {
		return NoSpill{}, nil
	}
	q.spillMu.Lock()
	defer q.spillMu.Unlock()
	if q.closed.Load() {
		return nil, ErrQueryBudgetClosed
	}
	if q.spillStore != nil {
		return q.spillStore, nil
	}
	store, err := NewQuerySpillStore(q.spillOptions)
	if err != nil {
		return nil, err
	}
	q.spillStore = store
	return store, nil
}

// Close prevents new reservations. It does not invalidate live output;
// existing reservations remain releasable by DataFrame.Close or sink cleanup.
// Any lazily created spill store is closed and its query directory removed.
func (q *QueryBudget) Close() error {
	if q == nil {
		return nil
	}
	q.closeOnce.Do(func() {
		q.reserveMu.Lock()
		q.closed.Store(true)
		q.reserveMu.Unlock()
		q.spillMu.Lock()
		defer q.spillMu.Unlock()
		if q.spillStore != nil {
			q.closeErr = q.spillStore.Close()
			q.spillStore = nil
		}
	})
	return q.closeErr
}

// Snapshot returns class/operator usage and spill arbitration counters.
func (q *QueryBudget) Snapshot() BudgetSnapshot {
	out := BudgetSnapshot{ByClass: make(map[MemoryClass]int64), Operators: make(map[string]int64)}
	if q == nil {
		return out
	}
	out.UsedBytes, out.PeakBytes, out.SoftLimitBytes, out.HardLimitBytes, out.Closed = q.Used(), q.peak.Load(), q.soft, q.hard, q.closed.Load()
	for class := MemoryClass(0); class < memoryClassCount; class++ {
		out.ByClass[class] = q.classes[class].Load()
	}
	q.operatorsMu.Lock()
	for _, op := range q.operators {
		out.Operators[op.name] += op.used.Load()
	}
	q.operatorsMu.Unlock()
	if q.arbiter != nil {
		out.SpillRequests = q.arbiter.requests.Load()
		out.SpilledBytes = q.arbiter.spilled.Load()
	}
	return out
}

func (q *QueryBudget) reserve(ctx context.Context, operator string, class MemoryClass, op *OperatorBudget, bytes int64) (Reservation, error) {
	if q == nil || q.engine == nil {
		return nil, fmt.Errorf("query memory budget is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("memory reservation context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if class >= memoryClassCount {
		return nil, fmt.Errorf("invalid memory class %d", class)
	}
	if bytes <= 0 {
		return &hierarchicalReservation{}, nil
	}
	if q.closed.Load() {
		return nil, ErrQueryBudgetClosed
	}
	inSpill := spillArbitrationActive(ctx)
	if !inSpill && q.soft > 0 && bytes > q.soft-q.Used() {
		_, _ = q.arbiter.spillTo(ctx, maxInt64(0, q.soft-bytes), SpillForSoftLimit, operator, class)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if !inSpill && q.hard > 0 && bytes > q.hard-q.Used() {
			_, spillErr := q.arbiter.spillTo(ctx, maxInt64(0, q.hard-bytes), SpillForHardLimit, operator, class)
			if spillErr != nil {
				return nil, spillErr
			}
		}
		q.reserveMu.Lock()
		used := q.used.Load()
		if q.closed.Load() {
			q.reserveMu.Unlock()
			return nil, ErrQueryBudgetClosed
		}
		if q.hard > 0 && bytes > q.hard-used {
			q.reserveMu.Unlock()
			return nil, newMemoryResourceError("query", operator, class, bytes, used, q.hard, nil)
		}
		rootReservation, err := q.engine.reserveRoot(ctx, bytes)
		if err == nil {
			usedNow := q.used.Add(bytes)
			for peak := q.peak.Load(); usedNow > peak && !q.peak.CompareAndSwap(peak, usedNow); peak = q.peak.Load() {
			}
			q.classes[class].Add(bytes)
			if op != nil {
				op.used.Add(bytes)
			}
			q.reserveMu.Unlock()
			return &hierarchicalReservation{bytes: bytes, root: rootReservation, query: q, operator: op, class: class}, nil
		}
		q.reserveMu.Unlock()
		if !errors.Is(err, ErrBudgetExceeded) || attempt == 1 {
			return nil, newMemoryResourceError("engine", operator, class, bytes, q.engine.Used(), q.engine.Limit(), err)
		}
		// Root reclamation must never run under reserveMu: a cache callback
		// may detach or release state owned by any query on this root.
		q.engine.reclaim(ctx, bytes)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !inSpill && q.engine.rootShortfall(bytes) > 0 {
			if _, spillErr := q.arbiter.spillTo(ctx, maxInt64(0, q.Used()-bytes), SpillForEngineLimit, operator, class); spillErr != nil {
				return nil, spillErr
			}
		}
	}
	panic("unreachable query reservation retry")
}

func newMemoryResourceError(scope, operator string, class MemoryClass, requested, used, limit int64, cause error) error {
	if cause == nil {
		cause = ErrBudgetExceeded
	}
	hint := "increase the memory limit or enable a spillable physical operator"
	if class == MemoryOutput {
		hint = "use CollectBatches or a native file sink, or increase the memory limit"
	}
	return &ResourceError{Scope: scope, Operator: operator, Class: class, RequestedBytes: requested, UsedBytes: used, LimitBytes: limit, Hint: hint, Cause: cause}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type hierarchicalReservation struct {
	bytes    int64
	root     Reservation
	query    *QueryBudget
	operator *OperatorBudget
	class    MemoryClass
	done     atomic.Bool
}

// Bytes returns the charge released across every hierarchy level.
func (r *hierarchicalReservation) Bytes() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

// Release idempotently returns the charge to the operator, query, and engine.
func (r *hierarchicalReservation) Release() {
	if r == nil || !r.done.CompareAndSwap(false, true) {
		return
	}
	if r.operator != nil {
		r.operator.used.Add(-r.bytes)
	}
	if r.query != nil {
		r.query.classes[r.class].Add(-r.bytes)
		r.query.used.Add(-r.bytes)
	}
	if r.root != nil {
		r.root.Release()
	}
}

// OperatorBudget is one named child within a QueryBudget.
type OperatorBudget struct {
	query        *QueryBudget
	name         string
	defaultClass MemoryClass
	used         atomic.Int64
}

// Reserve implements Manager using the operator's default memory class.
func (o *OperatorBudget) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if o == nil || o.query == nil {
		return nil, fmt.Errorf("operator memory budget is nil")
	}
	return o.query.reserve(ctx, o.name, o.defaultClass, o, bytes)
}

// ReserveClass charges a non-default class while retaining this operator name.
func (o *OperatorBudget) ReserveClass(ctx context.Context, class MemoryClass, bytes int64) (Reservation, error) {
	if o == nil || o.query == nil {
		return nil, fmt.Errorf("operator memory budget is nil")
	}
	return o.query.reserve(ctx, o.name, class, o, bytes)
}

// ReserveGrowth charges retained growth without recursively invoking the soft
// spill arbiter from inside a data-structure mutation. The hard limit remains
// mandatory. Spillable operators must call RequestSoftSpill at a safe boundary
// and retry a failed growth only after their Spill callback has completed.
func (o *OperatorBudget) ReserveGrowth(ctx context.Context, bytes int64) (Reservation, error) {
	if o == nil || o.query == nil {
		return nil, fmt.Errorf("operator memory budget is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("memory reservation context is nil")
	}
	return o.query.reserve(withSpillArbitration(ctx), o.name, o.defaultClass, o, bytes)
}

// RequestSoftSpill asks the query arbiter to reduce live bytes to its soft
// limit. It is intended for batch/morsel boundaries, never row hot loops.
func (o *OperatorBudget) RequestSoftSpill(ctx context.Context) (int64, error) {
	if o == nil || o.query == nil {
		return 0, fmt.Errorf("operator memory budget is nil")
	}
	if ctx == nil {
		return 0, fmt.Errorf("spill request context is nil")
	}
	if o.query.soft <= 0 || o.query.Used() <= o.query.soft {
		return 0, nil
	}
	return o.query.arbiter.spillTo(ctx, o.query.soft, SpillForSoftLimit, o.name, o.defaultClass)
}

// RequestHardSpill asks the query arbiter to release at least targetBytes so a
// failed retained growth can be retried outside the mutating data structure.
func (o *OperatorBudget) RequestHardSpill(ctx context.Context, targetBytes int64) (int64, error) {
	if o == nil || o.query == nil {
		return 0, fmt.Errorf("operator memory budget is nil")
	}
	if ctx == nil {
		return 0, fmt.Errorf("spill request context is nil")
	}
	if targetBytes < 1 {
		targetBytes = 1
	}
	return o.query.arbiter.spillTo(ctx, maxInt64(0, o.query.Used()-targetBytes), SpillForHardLimit, o.name, o.defaultClass)
}

// Used returns this operator child's live bytes.
func (o *OperatorBudget) Used() int64 {
	if o == nil {
		return 0
	}
	return o.used.Load()
}

// Limit returns the owning query hard limit.
func (o *OperatorBudget) Limit() int64 {
	if o == nil || o.query == nil {
		return 0
	}
	return o.query.Limit()
}

// RegisterSpiller makes a spillable consumer available to serialized budget
// arbitration. The returned function unregisters it idempotently.
func (o *OperatorBudget) RegisterSpiller(spiller SpillableConsumer) (func(), error) {
	if o == nil || o.query == nil {
		return nil, fmt.Errorf("operator memory budget is nil")
	}
	return o.query.arbiter.register(o.name, spiller)
}

// ClassifiedManager assigns an operator and class to reservations while
// preserving the existing Manager interface used by physical operators.
func ClassifiedManager(base Manager, operator string, class MemoryClass) Manager {
	if base == nil {
		return nil
	}
	return &classifiedManager{base: base, operator: operator, class: class}
}

type classifiedManager struct {
	base     Manager
	operator string
	class    MemoryClass
}

// Reserve routes a charge through the bound query when one is in ctx.
func (m *classifiedManager) Reserve(ctx context.Context, bytes int64) (Reservation, error) {
	if q := QueryBudgetFromContext(ctx); q != nil {
		return q.Operator(m.operator, m.class).Reserve(ctx, bytes)
	}
	return m.base.Reserve(ctx, bytes)
}

// Used returns usage reported by the compatibility base manager.
func (m *classifiedManager) Used() int64 { return m.base.Used() }

// Limit returns the hard limit reported by the compatibility base manager.
func (m *classifiedManager) Limit() int64 { return m.base.Limit() }

// SpillReason identifies the pressure threshold requesting reclamation.
type SpillReason uint8

const (
	// SpillForSoftLimit proactively returns usage below the query soft limit.
	SpillForSoftLimit SpillReason = iota
	// SpillForHardLimit is required for a query reservation to make progress.
	SpillForHardLimit
	// SpillForEngineLimit addresses contention at the shared engine root.
	SpillForEngineLimit
)

// SpillRequest is emitted only by query memory arbitration.
type SpillRequest struct {
	Reason      SpillReason
	Operator    string
	Class       MemoryClass
	TargetBytes int64
	UsedBytes   int64
	SoftLimit   int64
	HardLimit   int64
}

// SpillableConsumer releases retained reservations by persisting cold state.
// SpillableBytes is an estimate; accounting uses actual reservation releases.
type SpillableConsumer interface {
	SpillableBytes() int64
	Spill(context.Context, SpillRequest) (releasedBytes int64, err error)
}

type namedSpiller struct {
	name    string
	spiller SpillableConsumer
}

type spillArbiter struct {
	query *QueryBudget
	mu    sync.Mutex
	items map[string]SpillableConsumer

	requests atomic.Int64
	spilled  atomic.Int64
}

func newSpillArbiter(query *QueryBudget) *spillArbiter {
	return &spillArbiter{query: query, items: make(map[string]SpillableConsumer)}
}

func (a *spillArbiter) register(name string, spiller SpillableConsumer) (func(), error) {
	if spiller == nil {
		return nil, fmt.Errorf("spillable consumer is nil")
	}
	a.mu.Lock()
	if _, exists := a.items[name]; exists {
		a.mu.Unlock()
		return nil, fmt.Errorf("spillable consumer %q already registered", name)
	}
	a.items[name] = spiller
	a.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			delete(a.items, name)
			a.mu.Unlock()
		})
	}, nil
}

func (a *spillArbiter) spillTo(ctx context.Context, targetUsed int64, reason SpillReason, operator string, class MemoryClass) (int64, error) {
	if a == nil || a.query == nil || a.query.Used() <= targetUsed {
		return 0, nil
	}
	// One mutex covers selection and calls. Concurrent pressure waits, then
	// observes the first request's released bytes instead of spilling twice.
	a.mu.Lock()
	defer a.mu.Unlock()
	before := a.query.Used()
	if before <= targetUsed {
		return 0, nil
	}
	items := make([]namedSpiller, 0, len(a.items))
	for name, spiller := range a.items {
		items = append(items, namedSpiller{name: name, spiller: spiller})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].spiller.SpillableBytes() > items[j].spiller.SpillableBytes() })
	if len(items) == 0 {
		return 0, nil
	}
	a.requests.Add(1)
	for _, item := range items {
		current := a.query.Used()
		if current <= targetUsed {
			break
		}
		request := SpillRequest{Reason: reason, Operator: item.name, Class: class, TargetBytes: current - targetUsed, UsedBytes: current, SoftLimit: a.query.soft, HardLimit: a.query.hard}
		if _, err := item.spiller.Spill(withSpillCallback(ctx), request); err != nil {
			return before - a.query.Used(), fmt.Errorf("spill operator %s: %w", item.name, err)
		}
	}
	released := before - a.query.Used()
	if released > 0 {
		a.spilled.Add(released)
	}
	return released, nil
}

var _ Manager = (*EngineBudget)(nil)
var _ Manager = (*QueryBudget)(nil)
var _ Manager = (*OperatorBudget)(nil)
var _ Manager = (*classifiedManager)(nil)
