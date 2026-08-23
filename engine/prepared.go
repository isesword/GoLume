package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/planner"
	"gocolumn/source"
	"sync"
	"time"
)

// ErrPreparedClosed is returned when executing a closed PreparedQuery.
var ErrPreparedClosed = errors.New("prepared query closed")

// PrepareOptions controls retained workspace concurrency for a prepared plan.
type PrepareOptions struct {
	// MaxConcurrency bounds simultaneous Execute calls and therefore retained
	// workspace/memory reservations. Zero means 1.
	MaxConcurrency int
	// FeedbackKey identifies a stable logical plan for runtime-statistics
	// feedback. Empty disables feedback for this prepared query.
	FeedbackKey string
}

// PlanInfo exposes stable physical-plan diagnostics without leaking internal state.
type PlanInfo struct {
	Strategy             planner.Strategy
	Rows                 int64
	Partitions           int
	KeyWidthBytes        int
	PredicateOps         int
	GroupColumns         int
	WorkspacePool        int
	FeedbackSamples      uint64
	EstimatedSelectivity float64
	EstimatedCardinality int
}

// PreparedQuery is a compiled physical plan with reusable, budget-accounted workspaces.
type PreparedQuery struct {
	e               *Engine
	src             source.Source
	cq              CompiledQuery
	stats           source.Stats
	parts           []source.Partition
	strategy        planner.Strategy
	pool            *workspacePool
	partBase        []int
	maxPartRows     int
	dicts           map[string]source.Dictionary
	feedbackKey     string
	feedbackSamples uint64

	lifeMu sync.Mutex
	active sync.WaitGroup
	closed bool
}

// Prepare compiles q for src and creates a reusable PreparedQuery.
func (e *Engine) Prepare(ctx context.Context, src source.Source, q expr.Query, opts PrepareOptions) (*PreparedQuery, error) {
	if src == nil {
		return nil, fmt.Errorf("source is nil")
	}
	feedbackSamples := uint64(0)
	if fb, ok := e.RuntimeFeedback(opts.FeedbackKey); ok {
		feedbackSamples = fb.Samples
		if q.Hints.Selectivity <= 0 && fb.Selectivity > 0 && fb.Selectivity <= 1 {
			q.Hints.Selectivity = fb.Selectivity
		}
		if q.Hints.Cardinality <= 0 && fb.Cardinality > 0 {
			q.Hints.Cardinality = fb.Cardinality
		}
	}
	cq, err := compileQuery(src.Schema(), q)
	if err != nil {
		return nil, err
	}
	stats, err := src.Stats(ctx)
	if err != nil {
		return nil, err
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	workers := e.opts.Workers
	if q.Hints.ForceWorkerCount > 0 {
		workers = q.Hints.ForceWorkerCount
	}
	if workers > e.scheduler.MaxWorkers() {
		workers = e.scheduler.MaxWorkers()
	}
	strategy, err := e.opts.CostModel.Choose(planner.Input{
		Rows: stats.Rows, Partitions: stats.Partitions, Workers: workers,
		Selectivity: cq.SelectivityHint, Cardinality: cq.CardinalityHint,
		ReuseSelection: cq.ReuseSelection, Kernel: e.kernels.Caps,
		ForceStrategy: cq.ForceStrategy, ForceRadixBits: cq.ForceRadixBits,
		FusedCapable: cq.FusedCapable, RadixCapable: cq.RadixCapable,
		KeyWidthBytes: cq.KeyWidthBytes, PredicateComplexity: cq.Predicate.Complexity,
	})
	if err != nil {
		return nil, err
	}
	if stats.Rows < 0 || stats.Rows > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("row count %d exceeds addressable workspace", stats.Rows)
	}
	partBase := make([]int, len(parts)+1)
	maxPart := 0
	for i, p := range parts {
		if p.Rows < 0 || p.Rows > int64(^uint(0)>>1) {
			return nil, fmt.Errorf("partition %d too large", p.ID)
		}
		partBase[i+1] = partBase[i] + int(p.Rows)
		if int(p.Rows) > maxPart {
			maxPart = int(p.Rows)
		}
	}
	maxConc := opts.MaxConcurrency
	if maxConc < 1 {
		maxConc = 1
	}
	pq := &PreparedQuery{e: e, src: src, cq: cq, stats: stats, parts: parts, strategy: strategy, partBase: partBase, maxPartRows: maxPart, dicts: make(map[string]source.Dictionary), feedbackKey: opts.FeedbackKey, feedbackSamples: feedbackSamples}
	// Load source-level dictionaries once when available. Absence is allowed;
	// materialized results will expose code+domain instead of decoded strings.
	if dp, ok := src.(source.DictionaryProvider); ok {
		for _, g := range cq.GroupBy {
			if g.Type == memory.TypeString && g.Encoding == memory.EncodingDictionary {
				d, derr := dp.Dictionary(ctx, g.Name)
				if derr != nil {
					return nil, derr
				}
				if d.Domain != g.DictionaryDomain {
					return nil, fmt.Errorf("dictionary %q domain=%q want=%q", g.Name, d.Domain, g.DictionaryDomain)
				}
				pq.dicts[g.Name] = d
			}
		}
	}
	pq.pool = newWorkspacePool(maxConc, pq.newWorkspace)
	return pq, nil
}

// Plan returns physical-plan diagnostics selected during preparation.
func (p *PreparedQuery) Plan() PlanInfo {
	return PlanInfo{Strategy: p.strategy, Rows: p.stats.Rows, Partitions: p.stats.Partitions, KeyWidthBytes: p.cq.KeyWidthBytes, PredicateOps: p.cq.Predicate.Complexity, GroupColumns: len(p.cq.GroupBy), WorkspacePool: p.pool.max, FeedbackSamples: p.feedbackSamples, EstimatedSelectivity: p.cq.SelectivityHint, EstimatedCardinality: p.cq.CardinalityHint}
}

func (p *PreparedQuery) begin() error {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.closed {
		return ErrPreparedClosed
	}
	p.active.Add(1)
	return nil
}
func (p *PreparedQuery) end() { p.active.Done() }

type profileOperatorIDProvider interface{ OperatorID() string }

// Execute runs the prepared physical plan using one leased workspace.
func (p *PreparedQuery) Execute(ctx context.Context, opts ExecuteOptions) (Result, error) {
	if err := p.begin(); err != nil {
		return Result{}, err
	}
	defer p.end()
	start := time.Now()
	ws, err := p.pool.acquire(ctx)
	if err != nil {
		return Result{}, err
	}
	defer p.pool.release(ws)
	var r Result
	switch p.strategy.Kind {
	case planner.StrategyDirectFused:
		r, err = p.executeDirectFused(ctx, ws, opts)
	case planner.StrategyBitmapDirect:
		r, err = p.executeBitmapDirect(ctx, ws, opts)
	case planner.StrategyRadix:
		r, err = p.executeRadix(ctx, ws, opts)
	default:
		err = fmt.Errorf("unsupported physical strategy %v", p.strategy.Kind)
	}
	if err != nil {
		return Result{}, err
	}
	r.Strategy = p.strategy
	r.Elapsed = time.Since(start)
	if p.feedbackKey != "" {
		p.e.feedback.observe(p.feedbackKey, p.stats.Rows, r.SelectedRows, r.GroupCount)
	}
	p.e.opts.Observer.Observe(observer.Event{Stage: "prepared-query", Duration: r.Elapsed, Rows: p.stats.Rows, Bytes: p.stats.Bytes, Workers: p.strategy.Workers, Attributes: map[string]string{"strategy": p.strategy.Kind.String(), "reason": p.strategy.Reason}})
	if trace := observer.TraceFromContext(ctx); trace != nil {
		children := []string(nil)
		if profiled, ok := p.src.(profileOperatorIDProvider); ok && profiled.OperatorID() != "" {
			children = append(children, profiled.OperatorID())
		}
		label := p.strategy.Kind.String()
		if p.cq.Predicate.Complexity > 0 {
			label += "+predicate-fused"
		}
		node := trace.NewNode("aggregate", label, children...)
		trace.Record(node.ID, observer.PhaseFromContext(ctx), p.stats.Rows, int64(r.GroupCount), r.Elapsed)
	}
	return r, nil
}

// Close waits for active executions and releases all retained workspaces.
func (p *PreparedQuery) Close() error {
	p.lifeMu.Lock()
	if p.closed {
		p.lifeMu.Unlock()
		return nil
	}
	p.closed = true
	p.lifeMu.Unlock()
	p.active.Wait()
	return p.pool.close()
}

// createdWorkspaceCount is used by allocation/reuse tests and diagnostics.
func (p *PreparedQuery) createdWorkspaceCount() int { return int(p.pool.created.Load()) }
