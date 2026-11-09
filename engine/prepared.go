package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source"
	"sync"
)

// ErrPreparedClosed is returned when executing a closed PreparedQuery.
var ErrPreparedClosed = errors.New("prepared query closed")

// PrepareOptions controls execution concurrency for a prepared plan.
type PrepareOptions struct {
	// MaxConcurrency bounds simultaneous Execute calls. Zero means 1.
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
	MaxConcurrency       int
	FeedbackSamples      uint64
	EstimatedSelectivity float64
	EstimatedCardinality int
}

// PreparedQuery is an immutable compiled plan. Each execution owns its native
// streams and query-scoped memory. Eligible fixed-width resident state and
// bounded scratch can remain in the engine's charged, reclaimable cache; source
// batches and output buffers never survive as cached execution state.
type PreparedQuery struct {
	e   *Engine
	src source.Source
	cq  CompiledQuery
	// Internal Execute consumers share this immutable output descriptor.
	// Public stream construction still receives an independent caller schema.
	executionSchema memory.Schema
	stats           source.Stats
	parts           []source.Partition
	strategy        planner.Strategy
	executions      chan struct{}
	dicts           map[string]source.Dictionary
	feedbackKey     string
	feedbackSamples uint64
	executionPolicy ExecutionPolicy

	cacheHandle *preparedCacheHandle
	lifeMu      sync.Mutex
	active      sync.WaitGroup
	closed      bool
	closeDone   chan struct{}
}

// Prepare compiles q for src and creates a reusable PreparedQuery.
func (e *Engine) Prepare(ctx context.Context, src source.Source, q expr.Query, opts PrepareOptions) (*PreparedQuery, error) {
	return e.prepare(ctx, src, q, opts, nil)
}

func (e *Engine) prepare(ctx context.Context, src source.Source, q expr.Query, opts PrepareOptions, quantiles []expr.QuantileOption) (*PreparedQuery, error) {
	if e == nil {
		return nil, fmt.Errorf("engine is nil")
	}
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if e.opts.PreparedCacheBytes < 0 {
		return nil, fmt.Errorf("prepared cache limit must be nonnegative")
	}
	if ctx == nil {
		return nil, fmt.Errorf("prepare context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	policy := ExecutionPolicyFromContext(ctx, e.opts.ExecutionPolicy)
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := e.opts.Pipeline.Validate(); err != nil {
		return nil, err
	}
	if policy == ExecutionBounded {
		if e.memory.Limit() <= 0 {
			return nil, ErrBoundedMemoryLimitRequired
		}
	}
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
	if q.Hints.ForceStrategy == "radix" {
		return nil, fmt.Errorf("input-wide radix execution was removed; use automatic native stream planning or bitmap-direct")
	}
	cq, err := compileConfiguredQuery(src.Schema(), q, quantiles)
	if err != nil {
		return nil, err
	}
	if policy == ExecutionBounded && cq.MultiAggregate != nil && cq.MultiAggregate.Quantiles != nil && !e.SpillConfigured() {
		return nil, &UnsupportedBoundedPlanError{Operator: "Quantile", Path: []string{"Aggregate", "Quantile"}, Detail: "exact quantiles require spill in bounded mode; group ordering does not bound a group's values", Cause: ErrBoundedSpillRequired}
	}
	stats, err := src.Stats(ctx)
	if err != nil {
		return nil, err
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		if part.Rows < 0 {
			return nil, fmt.Errorf("partition %d has negative row count %d", part.ID, part.Rows)
		}
	}
	workers := e.opts.Workers

	if workers > e.scheduler.MaxWorkers() {
		workers = e.scheduler.MaxWorkers()
	}
	// With no keys every selected row belongs to the same group. The existing
	// adjacent-key operator is bounded regardless of source ordering. Reject
	// incompatible forced strategies rather than silently ignoring a hint.
	if cq.GroupShape == GroupShapeGlobal && cq.ForceStrategy != "" && cq.ForceStrategy != "sorted" {
		return nil, fmt.Errorf("global aggregation requires the single-state sorted strategy, got forced %q", cq.ForceStrategy)
	}
	strategy, err := e.opts.CostModel.Choose(planner.Input{
		Rows: stats.Rows, Partitions: stats.Partitions, Workers: workers,
		Selectivity: cq.SelectivityHint, Cardinality: cq.CardinalityHint,
		ReuseSelection: cq.ReuseSelection, Kernel: e.kernels.Capabilities(),
		ForceStrategy: cq.ForceStrategy,
		FusedCapable:  cq.FusedCapable,
		ComparisonID:  cq.ComparisonID, HasComparisonID: cq.HasComparisonID,
		SortedCapable: cq.GroupShape == GroupShapeGlobal || source.OrderedByPrefix(src, q.GroupBy),
		KeyWidthBytes: cq.KeyWidthBytes, PredicateComplexity: cq.Predicate.InstructionCount(),
	})
	if err != nil {
		return nil, err
	}
	if cq.GroupShape == GroupShapeGlobal {
		strategy = planner.Strategy{Kind: planner.StrategySorted, Workers: 1, Reason: "global aggregation uses one bounded state"}
	}
	if policy == ExecutionBounded && strategy.Kind != planner.StrategySorted && !e.SpillConfigured() {
		return nil, &UnsupportedBoundedPlanError{
			Operator: "Aggregate",
			Path:     []string{"Aggregate"},
			Detail:   ErrBoundedSpillRequired.Error() + "; configure engine.Options.Spill.MaxBytes or provide a proven group-key ordering",
			Cause:    ErrBoundedSpillRequired,
		}
	}
	if strategy.Kind != planner.StrategySorted && policy != ExecutionMemoryOnly && e.memory.Limit() > 0 && e.SpillConfigured() {
		strategy = planner.Strategy{Kind: planner.StrategySpillHash, Workers: 1, Reason: "finite query budget with spill quota"}
	}
	if cq.MultiAggregate != nil && cq.MultiAggregate.Distinct != nil {
		strategy.Reason = "exact-global-tuple-sort fallback (group ordering alone does not bound exact distinct/quantile state); planned policy: " + strategy.Kind.String()
		strategy.Workers = 1
	}
	if stats.Rows < 0 || stats.Rows > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("row count %d exceeds addressable workspace", stats.Rows)
	}
	maxConc := opts.MaxConcurrency
	if maxConc < 1 {
		maxConc = 1
	}
	pq := &PreparedQuery{e: e, cacheHandle: e.cache.newHandle(), src: src, cq: cq, stats: stats, parts: parts, strategy: strategy, dicts: make(map[string]source.Dictionary), feedbackKey: opts.FeedbackKey, feedbackSamples: feedbackSamples, executionPolicy: policy, executions: make(chan struct{}, maxConc)}
	// Load source-level dictionaries once when available. Absence is allowed;
	// materialized results will expose code+domain instead of decoded strings.
	if dp, ok := src.(source.DictionaryProvider); ok && cq.GroupShape != GroupShapeGeneric {
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
	pq.executionSchema = pq.resultSchema()
	return pq, nil
}

// Plan returns physical-plan diagnostics selected during preparation.
func (p *PreparedQuery) Plan() PlanInfo {
	return PlanInfo{Strategy: p.strategy, Rows: p.stats.Rows, Partitions: p.stats.Partitions, KeyWidthBytes: p.cq.KeyWidthBytes, PredicateOps: p.cq.Predicate.InstructionCount(), GroupColumns: len(p.cq.GroupBy), MaxConcurrency: cap(p.executions), FeedbackSamples: p.feedbackSamples, EstimatedSelectivity: p.cq.SelectivityHint, EstimatedCardinality: p.cq.CardinalityHint}
}

func (p *PreparedQuery) begin() error {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.closed {
		return ErrPreparedClosed
	}
	if err := p.e.begin(); err != nil {
		return err
	}
	p.active.Add(1)
	return nil
}
func (p *PreparedQuery) end() { p.active.Done(); p.e.end() }

type profileOperatorIDProvider interface{ OperatorID() string }

// Close prevents new executions and waits for active stream consumers.
func (p *PreparedQuery) Close() error {
	if p == nil {
		return nil
	}
	p.lifeMu.Lock()
	if p.closed {
		done := p.closeDone
		p.lifeMu.Unlock()
		<-done
		return nil
	}
	p.closed = true
	if p.cacheHandle != nil {
		p.cacheHandle.closed.Store(true)
	}
	p.closeDone = make(chan struct{})
	done := p.closeDone
	p.lifeMu.Unlock()
	p.active.Wait()
	if p.cacheHandle != nil {
		p.e.cache.purge(p.cacheHandle.id)
		p.cacheHandle.retained.Wait()
	}
	close(done)
	return nil
}
