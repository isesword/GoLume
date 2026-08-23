package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/optimizer"
	"gocolumn/physical"
	"gocolumn/source"
	"io"
	"strings"
	"time"
)

// LazyFrame is an immutable logical query plan. Transformation methods only
// append logical nodes; they never scan or decode input data.
type LazyFrame struct {
	session *Session
	plan    logical.Plan
	hints   expr.Hints
	err     error
}

// GroupedFrame is an intermediate lazy builder returned by GroupBy.
type GroupedFrame struct {
	frame LazyFrame
	keys  []string
}

// SortExpr describes one lazy column ordering key.
type SortExpr struct {
	Column     string
	Descending bool
	NullsLast  bool
}

// Asc orders column ascending with NULL values last.
func Asc(column string) SortExpr { return SortExpr{Column: column, NullsLast: true} }

// Desc orders column descending with NULL values last.
func Desc(column string) SortExpr { return SortExpr{Column: column, Descending: true, NullsLast: true} }

// AscNullsFirst orders column ascending with NULL values first.
func AscNullsFirst(column string) SortExpr { return SortExpr{Column: column} }

// DescNullsFirst orders column descending with NULL values first.
func DescNullsFirst(column string) SortExpr { return SortExpr{Column: column, Descending: true} }

// Prepared is a lazily compiled physical query with reusable engine workspaces.
type Prepared struct {
	query   *engine.PreparedQuery
	closers []io.Closer
}

// Col constructs a logical column reference for fluent lazy expressions.
func Col(name string) expr.Column { return expr.Col(name) }

// Lit constructs an int64 scalar literal for fluent lazy expressions.
func Lit(v int64) expr.Int64Literal { return expr.Lit(v) }

// F64 constructs a float64 scalar literal for fluent lazy expressions.
func F64(v float64) expr.Float64Literal { return expr.F64(v) }

// Str constructs a UTF-8 string literal for fluent lazy expressions.
func Str(v string) expr.StringLiteral { return expr.Str(v) }

// Date32 constructs a date32 literal expressed as days since the Unix epoch.
func Date32(days int32) expr.Date32Literal { return expr.Date32(days) }

// Timestamp constructs a physical timestamp literal in the source column's
// Arrow timestamp unit.
func Timestamp(v int64) expr.TimestampLiteral { return expr.Timestamp(v) }

// Cast constructs an explicit typed scalar cast.
func Cast(e expr.Expr, target memory.DataType) expr.Cast { return expr.CastTo(e, target) }

// Concat constructs a UTF-8 concatenation expression.
func Concat(left, right expr.Expr) expr.Binary { return expr.Concat(left, right) }

// Sum constructs SUM(column) AS alias.
func Sum(column, alias string) expr.Aggregate { return expr.Sum(column, alias) }

// Count constructs COUNT(*) AS alias.
func Count(alias string) expr.Aggregate { return expr.Count(alias) }

// CountColumn constructs COUNT(column) AS alias.
func CountColumn(column, alias string) expr.Aggregate { return expr.CountColumn(column, alias) }

// Min constructs MIN(column) AS alias.
func Min(column, alias string) expr.Aggregate { return expr.Min(column, alias) }

// Max constructs MAX(column) AS alias.
func Max(column, alias string) expr.Aggregate { return expr.Max(column, alias) }

// Avg constructs AVG(column) AS alias.
func Avg(column, alias string) expr.Aggregate { return expr.Avg(column, alias) }

// Filter appends a residual SQL predicate to the lazy plan.
func (f LazyFrame) Filter(predicate expr.Expr) LazyFrame {
	if f.err != nil {
		return f
	}
	f.plan = &logical.Filter{Input: f.plan, Predicate: predicate}
	return f
}

// Select appends a column-only logical projection. Projection pushdown later
// computes the union of selected, filtered, grouped, and aggregated columns
// actually required by the source scan.
func (f LazyFrame) Select(columns ...string) LazyFrame {
	if f.err != nil {
		return f
	}
	f.plan = &logical.Projection{Input: f.plan, Columns: append([]string(nil), columns...)}
	return f
}

// SelectExprs appends a scalar expression projection. Only the projected
// expressions remain visible to downstream logical operators.
func (f LazyFrame) SelectExprs(expressions ...expr.NamedExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if len(expressions) == 0 {
		f.err = fmt.Errorf("SelectExprs requires at least one expression")
		return f
	}
	f.plan = &logical.ExprProjection{Input: f.plan, Exprs: append([]expr.NamedExpr(nil), expressions...)}
	return f
}

// WithColumns appends or replaces named scalar expressions while retaining all
// existing logical columns. Computation remains lazy until Collect/Prepare.
func (f LazyFrame) WithColumns(expressions ...expr.NamedExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if len(expressions) == 0 {
		return f
	}
	f.plan = &logical.ExprProjection{Input: f.plan, Exprs: append([]expr.NamedExpr(nil), expressions...), IncludeInput: true}
	return f
}

// Named assigns an output name to a scalar expression.
func Named(e expr.Expr, alias string) expr.NamedExpr { return expr.Named(e, alias) }

// Sort appends a global stable ordering operation. Sort is lazy and executes
// only when the plan is collected or prepared.
func (f LazyFrame) Sort(by ...SortExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if len(by) == 0 {
		f.err = fmt.Errorf("Sort requires at least one key")
		return f
	}
	keys := make([]logical.SortKey, len(by))
	for i, k := range by {
		if k.Column == "" {
			f.err = fmt.Errorf("Sort key %d has an empty column", i)
			return f
		}
		keys[i] = logical.SortKey{Column: k.Column, Descending: k.Descending, NullsLast: k.NullsLast}
	}
	f.plan = &logical.Sort{Input: f.plan, By: keys}
	return f
}

// TopK appends a bounded-memory ordering operator that returns the best K rows.
func (f LazyFrame) TopK(k int, by ...SortExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if k < 0 {
		f.err = fmt.Errorf("TopK requires a non-negative K")
		return f
	}
	if len(by) == 0 {
		f.err = fmt.Errorf("TopK requires at least one key")
		return f
	}
	keys := make([]logical.SortKey, len(by))
	for i, key := range by {
		if key.Column == "" {
			f.err = fmt.Errorf("TopK key %d has an empty column", i)
			return f
		}
		keys[i] = logical.SortKey{Column: key.Column, Descending: key.Descending, NullsLast: key.NullsLast}
	}
	f.plan = &logical.TopK{Input: f.plan, K: k, By: keys}
	return f
}

// Limit appends an order-preserving global row limit.
func (f LazyFrame) Limit(n int64) LazyFrame {
	if f.err != nil {
		return f
	}
	if n < 0 {
		f.err = fmt.Errorf("Limit requires a non-negative row count")
		return f
	}
	f.plan = &logical.Limit{Input: f.plan, N: n}
	return f
}

// WithHints returns a frame carrying physical cost-model hints for the next
// aggregate node. Hints are optional and do not change logical semantics.
func (f LazyFrame) WithHints(hints expr.Hints) LazyFrame {
	if f.err != nil {
		return f
	}
	f.hints = hints
	return f
}

// GroupBy starts a grouped aggregation over one or two Phase 2.3 group keys.
func (f LazyFrame) GroupBy(columns ...string) GroupedFrame {
	return GroupedFrame{frame: f, keys: append([]string(nil), columns...)}
}

// Agg completes a grouped aggregation without executing it.
func (g GroupedFrame) Agg(aggregates ...expr.Aggregate) LazyFrame {
	f := g.frame
	f.plan = &logical.Aggregate{Input: f.plan, GroupBy: append([]string(nil), g.keys...), Aggregates: append([]expr.Aggregate(nil), aggregates...), Hints: f.hints}
	f.hints = expr.Hints{}
	return f
}

// Schema infers and validates the current logical output schema without reading
// column data.
func (f LazyFrame) Schema() (memory.Schema, error) {
	if f.err != nil {
		return memory.Schema{}, f.err
	}
	return logical.InferSchema(f.plan)
}

// Explain renders the unoptimized logical plan.
func (f LazyFrame) Explain() string {
	if f.err != nil {
		return "INVALID: " + f.err.Error()
	}
	return logical.Explain(f.plan)
}

// ExplainOptimized validates and renders the optimized logical plan, including
// scan-level projection and predicate annotations but before source metadata is
// used to prune row groups.
func (f LazyFrame) ExplainOptimized() (string, error) {
	if f.err != nil {
		return "", f.err
	}
	result, err := optimizer.Optimize(f.plan)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if len(result.AppliedRules) != 0 {
		fmt.Fprintf(&b, "rules=[%s]\n", strings.Join(result.AppliedRules, ", "))
	}
	b.WriteString(logical.Explain(result.Plan))
	return b.String(), nil
}

// ExplainPhysical applies source pushdown and physical planning, then returns a
// diagnostic plan including row-group pruning and selected execution strategy.
// It does not execute query rows.
func (f LazyFrame) ExplainPhysical(ctx context.Context) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if _, ok := f.plan.(*logical.Aggregate); !ok {
		return f.explainRowPhysical(ctx)
	}
	prepared, optimized, report, closers, err := f.prepare(ctx, engine.PrepareOptions{})
	if err != nil {
		return "", err
	}
	defer closePreparedResources(prepared, closers)
	info := prepared.Plan()
	var b strings.Builder
	b.WriteString(logical.Explain(optimized))
	fmt.Fprintf(&b, "\nPHYSICAL strategy=%s workers=%d radix_bits=%d key_width=%d predicate_ops=%d feedback_samples=%d est_selectivity=%.4f est_cardinality=%d", info.Strategy.Kind, info.Strategy.Workers, info.Strategy.RadixBits, info.KeyWidthBytes, info.PredicateOps, info.FeedbackSamples, info.EstimatedSelectivity, info.EstimatedCardinality)
	appendPushdownExplain(&b, report)
	return b.String(), nil
}

func (f LazyFrame) explainRowPhysical(ctx context.Context) (string, error) {
	if f.session == nil || f.session.engine == nil {
		return "", fmt.Errorf("lazy frame has no execution session")
	}
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return "", err
	}
	physicalPlan, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return "", err
	}
	src, pending, closers, err := lowerSourcePipeline(ctx, physicalPlan, f.session.engine.Memory(), f.session.engine.KernelCapabilities().Mode, f.session.engine)
	if err != nil {
		return "", err
	}
	defer closeClosers(closers)
	if pending != nil {
		src, err = physical.NewFilterSource(src, pending, f.session.engine.KernelCapabilities().Mode)
		if err != nil {
			return "", err
		}
	}
	stats, err := src.Stats(ctx)
	if err != nil {
		return "", err
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(logical.Explain(physicalPlan))
	fmt.Fprintf(&b, "\nPHYSICAL row-pipeline partitions=%d rows=%d columns=[%s]", len(parts), stats.Rows, strings.Join(schemaColumnNames(src.Schema()), ", "))
	appendPushdownExplain(&b, report)
	return b.String(), nil
}

func appendPushdownExplain(b *strings.Builder, report source.PushdownReport) {
	if report.TotalPartitions <= 0 {
		return
	}
	fmt.Fprintf(b, "\nPUSHDOWN columns=[%s] partitions=%d/%d row_groups=%d/%d pruned=%d rows=%d/%d source_bytes=%d/%d", strings.Join(report.Projection, ", "), report.SelectedPartitions, report.TotalPartitions, report.SelectedPartitions, report.TotalPartitions, report.PrunedPartitions, report.SelectedRows, report.TotalRows, report.SelectedBytes, report.TotalBytes)
}

func schemaColumnNames(schema memory.Schema) []string {
	out := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		out[i] = field.Name
	}
	return out
}

func closeClosers(closers []io.Closer) {
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i].Close()
	}
}

// Prepare optimizes the logical plan, applies source pushdown, and creates a
// reusable physical query. The caller must close the returned Prepared value.
func (f LazyFrame) Prepare(ctx context.Context, opts engine.PrepareOptions) (*Prepared, error) {
	query, _, _, closers, err := f.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Prepared{query: query, closers: closers}, nil
}

// Collect optimizes and executes the lazy plan once, materializing grouped
// output rows. Repeated workloads should use Prepare instead.
func (f LazyFrame) Collect(ctx context.Context) (engine.Result, error) {
	prepared, err := f.Prepare(ctx, engine.PrepareOptions{})
	if err != nil {
		return engine.Result{}, err
	}
	defer prepared.Close()
	return prepared.Execute(ctx, engine.ExecuteOptions{Materialize: true})
}

// Execute runs the prepared physical plan with caller-selected materialization.
func (p *Prepared) Execute(ctx context.Context, opts engine.ExecuteOptions) (engine.Result, error) {
	if p == nil || p.query == nil {
		return engine.Result{}, fmt.Errorf("lazy prepared query is nil")
	}
	return p.query.Execute(ctx, opts)
}

// Plan returns diagnostics from the underlying physical prepared query.
func (p *Prepared) Plan() engine.PlanInfo {
	if p == nil || p.query == nil {
		return engine.PlanInfo{}
	}
	return p.query.Plan()
}

// Close releases retained workspaces owned by the prepared query.
func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	var first error
	if p.query != nil {
		first = p.query.Close()
	}
	for i := len(p.closers) - 1; i >= 0; i-- {
		if err := p.closers[i].Close(); err != nil && first == nil {
			first = err
		}
	}
	p.closers = nil
	return first
}

func (f LazyFrame) prepare(ctx context.Context, opts engine.PrepareOptions) (*engine.PreparedQuery, logical.Plan, source.PushdownReport, []io.Closer, error) {
	if f.err != nil {
		return nil, nil, source.PushdownReport{}, nil, f.err
	}
	if f.session == nil || f.session.engine == nil {
		return nil, nil, source.PushdownReport{}, nil, fmt.Errorf("lazy frame has no execution session")
	}
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, nil, err
	}
	if opts.FeedbackKey == "" {
		opts.FeedbackKey = planFeedbackKey(optimized.Plan)
	}
	physicalLogical, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, nil, err
	}
	var physicalSource source.Source
	var query expr.Query
	var closers []io.Closer
	if containsAdvancedPlan(physicalLogical) {
		agg, ok := physicalLogical.(*logical.Aggregate)
		if !ok {
			return nil, nil, source.PushdownReport{}, nil, fmt.Errorf("Collect/Prepare requires GroupBy(...).Agg(...) as the terminal lazy operation")
		}
		physicalSource, query.Filter, closers, err = lowerSourcePipeline(ctx, agg.Input, f.session.engine.Memory(), f.session.engine.KernelCapabilities().Mode, f.session.engine)
		if err != nil {
			return nil, nil, source.PushdownReport{}, nil, err
		}
		query.GroupBy = append([]string(nil), agg.GroupBy...)
		query.Aggregates = append([]expr.Aggregate(nil), agg.Aggregates...)
		query.Hints = agg.Hints
	} else {
		var scan *logical.Scan
		scan, query, err = lowerAggregate(physicalLogical)
		if err != nil {
			return nil, nil, source.PushdownReport{}, nil, err
		}
		base := profileSource(ctx, scan.Source, "scan", sourceDescription(scan.Source))
		physicalSource, err = projectScanSource(base, scan.Projection)
		if err != nil {
			return nil, nil, source.PushdownReport{}, nil, err
		}
		if physicalSource != base {
			physicalSource = profileSource(ctx, physicalSource, "projection", "scan projection", profileSourceID(base))
		}
	}
	if opts.FeedbackKey == "" {
		opts.FeedbackKey = planFeedbackKey(physicalLogical)
	}
	prepared, err := f.session.engine.Prepare(ctx, physicalSource, query, opts)
	if err != nil {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
		return nil, nil, source.PushdownReport{}, nil, err
	}
	return prepared, physicalLogical, report, closers, nil
}

func applyPushdown(ctx context.Context, plan logical.Plan) (logical.Plan, source.PushdownReport, error) {
	switch n := plan.(type) {
	case *logical.Scan:
		out := *n
		out.Projection = append([]string(nil), n.Projection...)
		capable, ok := n.Source.(source.PushdownSource)
		if !ok {
			return &out, source.PushdownReport{}, nil
		}
		src, report, err := capable.ApplyPushdown(ctx, source.PushdownRequest{Projection: n.Projection, Predicate: n.PushedPredicate})
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		out.Source = src
		out.Pushdown = report
		out.Projection = append([]string(nil), report.Projection...)
		return &out, report, nil
	case *logical.Filter:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Filter{Input: child, Predicate: n.Predicate}, report, nil
	case *logical.Projection:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Projection{Input: child, Columns: append([]string(nil), n.Columns...)}, report, nil
	case *logical.ExprProjection:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.ExprProjection{Input: child, Exprs: append([]expr.NamedExpr(nil), n.Exprs...), IncludeInput: n.IncludeInput}, report, nil
	case *logical.Sort:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Sort{Input: child, By: append([]logical.SortKey(nil), n.By...)}, report, nil
	case *logical.TopK:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.TopK{Input: child, K: n.K, By: append([]logical.SortKey(nil), n.By...)}, report, nil
	case *logical.Limit:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Limit{Input: child, N: n.N}, report, nil
	case *logical.Join:
		left, lr, err := applyPushdown(ctx, n.Left)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		right, rr, err := applyPushdown(ctx, n.Right)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		report := source.PushdownReport{TotalPartitions: lr.TotalPartitions + rr.TotalPartitions, SelectedPartitions: lr.SelectedPartitions + rr.SelectedPartitions, PrunedPartitions: lr.PrunedPartitions + rr.PrunedPartitions, TotalRows: lr.TotalRows + rr.TotalRows, SelectedRows: lr.SelectedRows + rr.SelectedRows, PrunedRows: lr.PrunedRows + rr.PrunedRows, TotalBytes: lr.TotalBytes + rr.TotalBytes, SelectedBytes: lr.SelectedBytes + rr.SelectedBytes}
		return &logical.Join{Left: left, Right: right, Type: n.Type, Keys: append([]logical.JoinKey(nil), n.Keys...), Suffix: n.Suffix, RadixBits: n.RadixBits, Workers: n.Workers}, report, nil
	case *logical.Aggregate:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Aggregate{Input: child, GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}, report, nil
	default:
		return nil, source.PushdownReport{}, fmt.Errorf("unsupported logical node %T", plan)
	}
}

func lowerAggregate(plan logical.Plan) (*logical.Scan, expr.Query, error) {
	agg, ok := plan.(*logical.Aggregate)
	if !ok {
		return nil, expr.Query{}, fmt.Errorf("phase-2.3 Collect/Prepare requires GroupBy(...).Agg(...) as the terminal lazy operation")
	}
	var predicate expr.Expr
	var scan *logical.Scan
	var walk func(logical.Plan) error
	walk = func(node logical.Plan) error {
		switch n := node.(type) {
		case *logical.Filter:
			predicate = expr.MergeAnd(predicate, n.Predicate)
			return walk(n.Input)
		case *logical.Projection:
			return walk(n.Input)
		case *logical.Scan:
			scan = n
			return nil
		default:
			return fmt.Errorf("phase-2.3 physical lowering does not support %T below Aggregate", node)
		}
	}
	if err := walk(agg.Input); err != nil {
		return nil, expr.Query{}, err
	}
	if scan == nil {
		return nil, expr.Query{}, fmt.Errorf("aggregate plan has no scan")
	}
	q := expr.Query{Filter: predicate, GroupBy: append([]string(nil), agg.GroupBy...), Aggregates: append([]expr.Aggregate(nil), agg.Aggregates...), Hints: agg.Hints}
	return scan, q, nil
}

func containsAdvancedPlan(plan logical.Plan) bool {
	switch n := plan.(type) {
	case *logical.ExprProjection:
		return true
	case *logical.Sort, *logical.TopK, *logical.Limit:
		return true
	case *logical.Filter:
		return containsAdvancedPlan(n.Input)
	case *logical.Projection:
		return containsAdvancedPlan(n.Input)
	case *logical.Aggregate:
		return containsAdvancedPlan(n.Input)
	case *logical.Join:
		return true
	default:
		return false
	}
}

func lowerSourcePipeline(ctx context.Context, plan logical.Plan, manager memory.Manager, mode kernel.Mode, exec join.ParallelExecutor) (source.Source, expr.Expr, []io.Closer, error) {
	switch n := plan.(type) {
	case *logical.Scan:
		base := profileSource(ctx, n.Source, "scan", sourceDescription(n.Source))
		src, err := projectScanSource(base, n.Projection)
		if err != nil {
			return nil, nil, nil, err
		}
		if src != base {
			src = profileSource(ctx, src, "projection", "scan projection", profileSourceID(base))
		}
		return src, nil, nil, nil
	case *logical.Filter:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		return src, expr.MergeAnd(pending, n.Predicate), closers, nil
	case *logical.Projection:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending, mode)
			if err != nil {
				return nil, nil, nil, err
			}
			src = profileSource(ctx, src, "filter", "residual filter", childID)
		}
		named := make([]expr.NamedExpr, 0, len(n.Columns))
		for _, name := range n.Columns {
			named = append(named, expr.Named(expr.Col(name), name))
		}
		childID := profileSourceID(src)
		projected, err := physical.NewProjectSource(src, named, false)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, projected, "projection", "column projection", childID), nil, closers, nil
	case *logical.ExprProjection:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending, mode)
			if err != nil {
				return nil, nil, nil, err
			}
			src = profileSource(ctx, src, "filter", "residual filter", childID)
		}
		childID := profileSourceID(src)
		projected, err := physical.NewProjectSource(src, n.Exprs, n.IncludeInput)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, projected, "projection", "expression projection", childID), nil, closers, nil
	case *logical.Sort:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending, mode)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before sort", childID)
		}
		keys := make([]physical.SortKey, len(n.By))
		for i, k := range n.By {
			keys[i] = physical.SortKey{Column: k.Column, Descending: k.Descending, NullsLast: k.NullsLast}
		}
		childID := profileSourceID(src)
		sorted, err := physical.NewSortSource(src, keys, manager)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, sorted, "sort", "global sort", childID), nil, closers, nil
	case *logical.TopK:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending, mode)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before top-k", childID)
		}
		keys := make([]physical.SortKey, len(n.By))
		for i, k := range n.By {
			keys[i] = physical.SortKey{Column: k.Column, Descending: k.Descending, NullsLast: k.NullsLast}
		}
		childID := profileSourceID(src)
		top, err := physical.NewTopKSourceWithManager(src, n.K, keys, manager)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, top, "topk", fmt.Sprintf("top-k k=%d", n.K), childID), nil, closers, nil
	case *logical.Limit:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending, mode)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before limit", childID)
		}
		childID := profileSourceID(src)
		limited, err := physical.NewLimitSource(src, n.N)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, limited, "limit", fmt.Sprintf("limit n=%d", n.N), childID), nil, closers, nil
	case *logical.Aggregate:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		eng, ok := exec.(*engine.Engine)
		if !ok || eng == nil {
			return nil, nil, closers, fmt.Errorf("aggregate source lowering requires *engine.Engine executor")
		}
		q := expr.Query{Filter: pending, GroupBy: append([]string(nil), n.GroupBy...), Aggregates: append([]expr.Aggregate(nil), n.Aggregates...), Hints: n.Hints}
		pq, err := eng.Prepare(ctx, src, q, engine.PrepareOptions{MaxConcurrency: eng.MaxWorkers()})
		if err != nil {
			return nil, nil, closers, err
		}
		outSchema, err := logical.InferSchema(n)
		if err != nil {
			_ = pq.Close()
			return nil, nil, closers, err
		}
		aggSrc, err := newAggregateResultSource(ctx, pq, src, outSchema, n.GroupBy, n.Aggregates)
		if err != nil {
			_ = pq.Close()
			return nil, nil, closers, err
		}
		closers = append(closers, pq)
		childID := profileSourceID(src)
		return profileSource(ctx, aggSrc, "aggregate", "materialized aggregate source", childID), nil, closers, nil
	case *logical.Join:
		left, lp, lc, err := lowerSourcePipeline(ctx, n.Left, manager, mode, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if lp != nil {
			childID := profileSourceID(left)
			left, err = physical.NewFilterSource(left, lp, mode)
			if err != nil {
				return nil, nil, nil, err
			}
			left = profileSource(ctx, left, "filter", "left join filter", childID)
		}
		right, rp, rc, err := lowerSourcePipeline(ctx, n.Right, manager, mode, exec)
		if err != nil {
			for i := len(lc) - 1; i >= 0; i-- {
				_ = lc[i].Close()
			}
			return nil, nil, nil, err
		}
		if rp != nil {
			childID := profileSourceID(right)
			right, err = physical.NewFilterSource(right, rp, mode)
			if err != nil {
				return nil, nil, nil, err
			}
			right = profileSource(ctx, right, "filter", "right join filter", childID)
		}
		keys := make([]physical.JoinKey, len(n.Keys))
		for i, k := range n.Keys {
			keys[i] = physical.JoinKey{Left: k.Left, Right: k.Right}
		}
		workers := n.Workers
		if workers == 0 && exec != nil {
			workers = exec.MaxWorkers()
		}
		joinStart := time.Now()
		joined, err := physical.NewJoinSourceWithOptions(ctx, left, right, n.Type, keys, n.Suffix, manager, physical.JoinSourceOptions{RadixBits: n.RadixBits, Workers: workers, Executor: exec, AutoParallelRadix: n.RadixBits == 0})
		joinPrepare := time.Since(joinStart)
		if err != nil {
			for i := len(rc) - 1; i >= 0; i-- {
				_ = rc[i].Close()
			}
			for i := len(lc) - 1; i >= 0; i-- {
				_ = lc[i].Close()
			}
			return nil, nil, nil, err
		}
		closers := append(lc, rc...)
		closers = append(closers, joined)
		trace := observer.TraceFromContext(ctx)
		var profiled source.Source = joined
		if trace != nil {
			node := trace.NewNode("join", joined.Strategy(), profileSourceID(left), profileSourceID(right))
			rightStats, _ := right.Stats(ctx)
			trace.Record(node.ID, observer.PhaseFromContext(ctx), rightStats.Rows, 0, joinPrepare)
			profiled = physical.NewProfileSource(joined, trace, node)
		}
		return profiled, nil, closers, nil
	default:
		return nil, nil, nil, fmt.Errorf("phase-2.5 source lowering does not support %T", plan)
	}
}

type profileIDProvider interface{ OperatorID() string }

func profileSourceID(src source.Source) string {
	if p, ok := src.(profileIDProvider); ok {
		return p.OperatorID()
	}
	return ""
}

func profileSource(ctx context.Context, src source.Source, kind, label string, children ...string) source.Source {
	trace := observer.TraceFromContext(ctx)
	if trace == nil || src == nil {
		return src
	}
	filtered := children[:0]
	for _, child := range children {
		if child != "" {
			filtered = append(filtered, child)
		}
	}
	node := trace.NewNode(kind, label, filtered...)
	return physical.NewProfileSource(src, trace, node)
}

func sourceDescription(src source.Source) string {
	if d, ok := src.(source.Describer); ok {
		return d.Description()
	}
	return fmt.Sprintf("%T", src)
}

func projectScanSource(src source.Source, projection []string) (source.Source, error) {
	if src == nil || len(projection) == 0 || equalColumnNames(projection, schemaColumnNames(src.Schema())) {
		return src, nil
	}
	named := make([]expr.NamedExpr, 0, len(projection))
	for _, name := range projection {
		if _, ok := src.Schema().Field(name); !ok {
			return nil, fmt.Errorf("physical scan projection column %q not found", name)
		}
		named = append(named, expr.Named(expr.Col(name), name))
	}
	return physical.NewProjectSource(src, named, false)
}

func equalColumnNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func closePreparedResources(q *engine.PreparedQuery, closers []io.Closer) {
	if q != nil {
		_ = q.Close()
	}
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i].Close()
	}
}
