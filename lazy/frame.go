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
	"gocolumn/pipeline"
	"gocolumn/planner"
	"gocolumn/source"
	"io"
	"math"
	"strings"
	"time"
)

// LazyFrame is an immutable logical query plan. Transformation methods only
// append logical nodes; they never scan or decode input data.
type LazyFrame struct {
	session            *Session
	plan               logical.Plan
	hints              expr.Hints
	executionPolicy    engine.ExecutionPolicy
	hasExecutionPolicy bool
	err                error
}

// GroupedFrame is an intermediate lazy builder returned by GroupBy.
type GroupedFrame struct {
	frame LazyFrame
	keys  []string
}

// SortExpr describes one lazy ordering key.
type SortExpr struct {
	Expr       expr.Expr
	Descending bool
	NullsLast  bool
}

// WindowSpec is the shared partitioning and ordering definition for one
// WithWindows call.
type WindowSpec = logical.WindowSpec

// WindowExpr describes one named ranking or rolling aggregate output.
type WindowExpr = logical.WindowExpr

// RowsFrame is an inclusive, partition-local ROWS frame.
type RowsFrame = logical.RowsFrame

// RowBound identifies one side of a ROWS frame.
type RowBound = logical.RowBound

// UnboundedPreceding is the first row of the current partition.
var UnboundedPreceding = logical.RowBound{Kind: logical.RowUnboundedPreceding}

// CurrentRow is the current row in the window ordering.
var CurrentRow = logical.RowBound{Kind: logical.RowCurrent}

// UnboundedFollowing is the last row of the current partition.
var UnboundedFollowing = logical.RowBound{Kind: logical.RowUnboundedFollowing}

// PartitionBy lists direct input columns used to form window partitions.
func PartitionBy(columns ...string) []string { return append([]string(nil), columns...) }

// OrderBy groups ordinary lazy ordering expressions for NewWindow.
func OrderBy(keys ...SortExpr) []SortExpr { return append([]SortExpr(nil), keys...) }

// NewWindow creates a window specification. Validation is deferred to Schema,
// PrepareFrame, or CollectFrame so builders remain immutable and composable.
func NewWindow(partitionBy []string, orderBy []SortExpr) WindowSpec {
	keys := make([]logical.SortKey, len(orderBy))
	for i, key := range orderBy {
		keys[i] = logical.SortKey{Expr: key.Expr, Descending: key.Descending, NullsLast: key.NullsLast}
	}
	return logical.WindowSpec{PartitionBy: append([]string(nil), partitionBy...), By: keys}
}

// Preceding identifies a row offset before the current ordered row.
func Preceding(n int64) RowBound { return logical.RowBound{Kind: logical.RowPreceding, Offset: n} }

// Following identifies a row offset after the current ordered row.
func Following(n int64) RowBound { return logical.RowBound{Kind: logical.RowFollowing, Offset: n} }

// RowsBetween creates an inclusive ROWS frame. Invalid boundary combinations
// are reported during logical schema validation.
func RowsBetween(start, end RowBound) RowsFrame { return logical.RowsFrame{Start: start, End: end} }

// RowNumber assigns one-based consecutive positions inside each partition.
func RowNumber(alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowRowNumber, Alias: alias}
}

// Rank assigns one-based rank positions with gaps after ties.
func Rank(alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowRank, Alias: alias}
}

// DenseRank assigns one-based rank positions without gaps after ties.
func DenseRank(alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowDenseRank, Alias: alias}
}

// RollingCountRows counts rows in frame, including rows whose values are NULL.
func RollingCountRows(frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowCountRows, Frame: frame, Alias: alias}
}

// RollingCount counts non-NULL values of column in frame.
func RollingCount(column string, frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowCountValues, Column: column, Frame: frame, Alias: alias}
}

// RollingSum sums numeric column values in frame.
func RollingSum(column string, frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowSum, Column: column, Frame: frame, Alias: alias}
}

// RollingMin returns the minimum non-NULL numeric column value in frame.
func RollingMin(column string, frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowMin, Column: column, Frame: frame, Alias: alias}
}

// RollingMax returns the maximum non-NULL numeric column value in frame.
func RollingMax(column string, frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowMax, Column: column, Frame: frame, Alias: alias}
}

// RollingAvg returns the mean of non-NULL numeric column values in frame.
func RollingAvg(column string, frame RowsFrame, alias string) WindowExpr {
	return logical.WindowExpr{Function: logical.WindowAvg, Column: column, Frame: frame, Alias: alias}
}

// Asc orders column ascending with NULL values last.
func Asc(column string) SortExpr { return SortExpr{Expr: expr.Col(column), NullsLast: true} }

// Desc orders column descending with NULL values last.
func Desc(column string) SortExpr {
	return SortExpr{Expr: expr.Col(column), Descending: true, NullsLast: true}
}

// AscNullsFirst orders column ascending with NULL values first.
func AscNullsFirst(column string) SortExpr { return SortExpr{Expr: expr.Col(column)} }

// DescNullsFirst orders column descending with NULL values first.
func DescNullsFirst(column string) SortExpr {
	return SortExpr{Expr: expr.Col(column), Descending: true}
}

// AscExpr orders a scalar expression ascending with NULL values last.
func AscExpr(value expr.Expr) SortExpr { return SortExpr{Expr: value, NullsLast: true} }

// DescExpr orders a scalar expression descending with NULL values last.
func DescExpr(value expr.Expr) SortExpr {
	return SortExpr{Expr: value, Descending: true, NullsLast: true}
}

// AscExprNullsFirst orders a scalar expression ascending with NULL values first.
func AscExprNullsFirst(value expr.Expr) SortExpr { return SortExpr{Expr: value} }

// DescExprNullsFirst orders a scalar expression descending with NULL values first.
func DescExprNullsFirst(value expr.Expr) SortExpr {
	return SortExpr{Expr: value, Descending: true}
}

// PreparedResult is a specialized aggregate query with reusable engine
// workspaces and execution diagnostics.
type PreparedResult struct {
	query   *engine.PreparedQuery
	closers []io.Closer
}

// Prepared is a deprecated compatibility alias for PreparedResult.
type Prepared = PreparedResult

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

// Timestamp constructs a UTC Unix timestamp literal expressed in nanoseconds.
func Timestamp(v int64) expr.TimestampLiteral { return expr.Timestamp(v) }

// Duration constructs an elapsed-time literal normalized to nanoseconds.
func Duration(v time.Duration) expr.DurationLiteral { return expr.Duration(v) }

// TimestampSeconds constructs a UTC Unix timestamp literal from seconds.
func TimestampSeconds(v int64) expr.TimestampUnitLiteral { return expr.TimestampSeconds(v) }

// TimestampMillis constructs a UTC Unix timestamp literal from milliseconds.
func TimestampMillis(v int64) expr.TimestampUnitLiteral { return expr.TimestampMillis(v) }

// TimestampMicros constructs a UTC Unix timestamp literal from microseconds.
func TimestampMicros(v int64) expr.TimestampUnitLiteral { return expr.TimestampMicros(v) }

// Cast constructs an explicit typed scalar cast.
func Cast(e expr.Expr, target memory.DataType) expr.Cast { return expr.CastTo(e, target) }

// Concat constructs a UTF-8 concatenation expression.
func Concat(left, right expr.Expr) expr.Binary { return expr.Concat(left, right) }

// Contains constructs a case-sensitive UTF-8 substring predicate.
func Contains(value, needle expr.Expr) expr.Binary { return expr.Contains(value, needle) }

// StartsWith constructs a case-sensitive UTF-8 prefix predicate.
func StartsWith(value, prefix expr.Expr) expr.Binary { return expr.StartsWith(value, prefix) }

// EndsWith constructs a case-sensitive UTF-8 suffix predicate.
func EndsWith(value, suffix expr.Expr) expr.Binary { return expr.EndsWith(value, suffix) }

// RegexMatch constructs a compiled regular-expression predicate.
func RegexMatch(value, pattern expr.Expr) expr.Binary { return expr.RegexMatch(value, pattern) }

// Length constructs a UTF-8 character-count projection expression.
func Length(value expr.Expr) expr.Unary { return expr.Length(value) }

// ByteLength constructs a UTF-8 byte-count projection expression.
func ByteLength(value expr.Expr) expr.Unary { return expr.ByteLength(value) }

// Trim removes leading and trailing Unicode whitespace from a string.
func Trim(value expr.Expr) expr.Unary { return expr.Trim(value) }

// TrimStart removes leading Unicode whitespace from a string.
func TrimStart(value expr.Expr) expr.Unary { return expr.TrimStart(value) }

// TrimEnd removes trailing Unicode whitespace from a string.
func TrimEnd(value expr.Expr) expr.Unary { return expr.TrimEnd(value) }

// Lower applies Unicode simple case mapping to lower case.
func Lower(value expr.Expr) expr.Unary { return expr.Lower(value) }

// Upper applies Unicode simple case mapping to upper case.
func Upper(value expr.Expr) expr.Unary { return expr.Upper(value) }

// Find returns the zero-based Unicode code-point index of needle, or -1.
func Find(value, needle expr.Expr) expr.Binary { return expr.Find(value, needle) }

// Replace replaces the first literal occurrence of old with new.
func Replace(value, old, new expr.Expr) expr.Ternary { return expr.Replace(value, old, new) }

// ReplaceAll replaces every literal occurrence of old with new.
func ReplaceAll(value, old, new expr.Expr) expr.Ternary {
	return expr.ReplaceAll(value, old, new)
}

// Slice returns up to length Unicode code points beginning at offset.
func Slice(value, offset, length expr.Expr) expr.Ternary { return expr.Slice(value, offset, length) }

// RegexExtract returns a capture from the first regular-expression match.
func RegexExtract(value, pattern, group expr.Expr) expr.Ternary {
	return expr.RegexExtract(value, pattern, group)
}

// Split separates value using a possibly row-dependent delimiter.
func Split(value, delimiter expr.Expr) expr.Binary { return expr.Split(value, delimiter) }

// RegexExtractAll returns one capture per regular-expression match.
func RegexExtractAll(value, pattern, group expr.Expr) expr.Ternary {
	return expr.RegexExtractAll(value, pattern, group)
}

// JSONDecode decodes a JSON object using an explicit Struct child schema.
func JSONDecode(value expr.Expr, target memory.Schema) expr.JSONDecodeExpr {
	return expr.JSONDecode(value, target)
}

// Year extracts the UTC calendar year from a date or timestamp.
func Year(value expr.Expr) expr.Unary { return expr.Year(value) }

// Month extracts the UTC calendar month from a date or timestamp.
func Month(value expr.Expr) expr.Unary { return expr.Month(value) }

// Day extracts the UTC calendar day of month from a date or timestamp.
func Day(value expr.Expr) expr.Unary { return expr.Day(value) }

// Hour extracts the UTC hour from a timestamp.
func Hour(value expr.Expr) expr.Unary { return expr.Hour(value) }

// Minute extracts the UTC minute from a timestamp.
func Minute(value expr.Expr) expr.Unary { return expr.Minute(value) }

// Second extracts the UTC second from a timestamp.
func Second(value expr.Expr) expr.Unary { return expr.Second(value) }

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

// WithWindows computes named analytic outputs using one shared window
// specification. Window results retain the input row order.
func (f LazyFrame) WithWindows(spec WindowSpec, expressions ...WindowExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if len(expressions) == 0 {
		f.err = fmt.Errorf("WithWindows requires at least one expression")
		return f
	}
	f.plan = &logical.Window{Input: f.plan, Spec: spec, Exprs: append([]logical.WindowExpr(nil), expressions...)}
	return f
}

// Named assigns an output name to a scalar expression.
func Named(e expr.Expr, alias string) expr.NamedExpr { return expr.Named(e, alias) }

// Sort appends a global stable ordering operation. Sort is lazy and executes
// only when the plan is collected or prepared.
func (f LazyFrame) Sort(by ...SortExpr) LazyFrame {
	return f.orderBy("Sort", by...)
}

// SortBy is the expression-oriented spelling of Sort. It accepts AscExpr and
// DescExpr keys directly, so callers do not need a visible WithColumns step.
func (f LazyFrame) SortBy(by ...SortExpr) LazyFrame {
	return f.orderBy("SortBy", by...)
}

func (f LazyFrame) orderBy(operation string, by ...SortExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if len(by) == 0 {
		f.err = fmt.Errorf("%s requires at least one key", operation)
		return f
	}
	keys, err := lazySortKeys(operation, by)
	if err != nil {
		f.err = err
		return f
	}
	f.plan = &logical.Sort{Input: f.plan, By: keys}
	return f
}

// TopK appends a bounded-memory ordering operator that returns the best K rows.
func (f LazyFrame) TopK(k int, by ...SortExpr) LazyFrame {
	return f.topKBy("TopK", k, by...)
}

// TopKBy is the expression-oriented spelling of TopK.
func (f LazyFrame) TopKBy(k int, by ...SortExpr) LazyFrame {
	return f.topKBy("TopKBy", k, by...)
}

func (f LazyFrame) topKBy(operation string, k int, by ...SortExpr) LazyFrame {
	if f.err != nil {
		return f
	}
	if k < 0 {
		f.err = fmt.Errorf("%s requires a non-negative K", operation)
		return f
	}
	if len(by) == 0 {
		f.err = fmt.Errorf("%s requires at least one key", operation)
		return f
	}
	keys, err := lazySortKeys(operation, by)
	if err != nil {
		f.err = err
		return f
	}
	f.plan = &logical.TopK{Input: f.plan, K: k, By: keys}
	return f
}

func lazySortKeys(operation string, by []SortExpr) ([]logical.SortKey, error) {
	keys := make([]logical.SortKey, len(by))
	for i, key := range by {
		if key.Expr == nil {
			return nil, fmt.Errorf("%s key %d has an empty expression", operation, i)
		}
		keys[i] = logical.SortKey{Expr: key.Expr, Descending: key.Descending, NullsLast: key.NullsLast}
	}
	return keys, nil
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
	summary, err := f.executionSummary()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "\n%s", summary)
	fmt.Fprintf(&b, "\nPHYSICAL strategy=%s workers=%d radix_bits=%d key_width=%d predicate_ops=%d feedback_samples=%d est_selectivity=%.4f est_cardinality=%d", info.Strategy.Kind, info.Strategy.Workers, info.Strategy.RadixBits, info.KeyWidthBytes, info.PredicateOps, info.FeedbackSamples, info.EstimatedSelectivity, info.EstimatedCardinality)
	appendPushdownExplain(&b, report)
	policy, policyErr := f.effectiveExecutionPolicy()
	if policyErr != nil {
		return "", policyErr
	}
	aggregateClass := source.StreamBlocking
	if info.Strategy.Kind == planner.StrategySorted {
		aggregateClass = source.StreamBounded
	} else if policy == engine.ExecutionBounded || policy == engine.ExecutionAuto && f.session.engine.Memory().Limit() > 0 && f.session.engine.SpillConfigured() {
		aggregateClass = source.StreamSpillable
	}
	streamRoot := source.StreamPlanNode{Operator: "Collect", Class: source.StreamMemorySink, Children: []source.StreamPlanNode{streamPlanForLogical(optimized, aggregateClass)}}
	fmt.Fprintf(&b, "\n%s", source.FormatStreamPlan(streamRoot))
	return b.String(), nil
}

func (f LazyFrame) explainRowPhysical(ctx context.Context) (string, error) {
	if f.session == nil || f.session.engine == nil {
		return "", fmt.Errorf("lazy frame has no execution session")
	}
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return "", err
	}
	ctx = engine.WithExecutionPolicy(ctx, policy)
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return "", err
	}
	ctx = withJoinFeedbackKey(ctx, planFeedbackKey(optimized.Plan))
	physicalPlan, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return "", err
	}
	if err := f.validateExecutionLogical(physicalPlan); err != nil {
		return "", err
	}
	src, pending, closers, err := lowerSourcePipeline(ctx, physicalPlan, f.session.engine.Memory(), f.session.engine.KernelSet(), f.session.engine)
	if err != nil {
		return "", err
	}
	defer closeClosers(closers)
	if pending != nil {
		src, err = physical.NewFilterSource(src, pending)
		if err != nil {
			return "", err
		}
	}
	if err := f.validateExecutionSource(src); err != nil {
		return "", err
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
	summary, err := f.executionSummary()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "\n%s", summary)
	fmt.Fprintf(&b, "\nPHYSICAL row-pipeline partitions=%d rows=%d columns=[%s]", len(parts), stats.Rows, strings.Join(schemaColumnNames(src.Schema()), ", "))
	appendPushdownExplain(&b, report)
	streamRoot := source.StreamPlanNode{Operator: "Collect", Class: source.StreamMemorySink, Children: []source.StreamPlanNode{source.StreamPlanOf(src)}}
	fmt.Fprintf(&b, "\n%s", source.FormatStreamPlan(streamRoot))
	pipelineOpts := f.session.engine.PipelineOptions()
	workers := pipeline.EffectiveWorkers(src, parts, pipelineOpts)
	fmt.Fprintf(&b, "\nPIPELINE id=p00 workers=%d ordering=%s target_morsel_bytes=%d", workers, pipelineOpts.Ordering, pipelineOpts.TargetMorselBytes)
	if workers > 1 {
		fmt.Fprintf(&b, "\nEXCHANGE id=x00 pipeline=p00 max_in_flight_bytes=%d max_morsel_bytes=%d estimated_resident_bytes=%d", pipelineOpts.MaxInFlightBytes, pipelineOpts.MaxMorselBytes, pipelineOpts.MaxInFlightBytes)
	}
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

// PrepareResult creates the legacy specialized aggregate query. New code that
// needs a DataFrame should use Prepare.
func (f LazyFrame) PrepareResult(ctx context.Context, opts engine.PrepareOptions) (*PreparedResult, error) {
	query, _, _, closers, err := f.prepare(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &PreparedResult{query: query, closers: closers}, nil
}

// CollectResult executes the legacy specialized aggregate path and returns
// engine diagnostics. New code should use Collect for ordinary DataFrames.
func (f LazyFrame) CollectResult(ctx context.Context) (engine.Result, error) {
	prepared, err := f.PrepareResult(ctx, engine.PrepareOptions{})
	if err != nil {
		return engine.Result{}, err
	}
	defer prepared.Close()
	return prepared.Execute(ctx, engine.ExecuteOptions{Materialize: true})
}

// Execute runs the prepared physical plan with caller-selected materialization.
func (p *PreparedResult) Execute(ctx context.Context, opts engine.ExecuteOptions) (engine.Result, error) {
	if p == nil || p.query == nil {
		return engine.Result{}, fmt.Errorf("lazy prepared query is nil")
	}
	return p.query.Execute(ctx, opts)
}

// Plan returns diagnostics from the underlying physical prepared query.
func (p *PreparedResult) Plan() engine.PlanInfo {
	if p == nil || p.query == nil {
		return engine.PlanInfo{}
	}
	return p.query.Plan()
}

// Close releases retained workspaces owned by the prepared query.
func (p *PreparedResult) Close() error {
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
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return nil, nil, source.PushdownReport{}, nil, err
	}
	ctx = engine.WithExecutionPolicy(ctx, policy)
	optimized, err := optimizer.Optimize(f.plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, nil, err
	}
	if opts.FeedbackKey == "" {
		opts.FeedbackKey = planFeedbackKey(optimized.Plan)
	}
	ctx = withJoinFeedbackKey(ctx, planFeedbackKey(optimized.Plan))
	physicalLogical, report, err := applyPushdown(ctx, optimized.Plan)
	if err != nil {
		return nil, nil, source.PushdownReport{}, nil, err
	}
	if err := f.validateExecutionLogical(physicalLogical); err != nil {
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
		physicalSource, query.Filter, closers, err = lowerSourcePipeline(ctx, agg.Input, f.session.engine.Memory(), f.session.engine.KernelSet(), f.session.engine)
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
	case *logical.Window:
		child, report, err := applyPushdown(ctx, n.Input)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		return &logical.Window{Input: child, Spec: logical.WindowSpec{PartitionBy: append([]string(nil), n.Spec.PartitionBy...), By: append([]logical.SortKey(nil), n.Spec.By...)}, Exprs: append([]logical.WindowExpr(nil), n.Exprs...)}, report, nil
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
	case *logical.Sort, *logical.TopK, *logical.Limit, *logical.Window:
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

// canSplitTopKInput identifies row-preserving pipelines that can be lowered
// twice: once with only ordering-key dependencies and once with the complete
// payload. Joins and aggregate-result sources are intentionally excluded
// because their prepared build/aggregation work should be reused, not repeated
// solely to narrow a Top-K first pass.
func canSplitTopKInput(plan logical.Plan) bool {
	switch n := plan.(type) {
	case *logical.Scan:
		return true
	case *logical.Filter:
		return canSplitTopKInput(n.Input)
	case *logical.Projection:
		return canSplitTopKInput(n.Input)
	case *logical.ExprProjection:
		return canSplitTopKInput(n.Input)
	default:
		return false
	}
}

// lowerTopKKeySource creates an independent, narrow replay of a row-preserving
// Top-K input. Re-optimizing the added key projection lets PushdownSource
// implementations avoid decoding non-ordering payload columns on the first
// Top-K pass. The residual filter is replayed on both inputs, preserving the
// row offsets used to gather payload rows from winning partitions.
func lowerTopKKeySource(ctx context.Context, input logical.Plan, order orderKeyPlan, manager memory.Manager, kernels kernel.Set, exec join.ParallelExecutor) (source.Source, error) {
	keyPlan, err := optimizer.Optimize(order.keyPlan(input))
	if err != nil {
		return nil, err
	}
	physicalPlan, _, err := applyPushdown(ctx, keyPlan.Plan)
	if err != nil {
		return nil, err
	}
	keySrc, pending, closers, err := lowerSourcePipeline(ctx, physicalPlan, manager, kernels, exec)
	if err != nil {
		return nil, err
	}
	if len(closers) != 0 {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
		return nil, fmt.Errorf("top-k key source unexpectedly retains physical resources")
	}
	if pending == nil {
		return keySrc, nil
	}
	childID := profileSourceID(keySrc)
	keySrc, err = physical.NewFilterSource(keySrc, pending)
	if err != nil {
		return nil, err
	}
	return profileSource(ctx, keySrc, "filter", "residual filter before top-k keys", childID), nil
}

const internalOrderKeyPrefix = "__gocolumn_order_key_"

// orderKeyPlan describes the hidden physical columns that back logical
// ordering expressions. Direct columns keep their existing fast path; only
// computed expressions are materialized and later removed from output.
type orderKeyPlan struct {
	computed      []expr.NamedExpr
	keys          []physical.SortKey
	keyColumns    []string
	hiddenColumns []string
}

func newOrderKeyPlan(schema memory.Schema, by []logical.SortKey) (orderKeyPlan, error) {
	plan := orderKeyPlan{
		keys:          make([]physical.SortKey, len(by)),
		keyColumns:    make([]string, 0, len(by)),
		hiddenColumns: make([]string, 0, len(by)),
	}
	used := make(map[string]struct{}, len(schema.Fields)+len(by))
	for _, field := range schema.Fields {
		used[field.Name] = struct{}{}
	}
	keySeen := make(map[string]struct{}, len(by))
	for i, key := range by {
		value, err := key.Expression()
		if err != nil {
			return orderKeyPlan{}, fmt.Errorf("ordering key %d: %w", i, err)
		}
		if _, err := expr.InferScalarType(schema, value); err != nil {
			return orderKeyPlan{}, fmt.Errorf("ordering key %d: %w", i, err)
		}
		column := ""
		if direct, ok := value.(expr.Column); ok {
			column = direct.Name
		} else {
			base := fmt.Sprintf("%s%d", internalOrderKeyPrefix, i)
			column = base
			for suffix := 1; ; suffix++ {
				if _, exists := used[column]; !exists {
					break
				}
				column = fmt.Sprintf("%s_%d", base, suffix)
			}
			used[column] = struct{}{}
			plan.computed = append(plan.computed, expr.Named(value, column))
			plan.hiddenColumns = append(plan.hiddenColumns, column)
		}
		plan.keys[i] = physical.SortKey{Column: column, Descending: key.Descending, NullsLast: key.NullsLast}
		if _, exists := keySeen[column]; !exists {
			keySeen[column] = struct{}{}
			plan.keyColumns = append(plan.keyColumns, column)
		}
	}
	return plan, nil
}

func (p orderKeyPlan) materialize(input source.Source) (source.Source, error) {
	if len(p.computed) == 0 {
		return input, nil
	}
	return physical.NewProjectSource(input, p.computed, true)
}

func (p orderKeyPlan) hide(input source.Source) (source.Source, error) {
	return physical.NewDropColumnsSource(input, p.hiddenColumns)
}

func (p orderKeyPlan) keyPlan(input logical.Plan) logical.Plan {
	if len(p.computed) == 0 {
		return &logical.Projection{Input: input, Columns: append([]string(nil), p.keyColumns...)}
	}
	return &logical.Projection{
		Input:   &logical.ExprProjection{Input: input, Exprs: append([]expr.NamedExpr(nil), p.computed...), IncludeInput: true},
		Columns: append([]string(nil), p.keyColumns...),
	}
}

func lowerSourcePipeline(ctx context.Context, plan logical.Plan, manager memory.Manager, kernels kernel.Set, exec join.ParallelExecutor) (source.Source, expr.Expr, []io.Closer, error) {
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
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		return src, expr.MergeAnd(pending, n.Predicate), closers, nil
	case *logical.Projection:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
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
		projected, err := physical.NewProjectSource(src, named, false, manager)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, projected, "projection", "column projection", childID), nil, closers, nil
	case *logical.ExprProjection:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				return nil, nil, nil, err
			}
			src = profileSource(ctx, src, "filter", "residual filter", childID)
		}
		childID := profileSourceID(src)
		projected, err := physical.NewProjectSource(src, n.Exprs, n.IncludeInput, manager)
		if err != nil {
			return nil, nil, closers, err
		}
		return profileSource(ctx, projected, "projection", "expression projection", childID), nil, closers, nil
	case *logical.Sort:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before sort", childID)
		}
		order, err := newOrderKeyPlan(src.Schema(), n.By)
		if err != nil {
			return nil, nil, closers, err
		}
		keyed, err := order.materialize(src)
		if err != nil {
			return nil, nil, closers, err
		}
		if keyed != src {
			keyed = profileSource(ctx, keyed, "projection", "internal expression sort keys", profileSourceID(src))
		}
		childID := profileSourceID(keyed)
		workers := 1
		if exec != nil {
			workers = exec.MaxWorkers()
		}
		spillable := false
		if eng, ok := exec.(*engine.Engine); ok && eng != nil {
			policy := engine.ExecutionPolicyFromContext(ctx, eng.ExecutionPolicy())
			spillable = policy == engine.ExecutionBounded && eng.SpillConfigured() ||
				policy == engine.ExecutionAuto && eng.SpillConfigured() && shouldUseExternalOrder(ctx, keyed, eng.Memory().Limit())
		}
		sorted, err := physical.NewSortSourceWithOptions(keyed, order.keys, manager, physical.SortSourceOptions{Workers: workers, Executor: exec, Spillable: spillable})
		if err != nil {
			return nil, nil, closers, err
		}
		sorted = profileSource(ctx, sorted, "sort", "global sort", childID)
		out, err := order.hide(sorted)
		if err != nil {
			return nil, nil, closers, err
		}
		return out, nil, closers, nil
	case *logical.TopK:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before top-k", childID)
		}
		order, err := newOrderKeyPlan(src.Schema(), n.By)
		if err != nil {
			return nil, nil, closers, err
		}
		keyed, err := order.materialize(src)
		if err != nil {
			return nil, nil, closers, err
		}
		if keyed != src {
			keyed = profileSource(ctx, keyed, "projection", "internal expression top-k keys", profileSourceID(src))
		}
		childID := profileSourceID(keyed)
		var top source.Source
		if canSplitTopKInput(n.Input) {
			keySrc, err := lowerTopKKeySource(ctx, n.Input, order, manager, kernels, exec)
			if err != nil {
				return nil, nil, closers, err
			}
			top, err = physical.NewTopKSourceWithKeyInput(keyed, keySrc, n.K, order.keys, manager)
			if err != nil {
				return nil, nil, closers, err
			}
			top = profileSource(ctx, top, "topk", fmt.Sprintf("top-k k=%d", n.K), childID, profileSourceID(keySrc))
		} else {
			top, err = physical.NewTopKSourceWithManager(keyed, n.K, order.keys, manager)
			if err != nil {
				return nil, nil, closers, err
			}
			top = profileSource(ctx, top, "topk", fmt.Sprintf("top-k k=%d", n.K), childID)
		}
		out, err := order.hide(top)
		if err != nil {
			return nil, nil, closers, err
		}
		return out, nil, closers, nil
	case *logical.Limit:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
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
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
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
		policy := engine.ExecutionPolicyFromContext(ctx, eng.ExecutionPolicy())
		spillable := (policy == engine.ExecutionBounded || policy == engine.ExecutionAuto && eng.Memory().Limit() > 0 && eng.SpillConfigured()) && pq.Plan().Strategy.Kind != planner.StrategySorted
		aggSrc, err := newAggregateResultSource(ctx, pq, src, outSchema, n.GroupBy, n.Aggregates, spillable)
		if err != nil {
			_ = pq.Close()
			return nil, nil, closers, err
		}
		closers = append(closers, pq)
		childID := profileSourceID(src)
		return profileSource(ctx, aggSrc, "aggregate", "materialized aggregate source", childID), nil, closers, nil
	case *logical.Window:
		src, pending, closers, err := lowerSourcePipeline(ctx, n.Input, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if pending != nil {
			childID := profileSourceID(src)
			src, err = physical.NewFilterSource(src, pending)
			if err != nil {
				return nil, nil, closers, err
			}
			src = profileSource(ctx, src, "filter", "residual filter before window", childID)
		}
		order, err := newOrderKeyPlan(src.Schema(), n.Spec.By)
		if err != nil {
			return nil, nil, closers, err
		}
		keyed, err := order.materialize(src)
		if err != nil {
			return nil, nil, closers, err
		}
		if keyed != src {
			keyed = profileSource(ctx, keyed, "projection", "internal expression window keys", profileSourceID(src))
		}
		outSchema, err := logical.InferSchema(n)
		if err != nil {
			return nil, nil, closers, err
		}
		outputFields := make([]memory.Field, len(n.Exprs))
		for i, window := range n.Exprs {
			field, ok := outSchema.Field(window.Alias)
			if !ok {
				return nil, nil, closers, fmt.Errorf("window output %q missing from schema", window.Alias)
			}
			outputFields[i] = field
		}
		mode := physical.WindowExecutionResident
		capable := logicalWindowBoundedCapable(n.Exprs)
		if capable && windowInputOrdered(keyed, n.Spec.PartitionBy, order.keys) {
			mode = physical.WindowExecutionOrdered
		} else if capable {
			if eng, ok := exec.(*engine.Engine); ok && eng != nil {
				policy := engine.ExecutionPolicyFromContext(ctx, eng.ExecutionPolicy())
				if policy == engine.ExecutionBounded && eng.SpillConfigured() ||
					policy == engine.ExecutionAuto && eng.SpillConfigured() && shouldUseExternalOrder(ctx, keyed, eng.Memory().Limit()) {
					mode = physical.WindowExecutionExternalSort
				}
			}
		}
		windowed, err := physical.NewWindowSourceWithOptions(keyed, n.Spec.PartitionBy, order.keys, lowerWindowExpressions(n.Exprs), outputFields, manager, physical.WindowSourceOptions{Mode: mode})
		if err != nil {
			return nil, nil, closers, err
		}
		windowed = profileSource(ctx, windowed, "window", fmt.Sprintf("analytic window mode=%d", mode), profileSourceID(keyed))
		out, err := order.hide(windowed)
		if err != nil {
			return nil, nil, closers, err
		}
		return out, nil, closers, nil
	case *logical.Join:
		left, lp, lc, err := lowerSourcePipeline(ctx, n.Left, manager, kernels, exec)
		if err != nil {
			return nil, nil, nil, err
		}
		if lp != nil {
			childID := profileSourceID(left)
			left, err = physical.NewFilterSource(left, lp)
			if err != nil {
				return nil, nil, nil, err
			}
			left = profileSource(ctx, left, "filter", "left join filter", childID)
		}
		right, rp, rc, err := lowerSourcePipeline(ctx, n.Right, manager, kernels, exec)
		if err != nil {
			for i := len(lc) - 1; i >= 0; i-- {
				_ = lc[i].Close()
			}
			return nil, nil, nil, err
		}
		if rp != nil {
			childID := profileSourceID(right)
			right, err = physical.NewFilterSource(right, rp)
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
		policy := engine.ExecutionPolicyFromContext(ctx, engine.ExecutionAuto)
		engineExecutor, _ := exec.(*engine.Engine)
		spillConfigured := engineExecutor != nil && engineExecutor.SpillConfigured()
		finiteMemory := engineExecutor != nil && engineExecutor.Memory().Limit() > 0
		joinObserver := observer.Observer(observer.Nop{})
		var joinFeedback func(physical.JoinRuntimeFeedback)
		if engineExecutor != nil {
			joinObserver = engineExecutor.Observer()
			feedbackKey := joinFeedbackKeyFromContext(ctx)
			if feedbackKey == "" {
				feedbackKey = planFeedbackKey(n)
			}
			joinFeedback = func(stats physical.JoinRuntimeFeedback) {
				engineExecutor.ObserveJoinFeedback(
					feedbackKey, stats.BuildBytes, stats.ProbeBytes, stats.ProbeRows,
					stats.OutputRows, stats.SpillBytes, stats.MaxMultiplicity,
				)
			}
		}
		spillable := policy == engine.ExecutionBounded || policy == engine.ExecutionAuto && finiteMemory && spillConfigured
		joined, err := physical.NewJoinSourceWithOptions(ctx, left, right, n.Type, keys, n.Suffix, manager, physical.JoinSourceOptions{
			Kernels: kernels, RadixBits: n.RadixBits, Workers: workers, Executor: exec, AutoParallelRadix: n.RadixBits == 0,
			Spillable: spillable, RequireBounded: policy == engine.ExecutionBounded, SpillConfigured: spillConfigured, Observer: joinObserver,
			Feedback: joinFeedback,
			MemoryLimitBytes: func() int64 {
				if engineExecutor != nil {
					return engineExecutor.Memory().Limit()
				}
				return 0
			}(),
		})
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

func lowerWindowExpressions(expressions []logical.WindowExpr) []physical.WindowExpression {
	out := make([]physical.WindowExpression, len(expressions))
	for i, window := range expressions {
		out[i] = physical.WindowExpression{
			Function: physical.WindowFunction(window.Function),
			Column:   window.Column,
			Alias:    window.Alias,
			Frame: physical.WindowFrame{
				Start: physical.WindowBound{Kind: physical.WindowBoundKind(window.Frame.Start.Kind), Offset: window.Frame.Start.Offset},
				End:   physical.WindowBound{Kind: physical.WindowBoundKind(window.Frame.End.Kind), Offset: window.Frame.End.Offset},
			},
		}
	}
	return out
}

func windowInputOrdered(input source.Source, partitionBy []string, orderBy []physical.SortKey) bool {
	provider, ok := input.(source.OrderingProvider)
	if !ok {
		return false
	}
	ordering := provider.Ordering()
	if len(ordering) < len(partitionBy)+len(orderBy) {
		return false
	}
	for index, column := range partitionBy {
		key := ordering[index]
		if key.Name != column || key.Descending || key.NullsLast {
			return false
		}
	}
	for index, expected := range orderBy {
		key := ordering[len(partitionBy)+index]
		if key.Name != expected.Column || key.Descending != expected.Descending || key.NullsLast != expected.NullsLast {
			return false
		}
	}
	return true
}

func shouldUseExternalOrder(ctx context.Context, input source.Source, limit int64) bool {
	if limit <= 0 {
		return false
	}
	stats, err := input.Stats(ctx)
	if err != nil || stats.Rows < 0 {
		return true
	}
	estimated := stats.Bytes
	if estimated <= 0 {
		rowBytes := pipeline.EstimateSchemaRowBytes(input.Schema())
		if rowBytes <= 0 || stats.Rows > math.MaxInt64/rowBytes {
			return true
		}
		estimated = stats.Rows * rowBytes
	}
	// Resident sorting simultaneously retains input, row ordinals, and gathered
	// output. Keep a conservative four-times headroom before Auto avoids Spill.
	return estimated > limit/4
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
