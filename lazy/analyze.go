package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/observer"
	"runtime"
	"strings"
	"time"
)

// AnalyzeReport records one real execution of a lazy plan together with its
// logical, optimized, physical, and operator-level diagnostics. Allocation
// counts are process-wide Go runtime deltas for the measured phase; they are not
// attributed to individual parallel operators because doing so would be
// misleading under concurrent execution.
type AnalyzeReport struct {
	LogicalPlan       string
	OptimizedPlan     string
	PhysicalPlan      string
	PrepareDuration   time.Duration
	ExecuteDuration   time.Duration
	PrepareAllocBytes uint64
	ExecuteAllocBytes uint64
	OutputRows        int64
	SelectedRows      int64
	GroupCount        int
	Strategy          string
	MaterializedRows  int
	PrepareOperators  []observer.OperatorStat
	ExecuteOperators  []observer.OperatorStat
}

// String renders the report in a stable human-readable explain-analyze format.
func (r AnalyzeReport) String() string {
	var b strings.Builder
	b.WriteString("LOGICAL\n")
	b.WriteString(r.LogicalPlan)
	b.WriteString("\nOPTIMIZED\n")
	b.WriteString(r.OptimizedPlan)
	b.WriteString("\nPHYSICAL\n")
	b.WriteString(r.PhysicalPlan)
	fmt.Fprintf(&b, "\nANALYZE prepare=%s execute=%s prepare_alloc=%dB execute_alloc=%dB output_rows=%d", r.PrepareDuration, r.ExecuteDuration, r.PrepareAllocBytes, r.ExecuteAllocBytes, r.OutputRows)
	if r.GroupCount != 0 || r.SelectedRows != 0 {
		fmt.Fprintf(&b, " groups=%d selected_rows=%d", r.GroupCount, r.SelectedRows)
	}
	if r.Strategy != "" {
		fmt.Fprintf(&b, " strategy=%s", r.Strategy)
	}
	appendOperatorStats(&b, "PREPARE OPERATORS", r.PrepareOperators)
	appendOperatorStats(&b, "EXECUTE OPERATORS", r.ExecuteOperators)
	return b.String()
}

func appendOperatorStats(b *strings.Builder, title string, stats []observer.OperatorStat) {
	if len(stats) == 0 {
		return
	}
	b.WriteString("\n")
	b.WriteString(title)
	b.WriteString("\n")
	b.WriteString("id    kind        calls   input_rows   output_rows   output_bytes   inclusive      self           label\n")
	for _, s := range stats {
		fmt.Fprintf(b, "%-5s %-11s %5d %12d %13d %14d %13s %13s   %s\n", s.ID, s.Kind, s.Calls, s.InputRows, s.OutputRows, s.OutputBytes, s.Inclusive, s.SelfTime, s.Label)
	}
}

func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// Analyze executes f exactly once and returns plan text plus measured prepare,
// execution, allocation, and operator-level diagnostics. Profiling wrappers are
// constructed only for this call; ordinary Collect/Prepare hot paths contain no
// operator timing branches.
func (f LazyFrame) Analyze(ctx context.Context) (AnalyzeReport, error) {
	if f.err != nil {
		return AnalyzeReport{}, f.err
	}
	report := AnalyzeReport{LogicalPlan: f.Explain()}
	optimizedText, err := f.ExplainOptimized()
	if err != nil {
		return AnalyzeReport{}, err
	}
	report.OptimizedPlan = optimizedText
	trace := observer.NewOperatorTrace()
	prepareCtx := observer.WithOperatorTrace(observer.WithPhase(ctx, observer.PhasePrepare), trace)

	if _, ok := f.plan.(*logical.Aggregate); ok {
		alloc0 := totalAlloc()
		start := time.Now()
		query, physicalLogical, pushdown, closers, err := f.prepare(prepareCtx, engine.PrepareOptions{})
		report.PrepareDuration = time.Since(start)
		report.PrepareAllocBytes = totalAlloc() - alloc0
		if err != nil {
			return AnalyzeReport{}, err
		}
		defer closePreparedResources(query, closers)
		info := query.Plan()
		report.Strategy = info.Strategy.Kind.String()
		var physical strings.Builder
		physical.WriteString(logical.Explain(physicalLogical))
		fmt.Fprintf(&physical, "\nPHYSICAL strategy=%s workers=%d radix_bits=%d key_width=%d predicate_ops=%d feedback_samples=%d est_selectivity=%.4f est_cardinality=%d", info.Strategy.Kind, info.Strategy.Workers, info.Strategy.RadixBits, info.KeyWidthBytes, info.PredicateOps, info.FeedbackSamples, info.EstimatedSelectivity, info.EstimatedCardinality)
		appendPushdownExplain(&physical, pushdown)
		report.PhysicalPlan = physical.String()

		execCtx := observer.WithOperatorTrace(observer.WithPhase(ctx, observer.PhaseExecute), trace)
		alloc0 = totalAlloc()
		start = time.Now()
		result, err := query.Execute(execCtx, engine.ExecuteOptions{Materialize: false})
		report.ExecuteDuration = time.Since(start)
		report.ExecuteAllocBytes = totalAlloc() - alloc0
		if err != nil {
			return AnalyzeReport{}, err
		}
		report.OutputRows = int64(result.GroupCount)
		report.GroupCount = result.GroupCount
		report.SelectedRows = int64(result.SelectedRows)
		report.PrepareOperators = trace.Stats(observer.PhasePrepare)
		report.ExecuteOperators = trace.Stats(observer.PhaseExecute)
		return report, nil
	}

	alloc0 := totalAlloc()
	start := time.Now()
	prepared, physicalLogical, pushdown, err := f.prepareFrame(prepareCtx)
	report.PrepareDuration = time.Since(start)
	report.PrepareAllocBytes = totalAlloc() - alloc0
	if err != nil {
		return AnalyzeReport{}, err
	}
	defer prepared.Close()
	stats, err := prepared.src.Stats(ctx)
	if err != nil {
		return AnalyzeReport{}, err
	}
	var physical strings.Builder
	physical.WriteString(logical.Explain(physicalLogical))
	fmt.Fprintf(&physical, "\nPHYSICAL row-pipeline partitions=%d rows=%d columns=[%s]", len(prepared.parts), stats.Rows, strings.Join(schemaColumnNames(prepared.schema), ", "))
	appendPushdownExplain(&physical, pushdown)
	report.PhysicalPlan = physical.String()

	execCtx := observer.WithOperatorTrace(observer.WithPhase(ctx, observer.PhaseExecute), trace)
	alloc0 = totalAlloc()
	start = time.Now()
	frame, err := prepared.Execute(execCtx)
	report.ExecuteDuration = time.Since(start)
	report.ExecuteAllocBytes = totalAlloc() - alloc0
	if err != nil {
		return AnalyzeReport{}, err
	}
	report.OutputRows = frame.Rows
	for _, batch := range frame.Batches {
		report.MaterializedRows += batch.Rows
	}
	report.PrepareOperators = trace.Stats(observer.PhasePrepare)
	report.ExecuteOperators = trace.Stats(observer.PhaseExecute)
	return report, nil
}

// ExplainAnalyze executes f once and returns AnalyzeReport.String().
func (f LazyFrame) ExplainAnalyze(ctx context.Context) (string, error) {
	report, err := f.Analyze(ctx)
	if err != nil {
		return "", err
	}
	return report.String(), nil
}
