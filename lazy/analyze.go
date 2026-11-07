package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"runtime"
	"sort"
	"strings"
	"time"
)

// AnalyzeTermination describes why one analyzed execution stopped.
type AnalyzeTermination string

const (
	// AnalyzeCompleted means the stream exhausted and all cleanup succeeded.
	AnalyzeCompleted AnalyzeTermination = "completed"
	// AnalyzeFailed means preparation or execution failed; see FailurePhase.
	AnalyzeFailed AnalyzeTermination = "failed"
	// AnalyzeCanceled includes cancellation and deadline expiry.
	AnalyzeCanceled AnalyzeTermination = "canceled"
	// AnalyzeBudgetExceeded means a memory or spill quota was exhausted.
	AnalyzeBudgetExceeded AnalyzeTermination = "budget-exceeded"
	// AnalyzeCleanupFailed means Next exhausted normally but top-level Close failed.
	AnalyzeCleanupFailed AnalyzeTermination = "cleanup-failed"
)

// AnalyzeReport records one real execution without retaining its output. It is
// returned even on failure: row counts describe only successfully observed
// output, and plan/operator diagnostics describe the stages reached. Operator
// snapshots are finalized after cleanup and grow with plan size, not row count.
// Allocation counts are process-wide Go runtime deltas, not exclusive query or
// operator allocation measurements. Built-in peak_memory_bytes attributes are
// marked peak_memory_scope=query: shared budget high-water marks, including any
// caller allocations, not exclusive operator peaks. Do not sum them.
type AnalyzeReport struct {
	LogicalPlan     string
	OptimizedPlan   string
	PhysicalPlan    string
	PrepareDuration time.Duration
	// ExecuteDuration includes opening, draining, and closing the stream and
	// prepared resources. FirstBatchDuration starts at the same stream-open
	// boundary and ends at the first nonempty output batch. It is zero when no
	// nonempty batch was observed, including empty or failed executions.
	ExecuteDuration    time.Duration
	FirstBatchDuration time.Duration
	PrepareAllocBytes  uint64
	ExecuteAllocBytes  uint64
	OutputRows         int64
	// OutputBatches counts nonempty output batches, not zero-row progress calls.
	OutputBatches int64
	// These three summary fields describe only a terminal aggregate. Nested
	// aggregate diagnostics remain attached to their own ExecuteOperators.
	SelectedRows     int64
	GroupCount       int
	Strategy         string
	PrepareOperators []observer.OperatorStat
	ExecuteOperators []observer.OperatorStat
	Termination      AnalyzeTermination
	// FailurePhase is prepare or execute when Error is non-nil; it is empty
	// for success or a cleanup-only failure.
	FailurePhase observer.Phase
	// Error is the failure returned by preparation, stream opening, or Next.
	// CleanupError holds errors from the top-level reader/budget/plan Close.
	// These are API boundaries, not a global attribution: an operator can return
	// nested cleanup failures through Next or preparation. Its close_error
	// diagnostic identifies that origin when the operator is profiled. Both
	// fields remain discoverable with errors.Is/As in Analyze's returned error.
	Error        error
	CleanupError error
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
	fmt.Fprintf(&b, " output_batches=%d first_batch=%s termination=%s", r.OutputBatches, r.FirstBatchDuration, r.Termination)
	if r.FailurePhase != "" {
		fmt.Fprintf(&b, " failure_phase=%s", r.FailurePhase)
	}
	if r.GroupCount != 0 || r.SelectedRows != 0 {
		fmt.Fprintf(&b, " groups=%d selected_rows=%d", r.GroupCount, r.SelectedRows)
	}
	if r.Strategy != "" {
		fmt.Fprintf(&b, " strategy=%s", r.Strategy)
	}
	if r.Error != nil {
		fmt.Fprintf(&b, "\nERROR %v", r.Error)
	}
	if r.CleanupError != nil {
		fmt.Fprintf(&b, "\nCLEANUP ERROR %v", r.CleanupError)
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
		fmt.Fprintf(b, "%-5s %-11s %5d %12d %13d %14d %13s %13s   %s", s.ID, s.Kind, s.Calls, s.InputRows, s.OutputRows, s.OutputBytes, s.Inclusive, s.SelfTime, s.Label)
		if len(s.Attributes) != 0 {
			keys := make([]string, 0, len(s.Attributes))
			for key := range s.Attributes {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				fmt.Fprintf(b, " %s=%s", key, s.Attributes[key])
			}
		}
		b.WriteByte('\n')
	}
}

func totalAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// Analyze prepares once and drains exactly one native execution, borrowing each
// output batch only until the next Next call. It never builds a result DataFrame.
// Blocking operators still need their ordinary state and obey the same budgets
// and spill policy as ExecuteBatches. Every failure returns the available report
// alongside the error; cleanup errors never replace the primary failure. A
// caller-supplied QueryBudget is honored and remains owned by the caller.
func (f LazyFrame) Analyze(ctx context.Context) (report AnalyzeReport, resultErr error) {
	report.LogicalPlan = f.Explain()
	trace := observer.NewOperatorTrace()
	phase := observer.PhasePrepare
	alloc0 := totalAlloc()
	start := time.Now()
	var prepared *PreparedDataFrame
	var reader *BatchReader
	defer func() {
		if reader != nil {
			// Close is idempotent after EOF or a failed Next. Read each category
			// once rather than joining Err and Close (which repeat the same error).
			_ = reader.Close()
			report.Error = reader.err
			report.CleanupError = errors.Join(report.CleanupError, reader.closeErr)
			if terminalAggregate(f.plan) {
				report.GroupCount = int(report.OutputRows)
				report.Strategy = reader.diagnostics["strategy"]
				if report.Strategy == "" {
					report.Strategy = "unknown"
				}
				report.SelectedRows = int64(parseDiagInt(reader.diagnostics, "selected_rows", 0))
			}
		}
		if prepared != nil {
			report.CleanupError = errors.Join(report.CleanupError, prepared.Close())
		}
		if phase == observer.PhasePrepare {
			report.PrepareDuration = time.Since(start)
			report.PrepareAllocBytes = totalAlloc() - alloc0
		} else {
			report.ExecuteDuration = time.Since(start)
			report.ExecuteAllocBytes = totalAlloc() - alloc0
		}
		report.PrepareOperators = trace.Stats(observer.PhasePrepare)
		report.ExecuteOperators = trace.Stats(observer.PhaseExecute)
		report.Termination = AnalyzeCompleted
		if report.Error != nil {
			report.FailurePhase = phase
			switch {
			case errors.Is(report.Error, memory.ErrBudgetExceeded), errors.Is(report.Error, memory.ErrSpillQuotaExceeded):
				report.Termination = AnalyzeBudgetExceeded
			case errors.Is(report.Error, context.Canceled), errors.Is(report.Error, context.DeadlineExceeded):
				report.Termination = AnalyzeCanceled
			default:
				report.Termination = AnalyzeFailed
			}
		} else if report.CleanupError != nil {
			report.Termination = AnalyzeCleanupFailed
		}
		resultErr = errors.Join(report.Error, report.CleanupError)
	}()

	prepareCtx := ctx
	if ctx != nil {
		prepareCtx = observer.WithOperatorTrace(observer.WithPhase(ctx, observer.PhasePrepare), trace)
	}
	var preparation framePreparation
	prepared, preparation, report.Error = f.prepareFrame(prepareCtx)
	report.CleanupError = preparation.cleanupErr
	if preparation.optimized.Plan != nil {
		var optimized strings.Builder
		if len(preparation.optimized.AppliedRules) != 0 {
			fmt.Fprintf(&optimized, "rules=[%s]\n", strings.Join(preparation.optimized.AppliedRules, ", "))
		}
		optimized.WriteString(logical.Explain(preparation.optimized.Plan))
		report.OptimizedPlan = optimized.String()
	}
	var physical strings.Builder
	if preparation.physical != nil {
		physical.WriteString(logical.Explain(preparation.physical))
		appendPushdownExplain(&physical, preparation.pushdown)
		report.PhysicalPlan = physical.String()
	}
	if report.Error != nil {
		return
	}
	// Record the executable graph before reading optional source estimates, so
	// a metadata failure still exposes the physical plan already constructed.
	fmt.Fprintf(&physical, "\nPHYSICAL row-pipeline partitions=%d columns=[%s]", len(prepared.parts), strings.Join(schemaColumnNames(prepared.schema), ", "))
	streamRoot := source.StreamPlanNode{Operator: "Analyze", Class: source.StreamPipeline, Children: []source.StreamPlanNode{source.StreamPlanOf(prepared.src)}}
	fmt.Fprintf(&physical, "\n%s", source.FormatStreamPlan(streamRoot))
	report.PhysicalPlan = physical.String()
	stats, err := prepared.src.Stats(prepareCtx)
	if err != nil {
		report.Error = err
		return
	}
	fmt.Fprintf(&physical, "\nESTIMATE rows=%d", stats.Rows)
	report.PhysicalPlan = physical.String()
	report.PrepareDuration = time.Since(start)
	report.PrepareAllocBytes = totalAlloc() - alloc0

	execCtx := observer.WithOperatorTrace(observer.WithPhase(ctx, observer.PhaseExecute), trace)
	phase = observer.PhaseExecute
	alloc0 = totalAlloc()
	start = time.Now()
	reader, report.Error, report.CleanupError = prepared.openBatches(execCtx, BatchOptions{})
	if report.Error != nil {
		return
	}
	reader.captureDiagnostics = true
	for reader.Next(execCtx) {
		rows := reader.Batch().Len()
		if rows == 0 {
			continue
		}
		if report.OutputBatches == 0 {
			report.FirstBatchDuration = time.Since(start)
		}
		report.OutputBatches++
		report.OutputRows += rows
	}
	return
}

// ExplainAnalyze executes f once and returns the complete or partial report
// text, including when execution or cleanup fails. Callers must check the error.
func (f LazyFrame) ExplainAnalyze(ctx context.Context) (string, error) {
	report, err := f.Analyze(ctx)
	return report.String(), err
}
