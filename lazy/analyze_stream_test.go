package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This source reuses one borrowed buffer for arbitrarily many output batches.
// Its sole reservation belongs to the stream, not to the diagnostic sink.
type analyzeStreamSource struct {
	rows, batches, emptyBatches int
	reserveBytes                int64
	openErr, nextErr, closeErr  error
	statsErr, partsErr          error
	errAt                       int
	closeErrOnOpen              int
	beforeNext                  func(int)
	opens, nexts, closes        int
	borrowed                    *analyzeBorrowedValue
	last                        *analyzeStream
}

func (s *analyzeStreamSource) Replayability() source.Replayability { return source.ReplayStable }

func (s *analyzeStreamSource) Schema() memory.Schema {
	fields := []memory.Field{{Name: "x", Type: memory.TypeInt64}}
	if s.borrowed != nil {
		fields = append(fields, memory.Field{Name: "opaque", Type: memory.TypeArrow})
	}
	return memory.NewSchema(fields...)
}

func (s *analyzeStreamSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.rows * s.batches), Partitions: 1}, s.statsErr
}

func (s *analyzeStreamSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.rows * s.batches)}}, s.partsErr
}

func (s *analyzeStreamSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.openErr != nil {
		return nil, s.openErr
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r := &analyzeStream{owner: s, values: make([]int64, s.rows), openIndex: s.opens}
	if s.reserveBytes > 0 {
		var err error
		r.reservation, err = opts.MemoryManager.Reserve(ctx, s.reserveBytes)
		if err != nil {
			return nil, err
		}
	}
	s.last = r
	return r, nil
}

type analyzeStream struct {
	owner       *analyzeStreamSource
	values      []int64
	index       int
	openIndex   int
	closed      bool
	reservation memory.Reservation
}

func (s *analyzeStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.owner.nexts++
	if s.owner.beforeNext != nil {
		s.owner.beforeNext(s.index)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	dst.ClearColumns()
	if s.owner.nextErr != nil && s.index == s.owner.errAt {
		return false, s.owner.nextErr
	}
	if s.index == s.owner.batches+s.owner.emptyBatches {
		return false, nil
	}
	s.index++
	if s.index <= s.owner.emptyBatches {
		dst.Reset(0)
		return true, nil
	}
	for i := range s.values {
		s.values[i] = int64(i)
	}
	dst.Int64s["x"] = &memory.Int64Vector{Values: s.values}
	if s.owner.borrowed != nil {
		dst.Opaque["opaque"] = &memory.OpaqueVector{Rows: len(s.values), Type: memory.TypeArrow, Value: s.owner.borrowed}
	}
	dst.Reset(len(s.values))
	return true, nil
}

func (s *analyzeStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes++
		if s.reservation != nil {
			s.reservation.Release()
		}
		if s.owner.borrowed != nil {
			s.owner.borrowed.Release()
		}
	}
	if s.owner.closeErrOnOpen != 0 && s.owner.closeErrOnOpen != s.openIndex {
		return nil
	}
	return s.owner.closeErr
}

func (s *analyzeStream) StreamDiagnostics() map[string]string {
	return map[string]string{"fixture_nexts": strconv.Itoa(s.owner.nexts), "fixture_closed": strconv.FormatBool(s.closed)}
}

type analyzeBorrowedValue struct{ releases int }

func (v *analyzeBorrowedValue) Release() { v.releases++ }

func assertAnalyzeReleased(t *testing.T, session *Session, src *analyzeStreamSource) {
	t.Helper()
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("analyze leaked %d bytes", used)
	}
	if src.opens != 1 || src.closes != 1 {
		t.Fatalf("stream opens=%d closes=%d, want one of each", src.opens, src.closes)
	}
}

func TestAnalyzeStreamsLargeOutputUnderBoundedBudget(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 4096
	session := NewSession(opts)
	defer session.Close()
	newSource := func() *analyzeStreamSource {
		return &analyzeStreamSource{rows: 128, batches: 8192, reserveBytes: 1024}
	}
	src := newSource()
	report, err := session.FromSource(src).WithExecution(ExecutionOptions{Policy: ExecutionBounded}).Analyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Termination != AnalyzeCompleted || report.OutputRows != 128*8192 || report.OutputBatches != 8192 {
		t.Fatalf("report=%+v", report)
	}
	if report.FirstBatchDuration <= 0 || report.FirstBatchDuration > report.ExecuteDuration {
		t.Fatalf("first batch=%s execution=%s", report.FirstBatchDuration, report.ExecuteDuration)
	}
	if len(report.ExecuteOperators) != 1 || report.ExecuteOperators[0].Calls != 8193 {
		t.Fatalf("diagnostics must aggregate per operator, not per batch: %+v", report.ExecuteOperators)
	}
	if !strings.Contains(report.PhysicalPlan, "STREAM PIPELINE Analyze") || strings.Contains(report.PhysicalPlan, "MEMORY_SINK") {
		t.Fatalf("physical plan=%s", report.PhysicalPlan)
	}
	assertAnalyzeReleased(t, session, src)

	collected := newSource()
	_, err = session.FromSource(collected).Collect(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("materializing the same output should exceed budget: %v", err)
	}
	assertAnalyzeReleased(t, session, collected)
}

func TestAnalyzeBorrowedOpaqueOutputIsNotRetainedOrReleasedBySink(t *testing.T) {
	value := &analyzeBorrowedValue{}
	src := &analyzeStreamSource{rows: 2, batches: 4, borrowed: value}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	report, err := session.FromSource(src).Analyze(context.Background())
	if err != nil || report.OutputRows != 8 || value.releases != 1 {
		t.Fatalf("report=%+v error=%v releases=%d", report, err, value.releases)
	}
	assertAnalyzeReleased(t, session, src)
}

func TestAnalyzeEmptyOutputAndFirstNonemptyBatchLatency(t *testing.T) {
	for _, batches := range []int{0, 1} {
		t.Run(fmt.Sprintf("batches=%d", batches), func(t *testing.T) {
			var firstNonempty time.Time
			src := &analyzeStreamSource{rows: 3, batches: batches, emptyBatches: 2}
			src.beforeNext = func(index int) {
				if index == 2 && batches > 0 {
					time.Sleep(5 * time.Millisecond)
					firstNonempty = time.Now()
				}
			}
			session := NewSession(engine.DefaultOptions())
			defer session.Close()
			report, err := session.FromSource(src).Analyze(context.Background())
			if err != nil || report.OutputRows != int64(3*batches) || report.OutputBatches != int64(batches) {
				t.Fatalf("report=%+v error=%v", report, err)
			}
			if batches == 0 && report.FirstBatchDuration != 0 {
				t.Fatalf("empty result has first-batch latency %s", report.FirstBatchDuration)
			}
			if batches > 0 && (firstNonempty.IsZero() || report.FirstBatchDuration < 5*time.Millisecond || report.FirstBatchDuration > report.ExecuteDuration) {
				t.Fatalf("nonempty first-batch latency=%s execute=%s", report.FirstBatchDuration, report.ExecuteDuration)
			}
			assertAnalyzeReleased(t, session, src)
		})
	}
}

func TestAnalyzePreservesPartialRowsNextAndCloseErrors(t *testing.T) {
	nextErr, closeErr := errors.New("next failure"), errors.New("close failure")
	for _, failNext := range []bool{false, true} {
		t.Run(fmt.Sprintf("next=%t", failNext), func(t *testing.T) {
			src := &analyzeStreamSource{rows: 3, batches: 4, closeErr: closeErr, reserveBytes: 32}
			wantRows, wantTermination := int64(12), AnalyzeCleanupFailed
			if failNext {
				src.nextErr, src.errAt = nextErr, 2
				wantRows, wantTermination = 6, AnalyzeFailed
			}
			session := NewSession(engine.DefaultOptions())
			defer session.Close()
			report, err := session.FromSource(src).Analyze(context.Background())
			if !errors.Is(err, closeErr) || !errors.Is(report.CleanupError, closeErr) || report.Termination != wantTermination || report.OutputRows != wantRows {
				t.Fatalf("report=%+v error=%v", report, err)
			}
			if failNext && (!errors.Is(err, nextErr) || !errors.Is(report.Error, nextErr) || report.FailurePhase != observer.PhaseExecute) {
				t.Fatalf("missing primary failure: report=%+v error=%v", report, err)
			}
			if report.LogicalPlan == "" || report.OptimizedPlan == "" || report.PhysicalPlan == "" || report.FirstBatchDuration <= 0 || len(report.ExecuteOperators) != 1 {
				t.Fatalf("missing partial diagnostics: %+v", report)
			}
			attrs := report.ExecuteOperators[0].Attributes
			if attrs["fixture_closed"] != "true" || attrs["close_error"] != closeErr.Error() {
				t.Fatalf("diagnostics not finalized after Close: %v", attrs)
			}
			assertAnalyzeReleased(t, session, src)
		})
	}
}

func TestExplainAnalyzeReturnsPartialTextWithError(t *testing.T) {
	failure := errors.New("explain failure")
	src := &analyzeStreamSource{rows: 2, batches: 2, nextErr: failure, errAt: 1}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	text, err := session.FromSource(src).ExplainAnalyze(context.Background())
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	for _, want := range []string{"output_rows=2", "termination=failed", "failure_phase=execute", "explain failure", "EXECUTE OPERATORS"} {
		if !strings.Contains(text, want) {
			t.Fatalf("partial text missing %q:\n%s", want, text)
		}
	}
	assertAnalyzeReleased(t, session, src)
}

func TestAnalyzeOpenAndPreparationFailuresReturnDiagnostics(t *testing.T) {
	failure := errors.New("injected failure")
	for _, stage := range []string{"open", "partitions", "stats", "nil-context", "invalid-plan"} {
		t.Run(stage, func(t *testing.T) {
			src := &analyzeStreamSource{rows: 2, batches: 2}
			session := NewSession(engine.DefaultOptions())
			defer session.Close()
			frame := session.FromSource(src)
			ctx := context.Background()
			switch stage {
			case "open":
				src.openErr = failure
			case "partitions":
				src.partsErr = failure
			case "stats":
				src.statsErr = failure
			case "nil-context":
				ctx = nil
			case "invalid-plan":
				frame = frame.Select("missing")
			}
			report, err := frame.Analyze(ctx)
			if err == nil || report.Error == nil || report.LogicalPlan == "" || report.Termination != AnalyzeFailed {
				t.Fatalf("report=%+v error=%v", report, err)
			}
			wantPhase := observer.PhasePrepare
			if stage == "open" {
				wantPhase = observer.PhaseExecute
				if len(report.ExecuteOperators) != 1 || report.ExecuteOperators[0].Attributes["open_error"] != failure.Error() {
					t.Fatalf("missing open diagnostic: %+v", report.ExecuteOperators)
				}
			}
			if report.FailurePhase != wantPhase || report.OutputRows != 0 || report.FirstBatchDuration != 0 {
				t.Fatalf("report=%+v", report)
			}
			if stage != "nil-context" && stage != "invalid-plan" && (!errors.Is(err, failure) || report.OptimizedPlan == "" || report.PhysicalPlan == "") {
				t.Fatalf("missing available plan: report=%+v error=%v", report, err)
			}
			if src.closes != 0 || session.Engine().Memory().Used() != 0 {
				t.Fatalf("unopened stream was closed or resources leaked")
			}
		})
	}
}

func TestAnalyzeCancellationPreservesPartialReport(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprintf("before=%t", before), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := &analyzeStreamSource{rows: 3, batches: 4, reserveBytes: 32}
			if before {
				cancel()
			} else {
				src.beforeNext = func(index int) {
					if index == 2 {
						cancel()
						// Base cancellation propagation is asynchronous during Next.
						// Return its typed cause deterministically from this source.
						src.nextErr, src.errAt = context.Canceled, 2
					}
				}
			}
			session := NewSession(engine.DefaultOptions())
			defer session.Close()
			report, err := session.FromSource(src).Analyze(ctx)
			if !errors.Is(err, context.Canceled) || report.Termination != AnalyzeCanceled || report.LogicalPlan == "" {
				t.Fatalf("report=%+v error=%v", report, err)
			}
			if before {
				if src.opens != 0 || report.FailurePhase != observer.PhasePrepare {
					t.Fatalf("canceled preparation executed source: %+v", report)
				}
			} else {
				if report.OutputRows != 6 || report.FailurePhase != observer.PhaseExecute || len(report.ExecuteOperators) != 1 {
					t.Fatalf("missing canceled partial report: %+v", report)
				}
				assertAnalyzeReleased(t, session, src)
			}
		})
	}
}

func TestAnalyzeBudgetFailurePreservesOriginalTypedCause(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64
	session := NewSession(opts)
	defer session.Close()
	src := &analyzeStreamSource{rows: 16, batches: 1, reserveBytes: 128}
	report, err := session.FromSource(src).Analyze(context.Background())
	var resourceErr *memory.ResourceError
	if !errors.Is(err, memory.ErrBudgetExceeded) || !errors.As(err, &resourceErr) || report.Termination != AnalyzeBudgetExceeded || report.FailurePhase != observer.PhaseExecute {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if report.PhysicalPlan == "" || len(report.ExecuteOperators) != 1 || session.Engine().Memory().Used() != 0 {
		t.Fatalf("missing budget failure diagnostics or leaked memory: %+v", report)
	}
}

func TestAnalyzeNestedAggregateKeepsOperatorDiagnostics(t *testing.T) {
	src := &analyzeStreamSource{rows: 4, batches: 2}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	report, err := session.FromSource(src).GroupBy("x").Agg(Count("n")).Filter(Col("n").Gt(Lit(1))).Analyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.OutputRows != 4 || report.Strategy != "" || report.GroupCount != 0 || report.SelectedRows != 0 {
		t.Fatalf("nested aggregate must not overwrite terminal summary: %+v", report)
	}
	found := false
	for _, stat := range report.ExecuteOperators {
		if stat.Kind == "aggregate" {
			found = stat.Attributes["strategy"] != "" && stat.Attributes["selected_rows"] == "8"
		}
	}
	if !found {
		t.Fatalf("nested aggregate diagnostics=%+v", report.ExecuteOperators)
	}
	if !strings.Contains(report.String(), "peak_memory_scope=query") {
		t.Fatalf("query-wide peak must be explicitly labeled: %s", report.String())
	}
	for _, stat := range report.ExecuteOperators {
		if _, hasPeak := stat.Attributes["peak_memory_bytes"]; hasPeak && stat.Attributes["peak_memory_scope"] != "query" {
			t.Fatalf("unlabeled built-in query peak: %+v", stat)
		}
	}
	assertAnalyzeReleased(t, session, src)
}

func TestAnalyzeJoinPreparationPreservesReadAndCloseErrors(t *testing.T) {
	nextErr, closeErr := errors.New("build next failure"), errors.New("build close failure")
	left := &analyzeStreamSource{rows: 2, batches: 1}
	build := &analyzeStreamSource{rows: 2, batches: 1}
	right := &analyzeStreamSource{rows: 2, batches: 2, nextErr: nextErr, errAt: 1, closeErr: closeErr}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	joinOptions := JoinOptions{Type: InnerJoin, LeftOn: []string{"x"}, RightOn: []string{"x"}}
	// The first join has already acquired retained state before the outer join
	// fails. Its closer must propagate through the failed parent exactly once.
	leftFrame := session.FromSource(left).Join(session.FromSource(build), joinOptions)
	report, err := leftFrame.Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"x"}, RightOn: []string{"x"}, Suffix: "_outer"}).Analyze(context.Background())
	if !errors.Is(err, nextErr) || !errors.Is(err, closeErr) || report.FailurePhase != observer.PhasePrepare || report.OutputRows != 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if report.OptimizedPlan == "" || report.PhysicalPlan == "" || len(report.PrepareOperators) < 2 || len(report.ExecuteOperators) != 0 {
		t.Fatalf("missing preparation diagnostics or misattributed phase: %+v", report)
	}
	foundClose := false
	for _, stat := range report.PrepareOperators {
		foundClose = foundClose || stat.Attributes["close_error"] == closeErr.Error()
	}
	if !foundClose {
		t.Fatalf("missing nested cleanup error: %+v", report.PrepareOperators)
	}
	if left.opens != 0 {
		t.Fatalf("failed prepare executed probe side %d times", left.opens)
	}
	assertAnalyzeReleased(t, session, right)
	assertAnalyzeReleased(t, session, build)
}

func TestAnalyzeBoundedJoinDoesNotHideBudgetAndCloseFailure(t *testing.T) {
	budgetErr := &memory.ResourceError{Scope: "query", Operator: "fixture", RequestedBytes: 1024, LimitBytes: 512}
	closeErr := errors.New("broadcast close failure")
	left := &analyzeStreamSource{rows: 2, batches: 1}
	right := &analyzeStreamSource{rows: 2, batches: 1, nextErr: budgetErr, errAt: 0, closeErr: closeErr}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
	session := NewSession(opts)
	defer session.Close()
	report, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"x"}, RightOn: []string{"x"}}).Analyze(context.Background())
	var resourceErr *memory.ResourceError
	if !errors.Is(err, memory.ErrBudgetExceeded) || !errors.Is(err, closeErr) || !errors.As(err, &resourceErr) || resourceErr != budgetErr {
		t.Fatalf("lost typed original failures: report=%+v error=%v", report, err)
	}
	if report.Termination != AnalyzeBudgetExceeded || report.FailurePhase != observer.PhasePrepare || left.opens != 0 {
		t.Fatalf("failed cleanup must not enter spill fallback: %+v left opens=%d", report, left.opens)
	}
	assertAnalyzeReleased(t, session, right)
}

func TestPreparedClosePreservesAllCleanupErrorsOnce(t *testing.T) {
	first, second := errors.New("first cleanup"), errors.New("second cleanup")
	var order []int
	p := &PreparedDataFrame{closers: []io.Closer{
		resultCloseFunc(func() error { order = append(order, 1); return first }),
		resultCloseFunc(func() error { order = append(order, 2); return second }),
	}}
	for range 2 {
		err := p.Close()
		if !errors.Is(err, first) || !errors.Is(err, second) {
			t.Fatalf("cleanup error=%v", err)
		}
	}
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("closers must run once in reverse acquisition order: %v", order)
	}
}

func TestAnalyzeTopKReplayCloseFailureIsReturned(t *testing.T) {
	closeErr := errors.New("top-k replay close failure")
	src := &analyzeStreamSource{rows: 4, batches: 2, closeErr: closeErr, closeErrOnOpen: 2}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	report, err := session.FromSource(src).Sort(Asc("x")).Limit(2).Analyze(context.Background())
	if !errors.Is(err, closeErr) || report.Termination != AnalyzeFailed || report.FailurePhase != observer.PhaseExecute || report.OutputRows != 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if src.opens != 2 || src.closes != 2 || session.Engine().Memory().Used() != 0 {
		t.Fatalf("two-pass top-k cleanup: opens=%d closes=%d used=%d", src.opens, src.closes, session.Engine().Memory().Used())
	}
}

func TestLazyExecutionHonorsBorrowedQueryBudget(t *testing.T) {
	for _, operation := range []string{"Analyze", "Collect", "CollectBatches", "SinkBatches"} {
		for _, outcome := range []string{"success", "budget", "next-error", "close-error"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				root := memory.NewEngineBudget(256)
				budget, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 64})
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				caller, err := budget.Operator("Caller", memory.MemoryRetainedState).Reserve(ctx, 8)
				if err != nil {
					t.Fatal(err)
				}
				defer caller.Release()
				failure := errors.New("borrowed budget execution failure")
				src := &analyzeStreamSource{rows: 2, batches: 2, reserveBytes: 16}
				switch outcome {
				case "budget":
					src.reserveBytes = 128
				case "next-error":
					src.nextErr, src.errAt = failure, 1
				case "close-error":
					src.closeErr = failure
				}
				session := NewSession(engine.DefaultOptions())
				defer session.Close()
				frame := session.FromSource(src)
				switch operation {
				case "Analyze":
					_, err = frame.Analyze(ctx)
				case "Collect":
					var data DataFrame
					data, err = frame.Collect(ctx)
					err = errors.Join(err, data.Close())
				case "CollectBatches":
					var reader *BatchReader
					reader, err = frame.CollectBatches(ctx, BatchOptions{})
					if err == nil {
						for reader.Next(ctx) {
						}
						err = reader.Close()
					}
				case "SinkBatches":
					err = frame.SinkBatches(ctx, BatchOptions{}, func(context.Context, BatchView) error { return nil })
				}
				switch outcome {
				case "success":
					if err != nil {
						t.Fatal(err)
					}
				case "budget":
					if !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatalf("external hard limit bypassed: %v", err)
					}
				default:
					if !errors.Is(err, failure) {
						t.Fatalf("lost failure: %v", err)
					}
				}
				if snapshot := budget.Snapshot(); snapshot.Closed || snapshot.UsedBytes != 8 {
					t.Fatalf("execution must release its charges without closing caller budget or releasing caller reservation: %+v", snapshot)
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("session budget leaked")
				}
				reservation, err := budget.Operator("After", memory.MemoryWorkspace).Reserve(ctx, 1)
				if err != nil {
					t.Fatalf("caller budget no longer usable: %v", err)
				}
				reservation.Release()
			})
		}
	}
}

func TestLazyExternalBudgetDoesNotBypassClosedResources(t *testing.T) {
	for _, closed := range []string{"engine", "budget"} {
		for _, operation := range []string{"Analyze", "Execute", "ExecuteBatches"} {
			t.Run(closed+"/"+operation, func(t *testing.T) {
				src := &analyzeStreamSource{rows: 1, batches: 1}
				session := NewSession(engine.DefaultOptions())
				defer session.Close()
				frame := session.FromSource(src)
				prepared, err := frame.Prepare(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				root := memory.NewEngineBudget(128)
				budget, err := root.BeginQuery(memory.BudgetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				want := engine.ErrEngineClosed
				if closed == "engine" {
					session.Close()
				} else {
					budget.Close()
					want = memory.ErrQueryBudgetClosed
				}
				switch operation {
				case "Analyze":
					_, err = frame.Analyze(ctx)
				case "Execute":
					_, err = prepared.Execute(ctx)
				case "ExecuteBatches":
					_, err = prepared.ExecuteBatches(ctx, BatchOptions{})
				}
				if !errors.Is(err, want) || src.opens != 0 {
					t.Fatalf("closed resource bypassed: error=%v opens=%d", err, src.opens)
				}
				if closed == "engine" && budget.Snapshot().Closed {
					t.Fatal("external budget closed by failed execution")
				}
			})
		}
	}
}

func TestLazyBoundedExecutionRejectsUnlimitedExternalBudget(t *testing.T) {
	for _, policySource := range []string{"session", "frame"} {
		for _, operation := range []string{"Analyze", "Execute", "ExecuteBatches"} {
			t.Run(policySource+"/"+operation, func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.MemoryLimitBytes = 64
				if policySource == "session" {
					opts.ExecutionPolicy = engine.ExecutionBounded
				}
				session := NewSession(opts)
				defer session.Close()
				src := &analyzeStreamSource{rows: 1, batches: 1, reserveBytes: 128}
				frame := session.FromSource(src)
				if policySource == "frame" {
					frame = frame.WithExecution(ExecutionOptions{Policy: ExecutionBounded})
				}
				prepared, err := frame.Prepare(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				budget, err := memory.NewEngineBudget(0).BeginQuery(memory.BudgetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				switch operation {
				case "Analyze":
					_, err = frame.Analyze(ctx)
				case "Execute":
					_, err = prepared.Execute(ctx)
				case "ExecuteBatches":
					_, err = prepared.ExecuteBatches(ctx, BatchOptions{})
				}
				if !errors.Is(err, ErrBoundedMemoryLimitRequired) || src.opens != 0 || budget.Used() != 0 || budget.Snapshot().Closed {
					t.Fatalf("bounded contract bypassed: error=%v opens=%d budget=%+v", err, src.opens, budget.Snapshot())
				}
			})
		}
	}
}

func TestAnalyzeBoundedExternalBudgetWithoutSpillFailsExplicitly(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
	session := NewSession(opts)
	defer session.Close()
	budget, err := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	src := &analyzeStreamSource{rows: 4, batches: 2}
	report, err := session.FromSource(src).GroupBy("x").Agg(Count("n")).Analyze(ctx)
	if err == nil || !strings.Contains(err.Error(), "explicit spill quota") || report.Termination == AnalyzeCompleted || report.PhysicalPlan == "" {
		t.Fatalf("missing explicit spill configuration error: report=%+v error=%v", report, err)
	}
	if budget.Snapshot().Closed || budget.Used() != 0 || session.Engine().Memory().Used() != 0 {
		t.Fatalf("spill failure closed external budget or leaked: %+v", budget.Snapshot())
	}
}

func TestAnalyzeEmptyTerminalAggregateSemantics(t *testing.T) {
	src := &analyzeStreamSource{rows: 2}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	report, err := session.FromSource(src).GroupBy("x").Agg(Count("n")).Analyze(context.Background())
	if err != nil || report.OutputRows != 0 || report.GroupCount != 0 || report.SelectedRows != 0 || report.Strategy == "" || report.FirstBatchDuration != 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	assertAnalyzeReleased(t, session, src)
}

func TestAnalyzeExpiredDeadlineBeforePreparation(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	src := &analyzeStreamSource{rows: 1, batches: 1}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	report, err := session.FromSource(src).Analyze(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || report.Termination != AnalyzeCanceled || report.FailurePhase != observer.PhasePrepare || src.opens != 0 {
		t.Fatalf("report=%+v error=%v opens=%d", report, err, src.opens)
	}
}
