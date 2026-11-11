package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	parquetSource "gocolumn/source/parquet"
)

const fanOutWindowRows = 4096
const fanOutWindowRowGroupRows = 512
const fanOutWindowFirst = fanOutWindowRowGroupRows
const fanOutWindowEnd = fanOutWindowRows - fanOutWindowRowGroupRows

// The fixture is sorted by (grp, id). Pushdown removes only whole row groups,
// so every retained row satisfies the predicate without a residual filter.
func fanOutWindowValue(row int) (float64, bool) {
	values := [...]float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1, 2, 3, 1e16, 1, -1e16, 0, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64}
	return values[row%len(values)], row%257 >= 8
}

func fanOutWindowParquet(t *testing.T) source.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "grp", Type: memory.TypeInt64},
		memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true},
	)
	input := memory.NewBatch()
	defer input.ReleaseOwned()
	input.Rows = fanOutWindowRows
	input.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, input.Rows)}
	input.Int64s["grp"] = &memory.Int64Vector{Values: make([]int64, input.Rows)}
	input.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, input.Rows), Validity: make(memory.Bitmap, (input.Rows+63)/64)}
	for row := 0; row < input.Rows; row++ {
		input.Int64s["id"].Values[row] = int64(row)
		input.Int64s["grp"].Values[row] = int64(row / 1024)
		value, valid := fanOutWindowValue(row)
		input.Float64s["v"].Values[row] = value
		if valid {
			memory.Set(input.Float64s["v"].Validity, row)
		}
	}
	input.Reset(input.Rows)
	path := filepath.Join(t.TempDir(), "fanout-window.parquet")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	arrowSchema, err := memory.ArrowSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := pqarrow.NewFileWriter(arrowSchema, file, arrowparquet.NewWriterProperties(arrowparquet.WithMaxRowGroupLength(fanOutWindowRowGroupRows)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for offset := 0; offset < input.Rows; offset += fanOutWindowRowGroupRows {
		record, err := memory.ArrowRecordRange(arrowmem.NewGoAllocator(), schema, input, offset, fanOutWindowRowGroupRows)
		if err != nil {
			t.Fatal(err)
		}
		err = writer.Write(record)
		record.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := parquetSource.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if source.ReplayabilityOf(base) != source.ReplayUnknown {
		t.Fatal("Parquet unexpectedly permits independent fan-out rescans")
	}
	// Repeated access to id statistics within one AND evaluation exercises the
	// metadata-cache path as well as the native Parquet scan itself.
	predicate := expr.And(expr.GE(expr.Col("id"), expr.I64(fanOutWindowFirst)), expr.LT(expr.Col("id"), expr.I64(fanOutWindowEnd)))
	selected, report, err := base.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id", "grp", "v"}, Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	if report.TotalPartitions != 8 || report.PrunedPartitions != 2 || report.SelectedPartitions != 6 || report.SelectedRows != fanOutWindowEnd-fanOutWindowFirst {
		t.Fatalf("unexpected whole-row-group pruning: %+v", report)
	}
	return selected
}

// Forward all scan options and partitions unchanged, while proving that the
// multiplexer opens one real Parquet stream, rather than one scan per branch.
type fanOutWindowScanSource struct {
	source.Source
	opens  atomic.Int32
	closes atomic.Int32
}

func (s *fanOutWindowScanSource) Replayability() source.Replayability {
	return source.ReplayabilityOf(s.Source)
}
func (s *fanOutWindowScanSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanOf(s.Source)
}
func (s *fanOutWindowScanSource) NewBatchStream(ctx context.Context, options source.ReadOptions) (source.BatchStream, error) {
	stream, err := source.OpenBatchStream(ctx, s.Source, options)
	return s.wrap(stream, err)
}
func (s *fanOutWindowScanSource) NewBatchStreamWithPartitions(ctx context.Context, options source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	stream, err := source.OpenBatchStreamWithPartitions(ctx, s.Source, options, parts)
	return s.wrap(stream, err)
}
func (s *fanOutWindowScanSource) wrap(stream source.BatchStream, err error) (source.BatchStream, error) {
	if err != nil {
		return nil, err
	}
	s.opens.Add(1)
	return &fanOutWindowScanStream{BatchStream: stream, closes: &s.closes}, nil
}

type fanOutWindowScanStream struct {
	source.BatchStream
	closes *atomic.Int32
	once   sync.Once
	err    error
}

func (s *fanOutWindowScanStream) Close() error {
	s.once.Do(func() {
		s.err = s.BatchStream.Close()
		s.closes.Add(1)
	})
	return s.err
}

// The leaf can only expose its already-open branch, never reopen the Parquet
// source. Its ordering follows the generated fixture and stable fan-out order.
type fanOutWindowBranchSource struct {
	source.Source
	stream source.BatchStream
	opened atomic.Bool
}

func (s *fanOutWindowBranchSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "grp", NullsLast: true}, {Name: "id", NullsLast: true}}
}
func (s *fanOutWindowBranchSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	if !s.opened.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("window attempted to reopen its fan-out branch")
	}
	return s.stream, nil
}

// Use an independent rational oracle, including finite sums that overflow to
// infinity but whose exact averages remain finite. No running float sum is used.
func fanOutWindowCheck(batch *memory.Batch, start, preceding int) error {
	ids, groups, values := batch.Int64s["id"], batch.Int64s["grp"], batch.Float64s["v"]
	sums, averages := batch.Float64s["s"], batch.Float64s["a"]
	if ids == nil || groups == nil || values == nil || sums == nil || averages == nil || batch.Rows <= 0 {
		return fmt.Errorf("incomplete or empty window output")
	}
	for i := 0; i < batch.Rows; i++ {
		row := start + i
		if row >= fanOutWindowEnd || ids.Values[i] != int64(row) || groups.Values[i] != int64(row/1024) {
			return fmt.Errorf("input order/group changed at row %d", row)
		}
		wantValue, wantValid := fanOutWindowValue(row)
		if values.IsValid(i) != wantValid || (wantValid && math.Float64bits(values.Values[i]) != math.Float64bits(wantValue)) {
			return fmt.Errorf("input value/validity changed at row %d", row)
		}
		lo := max(fanOutWindowFirst, (row/1024)*1024, row-preceding)
		exact := new(big.Rat)
		count := int64(0)
		for j := lo; j <= row; j++ {
			value, valid := fanOutWindowValue(j)
			if valid {
				exact.Add(exact, new(big.Rat).SetFloat64(value))
				count++
			}
		}
		if sums.IsValid(i) != (count > 0) || averages.IsValid(i) != (count > 0) {
			return fmt.Errorf("aggregate validity changed at row %d", row)
		}
		if count == 0 {
			continue
		}
		wantSum, _ := exact.Float64()
		exact.Quo(exact, new(big.Rat).SetInt64(count))
		wantAverage, _ := exact.Float64()
		if math.Float64bits(sums.Values[i]) != math.Float64bits(wantSum) || math.Float64bits(averages.Values[i]) != math.Float64bits(wantAverage) {
			return fmt.Errorf("row %d sum=%g want=%g average=%g want=%g", row, sums.Values[i], wantSum, averages.Values[i], wantAverage)
		}
	}
	return nil
}

type fanOutWindowResult struct {
	branch   int
	rows     int
	items    int
	err      error
	closeErr error
}

// Cancellation must be the only error, not merely one member of an upstream
// errors.Join that could also contain a decoder or Close failure.
func fanOutWindowCanceledOnly(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !fanOutWindowCanceledOnly(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return fanOutWindowCanceledOnly(wrapped.Unwrap())
	}
	return err == context.Canceled
}

func fanOutWindowWait(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fanOutWindowConsume(ctx context.Context, cancel context.CancelFunc, mode string, branch int, leaf *fanOutWindowBranchSource, manager memory.Manager, options source.ReadOptions, slowHasFirst, fastAdvanced chan struct{}, results chan<- fanOutWindowResult) {
	result := fanOutWindowResult{branch: branch}
	out := memory.NewBatch()
	var stream source.BatchStream
	defer func() {
		out.ClearColumns()
		if stream != nil {
			result.closeErr = errors.Join(result.closeErr, stream.Close(), stream.Close())
		}
		result.closeErr = errors.Join(result.closeErr, leaf.stream.Close(), leaf.stream.Close())
		if result.err != nil && !(mode == "cancel-live-windows" && fanOutWindowCanceledOnly(result.err)) {
			cancel()
		}
		results <- result
	}()
	preceding := 2
	if branch == 1 {
		preceding = 7
	}
	frame := physical.WindowFrame{Start: physical.WindowBound{Kind: physical.WindowPreceding, Offset: int64(preceding)}, End: physical.WindowBound{Kind: physical.WindowCurrentRow}}
	window, err := physical.NewWindowSourceWithOptions(leaf, []string{"grp"}, []physical.SortKey{{Column: "id", NullsLast: true}}, []physical.WindowExpression{
		{Function: physical.WindowSum, Column: "v", Alias: "s", Frame: frame},
		{Function: physical.WindowAvg, Column: "v", Alias: "a", Frame: frame},
	}, []memory.Field{{Name: "s", Type: memory.TypeFloat64, Nullable: true}, {Name: "a", Type: memory.TypeFloat64, Nullable: true}}, manager, physical.WindowSourceOptions{Mode: physical.WindowExecutionOrdered})
	if err != nil {
		result.err = err
		return
	}
	if plan := source.StreamPlanOf(window); plan.Class != source.StreamBounded {
		result.err = fmt.Errorf("window is not bounded: %+v", plan)
		return
	}
	stream, err = source.OpenBatchStream(ctx, window, options)
	if err != nil {
		result.err = err
		return
	}
	for {
		ok, err := stream.Next(ctx, out)
		if err != nil || !ok {
			result.err = err
			return
		}
		start := fanOutWindowFirst + result.rows
		if err := fanOutWindowCheck(out, start, preceding); err != nil {
			result.err = err
			return
		}
		result.rows += out.Rows
		result.items++
		if branch == 0 && result.items == 1 {
			// Both windows have live first outputs before the fast branch advances.
			if err := fanOutWindowWait(ctx, slowHasFirst); err != nil {
				result.err = err
				return
			}
		}
		if branch == 0 && result.items == 2 {
			close(fastAdvanced)
			if mode == "cancel-live-windows" {
				// Keep this output and the slow branch's first output live until
				// that branch rechecks its data and cancels the shared context.
				<-ctx.Done()
			}
		}
		if branch == 1 && result.items == 1 {
			close(slowHasFirst)
			if err := fanOutWindowWait(ctx, fastAdvanced); err != nil {
				result.err = err
				return
			}
			// The first shared input must remain immutable after a sibling's
			// Next/ACK and the producer's subsequent progress.
			if err := fanOutWindowCheck(out, start, preceding); err != nil {
				result.err = fmt.Errorf("slow retained output: %w", err)
				return
			}
			if mode == "close-slow-branch" {
				return
			}
			if mode == "cancel-live-windows" {
				cancel()
			}
		}
		out.ClearColumns()
	}
}

func TestFanOutWindowParquetIntegration(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)
	base := fanOutWindowParquet(t)
	for _, mode := range []string{"complete-slow-branch", "close-slow-branch", "cancel-live-windows"} {
		t.Run(mode, func(t *testing.T) {
			const queryLimit = 512 << 10
			engine := memory.NewEngineBudget(1 << 30)
			query, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: queryLimit})
			if err != nil {
				t.Fatal(err)
			}
			defer query.Close()
			ctx, cancel := context.WithTimeout(memory.WithQueryBudget(context.Background(), query), 30*time.Second)
			defer cancel()
			scan := &fanOutWindowScanSource{Source: base}
			read := source.ReadOptions{Kernels: kernel.New(kernel.ModeAuto), BatchRows: 256, BatchBytes: 64 << 10, BatchPayloadTargetBytes: 16 << 10}
			opts := pipeline.Options{Workers: 1, TargetMorselBytes: 16 << 10, MaxMorselBytes: 64 << 10, MaxInFlightBytes: 128 << 10, MinMorselRows: 256, MaxMorselRows: 256, Ordering: pipeline.OrderingStable}
			plan, err := pipeline.PlanFanOut(scan, 2, opts.MaxInFlightBytes)
			if err != nil || plan.Strategy != pipeline.FanOutBoundedMultiplexer {
				t.Fatalf("expected one bounded multiplexer: %+v, err=%v", plan, err)
			}
			parts, err := scan.Partitions(ctx)
			if err != nil {
				t.Fatal(err)
			}
			collector := new(observer.Collector)
			branches, err := pipeline.OpenFanOut(ctx, scan, read, parts, query, opts, 2, collector)
			if err != nil {
				t.Fatal(err)
			}
			results := make(chan fanOutWindowResult, 2)
			slowHasFirst, fastAdvanced := make(chan struct{}), make(chan struct{})
			leaves := make([]*fanOutWindowBranchSource, len(branches))
			for branch, raw := range branches {
				leaves[branch] = &fanOutWindowBranchSource{Source: base, stream: raw}
				go fanOutWindowConsume(ctx, cancel, mode, branch, leaves[branch], query, read, slowHasFirst, fastAdvanced, results)
			}
			// This independent bound also catches a Close/ACK deadlock after
			// the query context expires or is deliberately canceled.
			completion := time.NewTimer(45 * time.Second)
			defer completion.Stop()
			finished := make([]bool, len(branches))
			for range branches {
				var result fanOutWindowResult
				select {
				case result = <-results:
					finished[result.branch] = true
				case <-completion.C:
					cancel()
					t.Fatalf("fan-out windows did not finish Next/Close; completed branches=%v", finished)
				}
				if result.closeErr != nil {
					t.Errorf("branch %d Close failed: %v", result.branch, result.closeErr)
				}
				if mode == "cancel-live-windows" {
					if !fanOutWindowCanceledOnly(result.err) || result.rows == 0 || result.rows >= fanOutWindowEnd-fanOutWindowFirst {
						t.Errorf("branch %d expected canceled partial prefix, got %+v", result.branch, result)
					}
				} else if result.err != nil {
					t.Errorf("branch %d failed: %v", result.branch, result.err)
				} else if mode == "close-slow-branch" && result.branch == 1 {
					if result.items != 1 || result.rows <= 0 || result.rows >= fanOutWindowEnd-fanOutWindowFirst {
						t.Errorf("slow branch did not close with exactly one live output: %+v", result)
					}
				} else if result.rows != fanOutWindowEnd-fanOutWindowFirst || result.items < 2 {
					t.Errorf("branch %d incomplete result: %+v", result.branch, result)
				}
			}
			for branch, leaf := range leaves {
				if !leaf.opened.Load() {
					t.Errorf("branch %d never opened its physical window", branch)
				}
			}
			if scan.opens.Load() != 1 || scan.closes.Load() != 1 {
				t.Errorf("Parquet scan opens=%d closes=%d, want one of each", scan.opens.Load(), scan.closes.Load())
			}
			multiplexers := 0
			for _, event := range collector.Events() {
				if event.Stage != "multiplexer" {
					continue
				}
				multiplexers++
				shared, err := strconv.ParseInt(event.Attributes["shared_native_ranges"], 10, 64)
				if err != nil || shared <= 0 || event.Attributes["legacy_opaque_items"] != "0" {
					t.Errorf("native shared path was not exercised: %+v", event)
				}
			}
			if multiplexers != 1 {
				t.Errorf("multiplexer events=%d, want one", multiplexers)
			}
			// Check before Query.Close: closing a budget must not mask live leases.
			snapshot := query.Snapshot()
			if snapshot.UsedBytes != 0 || engine.Used() != 0 || snapshot.PeakBytes <= 0 || snapshot.PeakBytes > queryLimit {
				t.Errorf("budget leaked or exceeded: query=%+v engine=%d", snapshot, engine.Used())
			}
			for class, used := range snapshot.ByClass {
				if used != 0 {
					t.Errorf("class %v retains %d bytes", class, used)
				}
			}
			for name, used := range snapshot.Operators {
				if used != 0 {
					t.Errorf("operator %s retains %d bytes", name, used)
				}
			}
			if err := query.Close(); err != nil {
				t.Fatal(err)
			}
			if err := query.Close(); err != nil || query.Used() != 0 || engine.Used() != 0 {
				t.Errorf("idempotent query close: err=%v query=%d engine=%d", err, query.Used(), engine.Used())
			}
		})
	}
}
