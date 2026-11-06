package lazy

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestResultTerminalMaterializesNullableAggregateRecords(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 99, 99}, Validity: memory.Bitmap{7}}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 20, 999, 4, 999}, Validity: memory.Bitmap{11}}
	batch.Reset(5)
	input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32, Nullable: true}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}), batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	query := session.FromSource(input).GroupBy("key").Agg(Sum("value", "sum"), Count("rows"), CountColumn("value", "values"), Min("value", "min"), Max("value", "max"), Avg("value", "avg"))
	ctx := observer.WithOperatorTrace(context.Background(), observer.NewOperatorTrace())
	prepared, err := query.PrepareResult(ctx, engine.PrepareOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, ok := prepared.frame.src.(*physical.ProfileSource); !ok {
		t.Fatalf("profiled root=%T", prepared.frame.src)
	}
	if prepared.query != preparedAggregateQuery(prepared.frame.src) {
		t.Fatal("terminal did not reuse prepared aggregate")
	}
	result, err := prepared.Execute(ctx, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 3 || result.SelectedRows != 5 || result.Checksum != 34 || len(result.Rows) != 3 || result.Elapsed <= 0 {
		t.Fatalf("result=%+v", result)
	}
	got := make(map[string]engine.AggregateRow)
	for _, row := range result.Rows {
		if len(row.Keys) != 1 {
			t.Fatalf("keys=%v", row.Keys)
		}
		key := "NULL"
		if row.Keys[0].Valid {
			if row.Keys[0].Int64 == 1 {
				key = "one"
			} else if row.Keys[0].Int64 == 2 {
				key = "two"
			} else {
				t.Fatalf("key=%+v", row.Keys[0])
			}
		}
		if _, exists := got[key]; exists {
			t.Fatalf("duplicate key %s", key)
		}
		got[key] = row
	}
	expected := map[string]struct {
		sum      int64
		count    uint64
		values   int64
		min, max int64
		avg      float64
		valid    bool
	}{
		"one": {30, 2, 2, 10, 20, 15, true}, "two": {0, 1, 0, 0, 0, 0, false}, "NULL": {4, 2, 1, 4, 4, 4, true},
	}
	for key, want := range expected {
		row, ok := got[key]
		if !ok {
			t.Fatalf("missing group %s", key)
		}
		if row.Sum != want.sum || row.Count != want.count {
			t.Fatalf("%s row=%+v", key, row)
		}
		values := []engine.AggregateValue{
			{Alias: "sum", Kind: expr.AggSum, Valid: want.valid, Int64: want.sum},
			{Alias: "rows", Kind: expr.AggCountRows, Valid: true, Int64: int64(want.count)},
			{Alias: "values", Kind: expr.AggCountValues, Valid: true, Int64: want.values},
			{Alias: "min", Kind: expr.AggMin, Valid: want.valid, Int64: want.min},
			{Alias: "max", Kind: expr.AggMax, Valid: want.valid, Int64: want.max},
			{Alias: "avg", Kind: expr.AggAvg, Valid: want.valid, Float64: want.avg, IsFloat: true},
		}
		if !reflect.DeepEqual(row.Aggregates, values) {
			t.Fatalf("%s aggregates=%+v want=%+v", key, row.Aggregates, values)
		}
	}
	retained := append([]engine.AggregateRow(nil), result.Rows...)
	for i := range retained {
		retained[i].Keys = append([]engine.KeyPart(nil), retained[i].Keys...)
		retained[i].Aggregates = append([]engine.AggregateValue(nil), retained[i].Aggregates...)
	}
	summary, err := prepared.Execute(ctx, engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Rows != nil || summary.GroupCount != 3 || summary.SelectedRows != 5 || summary.Checksum != 34 || summary.Elapsed <= 0 {
		t.Fatalf("summary=%+v", summary)
	}
	collected, err := query.CollectResult(context.Background())
	if err != nil || len(collected.Rows) != 3 {
		t.Fatalf("CollectResult rows=%v err=%v", collected.Rows, err)
	}
	// Returned records remain caller-owned after later executions and graph Close.
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, retained) {
		t.Fatal("returned result mutated")
	}
	if _, err := prepared.Execute(ctx, engine.ExecuteOptions{}); !errors.Is(err, engine.ErrPreparedClosed) {
		t.Fatalf("closed err=%v", err)
	}
	if session.Engine().Memory().Used() != 0 {
		t.Fatal("terminal leaked reservations")
	}
}

func TestResultTerminalRejectsNonAggregateRoots(t *testing.T) {
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	base := session.FromSource(makeI32Source(t, []int32{1, 1, 2}))
	aggregate := base.GroupBy("key").Agg(Count("n"))
	for _, query := range []LazyFrame{base, aggregate.Sort(Asc("key")), aggregate.Limit(1), aggregate.Select("key")} {
		if prepared, err := query.PrepareResult(context.Background(), engine.PrepareOptions{}); err == nil {
			prepared.Close()
			t.Fatal("nonterminal aggregate accepted")
		} else if !strings.Contains(err.Error(), "requires a terminal Aggregate") {
			t.Fatal(err)
		}
	}
}

func TestResultTerminalSummaryFitsBoundedBudgetWithoutRetainingRows(t *testing.T) {
	const rows = 8192
	batch := memory.NewBatch()
	keys, values := make([]int32, rows), make([]int64, rows)
	for i := range keys {
		keys[i], values[i] = int32(i), 1
	}
	batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
	batch.Int64s["value"] = &memory.Int64Vector{Values: values}
	batch.Reset(rows)
	input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	ordered := &orderedWindowTestSource{Source: input, ordering: []source.OrderingColumn{{Name: "key"}}}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 16 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 1024, MaxMorselBytes: 4096, MaxInFlightBytes: 8192, MinMorselRows: 1, MaxMorselRows: 64}
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(ordered).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	for run := 0; run < 2; run++ {
		result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if result.Rows != nil || result.GroupCount != rows || result.SelectedRows != rows || result.Checksum != rows {
			t.Fatalf("summary=%+v", result)
		}
	}
	if _, err := prepared.Execute(ctx, engine.ExecuteOptions{Materialize: true}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("materialization err=%v", err)
	}
	if session.Engine().Memory().Used() != 0 {
		t.Fatalf("used=%d", session.Engine().Memory().Used())
	}
	// The caller supplied this query budget; the terminal must not close it.
	lease, err := budget.Reserve(ctx, 1)
	if err != nil {
		t.Fatalf("external budget closed: %v", err)
	}
	lease.Release()
	if snapshot := budget.Snapshot(); snapshot.PeakBytes > opts.MemoryLimitBytes {
		t.Fatalf("peak=%d limit=%d", snapshot.PeakBytes, opts.MemoryLimitBytes)
	}
}

type resultBlockingSource struct {
	source.Source
	entered chan struct{}
	closed  atomic.Int64
}

func (s *resultBlockingSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	inner, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	return &resultBlockingStream{owner: s, inner: inner}, nil
}

type resultBlockingStream struct {
	owner           *resultBlockingSource
	inner           source.BatchStream
	started, closed bool
}

func (s *resultBlockingStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if !s.started {
		s.started = true
		close(s.owner.entered)
	}
	<-ctx.Done()
	dst.ClearColumns()
	return false, ctx.Err()
}
func (s *resultBlockingStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.owner.closed.Add(1)
	return s.inner.Close()
}

func TestResultTerminalCloseWaitsForCanceledExecution(t *testing.T) {
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	input := &resultBlockingSource{Source: makeI32Source(t, []int32{1, 2, 3}), entered: make(chan struct{})}
	prepared, err := session.FromSource(input).GroupBy("key").Agg(Count("n")).PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executed := make(chan error, 1)
	go func() { _, err := prepared.Execute(ctx, engine.ExecuteOptions{Materialize: true}); executed <- err }()
	select {
	case <-input.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not enter native stream")
	}
	closed := make(chan error, 1)
	go func() { closed <- prepared.Close() }()
	// Observe Close marking the graph closed while its execution lease remains held.
	deadline := time.After(5 * time.Second)
	for {
		prepared.frame.lifeMu.Lock()
		closing := prepared.frame.closed
		prepared.frame.lifeMu.Unlock()
		if closing {
			break
		}
		select {
		case err := <-closed:
			t.Fatalf("Close returned before execution: %v", err)
		case <-deadline:
			t.Fatal("Close did not mark the graph closed")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before cancellation: %v", err)
	default:
	}
	secondClosed := make(chan error, 1)
	go func() { secondClosed <- prepared.Close() }()
	select {
	case err := <-secondClosed:
		t.Fatalf("second Close returned before execution completed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	if err := <-executed; !errors.Is(err, context.Canceled) {
		t.Fatalf("execution err=%v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := <-secondClosed; err != nil {
		t.Fatal(err)
	}
	if input.closed.Load() != 1 || session.Engine().Memory().Used() != 0 {
		t.Fatalf("closed=%d used=%d", input.closed.Load(), session.Engine().Memory().Used())
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Execute(context.Background(), engine.ExecuteOptions{}); !errors.Is(err, engine.ErrPreparedClosed) {
		t.Fatalf("closed execution err=%v", err)
	}
}

type resultCloseFunc func() error

func (f resultCloseFunc) Close() error { return f() }

func TestPreparedDataFrameConcurrentCloseSharesCleanupError(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("cleanup failure")
	var calls atomic.Int64
	prepared := &PreparedDataFrame{closers: []io.Closer{resultCloseFunc(func() error {
		calls.Add(1)
		close(entered)
		<-release
		return failure
	})}}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- prepared.Close() }()
	<-entered
	go func() { second <- prepared.Close() }()
	select {
	case err := <-second:
		close(release)
		t.Fatalf("second Close returned before cleanup: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-first; !errors.Is(err, failure) {
		t.Fatalf("first error=%v", err)
	}
	if err := <-second; !errors.Is(err, failure) {
		t.Fatalf("second error=%v", err)
	}
	if err := prepared.Close(); !errors.Is(err, failure) {
		t.Fatalf("repeated error=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("cleanup calls=%d", calls.Load())
	}
}
