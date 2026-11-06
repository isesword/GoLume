package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/planner"
	"gocolumn/source"
	"sync/atomic"
	"testing"
	"unsafe"
)

// This source deliberately has only a native stream: partition metadata can be
// large, but it can never expose or allocate a whole-partition batch.
type generatedStreamSource struct {
	rows                     int
	sorted                   bool
	groups                   int
	opened, closed, maxBatch atomic.Int64
	cancel                   context.CancelFunc
	cancelAt                 int
}

func (s *generatedStreamSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
}
func (s *generatedStreamSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.rows), Partitions: 1}, nil
}
func (s *generatedStreamSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.rows)}}, nil
}
func (s *generatedStreamSource) Ordering() []source.OrderingColumn {
	if s.sorted {
		return []source.OrderingColumn{{Name: "key"}}
	}
	return nil
}
func (s *generatedStreamSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	opts = opts.Normalize()
	rows := min(opts.BatchRows, s.rows)
	if rows < 1 {
		rows = 1
	}
	lease, err := opts.MemoryManager.Reserve(ctx, int64(rows)*12)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &generatedStream{source: s, keys: make([]int32, rows), values: make([]int64, rows), lease: lease}, nil
}
func (s *generatedStreamSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if len(parts) != 1 || parts[0].ID != 0 {
		return nil, fmt.Errorf("unexpected prepared partition selection")
	}
	return s.NewBatchStream(ctx, opts)
}

type generatedStream struct {
	source *generatedStreamSource
	keys   []int32
	values []int64
	lease  memory.Reservation
	offset int
	closed bool
}

func (s *generatedStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.offset == s.source.rows {
		return false, nil
	}
	if s.source.cancel != nil && s.offset >= s.source.cancelAt {
		s.source.cancel()
		return false, ctx.Err()
	}
	n := min(len(s.keys), s.source.rows-s.offset)
	for row := 0; row < n; row++ {
		key := (s.offset + row) % s.source.groups
		if s.source.sorted {
			key = (s.offset + row) * s.source.groups / max(1, s.source.rows)
		}
		s.keys[row], s.values[row] = int32(key), 1
	}
	dst.Int32s["key"] = &memory.Int32Vector{Values: s.keys[:n]}
	dst.Int64s["value"] = &memory.Int64Vector{Values: s.values[:n]}
	dst.Reset(n)
	s.offset += n
	s.source.maxBatch.Store(max(s.source.maxBatch.Load(), int64(n)))
	return true, nil
}
func (s *generatedStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.keys, s.values = nil, nil
	s.lease.Release()
	s.source.closed.Add(1)
	return nil
}
func streamTestOptions() Options {
	options := DefaultOptions()
	options.Workers = 1
	options.MemoryLimitBytes = 128 << 10
	options.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 1024, MaxMorselBytes: 4096, MaxInFlightBytes: 8192, MinMorselRows: 1, MaxMorselRows: 64}
	return options
}
func streamTestQuery() expr.Query {
	return expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("n")}, Hints: expr.Hints{Cardinality: 8, ForceStrategy: "bitmap-direct"}}
}

func TestResultScanScratchIndependentOfPartitionRows(t *testing.T) {
	var peaks []int64
	for _, rows := range []int{4096, 1 << 20} {
		src := &generatedStreamSource{rows: rows, groups: 8}
		eng := New(streamTestOptions())
		prepared, err := eng.Prepare(context.Background(), src, streamTestQuery(), PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for run := 0; run < 2; run++ {
			budget, err := eng.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			result, err := prepared.Execute(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if result.SelectedRows != rows || result.Checksum != int64(rows) || result.GroupCount != 8 || len(result.Rows) != 8 {
				t.Fatalf("result=%+v", result)
			}
			if used := eng.Memory().Used(); used != 0 {
				t.Fatalf("execution retained %d bytes", used)
			}
			if src.maxBatch.Load() > 64 {
				t.Fatalf("unbounded scan batch=%d", src.maxBatch.Load())
			}
			if run == 0 {
				peaks = append(peaks, budget.Snapshot().PeakBytes)
			}
			budget.Close()
		}
		prepared.Close()
		eng.Close()
		if src.opened.Load() != 2 || src.closed.Load() != 2 {
			t.Fatalf("stream lifecycle opened=%d closed=%d", src.opened.Load(), src.closed.Load())
		}
	}
	if peaks[0] != peaks[1] {
		t.Fatalf("scratch scales with partition rows: peaks=%v", peaks)
	}
}

func TestResultCancellationClosesNativeStreamAndReleasesBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &generatedStreamSource{rows: 1 << 20, groups: 8, cancel: cancel, cancelAt: 256}
	eng := New(streamTestOptions())
	defer eng.Close()
	_, err := eng.Execute(ctx, src, streamTestQuery(), ExecuteOptions{Materialize: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if src.closed.Load() != 1 || eng.Memory().Used() != 0 {
		t.Fatalf("closed=%d used=%d", src.closed.Load(), eng.Memory().Used())
	}
}

func TestResultUsesSortedAndSpillNativeStreams(t *testing.T) {
	for _, sorted := range []bool{true, false} {
		t.Run(fmt.Sprint("sorted=", sorted), func(t *testing.T) {
			src := &generatedStreamSource{rows: 8192, groups: 8, sorted: sorted}
			options := streamTestOptions()
			options.ExecutionPolicy = ExecutionBounded
			options.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
			eng := New(options)
			defer eng.Close()
			query := streamTestQuery()
			query.Hints.ForceStrategy = ""
			result, err := eng.Execute(context.Background(), src, query, ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			want := planner.StrategySpillHash
			if sorted {
				want = planner.StrategySorted
			}
			if result.Strategy.Kind != want || result.GroupCount != 8 || result.SelectedRows != 8192 || result.Checksum != 8192 || len(result.Rows) != 8 {
				t.Fatalf("result=%+v", result)
			}
			for _, row := range result.Rows {
				if row.Sum != 1024 || row.Count != 1024 {
					t.Fatalf("row=%+v", row)
				}
			}
			if eng.Memory().Used() != 0 || src.closed.Load() != 1 {
				t.Fatalf("used=%d closed=%d", eng.Memory().Used(), src.closed.Load())
			}
		})
	}
}

func TestResultMaterializationMustFitBudget(t *testing.T) {
	src := &generatedStreamSource{rows: 8192, groups: 8192, sorted: true}
	options := streamTestOptions()
	options.MemoryLimitBytes = 16 << 10
	options.ExecutionPolicy = ExecutionBounded
	eng := New(options)
	defer eng.Close()
	query := streamTestQuery()
	query.Hints.ForceStrategy = ""
	if _, err := eng.Execute(context.Background(), src, query, ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Execute(context.Background(), src, query, ExecuteOptions{Materialize: true}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("materialization error=%v", err)
	}
	if eng.Memory().Used() != 0 {
		t.Fatalf("retained=%d", eng.Memory().Used())
	}
}

func TestResultCollectorReleasesReplacedBackingArray(t *testing.T) {
	budget := memory.NewBudgetManager(1 << 20)
	query := &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeI32, GroupBy: []CompiledGroupColumn{{Name: "key", Type: memory.TypeInt32}}, Aggregates: []expr.Aggregate{expr.Count("n")}}}
	result := Result{}
	collector := resultCollector{query: query, result: &result, budget: budget}
	for row := 0; row < 100; row++ {
		if err := collector.append(context.Background(), aggregateRecord{a: uint64(row), validMask: 1}); err != nil {
			t.Fatal(err)
		}
	}
	want := int64(cap(result.Rows))*int64(unsafe.Sizeof(AggregateRow{})) + int64(len(result.Rows))*int64(unsafe.Sizeof(KeyPart{})+unsafe.Sizeof(AggregateValue{})) + int64(cap(collector.leases))*int64(unsafe.Sizeof(memory.Reservation(nil)))
	if budget.Used() != want {
		t.Fatalf("charged=%d want live=%d", budget.Used(), want)
	}
	collector.close()
	if budget.Used() != 0 {
		t.Fatalf("leaked=%d", budget.Used())
	}
}

func TestResultDoesNotOverridePreparedStrategyWithExternalBudget(t *testing.T) {
	src := &generatedStreamSource{rows: 64, groups: 8}
	options := streamTestOptions()
	options.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
	eng := New(options)
	defer eng.Close()
	prepared, err := eng.Prepare(context.Background(), src, streamTestQuery(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.Plan().Strategy.Kind != planner.StrategySpillHash {
		t.Fatal("expected prepared spill plan")
	}
	other := New(streamTestOptions())
	defer other.Close()
	budget, err := other.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	// The caller's smaller budget lacks spill permission. Do not silently run a
	// resident operator and claim that the prepared spill plan was executed.
	if _, err := prepared.Execute(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{}); err == nil {
		t.Fatal("incompatible execution budget silently changed strategy")
	}
	if eng.Memory().Used() != 0 || other.Memory().Used() != 0 {
		t.Fatal("incompatible budget leaked reservations")
	}
}

func TestSortedStringGrowthAccountsForOldAndNewBuffers(t *testing.T) {
	eng := New(Options{MemoryLimitBytes: 160})
	defer eng.Close()
	budget, err := eng.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream := &sortedAggregateStream{query: &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeStringView, GroupBy: []CompiledGroupColumn{{Name: "key", Type: memory.TypeString}}}}, outputBudget: budget.Operator("test-output", memory.MemoryWorkspace)}
	batch := memory.NewBatch()
	batch.Strings["key"] = &memory.StringViewVector{}
	if err := stream.ensureOutputString(context.Background(), batch, aggregateRecord{validMask: 1, stringKey: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	batch.Strings["key"].Data = batch.Strings["key"].Data[:64]
	if budget.Used() != 64 {
		t.Fatalf("initial used=%d", budget.Used())
	}
	// The final128-byte vector fits160bytes, but its growth must also hold the
	// previous64-byte vector until copy/rebind has completed.
	if err := stream.ensureOutputString(context.Background(), batch, aggregateRecord{validMask: 1, stringKey: []byte("b")}); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("growth error=%v", err)
	}
	if cap(batch.Strings["key"].Data) != 64 || budget.Used() != 64 {
		t.Fatalf("failed growth mutated ownership: cap=%d used=%d", cap(batch.Strings["key"].Data), budget.Used())
	}
	batch.ReleaseOwned()
	stream.releaseOutput()
	if budget.Used() != 0 {
		t.Fatalf("leaked=%d", budget.Used())
	}
}

func TestExternalFiniteBudgetConstrainsPreparedResidentPlan(t *testing.T) {
	src := &generatedStreamSource{rows: 8192, groups: 8192}
	options := streamTestOptions()
	options.MemoryLimitBytes = 0
	eng := New(options)
	defer eng.Close()
	prepared, err := eng.Prepare(context.Background(), src, streamTestQuery(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.Plan().Strategy.Kind != planner.StrategyBitmapDirect {
		t.Fatal("expected resident plan")
	}
	externalOptions := streamTestOptions()
	externalOptions.MemoryLimitBytes = 8 << 10
	other := New(externalOptions)
	defer other.Close()
	budget, err := other.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	_, err = prepared.Execute(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v", err)
	}
	if snap := budget.Snapshot(); snap.PeakBytes > externalOptions.MemoryLimitBytes || snap.UsedBytes != 0 {
		t.Fatalf("external budget=%+v", snap)
	}
	if src.maxBatch.Load() > 64 || src.closed.Load() != 1 {
		t.Fatalf("batch=%d closed=%d", src.maxBatch.Load(), src.closed.Load())
	}
}

func TestBoundedPlanRejectsExternalUnlimitedBudget(t *testing.T) {
	src := &generatedStreamSource{rows: 64, groups: 8, sorted: true}
	options := streamTestOptions()
	options.ExecutionPolicy = ExecutionBounded
	eng := New(options)
	defer eng.Close()
	query := streamTestQuery()
	query.Hints.ForceStrategy = ""
	prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	other := New(Options{})
	defer other.Close()
	budget, err := other.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	if _, err := prepared.Execute(ctx, ExecuteOptions{}); !errors.Is(err, ErrBoundedMemoryLimitRequired) {
		t.Fatalf("execute error=%v", err)
	}
	if _, err := prepared.NewSortedAggregateStream(ctx, prepared.resultSchema(), 8); !errors.Is(err, ErrBoundedMemoryLimitRequired) {
		t.Fatalf("stream error=%v", err)
	}
	if _, err := prepared.NewResidentAggregateStream(ctx, prepared.resultSchema(), 8); !errors.Is(err, ErrBoundedMemoryLimitRequired) {
		t.Fatalf("resident stream error=%v", err)
	}
	if src.opened.Load() != 0 || budget.Used() != 0 {
		t.Fatal("invalid budget opened source or retained memory")
	}
}

func TestResidentStreamTerminalErrorsRemainObservable(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint("cancelled=", cancelled), func(t *testing.T) {
			src := &generatedStreamSource{rows: 8192, groups: 8192}
			options := streamTestOptions()
			options.MemoryLimitBytes = 8 << 10
			eng := New(options)
			defer eng.Close()
			prepared, err := eng.Prepare(context.Background(), src, streamTestQuery(), PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			budget, err := eng.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
			defer cancel()
			stream, err := prepared.NewResidentAggregateStream(ctx, prepared.resultSchema(), 8)
			if err != nil {
				t.Fatal(err)
			}
			if cancelled {
				cancel()
			}
			batch := memory.NewBatch()
			defer batch.ReleaseOwned()
			_, first := stream.Next(ctx, batch)
			want := memory.ErrBudgetExceeded
			if cancelled {
				want = context.Canceled
			}
			if !errors.Is(first, want) {
				t.Fatalf("first error=%v", first)
			}
			if _, second := stream.Next(ctx, batch); second != first {
				t.Fatalf("terminal error changed: first=%v second=%v", first, second)
			}
			if budget.Used() != 0 || src.closed.Load() != 1 {
				t.Fatalf("terminal cleanup used=%d closed=%d", budget.Used(), src.closed.Load())
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Next(ctx, batch); !errors.Is(err, source.ErrBatchStreamClosed) {
				t.Fatalf("explicit close error=%v", err)
			}
		})
	}
}

func TestResultCollectorChargesReservationMetadata(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	collector := resultCollector{budget: manager}
	for i := 1; i <= 131; i++ {
		if err := collector.reserve(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		actual := int64(i) + int64(cap(collector.leases))*int64(unsafe.Sizeof(memory.Reservation(nil)))
		if manager.Used() != actual {
			t.Fatalf("reservations=%d charged=%d actual=%d", i, manager.Used(), actual)
		}
	}
	collector.close()
	collector.close()
	if manager.Used() != 0 {
		t.Fatalf("closed used=%d", manager.Used())
	}
}

func TestResultCollectorMetadataGrowthRejectsBeforeAllocation(t *testing.T) {
	width := int64(unsafe.Sizeof(memory.Reservation(nil)))
	manager := memory.NewBudgetManager(8 + 8*width + 16*width - 1)
	collector := resultCollector{budget: manager}
	for i := 0; i < 8; i++ {
		if err := collector.reserve(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	before := manager.Used()
	backing := &collector.leases[0]
	if err := collector.reserve(context.Background(), 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("overlap error=%v", err)
	}
	if manager.Used() != before || len(collector.leases) != 8 || cap(collector.leases) != 8 || &collector.leases[0] != backing {
		t.Fatal("failed metadata growth allocated or changed ownership")
	}
	collector.close()
	if manager.Used() != 0 {
		t.Fatalf("closed used=%d", manager.Used())
	}
	manager = memory.NewBudgetManager(8*width - 1)
	collector = resultCollector{budget: manager}
	if err := collector.reserve(context.Background(), 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("initial metadata error=%v", err)
	}
	if manager.Used() != 0 || collector.leases != nil {
		t.Fatal("initial metadata denial allocated payload or backing")
	}
}
