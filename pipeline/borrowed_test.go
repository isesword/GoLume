package pipeline_test

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// leaseProbeStream deliberately overwrites its buffers on Next and Close.
// A copying exchange cannot satisfy the pointer identity assertions below.
type leaseProbeSource struct {
	source.Source
	opened     chan *leaseProbeStream
	failSecond <-chan struct{}
	nullable   bool
}
type leaseProbeStream struct {
	nullable bool
	id       int
	calls    atomic.Int32
	closed   atomic.Bool
	values   []int64
	fail     <-chan struct{}
	once     sync.Once
}

func (s *leaseProbeSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	parts, err := s.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	return s.NewBatchStreamWithPartitions(ctx, opts, parts)
}
func (s *leaseProbeSource) NewBatchStreamWithPartitions(_ context.Context, _ source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if len(parts) != 1 {
		return nil, errors.New("probe requires exactly one partition per worker")
	}
	child := &leaseProbeStream{id: parts[0].ID, values: make([]int64, 16), nullable: s.nullable}
	if child.id == 1 {
		child.fail = s.failSecond
	}
	s.opened <- child
	return child, nil
}
func (s *leaseProbeStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	call := s.calls.Add(1)
	if s.fail != nil {
		select {
		case <-s.fail:
			return false, errInjectedRead
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if call > 2 {
		return false, nil
	}
	for i := range s.values {
		s.values[i] = int64(s.id*1000) + int64(call)*100 + int64(i)
	}
	dst.Int64s["x"] = &memory.Int64Vector{Values: s.values}
	if s.nullable {
		dst.Int64s["x"].Validity = memory.Bitmap{^uint64(0)}
	}
	dst.Rows = len(s.values)
	return true, nil
}
func (s *leaseProbeStream) Close() error {
	s.once.Do(func() {
		for i := range s.values {
			s.values[i] = -1
		}
		s.closed.Store(true)
	})
	return nil
}
func openLeaseProbe(t *testing.T, ctx context.Context, gate <-chan struct{}) (source.BatchStream, map[int]*leaseProbeStream) {
	t.Helper()
	src := &leaseProbeSource{Source: newTrackedIntSource(t, 2, 16), opened: make(chan *leaseProbeStream, 2), failSecond: gate}
	parts, _ := src.Partitions(ctx)
	opts := pipeline.Options{Workers: 2, TargetMorselBytes: 64, MaxMorselBytes: 128, MaxInFlightBytes: 512, MinMorselRows: 1, MaxMorselRows: 16, Ordering: pipeline.OrderingStable}
	stream, err := pipeline.OpenBatchStream(ctx, src, scalarReadOptions(), parts, memory.NewBudgetManager(1<<20), opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	opened := make(map[int]*leaseProbeStream)
	for len(opened) < 2 {
		select {
		case child := <-src.opened:
			opened[child.id] = child
		case <-time.After(5 * time.Second):
			t.Fatal("workers failed to open")
		}
	}
	return stream, opened
}
func TestParallelBorrowedLeaseWaitsForEveryRange(t *testing.T) {
	stream, workers := openLeaseProbe(t, context.Background(), nil)
	first := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
		t.Fatalf("first=(%v,%v)", ok, err)
	}
	if first.Rows != 8 || &first.Int64s["x"].Values[0] != &workers[0].values[0] {
		t.Fatal("parallel batch copied source values")
	}
	first.ClearColumns()
	second := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), second); !ok || err != nil {
		t.Fatalf("second=(%v,%v)", ok, err)
	}
	if second.Rows != 8 || &second.Int64s["x"].Values[0] != &workers[0].values[8] || workers[0].calls.Load() != 1 {
		t.Fatal("source advanced before final range was consumed")
	}
	time.Sleep(10 * time.Millisecond)
	if workers[0].calls.Load() != 1 || second.Int64s["x"].Values[0] != 108 {
		t.Fatal("held range changed")
	}
	if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
		t.Fatalf("next batch=(%v,%v)", ok, err)
	}
	if second.Rows != 0 || first.Int64s["x"].Values[0] != 200 || workers[0].calls.Load() != 2 {
		t.Fatal("last range did not release source lease")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if first.Rows != 0 {
		t.Fatal("close retained destination bindings")
	}
	for _, worker := range workers {
		if !worker.closed.Load() {
			t.Fatal("close left producer alive")
		}
	}
	if stats := stream.(pipeline.StreamStatsProvider).PipelineStats(); stats.Exchange.CurrentInFlightBytes != 0 {
		t.Fatalf("leaked lease bytes=%d", stats.Exchange.CurrentInFlightBytes)
	}
}
func TestParallelBorrowedFailureCannotRevokePublishedBatch(t *testing.T) {
	gate := make(chan struct{})
	stream, workers := openLeaseProbe(t, context.Background(), gate)
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=(%v,%v)", ok, err)
	}
	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for !workers[1].closed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("failing source did not close")
		}
		time.Sleep(time.Millisecond)
	}
	if workers[0].closed.Load() || workers[0].calls.Load() != 1 || out.Int64s["x"].Values[0] != 100 {
		t.Fatal("failure revoked a published borrowed range")
	}
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, errInjectedRead) {
		t.Fatalf("failure=(%v,%v)", ok, err)
	}
	for _, worker := range workers {
		if !worker.closed.Load() {
			t.Fatal("failure cleanup left producer alive")
		}
	}
	if out.Rows != 0 {
		t.Fatal("failure retained destination")
	}
}
func TestParallelBorrowedParentCancelDrainsPendingLeases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, workers := openLeaseProbe(t, ctx, nil)
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=(%v,%v)", ok, err)
	}
	cancel()
	if workers[0].closed.Load() || out.Int64s["x"].Values[0] != 100 {
		t.Fatal("cancel revoked borrowed data before consumer released it")
	}
	done := make(chan error, 1)
	go func() { _, err := stream.Next(context.Background(), out); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel deadlocked on published leases")
	}
	for _, worker := range workers {
		if !worker.closed.Load() {
			t.Fatal("cancel left worker alive")
		}
	}
}

func TestParallelBorrowedFullBatchQuotaRejectsUnboundedSource(t *testing.T) {
	src := &leaseProbeSource{Source: newTrackedIntSource(t, 2, 16), opened: make(chan *leaseProbeStream, 2)}
	parts, _ := src.Partitions(context.Background())
	manager := memory.NewBudgetManager(1024)
	opts := pipeline.Options{Workers: 2, TargetMorselBytes: 32, MaxMorselBytes: 64, MaxInFlightBytes: 128, MinMorselRows: 1, MaxMorselRows: 16}
	stream, err := pipeline.OpenBatchStream(context.Background(), src, scalarReadOptions(), parts, manager, opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	// The probe deliberately ignores tightened ReadOptions and returns 128 bytes.
	// Small output ranges cannot disguise the full retained 128-byte batch.
	if ok, err := stream.Next(context.Background(), memory.NewBatch()); ok || err == nil || !strings.Contains(err.Error(), "per-worker capacity 64") {
		t.Fatalf("quota result=(%v,%v)", ok, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("leaked metadata=%d", manager.Used())
	}
	// A fast failure may cancel the peer before its source is opened.
	opened := 0
	for {
		select {
		case worker := <-src.opened:
			opened++
			if !worker.closed.Load() {
				t.Fatal("quota failure left source open")
			}
		default:
			if opened == 0 {
				t.Fatal("quota probe never opened")
			}
			return
		}
	}
}

func TestParallelBorrowedChargesOnlyAllocatedMetadata(t *testing.T) {
	src := &leaseProbeSource{Source: newTrackedIntSource(t, 2, 16), opened: make(chan *leaseProbeStream, 2), nullable: true}
	parts, _ := src.Partitions(context.Background())
	// Each whole source batch has 128 payload bytes plus an 8-byte bitmap.
	// Three ranges require at most one additional 8-byte bitmap at a time.
	manager := memory.NewBudgetManager(16)
	opts := pipeline.Options{Workers: 2, TargetMorselBytes: 64, MaxMorselBytes: 128, MaxInFlightBytes: 512, MinMorselRows: 1, MaxMorselRows: 16}
	stream, err := pipeline.OpenBatchStream(context.Background(), src, scalarReadOptions(), parts, manager, opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=(%v,%v)", ok, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for manager.Used() != 16 {
		if time.Now().After(deadline) {
			t.Fatalf("metadata charged=%d want=16", manager.Used())
		}
		time.Sleep(time.Millisecond)
	}
	stats := stream.(pipeline.StreamStatsProvider).PipelineStats()
	if stats.Exchange.PeakInFlightBytes != 288 {
		t.Fatalf("full retained bytes=%d want=288", stats.Exchange.PeakInFlightBytes)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("close retained metadata=%d", manager.Used())
	}
}

func TestParallelBorrowedFailureClearsAlternateDestination(t *testing.T) {
	stream, _ := openLeaseProbe(t, context.Background(), nil)
	first := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), first); !ok || err != nil {
		t.Fatalf("first=(%v,%v)", ok, err)
	}
	alternate := memory.NewBatch()
	alternate.Rows = 1
	alternate.Int64s["stale"] = &memory.Int64Vector{Values: []int64{99}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, alternate); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled=(%v,%v)", ok, err)
	}
	if first.Rows != 0 || alternate.Rows != 0 || len(alternate.Int64s) != 0 {
		t.Fatal("error retained old or newly supplied destination bindings")
	}
}

func TestParallelBorrowedMetadataUsesActiveExternalQueryBudget(t *testing.T) {
	preparedEngine := memory.NewEngineBudget(1 << 20)
	executingEngine := memory.NewEngineBudget(1 << 20)
	query, err := executingEngine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	ctx := memory.WithQueryBudget(context.Background(), query)
	src := &leaseProbeSource{Source: newTrackedIntSource(t, 2, 16), opened: make(chan *leaseProbeStream, 2), nullable: true}
	parts, _ := src.Partitions(ctx)
	opts := pipeline.Options{Workers: 2, TargetMorselBytes: 64, MaxMorselBytes: 128, MaxInFlightBytes: 512, MinMorselRows: 1, MaxMorselRows: 16}
	readOpts := scalarReadOptions()
	readOpts.MemoryManager = preparedEngine
	stream, err := pipeline.OpenBatchStream(ctx, src, readOpts, parts, preparedEngine, opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if ok, err := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("external exchange budget=(%v,%v)", ok, err)
	}
	_ = stream.Close()
	if preparedEngine.Used() != 0 || executingEngine.Used() != 0 || query.Used() != 0 {
		t.Fatalf("budget bypass/leak prepared=%d executing=%d query=%d", preparedEngine.Used(), executingEngine.Used(), query.Used())
	}
}
