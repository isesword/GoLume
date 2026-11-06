package pipeline_test

import (
	"context"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source"
	"sync"
	"testing"
)

func TestPipelinePropagatesBoundedScanOptions(t *testing.T) {
	for _, workers := range []int{1, 2} {
		t.Run(map[int]string{1: "direct", 2: "parallel"}[workers], func(t *testing.T) {
			base := newTrackedIntSource(t, 4, 2)
			recorder := &scanOptionsRecorder{Source: base}
			parts, _ := recorder.Partitions(context.Background())
			engine := memory.NewEngineBudget(1 << 20)
			budget, err := engine.BeginQuery(memory.BudgetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			opts := pipeline.Options{Workers: workers, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 128, MinMorselRows: 1, MaxMorselRows: 2}
			readOpts := scalarReadOptions()
			stream, err := pipeline.OpenBatchStream(ctx, recorder, readOpts, parts, budget, opts, observer.Nop{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.DrainBatchStream(ctx, stream); err != nil {
				t.Fatal(err)
			}
			seen := recorder.options()
			if len(seen) != workers {
				t.Fatalf("opens=%d want=%d", len(seen), workers)
			}
			for _, got := range seen {
				if got.Kernels != readOpts.Kernels || got.BatchRows != 2 || got.BatchBytes != 32 || got.MemoryManager != budget.Operator("SourceScan", memory.MemoryWorkspace) {
					t.Fatalf("scan options=%+v", got)
				}
			}
		})
	}
}

func TestPipelinePreservesStricterScanOptionsAndExplicitManager(t *testing.T) {
	base := newTrackedIntSource(t, 1, 2)
	recorder := &scanOptionsRecorder{Source: base}
	parts, _ := recorder.Partitions(context.Background())
	manager := memory.NewBudgetManager(1 << 20)
	readOpts := scalarReadOptions()
	readOpts.BatchRows, readOpts.BatchBytes, readOpts.MemoryManager = 1, 16, manager
	opts := (pipeline.Options{Workers: 1}).Normalize(1)
	stream, err := pipeline.OpenBatchStream(context.Background(), recorder, readOpts, parts, manager, opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.DrainBatchStream(context.Background(), stream); err != nil {
		t.Fatal(err)
	}
	seen := recorder.options()
	if len(seen) != 1 || seen[0] != readOpts.Normalize() {
		t.Fatalf("scan options=%+v want=%+v", seen, readOpts)
	}
}

func TestFanOutPropagatesBoundedScanOptions(t *testing.T) {
	for _, replay := range []source.Replayability{source.ReplayUnknown, source.ReplayStable} {
		t.Run(replay.String(), func(t *testing.T) {
			recorder := &scanOptionsRecorder{Source: newTrackedIntSource(t, 2, 2), replay: replay}
			parts, _ := recorder.Partitions(context.Background())
			manager := memory.NewBudgetManager(1 << 20)
			opts := pipeline.Options{Workers: 1, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 128, MinMorselRows: 1, MaxMorselRows: 2}
			streams, err := pipeline.OpenFanOut(context.Background(), recorder, scalarReadOptions(), parts, manager, opts, 2, observer.Nop{})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, len(streams))
			for _, stream := range streams {
				wg.Add(1)
				go func(stream source.BatchStream) {
					defer wg.Done()
					_, err := source.DrainBatchStream(context.Background(), stream)
					errs <- err
				}(stream)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			seen := recorder.options()
			want := 1
			if replay == source.ReplayStable {
				want = 2
			}
			if len(seen) != want {
				t.Fatalf("opens=%d want=%d", len(seen), want)
			}
			for _, got := range seen {
				if got.BatchRows != 2 || got.BatchBytes != 32 || got.MemoryManager == nil {
					t.Fatalf("scan options=%+v", got)
				}
				lease, err := got.MemoryManager.Reserve(context.Background(), 16)
				if err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 16 {
					t.Fatalf("fallback manager used=%d", manager.Used())
				}
				lease.Release()
			}
		})
	}
}

type scanOptionsRecorder struct {
	source.Source
	mu     sync.Mutex
	seen   []source.ReadOptions
	replay source.Replayability
}

func (s *scanOptionsRecorder) Replayability() source.Replayability { return s.replay }
func (s *scanOptionsRecorder) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	parts, err := s.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	return s.NewBatchStreamWithPartitions(ctx, opts, parts)
}
func (s *scanOptionsRecorder) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	s.mu.Lock()
	s.seen = append(s.seen, opts)
	s.mu.Unlock()
	return source.OpenBatchStreamWithPartitions(ctx, s.Source, opts, parts)
}
func (s *scanOptionsRecorder) options() []source.ReadOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]source.ReadOptions(nil), s.seen...)
}
