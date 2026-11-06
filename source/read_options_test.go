package source_test

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

func TestReadOptionsBatchDefaultsAndValidation(t *testing.T) {
	kernels := kernel.New(kernel.ModeScalar)
	original := source.ReadOptions{Kernels: kernels}
	if err := original.Validate(); err != nil {
		t.Fatalf("legacy Kernels-only options: %v", err)
	}
	normalized := original.Normalize()
	if normalized.BatchRows != source.DefaultBatchRows || normalized.BatchBytes != source.DefaultBatchBytes || normalized.Kernels != kernels || normalized.MemoryManager != nil {
		t.Fatalf("normalized=%+v", normalized)
	}
	manager := memory.NewBudgetManager(1024)
	explicit := source.ReadOptions{Kernels: kernels, BatchRows: 3, BatchBytes: 128, MemoryManager: manager}
	if normalized := explicit.Normalize(); normalized != explicit {
		t.Fatalf("normalization changed explicit options: %+v", normalized)
	}
	for _, invalid := range []source.ReadOptions{
		{}, {Kernels: kernels, BatchRows: -1}, {Kernels: kernels, BatchBytes: -1},
	} {
		if err := invalid.Normalize().Validate(); err == nil {
			t.Fatalf("accepted invalid options=%+v", invalid)
		}
	}
}

func TestOpenBatchStreamBindsQueryBudgetAndPreservesExplicitManager(t *testing.T) {
	engine := memory.NewEngineBudget(1024)
	budget, err := engine.BeginQuery(memory.BudgetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 3, BatchBytes: 128}
	src := &readOptionsSource{}
	stream, err := source.OpenBatchStream(ctx, src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if src.seen.MemoryManager != budget.Operator("SourceScan", memory.MemoryWorkspace) || src.seen.BatchRows != opts.BatchRows || src.seen.BatchBytes != opts.BatchBytes {
		t.Fatalf("query options=%+v", src.seen)
	}
	lease, err := src.seen.MemoryManager.Reserve(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 64 || engine.Used() != 64 {
		t.Fatalf("query=%d engine=%d", budget.Used(), engine.Used())
	}
	lease.Release()
	manager := memory.NewBudgetManager(256)
	opts.MemoryManager = manager
	stream, err = source.OpenBatchStream(ctx, src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if src.seen != opts {
		t.Fatalf("explicit options changed: %+v", src.seen)
	}
	opts.MemoryManager = nil
	stream, err = source.OpenBatchStream(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if src.seen.MemoryManager != nil {
		t.Fatal("queryless direct scan acquired a hidden manager")
	}
}

type readOptionsSource struct{ seen source.ReadOptions }

func (*readOptionsSource) Schema() memory.Schema                                  { return memory.Schema{} }
func (*readOptionsSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (*readOptionsSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *readOptionsSource) NewBatchStream(_ context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.seen = opts
	return emptyReadOptionsStream{}, nil
}

type emptyReadOptionsStream struct{}

func (emptyReadOptionsStream) Next(context.Context, *memory.Batch) (bool, error) { return false, nil }
func (emptyReadOptionsStream) Close() error                                      { return nil }
