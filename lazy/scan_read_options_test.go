package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"testing"
)

func TestQueryScanOptionsReachRowAndAggregateInputs(t *testing.T) {
	for _, mode := range []string{"collect", "batches", "resident-aggregate", "sorted-aggregate", "spill-aggregate"} {
		t.Run(mode, func(t *testing.T) {
			src := &queryScanOptionsSource{phase30NativeStreamSource: phase30NativeStreamSource{
				schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}),
				values: [][]int64{{1, 1}, {2, 3}},
			}, ordered: mode == "sorted-aggregate"}
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 16 << 20
			opts.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 128, MaxMorselBytes: 256, MaxInFlightBytes: 1024, MinMorselRows: 1, MaxMorselRows: 4}
			if mode == "spill-aggregate" {
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			}
			session := NewSession(opts)
			defer session.Close()
			query := session.FromSource(src)
			if mode == "resident-aggregate" || mode == "sorted-aggregate" || mode == "spill-aggregate" {
				query = query.GroupBy("x").Agg(Count("n"))
			}
			wantBytes := int64(256)
			if mode == "collect" {
				frame, err := query.Collect(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := frame.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				wantBytes = 64
				reader, err := query.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 32, MaxBatchBytes: wantBytes})
				if err != nil {
					t.Fatal(err)
				}
				for reader.Next(context.Background()) {
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if src.seen.BatchRows != 4 || src.seen.BatchBytes != wantBytes || src.seen.Kernels != session.Engine().KernelSet() {
				t.Fatalf("scan options=%+v want rows=4 bytes=%d", src.seen, wantBytes)
			}
			if src.query == nil || src.seen.MemoryManager != src.query.Operator("SourceScan", memory.MemoryWorkspace) {
				t.Fatalf("scan was not bound to owning query: %+v", src.seen)
			}
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("query leaked %d bytes", used)
			}
		})
	}
}

type queryScanOptionsSource struct {
	phase30NativeStreamSource
	ordered bool
	seen    source.ReadOptions
	query   *memory.QueryBudget
}

func (s *queryScanOptionsSource) Ordering() []source.OrderingColumn {
	if s.ordered {
		return []source.OrderingColumn{{Name: "x"}}
	}
	return nil
}
func (s *queryScanOptionsSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.seen = opts
	s.query = memory.QueryBudgetFromContext(ctx)
	if s.query == nil || opts.MemoryManager != s.query.Operator("SourceScan", memory.MemoryWorkspace) {
		return nil, fmt.Errorf("scan did not receive owning query memory manager")
	}
	lease, err := opts.MemoryManager.Reserve(context.Background(), 64)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if s.query.Snapshot().Operators["SourceScan"] != 64 {
		return nil, fmt.Errorf("scan allocation did not reach query accounting")
	}
	return s.phase30NativeStreamSource.NewBatchStream(ctx, opts)
}
