package physical

import (
	"context"
	"testing"

	"gocolumn/memory"
	"gocolumn/scheduler"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func benchmarkOrderSource(b *testing.B, topK bool, sortOpts SortSourceOptions) {
	const rows = 1 << 18
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	ids := make([]int64, rows)
	scores := make([]int64, rows)
	payload := make([]int64, rows)
	x := uint64(0x9e3779b97f4a7c15)
	for i := 0; i < rows; i++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		ids[i] = int64(i)
		scores[i] = int64(x * 2685821657736338717)
		payload[i] = int64(i * 3)
	}
	batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
	batch.Int64s["score"] = &memory.Int64Vector{Values: scores}
	batch.Int64s["payload"] = &memory.Int64Vector{Values: payload}
	batch.Reset(rows)

	base, err := inmemory.New(schema, batch)
	if err != nil {
		b.Fatal(err)
	}
	keys := []SortKey{{Column: "score", Descending: true, NullsLast: true}}
	ctx := context.Background()

	newSource := func() (source.Source, error) {
		if topK {
			return NewTopKSourceWithManager(base, 100, keys, nil)
		}
		return NewSortSourceWithOptions(base, keys, memory.NewBudgetManager(0), sortOpts)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := newSource()
		if err != nil {
			b.Fatal(err)
		}
		parts, err := s.Partitions(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(parts) != 1 {
			b.Fatalf("ordered source partitions=%d, want 1", len(parts))
		}
		r, err := source.OpenBatchStream(ctx, s, testReadOptions())
		if err != nil {
			b.Fatal(err)
		}
		dst := memory.NewBatch()
		rowsRead := 0
		for {
			ok, err := r.Next(ctx, dst)
			if err != nil {
				_ = r.Close()
				b.Fatal(err)
			}
			if !ok {
				break
			}
			rowsRead += dst.Rows
			dst.ReleaseOwned()
		}
		want := rows
		if topK {
			want = 100
		}
		if rowsRead != want {
			_ = r.Close()
			b.Fatalf("ordered rows=%d, want %d", rowsRead, want)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalSort262K(b *testing.B) { benchmarkOrderSource(b, false, SortSourceOptions{}) }

func BenchmarkGlobalSort262KParallel4(b *testing.B) {
	pool := scheduler.NewPool(4)
	b.Cleanup(func() { _ = pool.Close() })
	benchmarkOrderSource(b, false, SortSourceOptions{Workers: 4, Executor: pool})
}

func BenchmarkTopK100Of262K(b *testing.B) { benchmarkOrderSource(b, true, SortSourceOptions{}) }
