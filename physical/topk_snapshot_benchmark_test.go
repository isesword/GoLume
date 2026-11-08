package physical

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// BenchmarkTopKSnapshot uses identical fixtures for the unknown-replayability
// fallback and stable fast path. Fixture construction and Parquet encoding are
// outside the timer. No budget limit is imposed so this benchmark can also run
// unchanged against the former whole-input materialization implementation.
// Run with -benchmem; allocations include input scanning and output delivery.
func BenchmarkTopKSnapshot(b *testing.B) {
	for _, rows := range []int{4096, 262144, 1048576} {
		b.Run(fmt.Sprintf("Rows%d", rows), func(b *testing.B) {
			schema, batch := topKSnapshotFixture(rows)
			defer batch.ReleaseOwned()
			stable, err := inmemory.New(schema, batch)
			if err != nil {
				b.Fatal(err)
			}
			parquet := writeTopKSnapshotParquet(b, schema, batch)
			for _, tc := range []struct {
				name  string
				input source.Source
			}{
				{"StableInMemory", stable},
				{"UnknownInMemory", unstableReplaySource{inner: stable}},
				{"UnknownParquet", parquet},
			} {
				b.Run(tc.name, func(b *testing.B) {
					const k = 100
					ctx := context.Background()
					opts := testReadOptions()
					opts.BatchRows = 1024
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						manager := memory.NewBudgetManager(0)
						top, err := NewTopKSourceWithManager(tc.input, k, []SortKey{{Column: "score", Descending: true, NullsLast: true}}, manager)
						if err != nil {
							b.Fatal(err)
						}
						stream, err := source.OpenBatchStream(ctx, top, opts)
						if err != nil {
							b.Fatal(err)
						}
						dst := memory.NewBatch()
						count := 0
						for {
							ok, err := stream.Next(ctx, dst)
							if err != nil {
								_ = stream.Close()
								b.Fatal(err)
							}
							if !ok {
								break
							}
							count += dst.Rows
							dst.ReleaseOwned()
						}
						if err := stream.Close(); err != nil {
							b.Fatal(err)
						}
						if count != k {
							b.Fatalf("rows=%d, want %d", count, k)
						}
						if manager.Used() != 0 {
							b.Fatalf("reservation leaked: %d", manager.Used())
						}
					}
				})
			}
		})
	}
}

// BenchmarkTopKSnapshotReplacementHeavy exercises the worst admission pattern:
// every row after the first K replaces a winner. Unlike random/tied keys this
// requires a fresh owned snapshot for every input row, exposing allocation cost.
func BenchmarkTopKSnapshotReplacementHeavy(b *testing.B) {
	for _, rows := range []int{262144, 1048576} {
		b.Run(fmt.Sprintf("Rows%d", rows), func(b *testing.B) {
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeInt64})
			batch := memory.NewBatch()
			batch.Rows = rows
			ids, keys, payload := make([]int64, rows), make([]int64, rows), make([]int64, rows)
			for i := range ids {
				ids[i] = int64(i)
				keys[i] = int64(rows - i)
				payload[i] = int64(i * 3)
			}
			batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
			batch.Int64s["key"] = &memory.Int64Vector{Values: keys}
			batch.Int64s["payload"] = &memory.Int64Vector{Values: payload}
			stable, err := inmemory.New(schema, batch)
			if err != nil {
				b.Fatal(err)
			}
			input := unstableReplaySource{inner: stable}
			ctx := context.Background()
			opts := testReadOptions()
			opts.BatchRows = 1024
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				manager := memory.NewBudgetManager(0)
				top, err := NewTopKSourceWithManager(input, 100, []SortKey{{Column: "key"}}, manager)
				if err != nil {
					b.Fatal(err)
				}
				stream, err := source.OpenBatchStream(ctx, top, opts)
				if err != nil {
					b.Fatal(err)
				}
				dst := memory.NewBatch()
				count := 0
				for {
					ok, err := stream.Next(ctx, dst)
					if err != nil {
						_ = stream.Close()
						b.Fatal(err)
					}
					if !ok {
						break
					}
					for j := 0; j < dst.Rows; j++ {
						if got, want := dst.Int64s["id"].Values[j], int64(rows-1-count); got != want {
							b.Fatalf("id=%d want=%d", got, want)
						}
						count++
					}
					dst.ReleaseOwned()
				}
				if err := stream.Close(); err != nil {
					b.Fatal(err)
				}
				if count != 100 || manager.Used() != 0 {
					b.Fatalf("count=%d retained=%d", count, manager.Used())
				}
			}
		})
	}
}
