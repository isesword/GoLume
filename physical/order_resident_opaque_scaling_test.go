package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

// Real Arrow arrays are created before timing. The timed path retains, slices,
// orders, emits and closes them through the production resident implementation.
func opaqueScalingArrays(b *testing.B, n int) []arrow.Array {
	b.Helper()
	arrays := make([]arrow.Array, n)
	builder := array.NewLargeStringBuilder(arrowmem.DefaultAllocator)
	for i := range arrays {
		builder.Append("payload")
		arrays[i] = builder.NewArray()
	}
	builder.Release()
	b.Cleanup(func() {
		for _, a := range arrays {
			a.Release()
		}
	})
	return arrays
}

func BenchmarkResidentOpaqueAppendScaling(b *testing.B) {
	for _, n := range []int{1024, 4096, 16384, 65536} {
		for _, unique := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/unique=%v", n, unique), func(b *testing.B) {
				count := 1
				if unique {
					count = n
				}
				arrays := opaqueScalingArrays(b, count)
				ctx := context.Background()
				schema := memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow})
				batch := memory.NewBatch()
				batch.Rows = 1
				vector := &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: arrays[0]}
				batch.Opaque["raw"] = vector
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					manager := memory.NewBudgetManager(0)
					w := &sortWorkspace{parent: manager, bulkCredits: true}
					owner, err := newResidentOpaqueRows(ctx, schema, batch, w)
					if err != nil {
						b.Fatal(err)
					}
					for row := 0; row < n; row++ {
						vector.Value = arrays[row%count]
						if err = owner.appendRow(ctx, batch, 0); err != nil {
							b.Fatal(err)
						}
					}
					owner.close()
					w.close()
					if manager.Used() != 0 {
						b.Fatal("budget leak")
					}
				}
			})
		}
	}
}

type opaqueScalingSource struct {
	arrays []arrow.Array
	rows   int
	schema memory.Schema
}

func (s *opaqueScalingSource) Schema() memory.Schema { return s.schema }
func (s *opaqueScalingSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.rows)}, nil
}
func (s *opaqueScalingSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.rows)}}, nil
}
func (s *opaqueScalingSource) Replayability() source.Replayability { return source.ReplayStable }
func (s *opaqueScalingSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &opaqueScalingStream{source: s}, nil
}

type opaqueScalingStream struct {
	source *opaqueScalingSource
	next   int
	key    [1]int64
	vector memory.OpaqueVector
}

func (s *opaqueScalingStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.next == s.source.rows {
		return false, nil
	}
	s.key[0] = int64(s.source.rows - s.next)
	dst.Int64s["key"] = &memory.Int64Vector{Values: s.key[:]}
	s.vector = memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: s.source.arrays[s.next%len(s.source.arrays)]}
	dst.Opaque["raw"] = &s.vector
	s.next++
	dst.Rows = 1
	return true, nil
}
func (s *opaqueScalingStream) Close() error { return nil }

func BenchmarkResidentOpaqueSortScaling(b *testing.B) {
	for _, n := range []int{1024, 4096, 16384, 65536} {
		for _, unique := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/unique=%v", n, unique), func(b *testing.B) {
				count := 1
				if unique {
					count = n
				}
				arrays := opaqueScalingArrays(b, count)
				input := &opaqueScalingSource{arrays: arrays, rows: n, schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow})}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					manager := memory.NewBudgetManager(0)
					ordered, err := NewSortSource(input, []SortKey{{Column: "key"}}, manager)
					if err != nil {
						b.Fatal(err)
					}
					stream, err := source.OpenBatchStream(ctx, ordered, testReadOptions())
					if err != nil {
						b.Fatal(err)
					}
					out := memory.NewBatch()
					rows := 0
					for {
						ok, err := stream.Next(ctx, out)
						if err != nil {
							b.Fatal(err)
						}
						if !ok {
							break
						}
						rows += out.Rows
					}
					if err = stream.Close(); err != nil {
						b.Fatal(err)
					}
					if rows != n || manager.Used() != 0 {
						b.Fatal("row count or cleanup", rows, manager.Used())
					}
				}
			})
		}
	}
}

// Fixed total row count separates source-count effects from output row costs.
type opaqueBatchScalingSource struct {
	*opaqueScalingSource
	batchRows int
}

func (s *opaqueBatchScalingSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &opaqueBatchScalingStream{source: s, keys: make([]int64, s.batchRows)}, nil
}

type opaqueBatchScalingStream struct {
	source *opaqueBatchScalingSource
	next   int
	keys   []int64
	vector memory.OpaqueVector
}

func (s *opaqueBatchScalingStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.next == s.source.rows {
		return false, nil
	}
	n := min(s.source.batchRows, s.source.rows-s.next)
	for i := 0; i < n; i++ {
		s.keys[i] = int64(s.source.rows - s.next - i)
	}
	dst.Int64s["key"] = &memory.Int64Vector{Values: s.keys[:n]}
	s.vector = memory.OpaqueVector{Rows: n, Type: memory.TypeArrow, Value: s.source.arrays[(s.next/s.source.batchRows)%len(s.source.arrays)]}
	dst.Opaque["raw"] = &s.vector
	s.next += n
	dst.Rows = n
	return true, nil
}
func (s *opaqueBatchScalingStream) Close() error { return nil }

func BenchmarkResidentOpaqueBatchSizeScaling(b *testing.B) {
	const total = 65536
	for _, batchRows := range []int{1, 16, 256} {
		for _, topK := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch=%d/topk=%v", batchRows, topK), func(b *testing.B) {
				arrays := make([]arrow.Array, total/batchRows)
				builder := array.NewLargeStringBuilder(arrowmem.DefaultAllocator)
				for i := range arrays {
					for row := 0; row < batchRows; row++ {
						builder.Append("payload")
					}
					arrays[i] = builder.NewArray()
				}
				builder.Release()
				b.Cleanup(func() {
					for _, a := range arrays {
						a.Release()
					}
				})
				input := &opaqueBatchScalingSource{opaqueScalingSource: &opaqueScalingSource{arrays: arrays, rows: total, schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow})}, batchRows: batchRows}
				ctx := context.Background()
				want := total
				if topK {
					want = total / 2
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					manager := memory.NewBudgetManager(0)
					var ordered source.Source
					var err error
					if topK {
						ordered, err = NewTopKSourceWithManager(input, want, []SortKey{{Column: "key"}}, manager)
					} else {
						ordered, err = NewSortSource(input, []SortKey{{Column: "key"}}, manager)
					}
					if err != nil {
						b.Fatal(err)
					}
					stream, err := source.OpenBatchStream(ctx, ordered, testReadOptions())
					if err != nil {
						b.Fatal(err)
					}
					out := memory.NewBatch()
					rows := 0
					for {
						ok, err := stream.Next(ctx, out)
						if err != nil {
							b.Fatal(err)
						}
						if !ok {
							break
						}
						rows += out.Rows
					}
					if err = stream.Close(); err != nil {
						b.Fatal(err)
					}
					if rows != want || manager.Used() != 0 {
						b.Fatal("row count or cleanup", rows, manager.Used())
					}
				}
			})
		}
	}
}
