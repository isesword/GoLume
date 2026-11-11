package pipeline

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sync"
	"testing"
)

type perfUnknownSource struct{ source.Source }

func (s perfUnknownSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, p []source.Partition) (source.BatchStream, error) {
	o.BatchPayloadTargetBytes = 64 << 10
	return source.OpenBatchStreamWithPartitions(ctx, s.Source, o, p)
}
func perfFixture(kind string, rows int) (*memory.Batch, memory.Schema) {
	b := memory.NewBatch()
	b.Rows = rows
	switch kind {
	case "scalar":
		v := make([]int64, rows)
		for i := range v {
			v[i] = int64(i)
		}
		b.Int64s["x"] = &memory.Int64Vector{Values: v}
		return b, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	case "string":
		o := make([]uint32, rows+1)
		for i := range o {
			o[i] = uint32(8 * i)
		}
		b.Strings["x"] = &memory.StringViewVector{Offsets: o, Data: make([]byte, 8*rows)}
		return b, memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString})
	default:
		o := make([]uint32, rows+1)
		for i := range o {
			o[i] = uint32(i)
		}
		c := memory.NewBatch()
		c.Rows = rows
		c.StringLists["l"] = &memory.StringListVector{Offsets: o, Values: &memory.StringViewVector{Offsets: o, Data: make([]byte, rows)}}
		f := memory.ListStringField("l", false, false)
		b.Structs["s"] = &memory.StructVector{Rows: rows, Fields: []memory.Field{f}, Values: c}
		return b, memory.NewSchema(memory.StructField("s", false, f))
	}
}
func BenchmarkRealOwnedFanOut(b *testing.B) {
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		for _, batchRows := range []int{256, 65536} {
			b.Run(fmt.Sprintf("%s/batch%d", kind, batchRows), func(b *testing.B) {
				const rows = 65536
				input, schema := perfFixture(kind, rows)
				base, err := inmemory.New(schema, input)
				if err != nil {
					b.Fatal(err)
				}
				src := perfUnknownSource{Source: base}
				engine := memory.NewEngineBudget(1 << 30)
				q, err := engine.BeginQuery(memory.BudgetOptions{})
				if err != nil {
					b.Fatal(err)
				}
				defer q.Close()
				ctx := memory.WithQueryBudget(context.Background(), q)
				parts, _ := src.Partitions(ctx)
				ro := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: batchRows}
				opts := Options{Workers: 1, TargetMorselBytes: 64 << 10, MaxMorselBytes: 256 << 10, MaxInFlightBytes: 1 << 20, MinMorselRows: 256, MaxMorselRows: 65536}
				m := q
				var totalItems int64
				payload, _ := memory.EstimateBatchRangeBytes(input, 0, rows)
				b.SetBytes(2 * payload)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					streams, err := OpenFanOut(ctx, src, ro, parts, m, opts, 2, nil)
					if err != nil {
						b.Fatal(err)
					}
					var wg sync.WaitGroup
					type result struct {
						err   error
						items int
					}
					errs := make(chan result, 2)
					for _, st := range streams {
						wg.Add(1)
						go func(st source.BatchStream) {
							defer wg.Done()
							defer st.Close()
							out := memory.NewBatch()
							total, items := 0, 0
							for {
								ok, e := st.Next(ctx, out)
								if e != nil {
									errs <- result{err: e}
									return
								}
								if !ok {
									break
								}
								total += out.Rows
								items++
							}
							if total != rows {
								errs <- result{err: fmt.Errorf("rows %d", total)}
								return
							}
							errs <- result{items: items}
						}(st)
					}
					wg.Wait()
					close(errs)
					for e := range errs {
						if e.err != nil {
							b.Fatal(e.err)
						}
						totalItems += int64(e.items)
					}
					if m.Used() != 0 || engine.Used() != 0 {
						b.Fatal("reservation leak")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(q.Snapshot().PeakBytes), "peak-budget-B")
				b.ReportMetric(float64(totalItems)/float64(2*b.N), "items/branch")
			})
		}
	}
}
