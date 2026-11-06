package pipeline_test

import (
	"context"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestParallelPayloadBoundsNativeStringsAndExpandingProject(t *testing.T) {
	for _, kind := range []string{"inmemory", "arrow"} {
		for _, project := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/wide", true: "/project"}[project], func(t *testing.T) {
				schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
				batches := make([]*memory.Batch, 2)
				for part := range batches {
					b := memory.NewBatch()
					b.Rows = 70
					v := &memory.StringViewVector{Offsets: []uint32{0}, Validity: memory.EnsureBitmap(nil, 70)}
					for row := 0; row < 70; row++ {
						width := 180
						if project {
							width = 40
						}
						value := strings.Repeat(string(rune('a'+part)), width)
						v.Data = append(v.Data, value...)
						v.Offsets = append(v.Offsets, uint32(len(v.Data)))
						if row%5 != 1 {
							memory.Set(v.Validity, row)
						}
					}
					b.Strings["s"] = v
					batches[part] = b
				}
				var src source.Source
				if kind == "inmemory" {
					value, err := inmemory.New(schema, batches...)
					if err != nil {
						t.Fatal(err)
					}
					src = value
				} else {
					alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					a, err := memory.ArrowRecord(alloc, schema, batches[0])
					if err != nil {
						t.Fatal(err)
					}
					b, err := memory.ArrowRecord(alloc, schema, batches[1])
					if err != nil {
						a.Release()
						t.Fatal(err)
					}
					value, err := arrowrecord.NewSource(a, b)
					a.Release()
					b.Release()
					if err != nil {
						t.Fatal(err)
					}
					defer value.Close()
					src = value
				}
				manager := memory.NewBudgetManager(1 << 20)
				if project {
					value, err := physical.NewProjectSource(src, []expr.NamedExpr{expr.Named(expr.Concat(expr.Col("s"), expr.Col("s")), "twice")}, true, manager)
					if err != nil {
						t.Fatal(err)
					}
					src = value
				}
				parts := []source.Partition{{ID: 1, Rows: 70}, {ID: 0, Rows: 70}, {ID: 1, Rows: 70}, {ID: 0, Rows: 70}}
				opts := pipeline.Options{Workers: 2, TargetMorselBytes: 64, MaxMorselBytes: 256, MaxInFlightBytes: 512, MinMorselRows: 1, MaxMorselRows: 70, Ordering: pipeline.OrderingStable}
				stream, err := pipeline.OpenBatchStream(context.Background(), src, scalarReadOptions(), parts, manager, opts, observer.Nop{})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				count := 0
				for {
					ok, err := stream.Next(context.Background(), out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					size, err := memory.EstimateBatchRangeBytes(out, 0, out.Rows)
					if err != nil {
						t.Fatal(err)
					}
					if size > 256 {
						t.Fatalf("output payload=%d limit=256", size)
					}
					for row := 0; row < out.Rows; row++ {
						originalRow := count % 70
						part := parts[count/70].ID
						v := out.Strings["s"]
						valid := originalRow%5 != 1
						if v.IsValid(row) != valid {
							t.Fatalf("row %d validity", count)
						}
						width := 180
						if project {
							width = 40
						}
						want := strings.Repeat(string(rune('a'+part)), width)
						if valid && string(v.ValueBytes(row)) != want {
							t.Fatalf("row %d value", count)
						}
						if project {
							twice := out.Strings["twice"]
							if twice.IsValid(row) != valid || valid && string(twice.ValueBytes(row)) != want+want {
								t.Fatalf("row %d expanded value", count)
							}
						}
						count++
					}
				}
				if count != 280 {
					t.Fatalf("rows=%d want=280", count)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatalf("retained workspace=%d", manager.Used())
				}
				stats := stream.(pipeline.StreamStatsProvider).PipelineStats()
				if stats.Exchange.PeakInFlightBytes > 512 || stats.Exchange.CurrentInFlightBytes != 0 {
					t.Fatalf("inflight stats=%+v", stats.Exchange)
				}
			})
		}
	}
}
