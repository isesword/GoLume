package engine

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strconv"
	"testing"
)

func statisticsReviewSource(t *testing.T, sorted bool) *inmemory.Source {
	t.Helper()
	// Partition boundaries split both groups; the middle partition is completely
	// filtered out. Every emitted endpoint must retain pre-filter source order.
	batches := make([]*memory.Batch, 5)
	for part := range batches {
		b := memory.NewBatch()
		b.Rows = 8
		keys := make([]int32, 8)
		values := make([]int64, 8)
		keep := make([]uint32, 8)
		dates := make([]int32, 8)
		for i := range keys {
			row := part*8 + i
			keys[i] = int32(row % 2)
			if sorted {
				keys[i] = int32(row / 20)
			}
			values[i] = int64(100 + row)
			dates[i] = int32(-100 + row)
			if part != 2 && i%3 != 0 {
				keep[i] = 1
			}
		}
		b.Int32s["k"] = &memory.Int32Vector{Values: keys}
		b.Int64s["v"] = &memory.Int64Vector{Values: values}
		b.Int32s["d"] = &memory.Int32Vector{Values: dates, Validity: memory.Bitmap{0b01111111}}
		b.Uint32s["keep"] = &memory.Uint32Vector{Values: keep}
		batches[part] = b
	}
	s, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "d", Type: memory.TypeDate32, Nullable: true}, memory.Field{Name: "keep", Type: memory.TypeBool}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	if sorted {
		s.WithOrdering(source.OrderingColumn{Name: "k"})
	}
	return s
}
func statisticsReviewQuery() expr.Query {
	return expr.Query{GroupBy: []string{"k"}, Filter: expr.Col("keep"), Aggregates: []expr.Aggregate{expr.Last("d", "last_date"), expr.Sum("v", "sum"), expr.VarPopulation("v", "pop"), expr.First("d", "first_date"), expr.Count("count"), expr.Var("v", "sample"), expr.First("v", "first"), expr.Last("v", "last")}, Hints: expr.Hints{Cardinality: 2}}
}
func TestStatisticsReviewFilteredPartitions(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(mode, func(t *testing.T) {
			sorted := mode == "sorted"
			src := statisticsReviewSource(t, sorted)
			q := statisticsReviewQuery()
			opts := streamTestOptions()
			opts.Workers = 4
			opts.Pipeline.Workers = 4
			opts.Pipeline.Ordering = pipeline.OrderingRelaxed
			opts.Pipeline.MaxMorselRows = 2
			if mode == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			} else {
				q.Hints.ForceStrategy = mode
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			schema := p.resultSchema()
			for _, name := range []string{"last_date", "first_date"} {
				f, ok := schema.Field(name)
				if !ok || f.Type != memory.TypeDate32 || !f.Nullable {
					t.Fatal(f)
				}
			}
			for repeat := 0; repeat < 3; repeat++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Rows) != 2 {
					t.Fatalf("groups %d", len(r.Rows))
				}
				for _, row := range r.Rows {
					key := int(row.Keys[0].Value.Bits)
					var values []int
					for x := 0; x < 40; x++ {
						k := x % 2
						if sorted {
							k = x / 20
						}
						if k == key && x/8 != 2 && x%8%3 != 0 {
							values = append(values, x)
						}
					}
					first, last := values[0], values[len(values)-1]
					a := row.Aggregates
					if a[0].Value.Valid != (last%8 != 7) || a[3].Value.Valid != (first%8 != 7) {
						t.Fatal("endpoint NULL was skipped", a)
					}
					if a[3].Value.Valid && int32(a[3].Value.Bits) != int32(-100+first) {
						t.Fatal("first date", a)
					}
					if a[0].Value.Valid && int32(a[0].Value.Bits) != int32(-100+last) {
						t.Fatal("last date", a)
					}
					if a[6].Value.Bits != uint64(100+first) || a[7].Value.Bits != uint64(100+last) || a[4].Value.Bits != uint64(len(values)) {
						t.Fatal("order/count", a)
					}
					sum, mean := 0, 0.0
					for _, x := range values {
						sum += 100 + x
						mean += float64(100 + x)
					}
					mean /= float64(len(values))
					m2 := 0.0
					for _, x := range values {
						d := float64(100+x) - mean
						m2 += d * d
					}
					if a[1].Value.Bits != uint64(sum) {
						t.Fatal("sum", a)
					}
					for i, want := range map[int]float64{2: m2 / float64(len(values)), 5: m2 / float64(len(values)-1)} {
						v, ok := a[i].Value.AsFloat64()
						if !ok || math.Abs(v-want) > 1e-10 {
							t.Fatalf("stat %d got %g want %g", i, v, want)
						}
					}
				}
			}
			p.Close()
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			if e.Memory().Used() != 0 {
				t.Fatal("leaked", e.Memory().Used())
			}
		})
	}
}
func TestStatisticsReviewCleanup(t *testing.T) {
	for _, mode := range []string{"partitioned-hash", "sorted", "spill"} {
		for _, limit := range []int64{256, 1024, 4096, 1 << 20} {
			t.Run(mode+"/"+strconv.FormatInt(limit, 10), func(t *testing.T) {
				src := statisticsReviewSource(t, mode == "sorted")
				q := statisticsReviewQuery()
				opts := streamTestOptions()
				opts.MemoryLimitBytes = limit
				if mode == "spill" {
					opts.ExecutionPolicy = ExecutionBounded
					opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
				} else {
					q.Hints.ForceStrategy = mode
				}
				e := New(opts)
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err == nil {
					_, err = p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
					p.Close()
				}
				if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatal(err)
				}
				e.Close()
				if e.Memory().Used() != 0 {
					t.Fatalf("leak %d", e.Memory().Used())
				}
			})
		}
	}
}

func TestStatisticsReviewEarlyClose(t *testing.T) {
	for _, mode := range []string{"partitioned-hash", "sorted", "spill"} {
		for _, cancelled := range []bool{false, true} {
			t.Run(mode+"/"+strconv.FormatBool(cancelled), func(t *testing.T) {
				src := statisticsReviewSource(t, mode == "sorted")
				q := statisticsReviewQuery()
				opts := streamTestOptions()
				if mode == "spill" {
					opts.ExecutionPolicy = ExecutionBounded
					opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
				} else {
					q.Hints.ForceStrategy = mode
				}
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
				defer cancel()
				var stream source.BatchStream
				switch mode {
				case "sorted":
					stream, err = p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
				case "spill":
					stream, err = p.NewSpillableAggregateStream(ctx, p.resultSchema(), 1)
				default:
					stream, err = p.NewResidentAggregateStream(ctx, p.resultSchema(), 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				if cancelled {
					cancel()
				}
				dst := memory.NewBatch()
				ok, err := stream.Next(ctx, dst)
				if cancelled {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel returned %v", err)
					}
				} else if err != nil || !ok {
					t.Fatalf("next %v %v", ok, err)
				}
				if err = stream.Close(); err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if err = stream.Close(); err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				dst.ReleaseOwned()
				p.Close()
				budget.Close()
				e.Close()
				if e.Memory().Used() != 0 {
					t.Fatalf("retained after close %d", e.Memory().Used())
				}
			})
		}
	}
}
