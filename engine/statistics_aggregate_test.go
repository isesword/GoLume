package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestStatisticsStrategies(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "global"} {
		t.Run(mode, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 6
			b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 0, 1, 1, 1}}
			b.Uint64s["v"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64, 5, 6, 7}, Validity: memory.Bitmap{0b011111}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64, Nullable: true}), b)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Var("v", "var"), expr.Std("v", "std"), expr.VarPopulation("v", "pop"), expr.StdPopulation("v", "pstd"), expr.First("v", "first"), expr.Last("v", "last")}, Hints: expr.Hints{Cardinality: 2}}
			opts := streamTestOptions()
			opts.Workers = 3
			opts.Pipeline.MaxMorselRows = 1
			opts.Pipeline.Ordering = pipeline.OrderingRelaxed
			switch mode {
			case "global":
				q.GroupBy = nil
				q.Filter = expr.LT(expr.Col("k"), expr.I64(1))
			case "spill":
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			default:
				q.Hints.ForceStrategy = mode
			}
			if mode == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "k"})
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if len(p.cq.MultiAggregate.Columns) != 0 {
				t.Fatal("stats-only has fake numeric lanes")
			}
			for repeat := 0; repeat < 2; repeat++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				want := 2
				if mode == "global" {
					want = 1
				}
				if len(r.Rows) != want {
					t.Fatalf("groups %d", len(r.Rows))
				}
				for _, row := range r.Rows {
					key := uint64(0)
					if len(row.Keys) > 0 {
						key = row.Keys[0].Value.Bits
					}
					variance, pop := 1.0, 2.0/3
					if key == 1 {
						variance, pop = .5, .25
					}
					for i, want := range []float64{variance, math.Sqrt(variance), pop, math.Sqrt(pop)} {
						got, ok := row.Aggregates[i].Value.AsFloat64()
						if !ok || math.Abs(got-want) > 1e-12 {
							t.Fatalf("key%d stat%d got%v valid%v want%v", key, i, got, ok, want)
						}
					}
					first := row.Aggregates[4].Value
					last := row.Aggregates[5].Value
					if key == 0 {
						if !first.Valid || first.Bits != math.MaxUint64-2 || !last.Valid || last.Bits != math.MaxUint64 {
							t.Fatalf("endpoints %+v %+v", first, last)
						}
					} else {
						if !first.Valid || first.Bits != 5 || last.Valid {
							t.Fatalf("null endpoint %+v %+v", first, last)
						}
					}
				}
			}
		})
	}
}

func TestStatisticsEmptyAndMixed(t *testing.T) {
	src, _ := nativeAggregateFixture(t)
	for _, empty := range []bool{false, true} {
		q := expr.Query{Aggregates: []expr.Aggregate{expr.Var("u64", "v"), expr.First("flag", "f"), expr.Last("flag", "l"), expr.Count("n"), expr.Sum("i64", "s")}}
		if empty {
			q.Filter = expr.Bool(false)
		}
		e := New(streamTestOptions())
		r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Rows) != 1 {
			t.Fatal(r)
		}
		a := r.Rows[0].Aggregates
		if empty {
			for _, i := range []int{0, 1, 2, 4} {
				if a[i].Value.Valid {
					t.Fatalf("empty %d %+v", i, a[i])
				}
			}
			if a[3].Value.Bits != 0 {
				t.Fatal(a)
			}
		} else {
			if !a[1].Value.Valid || a[1].Value.Bits != 0 || !a[2].Value.Valid || a[2].Value.Bits != 1 || a[3].Value.Bits != 6 {
				t.Fatal(a)
			}
		}
	}
}

func TestStatisticsSpillStreamPrecision(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64})
	batches := make([]*memory.Batch, 0, 64)
	for part := 0; part < 64; part++ {
		b := memory.NewBatch()
		b.Rows = 64
		k, v := make([]int32, 64), make([]uint64, 64)
		for row := range k {
			k[row] = int32((part*64 + row) % 1024)
			v[row] = math.MaxUint64 - 3 + uint64(part/16)
		}
		b.Int32s["key"] = &memory.Int32Vector{Values: k}
		b.Uint64s["v"] = &memory.Uint64Vector{Values: v}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 128 << 10
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
	e := New(opts)
	defer e.Close()
	p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Var("v", "var"), expr.Std("v", "std"), expr.First("v", "first"), expr.Last("v", "last"), expr.Count("n")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := make([]bool, 1024)
	count := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < out.Rows; row++ {
			key := int(out.Int32s["key"].Values[row])
			if seen[key] {
				t.Fatal("duplicate")
			}
			seen[key] = true
			count++
			if math.Abs(out.Float64s["var"].Values[row]-5.0/3) > 1e-12 || math.Abs(out.Float64s["std"].Values[row]-math.Sqrt(5.0/3)) > 1e-12 || out.Uint64s["first"].Values[row] != math.MaxUint64-3 || out.Uint64s["last"].Values[row] != math.MaxUint64 || out.Uint64s["n"].Values[row] != 4 {
				t.Fatalf("key %d lost native precision", key)
			}
		}
	}
	if count != 1024 || budget.Used() != 0 {
		t.Fatalf("count %d budget %d", count, budget.Used())
	}
	if stats := stream.(*spillableAggregateStream).Stats(); stats.SpillRuns < 2 || stats.SpillBytes == 0 {
		t.Fatalf("stats %+v", stats)
	}
}
