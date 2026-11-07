package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

func multiAggregateFixture(t testing.TB, rows int) (*inmemory.Source, map[int64][2]aggregate.Int64Result) {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32, Nullable: true}, memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true})
	batches := []*memory.Batch{}
	states := map[int64][2]*aggregate.Int64Vector{}
	layout := aggregate.Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	for start := 0; start < rows; start += 64 {
		n := min(64, rows-start)
		batch := memory.NewBatch()
		batch.Rows = n
		keys := &memory.Int32Vector{Values: make([]int32, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
		a := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
		b := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
		for row := 0; row < n; row++ {
			i := start + row
			key := int64(i / 8)
			keys.Values[row] = int32(key)
			if key != 0 {
				memory.Set(keys.Validity, row)
			}
			a.Values[row], b.Values[row] = int64(i%17-8), int64(i%31*3)
			if i%3 != 0 {
				memory.Set(a.Validity, row)
			}
			if i%5 != 0 {
				memory.Set(b.Validity, row)
			}
			pair, ok := states[key]
			if !ok {
				x, y := aggregate.NewInt64Vector(1, layout), aggregate.NewInt64Vector(1, layout)
				pair = [2]*aggregate.Int64Vector{&x, &y}
				states[key] = pair
			}
			pair[0].AddValue(0, a.Values[row], a.IsValid(row))
			pair[1].AddValue(0, b.Values[row], b.IsValid(row))
		}
		batch.Int32s["key"], batch.Int64s["a"], batch.Int64s["b"] = keys, a, b
		batches = append(batches, batch)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64][2]aggregate.Int64Result{}
	for key, pair := range states {
		want[key] = [2]aggregate.Int64Result{pair[0].Result(0), pair[1].Result(0)}
	}
	return src, want
}

func multiAggregateQuery() expr.Query {
	return expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{
		expr.Sum("a", "sum_a"), expr.Count("rows"), expr.CountColumn("a", "count_a"), expr.Min("b", "min_b"), expr.Max("a", "max_a"), expr.Avg("b", "avg_b"), expr.Sum("b", "sum_b"), expr.Min("a", "min_a"), expr.Max("b", "max_b"), expr.CountColumn("b", "count_b"), expr.Avg("a", "avg_a"), expr.Sum("a", "again_a"),
	}, Hints: expr.Hints{Cardinality: 8}}
}

func TestMultiColumnAggregateAcrossStrategies(t *testing.T) {
	for _, kind := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(kind, func(t *testing.T) {
			src, want := multiAggregateFixture(t, 4096)
			opts := streamTestOptions()
			opts.Workers = 4
			opts.MemoryLimitBytes = 512 << 10
			q := multiAggregateQuery()
			if kind == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "key"})
			}
			if kind == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.MemoryLimitBytes = 512 << 10
				opts.QueryMemorySoftLimitBytes = 48 << 10
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
			} else {
				q.Hints.ForceStrategy = kind
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			for repeat := 0; repeat < 2; repeat++ {
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				result, err := p.Execute(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if result.GroupCount != len(want) || result.SelectedRows != 4096 {
					t.Fatalf("summary %+v", result)
				}
				for _, row := range result.Rows {
					key := row.Keys[0].Int64
					if !row.Keys[0].Valid {
						key = 0
					}
					expected := want[key]
					for i, a := range q.Aggregates {
						lane := 0
						if a.Column == "b" {
							lane = 1
						}
						st := expected[lane]
						want := AggregateValue{Alias: a.Alias, Kind: a.Kind}
						switch a.Kind {
						case expr.AggSum:
							want.Valid, want.Int64 = st.SumValid, st.Sum
						case expr.AggCountRows:
							want.Valid, want.Int64 = true, int64(st.CountRows)
						case expr.AggCountValues:
							want.Valid, want.Int64 = true, int64(st.CountValues)
						case expr.AggMin:
							want.Valid, want.Int64 = st.MinValid, st.Min
						case expr.AggMax:
							want.Valid, want.Int64 = st.MaxValid, st.Max
						case expr.AggAvg:
							want.Valid, want.Float64, want.IsFloat = st.AvgValid, st.Avg, true
						}
						if !reflect.DeepEqual(row.Aggregates[i], want) {
							t.Fatalf("key %d agg %s got %+v want %+v", key, a.Alias, row.Aggregates[i], want)
						}
					}
				}
				if budget.Used() != 0 {
					t.Fatalf("leaked %d", budget.Used())
				}
				budget.Close()
			}
		})
	}
}

func TestMultiColumnGlobalAndEmpty(t *testing.T) {
	for _, rows := range []int{0, 129} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			src, states := multiAggregateFixture(t, rows)
			q := multiAggregateQuery()
			q.GroupBy = nil
			e := New(streamTestOptions())
			defer e.Close()
			result, err := e.Execute(context.Background(), src, q, ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if result.GroupCount != 1 || len(result.Rows) != 1 || len(result.Rows[0].Keys) != 0 {
				t.Fatalf("result %+v", result)
			}
			var a, b int64
			for _, pair := range states {
				a += pair[0].Sum
				b += pair[1].Sum
			}
			got := result.Rows[0].Aggregates
			if got[0].Int64 != a || got[6].Int64 != b || got[1].Int64 != int64(rows) || got[0].Valid != (rows > 0) {
				t.Fatalf("aggregates %+v", got)
			}
		})
	}
}

func TestMultiColumnSpillStreamingBounded(t *testing.T) {
	src, want := multiAggregateFixture(t, 8192)
	opts := streamTestOptions()
	opts.ExecutionPolicy = ExecutionBounded
	opts.MemoryLimitBytes = 128 << 10
	opts.QueryMemorySoftLimitBytes = 64 << 10
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
	e := New(opts)
	defer e.Close()
	p, err := e.Prepare(context.Background(), src, multiAggregateQuery(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := map[int64]bool{}
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < out.Rows; row++ {
			key := int64(out.Int32s["key"].Values[row])
			if !out.Int32s["key"].IsValid(row) {
				key = 0
			}
			if seen[key] {
				t.Fatalf("duplicate group %d", key)
			}
			seen[key] = true
			pair := want[key]
			if out.Int64s["sum_a"].Values[row] != pair[0].Sum || out.Int64s["sum_b"].Values[row] != pair[1].Sum || out.Float64s["avg_b"].Values[row] != pair[1].Avg || out.Uint64s["rows"].Values[row] != pair[0].CountRows {
				t.Fatalf("key %d output mismatch", key)
			}
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("seen %d want %d", len(seen), len(want))
	}
	if budget.Used() != 0 || budget.Snapshot().PeakBytes > opts.MemoryLimitBytes {
		t.Fatalf("budget %+v", budget.Snapshot())
	}
	stats := stream.(*spillableAggregateStream).Stats()
	if stats.SpillRuns < 2 || stats.SpillBytes == 0 {
		t.Fatalf("no effective spill %+v", stats)
	}
}
