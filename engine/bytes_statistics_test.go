package engine

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func bytesStatisticsFixture(t *testing.T) (*inmemory.Source, *memory.Batch) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 0, 1, 1, 1}}
	b.Int64s["n"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5, 6}}
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 3, 4, 4}, Data: []byte("abcd"), Validity: memory.Bitmap{0b110111}}
	b.Strings["b"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 2, 3, 3, 4, 6}, Data: []byte{0, 255, 0, 'x', 255, 0}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "n", Type: memory.TypeInt64}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeBinary}), b)
	if err != nil {
		t.Fatal(err)
	}
	return src, b
}
func TestBytesStatisticsStrategies(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "distinct"} {
		t.Run(mode, func(t *testing.T) {
			src, b := bytesStatisticsFixture(t)
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.First("s", "first"), expr.Last("s", "last"), expr.First("b", "bf"), expr.Last("b", "bl"), expr.Sum("n", "sum"), expr.Var("n", "var")}, Hints: expr.Hints{Cardinality: 2}}
			opts := streamTestOptions()
			opts.Workers = 3
			opts.Pipeline.MaxMorselRows = 1
			if mode == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else if mode == "distinct" {
				q.Aggregates = append(q.Aggregates, expr.NUnique("s", "unique"))
			} else {
				q.Hints.ForceStrategy = mode
			}
			if mode == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "k"})
			}
			e := New(opts)
			r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
			e.Close()
			if err != nil {
				t.Fatal(err)
			}
			b.Strings["s"].Data[0] = 'X'
			b.Strings["b"].Data[0] = 'X'
			if len(r.Rows) != 2 {
				t.Fatal(r)
			}
			for _, row := range r.Rows {
				key := row.Keys[0].Value.Bits
				a := row.Aggregates
				if key == 0 {
					if a[0].Value.Text != "a" || !a[0].Value.Valid || a[1].Value.Text != "c" || a[2].Value.Text != string([]byte{0, 255}) || a[3].Value.Text != string([]byte{0}) || a[4].Value.Bits != 6 {
						t.Fatal(a)
					}
				} else {
					if a[0].Value.Valid || !a[1].Value.Valid || a[1].Value.Text != "" || a[2].Value.Text != "" || a[3].Value.Text != string([]byte{255, 0}) || a[4].Value.Bits != 15 {
						t.Fatal(a)
					}
				}
			}
		})
	}
}

func TestBytesStatisticsColumnarStrategies(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "distinct"} {
		t.Run(mode, func(t *testing.T) {
			src, _ := bytesStatisticsFixture(t)
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.First("s", "first"), expr.Last("b", "last"), expr.Sum("n", "sum")}, Hints: expr.Hints{Cardinality: 2}}
			opts := streamTestOptions()
			opts.Workers = 3
			if mode == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else if mode == "distinct" {
				q.Aggregates = append(q.Aggregates, expr.NUnique("s", "unique"))
			} else {
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
			budget, err := e.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			var stream source.BatchStream
			if mode == "sorted" {
				stream, err = p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
			} else if mode == "spill" {
				stream, err = p.NewSpillableAggregateStream(ctx, p.resultSchema(), 1)
			} else {
				stream, err = p.NewResidentAggregateStream(ctx, p.resultSchema(), 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			count := 0
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for row := 0; row < out.Rows; row++ {
					k := out.Int32s["k"].Values[row]
					f, l := out.Strings["first"], out.Strings["last"]
					if k == 0 {
						if !f.IsValid(row) || string(f.ValueBytes(row)) != "a" || string(l.ValueBytes(row)) != string([]byte{0}) {
							t.Fatal("group0")
						}
					} else {
						if f.IsValid(row) || string(l.ValueBytes(row)) != string([]byte{255, 0}) {
							t.Fatal("group1")
						}
					}
					count++
				}
			}
			if count != 2 || budget.Used() != 0 {
				t.Fatalf("count%d budget%d", count, budget.Used())
			}
		})
	}
}

func TestBytesStatisticsPressureSpill(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64}, memory.Field{Name: "s", Type: memory.TypeString})
	batches := make([]*memory.Batch, 0, 64)
	for part := 0; part < 64; part++ {
		b := memory.NewBatch()
		b.Rows = 64
		k, v := make([]int32, 64), make([]uint64, 64)
		for row := range k {
			k[row] = int32((part*64 + row) % 1024)
			v[row] = math.MaxUint64 - 3 + uint64(part/16)
		}
		offsets := make([]uint32, 65)
		data := make([]byte, 64)
		for i := range data {
			offsets[i] = uint32(i)
			data[i] = byte(part / 16)
		}
		offsets[64] = 64
		b.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data}
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
	p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Var("v", "var"), expr.Std("v", "std"), expr.First("s", "first"), expr.Last("s", "last"), expr.Count("n")}}, PrepareOptions{})
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
			if math.Abs(out.Float64s["var"].Values[row]-5.0/3) > 1e-12 || math.Abs(out.Float64s["std"].Values[row]-math.Sqrt(5.0/3)) > 1e-12 || string(out.Strings["first"].ValueBytes(row)) != string([]byte{0}) || string(out.Strings["last"].ValueBytes(row)) != string([]byte{3}) || out.Uint64s["n"].Values[row] != 4 {
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

func TestBytesStatisticsRowRetryIsAtomic(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["n"] = &memory.Int64Vector{Values: []int64{1, 2}}
	b.Strings["a"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
	b.Strings["b"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("cd")}
	schema := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeString}, memory.Field{Name: "b", Type: memory.TypeBinary})
	compiled, err := compileMultiAggregate(schema, []expr.Aggregate{expr.Sum("n", "sum"), expr.Last("a", "a"), expr.Last("b", "b")})
	if err != nil {
		t.Fatal(err)
	}
	budget := memory.NewBudgetManager(1 << 20)
	calls, failAt := 0, 0
	reserve := func(n int64) (memory.Reservation, error) {
		calls++
		if calls == failAt {
			return nil, memory.ErrBudgetExceeded
		}
		return budget.Reserve(context.Background(), n)
	}
	table, err := newAggregateStateTable(GroupShapeGlobal, aggregate.Layout{}, 1, 16, 0, reserve, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = table.multi.add(aggregateRecord{ordinal: aggregate.SourceOrdinal{Row: 0}}, b, 0); err != nil {
		t.Fatal(err)
	}
	used := budget.Used()
	failAt = calls + 2
	if _, err = table.multi.add(aggregateRecord{ordinal: aggregate.SourceOrdinal{Row: 1}}, b, 1); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if budget.Used() != used || table.multi.numeric.Result(0, 0).SumBits != 1 || string(table.multi.statistics.BytesResult(0, 0).Last) != "a" {
		t.Fatal("failed row partially committed")
	}
	failAt = 0
	if _, err = table.multi.add(aggregateRecord{ordinal: aggregate.SourceOrdinal{Row: 1}}, b, 1); err != nil {
		t.Fatal(err)
	}
	if table.multi.numeric.Result(0, 0).SumBits != 3 || string(table.multi.statistics.BytesResult(0, 0).Last) != "b" || string(table.multi.statistics.BytesResult(0, 1).Last) != "d" {
		t.Fatal("retry double counted or lost endpoints")
	}
	table.close()
	if budget.Used() != 0 {
		t.Fatal("retry leaked", budget.Used())
	}
}
