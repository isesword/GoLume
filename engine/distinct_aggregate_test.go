package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func TestDistinctStrategies(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "global"} {
		t.Run(mode, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 10
			b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}}
			b.Float64s["v"] = &memory.Float64Vector{Values: []float64{math.NaN(), math.Float64frombits(0x7ff0000000000001), 0, math.Copysign(0, -1), 10, 1, 1, 2, 3, 4}, Validity: memory.Bitmap{0b0111111111}}
			b.Int64s["n"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "n", Type: memory.TypeInt64}), b)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), expr.NUnique("v", "same"), expr.NUnique("n", "un"), expr.Sum("n", "sum"), expr.Count("rows"), expr.Var("n", "var"), expr.First("n", "first"), expr.Last("n", "last")}, Hints: expr.Hints{Cardinality: 2}}
			opts := streamTestOptions()
			opts.Workers = 3
			opts.Pipeline.MaxMorselRows = 2
			switch mode {
			case "global":
				q.GroupBy = nil
			case "spill":
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
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
			if len(p.distinct().Fields) != 2 {
				t.Fatal("duplicate lane not shared")
			}
			for repeat := 0; repeat < 2; repeat++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				wantGroups := 2
				if mode == "global" {
					wantGroups = 1
				}
				if len(r.Rows) != wantGroups {
					t.Fatal(r)
				}
				for _, row := range r.Rows {
					u, sum, n, first, last, variance := uint64(3), uint64(15), uint64(5), uint64(1), uint64(5), 2.5
					if mode == "global" {
						u, sum, n, first, last, variance = 7, 55, 10, 1, 10, 55.0/6
					} else if row.Keys[0].Value.Bits == 1 {
						u, sum, first, last = 4, 40, 6, 10
					}
					for i, want := range []uint64{u, u, n, sum, n} {
						if row.Aggregates[i].Value.Bits != want || !row.Aggregates[i].Value.Valid {
							t.Fatalf("%s agg%d got%+v want%d", mode, i, row.Aggregates[i], want)
						}
					}
					got, _ := row.Aggregates[5].Value.AsFloat64()
					if math.Abs(got-variance) > 1e-12 {
						t.Fatalf("var %v want%v", got, variance)
					}
					if row.Aggregates[6].Value.Bits != first || row.Aggregates[7].Value.Bits != last {
						t.Fatal(row)
					}
				}
			}
		})
	}
}

func TestDistinctEmptyAndNull(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{0, 0, 0}, Validity: memory.Bitmap{0}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, empty := range []bool{false, true} {
		e := New(streamTestOptions())
		q := expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("v", "u")}}
		if empty {
			q.Filter = expr.Bool(false)
		}
		r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		want := uint64(1)
		if empty {
			want = 0
		}
		if len(r.Rows) != 1 || r.Rows[0].Aggregates[0].Value.Bits != want {
			t.Fatal(r)
		}
	}
}

func TestDistinctSpillHotAndManyGroups(t *testing.T) {
	for _, groups := range []int{1, 128} {
		t.Run(fmt.Sprint(groups), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64})
			var batches []*memory.Batch
			for part := 0; part < 48; part++ {
				b := memory.NewBatch()
				b.Rows = 32
				k := make([]int32, 32)
				v := make([]uint64, 32)
				for row := range k {
					i := part*32 + row
					k[row] = int32(i % groups)
					v[row] = math.MaxUint64 - uint64(i%512)
				}
				b.Int32s["k"] = &memory.Int32Vector{Values: k}
				b.Uint64s["v"] = &memory.Uint64Vector{Values: v}
				batches = append(batches, b)
			}
			src, err := inmemory.New(schema, batches...)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.ExecutionPolicy = ExecutionBounded
			opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), expr.Count("n"), expr.First("v", "first"), expr.Last("v", "last")}}, PrepareOptions{})
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
			seen := 0
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				seen += out.Rows
				for row := 0; row < out.Rows; row++ {
					key := out.Int32s["k"].Values[row]
					if out.Uint64s["u"].Values[row] != uint64(512/groups) || out.Uint64s["n"].Values[row] != uint64(1536/groups) || out.Uint64s["first"].Values[row] != math.MaxUint64-uint64(key) || out.Uint64s["last"].Values[row] != math.MaxUint64-uint64(512-groups+int(key)) {
						t.Fatal(out)
					}
				}
			}
			if st := stream.(*distinctAggregateStream).Stats(); st.SpillRuns < 2 || st.SpillBytes == 0 {
				t.Fatalf("not genuine spill %+v", st)
			}
			if seen != groups || budget.Used() != 0 {
				t.Fatalf("groups%d used%d", seen, budget.Used())
			}
		})
	}
}

func TestDistinctNativeTypes(t *testing.T) {
	src, cols := nativeAggregateFixture(t)
	var aggs []expr.Aggregate
	for _, c := range cols {
		aggs = append(aggs, expr.NUnique(c.name, c.name))
	}
	e := New(streamTestOptions())
	defer e.Close()
	r, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: aggs}, ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		want := uint64(6)
		switch c.name {
		case "f32", "f64", "s":
			want = 5
		case "flag":
			want = 3
		}
		v := r.Rows[0].Aggregates[i].Value
		if v.Type != memory.TypeUint64 || !v.Valid || v.Bits != want {
			t.Fatalf("%s got%+v want%d", c.name, v, want)
		}
	}
}

func TestDistinctSinglePassAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		src := &generatedStreamSource{rows: 512, groups: 4}
		if cancelled {
			src.cancel = cancel
			src.cancelAt = 128
		}
		opts := streamTestOptions()
		opts.ExecutionPolicy = ExecutionBounded
		opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
		e := New(opts)
		p, err := e.Prepare(ctx, src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.NUnique("value", "u"), expr.Sum("value", "sum"), expr.First("value", "first")}}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		budget, err := e.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		r, err := p.ExecuteTyped(memory.WithQueryBudget(ctx, budget), ExecuteOptions{Materialize: true})
		if cancelled {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error%v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 4 {
				t.Fatal(r)
			}
			for _, row := range r.Rows {
				if row.Aggregates[0].Value.Bits != 1 || row.Aggregates[1].Value.Bits != 128 {
					t.Fatal(row)
				}
			}
		}
		if src.opened.Load() != 1 || src.closed.Load() != 1 || budget.Used() != 0 {
			t.Fatalf("opened%d closed%d used%d", src.opened.Load(), src.closed.Load(), budget.Used())
		}
		budget.Close()
		p.Close()
		e.Close()
		cancel()
	}
}

func BenchmarkExactDistinct(b *testing.B) {
	for _, spill := range []bool{false, true} {
		b.Run(fmt.Sprintf("spill_%v", spill), func(b *testing.B) {
			src := &generatedStreamSource{rows: 4096, groups: 64}
			opts := streamTestOptions()
			opts.MemoryLimitBytes = 4 << 20
			if spill {
				opts.MemoryLimitBytes = 256 << 10
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: b.TempDir(), MaxBytes: 32 << 20}
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.NUnique("value", "u"), expr.Sum("value", "sum"), expr.Count("n")}}, PrepareOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.ExecuteTyped(context.Background(), ExecuteOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDistinctDictionaryCodebooksAndLongValues(t *testing.T) {
	for _, spilled := range []bool{false, true} {
		t.Run(fmt.Sprint(spilled), func(t *testing.T) {
			long := strings.Repeat("x", 100000)
			var batches []*memory.Batch
			for i := 0; i < 2; i++ {
				b := memory.NewBatch()
				b.Rows = 2
				data, indices := "ab", []uint32{0, 1}
				if i == 1 {
					data, indices = "ba", []uint32{1, 0}
				}
				b.DictStrings["v"] = &memory.DictionaryStringVector{Indices: indices, Offsets: []uint32{0, 1, 2}, Data: []byte(data)}
				b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(long)), uint32(len(long))}, Data: []byte(long)}
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, memory.Field{Name: "s", Type: memory.TypeString}), batches...)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.MemoryLimitBytes = 4 << 20
			opts.Pipeline.MaxMorselBytes = 512 << 10
			opts.Pipeline.MaxInFlightBytes = 1 << 20
			if spilled {
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
			}
			e := New(opts)
			defer e.Close()
			r, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), expr.NUnique("s", "long")}}, ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 1 || r.Rows[0].Aggregates[0].Value.Bits != 2 || r.Rows[0].Aggregates[1].Value.Bits != 2 {
				t.Fatal(r)
			}
		})
	}
}
