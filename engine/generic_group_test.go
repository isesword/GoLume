package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestGenericGroupStrategies(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		for _, countOnly := range []bool{false, true} {
			t.Run(strategy+map[bool]string{true: "/count", false: "/sum"}[countOnly], func(t *testing.T) {
				b := memory.NewBatch()
				b.Rows = 8
				b.Float64s["f"] = &memory.Float64Vector{Values: []float64{0, math.Copysign(0, -1), math.NaN(), math.Float64frombits(0x7ff0000000000001), 0, 0, 0, 0}, Validity: memory.Bitmap{0xcf}}
				b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 1, 2, 4, 6, 7, 8}, Data: []byte("xxa\x00a\x00zz")}
				b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{1 << 63, 1 << 63, 7, 7, 8, 8, 0, 0}}
				b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5, 6, 7, 8}}
				src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "f", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "v", Type: memory.TypeInt64}), b)
				if err != nil {
					t.Fatal(err)
				}
				q := expr.Query{GroupBy: []string{"f", "s", "u"}, Aggregates: []expr.Aggregate{expr.Count("n")}}
				if !countOnly {
					q.Aggregates = append(q.Aggregates, expr.Sum("v", "sum"))
				}
				opts := streamTestOptions()
				opts.Workers = 3
				opts.MemoryLimitBytes = 1 << 20
				if strategy == "spill" {
					opts.ExecutionPolicy = ExecutionBounded
					opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
				} else {
					q.Hints.ForceStrategy = strategy
				}
				if strategy == "sorted" {
					src.WithOrdering(source.OrderingColumn{Name: "f"}, source.OrderingColumn{Name: "s"}, source.OrderingColumn{Name: "u"})
				}
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				summary, err := p.Execute(context.Background(), ExecuteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if summary.GroupCount != 4 || summary.SelectedRows != 8 {
					t.Fatalf("summary %+v", summary)
				}
				result, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Rows) != 4 {
					t.Fatalf("rows %d", len(result.Rows))
				}
				for _, row := range result.Rows {
					if len(row.Keys) != 3 {
						t.Fatalf("keys %+v", row.Keys)
					}
					if n, ok := row.Aggregates[0].Value.AsUint64(); !ok || n != 2 {
						t.Fatalf("count %+v", row)
					}
				}
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				var stream source.BatchStream
				if strategy == "sorted" {
					stream, err = p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
				} else if strategy == "spill" {
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
					count += out.Rows
					for row := 0; row < out.Rows; row++ {
						if out.Uint64s["n"].Values[row] != 2 {
							t.Fatal("columnar count")
						}
						if out.Strings["s"] == nil || out.Uint64s["u"] == nil || out.Float64s["f"] == nil {
							t.Fatal("missing native key")
						}
					}
				}
				if count != 4 || budget.Used() != 0 {
					t.Fatalf("columnar rows=%d budget=%d", count, budget.Used())
				}

			})
		}
	}
}

func TestGenericDictionaryDifferentCodebooks(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			a, b := memory.NewBatch(), memory.NewBatch()
			a.Rows = 2
			b.Rows = 2
			a.DictStrings["k"] = &memory.DictionaryStringVector{Indices: []uint32{0, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
			b.DictStrings["k"] = &memory.DictionaryStringVector{Indices: []uint32{1, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ba")}
			a.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2}}
			b.Int64s["v"] = &memory.Int64Vector{Values: []int64{3, 4}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, memory.Field{Name: "v", Type: memory.TypeInt64}), a, b)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("n")}}
			opts := streamTestOptions()
			opts.Workers = 2
			opts.MemoryLimitBytes = 1 << 20
			if strategy == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else {
				q.Hints.ForceStrategy = strategy
			}
			if strategy == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "k"})
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			result, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("groups %d", len(result.Rows))
			}
			r := result.Rows[0]
			if key, ok := r.Keys[0].Value.AsString(); !ok || key != "a" {
				t.Fatalf("key %+v", r.Keys)
			}
			if sum, ok := r.Aggregates[0].Value.AsInt64(); !ok || sum != 10 {
				t.Fatalf("sum %+v", r.Aggregates)
			}
		})
	}
}

func TestGenericSpillStreamPrecision(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeUint64}, memory.Field{Name: "v", Type: memory.TypeUint64})
	batches := make([]*memory.Batch, 0, 64)
	for part := 0; part < 64; part++ {
		b := memory.NewBatch()
		b.Rows = 64
		k, v := make([]uint64, 64), make([]uint64, 64)
		for row := range k {
			k[row] = 1<<63 + uint64((part*64+row)%1024)
			v[row] = k[row]
		}
		b.Uint64s["key"] = &memory.Uint64Vector{Values: k}
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
	p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Min("v", "min"), expr.Count("n")}}, PrepareOptions{})
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
			key := int(out.Uint64s["key"].Values[row] - 1<<63)
			if seen[key] {
				t.Fatal("duplicate")
			}
			seen[key] = true
			count++
			if out.Uint64s["sum"].Values[row] != uint64(key)*4 || out.Uint64s["min"].Values[row] != (1<<63+uint64(key)) || out.Uint64s["n"].Values[row] != 4 {
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

func TestGenericKeyBudgetFailureCleanup(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 1
			b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 256 << 10}, Data: make([]byte, 256<<10)}
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{7}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "u", Type: memory.TypeUint64}), b)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: []string{"s", "u"}, Aggregates: []expr.Aggregate{expr.Count("n")}}
			opts := streamTestOptions()
			opts.Workers = 2
			opts.MemoryLimitBytes = 64 << 10
			if strategy == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			} else {
				q.Hints.ForceStrategy = strategy
			}
			if strategy == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "s"}, source.OrderingColumn{Name: "u"})
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
			_, err = p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
			if err == nil {
				t.Fatal("expected bounded oversized key failure")
			}
			if budget.Used() != 0 {
				t.Fatalf("leaked budget %d", budget.Used())
			}
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
			cancel()
			_, err = p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true})
			if err == nil {
				t.Fatal("expected cancellation")
			}
			if budget.Used() != 0 {
				t.Fatalf("cancel leaked %d", budget.Used())
			}
		})
	}
}

func TestGenericManyComponentsAndOwnedKeys(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			fields := []memory.Field{{Name: "text", Type: memory.TypeString, Nullable: true}}
			names := []string{"text"}
			for i := 0; i < 11; i++ {
				name := fmt.Sprintf("k%d", i)
				names = append(names, name)
				fields = append(fields, memory.Field{Name: name, Type: memory.TypeBool, Nullable: true})
			}
			var batches []*memory.Batch
			for group := 0; group < 3; group++ {
				b := memory.NewBatch()
				b.Rows = 2
				v := &memory.StringViewVector{Offsets: []uint32{0, 0, 0}}
				if group == 1 {
					v.Validity = memory.Bitmap{0}
				}
				if group == 2 {
					v.Offsets = []uint32{0, 1, 2}
					v.Data = []byte{0, 0}
				}
				b.Strings["text"] = v
				for i := 0; i < 11; i++ {
					v := &memory.Uint32Vector{Values: []uint32{1, 1}}
					if i == 10 && group == 1 {
						v.Validity = memory.Bitmap{0}
					}
					b.Uint32s[names[i+1]] = v
				}
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(fields...), batches...)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: names, Aggregates: []expr.Aggregate{expr.Count("custom_count")}}
			opts := streamTestOptions()
			opts.Workers = 3
			opts.MemoryLimitBytes = 1 << 20
			if strategy == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else {
				q.Hints.ForceStrategy = strategy
			}
			if strategy == "sorted" {
				var ordering []source.OrderingColumn
				for _, name := range names {
					ordering = append(ordering, source.OrderingColumn{Name: name})
				}
				src.WithOrdering(ordering...)
			}
			e := New(opts)
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				e.Close()
				t.Fatal(err)
			}
			result, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
			p.Close()
			e.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range batches {
				for i := range b.Strings["text"].Data {
					b.Strings["text"].Data[i] = 'x'
				}
			}
			if len(result.Rows) != 3 {
				t.Fatalf("groups %d", len(result.Rows))
			}
			seen := map[string]bool{}
			for _, row := range result.Rows {
				if len(row.Keys) != 12 {
					t.Fatalf("keys %d", len(row.Keys))
				}
				v := row.Keys[0].Value
				if !v.Valid {
					seen["null"] = true
					if row.Keys[11].Value.Valid {
						t.Fatal("component null beyond bit8 lost")
					}
				} else {
					seen[v.Text] = true
				}
				if row.Aggregates[0].Alias != "custom_count" || row.Aggregates[0].Value.Bits != 2 {
					t.Fatal("alias/count")
				}
			}
			if !seen["null"] || !seen[""] || !seen["\x00"] {
				t.Fatalf("owned distinct keys %+v", seen)
			}
		})
	}
}

func TestGenericKeyPreservesSpecializedDictionaryEstimate(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "stable"}, memory.Field{Name: "i", Type: memory.TypeInt64})
	for _, keys := range [][]string{{"d"}, {"d", "i"}} {
		cq, err := compileQuery(schema, expr.Query{GroupBy: keys, Aggregates: []expr.Aggregate{expr.Count("n")}})
		if err != nil {
			t.Fatal(err)
		}
		want := 4
		if len(keys) == 2 {
			want = 12
		}
		if cq.KeyWidthBytes != want || cq.GroupShape == GroupShapeGeneric {
			t.Fatalf("compiled shape=%v width=%d", cq.GroupShape, cq.KeyWidthBytes)
		}
	}
}
