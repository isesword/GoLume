package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

// Independent expectations intentionally do not use NumericVector as an oracle.
func TestNativeReviewIEEEAndUnsignedStrategies(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 6
			b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 1, 1, 2, 2}}
			b.Uint64s["u"] = &memory.Uint64Vector{Values: []uint64{math.MaxUint64, 2, 1<<63 + 1, 9, 0, 1}}
			b.Float32s["f"] = &memory.Float32Vector{Values: []float32{float32(math.NaN()), 3, float32(math.Copysign(0, -1)), 0, float32(math.Inf(1)), float32(math.Inf(-1))}}
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
			src, err := inmemory.New(schema, b)
			if err != nil {
				t.Fatal(err)
			}
			q := expr.Query{GroupBy: []string{"k"}, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Sum("u", "sum"), expr.Min("u", "min"), expr.Min("f", "fmin"), expr.Max("f", "fmax"), expr.Avg("f", "fmean")}, Hints: expr.Hints{Cardinality: 4}}
			opts := streamTestOptions()
			opts.Workers = 3
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
			r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if r.Summary.Rows != nil || len(r.Rows) != 3 {
				t.Fatalf("result=%+v", r)
			}
			for _, row := range r.Rows {
				key, ok := row.Keys[0].Value.AsInt64()
				if !ok || key < 0 || key > 2 {
					t.Fatal(row.Keys)
				}
				wantSum := []uint64{1, 1<<63 + 10, 1}[key]
				wantMin := []uint64{2, 9, 0}[key]
				for i, want := range []uint64{2, wantSum, wantMin} {
					got, ok := row.Aggregates[i].Value.AsUint64()
					if !ok || got != want {
						t.Fatalf("key=%d index=%d got=%d want=%d", key, i, got, want)
					}
				}
				lo, lok := row.Aggregates[3].Value.AsFloat64()
				hi, hik := row.Aggregates[4].Value.AsFloat64()
				mean, mok := row.Aggregates[5].Value.AsFloat64()
				if !lok || !hik || !mok || row.Aggregates[5].Value.Type != memory.TypeFloat32 {
					t.Fatal(row.Aggregates)
				}
				switch key {
				case 0:
					if lo != 3 || hi != 3 || !math.IsNaN(mean) {
						t.Fatalf("NaN handling: %v %v %v", lo, hi, mean)
					}
				case 1:
					if lo != 0 || !math.Signbit(lo) || hi != 0 || math.Signbit(hi) || mean != 0 {
						t.Fatalf("signed zeros: %v %v %v", lo, hi, mean)
					}
				case 2:
					if !math.IsInf(lo, -1) || !math.IsInf(hi, 1) || !math.IsNaN(mean) {
						t.Fatalf("infinities: %v %v %v", lo, hi, mean)
					}
				}
			}
		})
	}
}

func TestNativeReviewCountParentValidity(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{1, 2, 3}, Validity: memory.Bitmap{5}}
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 0, 1, 1}, Values: &memory.StringViewVector{Offsets: []uint32{0, 0}, Validity: memory.Bitmap{0}}, Validity: memory.Bitmap{6}, Encoding: memory.EncodingView}
	child := memory.Field{Name: "child", Type: memory.TypeInt64, Nullable: true}
	children := memory.NewBatch()
	children.Rows = 3
	children.Int64s["child"] = &memory.Int64Vector{Values: []int64{0, 0, 0}, Validity: memory.Bitmap{0}}
	b.Structs["record"] = &memory.StructVector{Rows: 3, Fields: []memory.Field{child}, Values: children, Validity: memory.Bitmap{3}, Encoding: memory.EncodingView}
	schema := memory.NewSchema(memory.Field{Name: "date", Type: memory.TypeDate32, Nullable: true}, memory.ListStringField("list", true, true), memory.StructField("record", true, child))
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	e := New(Options{MemoryLimitBytes: 1 << 20})
	defer e.Close()
	r, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.CountColumn("date", "dates"), expr.CountColumn("list", "lists"), expr.CountColumn("record", "records"), expr.Count("rows")}}, ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 {
		t.Fatal(r)
	}
	for i, expected := range []uint64{2, 2, 2, 3} {
		v, ok := r.Rows[0].Aggregates[i].Value.AsUint64()
		if !ok || v != expected {
			t.Fatalf("column %d got %d, want %d", i, v, expected)
		}
	}
}

func TestNativeReviewColumnarAllWidths(t *testing.T) {
	for _, mode := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill"} {
		t.Run(mode, func(t *testing.T) {
			src, cols := nativeAggregateFixture(t)
			q := expr.Query{GroupBy: []string{"key"}, Hints: expr.Hints{Cardinality: 4}}
			for _, c := range cols[:10] {
				q.Aggregates = append(q.Aggregates, expr.Min(c.name, c.name))
			}
			opts := streamTestOptions()
			opts.Workers = 3
			if mode == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else {
				q.Hints.ForceStrategy = mode
			}
			if mode == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "key"})
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
			defer stream.Close()
			out, groups := memory.NewBatch(), 0
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if out.Rows != 1 {
					t.Fatalf("batch rows %d", out.Rows)
				}
				groups++
				if len(out.Int8s["i8"].Values) != 1 || len(out.Int16s["i16"].Values) != 1 || len(out.Int32s["i32"].Values) != 1 || len(out.Int64s["i64"].Values) != 1 || len(out.Uint8s["u8"].Values) != 1 || len(out.Uint16s["u16"].Values) != 1 || len(out.Uint32s["u32"].Values) != 1 || len(out.Uint64s["u64"].Values) != 1 || len(out.Float32s["f32"].Values) != 1 || len(out.Float64s["f64"].Values) != 1 {
					t.Fatal("native output not shrunk to one row")
				}
				if !out.Int32s["key"].IsValid(0) {
					if out.Int8s["i8"].Values[0] != -128 || out.Int16s["i16"].Values[0] != -32768 || out.Uint64s["u64"].Values[0] != 1 || out.Float32s["f32"].Values[0] != 7 {
						t.Fatal("incorrect native null-key group")
					}
				}
			}
			if groups != 3 || budget.Used() != 0 {
				t.Fatalf("groups=%d retained=%d", groups, budget.Used())
			}
		})
	}
}
