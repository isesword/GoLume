package lazy

import (
	"context"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func TestConfiguredQuantileSchemaCopyAndValidation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32}, memory.Field{Name: "s", Type: memory.TypeString})
	src, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{})
	defer sess.Close()
	spec := expr.Quantile("v", .25, aggregate.QuantileLinear, "q")
	specs := []expr.AggregateSpec{spec, Median("f", "m"), Count("n")}
	q := sess.FromSource(src).GroupBy("k").AggConfigured(specs...)
	specs[0] = Median("absent", "bad")
	spec.Probability = math.NaN()
	out, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []memory.DataType{memory.TypeInt32, memory.TypeFloat64, memory.TypeFloat32, memory.TypeUint64} {
		if out.Fields[i].Type != want {
			t.Fatal(out)
		}
	}
	if !out.Fields[1].Nullable || !out.Fields[2].Nullable || out.Fields[3].Nullable {
		t.Fatal(out)
	}
	for _, bad := range []expr.AggregateSpec{Quantile("v", math.NaN(), aggregate.QuantileLinear, "q"), Quantile("v", .5, 99, "q"), Median("missing", "q"), Median("s", "q"), expr.Aggregate{Kind: expr.AggQuantile, Column: "v", Alias: "q"}, nil} {
		if _, err := sess.FromSource(src).AggConfigured(bad).Schema(); err == nil {
			t.Fatalf("accepted %#v", bad)
		}
	}
	if _, err := sess.FromSource(src).AggConfigured(Median("v", "same"), Count("same")).Schema(); err == nil {
		t.Fatal("duplicate alias")
	}
	if _, err := sess.FromSource(src).AggConfigured().Schema(); err == nil {
		t.Fatal("empty aggregates")
	}
	if !strings.Contains(q.Explain(), "q=0.25") {
		t.Fatal(q.Explain())
	}
}

func TestConfiguredQuantileMixedDownstreamAndPolicies(t *testing.T) {
	for _, policy := range []engine.ExecutionPolicy{engine.ExecutionMemoryOnly, engine.ExecutionAuto, engine.ExecutionBounded} {
		t.Run(policy.String(), func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 6
			b.Int32s["k"] = &memory.Int32Vector{Values: []int32{1, 1, 1, 2, 2, 2}}
			b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 3, 5, 2, 4, 6}}
			b.Float32s["f"] = &memory.Float32Vector{Values: []float32{1, 3, 5, 2, 4, 6}}
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
			src, err := inmemory.New(schema, b)
			if err != nil {
				t.Fatal(err)
			}
			sess := NewSession(engine.Options{ExecutionPolicy: policy, MemoryLimitBytes: 2 << 20, Spill: engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}})
			defer sess.Close()
			q := sess.FromSource(src).GroupBy("k").AggConfigured(Median("v", "m"), Quantile("f", .25, aggregate.QuantileLinear, "q"), Count("n"), NUnique("v", "u"), Var("v", "var"), First("v", "first"), Last("v", "last"))
			plan, err := q.ExplainPhysical(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plan, "Aggregate") {
				t.Fatal(plan)
			}
			df, err := q.Filter(expr.GT(Col("m"), Lit(3))).Select("k", "m", "q", "n", "u", "var", "first", "last").Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer df.Close()
			var rows int
			for _, out := range df.batches() {
				rows += out.Rows
				for i := 0; i < out.Rows; i++ {
					if out.Int32s["k"].Values[i] != 2 || out.Float64s["m"].Values[i] != 4 || out.Float32s["q"].Values[i] != 3 || out.Uint64s["n"].Values[i] != 3 || out.Uint64s["u"].Values[i] != 3 || out.Float64s["var"].Values[i] != 4 || out.Int64s["first"].Values[i] != 2 || out.Int64s["last"].Values[i] != 6 {
						t.Fatal("mixed output mismatch", out)
					}
				}
			}
			if rows != 1 {
				t.Fatal(rows)
			}
			nested, err := q.AggConfigured(Median("m", "median_of_groups")).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer nested.Close()
			if nested.batches()[0].Float64s["median_of_groups"].Values[0] != 3.5 {
				t.Fatal("nested")
			}
			prepared, err := q.PrepareResult(context.Background(), engine.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			if _, err := prepared.Execute(context.Background(), engine.ExecuteOptions{Materialize: false}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConfiguredQuantileGlobalEmptyAndNull(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	for _, empty := range []bool{true, false} {
		var batches []*memory.Batch
		if !empty {
			b := memory.NewBatch()
			b.Rows = 3
			b.Float64s["v"] = &memory.Float64Vector{Values: []float64{1, 2, 3}, Validity: memory.Bitmap{0}}
			batches = append(batches, b)
		}
		src, err := inmemory.New(schema, batches...)
		if err != nil {
			t.Fatal(err)
		}
		sess := NewSession(engine.Options{ExecutionPolicy: engine.ExecutionMemoryOnly, MemoryLimitBytes: 1 << 20})
		q := sess.FromSource(src).AggConfigured(Median("v", "m"), Count("n"))
		df, err := q.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		b := df.batches()[0]
		want := uint64(3)
		if empty {
			want = 0
		}
		if b.Rows != 1 || b.Float64s["m"].IsValid(0) || b.Uint64s["n"].Values[0] != want {
			t.Fatal("empty/null result", b)
		}
		df.Close()
		sess.Close()
	}
}

func TestConfiguredLegacyOnlyPreservesBehavior(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded, Spill: engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}})
	defer sess.Close()
	df, err := sess.FromSource(src).AggConfigured(Sum("v", "sum"), Count("n")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	out := df.batches()[0]
	if out.Rows != 1 || out.Int64s["sum"].Values[0] != 6 || out.Uint64s["n"].Values[0] != 3 {
		t.Fatal("legacy configured output")
	}
}
