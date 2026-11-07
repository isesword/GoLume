package lazy

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestReviewConfiguredAliasesAndDownstreamGrouping(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 1, 1, 2, 2}}
	b.Float32s["v"] = &memory.Float32Vector{Values: []float32{1, 3, 5, 7, 99, 99}, Validity: memory.Bitmap{15}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeFloat32, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{MemoryLimitBytes: 2 << 20})
	defer sess.Close()
	q := sess.FromSource(src).GroupBy("k").AggConfigured(Median("v", ""), Quantile("v", 0, aggregate.QuantileNearest, ""), Count(""))
	schema, err := q.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if schema.Fields[1].Name != "agg_0" || schema.Fields[2].Name != "agg_1" || schema.Fields[3].Name != "agg_2" {
		t.Fatal(schema)
	}
	out, err := q.Filter(expr.GT(Col("agg_0"), Lit(3))).Select("agg_0").AggConfigured(Median("agg_0", "m")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if len(out.batches()) != 1 || out.batches()[0].Rows != 1 || out.batches()[0].Float32s["m"].Values[0] != 6 {
		t.Fatal(out.batches())
	}
}

func TestReviewConfiguredBoundedWithoutSpillDiagnostics(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	sess := NewSession(engine.Options{ExecutionPolicy: engine.ExecutionBounded, MemoryLimitBytes: 1 << 20})
	defer sess.Close()
	plan, err := sess.FromSource(src).AggConfigured(Median("v", "m")).ExplainPhysical(context.Background())
	if err != nil {
		if !errors.Is(err, engine.ErrBoundedSpillRequired) {
			t.Fatalf("wrong bounded error: %v", err)
		}
		return
	}
	t.Log(plan)
	if strings.Contains(strings.ToLower(plan), "spillable") {
		t.Fatalf("claims spillable without configured store: %s", plan)
	}
}
