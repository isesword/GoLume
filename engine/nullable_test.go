package engine_test

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func nullableI32Source(t *testing.T) *inmemory.Source {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 6
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1, 1}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 20, 30, 40, 50, 60}}
	valid := make(memory.Bitmap, memory.WordsFor(6))
	for _, i := range []int{0, 2, 3, 5} {
		memory.Set(valid, i)
	}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{0, 0, 1, 1, 0, 0}, Validity: valid}
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func nullableQuery(strategy string, workers int) expr.Query {
	return expr.Query{
		Filter:     expr.GT(expr.Col("pred"), expr.I64(0)),
		GroupBy:    []string{"key"},
		Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")},
		Hints:      expr.Hints{Cardinality: 2, Selectivity: 1, ForceStrategy: strategy, ForceWorkerCount: workers},
	}
}

func assertNullableResult(t *testing.T, r engine.Result) {
	t.Helper()
	if r.GroupCount != 3 || r.SelectedRows != 6 || r.Checksum != 210 {
		t.Fatalf("result=%+v", r)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("materialized rows=%d want=3", len(r.Rows))
	}
	seen := map[string][2]int64{}
	for _, row := range r.Rows {
		k := "NULL"
		if row.Keys[0].Valid {
			k = string(rune('0' + row.Keys[0].Int64))
		}
		seen[k] = [2]int64{row.Sum, int64(row.Count)}
	}
	if seen["0"] != [2]int64{70, 2} || seen["1"] != [2]int64{70, 2} || seen["NULL"] != [2]int64{70, 2} {
		t.Fatalf("groups=%v", seen)
	}
}

func TestNullableSingleKeyAcrossStrategies(t *testing.T) {
	for _, tc := range []struct {
		strategy string
		workers  int
	}{{"direct-fused", 1}, {"bitmap-direct", 1}, {"radix", 2}} {
		t.Run(tc.strategy, func(t *testing.T) {
			e := engine.New(engine.Options{Workers: 2})
			defer e.Close()
			r, err := e.Execute(context.Background(), nullableI32Source(t), nullableQuery(tc.strategy, tc.workers), engine.ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			assertNullableResult(t, r)
		})
	}
}

func TestAdaptiveGrowthWithUnderestimatedCardinality(t *testing.T) {
	const n = 4096
	b := memory.NewBatch()
	b.Rows = n
	pred := make([]int64, n)
	key := make([]int32, n)
	value := make([]int64, n)
	for i := 0; i < n; i++ {
		pred[i], key[i], value[i] = 1, int32(i), 1
	}
	b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
	b.Int32s["key"] = &memory.Int32Vector{Values: key}
	b.Int64s["value"] = &memory.Int64Vector{Values: value}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), b)
	if err != nil {
		t.Fatal(err)
	}
	q := nullableQuery("direct-fused", 1)
	q.Hints.Cardinality = 8 // deliberately wrong by 512x
	e := engine.New(engine.Options{Workers: 1})
	defer e.Close()
	r, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != n || r.SelectedRows != n || r.Checksum != n {
		t.Fatalf("result=%+v", r)
	}
}

func TestAdaptiveGrowthRespectsMemoryBudget(t *testing.T) {
	const n = 2048
	b := memory.NewBatch()
	b.Rows = n
	pred := make([]int64, n)
	key := make([]int32, n)
	value := make([]int64, n)
	for i := 0; i < n; i++ {
		pred[i], key[i], value[i] = 1, int32(i), 1
	}
	b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
	b.Int32s["key"] = &memory.Int32Vector{Values: key}
	b.Int64s["value"] = &memory.Int64Vector{Values: value}
	src, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), b)
	q := nullableQuery("direct-fused", 1)
	q.Hints.Cardinality = 8
	// Enough for the initial table/predicate workspace, intentionally too small
	// for repeated table growth to 2K distinct keys.
	e := engine.New(engine.Options{Workers: 1, MemoryLimitBytes: 64 << 10})
	defer e.Close()
	_, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{})
	if err == nil || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("err=%v want memory.ErrBudgetExceeded", err)
	}
}

func TestRadixFallsBackWhenCardinalityHintIsWrong(t *testing.T) {
	const n = 4096
	b := memory.NewBatch()
	b.Rows = n
	pred := make([]int64, n)
	key := make([]int32, n)
	value := make([]int64, n)
	for i := 0; i < n; i++ {
		pred[i], key[i], value[i] = 1, int32(i), 1
	}
	b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
	b.Int32s["key"] = &memory.Int32Vector{Values: key}
	b.Int64s["value"] = &memory.Int64Vector{Values: value}
	src, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), b)
	q := nullableQuery("radix", 2)
	q.Hints.Cardinality = 8
	q.Hints.ForceRadixBits = 1
	e := engine.New(engine.Options{Workers: 2})
	defer e.Close()
	r, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != n || r.SelectedRows != n || r.Checksum != n || len(r.Rows) != n {
		t.Fatalf("result groups=%d selected=%d checksum=%d rows=%d", r.GroupCount, r.SelectedRows, r.Checksum, len(r.Rows))
	}
}

func TestNullableCompositeKeyRadix(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1, 1}}
	av := make(memory.Bitmap, memory.WordsFor(6))
	bv := make(memory.Bitmap, memory.WordsFor(6))
	for _, i := range []int{0, 1, 2, 4} {
		memory.Set(av, i)
	}
	for _, i := range []int{0, 2, 3, 4} {
		memory.Set(bv, i)
	}
	b.Int32s["a"] = &memory.Int32Vector{Values: []int32{1, 1, 1, 9, 2, 8}, Validity: av}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{10, 999, 20, 10, 10, 777}, Validity: bv}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5, 6}}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "a", Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), b)
	if err != nil {
		t.Fatal(err)
	}
	q := expr.Query{
		Filter:     expr.GT(expr.Col("pred"), expr.I64(0)),
		GroupBy:    []string{"a", "b"},
		Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")},
		Hints:      expr.Hints{Cardinality: 8, Selectivity: 1, ForceStrategy: "radix", ForceWorkerCount: 2, ForceRadixBits: 2},
	}
	e := engine.New(engine.Options{Workers: 2})
	defer e.Close()
	r, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != 6 || r.SelectedRows != 6 || r.Checksum != 21 {
		t.Fatalf("result=%+v", r)
	}
	for _, row := range r.Rows {
		if len(row.Keys) != 2 {
			t.Fatalf("row keys=%v", row.Keys)
		}
	}
}
