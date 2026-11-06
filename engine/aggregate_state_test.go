package engine_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestAggregateStateFunctionsAndValueNulls(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 8
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1, 1, 1, 1}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 1, 2, 2, 2, 3, 3}}
	valid := make(memory.Bitmap, memory.WordsFor(8))
	for _, i := range []int{0, 2, 3, 5} {
		memory.Set(valid, i)
	}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 999, 30, 4, 999, 8, 999, 999}, Validity: valid}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true},
	), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, strategy := range []string{"direct-fused", "bitmap-direct", "partitioned-hash"} {
		t.Run(strategy, func(t *testing.T) {
			q := expr.Query{
				Filter:  expr.GT(expr.Col("pred"), expr.I64(0)),
				GroupBy: []string{"key"},
				Aggregates: []expr.Aggregate{
					expr.Sum("value", "sum"), expr.Count("rows"), expr.CountColumn("value", "values"),
					expr.Min("value", "min"), expr.Max("value", "max"), expr.Avg("value", "avg"),
				},
				Hints: expr.Hints{Cardinality: 1, Selectivity: 1, ForceStrategy: strategy},
			}
			e := engine.New(engine.Options{Workers: 1})
			defer e.Close()
			r, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if r.GroupCount != 3 || r.SelectedRows != 8 || r.Checksum != 52 {
				t.Fatalf("summary=%+v", r)
			}
			rows := map[int64]engine.AggregateRow{}
			for _, row := range r.Rows {
				rows[row.Keys[0].Int64] = row
			}
			assertAggRow(t, rows[1], 40, true, 3, 2, 10, true, 30, true, 20, true)
			assertAggRow(t, rows[2], 12, true, 3, 2, 4, true, 8, true, 6, true)
			assertAggRow(t, rows[3], 0, false, 2, 0, 0, false, 0, false, 0, false)
		})
	}
}

func assertAggRow(t *testing.T, row engine.AggregateRow, sum int64, sumValid bool, countRows, countValues int64, min int64, minValid bool, max int64, maxValid bool, avg float64, avgValid bool) {
	t.Helper()
	got := map[string]engine.AggregateValue{}
	for _, v := range row.Aggregates {
		got[v.Alias] = v
	}
	if v := got["sum"]; v.Valid != sumValid || v.Int64 != sum {
		t.Fatalf("sum=%+v", v)
	}
	if v := got["rows"]; !v.Valid || v.Int64 != countRows {
		t.Fatalf("rows=%+v", v)
	}
	if v := got["values"]; !v.Valid || v.Int64 != countValues {
		t.Fatalf("values=%+v", v)
	}
	if v := got["min"]; v.Valid != minValid || v.Int64 != min {
		t.Fatalf("min=%+v", v)
	}
	if v := got["max"]; v.Valid != maxValid || v.Int64 != max {
		t.Fatalf("max=%+v", v)
	}
	if v := got["avg"]; v.Valid != avgValid || v.Float64 != avg || !v.IsFloat {
		t.Fatalf("avg=%+v", v)
	}
}

func TestRemovedRadixHintRejected(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 2}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 20}}
	src, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), b)
	q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(0)), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Min("value", "min")}, Hints: expr.Hints{ForceStrategy: "radix"}}
	e := engine.New(engine.Options{Workers: 2})
	defer e.Close()
	if _, err := e.Prepare(context.Background(), src, q, engine.PrepareOptions{}); err == nil {
		t.Fatal("expected removed radix hint to be rejected")
	}
}

func TestCountRowsOnlyDoesNotRequireValueColumn(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 6
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1, 1}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
	), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, strategy := range []string{"direct-fused", "bitmap-direct", "partitioned-hash"} {
		t.Run(strategy, func(t *testing.T) {
			q := expr.Query{
				Filter:     expr.GT(expr.Col("pred"), expr.I64(0)),
				GroupBy:    []string{"key"},
				Aggregates: []expr.Aggregate{expr.Count("rows")},
				Hints:      expr.Hints{Cardinality: 1, Selectivity: 1, ForceStrategy: strategy},
			}
			e := engine.New(engine.Options{Workers: 1})
			defer e.Close()
			r, err := e.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if r.GroupCount != 3 || r.SelectedRows != 6 || r.Checksum != 0 {
				t.Fatalf("summary=%+v", r)
			}
			want := map[int64]int64{1: 2, 2: 3, 3: 1}
			for _, row := range r.Rows {
				if len(row.Aggregates) != 1 || row.Aggregates[0].Alias != "rows" || row.Aggregates[0].Int64 != want[row.Keys[0].Int64] {
					t.Fatalf("row=%+v", row)
				}
			}
		})
	}
}
