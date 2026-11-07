package engine_test

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestGlobalAggregateIdentitiesAndReuse(t *testing.T) {
	for _, tc := range []struct {
		name           string
		values         []int64
		valid          []int
		filter         expr.Expr
		sum            int64
		count, nonnull int64
		min, max       int64
		avg            float64
	}{
		{name: "empty"},
		{name: "all-null", values: []int64{9, 8}, count: 2},
		{name: "mixed", values: []int64{2, 100, 8}, valid: []int{0, 2}, sum: 10, count: 3, nonnull: 2, min: 2, max: 8, avg: 5},
		{name: "filtered-empty", values: []int64{2, 8}, valid: []int{0, 1}, filter: expr.Bool(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = len(tc.values)
			validity := make(memory.Bitmap, memory.WordsFor(len(tc.values)))
			for _, i := range tc.valid {
				memory.Set(validity, i)
			}
			b.Int64s["v"] = &memory.Int64Vector{Values: tc.values, Validity: validity}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
			if err != nil {
				t.Fatal(err)
			}
			for _, policy := range []engine.ExecutionPolicy{engine.ExecutionAuto, engine.ExecutionMemoryOnly, engine.ExecutionBounded} {
				e := engine.New(engine.Options{Workers: 4, MemoryLimitBytes: 1 << 20, ExecutionPolicy: policy})
				q, err := e.Prepare(context.Background(), src, expr.Query{Filter: tc.filter, Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("rows"), expr.CountColumn("v", "values"), expr.Min("v", "min"), expr.Max("v", "max"), expr.Avg("v", "avg")}}, engine.PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if q.Plan().GroupColumns != 0 || q.Plan().EstimatedCardinality != 1 || q.Plan().Strategy.Kind != planner.StrategySorted {
					t.Fatalf("plan=%+v", q.Plan())
				}
				for repeat := 0; repeat < 3; repeat++ {
					r, err := q.Execute(context.Background(), engine.ExecuteOptions{Materialize: true})
					if err != nil {
						t.Fatal(err)
					}
					if r.GroupCount != 1 || len(r.Rows) != 1 || len(r.Rows[0].Keys) != 0 || r.SelectedRows != int(tc.count) {
						t.Fatalf("result=%+v", r)
					}
					assertAggRow(t, r.Rows[0], tc.sum, tc.nonnull > 0, tc.count, tc.nonnull, tc.min, tc.nonnull > 0, tc.max, tc.nonnull > 0, tc.avg, tc.nonnull > 0)
				}
				summary, err := q.Execute(context.Background(), engine.ExecuteOptions{})
				if err != nil || summary.GroupCount != 1 || len(summary.Rows) != 0 {
					t.Fatalf("summary=%+v err=%v", summary, err)
				}
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				if err := e.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestGlobalCountOnlyAndForcedStrategy(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 5
	src, err := inmemory.New(memory.NewSchema(), b)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{})
	defer e.Close()
	r, err := e.Execute(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Count("n")}}, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0].Aggregates[0].Int64 != 5 {
		t.Fatalf("result=%+v", r)
	}
	for _, hint := range []string{"bitmap-direct", "partitioned-hash", "direct-fused"} {
		_, err = e.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Count("n")}, Hints: expr.Hints{ForceStrategy: hint}}, engine.PrepareOptions{})
		if err == nil || !strings.Contains(err.Error(), "global aggregation") {
			t.Fatalf("hint %s: %v", hint, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.Execute(ctx, src, expr.Query{Aggregates: []expr.Aggregate{expr.Count("n")}}, engine.ExecuteOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}

func TestGlobalAggregateBudgetRelease(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{64, 1 << 20} {
		e := engine.New(engine.Options{MemoryLimitBytes: limit})
		p, err := e.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum")}}, engine.PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		budget, err := e.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		_, err = p.Execute(memory.WithQueryBudget(context.Background(), budget), engine.ExecuteOptions{Materialize: true})
		if (err != nil) != (limit == 64) {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if budget.Used() != 0 {
			t.Fatalf("limit %d leaked %d bytes", limit, budget.Used())
		}
		budget.Close()
		p.Close()
		e.Close()
	}
}

func TestGlobalAggregateAcrossPartitions(t *testing.T) {
	batches := make([]*memory.Batch, 0, 3)
	for _, values := range [][]int64{{-7, -3}, {2, 8}, {-1, 4}} {
		b := memory.NewBatch()
		b.Rows = len(values)
		b.Int64s["v"] = &memory.Int64Vector{Values: values}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{Workers: 4, MemoryLimitBytes: 1 << 20, ExecutionPolicy: engine.ExecutionBounded})
	defer e.Close()
	result, err := e.Execute(context.Background(), src, expr.Query{Filter: expr.GT(expr.Col("v"), expr.I64(0)), Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("n")}}, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 1 || result.SelectedRows != 3 || result.Checksum != 14 || len(result.Rows) != 1 || result.Rows[0].Aggregates[0].Int64 != 14 || result.Rows[0].Aggregates[1].Int64 != 3 {
		t.Fatalf("result=%+v", result)
	}
}
