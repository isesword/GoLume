package engine

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestGlobalAggregateFinalOutputLifecycle(t *testing.T) {
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	e := New(Options{MemoryLimitBytes: 1 << 20})
	defer e.Close()
	p, err := e.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Count("n")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, finish := range []string{"eof", "close", "cancel"} {
		t.Run(finish, func(t *testing.T) {
			budget, err := e.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			stream, err := p.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1000)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); err != nil || !ok || out.Rows != 1 {
				t.Fatalf("next %v %v rows=%d", ok, err, out.Rows)
			}
			if out.Uint64s["n"].Values[0] != 0 || out.Int64s["sum"].IsValid(0) {
				t.Fatal("incorrect empty output")
			}
			if budget.Used() == 0 {
				t.Fatal("borrowed output should remain charged")
			}
			if finish == "close" {
				err = stream.Close()
			} else {
				ctx := context.Background()
				if finish == "cancel" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var ok bool
				ok, err = stream.Next(ctx, memory.NewBatch())
				if ok {
					t.Fatal("global aggregate emitted more than one row")
				}
			}
			// EOF takes precedence after the final row: cancellation at EOF is harmless.
			if err != nil {
				t.Fatal(err)
			}
			if budget.Used() != 0 || out.Rows != 0 {
				t.Fatalf("retained output: bytes=%d rows=%d", budget.Used(), out.Rows)
			}
		})
	}
}

func TestGlobalAggregateStreamCancellationReleasesState(t *testing.T) {
	src := &generatedStreamSource{rows: 1024, groups: 3}
	e := New(streamTestOptions())
	defer e.Close()
	query := streamTestQuery()
	query.GroupBy = nil
	query.Hints.ForceStrategy = ""
	p, err := e.Prepare(context.Background(), src, query, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled next=%v err=%v", ok, err)
	}
	if budget.Used() != 0 {
		t.Fatalf("cancelled stream leaked %d bytes", budget.Used())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}
