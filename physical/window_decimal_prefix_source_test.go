package physical

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestN27DecimalPrefixNativeChild(t *testing.T) {
	ctx := context.Background()
	schema := n27Source(t, 2, 1).Schema()
	input, err := inmemory.New(schema, n27Page(2, 1, 1, nil, 5, 15), n27Page(2, 1, 3, nil, 25))
	if err != nil {
		t.Fatal(err)
	}
	input = input.WithOrdering(source.OrderingColumn{Name: "k"})
	op, err := NewDecimalPrefixAverageSource(input, n27Spec(3, 2, aggregate.DecimalAverageHalfEven))
	if err != nil {
		t.Fatal(err)
	}
	st, err := op.Stats(ctx)
	if err != nil || st.Rows != 3 || st.Partitions != 1 {
		t.Fatal(st, err)
	}
	ps, err := op.Partitions(ctx)
	if err != nil || len(ps) != 1 || ps[0].ID != 0 || ps[0].Rows != 3 {
		t.Fatal(ps, err)
	}
	if _, ok := op.(source.PreparedBatchStreamProvider); ok {
		t.Fatal("prefix exposed independent partitions")
	}
	budget := memory.NewBudgetManager(4096)
	opts := n27Opts(budget)
	opts.BatchRows = 1
	opts.BatchPayloadBytes = 48
	stream, err := source.OpenBatchStream(ctx, op, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	dst := memory.NewBatch()
	for i, expected := range []int64{50, 100, 150} {
		ok, e := stream.Next(ctx, dst)
		if !ok || e != nil || dst.Rows != 1 || !dst.Decimals["m"].IsValid(0) || dst.Decimals["m"].Values[0] != decimal128.FromI64(expected) {
			t.Fatal(i, ok, e, dst.Rows, dst.Decimals["m"])
		}
	}
	if ok, e := stream.Next(ctx, dst); ok || e != nil || budget.Used() != 0 {
		t.Fatal(ok, e, budget.Used())
	}
	opts.BatchPayloadBytes = 47
	failing, err := source.OpenBatchStream(ctx, op, opts)
	if err != nil {
		t.Fatal(err)
	}
	if ok, e := failing.Next(ctx, dst); ok || !errors.Is(e, memory.ErrBudgetExceeded) || budget.Used() != 0 || dst.Rows != 0 {
		t.Fatal(ok, e, budget.Used(), dst.Rows)
	}
	if err := failing.Close(); err != nil {
		t.Fatal(err)
	}
	opts.BatchPayloadTargetBytes = 1
	if bad, e := source.OpenBatchStream(ctx, op, opts); e == nil || bad != nil || budget.Used() != 0 {
		t.Fatal(bad, e, budget.Used())
	}
}
