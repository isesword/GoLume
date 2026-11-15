package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestCollectV4RetainedMetadataAdmissionAndFailureCleanup(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batches := make([]*memory.Batch, 128)
	for i := range batches {
		b := memory.NewBatch()
		b.Rows = 1
		b.Int64s["x"] = &memory.Int64Vector{Values: []int64{int64(i)}}
		batches[i] = b
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	one, err := memory.EstimateCollectBatchBytes(batches[0])
	if err != nil {
		t.Fatal(err)
	}
	one += collectEntryAllowance
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = one*129 + 1<<20
	session := NewSession(opts)
	defer session.Close()
	root := memory.NewEngineBudget(opts.MemoryLimitBytes)
	budget, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: one * 127})
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	_, err = session.FromSource(src).Collect(memory.WithQueryBudget(context.Background(), budget))
	if !errors.Is(err, memory.ErrBudgetExceeded) || budget.Used() != 0 {
		t.Fatalf("err=%v used=%d", err, budget.Used())
	}
	if budget.Snapshot().Closed {
		t.Fatal("closed borrowed budget")
	}
	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	copyDF := df
	if used := session.Engine().Memory().Used(); used != one*128 {
		t.Fatalf("used=%d want=%d", used, one*128)
	}
	batches[0].Int64s["x"].Values[0] = 999
	if got := frameInt64Column(t, df, "x"); len(got) != 128 || got[0] != 0 || got[127] != 127 {
		t.Fatal("ownership/order")
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if err := copyDF.Close(); err != nil {
		t.Fatal(err)
	}
	if session.Engine().Memory().Used() != 0 {
		t.Fatal("leaked output")
	}
}

func TestCollectV4RejectsLogicalPayloadOnlyBudget(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1024
	s := NewSession(opts)
	defer s.Close()
	_, err = s.FromSource(src).Collect(context.Background())
	var resource *memory.ResourceError
	if !errors.As(err, &resource) || resource.Operator != "Collect" || resource.RequestedBytes <= memory.BatchBytes(b) || s.Engine().Memory().Used() != 0 {
		t.Fatalf("err=%v", err)
	}
}
