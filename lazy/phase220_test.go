package lazy

import (
	"context"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestUnifiedCollectReturnsRowsAndTerminalAggregateDataFrames(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "category", Type: memory.TypeInt32}, memory.Field{Name: "amount", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int32s["category"] = &memory.Int32Vector{Values: []int32{1, 1, 2}}
	batch.Int64s["amount"] = &memory.Int64Vector{Values: []int64{10, 20, 7}}
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	rows, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rows.Height() != 3 {
		t.Fatalf("row height=%d", rows.Height())
	}
	_ = rows.Close()
	baseAggregate := session.FromSource(src).GroupBy("category").Agg(Sum("amount", "total"), Count("count"))
	aggregated := baseAggregate.Sort(Desc("total"))
	df, err := aggregated.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if df.Height() != 2 || df.Width() != 3 {
		t.Fatalf("aggregate shape=(%d,%d)", df.Height(), df.Width())
	}
	categories, _ := df.Column("category")
	categoryChunks, err := categories.Int32Chunks()
	if err != nil {
		t.Fatal(err)
	}
	category, valid, err := categoryChunks[0].Value(0)
	if err != nil || !valid || category != 1 {
		t.Fatalf("first category=(%d,%v,%v)", category, valid, err)
	}
	totals, _ := df.Column("total")
	values, validity, err := totals.CopyInt64()
	if err != nil || validity != nil || len(values) != 2 || values[0] != 30 || values[1] != 7 {
		t.Fatalf("totals=%v validity=%v err=%v", values, validity, err)
	}
	legacy, err := baseAggregate.CollectResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if legacy.GroupCount != 2 || legacy.Checksum != 37 {
		t.Fatalf("legacy=%+v", legacy)
	}
}

func TestUnifiedPrepareRepeatsAggregateDataFrameExecution(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2}}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{2, 3, 4}}
	batch.Reset(3)
	src, _ := inmemory.New(schema, batch)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	query := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"))
	prepared, err := query.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for run := 0; run < 2; run++ {
		df, err := prepared.Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if df.Height() != 2 {
			t.Fatalf("run %d height=%d", run, df.Height())
		}
		if err := df.Close(); err != nil {
			t.Fatal(err)
		}
	}
	resultPrepared, err := query.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer resultPrepared.Close()
	result, err := resultPrepared.Execute(context.Background(), engine.ExecuteOptions{Materialize: true})
	if err != nil || result.GroupCount != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestUnifiedPrepareResultPreservesPrepareOptions(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 3}}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{2, 3, 4, 5}}
	batch.Reset(4)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	const feedbackKey = "phase220-compat-options"
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum")).PrepareResult(context.Background(), engine.PrepareOptions{MaxConcurrency: 2, FeedbackKey: feedbackKey})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.frame == nil || prepared.frame.planInfo.WorkspacePool != 2 {
		t.Fatalf("workspace pool=%d, want 2", prepared.frame.planInfo.WorkspacePool)
	}
	if _, err := prepared.Execute(context.Background(), engine.ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if feedback, ok := session.Engine().RuntimeFeedback(feedbackKey); !ok || feedback.Samples != 1 {
		t.Fatalf("custom feedback=%+v ok=%v", feedback, ok)
	}
}
