package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestExplainAnalyzeReportsJoinOperators(t *testing.T) {
	leftBatch := memory.NewBatch()
	leftBatch.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	leftBatch.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	leftBatch.Reset(3)
	left, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), leftBatch)

	rightBatch := memory.NewBatch()
	rightBatch.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 2, 3}}
	rightBatch.Int64s["bonus"] = &memory.Int64Vector{Values: []int64{5, 7, 11}}
	rightBatch.Reset(3)
	right, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "bonus", Type: memory.TypeInt64},
	), rightBatch)

	opts := engine.DefaultOptions()
	opts.Workers = 4
	s := NewSession(opts)
	defer s.Close()
	lf := s.FromSource(left).
		Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}}).
		WithColumns(Col("value").Add(Col("bonus")).Alias("total"))

	report, err := lf.Analyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.OutputRows != 3 {
		t.Fatalf("rows=%d want=3", report.OutputRows)
	}
	var prepareJoin, executeJoin bool
	for _, stat := range report.PrepareOperators {
		prepareJoin = prepareJoin || stat.Kind == "join"
	}
	for _, stat := range report.ExecuteOperators {
		executeJoin = executeJoin || stat.Kind == "join"
	}
	if !prepareJoin || !executeJoin {
		t.Fatalf("prepare=%+v execute=%+v", report.PrepareOperators, report.ExecuteOperators)
	}
	text := report.String()
	if !strings.Contains(text, "PREPARE OPERATORS") || !strings.Contains(text, "EXECUTE OPERATORS") || !strings.Contains(text, "join") {
		t.Fatalf("analyze output missing operator table:\n%s", text)
	}
}

func TestLazyJoinRadixHintsSurviveOptimization(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2}}
	batch.Reset(2)
	src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32}), batch)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf := s.FromSource(src).Join(s.FromSource(src), JoinOptions{Type: InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}, RadixBits: 3, Workers: 2})
	optimized, err := lf.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(optimized, "radix_bits=3") || !strings.Contains(optimized, "workers=2") {
		t.Fatalf("optimized explain lost join hints: %s", optimized)
	}
}

func TestLazyParallelRadixJoinExecutes(t *testing.T) {
	leftBatch := memory.NewBatch()
	leftBatch.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3, 4}}
	leftBatch.Int64s["v"] = &memory.Int64Vector{Values: []int64{10, 20, 30, 40}}
	leftBatch.Reset(4)
	left, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "v", Type: memory.TypeInt64},
	), leftBatch)
	rightBatch := memory.NewBatch()
	rightBatch.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 2, 4}}
	rightBatch.Int64s["bonus"] = &memory.Int64Vector{Values: []int64{5, 7, 11}}
	rightBatch.Reset(3)
	right, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "bonus", Type: memory.TypeInt64},
	), rightBatch)
	opts := engine.DefaultOptions()
	opts.Workers = 4
	s := NewSession(opts)
	defer s.Close()
	lf := s.FromSource(left).
		Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}, RadixBits: 2, Workers: 4}).
		SelectExprs(Col("id").Alias("id"), Col("bonus").Alias("bonus"))
	frame, err := lf.CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Rows != 3 {
		t.Fatalf("rows=%d want=3", frame.Rows)
	}
}
