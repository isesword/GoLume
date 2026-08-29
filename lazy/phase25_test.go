package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestLazyWithColumnsComputedFilterAggregate(t *testing.T) {
	b := memory.NewBatch()
	b.Int32s["k"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2}}
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 4, 5, 1}}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{2, 3, 1, 1}}
	b.Reset(4)
	src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}), b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf := s.FromSource(src).WithColumns(Col("a").Mul(Lit(2)).Add(Col("b")).Alias("score")).Filter(Col("score").Gt(Lit(8))).GroupBy("k").Agg(Sum("score", "sum"), Count("count"))
	res, err := lf.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.GroupCount != 2 || res.SelectedRows != 2 || res.Checksum != 22 {
		t.Fatalf("result=%+v", res)
	}
}

func TestLazySelectExprsCollectFrame(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	b.Reset(3)
	src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}), b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	frame, err := s.FromSource(src).SelectExprs(Col("a").Alias("a"), Col("a").Add(Col("b")).Alias("total")).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Height() != 3 || len(frame.Schema().Fields) != 2 {
		t.Fatalf("frame rows=%d schema=%v", frame.Height(), frame.Schema().Fields)
	}
	v, _ := frame.batches()[0].Int64("total")
	if v.Values[0] != 11 || v.Values[2] != 33 {
		t.Fatalf("total=%v", v.Values)
	}
}

func TestLazyJoinThenFilterProjectionAggregate(t *testing.T) {
	lb := memory.NewBatch()
	lb.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	lb.Int64s["amount"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	lb.Reset(3)
	left, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32}, memory.Field{Name: "amount", Type: memory.TypeInt64}), lb)
	rb := memory.NewBatch()
	rb.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 2, 3}}
	rb.Int64s["bonus"] = &memory.Int64Vector{Values: []int64{5, 7, 11}}
	rb.Reset(3)
	right, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32}, memory.Field{Name: "bonus", Type: memory.TypeInt64}), rb)
	opts := engine.DefaultOptions()
	opts.Workers = 2
	s := NewSession(opts)
	defer s.Close()
	lf := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"id"}, RightOn: []string{"id"}}).
		WithColumns(Col("amount").Add(Col("bonus")).Alias("total")).Filter(Col("total").Gt(Lit(25))).GroupBy("id").Agg(Sum("total", "sum"), Count("count"))
	res, err := lf.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.GroupCount != 2 || res.SelectedRows != 2 || res.Checksum != 68 {
		t.Fatalf("result=%+v", res)
	}
}

func TestLazyJoinCollectFrame(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3})
	right := makeI32Source(t, []int32{2, 3})
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	frame, err := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).SelectExprs(Col("key").Alias("key")).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Height() != 2 {
		t.Fatalf("rows=%d", frame.Height())
	}
}

func TestPreparedFrameReusesJoinBuildAndReleasesMemory(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3, 4})
	right := makeI32Source(t, []int32{2, 3, 4})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()

	lf := s.FromSource(left).
		Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).
		SelectExprs(Col("key").Alias("key"))

	prepared, err := lf.PrepareFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if used := s.engine.Memory().Used(); used <= 0 {
		t.Fatalf("prepared join should retain accounted build memory, used=%d", used)
	}

	for i := 0; i < 2; i++ {
		frame, err := prepared.Execute(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if frame.Height() != 3 {
			t.Fatalf("run %d rows=%d want=3", i, frame.Height())
		}
		if err := frame.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if used := s.engine.Memory().Used(); used != 0 {
		t.Fatalf("prepared frame close leaked memory: %d", used)
	}
	if _, err := prepared.Execute(context.Background()); err != ErrPreparedFrameClosed {
		t.Fatalf("execute-after-close err=%v want=%v", err, ErrPreparedFrameClosed)
	}
}

func TestPreparedFrameConcurrentExecute(t *testing.T) {
	left := makeI32Source(t, []int32{1, 2, 3, 4})
	right := makeI32Source(t, []int32{2, 3, 4})
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	prepared, err := s.FromSource(left).
		Join(s.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).
		SelectExprs(Col("key").Alias("key")).
		PrepareFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	errCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			frame, err := prepared.Execute(context.Background())
			if err == nil && frame.Height() != 3 {
				err = fmt.Errorf("rows=%d want=3", frame.Height())
			}
			errCh <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestExplainPhysicalRowPipeline(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	b.Reset(3)
	src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64}), b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	text, err := s.FromSource(src).WithColumns(Col("a").Add(Col("b")).Alias("total")).SelectExprs(Col("total").Alias("total")).ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "PHYSICAL row-pipeline") || !strings.Contains(text, "columns=[total]") {
		t.Fatalf("explain=%s", text)
	}
}

func TestPhysicalScanProjectionFallbackForGenericSource(t *testing.T) {
	b := memory.NewBatch()
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 2}}
	b.Int64s["unused"] = &memory.Int64Vector{Values: []int64{9, 9}}
	b.Reset(2)
	src, _ := inmemory.New(memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "unused", Type: memory.TypeInt64},
	), b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	prepared, err := s.FromSource(src).Select("a").PrepareFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if _, ok := prepared.Schema().Field("unused"); ok {
		t.Fatalf("generic source physical projection retained unused column: %v", prepared.Schema().Fields)
	}
	frame, err := prepared.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.batches()) != 1 || len(frame.batches()[0].Int64s) != 1 || frame.batches()[0].Int64s["a"] == nil {
		t.Fatalf("unexpected projected frame: %+v", frame)
	}
}

func TestLazyTypedExpressionsFloatStringDateTimestamp(t *testing.T) {
	batch := memory.NewBatch()
	batch.Float64s["f"] = &memory.Float64Vector{Values: []float64{1.5, 2.5, 3.5}}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc"), Encoding: memory.EncodingView}
	batch.Int32s["d"] = &memory.Int32Vector{Values: []int32{10, 20, 30}}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{100, 200, 300}}
	batch.Reset(3)
	schema := memory.NewSchema(
		memory.Field{Name: "f", Type: memory.TypeFloat64},
		memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "d", Type: memory.TypeDate32},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	src, _ := inmemory.New(schema, batch)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()

	frame, err := s.FromSource(src).
		WithColumns(
			Col("f").Add(F64(0.5)).Alias("f2"),
			Concat(Col("s"), Str("!")).Alias("label"),
			Cast(Col("d"), memory.TypeString).Alias("day_text"),
		).
		Filter(Col("f2").Gt(F64(2.0))).
		SelectExprs(Col("f2").Alias("f2"), Col("label").Alias("label"), Col("day_text").Alias("day_text"), Col("ts").Alias("ts")).
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Height() != 2 {
		t.Fatalf("rows=%d", frame.Height())
	}
	fv, _ := frame.batches()[0].Float64("f2")
	if fv.Values[0] != 3 || fv.Values[1] != 4 {
		t.Fatalf("f2=%v", fv.Values)
	}
	label, _ := frame.batches()[0].String("label")
	if string(label.ValueBytes(0)) != "b!" || string(label.ValueBytes(1)) != "c!" {
		t.Fatalf("labels=%q,%q", label.ValueBytes(0), label.ValueBytes(1))
	}
	day, _ := frame.batches()[0].String("day_text")
	if string(day.ValueBytes(0)) != "20" || string(day.ValueBytes(1)) != "30" {
		t.Fatalf("days=%q,%q", day.ValueBytes(0), day.ValueBytes(1))
	}
	ts, _ := frame.batches()[0].Int64("ts")
	if ts.Values[0] != 200 || ts.Values[1] != 300 {
		t.Fatalf("ts=%v", ts.Values)
	}
}

func TestExplainAnalyzeAggregateAndFrame(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int32s["k"] = &memory.Int32Vector{Values: []int32{1, 1, 2}}
	batch.Int64s["v"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	batch.Reset(3)
	src, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64}), batch)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()

	agg := s.FromSource(src).GroupBy("k").Agg(Sum("v", "sum"), Count("rows"))
	report, err := agg.Analyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.GroupCount != 2 || report.SelectedRows != 3 || report.OutputRows != 2 || report.ExecuteDuration <= 0 {
		t.Fatalf("aggregate analyze=%+v", report)
	}
	if len(report.ExecuteOperators) < 2 {
		t.Fatalf("aggregate operator stats=%+v", report.ExecuteOperators)
	}
	if text := report.String(); !strings.Contains(text, "ANALYZE") || !strings.Contains(text, "strategy=") || !strings.Contains(text, "EXECUTE OPERATORS") || !strings.Contains(text, "aggregate") {
		t.Fatalf("analyze text=%s", text)
	}

	row := s.FromSource(src).SelectExprs(Col("v").Mul(Lit(2)).Alias("twice"))
	rowReport, err := row.Analyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rowReport.OutputRows != 3 || rowReport.MaterializedRows != 3 {
		t.Fatalf("row analyze=%+v", rowReport)
	}
	if len(rowReport.ExecuteOperators) < 2 {
		t.Fatalf("row operator stats=%+v", rowReport.ExecuteOperators)
	}
	if !strings.Contains(rowReport.String(), "projection") {
		t.Fatalf("row analyze missing projection: %s", rowReport.String())
	}
}
