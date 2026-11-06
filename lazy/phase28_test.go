package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestLazySortLimitAndTopKAgree(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
	)
	b1 := memory.NewBatch()
	b1.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b1.Int64s["score"] = &memory.Int64Vector{Values: []int64{30, 10, 50}}
	b1.Reset(3)
	b2 := memory.NewBatch()
	b2.Int64s["id"] = &memory.Int64Vector{Values: []int64{4, 5, 6}}
	b2.Int64s["score"] = &memory.Int64Vector{Values: []int64{20, 60, 40}}
	b2.Reset(3)
	src, err := inmemory.New(schema, b1, b2)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()

	sorted, err := s.FromSource(src).Sort(Desc("score")).Limit(3).Select("id", "score").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	top, err := s.FromSource(src).TopK(3, Desc("score")).Select("id", "score").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer top.Close()
	want := []int64{5, 3, 6}
	if got := frameInt64Column(t, sorted, "id"); !equalI64(got, want) {
		t.Fatalf("sort+limit ids=%v want=%v", got, want)
	}
	if got := frameInt64Column(t, top, "id"); !equalI64(got, want) {
		t.Fatalf("topk ids=%v want=%v", got, want)
	}
}

func TestLimitRunsAfterResidualFilter(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	b1 := memory.NewBatch()
	b1.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 9, 2}}
	b1.Reset(3)
	b2 := memory.NewBatch()
	b2.Int64s["x"] = &memory.Int64Vector{Values: []int64{8, 7, 3}}
	b2.Reset(3)
	src, _ := inmemory.New(schema, b1, b2)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	frame, err := s.FromSource(src).Filter(Col("x").Gt(Lit(5))).Limit(2).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	got := frameInt64Column(t, frame, "x")
	if !equalI64(got, []int64{9, 8}) {
		t.Fatalf("filtered limit=%v", got)
	}
}

func TestLazyTopKReplaysOnlyOrderingAndFilterColumnsOnFirstPass(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeInt64},
	)
	b1 := memory.NewBatch()
	b1.Int64s["pred"] = &memory.Int64Vector{Values: []int64{0, 10, 11}}
	b1.Int64s["score"] = &memory.Int64Vector{Values: []int64{100, 20, 40}}
	b1.Int64s["payload"] = &memory.Int64Vector{Values: []int64{1000, 2000, 3000}}
	b1.Reset(3)
	b2 := memory.NewBatch()
	b2.Int64s["pred"] = &memory.Int64Vector{Values: []int64{12, 3, 13}}
	b2.Int64s["score"] = &memory.Int64Vector{Values: []int64{30, 90, 50}}
	b2.Int64s["payload"] = &memory.Int64Vector{Values: []int64{4000, 5000, 6000}}
	b2.Reset(3)
	base, err := inmemory.New(schema, b1, b2)
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]source.PushdownRequest, 0, 2)
	src := &topKPushdownRecorder{input: base, requests: &requests}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()

	frame, err := s.FromSource(src).
		Filter(Col("pred").Gt(Lit(9))).
		TopK(2, Desc("score")).
		Select("payload").
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "payload"); !equalI64(got, []int64{6000, 3000}) {
		t.Fatalf("payload=%v want=[6000 3000]", got)
	}
	if len(requests) != 2 {
		t.Fatalf("pushdown requests=%d want 2", len(requests))
	}
	if got, want := requests[0].Projection, []string{"pred", "score", "payload"}; !equalStrings(got, want) {
		t.Fatalf("payload projection=%v want=%v", got, want)
	}
	if got, want := requests[1].Projection, []string{"pred", "score"}; !equalStrings(got, want) {
		t.Fatalf("top-k first-pass projection=%v want=%v", got, want)
	}
}

// topKPushdownRecorder keeps the production source behavior while exposing
// projection requests so the lazy Top-K first-pass contract can be tested.
type topKPushdownRecorder struct {
	input    source.Source
	requests *[]source.PushdownRequest
}

func (s *topKPushdownRecorder) Schema() memory.Schema { return s.input.Schema() }

func (s *topKPushdownRecorder) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

func (s *topKPushdownRecorder) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

func (s *topKPushdownRecorder) Replayability() source.Replayability {
	return source.ReplayabilityOf(s.input)
}

func (s *topKPushdownRecorder) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return source.OpenBatchStream(ctx, s.input, opts)
}

func (s *topKPushdownRecorder) ApplyPushdown(ctx context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	cloned := source.PushdownRequest{Projection: append([]string(nil), req.Projection...), Predicate: req.Predicate}
	*s.requests = append(*s.requests, cloned)
	pushdown, ok := s.input.(source.PushdownSource)
	if !ok {
		return nil, source.PushdownReport{}, fmt.Errorf("recorded source does not support pushdown")
	}
	next, report, err := pushdown.ApplyPushdown(ctx, req)
	if err != nil {
		return nil, source.PushdownReport{}, err
	}
	return &topKPushdownRecorder{input: next, requests: s.requests}, report, nil
}

func TestOpaqueArrowSurvivesFilterTopKAndMaterialization(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	xb := array.NewInt64Builder(alloc)
	defer xb.Release()
	xb.AppendValues([]int64{1, 4, 3, 2}, nil)
	xa := xb.NewArray()
	defer xa.Release()
	lb := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.Int32Builder)
	for _, vals := range [][]int32{{10}, {40, 41}, {30, 31, 32}, {20}} {
		lb.Append(true)
		vb.AppendValues(vals, nil)
	}
	la := lb.NewListArray()
	defer la.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}, {Name: "nested", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{xa, la}, 4)
	defer rec.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf, err := s.ScanArrow(rec)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := lf.Filter(Col("x").Gt(Lit(1))).TopK(2, Desc("x")).Select("x", "nested").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "x"); !equalI64(got, []int64{4, 3}) {
		t.Fatalf("x=%v", got)
	}
	if len(frame.batches()) != 1 {
		t.Fatalf("batches=%d", len(frame.batches()))
	}
	o := frame.batches()[0].Opaque["nested"]
	if o == nil || !o.Owned {
		t.Fatalf("nested opaque ownership=%+v", o)
	}
	arr, ok := o.Value.(*array.List)
	if !ok {
		t.Fatalf("nested type=%T", o.Value)
	}
	if arr.Len() != 2 {
		t.Fatalf("nested len=%d", arr.Len())
	}
	vals := arr.ListValues().(*array.Int32).Values()
	off := arr.Offsets()
	first := vals[off[0]:off[1]]
	second := vals[off[1]:off[2]]
	if len(first) != 2 || first[0] != 40 || first[1] != 41 {
		t.Fatalf("first=%v", first)
	}
	if len(second) != 3 || second[0] != 30 || second[2] != 32 {
		t.Fatalf("second=%v", second)
	}
}

func frameInt64Column(t *testing.T, f DataFrame, name string) []int64 {
	t.Helper()
	var out []int64
	for _, b := range f.batches() {
		v, err := b.Int64(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v.Values...)
	}
	return out
}
func equalI64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRuntimeFeedbackRefinesSubsequentPrepare(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	b := memory.NewBatch()
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{0, 0, 0, 10, 11, 12, 13, 14}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 1, 2, 2, 3, 3, 3}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1, 1, 1, 1}}
	b.Reset(8)
	src, _ := inmemory.New(schema, b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf := s.FromSource(src).Filter(Col("pred").Gt(Lit(9))).GroupBy("key").Agg(Sum("value", "sum"), Count("count"))
	p1, err := lf.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := p1.Plan().FeedbackSamples; got != 0 {
		t.Fatalf("first feedback samples=%d", got)
	}
	res, err := p1.Execute(context.Background(), engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.SelectedRows != 5 || res.GroupCount != 2 {
		t.Fatalf("selected=%d groups=%d", res.SelectedRows, res.GroupCount)
	}
	p1.Close()

	p2, err := lf.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	info := p2.Plan()
	if info.FeedbackSamples != 1 {
		t.Fatalf("feedback samples=%d", info.FeedbackSamples)
	}
	if info.EstimatedCardinality != 2 {
		t.Fatalf("estimated cardinality=%d", info.EstimatedCardinality)
	}
	if info.EstimatedSelectivity < 0.624 || info.EstimatedSelectivity > 0.626 {
		t.Fatalf("estimated selectivity=%f", info.EstimatedSelectivity)
	}
}

func TestAggregateCanFeedTopKAndLimit(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	b := memory.NewBatch()
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2, 2, 3}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{5, 5, 10, 10, 10, 7}}
	b.Reset(6)
	src, _ := inmemory.New(schema, b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf := s.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("count")).Sort(Desc("sum")).Limit(2)
	optimized, err := lf.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(optimized, "TOPK k=2") {
		t.Fatalf("expected sort+limit topk rewrite:\n%s", optimized)
	}
	frame, err := lf.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	keys := frameInt32Column(t, frame, "key")
	sums := frameInt64Column(t, frame, "sum")
	if !equalI32(keys, []int32{2, 1}) || !equalI64(sums, []int64{30, 10}) {
		t.Fatalf("keys=%v sums=%v", keys, sums)
	}
}

func frameInt32Column(t *testing.T, f DataFrame, name string) []int32 {
	t.Helper()
	var out []int32
	for _, b := range f.batches() {
		v, err := b.Int32(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v.Values...)
	}
	return out
}
func equalI32(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRuntimeFeedbackUpdatesNativeAggregateEstimates(t *testing.T) {
	const rowsPerBatch = 75000
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	batches := make([]*memory.Batch, 4)
	for bi := range batches {
		b := memory.NewBatch()
		keys := make([]int32, rowsPerBatch)
		for i := range keys {
			keys[i] = int32(i & 1)
		}
		b.Int32s["key"] = &memory.Int32Vector{Values: keys}
		b.Int64s["value"] = &memory.Int64Vector{Values: make([]int64, rowsPerBatch)}
		b.Reset(rowsPerBatch)
		batches[bi] = b
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	s := NewSession(opts)
	defer s.Close()
	lf := s.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("count"))

	p1, err := lf.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info := p1.Plan(); info.FeedbackSamples != 0 || info.EstimatedCardinality <= 2 {
		p1.Close()
		t.Fatalf("first plan=%+v, want unobserved default cardinality", info)
	}
	res, err := p1.Execute(context.Background(), engine.ExecuteOptions{})
	if err != nil {
		p1.Close()
		t.Fatal(err)
	}
	if res.SelectedRows != rowsPerBatch*4 || res.GroupCount != 2 {
		p1.Close()
		t.Fatalf("selected=%d groups=%d", res.SelectedRows, res.GroupCount)
	}
	p1.Close()

	p2, err := lf.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	info := p2.Plan()
	if info.FeedbackSamples != 1 {
		t.Fatalf("feedback samples=%d, want 1", info.FeedbackSamples)
	}
	if info.EstimatedCardinality != 2 {
		t.Fatalf("feedback cardinality=%d, want 2", info.EstimatedCardinality)
	}
}

func TestExplicitHintsOverrideRuntimeFeedback(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
	)
	b := memory.NewBatch()
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4, 5, 6, 7}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 1, 1, 2, 2, 2, 2}}
	b.Reset(8)
	src, _ := inmemory.New(schema, b)
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	base := s.FromSource(src).Filter(Col("pred").Gt(Lit(3)))
	lf := base.GroupBy("key").Agg(Count("count"))
	p1, err := lf.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p1.Execute(context.Background(), engine.ExecuteOptions{}); err != nil {
		p1.Close()
		t.Fatal(err)
	}
	p1.Close()

	hinted := base.WithHints(expr.Hints{Selectivity: 0.25, Cardinality: 50000}).GroupBy("key").Agg(Count("count"))
	p2, err := hinted.PrepareResult(context.Background(), engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	info := p2.Plan()
	if info.FeedbackSamples != 1 {
		t.Fatalf("feedback samples=%d, want 1", info.FeedbackSamples)
	}
	if info.EstimatedSelectivity != 0.25 {
		t.Fatalf("selectivity=%f, want explicit 0.25", info.EstimatedSelectivity)
	}
	if info.EstimatedCardinality != 50000 {
		t.Fatalf("cardinality=%d, want explicit 50000", info.EstimatedCardinality)
	}
}
