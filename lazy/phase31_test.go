package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
	"time"
)

func TestLazySortByExpressionsIsStableAndHidesTemporaryKeys(t *testing.T) {
	src := expressionOrderSource(t)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()

	frame, err := session.FromSource(src).
		SortBy(DescExpr(Length(Col("s"))), AscExpr(Year(Col("ts")))).
		Select("id").
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got, want := frameInt64Column(t, frame, "id"), []int64{4, 6, 2, 5, 1, 3}; !equalI64(got, want) {
		t.Fatalf("expression sort IDs=%v want=%v", got, want)
	}
	if len(frame.Schema.Fields) != 1 || frame.Schema.Fields[0].Name != "id" {
		t.Fatalf("temporary ordering columns leaked into output schema: %+v", frame.Schema.Fields)
	}

	explain := session.FromSource(src).SortBy(DescExpr(Length(Col("s"))), AscExpr(Year(Col("ts")))).Explain()
	if !strings.Contains(explain, "SORT [LENGTH(s) desc nulls-last, YEAR(ts) asc nulls-last]") {
		t.Fatalf("explain=%q", explain)
	}
}

func TestLazyExpressionSortLimitAndTopKAgreeWithNullPlacement(t *testing.T) {
	src := expressionOrderSource(t)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	keys := []SortExpr{DescExpr(Length(Col("s"))), AscExpr(Year(Col("ts")))}

	sorted, err := session.FromSource(src).SortBy(keys...).Limit(4).Select("id").CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	top, err := session.FromSource(src).TopKBy(4, keys...).Select("id").CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer top.Close()
	want := []int64{4, 6, 2, 5}
	if got := frameInt64Column(t, sorted, "id"); !equalI64(got, want) {
		t.Fatalf("sort+limit IDs=%v want=%v", got, want)
	}
	if got := frameInt64Column(t, top, "id"); !equalI64(got, want) {
		t.Fatalf("top-k IDs=%v want=%v", got, want)
	}

	nullsFirst, err := session.FromSource(src).
		SortBy(DescExprNullsFirst(Length(Col("s")))).
		Select("id").
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer nullsFirst.Close()
	if got, want := frameInt64Column(t, nullsFirst, "id"), []int64{3, 2, 4, 6, 1, 5}; !equalI64(got, want) {
		t.Fatalf("descending NULLS FIRST IDs=%v want=%v", got, want)
	}
}

func TestLazyExpressionTopKFirstPassReadsOnlyDependencies(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "payload", Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	batch.Int64s["score"] = &memory.Int64Vector{Values: []int64{1, 4, 2, 3}}
	batch.Int64s["payload"] = &memory.Int64Vector{Values: []int64{10, 40, 20, 30}}
	batch.Reset(4)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]source.PushdownRequest, 0, 2)
	src := &topKPushdownRecorder{input: base, requests: &requests}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).
		TopKBy(2, DescExpr(Col("score").Add(Lit(1)))).
		Select("payload").
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got, want := frameInt64Column(t, frame, "payload"), []int64{40, 30}; !equalI64(got, want) {
		t.Fatalf("payload=%v want=%v", got, want)
	}
	if len(requests) != 2 {
		t.Fatalf("pushdown requests=%d want 2", len(requests))
	}
	if got, want := requests[0].Projection, []string{"score", "payload"}; !equalStrings(got, want) {
		t.Fatalf("payload projection=%v want=%v", got, want)
	}
	if got, want := requests[1].Projection, []string{"score"}; !equalStrings(got, want) {
		t.Fatalf("top-k expression first-pass projection=%v want=%v", got, want)
	}
}

func TestLazySortByRejectsPredicateAndAmbiguousKeys(t *testing.T) {
	src := expressionOrderSource(t)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	if _, err := session.FromSource(src).SortBy(DescExpr(expr.Contains(Col("s"), Str("x")))).Schema(); err == nil {
		t.Fatal("predicate expression must not be accepted as a sort key")
	}
	if _, err := session.FromSource(src).SortBy(SortExpr{}).Schema(); err == nil {
		t.Fatal("empty sort expression must be rejected")
	}
}

func TestLazyExpressionSortAvoidsInternalKeyNameCollision(t *testing.T) {
	const userColumn = "__gocolumn_order_key_0"
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: userColumn, Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2}}
	batch.Int64s["score"] = &memory.Int64Vector{Values: []int64{10, 30}}
	batch.Int64s[userColumn] = &memory.Int64Vector{Values: []int64{100, 200}}
	batch.Reset(2)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).SortBy(DescExpr(Col("score").Add(Lit(1)))).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got, want := frameInt64Column(t, frame, "id"), []int64{2, 1}; !equalI64(got, want) {
		t.Fatalf("IDs=%v want=%v", got, want)
	}
	if got := frameInt64Column(t, frame, userColumn); !equalI64(got, []int64{200, 100}) {
		t.Fatalf("user column %q was overwritten or removed: %v", userColumn, got)
	}
}

func TestLazyParallelExpressionSortKeepsStableOrderAcrossSourcePartitions(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "score", Type: memory.TypeInt64},
	)
	batches := []*memory.Batch{
		{Rows: 3, Int64s: map[string]*memory.Int64Vector{
			"id":    {Values: []int64{0, 1, 2}},
			"score": {Values: []int64{2, 1, 1}},
		}},
		{Rows: 3, Int64s: map[string]*memory.Int64Vector{
			"id":    {Values: []int64{3, 4, 5}},
			"score": {Values: []int64{2, 1, 3}},
		}},
		{Rows: 3, Int64s: map[string]*memory.Int64Vector{
			"id":    {Values: []int64{6, 7, 8}},
			"score": {Values: []int64{1, 2, 1}},
		}},
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.Options{Workers: 3})
	defer session.Close()
	frame, err := session.FromSource(src).
		SortBy(AscExpr(Col("score").Add(Lit(1)))).
		Select("id").
		CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got, want := frameInt64Column(t, frame, "id"), []int64{1, 2, 4, 6, 8, 0, 3, 7, 5}; !equalI64(got, want) {
		t.Fatalf("parallel expression sort IDs=%v want=%v", got, want)
	}
}

func expressionOrderSource(t *testing.T) source.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp},
	)
	batch1 := memory.NewBatch()
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	batch1.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	batch1.Strings["s"] = &memory.StringViewVector{
		Offsets:  []uint32{0, 1, 7, 7},
		Data:     []byte("a语语"),
		Validity: valid,
		Encoding: memory.EncodingView,
	}
	batch1.Int64s["ts"] = &memory.Int64Vector{Values: []int64{
		time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
	}}
	batch1.Reset(3)
	batch2 := memory.NewBatch()
	batch2.Int64s["id"] = &memory.Int64Vector{Values: []int64{4, 5, 6}}
	batch2.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 2, 3, 5}, Data: []byte("xyzab"), Encoding: memory.EncodingView}
	batch2.Int64s["ts"] = &memory.Int64Vector{Values: []int64{
		time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2023, time.January, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
	}}
	batch2.Reset(3)
	src, err := inmemory.New(schema, batch1, batch2)
	if err != nil {
		t.Fatal(err)
	}
	return src
}
