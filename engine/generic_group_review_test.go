package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
	"unsafe"
)

// A one-row wide generic key must account for the vector objects retained in
// addition to their payloads. Map overhead is deliberately excluded here.
func TestReviewGenericWideOutputDescriptorAdmission(t *testing.T) {
	const columns = 1024
	fields := make([]memory.Field, columns)
	for i := range fields {
		fields[i] = memory.Field{Name: fmt.Sprintf("key%d", i), Type: memory.TypeUint8}
	}
	schema := memory.NewSchema(fields...)
	charged, err := aggregateOutputBytes(schema, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	lowerBound := int64(columns) * (1 + int64(unsafe.Sizeof(memory.Uint8Vector{})))
	if charged < lowerBound {
		t.Fatalf("output admission=%d bytes, payload+vector descriptors alone=%d bytes (%d keys); maps excluded", charged, lowerBound, columns)
	}
}

func TestReviewGenericRepeatedKeyDoesNotCorruptColumn(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x")}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), b)
	if err != nil {
		t.Fatal(err)
	}
	e := New(streamTestOptions())
	defer e.Close()
	p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"s", "s", "s"}, Aggregates: []expr.Aggregate{expr.Count("n")}}, PrepareOptions{})
	if err != nil {
		return
	} // Rejecting duplicate output names is safe.
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	stream, err := p.NewResidentAggregateStream(ctx, p.resultSchema(), 1)
	if err != nil {
		return
	}
	defer stream.Close()
	out := memory.NewBatch()
	ok, err := stream.Next(ctx, out)
	if err != nil {
		t.Fatalf("accepted repeated key fails during execution: %v", err)
	}
	if !ok || string(out.Strings["s"].ValueBytes(0)) != "x" {
		t.Fatalf("repeated key corrupted output: %+v", out.Strings["s"])
	}
}

func TestReviewGenericRejectOutputAliasCollisions(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "agg_0", Type: memory.TypeUint8})
	for _, q := range []expr.Query{
		{GroupBy: []string{"s"}, Aggregates: []expr.Aggregate{expr.Count("s")}},
		{GroupBy: []string{"s"}, Aggregates: []expr.Aggregate{expr.Count("n"), expr.Count("n")}},
		{GroupBy: []string{"agg_0"}, Aggregates: []expr.Aggregate{expr.Count("")}},
		{GroupBy: []string{"s"}, Aggregates: []expr.Aggregate{expr.Count(""), expr.Count("agg_0")}},
	} {
		if _, err := compileQuery(schema, q); err == nil {
			t.Fatalf("accepted colliding output names: %+v", q)
		}
	}
}
