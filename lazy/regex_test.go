package lazy

import (
	"context"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestLazyRegexFilterAndProjection(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "event", Type: memory.TypeString, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["event"] = phase33LazyStringVector([]string{"user:42", "语言:7", "invalid"})
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}

	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	pattern := Str(`^([^:]+):([0-9]+)$`)
	frame, err := session.FromSource(src).
		Filter(RegexMatch(Col("event"), pattern)).
		SelectExprs(
			RegexExtract(Col("event"), pattern, Lit(1)).Alias("name"),
			RegexExtract(Col("event"), pattern, Lit(2)).Alias("id"),
		).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 2 || len(frame.batches()) != 1 {
		t.Fatalf("rows=%d batches=%d", frame.Height(), len(frame.batches()))
	}
	phase33LazyRequireStrings(t, frame.batches()[0], "name", []string{"user", "语言"})
	phase33LazyRequireStrings(t, frame.batches()[0], "id", []string{"42", "7"})
}
