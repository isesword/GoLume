package lazy

import (
	"context"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestLazyStringTransformsProjectAndFilter(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeString, Encoding: memory.EncodingView})
	batch := memory.NewBatch()
	batch.Strings["raw"] = phase33LazyStringVector([]string{"  Go语言  ", "irrelevant", "  Go语  "})
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}

	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	normalized := Trim(Col("raw"))
	frame, err := session.FromSource(src).
		WithColumns(
			normalized.Alias("text"),
			ReplaceAll(normalized, Str("语"), Str("文")).Alias("replaced"),
			Slice(normalized, Lit(-2), Lit(2)).Alias("suffix"),
			Find(normalized, Str("语")).Alias("position"),
			ByteLength(normalized).Alias("bytes"),
		).
		Filter(Find(Col("text"), Str("语")).Eq(Lit(2))).
		SelectExprs(
			Col("replaced").Alias("replaced"),
			Col("suffix").Alias("suffix"),
			Col("position").Alias("position"),
			Col("bytes").Alias("bytes"),
		).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 2 || len(frame.batches()) != 1 {
		t.Fatalf("rows=%d batches=%d", frame.Height(), len(frame.batches()))
	}
	phase33LazyRequireStrings(t, frame.batches()[0], "replaced", []string{"Go文言", "Go文"})
	phase33LazyRequireStrings(t, frame.batches()[0], "suffix", []string{"语言", "o语"})
	for _, column := range []struct {
		name string
		want []int64
	}{
		{name: "position", want: []int64{2, 2}},
		{name: "bytes", want: []int64{8, 5}},
	} {
		v, err := frame.batches()[0].Int64(column.name)
		if err != nil {
			t.Fatal(err)
		}
		for row, want := range column.want {
			if v.Values[row] != want {
				t.Fatalf("%s row %d=%d want=%d", column.name, row, v.Values[row], want)
			}
		}
	}
}

func phase33LazyStringVector(values []string) *memory.StringViewVector {
	v := &memory.StringViewVector{Offsets: make([]uint32, len(values)+1), Encoding: memory.EncodingView}
	for i, value := range values {
		v.Data = append(v.Data, value...)
		v.Offsets[i+1] = uint32(len(v.Data))
	}
	return v
}

func phase33LazyRequireStrings(t *testing.T, batch *memory.Batch, name string, want []string) {
	t.Helper()
	v, err := batch.String(name)
	if err != nil {
		t.Fatal(err)
	}
	if v.Len() != len(want) {
		t.Fatalf("%s rows=%d want=%d", name, v.Len(), len(want))
	}
	for row, expected := range want {
		if !v.IsValid(row) || string(v.ValueBytes(row)) != expected {
			t.Fatalf("%s row %d=%q valid=%v want=%q", name, row, v.ValueBytes(row), v.IsValid(row), expected)
		}
	}
}
