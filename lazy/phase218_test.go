package lazy

import (
	"context"
	"errors"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestPhase218ListExpressionsFlowThroughLazyFrame(t *testing.T) {
	batch := memory.NewBatch()
	batch.Strings["value"] = &memory.StringViewVector{Offsets: []uint32{0, 5, 13}, Data: []byte("a,b,c语言-7"), Encoding: memory.EncodingView}
	batch.Strings["delimiter"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte(",-"), Encoding: memory.EncodingView}
	batch.Reset(2)
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingView},
		memory.Field{Name: "delimiter", Type: memory.TypeString, Encoding: memory.EncodingView},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(src).SelectExprs(
		Split(Col("value"), Col("delimiter")).Alias("parts"),
		RegexExtractAll(Col("value"), Str(`[^,\-0-9]+`), Lit(0)).Alias("words"),
	).CollectFrame(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Rows != 2 || len(frame.Batches) != 1 {
		t.Fatalf("frame rows=%d batches=%d", frame.Rows, len(frame.Batches))
	}
	parts, err := frame.Batches[0].StringList("parts")
	if err != nil {
		t.Fatal(err)
	}
	if start, end, ok := parts.ValueRange(0); !ok || end-start != 3 {
		t.Fatalf("parts row 0 range=(%d,%d,%v)", start, end, ok)
	}
	words, err := frame.Batches[0].StringList("words")
	if err != nil {
		t.Fatal(err)
	}
	if start, end, ok := words.ValueRange(1); !ok || end-start != 1 || string(words.Values.ValueBytes(start)) != "语言" {
		t.Fatalf("words row 1 range=(%d,%d,%v)", start, end, ok)
	}
}

func TestPhase218ListExpressionWorkspaceHonorsBudget(t *testing.T) {
	batch := memory.NewBatch()
	batch.Strings["value"] = &memory.StringViewVector{Offsets: []uint32{0, 10}, Data: []byte("a,b,c,d,e"), Encoding: memory.EncodingView}
	batch.Reset(1)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingView}), batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 16
	session := NewSession(opts)
	defer session.Close()
	_, err = session.FromSource(src).SelectExprs(Split(Col("value"), Str(",")).Alias("parts")).CollectFrame(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v want=%v", err, memory.ErrBudgetExceeded)
	}
	if used := session.engine.Memory().Used(); used != 0 {
		t.Fatalf("workspace reservation leaked %d bytes", used)
	}
}
