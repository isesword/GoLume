package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"strings"
	"testing"
)

func TestPhase32CollectUsesQueryOutputBudgetUntilDataFrameClose(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.Reset(4)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1024
	session := NewSession(opts)
	defer session.Close()

	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 32 {
		t.Fatalf("materialized output reservation=%d want=32", used)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("output reservation after DataFrame.Close=%d", used)
	}
}

func TestPhase32CollectBudgetFailureReportsTypedOutputHint(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.Reset(4)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 16
	session := NewSession(opts)
	defer session.Close()

	_, err = session.FromSource(src).Collect(context.Background())
	var resourceErr *memory.ResourceError
	if !errors.Is(err, memory.ErrBudgetExceeded) || !errors.As(err, &resourceErr) {
		t.Fatalf("collect error=%T %v", err, err)
	}
	if resourceErr.Operator != "Collect" || resourceErr.Class != memory.MemoryOutput || resourceErr.RequestedBytes != 32 || resourceErr.Hint == "" {
		t.Fatalf("resource error=%+v", resourceErr)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("failed collect reservation leak=%d", used)
	}
}

func TestPhase32ExplainShowsExplicitSpillLimitWithoutCreatingDirectory(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1024
	opts.Spill = engine.SpillOptions{Directory: base, MaxBytes: 4096}
	session := NewSession(opts)
	defer session.Close()
	explain, err := session.FromSource(src).ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(explain, "memory_limit=1024 spill_limit=4096") {
		t.Fatalf("explain missing resource limits:\n%s", explain)
	}
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) != 0 {
		t.Fatalf("Explain created spill directory entries=%v err=%v", entries, err)
	}
}
