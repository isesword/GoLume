package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestSortHonorsMemoryBudget(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, 1024)}
	b.Int64s["value"] = &memory.Int64Vector{Values: make([]int64, 1024)}
	b.Reset(1024)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1024)
	s, err := NewSortSource(base, []SortKey{{Column: "key", NullsLast: true}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := s.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, err := s.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	err = r.ReadPartition(ctx, parts[0], dst)
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("sort error=%v, want ErrBudgetExceeded", err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after rejected reservation, want 0", used)
	}
}

func TestTopKHonorsMemoryBudget(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, 1024)}
	b.Int64s["value"] = &memory.Int64Vector{Values: make([]int64, 1024)}
	b.Reset(1024)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(256)
	s, err := NewTopKSourceWithManager(base, 100, []SortKey{{Column: "key", NullsLast: true}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := s.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	parts, err := s.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = r.ReadPartition(ctx, parts[0], memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("top-k error=%v, want ErrBudgetExceeded", err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after rejected reservation, want 0", used)
	}
}

func TestTopKReleasesMemoryReservationOnClose(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{5, 4, 3, 2, 1}}
	b.Reset(5)
	base, _ := inmemory.New(schema, b)
	manager := memory.NewBudgetManager(1 << 20)
	s, err := NewTopKSourceWithManager(base, 2, []SortKey{{Column: "key", NullsLast: true}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := s.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := s.Partitions(ctx)
	dst := memory.NewBatch()
	if err := r.ReadPartition(ctx, parts[0], dst); err != nil {
		t.Fatal(err)
	}
	if manager.Used() == 0 {
		t.Fatal("top-k did not retain a memory reservation while reader is active")
	}
	dst.ReleaseOwned()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after Close, want 0", used)
	}
}
