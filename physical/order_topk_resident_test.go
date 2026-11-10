package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestResidentTopKLargeKSmallInput(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	b.Reset(3)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(64 << 10)
	top, err := NewTopKSourceWithManager(base, 1<<30, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	out := memory.NewBatch()
	ok, err := reader.Next(context.Background(), out)
	if err != nil || !ok {
		t.Fatalf("Next: %v, %v", ok, err)
	}
	values := out.Int64s["key"].Values
	if len(values) != 3 || values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("values=%v", values)
	}
	if manager.Used() == 0 {
		t.Fatal("live result is uncharged")
	}
	ok, err = reader.Next(context.Background(), out)
	if err != nil || ok {
		t.Fatalf("EOF: %v, %v", ok, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("used after EOF=%d", manager.Used())
	}
	if len(out.Int64s) != 0 {
		t.Fatal("EOF retained borrowed columns")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if ok, err := reader.Next(context.Background(), out); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
		t.Fatal("explicit Close after EOF", ok, err)
	}
}

func TestResidentTopKUnlimitedAccountsAndCancellationClearsBorrow(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1}}
	b.Reset(2)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	top, err := NewTopKSourceWithManager(base, 1, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	out := memory.NewBatch()
	ok, err := reader.Next(context.Background(), out)
	if err != nil || !ok {
		t.Fatalf("Next: %v, %v", ok, err)
	}
	if manager.Used() == 0 {
		t.Fatal("unlimited manager did not account output")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reader.Next(ctx, out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("used=%d", manager.Used())
	}
	if len(out.Int64s) != 0 {
		t.Fatal("cancellation retained borrowed columns")
	}
}

func TestResidentTopKReplacementAdmitsKeyOverlap(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString})
	data := make([]byte, 8192)
	for i := range data {
		if i < 4096 {
			data[i] = 'z'
		} else {
			data[i] = 'a'
		}
	}
	b := memory.NewBatch()
	b.Strings["key"] = &memory.StringViewVector{Offsets: []uint32{0, 4096, 8192}, Data: data, Encoding: memory.EncodingView}
	b.Reset(2)
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(9 << 10)
	top, err := NewTopKSourceWithManager(base, 1, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, err = reader.Next(context.Background(), memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) || !strings.Contains(err.Error(), "top-k candidate memory") {
		t.Fatalf("replacement overlap error=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("used after rejection=%d", manager.Used())
	}
}

func TestResidentTopKFailureClearsInputReferences(t *testing.T) {
	input, _ := newResidentDictionaryInput(memory.TypeBinary)
	manager := memory.NewBudgetManager(1)
	var owner source.Source
	var err error
	owner, err = NewTopKSourceWithManager(input, 5, []SortKey{{Column: "id"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), owner, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	r := stream.(*topKReader)
	// Seed an input borrow to exercise cleanup even when the first reservation
	// is denied before the source emits or clears its next batch.
	r.scratch.DictStrings["stale"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("x")}
	r.scratch.Rows = 1
	if ok, err := r.Next(context.Background(), memory.NewBatch()); ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(ok, err)
	}
	if r.scratch.Rows != 0 || len(r.scratch.DictStrings) != 0 || len(r.scratch.Int64s) != 0 || len(r.scratch.Opaque) != 0 {
		t.Fatal("failed reader retains input references")
	}
	if manager.Used() != 0 || input.opens != input.closes {
		t.Fatal("failure cleanup incomplete")
	}
}
