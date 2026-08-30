package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
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
	r, err := s.NewReader(ctx, testReadOptions())
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
	r, err := s.NewReader(ctx, testReadOptions())
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
	r, err := s.NewReader(ctx, testReadOptions())
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

func TestTopKRejectsActualVariableKeyStateOverBudget(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeString},
		memory.Field{Name: "id", Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	batch.Strings["key"] = &memory.StringViewVector{Offsets: []uint32{0, 8 << 10}, Data: make([]byte, 8<<10), Encoding: memory.EncodingView}
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(4 << 10)
	top, err := NewTopKSourceWithManager(base, 1, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := top.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	parts, err := top.Partitions(context.Background())
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	err = reader.ReadPartition(context.Background(), parts[0], memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		_ = reader.Close()
		t.Fatalf("top-k error=%v, want ErrBudgetExceeded for retained string key", err)
	}
	if manager.Used() == 0 {
		_ = reader.Close()
		t.Fatal("top-k should retain its accepted base reservation until Close")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after Close, want 0", used)
	}
}

func TestDescendingOrderKeepsNullPlacementForSortAndTopK(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true},
	)
	valid := memory.EnsureBitmap(nil, 4)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	memory.Set(valid, 3)
	batch := memory.NewBatch()
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 0, 2, 1}, Validity: valid}
	batch.Reset(4)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		make func() (source.Source, error)
	}{
		{name: "sort", make: func() (source.Source, error) {
			return NewSortSource(base, []SortKey{{Column: "key", Descending: true, NullsLast: true}}, nil)
		}},
		{name: "top-k", make: func() (source.Source, error) {
			return NewTopKSourceWithManager(base, 4, []SortKey{{Column: "key", Descending: true, NullsLast: true}}, nil)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ordered, err := tt.make()
			if err != nil {
				t.Fatal(err)
			}
			reader, err := ordered.NewReader(context.Background(), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			parts, err := ordered.Partitions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			if err := reader.ReadPartition(context.Background(), parts[0], out); err != nil {
				t.Fatal(err)
			}
			ids, err := out.Int64("id")
			if err != nil {
				t.Fatal(err)
			}
			if want := []int64{1, 3, 4, 2}; !equalInt64s(ids.Values, want) {
				t.Fatalf("ids=%v want=%v", ids.Values, want)
			}
		})
	}
}

func TestParallelSortMatchesSerialMultiKeyOrderAndKeepsGlobalStability(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "tie", Type: memory.TypeInt64},
	)
	keyValidity := func(rows int, validRows ...int) memory.Bitmap {
		valid := memory.EnsureBitmap(nil, rows)
		for _, row := range validRows {
			memory.Set(valid, row)
		}
		return valid
	}
	batches := []*memory.Batch{
		{
			Rows: 4,
			Int64s: map[string]*memory.Int64Vector{
				"id":  {Values: []int64{0, 1, 2, 3}},
				"key": {Values: []int64{2, 1, 1, 2}, Validity: keyValidity(4, 0, 1, 2, 3)},
				"tie": {Values: []int64{1, 1, 1, 0}},
			},
		},
		{
			Rows: 3,
			Int64s: map[string]*memory.Int64Vector{
				"id":  {Values: []int64{4, 5, 6}},
				"key": {Values: []int64{1, 0, 2}, Validity: keyValidity(3, 0, 2)},
				"tie": {Values: []int64{2, 9, 1}},
			},
		},
		{
			Rows: 3,
			Int64s: map[string]*memory.Int64Vector{
				"id":  {Values: []int64{7, 8, 9}},
				"key": {Values: []int64{1, 0, 2}, Validity: keyValidity(3, 0, 2)},
				"tie": {Values: []int64{2, 7, 0}},
			},
		},
	}
	base, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	keys := []SortKey{
		{Column: "key", NullsLast: true},
		{Column: "tie", Descending: true, NullsLast: true},
	}
	serial, err := NewSortSource(base, keys, nil)
	if err != nil {
		t.Fatal(err)
	}
	exec := &sortExecutorSpy{workers: 3}
	parallel, err := NewSortSourceWithOptions(base, keys, nil, SortSourceOptions{Workers: 3, Executor: exec})
	if err != nil {
		t.Fatal(err)
	}
	serialIDs := sortedIDs(t, serial)
	parallelIDs := sortedIDs(t, parallel)
	want := []int64{4, 7, 1, 2, 0, 6, 3, 9, 5, 8}
	if !equalInt64s(serialIDs, want) {
		t.Fatalf("serial IDs=%v want=%v", serialIDs, want)
	}
	if !equalInt64s(parallelIDs, want) {
		t.Fatalf("parallel IDs=%v want=%v", parallelIDs, want)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("parallel executor calls=%d want 1", len(exec.calls))
	}
	if call := exec.calls[0]; call.tasks != 3 || call.parallelism != 3 {
		t.Fatalf("parallel sort executor call=%+v want tasks=3 parallelism=3", call)
	}
}

func TestParallelSortRequiresExecutor(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1}}
	batch.Reset(2)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSortSourceWithOptions(base, []SortKey{{Column: "key"}}, nil, SortSourceOptions{Workers: 2}); err == nil {
		t.Fatal("parallel sort without executor succeeded")
	}
}

func TestParallelSortChargesMergedOrdinalBuffer(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{4, 3, 2, 1}}
	batch.Reset(4)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(estimateOrderBytes(schema, 4))
	ordered, err := NewSortSourceWithOptions(base, []SortKey{{Column: "key"}}, manager, SortSourceOptions{Workers: 2, Executor: &sortExecutorSpy{workers: 2}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ordered.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	parts, err := ordered.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = reader.ReadPartition(context.Background(), parts[0], memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("parallel sort error=%v, want ErrBudgetExceeded", err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after rejected parallel reservation, want 0", used)
	}
}

type sortExecutorCall struct {
	tasks       int
	parallelism int
}

type sortExecutorSpy struct {
	workers int
	calls   []sortExecutorCall
}

func (e *sortExecutorSpy) MaxWorkers() int { return e.workers }

func (e *sortExecutorSpy) ParallelFor(ctx context.Context, tasks, parallelism int, fn func(task int) error) error {
	e.calls = append(e.calls, sortExecutorCall{tasks: tasks, parallelism: parallelism})
	for task := 0; task < tasks; task++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(task); err != nil {
			return err
		}
	}
	return nil
}

func sortedIDs(t *testing.T, ordered source.Source) []int64 {
	t.Helper()
	ctx := context.Background()
	parts, err := ordered.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("ordered partitions=%d want 1", len(parts))
	}
	reader, err := ordered.NewReader(ctx, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	out := memory.NewBatch()
	defer out.ReleaseOwned()
	if err := reader.ReadPartition(ctx, parts[0], out); err != nil {
		t.Fatal(err)
	}
	ids, err := out.Int64("id")
	if err != nil {
		t.Fatal(err)
	}
	return append([]int64(nil), ids.Values...)
}

func equalInt64s(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
