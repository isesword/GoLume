package lazy

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestWindowInternalNamesReviewCascade(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "resident", true: "external"}[external], func(t *testing.T) {
			const first = "__gocolumn_order_key_0"
			const second = "__gocolumn_order_key_0_1"
			b := memory.NewBatch()
			b.Rows = 4
			b.Int64s["id"] = &memory.Int64Vector{Values: []int64{30, 10, 40, 20}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), b)
			if err != nil {
				t.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 8 << 20
			if external {
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
			}
			session := NewSession(opts)
			defer session.Close()
			query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(AscExpr(Col("id").Add(Lit(int64(1)))))), RowNumber(first)).WithWindows(NewWindow(nil, OrderBy(DescExpr(Col(first).Add(Lit(int64(1)))))), RowNumber(second), RollingMin(first, RowsBetween(CurrentRow, CurrentRow), "__golume_window_ordinal"))
			wantSchema, err := query.Schema()
			if err != nil {
				t.Fatal(err)
			}
			result, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			if !reflect.DeepEqual(result.Schema(), wantSchema) {
				t.Fatalf("schema=%+v want %+v", result.Schema(), wantSchema)
			}
			for name, want := range map[string][]int64{"id": {30, 10, 40, 20}, first: {3, 1, 4, 2}, second: {2, 4, 1, 3}, "__golume_window_ordinal": {3, 1, 4, 2}} {
				if got := frameInt64Column(t, result, name); !reflect.DeepEqual(got, want) {
					t.Errorf("%s=%v want %v", name, got, want)
				}
			}
		})
	}
}

func TestWindowInternalNamesReviewPublicSpill(t *testing.T) {
	// Exceed the public reader's prefetch so spill files remain observable
	// when its first batch is delivered.
	const rows = 31 * 1024
	const key = "__gocolumn_order_key_0"
	const ordinal = "__golume_window_ordinal"
	dir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	opts.QueryMemorySoftLimitBytes = 48 << 10
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	session := NewSession(opts)
	defer session.Close()
	var inputs []*memory.Batch
	for start := 0; start < rows; start += 127 {
		b := memory.NewBatch()
		b.Rows = min(127, rows-start)
		b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		for j := 0; j < b.Rows; j++ {
			b.Int64s["id"].Values[j] = int64(rows - 1 - start - j)
		}
		inputs = append(inputs, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), inputs...)
	if err != nil {
		t.Fatal(err)
	}
	query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(AscExpr(Col("id").Add(Lit(int64(1)))))), RowNumber(key), RollingMin("id", RowsBetween(CurrentRow, CurrentRow), ordinal))
	expected, err := query.Schema()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "strategy=external-sort-restore") {
		t.Fatal(plan)
	}
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	ctx := memory.WithQueryBudget(context.Background(), budget)
	reader, err := query.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen, batches, live := 0, 0, 0
	for reader.Next(ctx) {
		batches++
		if batches == 1 {
			if err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					live++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		batch := reader.Batch()
		if !reflect.DeepEqual(batch.Schema(), expected) {
			t.Fatalf("schema=%+v want %+v", batch.Schema(), expected)
		}
		columns := map[string][]any{}
		for _, name := range []string{"id", key, ordinal} {
			column, err := batch.Column(name)
			if err != nil {
				t.Fatal(err)
			}
			columns[name], err = column.ToAnySlice()
			if err != nil {
				t.Fatal(err)
			}
		}
		for row := 0; row < int(batch.Len()); row++ {
			id := int64(rows - 1 - seen)
			for name, want := range map[string]int64{"id": id, key: id + 1, ordinal: id} {
				if got := columns[name][row]; got != want {
					t.Fatalf("row %d column %s=%v want %d", seen, name, got, want)
				}
			}
			seen++
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if seen != rows || batches < 2 || live < 2 {
		t.Fatalf("rows=%d batches=%d live spill files=%d", seen, batches, live)
	}
	snapshot := budget.Snapshot()
	if snapshot.SpilledBytes == 0 || snapshot.PeakBytes > budget.Limit() || budget.Used() != 0 {
		t.Fatalf("spill accounting: %+v", snapshot)
	}
	t.Logf("rows=%d batches=%d live spill files=%d spill bytes=%d", seen, batches, live, snapshot.SpilledBytes)
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill leftovers=%v error=%v", entries, err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("memory leaked=%d", used)
	}
}
