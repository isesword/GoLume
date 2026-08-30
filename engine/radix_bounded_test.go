package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func radixWorkspaceSource(t *testing.T, batches, rows int) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	input := make([]*memory.Batch, batches)
	for batchIndex := range input {
		batch := memory.NewBatch()
		keys := make([]int32, rows)
		values := make([]int64, rows)
		for row := range keys {
			keys[row] = int32((batchIndex*rows + row) & 1023)
			values[row] = 1
		}
		batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(rows)
		input[batchIndex] = batch
	}
	src, err := inmemory.New(schema, input...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestFiniteBudgetRadixWorkspaceIsMorselBounded(t *testing.T) {
	src := radixWorkspaceSource(t, 8, 1024)
	query := expr.Query{
		GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("n")},
		Hints: expr.Hints{ForceStrategy: "radix", ForceWorkerCount: 4, Cardinality: 1024},
	}
	for _, tc := range []struct {
		name      string
		limit     int64
		wantRows  int
		isBounded bool
	}{
		{name: "unlimited-resident", wantRows: 8 * 1024},
		{name: "finite-wave", limit: 8 << 20, wantRows: 4 * 1024, isBounded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.Workers = 4
			opts.MemoryLimitBytes = tc.limit
			eng := New(opts)
			prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{})
			if err != nil {
				eng.Close()
				t.Fatal(err)
			}
			workspace, err := prepared.newWorkspace(context.Background())
			if err != nil {
				_ = prepared.Close()
				eng.Close()
				t.Fatal(err)
			}
			if workspace.radixBounded != tc.isBounded {
				t.Fatalf("radixBounded=%v want=%v", workspace.radixBounded, tc.isBounded)
			}
			if got := len(workspace.selectedKeys); got != tc.wantRows {
				t.Fatalf("selected workspace rows=%d want=%d", got, tc.wantRows)
			}
			if err := workspace.close(); err != nil {
				t.Fatal(err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			eng.Close()
		})
	}
}
