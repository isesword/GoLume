package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func externalSortTestSource(t testing.TB, rows, batchRows int) *inmemory.Source {
	t.Helper()
	batches := make([]*memory.Batch, 0, (rows+batchRows-1)/batchRows)
	for offset := 0; offset < rows; offset += batchRows {
		end := offset + batchRows
		if end > rows {
			end = rows
		}
		batch := memory.NewBatch()
		keys, ids := make([]int64, end-offset), make([]int64, end-offset)
		for row := range keys {
			id := offset + row
			keys[row], ids[row] = int64((rows-id)%257), int64(id)
		}
		batch.Int64s["key"] = &memory.Int64Vector{Values: keys}
		batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
		batch.Reset(len(keys))
		batches = append(batches, batch)
	}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64},
	), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func openExternalSortTestStream(t testing.TB, rows int, spillBytes int64) (*externalSortStream, *memory.QueryBudget) {
	t.Helper()
	// Include spill paths, handles, and retained run-reference capacity.
	const hard = int64(512 << 10)
	engineBudget := memory.NewEngineBudget(hard)
	query, err := engineBudget.BeginQuery(memory.BudgetOptions{
		SoftLimitBytes: 48 << 10, HardLimitBytes: hard,
		Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: spillBytes},
	})
	if err != nil {
		t.Fatal(err)
	}
	ordered, err := NewSortSourceWithOptions(
		externalSortTestSource(t, rows, 512), []SortKey{{Column: "key"}, {Column: "id"}}, engineBudget,
		SortSourceOptions{Spillable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), query)
	stream, err := ordered.(*SortSource).NewBatchStream(ctx, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	external := stream.(*externalSortStream)
	// Keep multiple hierarchy levels independent of the metadata allowance.
	external.chunkRows, external.runBytes = 128, 4096
	return external, query
}

func TestExternalSortHierarchicalMergeHasBoundedFanIn(t *testing.T) {
	stream, query := openExternalSortTestStream(t, 8192, 64<<20)
	ctx := memory.WithQueryBudget(context.Background(), query)
	stats, err := source.DrainBatchStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 8192 {
		t.Fatalf("rows=%d", stats.Rows)
	}
	diagnostics := stream.StreamDiagnostics()
	levels, _ := strconv.Atoi(diagnostics["merge_levels"])
	if levels < 2 || diagnostics["merge_fan_in"] != "4" {
		t.Fatalf("diagnostics=%v", diagnostics)
	}
	if snapshot := query.Snapshot(); snapshot.PeakBytes > query.Limit() || snapshot.UsedBytes != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortCorruptPendingRunFailsAndCleansUp(t *testing.T) {
	stream, query := openExternalSortTestStream(t, 8192, 64<<20)
	ctx := memory.WithQueryBudget(context.Background(), query)
	dst := memory.NewBatch()
	if ok, err := stream.Next(ctx, dst); err != nil || !ok {
		t.Fatalf("first batch ok=%v err=%v", ok, err)
	}
	store, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	directory := store.(*memory.QuerySpillStore).Directory()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".gls" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-1] ^= 0xff
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		corrupted++
	}
	if corrupted == 0 {
		t.Fatal("expected pending sort segments")
	}
	var got error
	for got == nil {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			got = err
			break
		}
		if !ok {
			break
		}
	}
	if !errors.Is(got, memory.ErrSpillCorrupt) {
		t.Fatalf("corrupt error=%v", got)
	}
	_ = stream.Close()
	if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 {
		t.Fatalf("corrupt cleanup snapshot=%+v", snapshot)
	}
	if err := query.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortDiskQuotaAndCancellationReleaseResources(t *testing.T) {
	t.Run("disk-quota", func(t *testing.T) {
		stream, query := openExternalSortTestStream(t, 4096, 512)
		ctx := memory.WithQueryBudget(context.Background(), query)
		_, err := stream.Next(ctx, memory.NewBatch())
		var resource *memory.SpillResourceError
		if !errors.As(err, &resource) {
			t.Fatalf("err=%v want SpillResourceError", err)
		}
		_ = stream.Close()
		if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 {
			t.Fatalf("quota cleanup snapshot=%+v", snapshot)
		}
		_ = query.Close()
	})

	t.Run("cancel", func(t *testing.T) {
		stream, query := openExternalSortTestStream(t, 8192, 64<<20)
		base, cancel := context.WithCancel(context.Background())
		ctx := memory.WithQueryBudget(base, query)
		dst := memory.NewBatch()
		if ok, err := stream.Next(ctx, dst); err != nil || !ok {
			t.Fatalf("first batch ok=%v err=%v", ok, err)
		}
		cancel()
		if _, err := stream.Next(ctx, dst); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel err=%v", err)
		}
		_ = stream.Close()
		if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 {
			t.Fatalf("cancel cleanup snapshot=%+v", snapshot)
		}
		_ = query.Close()
	})
}
