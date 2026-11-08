package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
)

func openSortMetadataStream(t *testing.T, hard int64, rows int) (*externalSortStream, *memory.QueryBudget, context.Context) {
	t.Helper()
	engine := memory.NewEngineBudget(hard)
	query, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = query.Close() })
	ordered, err := NewSortSourceWithOptions(externalSortTestSource(t, rows, 512), []SortKey{{Column: "key"}, {Column: "id"}}, engine, SortSourceOptions{Spillable: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), query)
	stream, err := ordered.(*SortSource).NewBatchStream(ctx, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	result := stream.(*externalSortStream)
	t.Cleanup(func() { _ = result.Close() })
	return result, query, ctx
}

func TestExternalSortMetadataHardLimitCleansPartialRuns(t *testing.T) {
	stream, query, ctx := openSortMetadataStream(t, 64<<10, 8192)
	_, err := stream.Next(ctx, memory.NewBatch())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("error=%v, want metadata pressure", err)
	}
	if stream.spillRuns == 0 {
		t.Fatal("test did not reach partial spill state")
	}
	if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 || snapshot.PeakBytes > query.Limit() {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestExternalSortMetadataReleasedAtEOF(t *testing.T) {
	stream, query, ctx := openSortMetadataStream(t, 512<<10, 8192)
	dst := memory.NewBatch()
	rows := 0
	var lastKey, lastID int64
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i, key := range dst.Int64s["key"].Values {
			id := dst.Int64s["id"].Values[i]
			if rows > 0 && (key < lastKey || key == lastKey && id < lastID) {
				t.Fatal("output is not ordered")
			}
			rows++
			lastKey, lastID = key, id
		}
	}
	if rows != 8192 || stream.spillRuns <= externalSortFanIn {
		t.Fatalf("rows=%d runs=%d", rows, stream.spillRuns)
	}
	if stream.finalRuns.Values != nil || stream.finalDecoded != nil || stream.finalHeap.items != nil {
		t.Fatal("EOF retained metadata arrays")
	}
	if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 || snapshot.PeakBytes > query.Limit() {
		t.Fatalf("EOF snapshot=%+v", snapshot)
	}
	// EOF closes only the operator's wrapper, leaving the query store usable.
	store, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Spill(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: "test", Payload: []byte("sibling")})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortMetadataCancellationReleasesRuns(t *testing.T) {
	stream, query, ctx := openSortMetadataStream(t, 512<<10, 8192)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dst := memory.NewBatch()
	if ok, err := stream.Next(ctx, dst); !ok || err != nil {
		t.Fatalf("first: ok=%v err=%v", ok, err)
	}
	cancel()
	if _, err := stream.Next(ctx, dst); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if snapshot := query.Snapshot(); snapshot.UsedBytes != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestExternalSortMetadataIncludesNestedSchema(t *testing.T) {
	children := make([]memory.Field, 128)
	fields := []memory.Field{{Name: "nested", Type: memory.TypeStruct, Children: children}}
	bytes, err := externalSortSchemaMetadataBytes(fields)
	if err != nil {
		t.Fatal(err)
	}
	if bytes < externalSortMetadataByte+130*512 {
		t.Fatalf("nested schema undercharged: %d", bytes)
	}
}
