package physical

import (
	"context"
	"errors"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

func openJoinMetadataStream(t *testing.T, hard int64) (context.Context, *spillableJoinStream, *memory.QueryBudget) {
	t.Helper()
	engine := memory.NewEngineBudget(hard)
	query, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := query.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := memory.WithQueryBudget(context.Background(), query)
	kernels := kernel.New(kernel.ModeScalar)
	owner, err := NewJoinSourceWithOptions(ctx, spillNarrowSource(t, 0, 256, 1), spillNarrowSource(t, 128, 256, 2), join.Full, []JoinKey{{Left: "id", Right: "id"}}, "_r", engine, JoinSourceOptions{Kernels: kernels, RadixBits: 4, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: hard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	stream, err := newSpillableJoinStream(ctx, owner, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, stream, query
}

func TestJoinMetadataHardLimitCleansPartialRuns(t *testing.T) {
	ctx, stream, query := openJoinMetadataStream(t, 64<<10)
	dst := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			break
		}
		if !ok {
			t.Fatal("expected explicit metadata/buffer admission failure")
		}
	}
	if got := query.Snapshot(); got.UsedBytes != 0 || got.PeakBytes > query.Limit() {
		t.Fatalf("snapshot=%+v", got)
	}
	if len(stream.outputRuns.Values) != 0 {
		t.Fatal("retained output reference array")
	}
}

func TestJoinMetadataReleasedAtEOF(t *testing.T) {
	ctx, stream, query := openJoinMetadataStream(t, 2<<20)
	dst := memory.NewBatch()
	rows := 0
	for {
		ok, err := stream.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rows += dst.Rows
	}
	if rows != 640 {
		t.Fatalf("rows=%d", rows)
	}
	if got := query.Snapshot(); got.UsedBytes != 0 || got.PeakBytes > query.Limit() {
		t.Fatalf("EOF before Close: %+v", got)
	}
	for i := range stream.leftRuns {
		if stream.leftRuns[i].Values != nil || stream.rightRuns[i].Values != nil {
			t.Fatal("retained partition references")
		}
	}
	if stream.outputRuns.Values != nil {
		t.Fatal("retained output references")
	}
}

func TestJoinMetadataCancellationCleansPreparedRuns(t *testing.T) {
	ctx, stream, query := openJoinMetadataStream(t, 2<<20)
	if err := stream.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if query.Snapshot().UsedBytes == 0 {
		t.Fatal("no admitted state")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stream.Next(canceled, memory.NewBatch()); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if got := query.Snapshot(); got.UsedBytes != 0 {
		t.Fatalf("canceled state=%+v", got)
	}
}

func TestJoinMetadataClosePreservesSharedQueryStore(t *testing.T) {
	ctx, stream, query := openJoinMetadataStream(t, 2<<20)
	shared, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	var fingerprint [32]byte
	handle, err := shared.(memory.MetadataSpillManager).SpillWithMetadata(ctx, memory.SpillObject{Kind: memory.SpillJoinState, Codec: memory.ColumnarRunCodec, SchemaFingerprint: fingerprint, Payload: []byte("other operator")}, query.Operator("OtherMetadata", memory.MemoryRetainedState))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := shared.Restore(ctx, handle, fingerprint, query.Operator("OtherRestore", memory.MemorySpillBuffer))
	if err != nil {
		t.Fatalf("join closed shared store: %v", err)
	}
	if string(restored.Object.Payload) != "other operator" {
		t.Fatal("shared run changed")
	}
	_ = restored.Close()
	if err := shared.Delete(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if got := query.Snapshot(); got.UsedBytes != 0 {
		t.Fatalf("shared store cleanup: %+v", got)
	}
}
