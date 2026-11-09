package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"strings"
	"testing"
)

func decodeMismatchError(t *testing.T, fn func() error) (err error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("row-count corruption panicked after closing decoded owner: %v", recovered)
			err = fmt.Errorf("unexpected row-count panic: %v", recovered)
		}
	}()
	return fn()
}

func TestJoinDecodeRowsMismatchKeepsErrorAndOwnership(t *testing.T) {
	ctx, stream, query := openJoinMetadataStream(t, 2<<20)
	if err := stream.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	var ref joinRunRef
	for _, runs := range stream.rightRuns {
		if len(runs.Values) != 0 {
			ref = runs.Values[0]
			break
		}
	}
	if ref.handle == nil {
		t.Fatal("no prepared right run")
	}
	actual := ref.rows
	ref.rows++
	before := query.Used()
	err := decodeMismatchError(t, func() error {
		decoded, err := stream.restoreRun(ctx, stream.rightSchema, stream.rightFP, ref)
		if decoded != nil {
			decoded.Close()
			t.Error("corrupt row-count returned a decoded owner")
		}
		return err
	})
	if !errors.Is(err, memory.ErrSpillCorrupt) || !strings.Contains(fmt.Sprint(err), fmt.Sprintf("rows=%d want=%d", actual, ref.rows)) {
		t.Errorf("lost row-count corruption details: %v", err)
	}
	if query.Used() != before {
		t.Errorf("failed restore leaked bytes: before=%d after=%d", before, query.Used())
	}
	// A copied manifest reference was malformed. The real run is still owned
	// by the stream and remains readable, rather than being deleted by Restore.
	ref.rows = actual
	decoded, err := stream.restoreRun(ctx, stream.rightSchema, stream.rightFP, ref)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Close()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if query.Used() != 0 {
		t.Fatalf("close leaked %d", query.Used())
	}
}

func TestJoinDecodeRowsMismatchAfterOutputIsTerminal(t *testing.T) {
	left := []skewBudgetRow{{key: 7}, {key: 7}}
	right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}}
	ctx, owner, stream, query, dir := openByteSpill(t, skewBudgetSource(t, left, 2, 0, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 1, 0, false)}, join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
	store := installSkewBudgetStore(t, stream, query, nil)
	if err := stream.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	store.arm(stream)
	first := memory.NewBatch()
	if ok, err := stream.Next(ctx, first); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if len(stream.outputRuns.Values) == 0 {
		t.Fatal("fixture did not leave an unread output run")
	}
	ref := &stream.outputRuns.Values[0]
	actual := ref.rows
	ref.rows++
	dst := memory.NewBatch()
	err := decodeMismatchError(t, func() error {
		ok, err := stream.Next(ctx, dst)
		if ok {
			t.Error("corrupt output reported success")
		}
		return err
	})
	if !errors.Is(err, memory.ErrSpillCorrupt) || !strings.Contains(fmt.Sprint(err), fmt.Sprintf("rows=%d want=%d", actual, actual+1)) {
		t.Errorf("lost output corruption: %v", err)
	}
	if stream.terminalErr != nil {
		if ok, retry := stream.Next(context.Background(), dst); ok || retry != err {
			t.Errorf("terminal error changed: %v / %v", err, retry)
		}
		if first.Rows != 0 || dst.Rows != 0 {
			t.Error("failed Next retained borrowed output")
		}
		assertSkewBudgetReleased(t, stream, query, dir)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, query, dir)
}

func TestExternalSortDecodeRowsMismatchKeepsErrorAndOwnership(t *testing.T) {
	stream, query, ctx := openSortMetadataStream(t, 512<<10, 1)
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{9}}
	batch.Uint64s[stream.ordinal] = &memory.Uint64Vector{Values: []uint64{0}}
	ref, err := stream.spillBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	stream.workspace.trim(0)
	before := query.Used()
	ref.rows++
	err = decodeMismatchError(t, func() error {
		decoded, err := stream.restoreSegment(ctx, ref)
		if decoded != nil {
			decoded.Close()
			t.Error("corrupt sort rows returned a decoded owner")
		}
		return err
	})
	if !errors.Is(err, memory.ErrSpillCorrupt) || !strings.Contains(fmt.Sprint(err), "rows=1 want=2") {
		t.Errorf("lost sort row-count corruption: %v", err)
	}
	if stream.workspace.Used() != 0 {
		t.Errorf("failed sort restore retained live token %d", stream.workspace.Used())
	}
	stream.workspace.trim(0)
	if query.Used() != before {
		t.Errorf("failed sort restore leaked: before=%d after=%d", before, query.Used())
	}
	ref.rows = 1
	decoded, err := stream.restoreSegment(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Close()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if query.Used() != 0 {
		t.Errorf("sort close leaked %d", query.Used())
	}
}

func TestJoinDecodeRestoresCallerContainers(t *testing.T) {
	for _, end := range []string{"close", "eof", "cancel"} {
		for _, clearBeforeNext := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%t", end, clearBeforeNext), func(t *testing.T) {
				left := []skewBudgetRow{{key: 7}, {key: 7}}
				right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}}
				ctx, owner, stream, query, dir := openByteSpill(t, skewBudgetSource(t, left, 2, 0, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 1, 0, false)}, join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
				store := installSkewBudgetStore(t, stream, query, nil)
				if err := stream.prepare(ctx); err != nil {
					t.Fatal(err)
				}
				store.arm(stream)
				first, second := memory.NewBatch(), memory.NewBatch()
				originalFirst, originalSecond := *first, *second
				assertOriginal := func(dst *memory.Batch, original memory.Batch) {
					t.Helper()
					if dst.Rows != 0 {
						t.Fatalf("retained borrowed rows=%d", dst.Rows)
					}
					// Mutating the saved empty map proves that its identity, not a
					// newly allocated replacement or a decoded map, was returned.
					probe := &memory.Int64Vector{Values: []int64{42}}
					original.Int64s["probe"] = probe
					if dst.Int64s["probe"] != probe {
						t.Fatal("original used-type map was not restored")
					}
					delete(original.Int64s, "probe")
					unused := &memory.Float64Vector{Values: []float64{1.5}}
					original.Float64s["unused"] = unused
					if dst.Float64s["unused"] != unused {
						t.Fatal("original unused-type map was not restored")
					}
					delete(original.Float64s, "unused")
				}
				if ok, err := stream.Next(ctx, first); !ok || err != nil {
					t.Fatal(ok, err)
				}
				if first.Float64s == nil || stream.decoded.Batch.Float64s != nil {
					t.Fatal("sparse decoder did not preserve reusable stream destination")
				}
				if clearBeforeNext {
					first.ClearColumns()
				}
				if ok, err := stream.Next(ctx, second); !ok || err != nil {
					t.Fatal(ok, err)
				}
				assertOriginal(first, originalFirst)
				if clearBeforeNext {
					second.ClearColumns()
				}
				switch end {
				case "close":
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if ok, err := stream.Next(cancelled, second); ok || !errors.Is(err, context.Canceled) {
						t.Fatal(ok, err)
					}
				case "eof":
					for {
						ok, err := stream.Next(ctx, second)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
					}
				}
				assertOriginal(second, originalSecond)
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				assertByteSpillClean(t, owner, query, dir)
			})
		}
	}
}

func TestExternalSortDecodeMetadataIsPhaseAdmitted(t *testing.T) {
	stream, query, ctx := openSortMetadataStream(t, 512<<10, 1)
	metadata, err := memory.ColumnarRunDecodeMetadataBytes(stream.internal)
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]externalSortRun, externalSortFanIn)
	for i := range inputs {
		inputs[i] = externalSortRun{bytes: 100, maxSegmentBytes: 100, maxRowBytes: 10}
	}
	if err := stream.reserveMergeWorkspace(ctx, inputs, false); err != nil {
		t.Fatal(err)
	}
	outputMetadata, err := externalSortAccumulatorMetadataBytes(stream.internal)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(externalSortFanIn)*(100+metadata) + 3*400 + 1024 + outputMetadata + 100 + 2048
	if stream.workspace.capacity != want {
		t.Fatalf("phase admission=%d want=%d (per-cursor metadata=%d)", stream.workspace.capacity, want, metadata)
	}
	if stream.workspace.Used() != 0 {
		t.Fatal("planning allocated live decode tokens")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if query.Used() != 0 {
		t.Fatalf("phase credit leak=%d", query.Used())
	}
}
