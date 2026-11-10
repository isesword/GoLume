package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
)

type aggregateCompactionQuotaStore struct {
	memory.SpillManager
	quotaRejects int
	writes       int
	maximum      int64
	used         func() int64
}

func (s *aggregateCompactionQuotaStore) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	handle, err := s.SpillManager.Spill(ctx, object)
	if errors.Is(err, memory.ErrSpillQuotaExceeded) {
		s.quotaRejects++
	}
	if handle != nil {
		s.writes++
	}
	s.maximum = max(s.maximum, s.used())
	return handle, err
}

func aggregateSingleGroupRunDiskBytes(t *testing.T) int64 {
	t.Helper()
	s, budget, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
	ctx := memory.WithQueryBudget(context.Background(), budget)
	if err := s.ensureTableLocked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.table.table.add(aggregateRecord{a: 0, validMask: 1}, 1, true); err != nil {
		t.Fatal(err)
	}
	if released, err := s.Spill(ctx, memory.SpillRequest{}); err != nil || released == 0 {
		t.Fatal(released, err)
	}
	var bytes int64
	files := 0
	for i := range s.partitions {
		for _, ref := range s.partitions[i].Values {
			bytes = ref.handle.Bytes()
			files++
		}
	}
	if files != 1 || bytes <= 0 {
		t.Fatal("expected one measured physical run", files, bytes)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
	return bytes
}

func TestAggregateInputCompactionQuotaIsOptionalEndToEnd(t *testing.T) {
	// Two one-row input batches spill the same group. Their two physical files
	// fit exactly; a third temporary compaction file does not. The EOF merge
	// can still delete both inputs before writing its single final output.
	perRun := aggregateSingleGroupRunDiskBytes(t)
	for _, spare := range []int64{0, perRun - 1} {
		t.Run(fmt.Sprintf("spare=%d", spare), func(t *testing.T) {
			opts := streamTestOptions()
			opts.MemoryLimitBytes = 256 << 10
			opts.QueryMemorySoftLimitBytes = 1
			opts.ExecutionPolicy = ExecutionBounded
			opts.Pipeline.MaxMorselRows = 1
			dir := t.TempDir()
			quota := 2*perRun + spare
			opts.Spill = SpillOptions{Directory: dir, MaxBytes: quota}
			eng := New(opts)
			defer eng.Close()
			src := &generatedStreamSource{rows: 2, groups: 1}
			query := streamTestQuery()
			query.Hints.ForceStrategy = ""
			prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			budget, err := eng.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			reader, err := prepared.NewSpillableAggregateStream(ctx, prepared.resultSchema(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			stream := reader.(*spillableAggregateStream)
			queryStore, err := budget.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			disk, ok := queryStore.(interface{ Used() int64 })
			if !ok {
				t.Fatal("query spill store does not expose its actual quota usage")
			}
			store := &aggregateCompactionQuotaStore{SpillManager: stream.store, used: disk.Used}
			stream.store = store
			out := memory.NewBatch()
			groups := 0
			for {
				ok, err := reader.Next(ctx, out)
				if err != nil {
					t.Fatalf("optional third-file quota admission must not abort the query: perRun=%d quota=%d err=%v", perRun, quota, err)
				}
				if !ok {
					break
				}
				if out.Rows != 1 || out.Int32s["key"].Values[0] != 0 || out.Int64s["sum"].Values[0] != 2 || out.Uint64s["n"].Values[0] != 2 {
					t.Fatal("quota fallback changed aggregate result")
				}
				groups += out.Rows
			}
			if groups != 1 || store.quotaRejects == 0 || store.writes != 3 || store.maximum > quota || src.maxBatch.Load() != 1 {
				t.Fatalf("fixture did not exercise two inputs, quota skip and final output: groups=%d quotaRejects=%d writes=%d maxDisk=%d quota=%d batch=%d", groups, store.quotaRejects, store.writes, store.maximum, quota, src.maxBatch.Load())
			}
			if stats := stream.Stats(); stats.SelectedRows != 2 || stats.Checksum != 2 || stats.Groups != 1 {
				t.Fatalf("wrong logical counters after quota fallback: %+v", stats)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, budget, dir)
		})
	}
}

func TestAggregateCompactionQuotaDoesNotHideFailures(t *testing.T) {
	quota := &memory.SpillResourceError{RequestedBytes: 1, UsedBytes: 2, LimitBytes: 2}
	ioFailure := errors.New("injected filesystem failure")
	for _, tc := range []struct {
		err  error
		skip bool
	}{
		{quota, true},
		{fmt.Errorf("optional write admission: %w", quota), true},
		{errors.Join(quota, memory.ErrBudgetExceeded), true},
		{errors.Join(quota, ioFailure), false},
		{errors.Join(quota, context.Canceled), false},
	} {
		if got := aggregateCompactionPressure(tc.err); got != tc.skip {
			t.Fatalf("error=%v skip=%v want=%v", tc.err, got, tc.skip)
		}
	}
}
