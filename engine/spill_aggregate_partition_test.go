package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
)

func TestAggregatePartitionForcedSpillExactlyOnce(t *testing.T) {
	const rows, groups = 32768, 8192
	s, budget, dir := aggregateMetadataStream(t, 1<<20, rows, groups)
	s.outputRows = 513
	ctx := memory.WithQueryBudget(context.Background(), budget)
	out := memory.NewBatch()
	seen := make([]bool, groups)
	count := 0
	for {
		ok, err := s.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i, key := range out.Int32s["key"].Values {
			if key < 0 || int(key) >= groups || seen[key] {
				t.Fatalf("invalid or duplicate group %d", key)
			}
			seen[key] = true
			count++
			if out.Int64s["sum"].Values[i] != rows/groups || out.Uint64s["n"].Values[i] != rows/groups {
				t.Fatalf("group %d lost or duplicated rows", key)
			}
		}
	}
	if count != groups || s.Stats().SpillRuns == 0 {
		t.Fatalf("groups=%d spill runs=%d", count, s.Stats().SpillRuns)
	}
	if ok, err := s.Next(ctx, out); ok || err != nil {
		t.Fatalf("EOF retry: %v %v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, budget, dir)
}

type failAggregatePartitionStore struct {
	memory.SpillManager
	calls, fail int
	err         error
}

func (s *failAggregatePartitionStore) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	s.calls++
	if s.calls == s.fail {
		return nil, s.err
	}
	return s.SpillManager.Spill(ctx, object)
}

func TestAggregatePartitionWriteFailureRetry(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		for _, fail := range []int{1, 8, 16} {
			t.Run(fmt.Sprintf("seeded=%v/fail=%d", seeded, fail), func(t *testing.T) {
				s, budget, dir := aggregateMetadataStream(t, 4<<20, 512, 512)
				ctx := memory.WithQueryBudget(context.Background(), budget)
				populate := func() {
					if err := s.ensureTableLocked(ctx); err != nil {
						t.Fatal(err)
					}
					for key := uint64(0); key < 512; key++ {
						if _, err := s.table.table.add(aggregateRecord{a: key, validMask: 1}, int64(key+1), true); err != nil {
							t.Fatal(err)
						}
					}
				}
				if seeded {
					populate()
					if _, err := s.Spill(ctx, memory.SpillRequest{}); err != nil {
						t.Fatal(err)
					}
				}
				prior := make([]string, len(s.partitions))
				for i, partition := range s.partitions {
					if len(partition.Values) > 0 {
						prior[i] = partition.Values[0].handle.ID()
					}
				}
				populate()
				injected := errors.New("injected partition write error")
				failing := &failAggregatePartitionStore{SpillManager: s.store, fail: fail, err: injected}
				s.store = failing
				if _, err := s.Spill(ctx, memory.SpillRequest{}); !errors.Is(err, injected) {
					t.Fatalf("spill error=%v calls=%d", err, failing.calls)
				}
				if s.table == nil || s.table.table.len() != 512 || s.encoder.Used() != 0 {
					t.Fatal("failed write changed authoritative table or leaked directory/buffer")
				}
				for i, partition := range s.partitions {
					if prior[i] == "" {
						if len(partition.Values) != 0 {
							t.Fatal("failed write published a partial run")
						}
					} else if len(partition.Values) != 1 || partition.Values[0].handle.ID() != prior[i] {
						t.Fatal("failed write changed earlier runs")
					}
				}
				failing.fail = 0
				if _, err := s.Spill(ctx, memory.SpillRequest{}); err != nil {
					t.Fatal(err)
				}
				seen := make([]int, 512)
				for _, partition := range s.partitions {
					for _, run := range partition.Values {
						if err := s.restoreRun(ctx, run, func(record aggregateRecord) error {
							if record.a >= 512 || record.state.Sum != int64(record.a+1) || record.state.CountRows != 1 {
								return fmt.Errorf("invalid restored group %+v", record)
							}
							seen[record.a]++
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
				}
				want := 1
				if seeded {
					want++
				}
				for key, count := range seen {
					if count != want {
						t.Fatalf("group %d appears %d times, want %d", key, count, want)
					}
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				assertAggregateMetadataClean(t, budget, dir)
			})
		}
	}
}
