package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
)

type failNthMetadataReview struct {
	memory.Manager
	n, fail int
}

func (m *failNthMetadataReview) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.n++
	if m.n == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestAggregateSpillPublishAtomicReview(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		for fail := 1; fail <= 6; fail++ {
			t.Run(fmt.Sprintf("seeded=%t/fail=%d", seeded, fail), func(t *testing.T) {
				s, b, dir := aggregateMetadataStream(t, 1<<20, 3, 3)
				ctx := memory.WithQueryBudget(context.Background(), b)
				populate := func() {
					if err := s.ensureTableLocked(ctx); err != nil {
						t.Fatal(err)
					}
					for i := uint64(1); i <= 3; i++ {
						if _, err := s.table.table.add(aggregateRecord{a: i, validMask: 1}, 1, true); err != nil {
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
				before := make(map[int]string)
				for i, p := range s.partitions {
					if len(p.Values) > 0 {
						before[i] = p.Values[0].handle.ID()
					}
				}
				populate()
				m := &failNthMetadataReview{Manager: s.metadata, fail: fail}
				s.metadata = m
				if _, err := s.Spill(ctx, memory.SpillRequest{}); !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("want injected error, got %v (%d calls)", err, m.n)
				}
				n := 0
				for i, p := range s.partitions {
					n += len(p.Values)
					if id, ok := before[i]; ok && (len(p.Values) != 1 || p.Values[0].handle.ID() != id) {
						t.Fatal("rollback changed prior runs")
					}
				}
				if n != len(before) || s.table == nil || s.table.table.len() != 3 {
					t.Fatalf("failed spill changed published run count: got=%d prior=%d", n, len(before))
				}
				if _, err := s.Spill(ctx, memory.SpillRequest{}); err != nil {
					t.Fatal(err)
				}
				n = 0
				for _, p := range s.partitions {
					n += len(p.Values)
				}
				if n != len(before)+3 || s.table != nil {
					t.Fatalf("retry published wrong run count: %d", n)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				assertAggregateMetadataClean(t, b, dir)
			})
		}
	}
}
