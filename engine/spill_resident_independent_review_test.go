package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"testing"
)

func TestIndependentResidentSinkRetry(t *testing.T) {
	for _, accepted := range []int{0, 1, 15, 31, 63} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			s, b, dir := aggregateMetadataStream(t, 1<<20, 256, 64)
			s.outputRows = 7
			ctx := memory.WithQueryBudget(context.Background(), b)
			seen := map[uint64]bool{}
			failed := false
			stats, err := s.consumeRecords(ctx, func(r aggregateRecord) error {
				if len(seen) == accepted && !failed {
					failed = true
					return memory.ErrBudgetExceeded
				}
				if seen[r.a] {
					return fmt.Errorf("duplicate %d", r.a)
				}
				seen[r.a] = true
				if r.state.Sum != 4 {
					return fmt.Errorf("bad sum")
				}
				return nil
			})
			if err != nil || len(seen) != 64 || stats.Groups != 64 {
				t.Fatalf("seen=%d stats=%+v err=%v", len(seen), stats, err)
			}
			assertAggregateMetadataClean(t, b, dir)
		})
	}
}
func TestIndependentResidentSuffixPublishFailure(t *testing.T) {
	for fail := 1; fail <= 5; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s, b, dir := aggregateMetadataStream(t, 1<<20, 256, 64)
			s.outputRows = 7
			ctx := memory.WithQueryBudget(context.Background(), b)
			out := memory.NewBatch()
			seen := map[int32]bool{}
			check := func() {
				for _, k := range out.Int32s["key"].Values {
					if seen[k] {
						t.Fatalf("duplicate %d", k)
					}
					seen[k] = true
				}
			}
			if ok, err := s.Next(ctx, out); !ok || err != nil {
				t.Fatal(ok, err)
			}
			check()
			cursor, emitted := s.outputCursor, s.outputEmitted
			m := &failNthMetadataReview{Manager: s.metadata, fail: fail}
			s.metadata = m
			if _, err := s.Spill(ctx, memory.SpillRequest{}); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("fail=%d calls=%d err=%v", fail, m.n, err)
			}
			if s.table == nil || s.outputCursor != cursor || s.outputEmitted != emitted || len(s.outputRuns) != 0 {
				t.Fatal("failed transition mutated authority")
			}
			if _, err := s.Spill(ctx, memory.SpillRequest{}); err != nil {
				t.Fatal(err)
			}
			for {
				ok, err := s.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				check()
			}
			if len(seen) != 64 {
				t.Fatal(len(seen))
			}
			assertAggregateMetadataClean(t, b, dir)
		})
	}
}
