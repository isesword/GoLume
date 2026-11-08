package physical

import (
	"context"
	"errors"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"testing"
)

type cleanupErrorStore struct {
	*memory.QuerySpillStore
	firstID string
	failAll bool
	calls   int
	failure error
}

func (s *cleanupErrorStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	s.calls++
	if s.failAll || h.ID() == s.firstID {
		return s.failure
	}
	return s.QuerySpillStore.Delete(ctx, h)
}

func TestSpillCleanupRetainsOnlyFirstError(t *testing.T) {
	for _, operator := range []string{"sort", "join"} {
		for _, mode := range []string{"first-only", "all"} {
			t.Run(operator+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				base, err := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				defer base.Close()
				failure := errors.New("injected cleanup failure")
				failing := &cleanupErrorStore{QuerySpillStore: base, failAll: mode == "all", failure: failure}
				manager := memory.NewBudgetManager(1 << 20)
				store := spillmeta.NewStore(failing, manager)
				defer store.Close()
				var sortRun externalSortRun
				var joinRefs spillmeta.Slice[joinRunRef]
				defer sortRun.segments.Release()
				defer joinRefs.Release()
				const runs = 64
				for i := 0; i < runs; i++ {
					h, err := store.Spill(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: "cleanup-v1", Payload: []byte("row")})
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						failing.firstID = h.ID()
					}
					if operator == "sort" {
						err = sortRun.segments.Append(ctx, manager, sortSegmentRef{handle: h, rows: 1})
					} else {
						err = joinRefs.Append(ctx, manager, joinRunRef{handle: h, rows: 1})
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if operator == "sort" {
					err = (&externalSortStream{store: store}).deleteRun(ctx, sortRun)
				} else {
					err = (&spillableJoinStream{store: store}).deleteRefsStrict(ctx, joinRefs.Values)
				}
				if err != failure {
					t.Errorf("cleanup retained a wrapped error tree: got %T, want original first failure", err)
				}
				if failing.calls != runs {
					t.Errorf("delete attempts=%d want=%d", failing.calls, runs)
				}
				sortRun.segments.Release()
				joinRefs.Release()
				if err := store.Close(); err != failure {
					t.Errorf("private Close error=%v", err)
				}
				remaining := 1
				if mode == "all" {
					remaining = runs
				}
				if failing.calls != runs+remaining {
					t.Errorf("retry attempts=%d want=%d", failing.calls, runs+remaining)
				}
				if manager.Used() == 0 || base.Used() == 0 {
					t.Fatal("failed files lost query ownership/accounting")
				}
				if err := base.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 || base.Used() != 0 {
					t.Fatal("query cleanup leaked resources")
				}
			})
		}
	}
}
