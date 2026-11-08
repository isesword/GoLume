package engine

import (
	"context"
	"errors"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"testing"
)

type aggregateCleanupErrorStore struct {
	*memory.QuerySpillStore
	firstID string
	failAll bool
	calls   int
	failure error
}

func (s *aggregateCleanupErrorStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	s.calls++
	if s.failAll || h.ID() == s.firstID {
		return s.failure
	}
	return s.QuerySpillStore.Delete(ctx, h)
}

func TestAggregateCleanupRetainsOnlyFirstError(t *testing.T) {
	for _, operation := range []string{"refs", "close"} {
		for _, mode := range []string{"first-only", "all"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				s, query, dir := aggregateMetadataStream(t, 1<<20, 1, 1)
				ctx := memory.WithQueryBudget(context.Background(), query)
				base, err := query.SpillManager()
				if err != nil {
					t.Fatal(err)
				}
				if err = s.store.Close(); err != nil {
					t.Fatal(err)
				}
				failure := errors.New("injected aggregate cleanup failure")
				failing := &aggregateCleanupErrorStore{QuerySpillStore: base.(*memory.QuerySpillStore), failAll: mode == "all", failure: failure}
				s.store = spillmeta.NewStore(failing, s.metadata)
				var refs spillmeta.Slice[aggregateRunRef]
				defer refs.Release()
				const runs = 64
				for i := 0; i < runs; i++ {
					h, err := s.store.Spill(ctx, memory.SpillObject{Kind: memory.SpillAggregateState, Codec: "cleanup-v1", Payload: []byte("row")})
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						failing.firstID = h.ID()
					}
					ref := aggregateRunRef{handle: h, groups: 1, partition: i % len(s.partitions)}
					if operation == "refs" {
						err = refs.Append(ctx, s.metadata, ref)
					} else if i < 48 {
						err = s.partitions[ref.partition].Append(ctx, s.metadata, ref)
					} else {
						err = s.outputRefs.Append(ctx, s.metadata, ref)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if operation == "refs" {
					if err := s.deleteRunRefsStrict(ctx, refs.Values); err != failure {
						t.Errorf("run cleanup retained wrapped error tree: %T", err)
					}
					if failing.calls != runs {
						t.Errorf("delete attempts=%d want=%d", failing.calls, runs)
					}
					refs.Release()
				} else {
					s.outputRuns = s.outputRefs.Values
				}
				if err := s.Close(); err != failure {
					t.Errorf("Close retained wrapped error tree: %T", err)
				}
				if s.closeErr != failure {
					t.Errorf("saved close error=%T want first failure", s.closeErr)
				}
				remaining := 1
				if mode == "all" {
					remaining = runs
				}
				if failing.calls != runs+remaining {
					t.Errorf("delete and retry attempts=%d want=%d", failing.calls, runs+remaining)
				}
				if err := s.Close(); err != failure {
					t.Errorf("repeat Close changed first failure: %T", err)
				}
				if query.Used() == 0 {
					t.Fatal("failed files lost query accounting")
				}
				if err := query.Close(); err != nil {
					t.Fatal(err)
				}
				assertAggregateMetadataClean(t, query, dir)
			})
		}
	}
}
