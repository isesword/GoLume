package physical

import (
	"context"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

// unstableReplaySource wraps a source without forwarding any replay contract,
// so ReplayabilityOf reports ReplayUnknown even when the wrapped source is a
// stable snapshot.
type unstableReplaySource struct{ inner source.Source }

// Schema forwards the wrapped schema.
func (s unstableReplaySource) Schema() memory.Schema { return s.inner.Schema() }

// Stats forwards the wrapped statistics.
func (s unstableReplaySource) Stats(ctx context.Context) (source.Stats, error) {
	return s.inner.Stats(ctx)
}

// Partitions forwards the wrapped partition list.
func (s unstableReplaySource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.inner.Partitions(ctx)
}

// NewBatchStream forwards stream creation without declaring replay stability.
func (s unstableReplaySource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return source.OpenBatchStream(ctx, s.inner, opts)
}

func TestSortAndTopKForwardInputReplayability(t *testing.T) {
	base := boolOrderTestSource(t)
	unstable := unstableReplaySource{inner: base}
	for _, tt := range []struct {
		name  string
		input source.Source
		want  source.Replayability
	}{
		{name: "sort over stable input", input: base, want: source.ReplayStable},
		{name: "sort over unknown input", input: unstable, want: source.ReplayUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sorted, err := NewSortSource(tt.input, []SortKey{{Column: "key"}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := source.ReplayabilityOf(sorted); got != tt.want {
				t.Fatalf("sort replayability=%s want=%s", got, tt.want)
			}
			top, err := NewTopKSourceWithManager(tt.input, 3, []SortKey{{Column: "key"}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := source.ReplayabilityOf(top); got != tt.want {
				t.Fatalf("top-k replayability=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestTopKOverSortedInput(t *testing.T) {
	base := boolOrderTestSource(t)
	sorted, err := NewSortSource(base, []SortKey{{Column: "key"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	top, err := NewTopKSourceWithManager(sorted, 3, []SortKey{{Column: "key"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := collectBatch(t, top)
	defer out.ReleaseOwned()
	ids, err := out.Int64("id")
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{3, 1, 6}; !equalInt64s(ids.Values, want) {
		t.Fatalf("ids=%v want=%v", ids.Values, want)
	}
	checkBoolColumn(t, out, "b", []uint32{0, 0, 1}, []bool{true, true, true})
}

func TestTopKOverNonReplayableInputMaterializesOnce(t *testing.T) {
	base := boolOrderTestSource(t)
	stable, err := NewTopKSourceWithManager(base, 3, []SortKey{{Column: "key"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := func() []int64 {
		out := collectBatch(t, stable)
		defer out.ReleaseOwned()
		ids, err := out.Int64("id")
		if err != nil {
			t.Fatal(err)
		}
		return append([]int64(nil), ids.Values...)
	}()
	unstable, err := NewTopKSourceWithManager(unstableReplaySource{inner: base}, 3, []SortKey{{Column: "key"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := collectBatch(t, unstable)
	defer out.ReleaseOwned()
	ids, err := out.Int64("id")
	if err != nil {
		t.Fatal(err)
	}
	if !equalInt64s(ids.Values, wantIDs) {
		t.Fatalf("non-replayable top-k ids=%v want=%v", ids.Values, wantIDs)
	}
	checkBoolColumn(t, out, "b", []uint32{0, 0, 1}, []bool{true, true, true})
}

func TestTopKNonReplayableHonorsMemoryBudget(t *testing.T) {
	base := boolOrderTestSource(t)
	manager := memory.NewBudgetManager(64)
	top, err := NewTopKSourceWithManager(unstableReplaySource{inner: base}, 3, []SortKey{{Column: "key"}}, manager)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	stream, err := source.OpenBatchStream(ctx, top, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Next(ctx, memory.NewBatch())
	if err == nil {
		t.Fatal("non-replayable top-k under a tiny budget succeeded")
	}
	if cerr := stream.Close(); cerr != nil {
		t.Fatal(cerr)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("memory used=%d after failed top-k, want 0", used)
	}
}
