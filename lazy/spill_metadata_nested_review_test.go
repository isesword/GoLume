package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"os"
	"strings"
	"testing"
)

func TestNestedAggregateSortMetadataOwnershipReview(t *testing.T) {
	for _, mode := range []string{"eof", "early-close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.MemoryLimitBytes = 4 << 20
			opts.QueryMemorySoftLimitBytes = 96 << 10
			dir := t.TempDir()
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
			opts.ExecutionPolicy = engine.ExecutionBounded
			session := NewSession(opts)
			defer session.Close()
			b, err := session.Engine().BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			ctx := memory.WithQueryBudget(context.Background(), b)
			store, err := b.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := store.(memory.MetadataSpillManager).SpillWithMetadata(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: "sibling-review", Payload: []byte("keep")}, b.Operator("SiblingMetadata", memory.MemoryRetainedState))
			if err != nil {
				t.Fatal(err)
			}
			frame := session.FromSource(spillAggregateInput(t, 8, 512, 1024)).GroupBy("key").Agg(Count("n")).Sort(Asc("key"))
			plan, err := frame.ExplainPhysical(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plan, "SPILLABLE Aggregate") || !strings.Contains(plan, "external") {
				t.Fatalf("not nested spilling operators:\n%s", plan)
			}
			reader, err := frame.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if !reader.Next(ctx) {
				t.Fatal(reader.Err())
			}
			if reader.budget != b || reader.ownedBudget {
				t.Fatal("nested pipeline did not borrow query budget")
			}
			snapshot := b.Snapshot()
			for _, name := range []string{"HashAggregateMetadata", "ExternalSortMetadata"} {
				if _, ok := snapshot.Operators[name]; !ok {
					t.Fatalf("missing nested operator %s: %+v", name, snapshot)
				}
			}
			rows, total := 0, int64(0)
			count := func() {
				rows += reader.batch.Rows
				for _, n := range reader.batch.Uint64s["n"].Values {
					total += int64(n)
				}
			}
			count()
			switch mode {
			case "eof":
				for reader.Next(ctx) {
					count()
				}
				if err := reader.Err(); err != nil {
					t.Fatal(err)
				}
				if rows != 1024 || total != 4096 {
					t.Fatalf("groups=%d rows=%d", rows, total)
				}
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if reader.Next(canceled) || !errors.Is(reader.Err(), context.Canceled) {
					t.Fatalf("cancel: %v", reader.Err())
				}
			}
			if err := reader.Close(); err != nil && !(mode == "cancel" && errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
			restored, err := store.Restore(ctx, sibling, [32]byte{}, b.Operator("SiblingRead", memory.MemorySpillBuffer))
			if err != nil {
				t.Fatalf("nested cleanup damaged shared query store: %v", err)
			}
			restored.Close()
			if err := store.Delete(ctx, sibling); err != nil {
				t.Fatal(err)
			}
			if b.Used() != 0 || b.Snapshot().PeakBytes > b.Limit() {
				t.Fatalf("nested metadata accounting: %+v", b.Snapshot())
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			files, err := os.ReadDir(dir)
			if err != nil || len(files) != 0 {
				t.Fatalf("spill leftovers: %v %v", files, err)
			}
		})
	}
}
