package physical

import (
	"context"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

// This regression deliberately uses only pre-existing operator APIs so it also
// reproduces the legacy Spill bypass on the pre-fix tree.
func TestOperatorSpillIncludesStoreMetadata(t *testing.T) {
	for _, name := range []string{"sort", "join"} {
		t.Run(name, func(t *testing.T) {
			const hard = 1 << 20
			budget := memory.NewEngineBudget(hard)
			query, err := budget.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			defer query.Close()
			ctx := memory.WithQueryBudget(context.Background(), query)
			if name == "sort" {
				owner, err := NewSortSourceWithOptions(externalSortTestSource(t, 1, 1), []SortKey{{Column: "key"}}, budget, SortSourceOptions{Spillable: true})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := newExternalSortStream(ctx, owner.(*SortSource), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				b := memory.NewBatch()
				b.Rows = 1
				b.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
				b.Int64s["id"] = &memory.Int64Vector{Values: []int64{1}}
				b.Uint64s[stream.ordinal] = &memory.Uint64Vector{Values: []uint64{0}}
				before := query.Used()
				if _, err = stream.spillBatch(ctx, b); err != nil {
					t.Fatal(err)
				}
				if delta := query.Used() - before; delta < 1024 {
					t.Fatalf("spill admitted only %d bytes: handle/path/map metadata bypassed", delta)
				}
			} else {
				kernels := kernel.New(kernel.ModeScalar)
				owner, err := NewJoinSourceWithOptions(ctx, spillNarrowSource(t, 0, 1, 1), spillNarrowSource(t, 0, 1, 1), join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "_r", budget, JoinSourceOptions{Kernels: kernels, RadixBits: 4, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: hard})
				if err != nil {
					t.Fatal(err)
				}
				defer owner.Close()
				stream, err := newSpillableJoinStream(ctx, owner, source.ReadOptions{Kernels: kernels})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				before := query.Used()
				if _, err = stream.spillTracked(ctx, memory.SpillObject{Kind: memory.SpillJoinState, Codec: "contract-v1", Payload: []byte("row")}, 1, 0); err != nil {
					t.Fatal(err)
				}
				if delta := query.Used() - before; delta < 1024 {
					t.Fatalf("spill admitted only %d bytes: handle/path/map metadata bypassed", delta)
				}
			}
		})
	}
}
