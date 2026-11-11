package physical

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestWindowInternalNamesReviewOrdinalSpill(t *testing.T) {
	for _, suffixes := range []bool{false, true} {
		for _, early := range []bool{false, true} {
			t.Run(fmt.Sprintf("suffixes=%t/early=%t", suffixes, early), func(t *testing.T) {
				manager := memory.NewEngineBudget(4 << 20)
				query, err := manager.BeginQuery(memory.BudgetOptions{SoftLimitBytes: 48 << 10, HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
				if err != nil {
					t.Fatal(err)
				}
				defer query.Close()
				ctx := memory.WithQueryBudget(context.Background(), query)
				const n = 8192
				const base = "__golume_window_ordinal"
				aliases := []string{base}
				inputs := []string{}
				if suffixes {
					inputs = []string{base, base + "_1"}
					aliases = []string{base + "_2", base + "_3"}
				}
				fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}, {Name: "v", Type: memory.TypeUint64}}
				for _, name := range inputs {
					fields = append(fields, memory.Field{Name: name, Type: memory.TypeUint64})
				}
				var batches []*memory.Batch
				for start := 0; start < n; start += 127 {
					end := min(start+127, n)
					b := memory.NewBatch()
					b.Rows = end - start
					b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
					b.Uint64s["v"] = &memory.Uint64Vector{Values: make([]uint64, b.Rows)}
					for _, name := range inputs {
						b.Uint64s[name] = &memory.Uint64Vector{Values: make([]uint64, b.Rows)}
					}
					for j := 0; j < b.Rows; j++ {
						id := n - 1 - start - j
						b.Int64s["id"].Values[j] = int64(id)
						b.Uint64s["v"].Values[j] = uint64(id * 3)
						for _, name := range inputs {
							b.Uint64s[name].Values[j] = uint64(id + 10000)
						}
					}
					batches = append(batches, b)
				}
				src, err := inmemory.New(memory.NewSchema(fields...), batches...)
				if err != nil {
					t.Fatal(err)
				}
				expressions := make([]WindowExpression, len(aliases))
				outputs := make([]memory.Field, len(aliases))
				for i, name := range aliases {
					expressions[i] = WindowExpression{Function: WindowMin, Column: "v", Alias: name, Frame: WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}}
					outputs[i] = memory.Field{Name: name, Type: memory.TypeUint64, Nullable: true}
				}
				win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, expressions, outputs, query, WindowSourceOptions{Mode: WindowExecutionExternalSort})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 16})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				restored := stream.(*externalWindowStream).child.(*dropColumnsBatchStream).state.child.(*externalSortStream)
				out := memory.NewBatch()
				defer out.ClearColumns()
				seen := 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for j := 0; j < out.Rows; j++ {
						id := n - 1 - seen - j
						if got := out.Int64s["id"].Values[j]; got != int64(id) {
							t.Fatalf("row order: got %d want %d", got, id)
						}
						for _, name := range aliases {
							v := out.Uint64s[name]
							if v == nil || !v.IsValid(j) || v.Values[j] != uint64(id*3) {
								t.Fatalf("output %s row %d: %+v", name, id, v)
							}
						}
						for _, name := range inputs {
							if got := out.Uint64s[name].Values[j]; got != uint64(id+10000) {
								t.Fatalf("input %s changed: %d", name, got)
							}
						}
					}
					seen += out.Rows
					if early {
						break
					}
				}
				if !early && seen != n {
					t.Fatalf("rows=%d", seen)
				}
				if query.Snapshot().SpilledBytes == 0 || restored.spillRuns < 2 {
					t.Fatalf("not multi-run spill: runs=%d snapshot=%+v", restored.spillRuns, query.Snapshot())
				}
				checkSpillOutputCleanup(t, stream, query)
			})
		}
	}
}
