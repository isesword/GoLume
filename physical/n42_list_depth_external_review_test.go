package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"testing"
)

func TestN42ReviewMetadataPreflightMutableSource(t *testing.T) {
	for _, kind := range []string{"missing-child", "too-deep", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			s := n40ReviewFixture(false)
			m := memory.NewEngineBudget(4 << 20)
			op, e := NewSortSourceWithOptions(s, []SortKey{{Column: "k"}}, m, SortSourceOptions{Spillable: true})
			if e != nil {
				t.Fatal(e)
			}
			dir := t.TempDir()
			q, e := m.BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 8 << 20}})
			if e != nil {
				t.Fatal(e)
			}
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			if kind == "missing-child" {
				s.schema.Fields[1].Children[0].Children = nil
			} else if kind == "too-deep" {
				f := s.schema.Fields[1]
				for i := 0; i < 16; i++ {
					f = memory.Field{Name: "v", Type: memory.TypeStruct, Children: []memory.Field{f}}
				}
				s.schema.Fields[1] = f
			} else {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			st, e := source.OpenBatchStream(ctx, op, testReadOptions())
			if e == nil {
				st.Close()
				t.Fatal("bad mutable schema accepted")
			}
			if kind == "cancel" && !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			entries, de := os.ReadDir(dir)
			if de != nil || len(entries) != 0 || s.reads != 0 || q.Used() != 0 || q.Snapshot().PeakBytes != 0 {
				t.Fatal("preflight side effects", de, len(entries), s.reads, q.Used(), q.Snapshot().PeakBytes)
			}
			if kind == "too-deep" {
				if _, e = externalSortSchemaMetadataBytes(s.schema.Fields); e == nil {
					t.Fatal("counting helper did not bound schema depth")
				}
			}
		})
	}
}
func TestN42ReviewCapturedSchemaOwnsChildren(t *testing.T) {
	s := n40ReviewFixture(false)
	m := memory.NewEngineBudget(4 << 20)
	op, e := NewSortSourceWithOptions(s, []SortKey{{Column: "k"}}, m, SortSourceOptions{Spillable: true})
	if e != nil {
		t.Fatal(e)
	}
	q, e := m.BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := source.OpenBatchStream(ctx, op, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	s.schema.Fields[1].Children[0].Name = "poison"
	s.schema.Fields[1].Children[0].Children = nil
	out := memory.NewBatch()
	count := 0
	for {
		ok, e := st.Next(ctx, out)
		if e != nil {
			st.Close()
			t.Fatal("execution reread source metadata", e)
		}
		if !ok {
			break
		}
		for r := 0; r < out.Rows; r++ {
			if out.Arrays["v"].Child.Child.Child.Int64.Values[r] != []int64{10, 11, 20}[count] {
				t.Fatal("values")
			}
			count++
		}
	}
	st.Close()
	if count != 3 || q.Used() != 0 {
		t.Fatal("captured schema cleanup", count, q.Used())
	}
}
