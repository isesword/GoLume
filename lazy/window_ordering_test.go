package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

func TestWindowReplacementInvalidatesDownstreamOrdering(t *testing.T) {
	for _, mode := range []string{"ordered", "resident", "external"} {
		for _, batchRows := range []int{1, 3, 7} {
			t.Run(fmt.Sprintf("%s/batch-%d", mode, batchRows), func(t *testing.T) {
				base := windowReplacementInput(t, []int64{3, 1, 3, 2, 0, 2, 0}, []bool{true, true, true, true, false, true, false}, batchRows)
				session, first := windowReplacementQuery(t, base, mode)
				defer session.Close()
				query := first.WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RowNumber("rn"), Rank("rank"), DenseRank("dense"))
				prepared, err := query.Prepare(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				// Independent, fixed ranks for [3, 1, 3, 2, NULL, 2, NULL].
				// Both windows must retain the original row sequence.
				want := map[string][]int64{
					"old_id": {1, 2, 3, 4, 5, 6, 7},
					"rn":     {4, 1, 5, 2, 6, 3, 7},
					"rank":   {4, 1, 4, 2, 6, 2, 6},
					"dense":  {3, 1, 3, 2, 4, 2, 4},
				}
				for repeat := 0; repeat < 2; repeat++ {
					frame, err := prepared.Execute(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					for column, expected := range want {
						if got := frameInt64Column(t, frame, column); !equalI64(got, expected) {
							t.Errorf("%s = %v, want %v", column, got, expected)
						}
					}
					frame.Close()
				}
			})
		}
	}
}

func TestWindowReplacementInvalidatesGroupedOrdering(t *testing.T) {
	for _, mode := range []string{"ordered", "resident", "external"} {
		t.Run(mode, func(t *testing.T) {
			base := windowReplacementInput(t, []int64{2, 1, 2, 0, 2}, []bool{true, true, true, false, true}, 2)
			session, first := windowReplacementQuery(t, base, mode)
			defer session.Close()
			query := first.GroupBy("id").Agg(Count("n"), Sum("old_id", "sum"))
			frame, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer frame.Close()
			if frame.Height() != 3 {
				t.Fatalf("groups = %d, want 3; equal nonadjacent keys must form one group", frame.Height())
			}
			seen := make(map[int64]bool)
			for _, batch := range frame.batches() {
				for row := 0; row < batch.Rows; row++ {
					key := batch.Int64s["id"].Values[row]
					count, sum := uint64(1), int64(4)
					if !batch.Int64s["id"].IsValid(row) {
						key = -1
					} else if key == 1 {
						sum = 2
					} else if key == 2 {
						count, sum = 3, 9
					} else {
						t.Fatalf("unexpected group key %d", key)
					}
					if seen[key] || batch.Uint64s["n"].Values[row] != count || batch.Int64s["sum"].Values[row] != sum {
						t.Fatalf("incorrect or duplicate group %d: count=%d sum=%d", key, batch.Uint64s["n"].Values[row], batch.Int64s["sum"].Values[row])
					}
					seen[key] = true
				}
			}
		})
	}
}

func TestWindowReplacementRetainsBoundedGroupingPrefix(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int32s["group"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2}}
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2, 1, 2}}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{4, 3, 9, 7}}
	batch.Reset(4)
	base, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "group", Type: memory.TypeInt32},
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	base.WithOrdering(source.OrderingColumn{Name: "group"}, source.OrderingColumn{Name: "id", NullsLast: true})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes, opts.ExecutionPolicy = 64<<10, engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(base).
		WithWindows(NewWindow(PartitionBy("group"), OrderBy(Asc("id"))), Shift("value", 0, "id")).
		GroupBy("group").Agg(Count("n"), Sum("id", "sum"))
	prepared, err := query.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if plan := source.StreamPlanOf(prepared.src); plan.Class != source.StreamBounded || !strings.Contains(plan.Detail, "sorted-adjacent") {
		t.Fatalf("unchanged leading key lost bounded grouping: %+v", plan)
	}
	frame, err := prepared.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "sum"); !equalI64(got, []int64{7, 16}) {
		t.Fatalf("group sums = %v, want [7 16]", got)
	}
}

func TestWindowReplacementRequiresNewBoundedOrderProof(t *testing.T) {
	base := windowReplacementInput(t, []int64{3, 1, 2}, []bool{true, true, true}, 1)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes, opts.ExecutionPolicy = 64<<10, engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(base).
		WithWindows(NewWindow(nil, OrderBy(Asc("id"))), Shift("value", 0, "id")).
		WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RowNumber("rn"))
	prepared, err := query.Prepare(context.Background())
	if prepared != nil {
		prepared.Close()
	}
	var unsupported *engine.UnsupportedBoundedPlanError
	if !errors.As(err, &unsupported) || unsupported.Operator != "Window" {
		t.Fatalf("err = %v, want bounded Window capability error without spill", err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("failed prepare leaked %d bytes", used)
	}
}

func windowReplacementInput(t *testing.T, values []int64, valid []bool, batchRows int) *inmemory.Source {
	t.Helper()
	var batches []*memory.Batch
	for start := 0; start < len(values); start += batchRows {
		end := min(start+batchRows, len(values))
		batch := memory.NewBatch()
		ids := make([]int64, end-start)
		validity := memory.EnsureBitmap(nil, end-start)
		for row := range ids {
			ids[row] = int64(start + row + 1)
			if valid[start+row] {
				memory.Set(validity, row)
			}
		}
		batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
		batch.Int64s["value"] = &memory.Int64Vector{Values: append([]int64(nil), values[start:end]...), Validity: validity}
		batch.Reset(end - start)
		batches = append(batches, batch)
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return base.WithOrdering(source.OrderingColumn{Name: "id", NullsLast: true})
}

func windowReplacementQuery(t *testing.T, base source.Source, mode string) (*Session, LazyFrame) {
	t.Helper()
	var session *Session
	if mode == "external" {
		session = boundedJoinSession(t, 512<<10)
	} else {
		session = NewSession(engine.DefaultOptions())
	}
	t.Cleanup(func() {
		session.Close()
		if used := session.Engine().Memory().Used(); used != 0 {
			t.Errorf("window execution leaked %d bytes", used)
		}
	})
	order, strategy := "value", "strategy=resident"
	if mode == "ordered" {
		order, strategy = "id", "strategy=ordered-state"
	} else if mode == "external" {
		strategy = "strategy=external-sort-restore"
	}
	// old_id keeps the replaced input column live through projection pruning,
	// so these tests exercise WindowSource's metadata rather than losing it in
	// an unrelated projection. Expressions in one window read the same input.
	first := session.FromSource(base).WithWindows(NewWindow(nil, OrderBy(Asc(order))), Shift("value", 0, "id"), Shift("id", 0, "old_id"))
	plan, err := first.ExplainPhysical(context.Background())
	if err != nil || !strings.Contains(plan, strategy) {
		t.Fatalf("expected %s, err=%v, plan:\n%s", strategy, err, plan)
	}
	return session, first
}
