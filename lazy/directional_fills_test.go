package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sort"
	"testing"
)

func TestDirectionalFillsStrategiesStableTies(t *testing.T) {
	for _, strategy := range []string{"resident", "ordered", "external"} {
		t.Run(strategy, func(t *testing.T) {
			ids := []int64{1, 0, 0, 1, 0, 2, 2, 1}
			groups := []int64{0, 1, 0, 1, 0, 1, 0, 0}
			values := []int64{0, 0, 7, 8, 0, 0, 9, 0}
			valid := []bool{false, false, true, true, false, false, true, false}
			order := make([]int, len(ids))
			for i := range order {
				order[i] = i
			}
			if strategy == "ordered" {
				sort.SliceStable(order, func(i, j int) bool {
					a, b := order[i], order[j]
					if groups[a] != groups[b] {
						return groups[a] < groups[b]
					}
					return ids[a] < ids[b]
				})
			}
			b := memory.NewBatch()
			b.Rows = len(ids)
			for _, name := range []string{"id", "g", "v", "__golume_window_future_1"} {
				b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
			}
			b.Int64s["v"].Validity = memory.EnsureBitmap(nil, b.Rows)
			for row, p := range order {
				b.Int64s["id"].Values[row] = ids[p]
				b.Int64s["g"].Values[row] = groups[p]
				b.Int64s["v"].Values[row] = values[p]
				b.Int64s["__golume_window_future_1"].Values[row] = 42
				if valid[p] {
					memory.Set(b.Int64s["v"].Validity, row)
				}
			}
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "__golume_window_future_1", Type: memory.TypeInt64})
			raw, err := inmemory.New(schema, b)
			if err != nil {
				t.Fatal(err)
			}
			var src source.Source = raw
			var session *Session
			switch strategy {
			case "external":
				session = boundedJoinSession(t, 1<<20)
			case "ordered":
				opts := engine.DefaultOptions()
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 1 << 20
				session = NewSession(opts)
				src = &orderedWindowTestSource{Source: raw, ordering: []source.OrderingColumn{{Name: "g"}, {Name: "id", NullsLast: true}}}
			default:
				session = NewSession(engine.DefaultOptions())
			}
			defer session.Close()
			query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("g"), OrderBy(Asc("id"))), ForwardFill("v", "f"), BackwardFill("v", "v"), Shift("v", -1, "next"), Shift("v", 1, "past"))
			result, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			sorted := append([]int(nil), order...)
			sort.SliceStable(sorted, func(i, j int) bool {
				a, b := sorted[i], sorted[j]
				if groups[a] != groups[b] {
					return groups[a] < groups[b]
				}
				return ids[a] < ids[b]
			})
			for _, name := range []string{"f", "v", "next", "past"} {
				got, gvalid := frameInt64WithValidity(t, result, name)
				for row, p := range order {
					pos := 0
					for sorted[pos] != p {
						pos++
					}
					target := -1
					if name == "next" {
						if pos+1 < len(sorted) && groups[sorted[pos+1]] == groups[p] {
							target = sorted[pos+1]
						}
					} else if name == "past" {
						if pos > 0 && groups[sorted[pos-1]] == groups[p] {
							target = sorted[pos-1]
						}
					} else {
						step := -1
						if name == "v" {
							step = 1
						}
						for j := pos; j >= 0 && j < len(sorted) && groups[sorted[j]] == groups[p]; j += step {
							if valid[sorted[j]] {
								target = sorted[j]
								break
							}
						}
					}
					wantValid := target >= 0 && valid[target]
					if gvalid[row] != wantValid || (wantValid && got[row] != values[target]) {
						t.Fatalf("%s row%d got %d/%v target%d", name, row, got[row], gvalid[row], target)
					}
				}
			}
		})
	}
}
