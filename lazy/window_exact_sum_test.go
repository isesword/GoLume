package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"strings"
	"testing"
)

func TestWindowExactPublicStrategies(t *testing.T) {
	for _, strategy := range []string{"resident", "ordered-state", "external-sort-restore"} {
		t.Run(strategy, func(t *testing.T) {
			values := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1, 2, 3, 1e16, 1, 1, 0}
			batch := memory.NewBatch()
			batch.Rows = len(values)
			ids := make([]int64, len(values))
			for i := range ids {
				ids[i] = int64(i)
			}
			batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
			batch.Float64s["v"] = &memory.Float64Vector{Values: values}
			schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64})
			input, err := inmemory.New(schema, batch)
			if err != nil {
				t.Fatal(err)
			}
			var src source.Source = input
			var session *Session
			if strategy == "resident" {
				session = NewSession(engine.DefaultOptions())
			} else {
				session = boundedJoinSession(t, 256<<10)
				if strategy == "ordered-state" {
					src = &orderedWindowTestSource{Source: src, ordering: []source.OrderingColumn{{Name: "id", NullsLast: true}}}
				}
			}
			defer session.Close()
			frame := RowsBetween(Preceding(1), CurrentRow)
			query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RollingSum("v", frame, "s"), RollingAvg("v", frame, "a"))
			plan, err := query.ExplainPhysical(context.Background())
			if err != nil || !strings.Contains(plan, "strategy="+strategy) {
				t.Fatalf("strategy=%s err=%v plan=%s", strategy, err, plan)
			}
			result, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			for _, name := range []string{"s", "a"} {
				got := frameFloat64Column(t, result, name)
				if len(got) != len(values) {
					t.Fatal(len(got))
				}
				for row := range values {
					rational := new(big.Rat)
					lo := max(0, row-1)
					for j := lo; j <= row; j++ {
						rational.Add(rational, new(big.Rat).SetFloat64(values[j]))
					}
					if name == "a" {
						rational.Quo(rational, new(big.Rat).SetInt64(int64(row-lo+1)))
					}
					want, _ := rational.Float64()
					if math.Float64bits(got[row]) != math.Float64bits(want) {
						t.Fatal(fmt.Sprintf("%s row%d got=%g want=%g", name, row, got[row], want))
					}
				}
			}
			if err := result.Close(); err != nil {
				t.Fatal(err)
			}
			session.Close()
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("leaked %d bytes", used)
			}
		})
	}
}
