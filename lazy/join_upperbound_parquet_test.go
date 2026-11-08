package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestFixedJoinUpperBoundParquet(t *testing.T) {
	left, lr := plainJoinParquet(t, "lid", 137)
	right, rr := plainJoinParquet(t, "rid", 75)
	for _, workers := range []int{1, 4} {
		opts := engine.DefaultOptions()
		opts.Workers = workers
		opts.MemoryLimitBytes = 32 << 20
		opts.ExecutionPolicy = engine.ExecutionMemoryOnly
		session := NewSession(opts)
		defer session.Close()
		for _, kind := range []string{"filter", "limit", "filter-limit", "filter-none", "limit0"} {
			for keys := 1; keys <= 2; keys++ {
				for typ := InnerJoin; typ <= FullJoin; typ++ {
					t.Run(fmt.Sprintf("workers%d/%s/keys%d/%s", workers, kind, keys, typ), func(t *testing.T) {
						build := session.FromSource(right)
						var ids []int
						for ri := range rr {
							if !strings.Contains(kind, "filter") || ri >= 20 {
								ids = append(ids, ri)
							}
						}
						if strings.Contains(kind, "filter") {
							build = build.Filter(expr.GE(Col("rid"), expr.I64(20)))
						}
						if strings.Contains(kind, "limit") {
							n := 15
							if kind == "limit0" {
								n = 0
							}
							build = build.Limit(int64(n))
							if len(ids) > n {
								ids = ids[:n]
							}
						}
						if kind == "filter-none" {
							build = build.Filter(expr.Bool(false))
							ids = nil
						}
						leftKeys, rightKeys := []string{"n"}, []string{"n"}
						if keys == 2 {
							leftKeys = append(leftKeys, "lid")
							rightKeys = append(rightKeys, "rid")
						}
						query := session.FromSource(left).Join(build, JoinOptions{Type: typ, LeftOn: leftKeys, RightOn: rightKeys})
						var want []string
						matched := make([]bool, len(ids))
						for li, l := range lr {
							found := false
							for j, ri := range ids {
								if l.n != rr[ri].n || (keys == 2 && li != ri) {
									continue
								}
								found = true
								matched[j] = true
								if typ == AntiJoin {
									continue
								}
								if typ == SemiJoin {
									want = append(want, fmt.Sprintf("%d/-1", li))
									break
								}
								want = append(want, fmt.Sprintf("%d/%d", li, ri))
							}
							if !found && (typ == LeftJoin || typ == FullJoin || typ == AntiJoin) {
								want = append(want, fmt.Sprintf("%d/-1", li))
							}
						}
						if typ == RightJoin || typ == FullJoin {
							for j, ri := range ids {
								if !matched[j] {
									want = append(want, fmt.Sprintf("-1/%d", ri))
								}
							}
						}
						sort.Strings(want)
						prepared, err := query.Prepare(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						defer prepared.Close()
						for repeat := 0; repeat < 2; repeat++ {
							budget, err := session.Engine().BeginQueryMemory()
							if err != nil {
								t.Fatal(err)
							}
							result, err := prepared.Execute(memory.WithQueryBudget(context.Background(), budget))
							if err != nil {
								budget.Close()
								t.Fatal(err)
							}
							var got []string
							for _, batch := range result.batches() {
								for row := 0; row < batch.Rows; row++ {
									li, ri := int64(-1), int64(-1)
									if v := batch.Int64s["lid"]; v != nil && v.IsValid(row) {
										li = v.Values[row]
									}
									if v := batch.Int64s["rid"]; v != nil && v.IsValid(row) {
										ri = v.Values[row]
									}
									got = append(got, fmt.Sprintf("%d/%d", li, ri))
								}
							}
							if err = result.Close(); err != nil {
								t.Fatal(err)
							}
							snap := budget.Snapshot()
							if snap.UsedBytes != 0 || snap.PeakBytes > budget.Limit() {
								t.Fatalf("invalid query balance: %+v", snap)
							}
							if err = budget.Close(); err != nil {
								t.Fatal(err)
							}
							sort.Strings(got)
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("repeat%d result pairs differ: got%d want%d", repeat, len(got), len(want))
							}
						}
					})
				}
			}
		}
	}
}
