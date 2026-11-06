package engine

import (
	"context"
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestAggregateStreamPredicateQueryIdentity(t *testing.T) {
	for _, mode := range []string{"resident", "sorted", "spill"} {
		for _, advance := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/first", true: "/later"}[advance], func(t *testing.T) {
				eng, _, q1, stream := outputLifecycleStream(t, mode, 17, 1)
				q2, err := eng.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer q2.Close()
				if advance {
					if ok, err := stream.Next(context.Background(), memory.NewBatch()); err != nil || !ok {
						t.Fatalf("initial next=%v %v", ok, err)
					}
				}
				_, err = stream.Next(memory.WithQueryBudget(context.Background(), q2), memory.NewBatch())
				if err == nil || !strings.Contains(err.Error(), "query ownership") {
					t.Fatalf("error=%v", err)
				}
				stream.Close()
				if q1.Used() != 0 || q2.Used() != 0 {
					t.Fatalf("q1=%d q2=%d", q1.Used(), q2.Used())
				}
			})
		}
		t.Run(mode+"/same-and-plain", func(t *testing.T) {
			_, _, budget, stream := outputLifecycleStream(t, mode, 17, 1)
			for _, ctx := range []context.Context{context.Background(), memory.WithQueryBudget(context.Background(), budget), context.Background()} {
				if ok, err := stream.Next(ctx, memory.NewBatch()); err != nil || !ok {
					t.Fatalf("next=%v %v", ok, err)
				}
			}
			stream.Close()
			if budget.Used() != 0 {
				t.Fatalf("used=%d", budget.Used())
			}
		})
	}
}
