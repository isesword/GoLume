package physical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

func TestReviewGenericFilteredBuild(t *testing.T) {
	src := genericJoinSource(t, []genericJoinRow{{s: "a", n: 1}, {s: "b", n: 2}}, false, 1)
	f, err := NewFilterSource(src, expr.EQ(expr.Col("n"), expr.I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLimitSource(src, 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, right := range map[string]source.Source{"filter": f, "limit": l} {
		t.Run(name, func(t *testing.T) {
			owner, err := NewJoinSource(context.Background(), src, right, join.Inner, []JoinKey{{"s", "s"}}, "", memory.NewBudgetManager(1<<20), kernel.New(kernel.ModeScalar))
			if err != nil {
				t.Fatalf("valid upper-bound build rejected: %v", err)
			}
			defer owner.Close()
			got, _ := collectGenericJoin(t, owner)
			if len(got) != 1 || got[0] != [2]int64{0, 0} {
				t.Fatal(got)
			}
		})
	}
}
