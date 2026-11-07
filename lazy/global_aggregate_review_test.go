package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestGlobalAggregateReviewComposition(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{2, 3, 7}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.Options{MemoryLimitBytes: 1 << 20})
	defer s.Close()
	base := s.FromSource(src)
	cases := []struct {
		name string
		f    LazyFrame
		rows int64
		col  string
		want uint64
	}{
		{"count-only", base.Agg(Count("n")), 1, "n", 3},
		{"count-after-limit", base.Limit(2).Agg(Count("n")), 1, "n", 2},
		{"count-after-empty-limit", base.Limit(0).Agg(Count("n")), 1, "n", 0},
		{"count-of-global", base.Agg(Sum("v", "total")).Agg(Count("n")), 1, "n", 1},
		{"post-filter", base.Agg(Count("n")).Filter(expr.EQ(expr.Col("n"), expr.I64(3))), 1, "n", 3},
		{"post-filter-false", base.Agg(Count("n")).Filter(expr.Bool(false)), 0, "n", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			df, err := tc.f.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer df.Close()
			if df.Height() != tc.rows {
				t.Fatalf("height=%d", df.Height())
			}
			if tc.rows > 0 {
				got := df.batches()[0].Uint64s[tc.col].Values[0]
				if got != tc.want {
					t.Fatalf("got=%d want=%d", got, tc.want)
				}
			}
		})
	}
}
