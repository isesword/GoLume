package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

func TestOffsetReviewStableTiesAliasesAndReuse(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{3, 1, 1, 2}}
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{30, 10, 11, 20}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, external := range []bool{false, true} {
		var s *Session
		if external {
			s = boundedJoinSession(t, 256<<10)
		} else {
			s = NewSession(engine.DefaultOptions())
		}
		q := s.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), Shift("v", 1, "v"), Diff("v", 1, "d"))
		for repeat := 0; repeat < 2; repeat++ {
			f, err := q.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			values, valid := frameInt64WithValidity(t, f, "v")
			if !equalI64(values, []int64{20, 0, 10, 11}) || !equalBools(valid, []bool{true, false, true, true}) {
				t.Fatal(external, values, valid)
			}
			values, valid = frameInt64WithValidity(t, f, "d")
			if !equalI64(values, []int64{10, 0, 1, 9}) || !equalBools(valid, []bool{true, false, true, true}) {
				t.Fatal(external, values, valid)
			}
			f.Close()
		}
		s.Close()
	}
	if !equalI64(b.Int64s["v"].Values, []int64{30, 10, 11, 20}) {
		t.Fatal("input mutated")
	}
}

func TestOffsetReviewExtremeResidentBounds(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{math.MinInt64, math.MaxInt64}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	f, err := s.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("v"))), Shift("v", math.MaxInt64, "past"), Shift("v", -math.MaxInt64, "future"), Diff("v", 1, "d")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, name := range []string{"past", "future"} {
		_, valid := frameInt64WithValidity(t, f, name)
		if !equalBools(valid, []bool{false, false}) {
			t.Fatal(name, valid)
		}
	}
	values, valid := frameInt64WithValidity(t, f, "d")
	if !equalI64(values, []int64{0, -1}) || !equalBools(valid, []bool{false, true}) {
		t.Fatal(values, valid)
	}
}
