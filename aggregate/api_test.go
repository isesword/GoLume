package aggregate

import "testing"

func TestBuiltInStates(t *testing.T) {
	cases := []struct {
		kind Kind
		want Value
	}{
		{KindSum, Value{Valid: true, Int64: 6}},
		{KindCountRows, Value{Valid: true, Int64: 4}},
		{KindCountValues, Value{Valid: true, Int64: 3}},
		{KindMin, Value{Valid: true, Int64: 1}},
		{KindMax, Value{Valid: true, Int64: 3}},
		{KindAvg, Value{Valid: true, Float64: 2, IsFloat: true}},
	}
	for _, tc := range cases {
		s, err := NewState(tc.kind)
		if err != nil {
			t.Fatal(err)
		}
		s.Add(1, true)
		s.Add(100, false)
		s.Add(2, true)
		s.Add(3, true)
		if got := s.Finalize(); got != tc.want {
			t.Fatalf("kind=%d got=%+v want=%+v", tc.kind, got, tc.want)
		}
	}
}

func TestStateMerge(t *testing.T) {
	a := &AvgState{}
	b := &AvgState{}
	a.Add(10, true)
	b.Add(20, true)
	b.Add(30, true)
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	got := a.Finalize()
	if !got.Valid || got.Float64 != 20 {
		t.Fatalf("got=%+v", got)
	}
}

func TestInt64VectorDefaultLayout(t *testing.T) {
	v := NewInt64Vector(4, DefaultLayout())
	v.Init(2, 10)
	v.Add(2, 5)
	got := v.Result(2)
	if got.Sum != 15 || got.CountRows != 2 {
		t.Fatalf("sum=%d count=%d", got.Sum, got.CountRows)
	}
	w := NewInt64Vector(8, DefaultLayout())
	v.Copy(&w, 2, 5)
	got = w.Result(5)
	if got.Sum != 15 || got.CountRows != 2 {
		t.Fatalf("copied sum=%d count=%d", got.Sum, got.CountRows)
	}
}

func TestInt64VectorAllNullSemantics(t *testing.T) {
	layout := Layout{Sum: true, CountRows: true, CountValues: true, Min: true, Max: true, Avg: true}
	v := NewInt64Vector(1, layout)
	v.InitValue(0, 123, false)
	v.AddValue(0, 456, false)
	got := v.Result(0)
	if got.SumValid || got.MinValid || got.MaxValid || got.AvgValid {
		t.Fatalf("NULL-only aggregate unexpectedly valid: %+v", got)
	}
	if got.CountRows != 2 || got.CountValues != 0 {
		t.Fatalf("counts=%+v", got)
	}
}
