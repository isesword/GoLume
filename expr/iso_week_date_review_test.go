package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
)

func TestISOWeekDateReviewIndependentBounds(t *testing.T) {
	// Independent integer Gregorian era decomposition and Thursday/year-day
	// oracle, not the production time.Date(Jan4) construction. Adjacent endpoints
	// demonstrate that the broad year envelope does not silently clamp Date32.
	for _, tc := range []struct {
		y, w, d int64
		want    int32
		err     error
	}{
		{-5877641, 26, 1, 0, ErrISOWeekDateRange},
		{-5877641, 26, 2, math.MinInt32, nil},
		{-5877641, 26, 3, math.MinInt32 + 1, nil},
		{5881580, 28, 4, math.MaxInt32 - 1, nil},
		{5881580, 28, 5, math.MaxInt32, nil},
		{5881580, 28, 6, 0, ErrISOWeekDateRange},
		{-1, 52, 6, -719528, nil}, // Gregorian 0000-01-01, ISO year -1.
		{-2, 53, 5, -719893, nil}, // Gregorian -0001-01-01, ISO year -2.
		{-400, 1, 1, -865623, nil}, {-100, 1, 1, -756052, nil},
		{-4, 1, 1, -720989, nil}, {-2, 1, 1, -720261, nil},
		{-1, 1, 1, -719890, nil}, {0, 1, 1, -719526, nil}, {1, 1, 1, -719162, nil},
		{4, 1, 1, -718070, nil}, {100, 1, 1, -683000, nil}, {400, 1, 1, -573429, nil},
		{2020, 53, 5, 18628, nil}, {2021, 53, 1, 0, ErrISOWeekDate},
		{math.MinInt64, 1, 1, 0, ErrISOWeekDateRange},
		{math.MaxInt64, 1, 1, 0, ErrISOWeekDateRange},
		{math.MaxInt64, 0, 1, 0, ErrISOWeekDate},
		{0, 1, 0, 0, ErrISOWeekDate}, {0, 54, 1, 0, ErrISOWeekDate},
	} {
		got, e := isoWeekDateDays(tc.y, tc.w, tc.d)
		if !errors.Is(e, tc.err) || e == nil && got != tc.want {
			t.Fatal(tc, got, e)
		}
		// Also exercise constants through typed compilation and vector evaluation.
		p, e := Compile(memory.NewSchema(), TryDateFromISOWeek(I64(tc.y), I64(tc.w), I64(tc.d)))
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(3)
		m := memory.NewBudgetManager(0)
		ws, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := ws.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		if out[0].Type != memory.TypeDate32 {
			t.Fatal("logical type")
		}
		for i := 0; i < 3; i++ {
			if out[0].Int32.IsValid(i) != (tc.err == nil) || tc.err == nil && out[0].Int32.Values[i] != tc.want {
				t.Fatal(tc, i, out[0])
			}
		}
		ws.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestISOWeekDateReviewComputedABC(t *testing.T) {
	z := timezoneFixture(t, "utc")
	y, w, d := ISOWeekYearInZone(Col("ts"), z), ISOWeekInZone(Col("ts"), z), ISOWeekdayInZone(Col("ts"), z)
	p, e := CompileExpressions(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), []NamedExpr{
		DateFromISOWeek(y, w, d).Alias("date"), y.Alias("y"), w.Alias("w"), d.Alias("d"), TryDateFromISOWeek(y, w, d).Alias("try"),
	})
	if e != nil {
		t.Fatal(e)
	}
	if p.SupportsSelection() {
		t.Fatal("new selected fast path unexpectedly enabled")
	}
	for _, idx := range []int{0, 4} {
		in := p.instructions[p.outputs[idx].root]
		if !reflect.DeepEqual(instructionInputs(in), []int{in.A, in.B, in.C}) {
			t.Fatal("missing ordered ABC liveness")
		}
		for _, child := range []int{in.A, in.B, in.C} {
			if in.storageOwner == p.instructions[child].storageOwner {
				t.Fatal("in-place owner")
			}
		}
		n, e := fixedRegisterBytes(in, 65)
		if e != nil || n != 65*4+2*8 {
			t.Fatal("Date32 charge", n, e)
		}
	}
	ts := []int64{civilTestUTC(t, "2021-01-01T12:00:00Z"), -1, civilTestUTC(t, "2024-03-10T12:00:00Z")}
	dates := []int32{18628, -1, 19792}
	ys := []int64{2020, 1970, 2024}
	weeks := []int64{53, 1, 10}
	days := []int64{5, 3, 7}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		m := memory.NewBudgetManager(0)
		ws, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		for _, n := range []int{130, 1, 65, 0, 130} {
			b := memory.NewBatch()
			b.Reset(n)
			v := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
			for i := range v.Values {
				v.Values[i] = ts[i%3]
				if i%5 != 0 {
					memory.Set(v.Validity, i)
				}
			}
			b.Int64s["ts"] = v
			out, e := ws.Evaluate(context.Background(), b, kernel.New(mode))
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < n; i++ {
				valid := i%5 != 0
				for _, col := range []int{0, 4} {
					if out[col].Int32.IsValid(i) != valid || valid && out[col].Int32.Values[i] != dates[i%3] {
						t.Fatal("computed result", mode, n, i, col)
					}
				}
				for j, want := range []int64{ys[i%3], weeks[i%3], days[i%3]} {
					if out[j+1].Int64.IsValid(i) != valid || valid && out[j+1].Int64.Values[i] != want {
						t.Fatal("retained child clobbered", j, i)
					}
				}
			}
			if m.Used() != ws.MemoryBytes() {
				t.Fatal("accounting")
			}
		}
		ws.Close()
		ws.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestISOWeekDateReviewDemandedStorage(t *testing.T) {
	output := func() scalarRegister {
		return scalarRegister{typ: memory.TypeDate32, i32: make([]int32, 1), ownedValidity: make(memory.Bitmap, 1)}
	}
	for null := 0; null < 3; null++ {
		inputs := []scalarRegister{{typ: memory.TypeInt64}, {typ: memory.TypeInt64}, {typ: memory.TypeInt64}}
		inputs[null].validity = memory.Bitmap{0}
		out := output()
		if e := evalISOWeekDate(&out, &inputs[0], &inputs[1], &inputs[2], false, 1, nil); e != nil || memory.IsSet(out.validity, 0) {
			t.Fatal("NULL demanded missing payload", null, e)
		}
	}
	y := scalarRegister{typ: memory.TypeInt64, i64: []int64{2021}}
	w := scalarRegister{typ: memory.TypeInt64, i64: []int64{53}}
	d := scalarRegister{typ: memory.TypeInt64, i64: []int64{1}}
	out := output()
	if e := evalISOWeekDate(&out, &y, &w, &d, true, 1, nil); e != nil || memory.IsSet(out.validity, 0) {
		t.Fatal("Try nonexistent week", e)
	}
	out = output()
	if e := evalISOWeekDate(&out, &y, &w, &d, false, 1, nil); !errors.Is(e, ErrISOWeekDate) {
		t.Fatal(e)
	}
	d.i64 = nil
	out = output()
	if e := evalISOWeekDate(&out, &y, &w, &d, true, 1, nil); e == nil || errors.Is(e, ErrISOWeekDate) || errors.Is(e, ErrISOWeekDateRange) {
		t.Fatal("Try swallowed third storage", e)
	}
	out = output()
	if e := evalISOWeekDate(&out, &y, &w, &d, true, 1, memory.Bitmap{0}); e != nil {
		t.Fatal("inactive storage demanded", e)
	}
	out = output()
	out.checkContext = func() error { return context.Canceled }
	if e := evalISOWeekDate(&out, &y, &w, &d, true, 1, memory.Bitmap{0}); !errors.Is(e, context.Canceled) {
		t.Fatal("inactive cancellation swallowed", e)
	}
}

func TestISOWeekDateReviewTryPreservesChildFailure(t *testing.T) {
	z := civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, ""))
	r := civilTestResolver(t, z, FoldReject)
	badYear := ISOWeekYearInZone(ParseCivilTimestamp(Str("bad"), r), z)
	sc := memory.NewSchema(memory.Field{Name: "week", Type: memory.TypeInt64, Nullable: true})
	ex := TryDateFromISOWeek(badYear, Col("week"), I64(1))
	bound, e := Bind(sc, ex)
	if e != nil || !bound.MayError {
		t.Fatal("child MayError", e)
	}
	p, e := Compile(sc, ex)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(1)
	b.Int64s["week"] = &memory.Int64Vector{Values: []int64{0}, Validity: memory.Bitmap{0}}
	m := memory.NewBudgetManager(0)
	ws, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ws.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar)); !errors.Is(e, ErrCivilSyntax) {
		t.Fatal("Try/NULL suppressed child failure", e)
	}
	ws.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}
