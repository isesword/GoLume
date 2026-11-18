package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func isoWeekDateSchema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "y", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "w", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "d", Type: memory.TypeInt64, Nullable: true})
}
func isoWeekDateBatch(n int) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(n)
	for name, value := range map[string]int64{"y": 2020, "w": 53, "d": 5} {
		v := make([]int64, n)
		for i := range v {
			v[i] = value
		}
		b.Int64s[name] = &memory.Int64Vector{Values: v}
	}
	return b
}
func TestISOWeekDateCalendar(t *testing.T) {
	// Fixed Gregorian facts, not expected values derived with production ISOWeek.
	for _, tc := range []struct {
		y, w, d int64
		date    string
	}{
		{2020, 53, 5, "2021-01-01"}, {2020, 53, 7, "2021-01-03"}, {2021, 1, 1, "2021-01-04"},
		{2015, 53, 4, "2015-12-31"}, {2015, 53, 7, "2016-01-03"}, {1970, 1, 4, "1970-01-01"},
		{1969, 1, 1, "1968-12-30"}, {2000, 9, 2, "2000-02-29"},
	} {
		got, e := isoWeekDateDays(tc.y, tc.w, tc.d)
		if e != nil || got != civilDateDays(t, tc.date) {
			t.Fatal(tc, got, e)
		}
	}
	for _, tc := range []struct {
		y, w, d int64
		err     error
	}{
		{2021, 53, 1, ErrISOWeekDate}, {2020, 0, 1, ErrISOWeekDate}, {2020, 54, 1, ErrISOWeekDate},
		{2020, 1, 0, ErrISOWeekDate}, {2020, 1, 8, ErrISOWeekDate},
		{math.MaxInt64, 1, 1, ErrISOWeekDateRange}, {math.MinInt64, 1, 1, ErrISOWeekDateRange},
		{math.MaxInt64, 0, 0, ErrISOWeekDate}, {6000000, 1, 1, ErrISOWeekDateRange}, {-6000000, 1, 1, ErrISOWeekDateRange},
	} {
		if _, e := isoWeekDateDays(tc.y, tc.w, tc.d); !errors.Is(e, tc.err) {
			t.Fatal(tc, e)
		}
	}
}
func TestISOWeekDateLifecycleAndIdentity(t *testing.T) {
	ex := DateFromISOWeek(Col("y"), Col("w"), Col("d"))
	p, e := CompileExpressions(isoWeekDateSchema(), []NamedExpr{ex.Alias("a"), ex.Alias("copy"), TryDateFromISOWeek(Col("y"), Col("w"), Col("d")).Alias("try"), DateFromISOWeek(Col("y"), I64(1), Col("d")).Alias("week"), DateFromISOWeek(I64(2021), Col("w"), Col("d")).Alias("year"), DateFromISOWeek(Col("y"), Col("w"), I64(1)).Alias("day")})
	// The year-changed expression deliberately differs in identity but would
	// reject week53; evaluate week1 batches below.
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copy not interned")
	}
	for i := 2; i < len(p.outputs); i++ {
		if p.outputs[i].root == p.outputs[0].root {
			t.Fatal("identity", i)
		}
	}
	if p.SupportsSelection() {
		t.Fatal("unexpected selected fast path")
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
		b := isoWeekDateBatch(n)
		for i := 0; i < n; i++ {
			b.Int64s["w"].Values[i] = 1
		}
		for j, name := range []string{"y", "w", "d"} {
			v := make(memory.Bitmap, memory.WordsFor(n))
			for i := 0; i < n; i++ {
				if i%(j+2) != 0 {
					memory.Set(v, i)
				}
			}
			b.Int64s[name].Validity = v
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil {
			t.Fatal(e)
		}
		for j, o := range out {
			if o.Type != memory.TypeDate32 {
				t.Fatal("type")
			}
			for i := 0; i < n; i++ {
				valid := i%2 != 0 && i%3 != 0 && i%4 != 0
				y, week, day := int64(2020), int64(1), int64(5)
				if j == 3 {
					valid = i%2 != 0 && i%4 != 0
				}
				if j == 4 {
					valid = i%3 != 0 && i%4 != 0
					y = 2021
				}
				if j == 5 {
					valid = i%2 != 0 && i%3 != 0
					day = 1
				}
				if o.Int32.IsValid(i) != valid {
					t.Fatal("NULL", j, i)
				}
				want, _ := isoWeekDateDays(y, week, day)
				if valid && o.Int32.Values[i] != want {
					t.Fatal("value", j, i)
				}
			}
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("accounting")
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
	for _, bad := range []Expr{DateFromISOWeek(Date32(0), I64(1), I64(1)), DateFromISOWeek(I64(2020), Str("1"), I64(1)), DateFromISOWeek(I64(2020), I64(1), Bool(true))} {
		if _, e := Compile(isoWeekDateSchema(), bad); e == nil {
			t.Fatal("implicit conversion")
		}
	}
}
func TestISOWeekDateErrorsGuardsAndRetry(t *testing.T) {
	sc := isoWeekDateSchema()
	for _, try := range []bool{false, true} {
		var ex Expr = DateFromISOWeek(Col("y"), Col("w"), Col("d"))
		if try {
			ex = TryDateFromISOWeek(Col("y"), Col("w"), Col("d"))
		}
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		b := isoWeekDateBatch(1)
		b.Int64s["y"].Values[0] = 2021
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if try {
			if e != nil || out[0].Int32.IsValid(0) {
				t.Fatal("Try", e)
			}
		} else if !errors.Is(e, ErrISOWeekDate) {
			t.Fatal("strict", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	p, e := Compile(sc, If(EQ(Col("d"), I64(0)), Date32(9), DateFromISOWeek(Col("y"), Col("w"), Col("d"))))
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	b := isoWeekDateBatch(2)
	b.Int64s["d"].Values[0] = 0
	out, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil || out[0].Int32.Values[0] != 9 {
		t.Fatal("guard", e)
	}
	w.Close()
	p, e = Compile(sc, TryDateFromISOWeek(Col("y"), Col("w"), Col("d")))
	if e != nil {
		t.Fatal(e)
	}
	b = isoWeekDateBatch(130)
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, e = NewWorkspace(context.Background(), p, probe)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	w.Close()
	if calls == 0 {
		t.Fatal("no charged reservation")
	}
	for fail := 1; fail <= calls; fail++ {
		fm := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		ws, e := NewWorkspace(context.Background(), p, fm)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = ws.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("admission", fail, e)
		}
		fm.fail = 0
		got, e := ws.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || got[0].Int32.Values[129] != civilDateDays(t, "2021-01-01") {
			t.Fatal("retry", e)
		}
		if fm.Used() != ws.MemoryBytes() {
			t.Fatal("accounting")
		}
		ws.Close()
		if fm.Used() != 0 {
			t.Fatal("leak")
		}
	}
}
func TestISOWeekDateCancellation(t *testing.T) {
	n := 65
	y := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 2020}
	w := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 53}
	d := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 5}
	for _, stop := range []int{1, 33, n + 1} {
		checks := 0
		out := scalarRegister{typ: memory.TypeDate32, i32: make([]int32, n), ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
		out.checkContext = func() error {
			checks++
			if checks == stop {
				return context.Canceled
			}
			return nil
		}
		if e := evalISOWeekDate(&out, &y, &w, &d, true, n, nil); !errors.Is(e, context.Canceled) {
			t.Fatal("cancel", e)
		}
		processed := 0
		for i := 0; i < n; i++ {
			if memory.IsSet(out.validity, i) {
				processed++
			}
		}
		if processed != stop-1 {
			t.Fatal("cancel progress", stop, processed)
		}
	}
}

// Each warm growth reservation is failed independently; admission must remain
// retryable without discarding or undercharging already owned small buffers.
func TestISOWeekDateWarmGrowthRetry(t *testing.T) {
	p, e := Compile(isoWeekDateSchema(), DateFromISOWeek(Col("y"), Col("w"), Col("d")))
	if e != nil {
		t.Fatal(e)
	}
	small, large := isoWeekDateBatch(1), isoWeekDateBatch(130)
	warm := func(fm *selectedFaultManager) *Workspace {
		ws, e := NewWorkspace(context.Background(), p, fm)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = ws.Evaluate(context.Background(), small, testKernelSet()); e != nil {
			t.Fatal(e)
		}
		fm.calls = 0
		return ws
	}
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	ws := warm(probe)
	if _, e = ws.Evaluate(context.Background(), large, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	ws.Close()
	if calls == 0 {
		t.Fatal("no growth reservations")
	}
	for fail := 1; fail <= calls; fail++ {
		fm := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		ws := warm(fm)
		fm.fail = fail
		if _, e = ws.Evaluate(context.Background(), large, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("warm failure", fail, e)
		}
		fm.fail = 0
		out, e := ws.Evaluate(context.Background(), large, testKernelSet())
		if e != nil || out[0].Int32.Values[129] != civilDateDays(t, "2021-01-01") {
			t.Fatal("warm retry", e)
		}
		if fm.Used() != ws.MemoryBytes() {
			t.Fatal("warm accounting")
		}
		ws.Close()
		if fm.Used() != 0 {
			t.Fatal("warm leak")
		}
	}
}
