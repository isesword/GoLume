package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func ymdDateSchema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "y", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "m", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "d", Type: memory.TypeInt64, Nullable: true})
}
func ymdDateBatch(n int) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(n)
	for name, value := range map[string]int64{"y": 2020, "m": 2, "d": 29} {
		v := make([]int64, n)
		for i := range v {
			v[i] = value
		}
		b.Int64s[name] = &memory.Int64Vector{Values: v}
	}
	return b
}
func TestYMDDateCalendar(t *testing.T) {
	for _, tc := range []struct {
		y, m, d int64
		date    string
	}{
		{2020, 2, 29, "2020-02-29"}, {2000, 2, 29, "2000-02-29"}, {1900, 3, 1, "1900-03-01"},
		{1970, 1, 1, "1970-01-01"}, {1969, 12, 31, "1969-12-31"}, {2024, 4, 30, "2024-04-30"},
	} {
		got, e := ymdDateDays(tc.y, tc.m, tc.d)
		if e != nil || got != civilDateDays(t, tc.date) {
			t.Fatal(tc, got, e)
		}
	}
	for _, tc := range []struct {
		y, m, d int64
		err     error
	}{
		{2021, 2, 29, ErrYMDDate}, {1900, 2, 29, ErrYMDDate}, {2100, 2, 29, ErrYMDDate},
		{2024, 4, 31, ErrYMDDate}, {2020, 0, 1, ErrYMDDate}, {2020, 13, 1, ErrYMDDate},
		{2020, 1, 0, ErrYMDDate}, {2020, 1, 32, ErrYMDDate},
		{math.MaxInt64, 1, 1, ErrYMDDateRange}, {math.MinInt64, 1, 1, ErrYMDDateRange},
		{math.MaxInt64, 0, 0, ErrYMDDate}, {6000000, 1, 1, ErrYMDDateRange}, {-6000000, 1, 1, ErrYMDDateRange},
		{-100, 2, 29, ErrYMDDate}, {0, 2, 30, ErrYMDDate},
	} {
		if _, e := ymdDateDays(tc.y, tc.m, tc.d); !errors.Is(e, tc.err) {
			t.Fatal(tc, e)
		}
	}
	// Gregorian leap rule extends unchanged to year zero and negative years.
	for _, y := range []int64{0, -400, -4, 400} {
		a, e := ymdDateDays(y, 2, 29)
		if e != nil {
			t.Fatal(y, e)
		}
		b, e := ymdDateDays(y, 3, 1)
		if e != nil || b != a+1 {
			t.Fatal(y, e)
		}
	}
}
func TestYMDDateLifecycleAndIdentity(t *testing.T) {
	ex := DateFromYMD(Col("y"), Col("m"), Col("d"))
	p, e := CompileExpressions(ymdDateSchema(), []NamedExpr{ex.Alias("a"), ex.Alias("copy"), TryDateFromYMD(Col("y"), Col("m"), Col("d")).Alias("try"), DateFromYMD(Col("y"), I64(1), Col("d")).Alias("month"), DateFromYMD(I64(2021), Col("m"), Col("d")).Alias("year"), DateFromYMD(Col("y"), Col("m"), I64(1)).Alias("day")})
	// Evaluate January batches so the changed-year output stays valid.
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
		b := ymdDateBatch(n)
		for i := 0; i < n; i++ {
			b.Int64s["m"].Values[i] = 1
		}
		for j, name := range []string{"y", "m", "d"} {
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
				y, month, day := int64(2020), int64(1), int64(29)
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
				want, _ := ymdDateDays(y, month, day)
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
	for _, bad := range []Expr{DateFromYMD(Date32(0), I64(1), I64(1)), DateFromYMD(I64(2020), Str("1"), I64(1)), DateFromYMD(I64(2020), I64(1), Bool(true))} {
		if _, e := Compile(ymdDateSchema(), bad); e == nil {
			t.Fatal("implicit conversion")
		}
	}
}
func TestYMDDateErrorsGuardsAndRetry(t *testing.T) {
	sc := ymdDateSchema()
	for _, try := range []bool{false, true} {
		var ex Expr = DateFromYMD(Col("y"), Col("m"), Col("d"))
		if try {
			ex = TryDateFromYMD(Col("y"), Col("m"), Col("d"))
		}
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		b := ymdDateBatch(1)
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
		} else if !errors.Is(e, ErrYMDDate) {
			t.Fatal("strict", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	p, e := Compile(sc, If(EQ(Col("d"), I64(0)), Date32(9), DateFromYMD(Col("y"), Col("m"), Col("d"))))
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	b := ymdDateBatch(2)
	b.Int64s["d"].Values[0] = 0
	out, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil || out[0].Int32.Values[0] != 9 {
		t.Fatal("guard", e)
	}
	w.Close()
	p, e = Compile(sc, TryDateFromYMD(Col("y"), Col("m"), Col("d")))
	if e != nil {
		t.Fatal(e)
	}
	b = ymdDateBatch(130)
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
		if e != nil || got[0].Int32.Values[129] != civilDateDays(t, "2020-02-29") {
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
func TestYMDDateCancellation(t *testing.T) {
	n := 65
	y := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 2020}
	w := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 2}
	d := scalarRegister{typ: memory.TypeInt64, constant: true, i64lit: 29}
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
		if e := evalYMDDate(&out, &y, &w, &d, true, n, nil); !errors.Is(e, context.Canceled) {
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
func TestYMDDateWarmGrowthRetry(t *testing.T) {
	p, e := Compile(ymdDateSchema(), DateFromYMD(Col("y"), Col("m"), Col("d")))
	if e != nil {
		t.Fatal(e)
	}
	small, large := ymdDateBatch(1), ymdDateBatch(130)
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
		if e != nil || out[0].Int32.Values[129] != civilDateDays(t, "2020-02-29") {
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
