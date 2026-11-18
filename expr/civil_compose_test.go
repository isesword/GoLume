package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
)

func civilComposeSchema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "date", Type: memory.TypeDate32, Nullable: true}, memory.Field{Name: "clock", Type: memory.TypeTime64, Nullable: true})
}
func civilComposeBatch(days []int32, clocks []int64, dvalid, cvalid memory.Bitmap) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(len(days))
	b.Int32s["date"] = &memory.Int32Vector{Values: days, Validity: dvalid}
	b.Int64s["clock"] = &memory.Int64Vector{Values: clocks, Validity: cvalid}
	return b
}
func TestCivilComposeResolveDateTime(t *testing.T) {
	ctx := context.Background()
	utc := civilTestResolver(t, civilTestZone(t, timezoneFixedData(0)), FoldReject)
	for _, tc := range []struct {
		day         int32
		clock, want int64
		err         error
	}{
		{0, 0, 0, nil}, {0, 1, 1, nil}, {-1, 86400e9 - 1, -1, nil},
		{-106752, 763145224192, math.MinInt64, nil}, {-106752, 763145224191, 0, ErrCivilRange},
		{106751, 85636854775807, math.MaxInt64, nil}, {106751, 85636854775808, 0, ErrCivilRange},
		{math.MinInt32, 0, 0, ErrCivilRange}, {math.MaxInt32, 86400e9 - 1, 0, ErrCivilRange},
		{0, -1, 0, ErrCivilClock}, {0, 86400e9, 0, ErrCivilClock}, {0, math.MinInt64, 0, ErrCivilClock}, {0, math.MaxInt64, 0, ErrCivilClock},
	} {
		v, e := utc.ResolveDateTime(ctx, tc.day, tc.clock)
		if tc.err != nil {
			if !errors.Is(e, tc.err) {
				t.Fatal(tc, e)
			}
		} else if e != nil || v != tc.want {
			t.Fatal(tc, v, e)
		}
	}
	for _, d := range []int32{-106751, -1, 0, 1, 106751} {
		a, e := utc.ResolveDate(ctx, d)
		b, f := utc.ResolveDateTime(ctx, d, 0)
		if a != b || e != f {
			t.Fatal("midnight compatibility", d, e, f)
		}
	}
	for _, tc := range []struct {
		zone, date  string
		clock       int64
		early, late string
		err         error
	}{
		{"new_york", "2024-03-10", 9000e9, "", "", ErrCivilGap},
		{"new_york", "2024-03-10", 10800e9, "2024-03-10T07:00:00Z", "", nil},
		{"new_york", "2024-11-03", 5400e9, "2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z", nil},
		{"apia", "2011-12-30", 0, "", "", ErrCivilGap},
		{"apia", "2011-12-31", 0, "2011-12-30T10:00:00Z", "", nil},
		{"paris", "1900-01-01", 561e9 + 1, "1900-01-01T00:00:00.000000001Z", "", nil},
	} {
		for _, policy := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
			r := civilTestResolver(t, timezoneFixture(t, tc.zone), policy)
			v, e := r.ResolveDateTime(ctx, civilDateDays(t, tc.date), tc.clock)
			wantErr := tc.err
			if tc.late != "" && policy == FoldReject {
				wantErr = ErrCivilFold
			}
			if wantErr != nil {
				if !errors.Is(e, wantErr) {
					t.Fatal(tc, policy, e)
				}
				continue
			}
			want := tc.early
			if tc.late != "" && policy == FoldLater {
				want = tc.late
			}
			if e != nil || v != civilTestUTC(t, want) {
				t.Fatal(tc, policy, v, e)
			}
		}
	}
	if _, e := utc.ResolveDateTime(nil, 0, -1); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
	if _, e := (CivilTimeResolver{}).ResolveDateTime(ctx, 0, -1); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal("config before clock", e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := (CivilTimeResolver{}).ResolveDateTime(cancelled, 0, -1); !errors.Is(e, context.Canceled) {
		t.Fatal("ctx before config", e)
	}
}

func TestCivilComposeTypedNULLReuseAndCSE(t *testing.T) {
	z := civilTestZone(t, timezoneFixedData(0))
	r := civilTestResolver(t, z, FoldReject)
	copyR := r
	reload := civilTestResolver(t, z, FoldReject)
	other := civilTestResolver(t, civilTestZone(t, timezoneFixedData(3600)), FoldReject)
	p, e := CompileExpressions(civilComposeSchema(), []NamedExpr{ComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("a"), ComposeCivilTimestamp(Col("date"), Col("clock"), copyR).Alias("copy"), ComposeCivilTimestamp(Col("date"), Col("clock"), reload).Alias("reload"), ComposeCivilTimestamp(Col("date"), Col("clock"), other).Alias("other"), TryComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("try"), ComposeCivilTimestamp(Date32(1), Col("clock"), r).Alias("date"), ComposeCivilTimestamp(Col("date"), TimeOfDayInZone(Timestamp(1), z), r).Alias("clock")})
	if e != nil {
		t.Fatal(e)
	}
	if p.SupportsSelection() {
		t.Fatal("new opcode selection enabled")
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copy CSE")
	}
	for i := 2; i < len(p.outputs); i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("identity/date/clock/mode merged", i, j)
			}
		}
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
		days := make([]int32, n)
		clocks := make([]int64, n)
		dv := make(memory.Bitmap, memory.WordsFor(n))
		cv := make(memory.Bitmap, memory.WordsFor(n))
		for i := range days {
			days[i] = 0
			clocks[i] = int64(i)
			if i%3 != 0 {
				memory.Set(dv, i)
			}
			if i%5 != 0 {
				memory.Set(cv, i)
			}
		}
		out, e := w.Evaluate(context.Background(), civilComposeBatch(days, clocks, dv, cv), testKernelSet())
		if e != nil {
			t.Fatal(e)
		}
		for j, o := range out {
			if o.Type != memory.TypeTimestamp || p.OutputField(j).Type != memory.TypeTimestamp || !p.OutputField(j).Nullable {
				t.Fatal("metadata")
			}
			for i := range days {
				valid := i%3 != 0 && i%5 != 0
				if j == 5 {
					valid = i%5 != 0
				}
				if j == 6 {
					valid = i%3 != 0
				}
				if o.Int64.IsValid(i) != valid {
					t.Fatal("NULL intersection", j, i)
				}
				if !valid {
					continue
				}
				want := int64(i)
				if j == 3 {
					want -= 3600e9
				}
				if j == 5 {
					want += 86400e9
				}
				if j == 6 {
					want = 1
				}
				if o.Int64.Values[i] != want {
					t.Fatal(j, i, o.Int64.Values[i], want)
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
	for _, ex := range []Expr{ComposeCivilTimestamp(I64(0), Col("clock"), r), ComposeCivilTimestamp(Col("date"), I64(0), r), ComposeCivilTimestamp(Col("clock"), Col("date"), r), TryComposeCivilTimestamp(Col("date"), Col("clock"), CivilTimeResolver{}), If(Bool(false), ComposeCivilTimestamp(Col("date"), Col("clock"), CivilTimeResolver{}), Timestamp(0))} {
		if _, e := Compile(civilComposeSchema(), ex); e == nil {
			t.Fatal("invalid input/config admitted", ex)
		}
	}
}

func TestCivilComposeStrictTryAndGuards(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldReject)
	for _, tc := range []struct {
		date  string
		clock int64
		err   error
	}{{"2024-03-10", 9000e9, ErrCivilGap}, {"2024-11-03", 5400e9, ErrCivilFold}, {"9999-01-01", 0, ErrCivilRange}, {"2024-01-01", -1, ErrCivilClock}, {"2024-01-01", 86400e9, ErrCivilClock}} {
		for _, try := range []bool{false, true} {
			var ex Expr = ComposeCivilTimestamp(Col("date"), Col("clock"), r)
			if try {
				ex = TryComposeCivilTimestamp(Col("date"), Col("clock"), r)
			}
			p, e := Compile(civilComposeSchema(), ex)
			if e != nil {
				t.Fatal(e)
			}
			b := civilComposeBatch([]int32{civilDateDays(t, tc.date)}, []int64{tc.clock}, nil, nil)
			v, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
			if try {
				if e != nil || v.Int64.IsValid(0) {
					t.Fatal("try", tc, e)
				}
			} else if !errors.Is(e, tc.err) {
				t.Fatal("strict", tc, e)
			}
		}
	}
	p, e := Compile(civilComposeSchema(), ComposeCivilTimestamp(Col("date"), Col("clock"), r))
	if e != nil {
		t.Fatal(e)
	}
	b := civilComposeBatch([]int32{0, math.MaxInt32}, []int64{math.MaxInt64, 0}, memory.Bitmap{2}, memory.Bitmap{1})
	out, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if e != nil || out.Int64.IsValid(0) || out.Int64.IsValid(1) {
		t.Fatal("NULL hides invalid opposite payload", e)
	}
	sc := memory.NewSchema(memory.Field{Name: "date", Type: memory.TypeDate32}, memory.Field{Name: "clock", Type: memory.TypeTime64}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeTimestamp, Nullable: true})
	bad := ComposeCivilTimestamp(Col("date"), Col("clock"), r)
	cond := GT(Col("i"), I64(0))
	for _, ex := range []Expr{If(cond, bad, Timestamp(123)), Case([]WhenThen{{When: cond, Then: bad}}, Timestamp(123)), Coalesce(Col("fallback"), bad)} {
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		b := civilComposeBatch([]int32{0, 0}, []int64{-1, 0}, nil, nil)
		b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1}}
		b.Int64s["fallback"] = &memory.Int64Vector{Values: []int64{123, 0}, Validity: memory.Bitmap{1}}
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[0] != 123 || out[0].Int64.Values[1] != 18000e9 {
			t.Fatal("guard", e)
		}
		b.Int64s["i"].Values[0] = 1
		b.Int64s["fallback"].Validity = memory.Bitmap{0}
		if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, ErrCivilClock) {
			t.Fatal("active error", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestCivilComposeAdmissionRetryCancellation(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, timezoneFixedData(0)), FoldReject)
	p, e := CompileExpressions(civilComposeSchema(), []NamedExpr{ComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("strict"), TryComposeCivilTimestamp(Col("date"), Col("clock"), r).Alias("try")})
	if e != nil {
		t.Fatal(e)
	}
	const n = 130
	days := make([]int32, n)
	clocks := make([]int64, n)
	for i := range days {
		days[i] = 1
		clocks[i] = int64(i)
	}
	b := civilComposeBatch(days, clocks, nil, nil)
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, e := NewWorkspace(context.Background(), p, probe)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	w.Close()
	if calls == 0 || probe.Used() != 0 {
		t.Fatal("admission probe")
	}
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("reserve", fail, e)
		}
		m.fail = 0
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[129] != 86400e9+129 {
			t.Fatal("retry", e)
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("accounting")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	makeOut := func() scalarRegister {
		o := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, n), ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
		for i := range o.i64 {
			o.i64[i] = -777
		}
		return o
	}
	date := scalarRegister{typ: memory.TypeDate32, i32: days}
	clock := scalarRegister{typ: memory.TypeTime64, i64: clocks}
	out := makeOut()
	checks := 0
	out.checkContext = func() error { checks++; return nil }
	if e := evalCivilCompose(&out, &date, &clock, r.state, true, n, nil); e != nil {
		t.Fatal(e)
	}
	total := checks
	for _, stop := range []int{1, total / 2, total} {
		out = makeOut()
		checks = 0
		out.checkContext = func() error {
			checks++
			if checks >= stop {
				return context.Canceled
			}
			return nil
		}
		e := evalCivilCompose(&out, &date, &clock, r.state, true, n, nil)
		if !errors.Is(e, context.Canceled) || checks != stop {
			t.Fatal("cancel", stop, e)
		}
		processed := 0
		for i, v := range out.i64 {
			if memory.IsSet(out.validity, i) {
				processed++
				if v != 86400e9+int64(i) {
					t.Fatal("partial value")
				}
			}
		}
		if stop == 1 && processed != 0 || stop == total && processed != n || stop > 1 && stop < total && (processed == 0 || processed == n) {
			t.Fatal("cancel boundary", stop, processed)
		}
	}
}
