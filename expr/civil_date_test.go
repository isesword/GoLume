package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
	"time"
)

func civilDateDays(t *testing.T, date string) int32 {
	t.Helper()
	v, e := time.Parse("2006-01-02", date)
	if e != nil {
		t.Fatal(e)
	}
	return int32(v.Unix() / 86400)
}
func civilDateBatch(days []int32, valid memory.Bitmap) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(len(days))
	b.Int32s["d"] = &memory.Int32Vector{Values: days, Validity: valid}
	return b
}
func civilDateSchema(nullable bool) memory.Schema {
	return memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32, Nullable: nullable})
}

func TestCivilDateResolveRealRulesAndRange(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		zone, date, want string
		err              error
	}{
		{"new_york", "2024-03-10", "2024-03-10T05:00:00Z", nil},
		{"new_york", "2024-03-11", "2024-03-11T04:00:00Z", nil},
		{"apia", "2011-12-30", "", ErrCivilGap},
		{"apia", "2011-12-31", "2011-12-30T10:00:00Z", nil},
		{"paris", "1900-01-01", "1899-12-31T23:50:39Z", nil},
	} {
		r := civilTestResolver(t, timezoneFixture(t, tc.zone), FoldReject)
		got, e := r.ResolveDate(ctx, civilDateDays(t, tc.date))
		if tc.err != nil {
			if !errors.Is(e, tc.err) {
				t.Fatal(tc, e)
			}
			continue
		}
		if e != nil || got != civilTestUTC(t, tc.want) {
			t.Fatal(tc, got, e)
		}
		parsed, e := r.Parse(ctx, tc.date+"T00:00:00")
		if e != nil || got != parsed {
			t.Fatal("parse compatibility", e)
		}
	}
	utc := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, "")), FoldReject)
	for _, d := range []int32{math.MinInt32, math.MaxInt32, -106752, 106752} {
		if _, e := utc.ResolveDate(ctx, d); !errors.Is(e, ErrCivilRange) {
			t.Fatal(d, e)
		}
	}
	for _, d := range []int32{-106751, -1, 0, 106751} {
		v, e := utc.ResolveDate(ctx, d)
		if e != nil || v != int64(d)*86400e9 {
			t.Fatal(d, v, e)
		}
	}
	for _, tc := range []struct {
		offset int32
		d      int32
		want   string
	}{{-3600, -106752, "1677-09-21T01:00:00Z"}, {3600, 106752, "2262-04-11T23:00:00Z"}} {
		r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{tc.offset}, nil, nil, nil, "")), FoldReject)
		v, e := r.ResolveDate(ctx, tc.d)
		if e != nil || v != civilTestUTC(t, tc.want) {
			t.Fatal(tc, v, e)
		}
	}
	if _, e := utc.ResolveDate(nil, 0); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
	if _, e := (CivilTimeResolver{}).ResolveDate(ctx, 0); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal(e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := utc.ResolveDate(cancelled, 0); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestCivilDateMidnightFoldAndGap(t *testing.T) {
	// UTC transition at epoch: offset +1h to zero repeats local midnight;
	// zero to +1h skips it. Expectations do not use time.Date's zone selection.
	fold := civilTestZone(t, civilTestData(0, []int32{3600, 0}, nil, []int64{0}, []byte{1}, ""))
	gap := civilTestZone(t, civilTestData(0, []int32{0, 3600}, nil, []int64{0}, []byte{1}, ""))
	for _, p := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
		r := civilTestResolver(t, fold, p)
		v, e := r.ResolveDate(context.Background(), 0)
		if p == FoldReject {
			if !errors.Is(e, ErrCivilFold) {
				t.Fatal(e)
			}
		} else {
			want := int64(0)
			if p == FoldEarlier {
				want = -3600e9
			}
			if e != nil || v != want {
				t.Fatal(p, v, e)
			}
		}
		if _, e := civilTestResolver(t, gap, p).ResolveDate(context.Background(), 0); !errors.Is(e, ErrCivilGap) {
			t.Fatal(p, e)
		}
	}
}

func TestCivilDateExprNullableReuseAndOldCast(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, "")), FoldReject)
	p, e := CompileExpressions(civilDateSchema(true), []NamedExpr{DateAtMidnight(Col("d"), r).Alias("strict"), TryDateAtMidnight(Col("d"), r).Alias("try"), CastTo(Col("d"), memory.TypeTimestamp).Alias("old")})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
		ds := make([]int32, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		for i := range ds {
			ds[i] = int32(i) - 65
			if i%7 != 0 {
				memory.Set(valid, i)
			} else {
				ds[i] = math.MaxInt32
			}
		}
		out, e := w.Evaluate(context.Background(), civilDateBatch(ds, valid), testKernelSet())
		if e != nil {
			t.Fatal(e)
		}
		for j, o := range out {
			if p.OutputField(j).Type != memory.TypeTimestamp || !p.OutputField(j).Nullable {
				t.Fatal("metadata")
			}
			for i, d := range ds {
				if o.Int64.IsValid(i) != (i%7 != 0) {
					t.Fatal("null", i)
				}
				if i%7 == 0 {
					continue
				}
				want := int64(d) * 86400e9
				if j < 2 {
					want -= 3600e9
				}
				if o.Int64.Values[i] != want {
					t.Fatal(i, j, o.Int64.Values[i], want)
				}
			}
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
	for _, bad := range []Expr{DateAtMidnight(I64(0), r), TryDateAtMidnight(Col("d"), CivilTimeResolver{}), If(Bool(false), DateAtMidnight(Col("d"), CivilTimeResolver{}), Timestamp(0))} {
		if _, e := Compile(civilDateSchema(false), bad); e == nil {
			t.Fatal("invalid bind accepted", bad)
		}
	}
}

func TestCivilDateExprStrictTryGuards(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "apia"), FoldReject)
	skipped := civilDateDays(t, "2011-12-30")
	safe := civilDateDays(t, "2011-12-31")
	for _, tc := range []struct {
		d   int32
		err error
	}{{skipped, ErrCivilGap}, {math.MaxInt32, ErrCivilRange}} {
		for _, try := range []bool{false, true} {
			var e Expr = DateAtMidnight(Col("d"), r)
			if try {
				e = TryDateAtMidnight(Col("d"), r)
			}
			p, err := Compile(civilDateSchema(false), e)
			if err != nil {
				t.Fatal(err)
			}
			if p.ResultField().Nullable != try {
				t.Fatal("nullable")
			}
			out, err := evaluateValueTest(p, civilDateBatch([]int32{tc.d}, nil), &evaluationState{}, testKernelSet())
			if try {
				if err != nil || out.Int64.IsValid(0) {
					t.Fatal("try", err)
				}
			} else if !errors.Is(err, tc.err) {
				t.Fatal("strict", err)
			}
		}
	}
	sc := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeTimestamp, Nullable: true})
	bad := DateAtMidnight(Col("d"), r)
	cond := GT(Col("i"), I64(0))
	for _, ex := range []Expr{If(cond, bad, Timestamp(123)), Case([]WhenThen{{When: cond, Then: bad}}, Timestamp(123)), Coalesce(Col("fallback"), bad)} {
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		b := civilDateBatch([]int32{skipped, safe}, nil)
		b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1}}
		b.Int64s["fallback"] = &memory.Int64Vector{Values: []int64{123, 0}, Validity: memory.Bitmap{1}}
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[0] != 123 || out[0].Int64.Values[1] != civilTestUTC(t, "2011-12-30T10:00:00Z") {
			t.Fatal("guard", e)
		}
		b.Int64s["i"].Values[0] = 1
		b.Int64s["fallback"].Validity = memory.Bitmap{0}
		if _, e := w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, ErrCivilGap) {
			t.Fatal("active", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestCivilDateAdmissionRetryCancellationStorage(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, "")), FoldReject)
	p, e := CompileExpressions(civilDateSchema(false), []NamedExpr{DateAtMidnight(Col("d"), r).Alias("a"), TryDateAtMidnight(Col("d"), r).Alias("b")})
	if e != nil {
		t.Fatal(e)
	}
	ds := make([]int32, 130)
	for i := range ds {
		ds[i] = 1
	}
	b := civilDateBatch(ds, nil)
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
		t.Fatal("admission probe", calls, probe.Used())
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
		if e != nil || out[0].Int64.Values[129] != 86400e9 {
			t.Fatal("retry", fail, e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
	makeOut := func() scalarRegister {
		o := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, 130), ownedValidity: make(memory.Bitmap, memory.WordsFor(130))}
		for i := range o.i64 {
			o.i64[i] = -777
		}
		return o
	}
	in := scalarRegister{typ: memory.TypeDate32, i32: ds}
	out := makeOut()
	checks := 0
	out.checkContext = func() error { checks++; return nil }
	if e := evalDateAtMidnight(&out, &in, r.state, true, 130, nil); e != nil {
		t.Fatal(e)
	}
	total := checks
	for _, stop := range []int{total / 2, total} {
		out = makeOut()
		checks = 0
		out.checkContext = func() error {
			checks++
			if checks >= stop {
				return context.Canceled
			}
			return nil
		}
		e := evalDateAtMidnight(&out, &in, r.state, true, 130, nil)
		if !errors.Is(e, context.Canceled) || checks != stop {
			t.Fatal("cancel", e, checks)
		}
		processed, untouched := 0, 0
		for i, v := range out.i64 {
			if memory.IsSet(out.validity, i) {
				processed++
				if v != 86400e9 {
					t.Fatal("value")
				}
			}
			if v == -777 {
				untouched++
			}
		}
		if processed == 0 || stop < total && untouched == 0 || stop == total && processed != 130 {
			t.Fatal("cancel boundary", processed, untouched)
		}
	}
	out = makeOut()
	short := scalarRegister{typ: memory.TypeDate32, i32: nil}
	if e := evalDateAtMidnight(&out, &short, r.state, true, 1, nil); e == nil || civilRowError(e) {
		t.Fatal("Try swallowed malformed storage", e)
	}
	if e := evalDateAtMidnight(&out, &short, r.state, true, 1, memory.Bitmap{0}); e != nil {
		t.Fatal("inactive storage demanded", e)
	}
	short.validity = memory.Bitmap{0}
	if e := evalDateAtMidnight(&out, &short, r.state, true, 1, nil); e != nil {
		t.Fatal("NULL storage demanded", e)
	}
	if e := evalDateAtMidnight(&out, &in, r.state, true, 130, memory.Bitmap{0}); e == nil {
		t.Fatal("short active mask")
	}
	// Literal Date32 uses a compact constant, not an i32 backing slice.
	cp, e := Compile(memory.NewSchema(), DateAtMidnight(Date32(1), r))
	if e != nil {
		t.Fatal(e)
	}
	cb := memory.NewBatch()
	cb.Reset(3)
	v, e := evaluateValueTest(cp, cb, &evaluationState{}, testKernelSet())
	if e != nil || v.Int64.Values[2] != 86400e9 {
		t.Fatal("constant", e)
	}
}
