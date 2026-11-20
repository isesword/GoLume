package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"math"
	"testing"
	"time"
)

func calendarInstant(t *testing.T, s string) int64 {
	t.Helper()
	x, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		t.Fatal(e)
	}
	return x.UnixNano()
}

func TestCalendarDaysTransitions(t *testing.T) {
	for _, tc := range []struct {
		zone, in, want string
		days           int64
		fold           CivilFoldPolicy
		err            error
	}{
		{"lord_howe", "2024-10-05T01:30:00Z", "2024-10-06T01:00:00Z", 1, FoldReject, nil},
		{"new_york", "2024-02-28T17:00:00Z", "2024-02-29T17:00:00Z", 1, FoldReject, nil},
		{"new_york", "2023-12-31T17:00:00Z", "2024-01-01T17:00:00Z", 1, FoldReject, nil},
		{"new_york", "2024-03-09T14:30:00.123456789Z", "2024-03-10T13:30:00.123456789Z", 1, FoldReject, nil},
		{"new_york", "2024-11-02T13:30:00Z", "2024-11-03T14:30:00Z", 1, FoldReject, nil},
		{"new_york", "2024-03-09T07:30:00Z", "", 1, FoldReject, ErrCivilGap},
		{"new_york", "2024-11-02T05:30:00Z", "", 1, FoldReject, ErrCivilFold},
		{"new_york", "2024-11-02T05:30:00Z", "2024-11-03T05:30:00Z", 1, FoldEarlier, nil},
		{"new_york", "2024-11-02T05:30:00Z", "2024-11-03T06:30:00Z", 1, FoldLater, nil},
		{"new_york", "2024-11-03T06:30:00Z", "2024-11-03T06:30:00Z", 0, FoldReject, nil},
		{"new_york", "2024-11-03T06:30:00Z", "2024-11-03T06:30:00Z", 0, FoldEarlier, nil},
		{"apia", "2011-12-29T22:00:00Z", "", 1, FoldReject, ErrCivilGap},
		{"new_york", "2024-03-10T13:30:00Z", "2024-03-09T14:30:00Z", -1, FoldReject, nil},
	} {
		r := civilTestResolver(t, timezoneFixture(t, tc.zone), tc.fold)
		got, e := r.AddCalendarDays(context.Background(), calendarInstant(t, tc.in), tc.days)
		if tc.err != nil {
			if !errors.Is(e, tc.err) {
				t.Fatal(tc, e)
			}
		} else if e != nil || got != calendarInstant(t, tc.want) {
			t.Fatal(tc, got, e)
		}
	}
}

func TestCalendarDaysRangeAndIdentity(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, timezoneFixedData(0)), FoldReject)
	for _, v := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		got, e := r.AddCalendarDays(context.Background(), v, 0)
		if e != nil || got != v {
			t.Fatal(v, got, e)
		}
	}
	for _, d := range []int64{math.MinInt64, math.MaxInt64} {
		if _, e := r.AddCalendarDays(context.Background(), 0, d); !errors.Is(e, ErrCivilRange) {
			t.Fatal(d, e)
		}
	}
	for _, tc := range []struct{ v, d int64 }{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		if _, e := r.AddCalendarDays(context.Background(), tc.v, tc.d); !errors.Is(e, ErrCivilRange) {
			t.Fatal(tc, e)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := (CivilTimeResolver{}).AddCalendarDays(ctx, 0, 0); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestCalendarDaysExpression(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, timezoneFixedData(0)), FoldReject)
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "days", Type: memory.TypeInt64, Nullable: true})
	p, e := CompileExpressions(schema, []NamedExpr{TryAddCalendarDays(Col("ts"), Col("days"), r).Alias("out")})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{1, 0, math.MaxInt64}}
	b.Int64s["days"] = &memory.Int64Vector{Values: []int64{1, math.MaxInt64, 1}}
	out, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	v := out[0].Int64
	if v == nil || v.Values[0] != 86400e9+1 || !v.IsValid(0) || v.IsValid(1) || v.IsValid(2) {
		t.Fatal("incorrect strict/try output", v)
	}
}

func TestCalendarDaysAdmissionRetry(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, timezoneFixedData(0)), FoldReject)
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "days", Type: memory.TypeInt64})
	p, e := CompileExpressions(schema, []NamedExpr{AddCalendarDays(Col("ts"), Col("days"), r).Alias("out")})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(130)
	ts, ds := make([]int64, 130), make([]int64, 130)
	for i := range ts {
		ts[i] = int64(i)
		ds[i] = 1
	}
	b.Int64s["ts"] = &memory.Int64Vector{Values: ts}
	b.Int64s["days"] = &memory.Int64Vector{Values: ds}
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
		t.Fatal("probe")
	}
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(fail, e)
		}
		m.fail = 0
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[129] != 86400e9+129 {
			t.Fatal("retry", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func calendarBatch(ts, days []int64, tv, dv memory.Bitmap) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(len(ts))
	b.Int64s["ts"] = &memory.Int64Vector{Values: ts, Validity: tv}
	b.Int64s["days"] = &memory.Int64Vector{Values: days, Validity: dv}
	return b
}
func calendarSchema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "days", Type: memory.TypeInt64, Nullable: true})
}

func TestCalendarDaysStrictTryGuardsAndNull(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldReject)
	for _, tc := range []struct {
		instant string
		err     error
	}{{"2024-03-09T07:30:00Z", ErrCivilGap}, {"2024-11-02T05:30:00Z", ErrCivilFold}} {
		b := calendarBatch([]int64{calendarInstant(t, tc.instant)}, []int64{1}, nil, nil)
		for _, try := range []bool{false, true} {
			var ex Expr = AddCalendarDays(Col("ts"), Col("days"), r)
			if try {
				ex = TryAddCalendarDays(Col("ts"), Col("days"), r)
			}
			p, e := Compile(calendarSchema(), ex)
			if e != nil {
				t.Fatal(e)
			}
			v, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
			if try {
				if e != nil || v.Int64.IsValid(0) {
					t.Fatal("Try", e)
				}
			} else if !errors.Is(e, tc.err) {
				t.Fatal("strict", e)
			}
		}
	}
	bad := AddCalendarDays(Col("ts"), Col("days"), r)
	p, e := Compile(calendarSchema(), bad)
	if e != nil {
		t.Fatal(e)
	}
	b := calendarBatch([]int64{math.MaxInt64, math.MaxInt64}, []int64{math.MaxInt64, math.MaxInt64}, memory.Bitmap{2}, memory.Bitmap{1})
	v, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if e != nil || v.Int64.IsValid(0) || v.Int64.IsValid(1) {
		t.Fatal("NULL", e)
	}
	b = calendarBatch([]int64{calendarInstant(t, "2024-03-09T07:30:00Z"), 1}, []int64{1, 0}, nil, nil)
	cond := EQ(Col("days"), I64(0))
	for _, ex := range []Expr{If(cond, bad, Timestamp(123)), Case([]WhenThen{{When: cond, Then: bad}}, Timestamp(123))} {
		p, e := Compile(calendarSchema(), ex)
		if e != nil {
			t.Fatal(e)
		}
		v, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
		if e != nil || v.Int64.Values[0] != 123 || v.Int64.Values[1] != 1 {
			t.Fatal("guard", e)
		}
	}
	for _, ex := range []Expr{AddCalendarDays(I64(0), I64(1), r), AddCalendarDays(Timestamp(0), Duration(time.Hour), r), If(Bool(false), AddCalendarDays(Timestamp(0), I64(0), CivilTimeResolver{}), Timestamp(0))} {
		if _, e := Compile(calendarSchema(), ex); e == nil {
			t.Fatal("invalid bind accepted")
		}
	}
}

func TestCalendarDaysIdentityAndChildError(t *testing.T) {
	z := civilTestZone(t, timezoneFixedData(0))
	r := civilTestResolver(t, z, FoldReject)
	reload := civilTestResolver(t, z, FoldReject)
	p, e := CompileExpressions(calendarSchema(), []NamedExpr{AddCalendarDays(Col("ts"), Col("days"), r).Alias("a"), AddCalendarDays(Col("ts"), Col("days"), r).Alias("b"), AddCalendarDays(Col("ts"), Col("days"), reload).Alias("c"), TryAddCalendarDays(Col("ts"), Col("days"), r).Alias("d")})
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root || p.outputs[0].root == p.outputs[2].root || p.outputs[0].root == p.outputs[3].root {
		t.Fatal("CSE identity")
	}
	sc := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	b, e := Bind(sc, TryAddCalendarDays(CastTo(Col("s"), memory.TypeTimestamp), I64(0), r))
	if e != nil || !b.MayError {
		t.Fatal("child errors", e)
	}
}
