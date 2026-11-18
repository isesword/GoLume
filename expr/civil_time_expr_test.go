package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"runtime"
	"strings"
	"testing"
)

func civilExprBatch(values []string, bits memory.Bitmap) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(values)
	var valid []bool
	if bits != nil {
		valid = make([]bool, len(values))
		for i := range valid {
			valid[i] = memory.IsSet(bits, i)
		}
	}
	b.Strings["s"] = phase32StringVector(values, valid)
	return b
}
func civilExprSchema(nullable bool) memory.Schema {
	return memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: nullable})
}
func TestCivilExprRealRules(t *testing.T) {
	cases := []struct{ zone, text, early, late string }{
		{"new_york", "2024-11-03T01:30:00", "2024-11-03T05:30:00Z", "2024-11-03T06:30:00Z"},
		{"lord_howe", "2024-04-07T01:45:00", "2024-04-06T14:45:00Z", "2024-04-06T15:15:00Z"},
		{"apia", "2011-12-31T00:00:00", "2011-12-30T10:00:00Z", "2011-12-30T10:00:00Z"},
		{"paris", "1900-01-01T00:09:21", "1900-01-01T00:00:00Z", "1900-01-01T00:00:00Z"},
	}
	for _, tc := range cases {
		for _, policy := range []CivilFoldPolicy{FoldEarlier, FoldLater} {
			t.Run(fmt.Sprintf("%s-%d", tc.zone, policy), func(t *testing.T) {
				r := civilTestResolver(t, timezoneFixture(t, tc.zone), policy)
				p, e := CompileExpressions(civilExprSchema(true), []NamedExpr{ParseCivilTimestamp(Col("s"), r).Alias("strict"), TryParseCivilTimestamp(Col("s"), r).Alias("try")})
				if e != nil {
					t.Fatal(e)
				}
				if p.SupportsSelection() {
					t.Fatal("String selection enabled")
				}
				for i := 0; i < 2; i++ {
					if p.OutputField(i).Type != memory.TypeTimestamp || !p.OutputField(i).Nullable {
						t.Fatal("metadata")
					}
				}
				m := memory.NewBudgetManager(0)
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
					v := make([]string, n)
					bits := make(memory.Bitmap, memory.WordsFor(n))
					for i := range v {
						v[i] = tc.text
						if i%7 != 0 {
							memory.Set(bits, i)
						} else {
							v[i] = "hidden invalid"
						}
					}
					b := civilExprBatch(v, bits)
					out, e := w.Evaluate(context.Background(), b, testKernelSet())
					if e != nil {
						t.Fatal(e)
					}
					expected := tc.early
					if policy == FoldLater {
						expected = tc.late
					}
					want := civilTestUTC(t, expected)
					for _, o := range out {
						for i := 0; i < n; i++ {
							if o.Int64.IsValid(i) != (i%7 != 0) || (i%7 != 0 && o.Int64.Values[i] != want) {
								t.Fatal("oracle", i, o.Int64.Values[i], want)
							}
						}
					}
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("leak")
				}
			})
		}
	}
}
func TestCivilExprStrictTryErrors(t *testing.T) {
	cases := []struct {
		zone, text string
		err        error
	}{{"new_york", "2024-03-10T02:30:00", ErrCivilGap}, {"new_york", "2024-11-03T01:30:00", ErrCivilFold}, {"apia", "2011-12-30T12:00:00", ErrCivilGap}, {"new_york", "2024-02-30T00:00:00", ErrCivilSyntax}, {"new_york", "2024-01-01T00:00:00Z", ErrCivilSyntax}, {"new_york", "9999-01-01T00:00:00", ErrCivilRange}}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			r := civilTestResolver(t, timezoneFixture(t, tc.zone), FoldReject)
			for _, try := range []bool{false, true} {
				var ex Expr = ParseCivilTimestamp(Col("s"), r)
				if try {
					ex = TryParseCivilTimestamp(Col("s"), r)
				}
				p, e := Compile(civilExprSchema(false), ex)
				if e != nil {
					t.Fatal(e)
				}
				if p.ResultField().Nullable != try {
					t.Fatal("nullable")
				}
				m := memory.NewBudgetManager(0)
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				out, e := w.Evaluate(context.Background(), civilExprBatch([]string{tc.text}, nil), testKernelSet())
				if try {
					if e != nil || out[0].Int64.IsValid(0) {
						t.Fatal("Try", e)
					}
				} else if !errors.Is(e, tc.err) {
					t.Fatal("strict identity", e, tc.err)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("leak")
				}
			}
		})
	}
}
func TestCivilExprIdentityDefaults(t *testing.T) {
	z0 := civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, ""))
	z1 := civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, ""))
	r0, r1 := civilTestResolver(t, z0, FoldReject), civilTestResolver(t, z1, FoldReject)
	p, e := CompileExpressions(civilExprSchema(false), []NamedExpr{ParseCivilTimestamp(Col("s"), r0).Alias("a"), ParseCivilTimestamp(Col("s"), r1).Alias("b"), CastTo(Col("s"), memory.TypeTimestamp).Alias("old")})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	b := civilExprBatch([]string{"1970-01-01T00:00:00"}, nil)
	for _, want := range []int64{0, 1e9} {
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil {
			t.Fatal(e)
		}
		if out[0].Int64.Values[0] != want || out[1].Int64.Values[0] != want-3600e9 || out[2].Int64.Values[0] != want {
			t.Fatal("same-label/default/cache")
		}
		b.Strings["s"] = phase32StringVector([]string{"1970-01-01T00:00:01"}, nil)
	}
	old, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = NewCivilTimeResolver(context.Background(), old, CivilResolveOptions{FoldReject, GapReject}); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal("raw provenance", e)
	}
	for _, ex := range []Expr{TryParseCivilTimestamp(Col("s"), CivilTimeResolver{}), If(Bool(false), ParseCivilTimestamp(Col("s"), CivilTimeResolver{}), Timestamp(0))} {
		if _, e = Compile(civilExprSchema(false), ex); !errors.Is(e, ErrCivilConfiguration) {
			t.Fatal("bind config", e)
		}
	}
}
func TestCivilExprGuardAndResources(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldReject)
	bad := ParseCivilTimestamp(Col("s"), r)
	b := civilExprBatch([]string{"2024-03-10T02:30:00", "2024-01-01T00:00:00"}, nil)
	sc := civilExprSchema(false)
	safe := Timestamp(123)
	for _, ex := range []Expr{If(Bool(false), bad, safe), Coalesce(safe, bad)} {
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		out, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
		if e != nil || out.Int64.Values[0] != 123 {
			t.Fatal("dead", e)
		}
	}
	for _, try := range []bool{false, true} {
		var ex Expr = bad
		if try {
			ex = TryParseCivilTimestamp(Col("s"), r)
		}
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		m := memory.NewBudgetManager(1)
		w, e := NewWorkspace(context.Background(), p, m)
		if e == nil {
			_, e = w.Evaluate(context.Background(), b, testKernelSet())
			w.Close()
		}
		if !errors.Is(e, memory.ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("budget", e)
		}
		m = memory.NewBudgetManager(0)
		w, e = NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, e = w.Evaluate(ctx, b, testKernelSet())
		w.Close()
		if !errors.Is(e, context.Canceled) || m.Used() != 0 {
			t.Fatal("cancel", e)
		}
	}
}

func TestCivilExprDynamicGuardsStorage(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldReject)
	bad := ParseCivilTimestamp(Col("s"), r)
	sc := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeTimestamp, Nullable: true})
	cond := GT(Col("i"), I64(0))
	for _, ex := range []Expr{If(cond, bad, Timestamp(123)), Case([]WhenThen{{When: cond, Then: bad}}, Timestamp(123)), Coalesce(Col("fallback"), bad)} {
		b := civilExprBatch([]string{"2024-03-10T02:30:00", "2024-01-01T00:00:00"}, nil)
		b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1}}
		b.Int64s["fallback"] = &memory.Int64Vector{Values: []int64{123, 0}, Validity: memory.Bitmap{1}}
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[0] != 123 || out[0].Int64.Values[1] != civilTestUTC(t, "2024-01-01T05:00:00Z") {
			t.Fatal("dynamic", e)
		}
		// Reuse the same batch: the formerly inactive gap is now demanded.
		b.Int64s["i"].Values[0] = 1
		b.Int64s["fallback"].Validity = memory.Bitmap{0}
		_, e = w.Evaluate(context.Background(), b, testKernelSet())
		if !errors.Is(e, ErrCivilGap) {
			t.Fatal("active error", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("guard leak")
		}
	}
	p, e := Compile(civilExprSchema(false), TryParseCivilTimestamp(Col("s"), r))
	if e != nil {
		t.Fatal(e)
	}
	b := civilExprBatch([]string{"2024-01-01T00:00:00"}, nil)
	b.Strings["s"].Offsets = []uint32{10, 0}
	if _, e = evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); e == nil {
		t.Fatal("Try swallowed storage")
	}
	reg := scalarRegister{typ: memory.TypeString, strView: phase32StringVector([]string{"2024-01-01T00:00:00"}, nil)}
	out := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, 1), ownedValidity: make(memory.Bitmap, 1), checkContext: func() error { return context.Canceled }}
	if e = evalCivilTimestamp(&out, &reg, r.state, true, 1, nil); !errors.Is(e, context.Canceled) {
		t.Fatal("Try swallowed direct cancellation", e)
	}
}

func TestCivilExprAdmissionRetryAndRunningCancel(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldEarlier)
	p, e := CompileExpressions(civilExprSchema(false), []NamedExpr{ParseCivilTimestamp(Col("s"), r).Alias("a"), TryParseCivilTimestamp(Col("s"), r).Alias("b")})
	if e != nil {
		t.Fatal(e)
	}
	values := make([]string, 130)
	for i := range values {
		values[i] = "2024-11-03T01:30:00"
	}
	b := civilExprBatch(values, nil)
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, e := NewWorkspace(context.Background(), p, probe)
	if e != nil {
		t.Fatal(e)
	}
	_, e = w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	w.Close()
	if calls == 0 {
		t.Fatal("no reservations")
	}
	t.Logf("actual initial output admission calls=%d", calls)
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		_, e = w.Evaluate(context.Background(), b, testKernelSet())
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("admission", fail, e)
		}
		m.fail = 0
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[129] != civilTestUTC(t, "2024-11-03T05:30:00Z") {
			t.Fatal("retry", fail, e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("release", fail)
		}
	}
	// Direct operator has pre-admitted scalar buffers; probe actual check count first.
	makeOutput := func() scalarRegister {
		o := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, 130), ownedValidity: make(memory.Bitmap, memory.WordsFor(130))}
		for i := range o.i64 {
			o.i64[i] = -777
		}
		return o
	}
	in := scalarRegister{typ: memory.TypeString, strView: phase32StringVector(values, nil)}
	out := makeOutput()
	checks := 0
	out.checkContext = func() error { checks++; return nil }
	if e = evalCivilTimestamp(&out, &in, r.state, true, 130, nil); e != nil {
		t.Fatal(e)
	}
	total := checks
	for _, stop := range []int{total / 2, total} {
		out = makeOutput()
		checks = 0
		out.checkContext = func() error {
			checks++
			if checks >= stop {
				return context.Canceled
			}
			return nil
		}
		e = evalCivilTimestamp(&out, &in, r.state, true, 130, nil)
		if !errors.Is(e, context.Canceled) || checks != stop {
			t.Fatal("running direct", e, checks, stop)
		}
		processed, untouched := 0, 0
		for i, v := range out.i64 {
			if memory.IsSet(out.validity, i) {
				processed++
				if v != civilTestUTC(t, "2024-11-03T05:30:00Z") {
					t.Fatal("processed")
				}
			}
			if v == -777 {
				untouched++
			}
		}
		if processed == 0 || stop < total && untouched == 0 || stop == total && processed != 130 {
			t.Fatal("cancel boundary", processed, untouched, stop, total)
		}
	}
	m := memory.NewBudgetManager(0)
	w, e = NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	base, cancel := context.WithCancel(context.Background())
	ctx := &civilExprCancelContext{Context: base, cancel: cancel, stop: 50}
	_, e = w.Evaluate(ctx, b, testKernelSet())
	cancel()
	if !errors.Is(e, context.Canceled) || m.Used() != 0 {
		t.Fatal("public running cancel", e, m.Used(), ctx.checks)
	}
	w.Close()
	lp, e := Compile(civilExprSchema(false), TryParseCivilTimestamp(Col("s"), r))
	if e != nil {
		t.Fatal(e)
	}
	long := civilExprBatch([]string{strings.Repeat("x", 1<<20)}, nil)
	v, e := evaluateValueTest(lp, long, &evaluationState{}, testKernelSet())
	if e != nil || v.Int64.IsValid(0) {
		t.Fatal("long Try", e)
	}
}

// Count only checks reached inside the new operator, not plan/setup entry.
type civilExprCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *civilExprCancelContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".evalCivilTimestamp") {
			c.checks++
			if c.checks >= c.stop {
				c.cancel()
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

func TestCivilExprDictionaryStorage(t *testing.T) {
	r := civilTestResolver(t, timezoneFixture(t, "new_york"), FoldReject)
	sc := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 4
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0, ^uint32(0)}, Offsets: []uint32{0, 19, 22}, Data: []byte("2024-01-01T00:00:00bad"), Validity: memory.Bitmap{7}}
	p, e := Compile(sc, TryParseCivilTimestamp(Col("s"), r))
	if e != nil {
		t.Fatal(e)
	}
	out, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if e != nil || out.Int64.Values[0] != civilTestUTC(t, "2024-01-01T05:00:00Z") || out.Int64.IsValid(1) || !out.Int64.IsValid(2) || out.Int64.IsValid(3) {
		t.Fatal("dictionary", e)
	}
	b.DictStrings["s"].Validity = nil
	if _, e = evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); e == nil {
		t.Fatal("Try swallowed dictionary corruption")
	}
}
