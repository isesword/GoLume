package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"
)

var isoCalendarParts = []func(Expr, TimeZone) ZonedExtractExpr{ISOWeekYearInZone, ISOWeekInZone, ISOWeekdayInZone, UTCOffsetInZone}

type isoCalendarCase struct {
	zone, instant string
	ns            int64
	want          [4]int64
}

// Expected local ISO triples are fixed calendar facts, independently checked
// using Python datetime.date.isocalendar, not Go's production ISOWeek call.
var isoCalendarCases = []isoCalendarCase{
	{"utc", "", -1, [4]int64{1970, 1, 3, 0}},
	{"utc", "", math.MinInt64, [4]int64{1677, 38, 2, 0}},
	{"utc", "", math.MaxInt64, [4]int64{2262, 15, 5, 0}},
	{"utc", "2015-12-31T23:59:59Z", 0, [4]int64{2015, 53, 4, 0}},
	{"utc", "2016-01-01T00:00:00Z", 0, [4]int64{2015, 53, 5, 0}},
	{"utc", "2021-01-01T12:00:00Z", 0, [4]int64{2020, 53, 5, 0}},
	{"utc", "2021-01-03T23:00:00Z", 0, [4]int64{2020, 53, 7, 0}},
	{"utc", "2021-01-04T00:00:00Z", 0, [4]int64{2021, 1, 1, 0}},
	{"new_york", "2021-01-04T00:00:00Z", 0, [4]int64{2020, 53, 7, -18000}},
	{"new_york", "2024-11-03T05:30:00Z", 0, [4]int64{2024, 44, 7, -14400}},
	{"new_york", "2024-11-03T06:30:00Z", 0, [4]int64{2024, 44, 7, -18000}},
	{"apia", "2011-12-30T09:59:59Z", 0, [4]int64{2011, 52, 4, -36000}},
	{"apia", "2011-12-30T10:00:00Z", 0, [4]int64{2011, 52, 6, 50400}},
	{"paris", "1900-01-01T00:00:00Z", 0, [4]int64{1900, 1, 1, 561}},
}

func isoCalendarNanos(t *testing.T, tc isoCalendarCase) int64 {
	t.Helper()
	if tc.instant == "" {
		return tc.ns
	}
	v, e := time.Parse(time.RFC3339Nano, tc.instant)
	if e != nil {
		t.Fatal(e)
	}
	return v.UnixNano()
}
func TestISOCalendarHardcodedOracle(t *testing.T) {
	for _, tc := range isoCalendarCases {
		t.Run(fmt.Sprintf("%s/%s/%d", tc.zone, tc.instant, tc.ns), func(t *testing.T) {
			z := timezoneFixture(t, tc.zone)
			schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true})
			es := []NamedExpr{}
			for i, fn := range isoCalendarParts {
				es = append(es, Named(fn(Col("ts"), z), fmt.Sprint(i)))
			}
			p, err := CompileExpressions(schema, es)
			if err != nil {
				t.Fatal(err)
			}
			if !p.SupportsSelection() {
				t.Fatal("selected path missing")
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				m := memory.NewBudgetManager(0)
				w, _ := NewWorkspace(context.Background(), p, m)
				for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
					b := memory.NewBatch()
					b.Rows = n
					v := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
					for i := range v.Values {
						v.Values[i] = isoCalendarNanos(t, tc)
						if i%7 != 0 {
							memory.Set(v.Validity, i)
						} else {
							v.Values[i] = math.MinInt64
						}
					}
					b.Int64s["ts"] = v
					for _, selected := range []bool{false, true} {
						rows := make([]int, 0)
						if selected {
							for i := 0; i < n; i += 2 {
								rows = append(rows, i)
							}
						}
						var out []ScalarValue
						if selected {
							out, err = w.EvaluateSelected(context.Background(), b, rows, kernel.New(mode))
						} else {
							out, err = w.Evaluate(context.Background(), b, kernel.New(mode))
						}
						if err != nil {
							t.Fatal(err)
						}
						count := n
						if selected {
							count = len(rows)
						}
						for part, val := range out {
							for i := 0; i < count; i++ {
								row := i
								if selected {
									row = rows[i]
								}
								valid := row%7 != 0
								want := int64(0)
								if valid {
									want = tc.want[part]
								}
								if val.Int64.IsValid(i) != valid || val.Int64.Values[i] != want {
									t.Fatalf("part%d row%d got%d valid%v want%d/%v", part, row, val.Int64.Values[i], val.Int64.IsValid(i), want, valid)
								}
							}
						}
					}
					if m.Used() != w.MemoryBytes() {
						t.Fatal("accounting drift")
					}
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("close leak")
				}
			}
		})
	}
}

func TestISOCalendarIdentityMetadataAndCompatibility(t *testing.T) {
	raw := timezoneBytes(t, "new_york")
	z, e := TimeZoneFromTZData("same", raw)
	if e != nil {
		t.Fatal(e)
	}
	copyZone := z
	reload, e := TimeZoneFromTZData("same", raw)
	if e != nil {
		t.Fatal(e)
	}
	other, e := TimeZoneFromTZData("same", timezoneBytes(t, "lord_howe"))
	if e != nil {
		t.Fatal(e)
	}
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "other", Type: memory.TypeTimestamp})
	p, e := CompileExpressions(sc, []NamedExpr{UTCOffsetInZone(Col("ts"), z).Alias("a"), UTCOffsetInZone(Col("ts"), copyZone).Alias("copy"), UTCOffsetInZone(Col("ts"), reload).Alias("reload"), UTCOffsetInZone(Col("ts"), other).Alias("rules"), ISOWeekdayInZone(Col("ts"), z).Alias("part"), UTCOffsetInZone(Col("other"), z).Alias("input")})
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copy identity")
	}
	for i := 2; i < len(p.outputs); i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("merged identity/part/input", i, j)
			}
		}
	}
	for _, fn := range isoCalendarParts {
		b, e := Bind(sc, fn(Col("ts"), z))
		if e != nil || b.Field.Type != memory.TypeInt64 || !b.Field.Nullable || b.MayError || !reflect.DeepEqual(Columns(b.Expr), []string{"ts"}) {
			t.Fatal("metadata", b, e)
		}
		for _, ex := range []Expr{fn(I64(0), z), fn(Col("ts"), TimeZone{}), If(Bool(false), fn(Col("ts"), TimeZone{}), I64(0)), fn(nil, z)} {
			if _, e := Compile(sc, ex); e == nil {
				t.Fatal("invalid bind", ex)
			}
		}
	}
	for i := range raw {
		raw[i] = 0
	}
	z = TimeZone{}
	copyZone = TimeZone{}
	reload = TimeZone{}
	other = TimeZone{}
	runtime.GC()
	b := memory.NewBatch()
	b.Reset(1)
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0}}
	b.Int64s["other"] = &memory.Int64Vector{Values: []int64{0}}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	for i, want := range []int64{-18000, -18000, -18000, 36000, 3, -18000} {
		if out[i].Int64.Values[0] != want {
			t.Fatal(i, out[i].Int64.Values[0])
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
	ny := timezoneFixture(t, "new_york")
	p, e = CompileExpressions(sc, []NamedExpr{Year(Col("ts")).Alias("utc_year"), YearInZone(Col("ts"), ny).Alias("local_year"), ISOWeekYearInZone(Col("ts"), ny).Alias("iso_year"), CastTo(Col("ts"), memory.TypeDate32).Alias("utc_date")})
	if e != nil {
		t.Fatal(e)
	}
	b.Int64s["ts"].Values[0] = civilTestUTC(t, "2021-01-01T00:00:00Z")
	w, e = NewWorkspace(context.Background(), p, nil)
	if e != nil {
		t.Fatal(e)
	}
	out, e = w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	if out[0].Int64.Values[0] != 2021 || out[1].Int64.Values[0] != 2020 || out[2].Int64.Values[0] != 2020 || out[3].Int32.Values[0] != 18628 {
		t.Fatal("old UTC semantics")
	}
	w.Close()
	// Appended operators do not silently extend ordinary unzoned Unary APIs.
	for _, op := range []Op{OpISOWeekYear, OpISOWeek, OpISOWeekday, OpUTCOffset} {
		if _, e := Compile(sc, Unary{Op: op, Value: Col("ts")}); e == nil {
			t.Fatal("implicit unzoned API", op)
		}
	}
}

func TestISOCalendarGuardsAndAdmission(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeInt64, Nullable: true})
	for part, fn := range isoCalendarParts {
		v := fn(Col("ts"), z)
		cond := GT(Col("i"), I64(0))
		for _, ex := range []Expr{If(cond, v, I64(-77)), Case([]WhenThen{{When: cond, Then: v}}, I64(-77)), Coalesce(Col("fallback"), v)} {
			p, e := Compile(sc, ex)
			if e != nil {
				t.Fatal(e)
			}
			b := memory.NewBatch()
			b.Reset(3)
			b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0, 0, 0}, Validity: memory.Bitmap{3}}
			b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1, 1}}
			b.Int64s["fallback"] = &memory.Int64Vector{Values: []int64{-77, 0, 0}, Validity: memory.Bitmap{1}}
			m := memory.NewBudgetManager(0)
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			out, e := w.Evaluate(context.Background(), b, testKernelSet())
			if e != nil {
				t.Fatal(e)
			}
			want := []int64{1970, 1, 3, -18000}[part]
			if out[0].Int64.Values[0] != -77 || out[0].Int64.Values[1] != want || out[0].Int64.IsValid(2) {
				t.Fatal("guard")
			}
			b.Int64s["i"].Values[0] = 1
			b.Int64s["fallback"].Validity = memory.Bitmap{0}
			out, e = w.Evaluate(context.Background(), b, testKernelSet())
			if e != nil || out[0].Int64.Values[0] != want {
				t.Fatal("reuse", e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("guard leak")
			}
		}
	}
	es := []NamedExpr{}
	for i, fn := range isoCalendarParts {
		es = append(es, fn(Col("ts"), z).Alias(fmt.Sprint(i)))
	}
	p, e := CompileExpressions(sc, es)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(130)
	b.Int64s["ts"] = &memory.Int64Vector{Values: make([]int64, 130)}
	rows := []int{0, 63, 64, 129}
	for _, selected := range []bool{false, true} {
		eval := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, rows, testKernelSet())
			}
			return w.Evaluate(context.Background(), b, testKernelSet())
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, e := NewWorkspace(context.Background(), p, probe)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = eval(w); e != nil {
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
			if _, e = eval(w); !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("reserve", selected, fail, e)
			}
			m.fail = 0
			out, e := eval(w)
			if e != nil || out[3].Int64.Values[0] != -18000 {
				t.Fatal("retry", e)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("actual accounting")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
}

func TestISOCalendarCancellation(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	for part, op := range []Op{OpISOWeekYear, OpISOWeek, OpISOWeekday, OpUTCOffset} {
		const n = 130
		for _, stop := range []int{1, 2, 4} {
			in := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, n)}
			out := scalarRegister{typ: memory.TypeInt64, i64: make([]int64, n), ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
			for i := range out.i64 {
				out.i64[i] = -777
			}
			checks := 0
			out.checkContext = func() error {
				checks++
				if checks >= stop {
					return context.Canceled
				}
				return nil
			}
			if e := evalZonedExtract(&out, &in, z.state, op, n); !errors.Is(e, context.Canceled) || checks != stop {
				t.Fatal("direct cancel", op, stop, e)
			}
			done := (stop - 1) * 64
			if done > n {
				done = n
			}
			for i, v := range out.i64 {
				want := int64(-777)
				if i < done {
					want = []int64{1970, 1, 3, -18000}[part]
				}
				if v != want {
					t.Fatal("cancel boundary", part, stop, i, v, want)
				}
			}
			p, e := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), isoCalendarParts[part](Col("ts"), z))
			if e != nil {
				t.Fatal(e)
			}
			for _, selected := range []bool{false, true} {
				m := memory.NewBudgetManager(0)
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				base, cancel := context.WithCancel(context.Background())
				ctx := &timezoneCancelContext{Context: base, cancel: cancel, stop: stop}
				if selected {
					rows := make([]int, n)
					for i := range rows {
						rows[i] = i
					}
					_, e = w.EvaluateSelected(ctx, timezoneBatch(n), rows, testKernelSet())
				} else {
					_, e = w.Evaluate(ctx, timezoneBatch(n), testKernelSet())
				}
				cancel()
				if !errors.Is(e, context.Canceled) || ctx.checks != stop {
					t.Fatal("public cancel", part, selected, stop, e, ctx.checks)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("cancel leak")
				}
			}
		}
	}
}
