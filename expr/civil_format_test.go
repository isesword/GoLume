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
	"strings"
	"testing"
	"time"
)

type civilFormatCase struct {
	zone, instant string
	ns            int64
	want          string
}

var civilFormatCases = []civilFormatCase{
	{"utc", "", 0, "1970-01-01T00:00:00.000000000"},
	{"utc", "", -1, "1969-12-31T23:59:59.999999999"},
	{"utc", "", math.MinInt64, "1677-09-21T00:12:43.145224192"},
	{"utc", "", math.MaxInt64, "2262-04-11T23:47:16.854775807"},
	{"new_york", "2024-03-10T06:59:59.123456789Z", 0, "2024-03-10T01:59:59.123456789"},
	{"new_york", "2024-03-10T07:00:00Z", 0, "2024-03-10T03:00:00.000000000"},
	{"new_york", "2024-11-03T05:30:00Z", 0, "2024-11-03T01:30:00.000000000"},
	{"new_york", "2024-11-03T06:30:00Z", 0, "2024-11-03T01:30:00.000000000"},
	{"apia", "2011-12-30T09:59:59Z", 0, "2011-12-29T23:59:59.000000000"},
	{"apia", "2011-12-30T10:00:00Z", 0, "2011-12-31T00:00:00.000000000"},
	{"paris", "1900-01-01T00:00:00.000000001Z", 0, "1900-01-01T00:09:21.000000001"},
}

func civilFormatSchema(nullable bool) memory.Schema {
	return memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: nullable})
}
func civilFormatBatch(n int, ns int64, nullable bool) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(n)
	v := &memory.Int64Vector{Values: make([]int64, n)}
	if nullable {
		v.Validity = make(memory.Bitmap, memory.WordsFor(n))
	}
	for i := range v.Values {
		v.Values[i] = ns
		if nullable && i%7 != 0 {
			memory.Set(v.Validity, i)
		}
	}
	b.Int64s["ts"] = v
	return b
}
func TestCivilFormatHardcodedOracleAndReuse(t *testing.T) {
	for _, tc := range civilFormatCases {
		t.Run(fmt.Sprintf("%s/%s/%d", tc.zone, tc.instant, tc.ns), func(t *testing.T) {
			ns := tc.ns
			if tc.instant != "" {
				ns = civilTestUTC(t, tc.instant)
			}
			z := timezoneFixture(t, tc.zone)
			p, e := Compile(civilFormatSchema(true), FormatCivilTimestamp(Col("ts"), z))
			if e != nil {
				t.Fatal(e)
			}
			if p.SupportsSelection() {
				t.Fatal("String selected enabled")
			}
			if p.ResultField().Type != memory.TypeString || !p.ResultField().Nullable {
				t.Fatal("field")
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				m := memory.NewBudgetManager(0)
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
					out, e := w.Evaluate(context.Background(), civilFormatBatch(n, ns, true), kernel.New(mode))
					if e != nil {
						t.Fatal(e)
					}
					v := out[0].String
					if out[0].DictionaryString != nil || len(v.Offsets) != n+1 || v.Offsets[0] != 0 {
						t.Fatal("plain offsets")
					}
					for i := 0; i < n; i++ {
						valid := i%7 != 0
						if v.IsValid(i) != valid {
							t.Fatal("validity")
						}
						if valid {
							if string(v.ValueBytes(i)) != tc.want || v.Offsets[i+1]-v.Offsets[i] != 29 {
								t.Fatal(i, string(v.ValueBytes(i)), tc.want)
							}
						} else if v.Offsets[i+1] != v.Offsets[i] {
							t.Fatal("NULL payload")
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
			}
		})
	}
	// Bounds plus extreme accepted int32-second TZif offsets still have four digits.
	for _, tc := range []struct {
		ns     int64
		offset int32
		want   string
	}{
		{math.MinInt64, math.MinInt32, "1609-09-02T20:58:35.145224192"}, {math.MinInt64, math.MaxInt32, "1745-10-10T03:26:50.145224192"},
		{math.MaxInt64, math.MinInt32, "2194-03-23T20:33:08.854775807"}, {math.MaxInt64, math.MaxInt32, "2330-05-01T03:01:23.854775807"},
	} {
		z := civilTestZone(t, timezoneFixedData(tc.offset))
		p, e := Compile(memory.NewSchema(), FormatCivilTimestamp(Timestamp(tc.ns), z))
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(3)
		out, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
		if e != nil || string(out.String.ValueBytes(2)) != tc.want {
			t.Fatal("extreme constant", tc, e)
		}
	}
}

func TestCivilFormatIdentityMetadataAndOldUTC(t *testing.T) {
	data := timezoneBytes(t, "new_york")
	z, e := TimeZoneFromTZData("same", data)
	if e != nil {
		t.Fatal(e)
	}
	copyZone := z
	reload, e := TimeZoneFromTZData("same", data)
	if e != nil {
		t.Fatal(e)
	}
	other, e := TimeZoneFromTZData("same", timezoneBytes(t, "lord_howe"))
	if e != nil {
		t.Fatal(e)
	}
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "other", Type: memory.TypeTimestamp})
	p, e := CompileExpressions(sc, []NamedExpr{FormatCivilTimestamp(Col("ts"), z).Alias("a"), FormatCivilTimestamp(Col("ts"), copyZone).Alias("copy"), FormatCivilTimestamp(Col("ts"), reload).Alias("reload"), FormatCivilTimestamp(Col("ts"), other).Alias("rules"), FormatCivilTimestamp(Col("other"), z).Alias("input"), CastTo(Col("ts"), memory.TypeString).Alias("old")})
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copy CSE")
	}
	for i := 2; i < len(p.outputs); i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("identity", i, j)
			}
		}
	}
	bound, e := Bind(sc, FormatCivilTimestamp(Col("ts"), z))
	if e != nil || bound.MayError || bound.Field.Type != memory.TypeString || bound.Field.Nullable || !reflect.DeepEqual(Columns(bound.Expr), []string{"ts"}) {
		t.Fatal("metadata", e)
	}
	for _, ex := range []Expr{FormatCivilTimestamp(I64(0), z), FormatCivilTimestamp(Col("ts"), TimeZone{}), If(Bool(false), FormatCivilTimestamp(Col("ts"), TimeZone{}), Str("safe")), FormatCivilTimestamp(nil, z)} {
		if _, e := Compile(sc, ex); e == nil {
			t.Fatal("invalid accepted", ex)
		}
	}
	for i := range data {
		data[i] = 0
	}
	z = TimeZone{}
	copyZone = TimeZone{}
	reload = TimeZone{}
	other = TimeZone{}
	runtime.GC()
	b := civilFormatBatch(1, 0, false)
	b.Int64s["other"] = &memory.Int64Vector{Values: []int64{1}}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	for i, want := range []string{"1969-12-31T19:00:00.000000000", "1969-12-31T19:00:00.000000000", "1969-12-31T19:00:00.000000000", "1970-01-01T10:00:00.000000000", "1969-12-31T19:00:00.000000001", "0"} {
		if string(out[i].String.ValueBytes(0)) != want {
			t.Fatal(i, string(out[i].String.ValueBytes(0)), want)
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}

func TestCivilFormatDynamicGuards(t *testing.T) {
	z := timezoneFixture(t, "utc")
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeString, Nullable: true})
	formatted := FormatCivilTimestamp(Col("ts"), z)
	cond := GT(Col("i"), I64(0))
	for _, ex := range []Expr{If(cond, formatted, Str("fallback")), Case([]WhenThen{{When: cond, Then: formatted}}, Str("fallback")), Coalesce(Col("fallback"), formatted)} {
		p, e := Compile(sc, ex)
		if e != nil {
			t.Fatal(e)
		}
		b := civilFormatBatch(3, 0, false)
		b.Int64s["ts"].Validity = memory.Bitmap{3}
		b.Int64s["i"] = &memory.Int64Vector{Values: []int64{0, 1, 1}}
		b.Strings["fallback"] = phase32StringVector([]string{"fallback", "", ""}, []bool{true, false, false})
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		out, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil {
			t.Fatal(e)
		}
		if string(out[0].String.ValueBytes(0)) != "fallback" || string(out[0].String.ValueBytes(1)) != "1970-01-01T00:00:00.000000000" || out[0].String.IsValid(2) {
			t.Fatal("guard")
		}
		b.Int64s["i"].Values[0] = 1
		b.Strings["fallback"].Validity = memory.Bitmap{0}
		b.Int64s["ts"].Values[0] = -1
		out, e = w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || string(out[0].String.ValueBytes(0)) != "1969-12-31T23:59:59.999999999" {
			t.Fatal("reuse", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestCivilFormatInitialAndWarmGrowthRetry(t *testing.T) {
	z := timezoneFixture(t, "utc")
	p, e := Compile(civilFormatSchema(true), FormatCivilTimestamp(Col("ts"), z))
	if e != nil {
		t.Fatal(e)
	}
	big := civilFormatBatch(130, -1, false)
	for _, warmRows := range []int{0, 1, 130} {
		warm := civilFormatBatch(warmRows, 0, false)
		if warmRows == 130 {
			warm.Int64s["ts"].Validity = make(memory.Bitmap, memory.WordsFor(warmRows))
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, e := NewWorkspace(context.Background(), p, probe)
		if e != nil {
			t.Fatal(e)
		}
		if warmRows != 0 {
			if _, e = w.Evaluate(context.Background(), warm, testKernelSet()); e != nil {
				t.Fatal(e)
			}
		}
		before := probe.calls
		if _, e = w.Evaluate(context.Background(), big, testKernelSet()); e != nil {
			t.Fatal(e)
		}
		growthCalls := probe.calls - before
		w.Close()
		if growthCalls == 0 || probe.Used() != 0 {
			t.Fatal("no real growth", warmRows, growthCalls)
		}
		t.Logf("warmRows=%d growth reservations=%d", warmRows, growthCalls)
		for fail := 1; fail <= growthCalls; fail++ {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			if warmRows != 0 {
				if _, e = w.Evaluate(context.Background(), warm, testKernelSet()); e != nil {
					t.Fatal(e)
				}
			}
			m.fail = m.calls + fail
			if _, e = w.Evaluate(context.Background(), big, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("growth failure", warmRows, fail, e)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("failed accounting")
			}
			m.fail = 0
			out, e := w.Evaluate(context.Background(), big, testKernelSet())
			if e != nil {
				t.Fatal("retry", warmRows, fail, e)
			}
			v := out[0].String
			for i := 0; i < 130; i++ {
				if !v.IsValid(i) || string(v.ValueBytes(i)) != "1969-12-31T23:59:59.999999999" || v.Offsets[i] != uint32(i*29) {
					t.Fatal("partial state leaked into retry", warmRows, fail, i)
				}
			}
			calls := m.calls
			for j := 0; j < 3; j++ {
				if _, e = w.Evaluate(context.Background(), big, testKernelSet()); e != nil {
					t.Fatal(e)
				}
			}
			if calls != m.calls {
				t.Fatal("warm allocation")
			}
			w.Close()
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
}

type civilFormatCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *civilFormatCancelContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".evalCivilFormat") {
			c.checks++
			if c.stop > 0 && c.checks >= c.stop {
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
func TestCivilFormatCancellationAndDemandedStorage(t *testing.T) {
	z := timezoneFixture(t, "utc")
	const n = 130
	makeOut := func(rows int) scalarRegister {
		return scalarRegister{typ: memory.TypeString, offsets: make([]uint32, rows+1), ownedValidity: make(memory.Bitmap, memory.WordsFor(rows)), data: make([]byte, 0, 29*rows), checkContext: func() error { return nil }}
	}
	in := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, n)}
	out := makeOut(n)
	checks := 0
	out.checkContext = func() error { checks++; return nil }
	if e := evalCivilFormat(&out, &in, z.state, n, nil); e != nil {
		t.Fatal(e)
	}
	total := checks
	for _, stop := range []int{1, total / 2, total} {
		out = makeOut(n)
		checks = 0
		out.checkContext = func() error {
			checks++
			if checks >= stop {
				return context.Canceled
			}
			return nil
		}
		e := evalCivilFormat(&out, &in, z.state, n, nil)
		if !errors.Is(e, context.Canceled) || checks != stop {
			t.Fatal("direct cancel", stop, e)
		}
		processed := 0
		for i := 0; i < n; i++ {
			if memory.IsSet(out.validity, i) {
				processed++
				if out.offsets[i+1]-out.offsets[i] != 29 {
					t.Fatal("published partial row")
				}
			}
		}
		if stop == 1 && processed != 0 || stop == total && processed != n || stop > 1 && stop < total && (processed == 0 || processed == n) {
			t.Fatal("cancel progress", stop, processed)
		}
	}
	p, e := Compile(civilFormatSchema(false), FormatCivilTimestamp(Col("ts"), z))
	if e != nil {
		t.Fatal(e)
	}
	b := civilFormatBatch(n, 0, false)
	run := func(stop int) int {
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		base, cancel := context.WithCancel(context.Background())
		ctx := &civilFormatCancelContext{Context: base, cancel: cancel, stop: stop}
		_, e = w.Evaluate(ctx, b, testKernelSet())
		cancel()
		if stop == 0 && e != nil || stop > 0 && (!errors.Is(e, context.Canceled) || ctx.checks != stop) {
			t.Fatal("public cancel", stop, e, ctx.checks)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("cancel leak")
		}
		return ctx.checks
	}
	publicTotal := run(0)
	for _, stop := range []int{1, publicTotal / 2, publicTotal} {
		run(stop)
	}
	short := scalarRegister{typ: memory.TypeTimestamp}
	out = makeOut(1)
	if e := evalCivilFormat(&out, &short, z.state, 1, nil); e == nil {
		t.Fatal("short demanded storage")
	}
	out = makeOut(1)
	if e := evalCivilFormat(&out, &short, z.state, 1, memory.Bitmap{0}); e != nil || len(out.data) != 0 || out.offsets[1] != 0 {
		t.Fatal("inactive read", e)
	}
	short.validity = memory.Bitmap{0}
	if e := evalCivilFormat(&out, &short, z.state, 1, nil); e != nil || memory.IsSet(out.validity, 0) {
		t.Fatal("NULL read", e)
	}
	out = makeOut(n)
	if e := evalCivilFormat(&out, &in, z.state, n, memory.Bitmap{0}); e == nil {
		t.Fatal("short active bitmap")
	}
	in.validity = memory.Bitmap{0}
	if e := evalCivilFormat(&out, &in, z.state, n, nil); e == nil {
		t.Fatal("short validity")
	}
	var scratch [29]byte
	instant := time.Unix(0, -1).UTC()
	var formatErr error
	if allocations := testing.AllocsPerRun(100, func() { formatErr = writeCivilFormat(&scratch, instant) }); allocations != 0 || formatErr != nil {
		t.Fatal("scratch formatting allocation", allocations, formatErr)
	}
}
