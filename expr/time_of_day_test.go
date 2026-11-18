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

var timeOfDayParts = []func(Expr, TimeZone) ZonedExtractExpr{TimeOfDayInZone}

type timeOfDayCase struct {
	zone, instant string
	ns            int64
	want          [1]int64
}

// Hardcoded local clock oracle. These coefficients encode wall-clock fields;
// notably New York's spring 03:00 is not two elapsed hours since midnight.
var timeOfDayCases = []timeOfDayCase{
	{"utc", "", 0, [1]int64{0}},
	{"utc", "", -1, [1]int64{86400e9 - 1}},
	{"utc", "", math.MinInt64, [1]int64{763145224192}},
	{"utc", "", math.MaxInt64, [1]int64{85636854775807}},
	{"new_york", "2024-03-10T06:59:59.123456789Z", 0, [1]int64{7199123456789}},
	{"new_york", "2024-03-10T07:00:00Z", 0, [1]int64{10800e9}},
	{"new_york", "2024-11-03T05:30:00Z", 0, [1]int64{5400e9}},
	{"new_york", "2024-11-03T06:30:00Z", 0, [1]int64{5400e9}},
	{"new_york", "", -1, [1]int64{68400e9 - 1}},
	{"apia", "2011-12-30T09:59:59Z", 0, [1]int64{86399e9}},
	{"apia", "2011-12-30T10:00:00Z", 0, [1]int64{0}},
	{"paris", "1900-01-01T00:00:00.000000001Z", 0, [1]int64{561e9 + 1}},
}

func timeOfDayNanos(t *testing.T, tc timeOfDayCase) int64 {
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
func TestTimeOfDayHardcodedOracle(t *testing.T) {
	for _, tc := range timeOfDayCases {
		t.Run(fmt.Sprintf("%s/%s/%d", tc.zone, tc.instant, tc.ns), func(t *testing.T) {
			z := timezoneFixture(t, tc.zone)
			schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true})
			es := []NamedExpr{}
			for i, fn := range timeOfDayParts {
				es = append(es, Named(fn(Col("ts"), z), fmt.Sprint(i)))
			}
			p, err := CompileExpressions(schema, es)
			if err != nil {
				t.Fatal(err)
			}
			if p.OutputField(0).Type != memory.TypeTime64 || !p.OutputField(0).Nullable {
				t.Fatal("Time64 metadata")
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
						v.Values[i] = timeOfDayNanos(t, tc)
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

func TestTimeOfDayIdentityMetadataAndLiveness(t *testing.T) {
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
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "other", Type: memory.TypeTimestamp})
	p, e := CompileExpressions(sc, []NamedExpr{TimeOfDayInZone(Col("ts"), z).Alias("a"), TimeOfDayInZone(Col("ts"), copyZone).Alias("copy"), TimeOfDayInZone(Col("ts"), reload).Alias("reload"), TimeOfDayInZone(Col("ts"), other).Alias("rules"), TimeOfDayInZone(Col("other"), z).Alias("input"), HourInZone(Col("ts"), z).Alias("hour"), Year(Col("ts")).Alias("old_year"), Col("ts").Alias("raw")})
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copy CSE")
	}
	for i := 2; i < len(p.outputs); i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("merged identity/input/part", i, j)
			}
		}
	}
	bound, e := Bind(sc, TimeOfDayInZone(Col("ts"), z))
	if e != nil || bound.MayError || bound.Field.Type != memory.TypeTime64 || bound.Field.Nullable || bound.Field.Encoding != memory.EncodingPlain || !reflect.DeepEqual(Columns(bound.Expr), []string{"ts"}) {
		t.Fatal("metadata", e)
	}
	for _, ex := range []Expr{TimeOfDayInZone(I64(0), z), TimeOfDayInZone(Col("ts"), TimeZone{}), If(Bool(false), TimeOfDayInZone(Col("ts"), TimeZone{}), TimeOfDayInZone(Timestamp(0), z)), TimeOfDayInZone(nil, z), Unary{Op: OpTimeOfDay, Value: Col("ts")}} {
		if _, e := Compile(sc, ex); e == nil {
			t.Fatal("invalid/implicit API", ex)
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
	for i, want := range []int64{68400e9, 68400e9, 68400e9, 36000e9, 68400e9 + 1, 19, 1970, 0} {
		if out[i].Int64.Values[0] != want {
			t.Fatal(i, out[i].Int64.Values[0], want)
		}
		if i < 5 && out[i].Type != memory.TypeTime64 {
			t.Fatal("logical type erased")
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
	for _, tc := range []struct {
		ns     int64
		offset int32
		want   int64
	}{{math.MinInt64, math.MinInt32, (20*3600+58*60+35)*1e9 + 145224192}, {math.MinInt64, math.MaxInt32, (3*3600+26*60+50)*1e9 + 145224192}, {math.MaxInt64, math.MinInt32, (20*3600+33*60+8)*1e9 + 854775807}, {math.MaxInt64, math.MaxInt32, (3*3600+60+23)*1e9 + 854775807}} {
		zone := civilTestZone(t, timezoneFixedData(tc.offset))
		p, e := Compile(memory.NewSchema(), TimeOfDayInZone(Timestamp(tc.ns), zone))
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Reset(3)
		out, e := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
		if e != nil || out.Type != memory.TypeTime64 || out.Int64.Values[2] != tc.want {
			t.Fatal("extreme constant", tc, e)
		}
	}
}

func TestTimeOfDayGuardsAndAdmission(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	sc := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "fallback", Type: memory.TypeTime64, Nullable: true})
	v := TimeOfDayInZone(Col("ts"), z)
	cond := GT(Col("i"), I64(0))
	for _, ex := range []Expr{If(cond, v, Col("fallback")), Case([]WhenThen{{When: cond, Then: v}}, Col("fallback")), Coalesce(Col("fallback"), v)} {
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
		if e != nil || out[0].Type != memory.TypeTime64 || out[0].Int64.Values[0] != -77 || out[0].Int64.Values[1] != 68400e9 || out[0].Int64.IsValid(2) {
			t.Fatal("guard/old Time64 domain", e)
		}
		b.Int64s["i"].Values[0] = 1
		b.Int64s["fallback"].Validity = memory.Bitmap{0}
		out, e = w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || out[0].Int64.Values[0] != 68400e9 {
			t.Fatal("reuse", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	p, e := Compile(sc, v)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(130)
	b.Int64s["ts"] = &memory.Int64Vector{Values: make([]int64, 130)}
	for _, selected := range []bool{false, true} {
		eval := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, []int{0, 63, 64, 129}, testKernelSet())
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
		if calls == 0 {
			t.Fatal("no admission")
		}
		for fail := 1; fail <= calls; fail++ {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = eval(w); !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("reserve", fail, e)
			}
			m.fail = 0
			out, e := eval(w)
			if e != nil || out[0].Type != memory.TypeTime64 || out[0].Int64.Values[0] != 68400e9 {
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
	}
}

func TestTimeOfDayCancellation(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	const n = 130
	for _, stop := range []int{1, 2, 4} {
		in := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, n)}
		out := scalarRegister{typ: memory.TypeTime64, i64: make([]int64, n), ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
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
		if e := evalZonedExtract(&out, &in, z.state, OpTimeOfDay, n); !errors.Is(e, context.Canceled) || checks != stop {
			t.Fatal("direct cancellation", e)
		}
		done := (stop - 1) * 64
		if done > n {
			done = n
		}
		for i, v := range out.i64 {
			want := int64(-777)
			if i < done {
				want = 68400e9
			}
			if v != want {
				t.Fatal("cancellation boundary", stop, i, v)
			}
		}
		p, e := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), TimeOfDayInZone(Col("ts"), z))
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
				t.Fatal("public cancellation", selected, e)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
}
