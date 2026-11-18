package expr

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func timezoneBytes(t *testing.T, name string) []byte {
	t.Helper()
	s, err := os.ReadFile(filepath.Join("testdata", "timezone", name+".tzif.b64"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(s)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func timezoneFixture(t *testing.T, name string) TimeZone {
	t.Helper()
	if name == "utc" {
		z, err := LoadTimeZone("UTC")
		if err != nil {
			t.Fatal(err)
		}
		return z
	}
	z, err := TimeZoneFromTZData(name, timezoneBytes(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return z
}

var timezoneParts = []func(Expr, TimeZone) ZonedExtractExpr{YearInZone, MonthInZone, DayInZone, HourInZone, MinuteInZone, SecondInZone}

type timezoneCase struct {
	zone, instant string
	ns            int64
	want          [6]int64
}

var timezoneCases = []timezoneCase{
	{"utc", "", -1, [6]int64{1969, 12, 31, 23, 59, 59}},
	{"utc", "", math.MinInt64, [6]int64{1677, 9, 21, 0, 12, 43}},
	{"utc", "", math.MaxInt64, [6]int64{2262, 4, 11, 23, 47, 16}},
	{"utc", "1970-01-01T00:00:00Z", 0, [6]int64{1970, 1, 1, 0, 0, 0}},
	{"new_york", "2024-03-10T06:59:59.999999999Z", 0, [6]int64{2024, 3, 10, 1, 59, 59}},
	{"new_york", "2024-03-10T07:00:00Z", 0, [6]int64{2024, 3, 10, 3, 0, 0}},
	{"new_york", "2024-11-03T05:59:59.999999999Z", 0, [6]int64{2024, 11, 3, 1, 59, 59}},
	{"new_york", "2024-11-03T06:00:00Z", 0, [6]int64{2024, 11, 3, 1, 0, 0}},
	{"new_york", "2024-11-03T06:30:00Z", 0, [6]int64{2024, 11, 3, 1, 30, 0}},
	{"new_york", "", -1, [6]int64{1969, 12, 31, 18, 59, 59}},
	{"new_york", "", math.MaxInt64, [6]int64{2262, 4, 11, 19, 47, 16}},
	{"lord_howe", "2024-04-06T14:59:59Z", 0, [6]int64{2024, 4, 7, 1, 59, 59}},
	{"lord_howe", "2024-04-06T15:00:00Z", 0, [6]int64{2024, 4, 7, 1, 30, 0}},
	{"lord_howe", "2024-10-05T15:29:59Z", 0, [6]int64{2024, 10, 6, 1, 59, 59}},
	{"lord_howe", "2024-10-05T15:30:00Z", 0, [6]int64{2024, 10, 6, 2, 30, 0}},
	{"apia", "2011-12-30T09:59:59Z", 0, [6]int64{2011, 12, 29, 23, 59, 59}},
	{"apia", "2011-12-30T10:00:00Z", 0, [6]int64{2011, 12, 31, 0, 0, 0}},
	{"paris", "1900-01-01T00:00:00Z", 0, [6]int64{1900, 1, 1, 0, 9, 21}},
}

func timezoneNanos(t *testing.T, tc timezoneCase) int64 {
	t.Helper()
	if tc.instant == "" {
		return tc.ns
	}
	v, err := time.Parse(time.RFC3339Nano, tc.instant)
	if err != nil {
		t.Fatal(err)
	}
	return v.UnixNano()
}
func TestTimeZoneHardcodedCalendarOracle(t *testing.T) {
	for _, tc := range timezoneCases {
		t.Run(fmt.Sprintf("%s/%s/%d", tc.zone, tc.instant, tc.ns), func(t *testing.T) {
			z := timezoneFixture(t, tc.zone)
			schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true})
			es := []NamedExpr{}
			for i, fn := range timezoneParts {
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
						v.Values[i] = timezoneNanos(t, tc)
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

func TestTimeZoneNamesMetadataAndIdentity(t *testing.T) {
	bytes := timezoneBytes(t, "new_york")
	for _, name := range []string{"", "Local", "nul\x00name", string([]byte{255}), strings.Repeat("a", 256)} {
		if _, err := TimeZoneFromTZData(name, bytes); err == nil {
			t.Fatalf("name accepted %q", name)
		}
		if _, err := LoadTimeZone(name); err == nil {
			t.Fatalf("loader name accepted %q", name)
		}
	}
	a, err := TimeZoneFromTZData("same", bytes)
	if err != nil {
		t.Fatal(err)
	}
	copyHandle := a
	b, err := TimeZoneFromTZData("same", bytes)
	if err != nil {
		t.Fatal(err)
	}
	other, err := TimeZoneFromTZData("same", timezoneBytes(t, "lord_howe"))
	if err != nil {
		t.Fatal(err)
	}
	if a.Name() != "same" || (TimeZone{}).Name() != "" {
		t.Fatal("name contract")
	}
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true})
	p, err := CompileExpressions(schema, []NamedExpr{Named(HourInZone(Col("ts"), a), "a"), Named(HourInZone(Col("ts"), copyHandle), "copy"), Named(HourInZone(Col("ts"), b), "reload"), Named(HourInZone(Col("ts"), other), "other"), Named(MinuteInZone(Col("ts"), a), "part")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("same handle CSE")
	}
	for _, i := range []int{2, 3, 4} {
		if p.outputs[0].root == p.outputs[i].root {
			t.Fatal("independent identity/part merged")
		}
	}
	for _, fn := range timezoneParts {
		bound, err := Bind(schema, fn(Col("ts"), a))
		if err != nil {
			t.Fatal(err)
		}
		if bound.Field.Type != memory.TypeInt64 || !bound.Field.Nullable || bound.MayError || !reflect.DeepEqual(Columns(bound.Expr), []string{"ts"}) {
			t.Fatal("bound metadata")
		}
	}
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeDate32, memory.TypeDate64, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration, memory.TypeString, memory.TypeBool, memory.TypeFloat64, memory.TypeDecimal} {
		s := memory.NewSchema(memory.Field{Name: "bad", Type: typ})
		if _, err := Compile(s, YearInZone(Col("bad"), a)); err == nil {
			t.Fatalf("accepted %s", typ)
		}
	}
	for _, e := range []Expr{YearInZone(Col("ts"), TimeZone{}), If(Bool(false), YearInZone(Col("ts"), TimeZone{}), I64(0)), YearInZone(nil, a)} {
		if _, err := Compile(schema, e); err == nil {
			t.Fatal("invalid zone/value accepted")
		}
	}
	// Mutating caller-owned bytes, dropping every external handle and GC must
	// not alter already compiled rules. A plan strongly owns its captured state.
	for i := range bytes {
		bytes[i] = 0
	}
	a = TimeZone{}
	b = TimeZone{}
	other = TimeZone{}
	copyHandle = TimeZone{}
	runtime.GC()
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0}}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	out, err := w.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Int64.Values[0] != 19 || out[1].Int64.Values[0] != 19 || out[2].Int64.Values[0] != 19 || out[3].Int64.Values[0] != 10 {
		t.Fatal("immutable same-name rules/identity")
	}
}

func timezoneFixedData(offset int32) []byte {
	b := make([]byte, 54)
	copy(b, "TZif")
	binary.BigEndian.PutUint32(b[36:40], 1)
	binary.BigEndian.PutUint32(b[40:44], 4)
	binary.BigEndian.PutUint32(b[44:48], uint32(offset))
	copy(b[50:], "FIX\x00")
	return b
}
func TestTimeZoneTZifValidation(t *testing.T) {
	good := timezoneBytes(t, "new_york")
	cases := map[string][]byte{"empty": nil, "truncated": good[:20], "too_large": make([]byte, (1<<20)+1), "bad_magic": append([]byte(nil), good...), "bad_version": append([]byte(nil), good...), "leap": append([]byte(nil), good...), "huge_counts": append([]byte(nil), good...)}
	cases["bad_magic"][0] = 'X'
	cases["bad_version"][4] = '4'
	binary.BigEndian.PutUint32(cases["leap"][28:32], 1)
	binary.BigEndian.PutUint32(cases["huge_counts"][32:36], math.MaxUint32)
	// Locate the second header using file-format lengths, not production parser.
	counts := func(i int) int { return int(binary.BigEndian.Uint32(good[20+i*4 : 24+i*4])) }
	second := 44 + counts(0) + counts(1) + counts(2)*8 + counts(3)*5 + counts(4)*6 + counts(5)
	if good[4] != '\x00' {
		cases["second_truncated"] = good[:second+20]
		v := append([]byte(nil), good...)
		v[second+4] = '4'
		cases["second_version"] = v
		v = append([]byte(nil), good...)
		binary.BigEndian.PutUint32(v[second+28:second+32], 1)
		cases["second_leap"] = v
		v = append([]byte(nil), good...)
		binary.BigEndian.PutUint32(v[second+32:second+36], math.MaxUint32)
		cases["second_counts"] = v
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := TimeZoneFromTZData("test", data); err == nil {
				t.Fatal("invalid TZif accepted")
			}
		})
	}
	// A real v1 fixed-offset stream also parses; explicit label UTC is not an
	// instruction to substitute UTC rules for supplied data.
	for _, tc := range []struct {
		name string
		data []byte
		hour int64
	}{{"fixed", timezoneFixedData(3600), 1}, {"UTC", good, 19}} {
		z, err := TimeZoneFromTZData(tc.name, tc.data)
		if err != nil {
			t.Fatal(err)
		}
		p, err := Compile(memory.NewSchema(), HourInZone(Timestamp(0), z))
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		b.Rows = 1
		w, _ := NewWorkspace(context.Background(), p, nil)
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		w.Close()
		if err != nil || out[0].Int64.Values[0] != tc.hour {
			t.Fatal("explicit TZif label semantics", err)
		}
	}
}
func TestTimeZoneNamedLoaderSnapshot(t *testing.T) {
	const marker = "GOLUME_ZONE_SNAPSHOT_CHILD"
	if os.Getenv(marker) == "1" {
		name := "Fixture/Snapshot"
		a, err := LoadTimeZone(name)
		if err != nil {
			t.Fatal(err)
		}
		b, err := LoadTimeZone(name)
		if err != nil {
			t.Fatal(err)
		}
		if a.state == b.state {
			t.Fatal("separate constructor identity")
		}
		if err := os.WriteFile(filepath.Join(os.Getenv("ZONEINFO"), filepath.FromSlash(name)), timezoneBytes(t, "lord_howe"), 0600); err != nil {
			t.Fatal(err)
		}
		newer, err := LoadTimeZone(name)
		if err != nil {
			t.Fatal(err)
		}
		p, err := CompileExpressions(memory.NewSchema(), []NamedExpr{Named(HourInZone(Timestamp(0), a), "old"), Named(HourInZone(Timestamp(0), b), "old2"), Named(HourInZone(Timestamp(0), newer), "new")})
		if err != nil {
			t.Fatal(err)
		}
		batch := memory.NewBatch()
		batch.Rows = 1
		w, _ := NewWorkspace(context.Background(), p, nil)
		defer w.Close()
		out, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if out[0].Int64.Values[0] != 19 || out[1].Int64.Values[0] != 19 || out[2].Int64.Values[0] != 10 {
			t.Fatal("named loader did not capture independent snapshots")
		}
		return
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "Fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "Fixture", "Snapshot"), timezoneBytes(t, "new_york"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestTimeZoneNamedLoaderSnapshot$", "-test.count=1")
	env := []string{}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "ZONEINFO=") && !strings.HasPrefix(e, marker+"=") {
			env = append(env, e)
		}
	}
	cmd.Env = append(env, "ZONEINFO="+dir, marker+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated ZONEINFO: %v\n%s", err, output)
	}
}
func TestTimeZoneGuardedChildErrorDomains(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "keep", Type: memory.TypeBool}, memory.Field{Name: "first", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "match", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["s"] = testStringVector([]string{"bad", "0", "bad"}, nil)
	b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0}}
	b.Int64s["first"] = &memory.Int64Vector{Values: []int64{7, 0, 7}, Validity: memory.Bitmap{5}}
	b.Int64s["match"] = &memory.Int64Vector{Values: []int64{1969, 0, 1969}}
	year := YearInZone(CastTo(CastTo(Col("s"), memory.TypeInt64), memory.TypeTimestamp), z)
	bound, err := Bind(schema, year)
	if err != nil {
		t.Fatal(err)
	}
	if !bound.MayError {
		t.Fatal("child fallibility lost")
	}
	for name, e := range map[string]Expr{"if": If(Col("keep"), year, I64(7)), "case": Case([]WhenThen{{When: Col("keep"), Then: year}}, I64(7)), "coalesce": Coalesce(Col("first"), year), "in": In(I64(1969), Col("match"), year)} {
		t.Run(name, func(t *testing.T) {
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			out, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if name == "in" {
				if !reflect.DeepEqual(out[0].Uint32.Values, []uint32{1, 1, 1}) {
					t.Fatal("IN")
				}
			} else if !reflect.DeepEqual(out[0].Int64.Values, []int64{7, 1969, 7}) {
				t.Fatal("guard result")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("guard budget")
			}
		})
	}
	p, err := Compile(schema, year)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "cast") {
		t.Fatal("active control", err)
	}
}

func timezoneBatch(n int) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = n
	v := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
	for i := range v.Values {
		v.Values[i] = -1
		if i%7 != 0 {
			memory.Set(v.Validity, i)
		}
	}
	b.Int64s["ts"] = v
	return b
}
func TestTimeZoneBudgetAndConcurrentWorkspaces(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	p, err := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), HourInZone(Col("ts"), z))
	if err != nil {
		t.Fatal(err)
	}
	b := timezoneBatch(130)
	for _, selected := range []bool{false, true} {
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, []int{0, 1, 63, 64, 129}, testKernelSet())
			}
			return w.Evaluate(context.Background(), b, testKernelSet())
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, _ := NewWorkspace(context.Background(), p, probe)
		if _, err := run(w); err != nil {
			t.Fatal(err)
		}
		calls := probe.calls
		w.Close()
		for fail := 1; fail <= calls; fail++ {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, _ := NewWorkspace(context.Background(), p, m)
			if _, err := run(w); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("fail%d: %v", fail, err)
			}
			m.fail = 0
			if _, err := run(w); err != nil {
				t.Fatal(err)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("retry leak")
			}
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := memory.NewBudgetManager(0)
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				errs <- e
				return
			}
		outer:
			for i := 0; i < 10; i++ {
				out, e := w.Evaluate(context.Background(), b, testKernelSet())
				if e != nil {
					errs <- e
					break
				}
				for row := 0; row < 130; row++ {
					want := int64(18)
					if row%7 == 0 {
						want = 0
					}
					if out[0].Int64.Values[row] != want || out[0].Int64.IsValid(row) != (row%7 != 0) {
						errs <- fmt.Errorf("concurrent row%d", row)
						break outer
					}
				}
			}
			w.Close()
			if m.Used() != 0 {
				errs <- fmt.Errorf("concurrent close leak")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

type timezoneCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *timezoneCancelContext) Err() error {
	var pcs [24]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".evalZonedExtract") {
			c.checks++
			if c.checks == c.stop {
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
func TestTimeZoneRunningCancellation(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	for _, stop := range []int{2, 3} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			const n = 193
			in := scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, n)}
			out := scalarRegister{typ: memory.TypeInt64, i64: make([]int64, n), ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
			for i := range in.i64 {
				in.i64[i] = -1
				out.i64[i] = -777
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := 0
			out.checkContext = func() error {
				checks++
				if checks == stop {
					cancel()
				}
				return ctx.Err()
			}
			err := evalZonedExtract(&out, &in, z.state, OpHour, n)
			if !errors.Is(err, context.Canceled) || checks != stop {
				t.Fatal(err, checks)
			}
			for i, v := range out.i64 {
				want := int64(-777)
				if i < (stop-1)*64 {
					want = 18
				}
				if v != want {
					t.Fatalf("slot%d got%d want%d", i, v, want)
				}
			}
			p, err := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), HourInZone(Col("ts"), z))
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			base, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			running := &timezoneCancelContext{Context: base, cancel: cancel2, stop: stop}
			if _, err = w.Evaluate(running, timezoneBatch(n), testKernelSet()); !errors.Is(err, context.Canceled) || running.checks != stop {
				t.Fatal("workspace runtime cancel", err, running.checks)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("cancel leak")
			}
		})
	}
}

func TestTimeZoneVersion3AndTimestampReuse(t *testing.T) {
	data := timezoneBytes(t, "new_york")
	counts := func(i int) int { return int(binary.BigEndian.Uint32(data[20+i*4 : 24+i*4])) }
	second := 44 + counts(0) + counts(1) + counts(2)*8 + counts(3)*5 + counts(4)*6 + counts(5)
	// Derived v3 parser fixture: this v2 payload has no v3-only extensions, so
	// changing both version tags is valid. This is not an alleged new tzdb file.
	data[4] = '3'
	data[second+4] = '3'
	z, err := TimeZoneFromTZData("derived-v3", data)
	if err != nil {
		t.Fatal(err)
	}
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp})
	p, err := Compile(schema, HourInZone(Col("ts"), z))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0}}
	for _, tc := range []struct{ ns, hour int64 }{{0, 19}, {-1, 18}, {3600 * 1e9, 20}, {math.MaxInt64, 19}, {0, 19}} {
		b.Int64s["ts"].Values[0] = tc.ns
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || out[0].Int64.Values[0] != tc.hour {
			t.Fatal("same address rule/value reuse", err)
		}
	}
	data[4] = '2'
	if _, err := TimeZoneFromTZData("mismatched-versions", data); err == nil {
		t.Fatal("inconsistent supported versions accepted")
	}
}
func TestTimeZoneExistingUTCAndDateSemantics(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	// Ordinary Year/Hour retain UTC interpretation; InZone is an explicit opt-in.
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "date", Type: memory.TypeDate32})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Year(Col("ts")), "utcYear"), Named(Hour(Col("ts")), "utcHour"), Named(YearInZone(Col("ts"), z), "nyYear"), Named(Year(Col("date")), "dateYear")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["ts"] = &memory.Int64Vector{Values: []int64{0}}
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{0}}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{1970, 0, 1969, 1970} {
		if out[i].Int64.Values[0] != want {
			t.Fatal("legacy UTC/date semantics", i)
		}
	}
}

// timezoneAbbreviationBlock makes small, transition-free TZif sections. Even
// rejected cases remain a few KiB; no allocation-amplification stress payload
// is passed to the standard parser during this regression.
func timezoneAbbreviationBlock(version byte, types int, abbreviations []byte, index byte) []byte {
	b := make([]byte, 44+6*types+len(abbreviations))
	copy(b, "TZif")
	b[4] = version
	binary.BigEndian.PutUint32(b[36:40], uint32(types))
	binary.BigEndian.PutUint32(b[40:44], uint32(len(abbreviations)))
	for i := 0; i < types; i++ {
		binary.BigEndian.PutUint32(b[44+6*i:48+6*i], 3600)
		b[44+6*i+5] = index
	}
	copy(b[44+6*types:], abbreviations)
	return b
}

func TestTimeZoneBoundedAbbreviations(t *testing.T) {
	maxAbbr := append([]byte(strings.Repeat("A", 255)), 0)
	cases := []struct {
		name  string
		types int
		chars []byte
		index byte
		ok    bool
	}{
		{"zero_types", 0, []byte("X\x00"), 0, false},
		{"too_many_types", 257, []byte("X\x00"), 0, false},
		{"max_types", 256, []byte("X\x00"), 0, true},
		{"max_abbreviation", 1, maxAbbr, 0, true},
		{"max_expansion_64KiB", 256, maxAbbr, 0, true},
		{"too_long", 1, append([]byte(strings.Repeat("A", 256)), 0), 0, false},
		{"unterminated_short", 1, []byte("ABC"), 0, false},
		{"unterminated_max", 1, []byte(strings.Repeat("A", 255)), 0, false},
		{"missing_index", 1, []byte("X\x00"), 2, false},
		{"unreferenced_long_tail", 1, append([]byte("X\x00"), []byte(strings.Repeat("Y", 1024))...), 0, true},
		{"max_byte_index", 1, append([]byte(strings.Repeat("Y", 255)), []byte("Z\x00")...), 255, true},
	}
	for _, version := range []byte{0, '2', '3'} {
		blocks := 1
		if version != 0 {
			blocks = 2
		}
		for badBlock := 0; badBlock < blocks; badBlock++ {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("version%d/block%d/%s", version, badBlock, tc.name), func(t *testing.T) {
					special := timezoneAbbreviationBlock(version, tc.types, tc.chars, tc.index)
					data := special
					if version != 0 {
						ordinary := timezoneAbbreviationBlock(version, 1, []byte("X\x00"), 0)
						if badBlock == 0 {
							data = append(special, ordinary...)
						} else {
							data = append(ordinary, special...)
						}
						data = append(data, '\n', '\n')
					}
					if len(data) > 4096 {
						t.Fatal("regression fixture exceeded small safe envelope")
					}
					z, err := TimeZoneFromTZData("bounded", data)
					if !tc.ok {
						if err == nil {
							t.Fatal("unbounded/invalid abbreviation metadata accepted")
						}
						return
					}
					if err != nil {
						t.Fatal("valid boundary rejected", err)
					}
					p, err := Compile(memory.NewSchema(), HourInZone(Timestamp(0), z))
					if err != nil {
						t.Fatal(err)
					}
					b := memory.NewBatch()
					b.Rows = 1
					w, _ := NewWorkspace(context.Background(), p, nil)
					out, err := w.Evaluate(context.Background(), b, testKernelSet())
					if err != nil {
						t.Fatal(err)
					}
					if out[0].Int64.Values[0] != 1 {
						t.Fatal("fixed offset rules changed")
					}
					w.Close()
				})
			}
		}
	}
}

func TestTimeZonePOSIXFooterSizeBoundary(t *testing.T) {
	for _, version := range []byte{0, '2', '3'} {
		for _, size := range []int{1024, 1025} {
			t.Run(fmt.Sprintf("version%d/bytes%d", version, size), func(t *testing.T) {
				first := timezoneAbbreviationBlock(version, 1, []byte("FIX\x00"), 0)
				second := timezoneAbbreviationBlock(version, 1, []byte("FIX\x00"), 0)
				// Valid bracketed POSIX standard-zone name with UTC offset0. Including
				// both newline delimiters, len(footer)=name length+5. The TZif base type
				// has offset+01:00, so a future result at00:00 proves Go parsed/applied the
				// footer rather than merely accepting an invalid uninterpreted string.
				footer := "\n<" + strings.Repeat("A", size-5) + ">0\n"
				if len(footer) != size {
					t.Fatal("fixture footer size")
				}
				data := first
				if version != 0 {
					data = append(data, second...)
				}
				data = append(data, []byte(footer)...)
				if len(data) > 1200 {
					t.Fatal("fixture exceeds small envelope")
				}
				zone, err := TimeZoneFromTZData("footer-boundary", data)
				if size == 1025 {
					if err == nil || !strings.Contains(err.Error(), "footer") {
						t.Fatalf("expected footer limit rejection, got%v", err)
					}
					return
				}
				if err != nil {
					t.Fatal("valid1024byte footer rejected", err)
				}
				instant, err := time.Parse(time.RFC3339, "2050-01-01T00:00:00Z")
				if err != nil {
					t.Fatal(err)
				}
				p, err := Compile(memory.NewSchema(), HourInZone(Timestamp(instant.UnixNano()), zone))
				if err != nil {
					t.Fatal(err)
				}
				b := memory.NewBatch()
				b.Rows = 1
				w, _ := NewWorkspace(context.Background(), p, nil)
				defer w.Close()
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil || out[0].Int64.Values[0] != 0 {
					t.Fatalf("valid POSIX footer was not applied: %v output%v", err, out)
				}
			})
		}
	}
}
