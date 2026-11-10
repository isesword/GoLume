package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"gocolumn/memory"
)

// Independent civil-from-days oracle: integer Gregorian eras, not time.Time.
func date32CivilOracle(days int32) (year, month, day int64) {
	z := int64(days) + 719468
	era := z / 146097
	if z < 0 && z%146097 != 0 {
		era--
	}
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	year = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	day = doy - (153*mp+2)/5 + 1
	month = mp + 3
	if mp >= 10 {
		month = mp - 9
	}
	if month <= 2 {
		year++
	}
	return
}

func date32TextOracle(days int32) string {
	y, m, d := date32CivilOracle(days)
	prefix := ""
	if y < 0 {
		prefix = "-"
		y = -y
	} else if y > 9999 {
		prefix = "+"
	}
	return fmt.Sprintf("%s%04d-%02d-%02d", prefix, y, m, d)
}

func TestDate32CalendarGoldenAndSyntax(t *testing.T) {
	for _, tc := range []struct {
		text string
		days int32
	}{
		{"1970-01-01", 0}, {"1969-12-31", -1}, {"2000-02-29", 11016},
		{"0000-01-01", -719528}, {"-0001-01-01", -719893},
		{"9999-12-31", 2932896}, {"+10000-01-01", 2932897},
		{"-5877641-06-23", math.MinInt32}, {"+5881580-07-11", math.MaxInt32},
	} {
		got, err := parseDate32Text([]byte(tc.text))
		if err != nil || got != tc.days {
			t.Fatalf("parse %s=%d %v want %d", tc.text, got, err, tc.days)
		}
		if got := string(appendDate32Text(nil, tc.days)); got != tc.text {
			t.Fatalf("format %d=%s", tc.days, got)
		}
	}
	for _, text := range []string{"", "1970-1-01", "1970-01-1", "1970-00-01", "1970-13-01", "1970-01-00", "1970-01-32", "1900-02-29", "2100-02-29", "2001-02-29", "2000-04-31", " 1970-01-01", "1970-01-01 ", "1970-01-01T00:00:00Z", "1970-01-01Z", "1970/01/01", "１９７０-01-01", "1970-01-0x", "1970-01-01\x00", "+1970-01-01", "10000-01-01", "+010000-01-01", "-0000-01-01", "-001-01-01", "-00001-01-01", "+99999999-01-01", strings.Repeat("0", 1000000)} {
		if _, err := parseDate32Text([]byte(text)); !errors.Is(err, errDate32Syntax) {
			t.Fatalf("syntax accepted %.40q: %v", text, err)
		}
	}
	for _, text := range []string{"-5877641-06-22", "+5881580-07-12", "-9999999-01-01", "+9999999-12-31"} {
		if _, err := parseDate32Text([]byte(text)); !errors.Is(err, errDate32Range) {
			t.Fatalf("range %.40q: %v", text, err)
		}
	}
	for _, text := range []string{"0000-02-29", "-0400-02-29", "+10000-02-29"} {
		if _, err := parseDate32Text([]byte(text)); err != nil {
			t.Fatal(text, err)
		}
	}
}

func TestDate32CalendarFullDomainOracle(t *testing.T) {
	check := func(days int32) {
		t.Helper()
		var buf [32]byte
		text := appendDate32Text(buf[:0], days)
		if want := date32TextOracle(days); string(text) != want {
			t.Fatalf("day=%d got=%s want=%s", days, text, want)
		}
		got, err := parseDate32Text(text)
		if err != nil || got != days {
			t.Fatalf("day=%d text=%s got=%d err=%v", days, text, got, err)
		}
	}
	// Every day of a complete Gregorian 400-year cycle, across year zero.
	for days := int32(-792577); days < -792577+146097; days++ {
		check(days)
	}
	for delta := int32(0); delta < 1024; delta++ {
		check(math.MinInt32 + delta)
		check(math.MaxInt32 - delta)
	}
	rng := rand.New(rand.NewSource(932211))
	for range 100000 {
		check(int32(rng.Uint32()))
	}
	var buf [32]byte
	text := []byte("-5877641-06-23")
	if allocs := testing.AllocsPerRun(1000, func() { d, _ := parseDate32Text(text); _ = appendDate32Text(buf[:0], d) }); allocs != 0 {
		t.Fatalf("date parser/formatter allocations %g", allocs)
	}
}

func TestDate32TimestampFloorAndRangeOracle(t *testing.T) {
	values := []int64{math.MinInt64, math.MinInt64 + 1, -date32NanosPerDay - 1, -date32NanosPerDay, -date32NanosPerDay + 1, -1, 0, 1, date32NanosPerDay - 1, date32NanosPerDay, date32NanosPerDay + 1, math.MaxInt64}
	rng := rand.New(rand.NewSource(31232))
	for range 10000 {
		values = append(values, int64(rng.Uint64()))
	}
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp})
	p, err := Compile(schema, CastTo(Col("ts"), memory.TypeDate32))
	if err != nil {
		t.Fatal(err)
	}
	b := batchWithInt64(map[string][]int64{"ts": values})
	var state evaluationState
	defer state.Close()
	out, err := evaluateValueTest(p, b, &state, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i, ns := range values {
		want := new(big.Int).Div(big.NewInt(ns), big.NewInt(date32NanosPerDay)).Int64()
		if int64(out.Int32.Values[i]) != want {
			t.Fatal(ns, out.Int32.Values[i], want)
		}
	}
	for _, days := range []int32{math.MinInt32, -106753, -106752, -106751, -1, 0, 1, 106751, 106752, math.MaxInt32} {
		want := new(big.Int).Mul(big.NewInt(int64(days)), big.NewInt(date32NanosPerDay))
		b := batchWithDate32("d", days)
		schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32})
		for _, tolerant := range []bool{false, true} {
			var e Expr = CastTo(Col("d"), memory.TypeTimestamp)
			if tolerant {
				e = TryCastTo(Col("d"), memory.TypeTimestamp)
			}
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			out, err := evaluateValueTest(p, b, &state, testKernelSet())
			if want.IsInt64() {
				if err != nil || !out.Int64.IsValid(0) || out.Int64.Values[0] != want.Int64() {
					t.Fatal(days, tolerant, err)
				}
			} else if tolerant {
				if err != nil || out.Int64.IsValid(0) || out.Int64.Values[0] != 0 {
					t.Fatal(days, "TRY range", err)
				}
			} else if !errors.Is(err, errDate32TimestampRange) {
				t.Fatal(days, "strict range", err)
			}
		}
	}
}

func TestDate32CastBindingPhysicalAndTextContracts(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "d", Type: memory.TypeDate32}, memory.Field{Name: "ts", Type: memory.TypeTimestamp})
	for _, tc := range []struct {
		e   Expr
		may bool
		typ memory.DataType
	}{
		{CastTo(Col("s"), memory.TypeDate32), true, memory.TypeDate32},
		{CastTo(Col("d"), memory.TypeTimestamp), true, memory.TypeTimestamp},
		{CastTo(Col("ts"), memory.TypeDate32), false, memory.TypeDate32},
		{CastTo(Col("d"), memory.TypeString), false, memory.TypeString},
		{TryCastTo(Col("d"), memory.TypeTimestamp), false, memory.TypeTimestamp},
		{TryCastTo(CastTo(Col("s"), memory.TypeDate32), memory.TypeTimestamp), true, memory.TypeTimestamp},
	} {
		bound, err := Bind(schema, tc.e)
		if err != nil || bound.MayError != tc.may || bound.Field.Type != tc.typ {
			t.Fatal(tc, bound, err)
		}
	}
	for _, from := range []memory.DataType{memory.TypeFloat64, memory.TypeDate64, memory.TypeDuration, memory.TypeTime32, memory.TypeTime64, memory.TypeBool, memory.TypeUint64} {
		for _, e := range []Expr{CastTo(Col("v"), memory.TypeDate32), TryCastTo(Col("v"), memory.TypeDate32)} {
			if _, err := Bind(memory.NewSchema(memory.Field{Name: "v", Type: from}), e); err == nil {
				t.Fatal("unsupported admission", from, e)
			}
		}
	}
	b := batchWithDate32("d", -1)
	p, err := CompileExpressions(schema, []NamedExpr{CastTo(Col("d"), memory.TypeString).Alias("calendar"), CastTo(CastTo(Col("d"), memory.TypeInt32), memory.TypeString).Alias("physical"), CastTo(CastTo(Col("d"), memory.TypeString), memory.TypeDate32).Alias("roundtrip")})
	if err != nil {
		t.Fatal(err)
	}
	var state evaluationState
	defer state.Close()
	out, err := evaluateProjectionTest(p, b, &state, testKernelSet())
	if err != nil || string(out[0].String.Data) != "1969-12-31" || string(out[1].String.Data) != "-1" || out[2].Int32.Values[0] != -1 {
		t.Fatal("calendar/physical contract", out, err)
	}
	for _, from := range []memory.DataType{memory.TypeInt32, memory.TypeInt64} {
		b := memory.NewBatch()
		b.Rows = 1
		if from == memory.TypeInt32 {
			b.Int32s["v"] = &memory.Int32Vector{Values: []int32{math.MinInt32}}
		} else {
			b.Int64s["v"] = &memory.Int64Vector{Values: []int64{math.MinInt32}}
		}
		p, _ := Compile(memory.NewSchema(memory.Field{Name: "v", Type: from}), TryCastTo(Col("v"), memory.TypeDate32))
		out, err := evaluateValueTest(p, b, &state, testKernelSet())
		if err != nil || out.Int32.Values[0] != math.MinInt32 {
			t.Fatal("physical conversion", err)
		}
	}
	p, _ = Compile(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), TryCastTo(Col("v"), memory.TypeDate32))
	outValue, err := evaluateValueTest(p, batchWithInt64(map[string][]int64{"v": {math.MinInt32 - 1, math.MaxInt32 + 1}}), &state, testKernelSet())
	if err != nil || outValue.Int32.IsValid(0) || outValue.Int32.IsValid(1) {
		t.Fatal("integer range", err)
	}
}

func TestDate32CastTryBitmapReuseAndSelection(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	parsed := TryCastTo(Col("s"), memory.TypeDate32)
	p, err := CompileExpressions(schema, []NamedExpr{parsed.Alias("date"), TryCastTo(parsed, memory.TypeTimestamp).Alias("ts"), CastTo(parsed, memory.TypeString).Alias("text")})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, n := range []int{129, 65, 64, 63, 1, 0, 257} {
		text := make([]string, n)
		valid := make(memory.Bitmap, memory.WordsFor(n))
		for i := range n {
			text[i] = []string{"1969-12-31", "invalid", "+5881580-07-11", "2000-02-29", "NULL poison"}[i%5]
			if i%5 != 4 {
				memory.Set(valid, i)
			}
		}
		b := numericCastBatch(text...)
		b.Strings["s"].Validity = valid
		for _, selected := range []bool{false, true} {
			if selected && !p.SupportsSelection() {
				continue
			}
			var selection []int
			for i := 0; i < n; i += 2 {
				selection = append(selection, i)
			}
			var out []ScalarValue
			if selected {
				out, err = w.EvaluateSelected(context.Background(), b, selection, testKernelSet())
			} else {
				out, err = w.Evaluate(context.Background(), b, testKernelSet())
			}
			if err != nil {
				t.Fatal(n, selected, err)
			}
			count := n
			if selected {
				count = len(selection)
			}
			for i := range count {
				row := i
				if selected {
					row = selection[i]
				}
				valid := row%5 != 1 && row%5 != 4
				if out[0].Int32.IsValid(i) != valid || out[1].Int64.IsValid(i) != (valid && row%5 != 2) || out[2].String.IsValid(i) != valid {
					t.Fatal("validity", n, selected, row)
				}
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("accounting")
			}
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("workspace leak")
	}
}

func TestDate32CastResourceAndChildFailures(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	p, _ := Compile(schema, TryCastTo(Col("s"), memory.TypeDate32))
	b := numericCastBatch("bad", "2000-02-29")
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, _ := NewWorkspace(context.Background(), p, probe)
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	calls := probe.calls
	w.Close()
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, _ := NewWorkspace(context.Background(), p, m)
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("swallowed budget error", fail, err)
		}
		m.fail = 0
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
			t.Fatal("retry", err)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	reg := scalarRegister{typ: memory.TypeString, strView: phase32StringVector([]string{"2000-01-01"}, nil)}
	out := scalarRegister{typ: memory.TypeDate32, i32: make([]int32, 1), checkContext: func() error { return context.Canceled }}
	if err := evalDate32Cast(&out, &reg, memory.TypeString, memory.TypeDate32, 1, true); !errors.Is(err, context.Canceled) {
		t.Fatal("swallowed cancellation", err)
	}
	child := TryCastTo(CastTo(Col("s"), memory.TypeDate32), memory.TypeTimestamp)
	p, _ = Compile(schema, child)
	if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); !errors.Is(err, errDate32Syntax) {
		t.Fatal("swallowed child", err)
	}
	// Try must not hide corruption in row payload storage.
	p, _ = Compile(schema, TryCastTo(Col("s"), memory.TypeDate32))
	b = numericCastBatch("2000-01-01")
	b.Strings["s"].Offsets = []uint32{10, 0}
	if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
		t.Fatal("swallowed storage")
	}
	b.Strings["s"].Validity = memory.Bitmap{0}
	if out, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil || out.Int32.IsValid(0) {
		t.Fatal("NULL payload read", err)
	}
}

func TestDate32CastDictionaryDemandOrderingAndEmpty(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 4
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0, math.MaxUint32}, Offsets: []uint32{0, 10, 13}, Data: []byte("1969-12-31bad"), Validity: memory.Bitmap{7}}
	p, err := Compile(schema, TryCastTo(Col("s"), memory.TypeDate32))
	if err != nil {
		t.Fatal(err)
	}
	out, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil || out.Int32.Values[0] != -1 || out.Int32.IsValid(1) || out.Int32.Values[2] != -1 || out.Int32.IsValid(3) {
		t.Fatal("dictionary", err)
	}
	b.DictStrings["s"].Validity = nil
	if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
		t.Fatal("TRY hid bad dictionary code")
	}
	bad := CastTo(Str("bad"), memory.TypeDate32)
	b = memory.NewBatch()
	b.Rows = 1
	for _, e := range []Expr{If(Bool(false), bad, Date32(0)), Coalesce(Date32(0), bad), In(Date32(0), Date32(0), bad)} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil {
			t.Fatal("unused arm", err)
		}
	}
	for _, e := range []Expr{And(Bool(false), EQ(bad, Date32(0))), Or(Bool(true), EQ(bad, Date32(0)))} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
			t.Fatal("eager error suppressed")
		}
	}
	p, _ = Compile(memory.NewSchema(), bad)
	b.Rows = 0
	if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil {
		t.Fatal("empty scalar evaluated", err)
	}
	schema = memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32})
	p, _ = Compile(schema, CastTo(Col("d"), memory.TypeTimestamp))
	b = memory.NewBatch()
	b.Rows = 4
	b.Int32s["d"] = &memory.Int32Vector{Values: []int32{0, math.MaxInt32, 1, math.MinInt32}}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err := w.EvaluateSelected(context.Background(), b, []int{0, 2, 3}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "row 2") {
		t.Fatal("selected logical row", err)
	}
	try, _ := Compile(schema, TryCastTo(Col("d"), memory.TypeTimestamp))
	tw, _ := NewWorkspace(context.Background(), try, nil)
	defer tw.Close()
	got, err := tw.EvaluateSelected(context.Background(), b, []int{0, 2, 3}, testKernelSet())
	if err != nil || got[0].Int64.Values[1] != date32NanosPerDay || got[0].Int64.IsValid(2) {
		t.Fatal("selected tolerant conversion", err)
	}
}

func BenchmarkDate32Calendar(b *testing.B) {
	for _, text := range []string{"2000-02-29", "-5877641-06-23", "invalid-date"} {
		b.Run("Parse/"+text, func(b *testing.B) {
			data := []byte(text)
			b.ReportAllocs()
			for b.Loop() {
				_, _ = parseDate32Text(data)
			}
		})
	}
	b.Run("Format", func(b *testing.B) {
		var buf [32]byte
		b.ReportAllocs()
		for b.Loop() {
			_ = appendDate32Text(buf[:0], math.MinInt32)
		}
	})
}

func BenchmarkDate32CastWorkspace(b *testing.B) {
	const rows = 4096
	text := make([]string, rows)
	days := make([]int32, rows)
	stamps := make([]int64, rows)
	for i := range rows {
		text[i] = "2000-02-29"
		days[i] = int32(i) - 2048
		stamps[i] = int64(days[i])*date32NanosPerDay + 1
	}
	batch := numericCastBatch(text...)
	batch.Int32s["d"] = &memory.Int32Vector{Values: days}
	batch.Int64s["ts"] = &memory.Int64Vector{Values: stamps}
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "d", Type: memory.TypeDate32}, memory.Field{Name: "ts", Type: memory.TypeTimestamp})
	for _, tc := range []struct {
		name string
		e    Expr
	}{
		{"Parse", CastTo(Col("s"), memory.TypeDate32)}, {"Format", CastTo(Col("d"), memory.TypeString)},
		{"DateToTimestamp", CastTo(Col("d"), memory.TypeTimestamp)}, {"TimestampToDate", CastTo(Col("ts"), memory.TypeDate32)},
		{"PhysicalInteger", CastTo(Col("d"), memory.TypeInt32)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p, err := Compile(schema, tc.e)
			if err != nil {
				b.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			if _, err := w.Evaluate(context.Background(), batch, testKernelSet()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ReportMetric(rows, "rows/op")
			b.ResetTimer()
			for b.Loop() {
				if _, err := w.Evaluate(context.Background(), batch, testKernelSet()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
