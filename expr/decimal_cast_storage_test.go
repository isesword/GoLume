package expr

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

func TestDecimalCastTryNativeRescaleAndBitmapReuse(t *testing.T) {
	field, _ := memory.Decimal128Field("d", true, 38, 2)
	schema := memory.NewSchema(field)
	p, err := CompileExpressions(schema, []NamedExpr{TryCastDecimal128(Col("d"), 3, 1).Alias("cast"), TryCastTo(Col("d"), memory.TypeInt8).Alias("integer")})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, manager)
	defer w.Close()
	for _, n := range []int{129, 65, 64, 63, 1, 0, 257} {
		b := memory.NewBatch()
		b.Rows = n
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: make(memory.Bitmap, memory.WordsFor(n)), Precision: 38, Scale: 2}
		for i := range n {
			value := int64(120)
			switch i % 5 {
			case 1:
				value = 123
			case 2:
				value = 123400
			case 3:
				value = -12800
			case 4:
				value = 0
			}
			v.Values[i] = decimal128.FromI64(value)
			if i%5 != 4 {
				memory.Set(v.Validity, i)
			} else {
				v.Values[i] = decimal128.New(math.MinInt64, 0)
			}
		}
		b.Decimals["d"] = v
		for _, selected := range []bool{false, true} {
			var rows []int
			var out []ScalarValue
			if selected {
				rows = make([]int, 0)
				for i := 0; i < n; i += 2 {
					rows = append(rows, i)
				}
				out, err = w.EvaluateSelected(context.Background(), b, rows, testKernelSet())
			} else {
				out, err = w.Evaluate(context.Background(), b, testKernelSet())
			}
			if err != nil {
				t.Fatal(err)
			}
			count := n
			if selected {
				count = len(rows)
			}
			for i := 0; i < count; i++ {
				row := i
				if selected {
					row = rows[i]
				}
				valid := row%5 == 0
				if out[0].Decimal.IsValid(i) != valid || valid && out[0].Decimal.Values[i] != decimal128.FromI64(12) {
					t.Fatal("decimal validity/value", n, row)
				}
				ivalid := row%5 == 3
				if out[1].Int8.IsValid(i) != ivalid || ivalid && out[1].Int8.Values[i] != -128 {
					t.Fatal("integer validity/value", n, row)
				}
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatal("try accounting")
			}
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("try close leak")
	}
	for _, tc := range []struct {
		value  int64
		status string
	}{{120, ""}, {123, "fractional"}, {123400, "overflows"}} {
		e := CastDecimal128(Decimal128(decimal128.FromI64(tc.value), 38, 2), 3, 1)
		p, _ := Compile(memory.NewSchema(), e)
		b := memory.NewBatch()
		b.Rows = 1
		result, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
		if tc.status == "" {
			if err != nil || result.Decimal.Values[0] != decimal128.FromI64(12) {
				t.Fatal(result, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tc.status) {
			t.Fatal(tc, err)
		}
	}
}

func TestDecimalCastStorageErrorsAndPoisonGuards(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	p, _ := Compile(schema, TryCastDecimal128(Col("s"), 5, 2))
	cases := []struct {
		name  string
		build func() *memory.Batch
	}{
		{"missing-offset", func() *memory.Batch { b := numericCastBatch("1"); b.Strings["s"].Offsets = []uint32{0}; return b }},
		{"decreasing-offset", func() *memory.Batch { b := numericCastBatch("1"); b.Strings["s"].Offsets = []uint32{1, 0}; return b }},
		{"overrun-offset", func() *memory.Batch { b := numericCastBatch("1"); b.Strings["s"].Offsets = []uint32{0, 2}; return b }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.build()
			w := evaluationState{}
			if _, err := evaluateValueTest(p, b, &w, testKernelSet()); err == nil {
				t.Fatal("storage error swallowed")
			}
			w.Close()
			b.Strings["s"].Validity = memory.Bitmap{0}
			out, err := evaluateValueTest(p, b, &w, testKernelSet())
			if tc.name == "missing-offset" {
				if err == nil {
					t.Fatal("structural row-count error suppressed")
				}
				w.Close()
				return
			}
			if err != nil || out.Decimal.IsValid(0) {
				t.Fatal("NULL payload inspected", err)
			}
			w.Close()
		})
	}
	dictSchema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true})
	p, _ = Compile(dictSchema, TryCastDecimal128(Col("s"), 5, 2))
	for _, corrupt := range []func(*memory.DictionaryStringVector){func(v *memory.DictionaryStringVector) { v.Indices = nil }, func(v *memory.DictionaryStringVector) { v.Indices[0] = math.MaxUint32 }, func(v *memory.DictionaryStringVector) { v.Offsets = nil }, func(v *memory.DictionaryStringVector) { v.Offsets = []uint32{1, 0} }, func(v *memory.DictionaryStringVector) { v.Offsets = []uint32{0, 2} }} {
		b := memory.NewBatch()
		b.Rows = 1
		v := &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("1"), DictionaryDomain: "poison"}
		corrupt(v)
		b.DictStrings["s"] = v
		w := evaluationState{}
		if _, err := evaluateValueTest(p, b, &w, testKernelSet()); err == nil {
			t.Fatal("dictionary storage error swallowed")
		}
		w.Close()
		v.Validity = memory.Bitmap{0}
		out, err := evaluateValueTest(p, b, &w, testKernelSet())
		if len(v.Indices) == 0 {
			if err == nil {
				t.Fatal("structural row-count error suppressed")
			}
			w.Close()
			continue
		}
		if err != nil || out.Decimal.IsValid(0) {
			t.Fatal("NULL dictionary payload inspected", err)
		}
		w.Close()
	}
	// A dead guarded arm must not even bind runtime storage for its unused column.
	p, _ = Compile(schema, If(Bool(false), CastDecimal128(Col("s"), 5, 2), CastDecimal128(I64(1), 5, 2)))
	b := memory.NewBatch()
	b.Rows = 1
	if out, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil || out.Decimal.Values[0] != decimal128.FromI64(100) {
		t.Fatal("dead storage read", err)
	}
}

func TestDecimalCastTryResourceFailuresAndCleanup(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	p, _ := Compile(schema, TryCastDecimal128(Col("s"), 38, 2))
	b := numericCastBatch("bad", "1.25", strings.Repeat("0", 12000)+"1.00")
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
			t.Fatal("Try swallowed admission", fail, err)
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("failed accounting")
		}
		m.fail = 0
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || out[0].Decimal.IsValid(0) || out[0].Decimal.Values[1] != decimal128.FromI64(125) {
			t.Fatal("retry", err)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
	// Semantic error closes all reservations; TRY resource errors do not become NULL.
	strict, _ := Compile(schema, CastDecimal128(Col("s"), 38, 2))
	m := memory.NewBudgetManager(0)
	w, _ = NewWorkspace(context.Background(), strict, m)
	if out, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil || out != nil {
		t.Fatal("strict did not fail atomically", err)
	}
	if m.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatal("semantic error retained reservation")
	}
	w.Close()
	cancel := func() error { return context.Canceled }
	reg := scalarRegister{typ: memory.TypeString, strView: phase32StringVector([]string{strings.Repeat("0", 10000)}, nil)}
	out := scalarRegister{typ: memory.TypeDecimal, decimal: make([]memory.Decimal128, 1), decimalType: &memory.Field{DecimalPrecision: 38}, checkContext: cancel}
	if err := evalDecimalCast(&out, &reg, memory.TypeString, memory.TypeDecimal, 1, true); !errors.Is(err, context.Canceled) {
		t.Fatal("TRY swallowed cancellation", err)
	}
}

func TestDecimalCastDemandOrderingAndSelectedErrorRows(t *testing.T) {
	zero := CastDecimal128(I64(0), 5, 2)
	bad := CastDecimal128(Str("bad"), 5, 2)
	b := memory.NewBatch()
	b.Rows = 1
	for _, e := range []Expr{In(zero, zero, bad), Coalesce(zero, bad)} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil {
			t.Fatal("guard demanded later argument", err)
		}
	}
	for _, e := range []Expr{Add(NullDecimal128(5, 2), bad), And(Bool(false), EQ(bad, zero)), Or(Bool(true), EQ(bad, zero))} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
			t.Fatal("eager child error suppressed", e)
		}
	}
	field, _ := memory.Decimal128Field("d", true, 4, 1)
	p, err := Compile(memory.NewSchema(field), CastTo(Col("d"), memory.TypeInt8))
	if err != nil {
		t.Fatal(err)
	}
	b.Rows = 67
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 67), Precision: 4, Scale: 1}
	for i := range v.Values {
		v.Values[i] = decimal128.FromI64(10)
	}
	v.Values[1] = decimal128.FromI64(11)
	v.Values[63] = decimal128.FromI64(9999)
	b.Decimals["d"] = v
	for _, selected := range []bool{false, true} {
		w, _ := NewWorkspace(context.Background(), p, nil)
		var err error
		want := "row 1"
		if selected {
			_, err = w.EvaluateSelected(context.Background(), b, []int{0, 2, 63, 64, 66}, testKernelSet())
			want = "row 2"
		} else {
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
		}
		if err == nil || !strings.Contains(err.Error(), "loses fractional digits at "+want) {
			t.Fatal("first failure", selected, err)
		}
		w.Close()
	}
	// Empty batches must not evaluate even invalid scalar conversions.
	p, _ = Compile(memory.NewSchema(), bad)
	b.Rows = 0
	if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err != nil {
		t.Fatal("empty scalar conversion", err)
	}
}

func TestDecimalCastLongChainsAndGuardedBudget(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	var e Expr = TryCastDecimal128(Col("s"), 20, 2)
	for range 24 {
		e = CastDecimal128(CastTo(e, memory.TypeString), 20, 2)
	}
	e = If(EQ(Col("s"), Str("bad")), NullDecimal128(20, 2), Coalesce(Case([]WhenThen{{When: Bool(true), Then: e}}, nil), CastDecimal128(I64(0), 20, 2)))
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	b := numericCastBatch("bad", "1.25", "-999.00", "0.001")
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, _ := NewWorkspace(context.Background(), p, probe)
	check := func(out []ScalarValue) {
		v := out[0].Decimal
		for i, want := range []int64{0, 125, -99900, 0} {
			if v.IsValid(i) != (i != 0) || i != 0 && v.Values[i] != decimal128.FromI64(want) {
				t.Fatal("long chain", i)
			}
		}
	}
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	check(out)
	calls := probe.calls
	w.Close()
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, _ := NewWorkspace(context.Background(), p, m)
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("guarded budget", fail, err)
		}
		if m.Used() != w.MemoryBytes() {
			t.Fatal("guard accounting")
		}
		m.fail = 0
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		check(out)
		w.Close()
		if m.Used() != 0 {
			t.Fatal("guard leak")
		}
	}
}

func BenchmarkDecimalCastTextCoefficient(b *testing.B) {
	text := []byte("-123456789012345678.12345678901234567890")
	b.ReportAllocs()
	var scratch [64]byte
	for b.Loop() {
		v, status, err := parseDecimal128Text(text, 38, 20, nil)
		if err != nil || status != decimalRescaleOK {
			b.Fatal(err, status)
		}
		_ = appendDecimal128Text(scratch[:0], v, 20)
	}
}

func TestDecimalCastDynamicMaskSkipsMalformedStringStorage(t *testing.T) {
	for _, dictionary := range []bool{false, true} {
		for _, tolerant := range []bool{false, true} {
			b := memory.NewBatch()
			b.Rows = 3
			b.Uint32s["c"] = &memory.Uint32Vector{Values: []uint32{1, 0, 1}}
			field := memory.Field{Name: "s", Type: memory.TypeString}
			if dictionary {
				field.Encoding = memory.EncodingDictionary
				b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, math.MaxUint32, 1}, Offsets: []uint32{0, 3, 5}, Data: []byte("12323"), DictionaryDomain: "dynamic-mask"}
			} else {
				// Only row 1 has decreasing offsets. Rows 0 and 2 remain readable.
				b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 3, 1, 3}, Data: []byte("123")}
			}
			convert := CastDecimal128(Col("s"), 5, 0)
			convert.Tolerant = tolerant
			schema := memory.NewSchema(field, memory.Field{Name: "c", Type: memory.TypeBool})
			p, err := Compile(schema, If(Col("c"), convert, CastDecimal128(I64(0), 5, 0)))
			if err != nil {
				t.Fatal(err)
			}
			w, _ := NewWorkspace(context.Background(), p, nil)
			out, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatalf("dictionary=%v tolerant=%v masked storage: %v", dictionary, tolerant, err)
			}
			for row, want := range []int64{123, 0, 23} {
				if !out[0].Decimal.IsValid(row) || out[0].Decimal.Values[row] != decimal128.FromI64(want) {
					t.Fatal("guarded result", dictionary, tolerant, row)
				}
			}
			// The exact same malformed row must fail when it becomes demanded, even
			// for TRY. A fresh evaluation must not retain the previous branch mask.
			b.Uint32s["c"].Values[1] = 1
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "invalid at row 1") {
				t.Fatalf("dictionary=%v tolerant=%v demanded storage: %v", dictionary, tolerant, err)
			}
			w.Close()
		}
	}
}
