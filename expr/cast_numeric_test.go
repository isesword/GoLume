package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func numericCastBatch(text ...string) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(text)
	b.Strings["s"] = phase32StringVector(text, nil)
	return b
}

func TestStringNumericCastBoundaries(t *testing.T) {
	tests := []struct {
		typ       memory.DataType
		good, bad []string
	}{
		{memory.TypeInt8, []string{"-128", "127", "+12", "0003"}, []string{"-129", "128"}},
		{memory.TypeInt16, []string{"-32768", "32767"}, []string{"-32769", "32768"}},
		{memory.TypeInt32, []string{"-2147483648", "2147483647"}, []string{"-2147483649", "2147483648"}},
		{memory.TypeInt64, []string{"-9223372036854775808", "9223372036854775807"}, []string{"-9223372036854775809", "9223372036854775808"}},
		{memory.TypeUint8, []string{"0", "255", "+12"}, []string{"-1", "256"}},
		{memory.TypeUint16, []string{"0", "65535"}, []string{"-1", "65536"}},
		{memory.TypeUint32, []string{"0", "4294967295"}, []string{"-1", "4294967296"}},
		{memory.TypeUint64, []string{"0", "18446744073709551615"}, []string{"-1", "18446744073709551616"}},
		{memory.TypeFloat32, []string{"1.25", "-0", "1e-45", "3.4028234663852886e38", "NaN", "+NaN", "-nan", "NAN", "Inf", "-Infinity", "3.5e38", "1e999"}, []string{"1e"}},
		{memory.TypeFloat64, []string{"1.25", "-0", "5e-324", "1.7976931348623157e308", "NaN", "+NaN", "-nan", "NAN", "Inf", "-Infinity", "1.8e308", "1e999"}, []string{"1e"}},
	}
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	for _, tc := range tests {
		t.Run(tc.typ.String(), func(t *testing.T) {
			p, err := Compile(schema, Col("s").Cast(tc.typ))
			if err != nil {
				t.Fatal(err)
			}
			var w evaluationState
			got, err := evaluateValueTest(p, numericCastBatch(tc.good...), &w, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != tc.typ {
				t.Fatalf("got %s", got.Type)
			}
			r := &w.regs[p.root]
			for row := range tc.good {
				if !isValidRegister(r, row) {
					t.Fatalf("valid input became null: %q", tc.good[row])
				}
			}
			// Invalid rows must neither parse nor surface payload errors.
			nulls := numericCastBatch("invalid")
			nulls.Strings["s"].Validity = memory.Bitmap{0}
			if _, err := evaluateValueTest(p, nulls, &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			bad := append(append([]string{}, tc.bad...), "", " 1", "1 ", "0x12", "1_2", "١", "1\x00", "+", "--1")
			if isSignedInteger(tc.typ) || unsignedInteger(tc.typ) {
				bad = append(bad, "1.0", "1e2", "NaN", "Inf")
			}
			for _, text := range bad {
				if _, err := evaluateValueTest(p, numericCastBatch("1", text), &w, testKernelSet()); err == nil || !strings.Contains(err.Error(), "row 1") {
					t.Fatalf("%q: err=%v", text, err)
				}
			}
			tolerant, err := Compile(schema, Col("s").TryCast(tc.typ))
			if err != nil {
				t.Fatal(err)
			}
			all := append(append([]string{}, tc.good...), bad...)
			if _, err := evaluateValueTest(tolerant, numericCastBatch(all...), &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			r = &w.regs[tolerant.root]
			for row := range all {
				if isValidRegister(r, row) != (row < len(tc.good)) {
					t.Fatalf("input %q: validity=%v", all[row], isValidRegister(r, row))
				}
			}
			_ = w.Close()
		})
	}
}

func TestStringNumericCastExactValues(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	p, _ := Compile(schema, Col("s").Cast(memory.TypeUint64))
	got, err := evaluateValueTest(p, numericCastBatch("18446744073709551615", "9007199254740993"), &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got.Uint64.Values[0] != math.MaxUint64 || got.Uint64.Values[1] != 9007199254740993 {
		t.Fatal(got.Uint64.Values)
	}
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		p, _ := Compile(schema, Col("s").Cast(typ))
		var w evaluationState
		_, err := evaluateValueTest(p, numericCastBatch("-0", "NaN", "Inf", "-Infinity", "1.25"), &w, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		r := &w.regs[p.root]
		a, _ := registerFloat64(r, 0)
		b, _ := registerFloat64(r, 1)
		c, _ := registerFloat64(r, 2)
		d, _ := registerFloat64(r, 3)
		e, _ := registerFloat64(r, 4)
		if !math.Signbit(a) || !math.IsNaN(b) || !math.IsInf(c, 1) || !math.IsInf(d, -1) || e != 1.25 {
			t.Fatalf("special values %v %v %v %v %v", a, b, c, d, e)
		}
	}
}

func TestTryCastNumericAndErrorEffects(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "f", Type: memory.TypeFloat64})
	for _, target := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		strict, _ := Bind(schema, Col("s").Cast(target))
		try, _ := Bind(schema, Col("s").TryCast(target))
		if !strict.MayError || try.MayError || !try.Field.Nullable {
			t.Fatalf("bad effects for %s", target)
		}
	}
	child := TryCastTo(CastTo(Col("s"), memory.TypeInt64), memory.TypeInt8)
	bound, err := Bind(schema, child)
	if err != nil || !bound.MayError {
		t.Fatalf("child error lost: %v %v", bound, err)
	}
	p, _ := Compile(schema, child)
	if _, err := evaluateValueTest(p, numericCastBatch("bad"), &evaluationState{}, testKernelSet()); err == nil {
		t.Fatal("suppressed child error")
	}
	b := memory.NewBatch()
	b.Rows = 8
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{1.9, -2.9, 128, math.NaN(), math.Inf(1), math.Inf(-1), math.Exp2(63), 0}}
	p, _ = Compile(schema, Col("f").TryCast(memory.TypeInt8))
	got, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got.Int8.Values[0] != 1 || got.Int8.Values[1] != -2 {
		t.Fatal(got.Int8.Values)
	}
	for row := 2; row < 7; row++ {
		if got.Int8.IsValid(row) {
			t.Fatalf("row %d should be null", row)
		}
	}
	for _, e := range []Expr{TryCastTo(Str("x"), memory.TypeString), TryCastTo(Bool(true), memory.TypeInt64)} {
		if _, err := Bind(schema, e); err == nil {
			t.Fatal("unsupported cast accepted")
		}
	}
	p, err = CompileExpressions(schema, []NamedExpr{Col("s").TryCast(memory.TypeInt64).Alias("try"), Col("s").Cast(memory.TypeInt64).Alias("strict")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evaluateProjectionTest(p, numericCastBatch("bad"), &evaluationState{}, testKernelSet()); err == nil {
		t.Fatal("CSE conflated strict and try casts")
	}
}

func TestTryCastDictionaryGuardAndWorkspaceReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 4
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2, 1}, Offsets: []uint32{0, 1, 4, 7}, Data: []byte("7bad127"), Validity: memory.Bitmap{7}, DictionaryDomain: "cast-test"}
	p, err := Compile(schema, Col("s").TryCast(memory.TypeInt8))
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got.Int8.Values[0] != 7 || got.Int8.Values[2] != 127 || got.Int8.IsValid(1) || got.Int8.IsValid(3) {
		t.Fatal(got.Int8)
	}
	schema = memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	p, err = Compile(schema, If(EQ(Col("s"), Str("bad")), I64(0), CastTo(Col("s"), memory.TypeInt64)))
	if err != nil {
		t.Fatal(err)
	}
	got, err = evaluateValueTest(p, numericCastBatch("bad", "12"), &evaluationState{}, testKernelSet())
	if err != nil || got.Int64.Values[1] != 12 {
		t.Fatalf("guard: %v %v", got, err)
	}
	var e Expr = Col("s").TryCast(memory.TypeInt64)
	for range 20 {
		e = TryCastTo(CastTo(e, memory.TypeString), memory.TypeInt64)
	}
	p, err = Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []int{0, 1, 63, 64, 65, 129, 1, 0, 257} {
		vals := make([]string, rows)
		for i := range vals {
			vals[i] = "42"
			if i%3 == 0 {
				vals[i] = "bad"
			}
		}
		batch := numericCastBatch(vals...)
		out, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar))
		if err != nil {
			t.Fatal(err)
		}
		for i := range vals {
			if out[0].Int64.IsValid(i) != (i%3 != 0) || (i%3 != 0 && out[0].Int64.Values[i] != 42) {
				t.Fatalf("rows=%d row=%d", rows, i)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("reservation leak: %d", manager.Used())
	}
	manager = memory.NewBudgetManager(1)
	w, err = NewWorkspace(context.Background(), p, manager)
	if err == nil {
		_, err = w.Evaluate(context.Background(), numericCastBatch("12"), testKernelSet())
		_ = w.Close()
	}
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("budget err=%v", err)
	}
}

func BenchmarkStringNumericCast(b *testing.B) {
	const rows = 4096
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	vals := make([]string, rows)
	for i := range vals {
		vals[i] = fmt.Sprint(i * 17)
	}
	batch := numericCastBatch(vals...)
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeFloat64} {
		b.Run(typ.String(), func(b *testing.B) {
			p, err := Compile(schema, Col("s").Cast(typ))
			if err != nil {
				b.Fatal(err)
			}
			w, err := NewWorkspace(context.Background(), p, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			kernels := kernel.New(kernel.ModeScalar)
			if _, err := w.Evaluate(context.Background(), batch, kernels); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(rows * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := w.Evaluate(context.Background(), batch, kernels); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTryCastStringValidity(b *testing.B) {
	const rows = 4096
	for _, dictionary := range []bool{false, true} {
		for _, input := range []struct{ name, value string }{{"valid", "123456"}, {"invalid", "not-a-number"}, {"long-invalid", strings.Repeat("9", 4096) + "x"}, {"long-valid", strings.Repeat("0", 4096) + "1"}, {"overflow", "1e999"}, {"long-overflow", strings.Repeat("9", 4096)}} {
			for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeFloat64} {
				b.Run(fmt.Sprintf("%s/%s/dictionary=%v", typ, input.name, dictionary), func(b *testing.B) {
					field := memory.Field{Name: "s", Type: memory.TypeString}
					batch := memory.NewBatch()
					batch.Rows = rows
					if dictionary {
						field.Encoding = memory.EncodingDictionary
						batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: make([]uint32, rows), Offsets: []uint32{0, uint32(len(input.value))}, Data: []byte(input.value), DictionaryDomain: "benchmark"}
					} else {
						texts := make([]string, rows)
						for i := range texts {
							texts[i] = input.value
						}
						batch = numericCastBatch(texts...)
					}
					p, err := Compile(memory.NewSchema(field), Col("s").TryCast(typ))
					if err != nil {
						b.Fatal(err)
					}
					w, err := NewWorkspace(context.Background(), p, nil)
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					ks := testKernelSet()
					if _, err := w.Evaluate(context.Background(), batch, ks); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := w.Evaluate(context.Background(), batch, ks); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestDecimalFloatOverflowBoundaryDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(19))
	for _, bits := range []int{32, 64} {
		boundary := float64OverflowBoundary
		if bits == 32 {
			boundary = float32OverflowBoundary
		}
		n, _ := new(big.Int).SetString(boundary, 10)
		texts := []string{boundary, boundary + ".0", boundary + ".0001", "0e99999999999999", "1e9999999999999", "1e-999999999999", "000.00001e999", "Infinity", "-inf", "NaN"}
		for _, delta := range []int64{-10000000, -1, 0, 1, 10000000} {
			texts = append(texts, new(big.Int).Add(n, big.NewInt(delta)).String())
		}
		for range 2000 {
			digits := make([]byte, rng.Intn(850)+1)
			for i := range digits {
				digits[i] = byte('0' + rng.Intn(10))
			}
			dot := rng.Intn(len(digits) + 1)
			token := string(digits[:dot]) + "." + string(digits[dot:]) + "e" + strconv.Itoa(rng.Intn(2200)-1100)
			texts = append(texts, token)
		}
		for _, text := range texts {
			for _, prefix := range []string{"", "-"} {
				input := prefix + text
				want, err := strconv.ParseFloat(input, bits)
				if err != nil && !errors.Is(err, strconv.ErrRange) {
					continue
				}
				got, err := parseDecimalFloat([]byte(input), bits)
				if err != nil || (!math.IsNaN(want) && math.Float64bits(got) != math.Float64bits(want)) {
					t.Fatalf("bits=%d input=%q got=%v want=%v err=%v", bits, input, got, want, err)
				}
			}
		}
	}
}

func TestTryCastCancellationAndNumericOracle(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "f", Type: memory.TypeFloat64})
	inputs := []float64{0, math.Copysign(0, -1), -1.9, 1.9, 127, 128, -129, 255, 256, 65536, math.Exp2(31), math.Exp2(32), math.Exp2(63), math.Exp2(64), math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64}
	batch := memory.NewBatch()
	batch.Rows = len(inputs)
	batch.Float64s["f"] = &memory.Float64Vector{Values: inputs}
	for _, target := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		try, _ := Compile(schema, Col("f").TryCast(target))
		strict, _ := Compile(schema, Col("f").Cast(target))
		var w evaluationState
		if _, err := evaluateValueTest(try, batch, &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		for row, value := range inputs {
			single := memory.NewBatch()
			single.Rows = 1
			single.Float64s["f"] = &memory.Float64Vector{Values: []float64{value}}
			var sw evaluationState
			_, err := evaluateValueTest(strict, single, &sw, testKernelSet())
			if isValidRegister(&w.regs[try.root], row) != (err == nil) {
				t.Fatalf("%s input=%v strict err=%v tolerant valid=%v", target, value, err, isValidRegister(&w.regs[try.root], row))
			}
			if err == nil {
				got, _ := appendScalarText(nil, &w.regs[try.root], row)
				want, _ := appendScalarText(nil, &sw.regs[strict.root], 0)
				if string(got) != string(want) {
					t.Fatalf("%s got=%s want=%s", target, got, want)
				}
			}
			_ = sw.Close()
		}
		_ = w.Close()
	}
	p, _ := Compile(schema, Col("f").TryCast(memory.TypeInt64))
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation suppressed: %v", err)
	}
}

func TestLongDecimalFloatExactOracle(t *testing.T) {
	for _, bits := range []int{32, 64} {
		boundary := float64OverflowBoundary
		if bits == 32 {
			boundary = float32OverflowBoundary
		}
		n, _ := new(big.Int).SetString(boundary, 10)
		for _, base := range []string{"1", "0", "12345", boundary, new(big.Int).Sub(n, big.NewInt(1)).String(), new(big.Int).Add(n, big.NewInt(1)).String(), "340282346638528859811704183484516925440", "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368"} {
			for _, suffix := range []string{strings.Repeat("0", 4096), strings.Repeat("0", 4095) + "1", strings.Repeat("9", 4096)} {
				for _, sign := range []string{"", "-"} {
					input := sign + base + suffix + "e-4096"
					exact, _, err := big.ParseFloat(input, 10, 24000, big.ToNearestEven)
					if err != nil {
						t.Fatal(err)
					}
					want, _ := exact.Float64()
					if bits == 32 {
						f, _ := exact.Float32()
						want = float64(f)
					}
					got, err := parseDecimalFloat([]byte(input), bits)
					if err != nil || math.Float64bits(got) != math.Float64bits(want) {
						t.Fatalf("bits=%d base=%s sign=%s got=%g want=%g err=%v", bits, base, sign, got, want, err)
					}
				}
			}
		}
	}
	// Exact subnormal midpoint, plus a nonzero digit far beyond the kept prefix.
	for _, bits := range []int{32, 64} {
		shift := 150
		if bits == 64 {
			shift = 1075
		}
		numerator := new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(shift)), nil).String()
		midpoint := "0." + strings.Repeat("0", shift-len(numerator)) + numerator
		for _, input := range []string{midpoint + strings.Repeat("0", 4096), midpoint + strings.Repeat("0", 4096) + "1"} {
			exact, _, err := big.ParseFloat(input, 10, 20000, big.ToNearestEven)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := exact.Float64()
			if bits == 32 {
				f, _ := exact.Float32()
				want = float64(f)
			}
			got, err := parseDecimalFloat([]byte(input), bits)
			if err != nil || math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("subnormal midpoint bits=%d got=%g want=%g err=%v", bits, got, want, err)
			}
		}
	}
}
