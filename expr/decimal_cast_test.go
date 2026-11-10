package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func TestDecimalCastParseSyntaxAndPriority(t *testing.T) {
	for _, tc := range []struct {
		text   string
		p, s   int32
		want   string
		status decimalRescaleStatus
	}{
		{"+0012.3000", 5, 2, "1230", decimalRescaleOK}, {".1", 1, 1, "1", decimalRescaleOK}, {"1.", 3, 2, "100", decimalRescaleOK},
		{"-0.00000", 1, 0, "0", decimalRescaleOK}, {"-1.20", 3, 1, "-12", decimalRescaleOK}, {"9007199254740993", 18, 0, "9007199254740993", decimalRescaleOK},
		{strings.Repeat("9", 38), 38, 0, strings.Repeat("9", 38), decimalRescaleOK}, {"-" + strings.Repeat("9", 38), 38, 0, "-" + strings.Repeat("9", 38), decimalRescaleOK},
		{"0." + strings.Repeat("0", 37) + "1", 38, 38, "1", decimalRescaleOK}, {strings.Repeat("0", 100000) + "1.00", 3, 2, "100", decimalRescaleOK},
		{"123.001", 1, 0, "", decimalRescaleInexact}, {"123.001!", 1, 0, "", decimalCastSyntax}, {"1.2", 1, 0, "", decimalRescaleInexact},
		{"10", 1, 0, "", decimalRescaleOverflow}, {"1", 38, 38, "", decimalRescaleOverflow}, {strings.Repeat("9", 100000), 38, 0, "", decimalRescaleOverflow},
	} {
		got, status, err := parseDecimal128Text([]byte(tc.text), tc.p, tc.s, nil)
		if err != nil || status != tc.status || status == decimalRescaleOK && got.BigInt().String() != tc.want {
			t.Fatalf("input %.50q p/s=%d/%d got=%s/%d err=%v want=%s/%d", tc.text, tc.p, tc.s, got.BigInt(), status, err, tc.want, tc.status)
		}
	}
	for _, s := range []string{"", "+", "-", ".", "+.", "1..0", " 1", "1 ", "1e2", "NaN", "-Inf", "0x1", "1_0", "١", "1\x00", "--1", "1-2"} {
		if _, status, err := parseDecimal128Text([]byte(s), 38, 3, nil); err != nil || status != decimalCastSyntax {
			t.Fatalf("accepted %q %d %v", s, status, err)
		}
	}
	calls := 0
	_, _, err := parseDecimal128Text([]byte(strings.Repeat("0", 20000)), 38, 0, func() error {
		calls++
		if calls == 3 {
			return context.Canceled
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 3 {
		t.Fatal("long parser cancellation", calls, err)
	}
}

func TestDecimalCastParseFormatBigIntOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(604137))
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	limit := pow(38)
	for i := 0; i < 12000; i++ {
		v := new(big.Int).Rand(rng, limit)
		if i%2 == 0 {
			v.Neg(v)
		}
		sourceScale := int32(rng.Intn(39))
		p := int32(rng.Intn(38) + 1)
		s := int32(rng.Intn(int(p) + 1))
		coeff := decimal128.FromBigInt(v)
		// Independent text construction, never Arrow's decimal formatting.
		sign := ""
		digits := new(big.Int).Abs(new(big.Int).Set(v)).String()
		if v.Sign() < 0 {
			sign = "-"
		}
		if len(digits) <= int(sourceScale) {
			digits = strings.Repeat("0", int(sourceScale)+1-len(digits)) + digits
		}
		text := sign + digits
		if sourceScale > 0 {
			at := len(digits) - int(sourceScale)
			text = sign + digits[:at] + "." + digits[at:]
		}
		var scratch [64]byte
		if got := string(appendDecimal128Text(scratch[:0], coeff, sourceScale)); got != text {
			t.Fatalf("format got=%s want=%s", got, text)
		}
		got, status, err := parseDecimal128Text([]byte(text), p, s, nil)
		want := new(big.Int).Set(v)
		remainder := new(big.Int)
		if s >= sourceScale {
			want.Mul(want, pow(s-sourceScale))
		} else {
			want.QuoRem(want, pow(sourceScale-s), remainder)
		}
		wantStatus := decimalRescaleOK
		if remainder.Sign() != 0 {
			wantStatus = decimalRescaleInexact
		} else if new(big.Int).Abs(new(big.Int).Set(want)).Cmp(pow(p)) >= 0 {
			wantStatus = decimalRescaleOverflow
		}
		if err != nil || status != wantStatus || status == decimalRescaleOK && got.BigInt().Cmp(want) != 0 {
			t.Fatalf("parse %s (%d,%d) got=%s/%d want=%s/%d err=%v", text, p, s, got.BigInt(), status, want, wantStatus, err)
		}
	}
	text := []byte("-999999999999999999.99999999999999999999")
	var scratch [64]byte
	if n := testing.AllocsPerRun(1000, func() {
		v, _, _ := parseDecimal128Text(text, 38, 20, nil)
		_ = appendDecimal128Text(scratch[:0], v, 20)
	}); n != 0 {
		t.Fatal("coefficient text conversion allocated", n)
	}
}

func TestDecimalCastIntegerBoundaries(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64}
	for _, typ := range types {
		width := integerWidth(typ)
		lo := big.NewInt(0)
		hi := new(big.Int).Lsh(big.NewInt(1), uint(width))
		hi.Sub(hi, big.NewInt(1))
		if isSignedInteger(typ) {
			hi.Rsh(hi, 1)
			lo.Neg(new(big.Int).Add(new(big.Int).Set(hi), big.NewInt(1)))
		}
		for _, value := range []*big.Int{new(big.Int).Sub(new(big.Int).Set(lo), big.NewInt(1)), lo, big.NewInt(0), hi, new(big.Int).Add(new(big.Int).Set(hi), big.NewInt(1))} {
			for _, scale := range []int32{0, 1, 18} {
				coefficient := new(big.Int).Mul(value, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
				if len(new(big.Int).Abs(new(big.Int).Set(coefficient)).String()) > 38 {
					continue
				}
				literal := Decimal128(decimal128.FromBigInt(coefficient), 38, scale)
				valid := value.Cmp(lo) >= 0 && value.Cmp(hi) <= 0
				for _, tolerant := range []bool{false, true} {
					var e Expr = CastTo(literal, typ)
					if tolerant {
						e = TryCastTo(literal, typ)
					}
					p, err := Compile(memory.NewSchema(), e)
					if err != nil {
						t.Fatal(err)
					}
					batch := memory.NewBatch()
					batch.Rows = 1
					w := evaluationState{}
					_, err = evaluateValueTest(p, batch, &w, testKernelSet())
					if !valid && !tolerant {
						if err == nil || !strings.Contains(err.Error(), "overflows") {
							t.Fatalf("%s to %s err=%v", value, typ, err)
						}
						w.Close()
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					r := w.regs[p.root]
					if isValidRegister(r, 0) != valid {
						t.Fatalf("validity %s to %s", value, typ)
					}
					if valid {
						if isSignedInteger(typ) {
							got, _ := registerInt64(r, 0)
							if got != value.Int64() {
								t.Fatal(got, value)
							}
						} else {
							got, _ := registerUint64(r, 0)
							if got != value.Uint64() {
								t.Fatal(got, value)
							}
						}
						// Full native-width round trip preserves the integer exactly, including Uint64 max.
						round, err := Compile(memory.NewSchema(), CastDecimal128(e, 38, scale))
						if err != nil {
							t.Fatal(err)
						}
						result, err := evaluateValueTest(round, batch, &evaluationState{}, testKernelSet())
						if err != nil || result.Decimal.Values[0].BigInt().Cmp(coefficient) != 0 {
							t.Fatal("integer round trip", typ, value, result, err)
						}
					}
					w.Close()
				}
			}
		}
	}
	for _, value := range []int64{-9999, -1, 1, 9999} {
		p, _ := Compile(memory.NewSchema(), CastTo(Decimal128(decimal128.FromI64(value), 4, 1), memory.TypeUint8))
		b := memory.NewBatch()
		b.Rows = 1
		if _, err := evaluateValueTest(p, b, &evaluationState{}, testKernelSet()); err == nil || !strings.Contains(err.Error(), "loses fractional") {
			t.Fatal("exactness must precede range", value, err)
		}
	}
}

func TestDecimalCastBindingAndIdentity(t *testing.T) {
	field, _ := memory.Decimal128Field("d", true, 8, 2)
	schema := memory.NewSchema(field, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	for _, value := range []Expr{I64(1), Col("s"), Col("d"), NullDecimal128(3, 1)} {
		for _, tolerant := range []bool{false, true} {
			e := CastDecimal128(value, 5, 3)
			e.Tolerant = tolerant
			b, err := Bind(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			if b.Field.DecimalPrecision != 5 || b.Field.DecimalScale != 3 || b.MayError == tolerant || tolerant && !b.Field.Nullable || !slices.Equal(Columns(e), b.Columns) {
				t.Fatalf("metadata %+v", b)
			}
			if _, err = Compile(schema, b.Expr); err != nil {
				t.Fatal("rebind", err)
			}
		}
	}
	for _, e := range []Expr{CastDecimal128(F64(1), 5, 2), TryCastDecimal128(Bool(true), 5, 2), CastTo(Col("d"), memory.TypeFloat64), TryCastTo(Col("d"), memory.TypeFloat32), CastTo(Col("d"), memory.TypeDecimal), CastTo(I64(1), memory.TypeDecimal), TryCastTo(Col("d"), memory.TypeString), CastDecimal128(I64(1), 0, 0), TryCastDecimal128(Str("0"), 39, 0), CastDecimal128(I64(1), 2, 3), CastDecimal128(I64(1), 2, -1), If(Bool(false), CastDecimal128(I64(0), 0, 0), NullDecimal128(1, 0)), CastDecimal128(Col("opaque"), 5, 0)} {
		s := memory.NewSchema(append(schema.Fields, memory.Field{Name: "opaque", Type: memory.TypeDecimal})...)
		if _, err := Compile(s, e); err == nil {
			t.Fatal("accepted invalid cast", e)
		}
	}
	bad := CastDecimal128(Str("bad"), 3, 1)
	p, err := CompileExpressions(schema, []NamedExpr{TryCastDecimal128(Str("bad"), 3, 1).Alias("try"), bad.Alias("strict")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 1
	if _, err := evaluateProjectionTest(p, b, &evaluationState{}, testKernelSet()); err == nil {
		t.Fatal("CSE conflated strict/try")
	}
	p, err = CompileExpressions(schema, []NamedExpr{CastDecimal128(I64(1), 3, 1).Alias("a"), CastDecimal128(I64(1), 4, 2).Alias("b"), CastDecimal128(I64(1), 3, 1).Alias("dup")})
	if err != nil {
		t.Fatal(err)
	}
	values, err := evaluateProjectionTest(p, b, &evaluationState{}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Decimal.Values[0] != decimal128.FromI64(10) || values[1].Decimal.Values[0] != decimal128.FromI64(100) || &values[0].Decimal.Values[0] != &values[2].Decimal.Values[0] {
		t.Fatal("CSE precision/scale/duplicate")
	}
}

func decimalCastFixture(t *testing.T, rows int) (Program, *memory.Batch) {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = rows
	v := &memory.Int64Vector{Values: make([]int64, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
	for i := range rows {
		v.Values[i] = int64(i - 1024)
		if i%7 != 0 {
			memory.Set(v.Validity, i)
		}
	}
	b.Int64s["i"] = v
	converted := CastDecimal128(Col("i"), 25, 5)
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}), []NamedExpr{converted.Alias("decimal"), CastTo(converted, memory.TypeInt64).Alias("integer"), CastTo(converted, memory.TypeString).Alias("text"), CastDecimal128(CastTo(converted, memory.TypeString), 30, 7).Alias("round"), converted.Alias("dup")})
	if err != nil {
		t.Fatal(err)
	}
	return p, b
}

func checkDecimalCastFixture(t *testing.T, b *memory.Batch, rows []int, out []ScalarValue) {
	t.Helper()
	n := b.Rows
	if rows != nil {
		n = len(rows)
	}
	for i := 0; i < n; i++ {
		row := i
		if rows != nil {
			row = rows[i]
		}
		valid := b.Int64s["i"].IsValid(row)
		value := b.Int64s["i"].Values[row]
		for _, index := range []int{0, 3, 4} {
			v := out[index].Decimal
			if v.IsValid(i) != valid {
				t.Fatal("null", row, index)
			}
			if valid {
				want := decimal128.FromI64(value).Mul(decimal128.GetScaleMultiplier(int(v.Scale)))
				if v.Values[i] != want {
					t.Fatal("coefficient", row, index)
				}
			} else if v.Values[i] != (memory.Decimal128{}) {
				t.Fatal("null payload")
			}
		}
		if out[1].Int64.IsValid(i) != valid || valid && out[1].Int64.Values[i] != value {
			t.Fatal("integer", row)
		}
		if valid {
			text := string(out[2].String.ValueBytes(i))
			if text != fmt.Sprintf("%d.00000", value) {
				t.Fatal("text", row, text)
			}
		}
	}
	if n > 0 && &out[0].Decimal.Values[0] != &out[4].Decimal.Values[0] {
		t.Fatal("duplicate did not share")
	}
}

func TestDecimalCastDenseSelectedReuse(t *testing.T) {
	p, _ := decimalCastFixture(t, 0)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		m := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, m)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{2049, 1, 65, 0, 64, 63, 130} {
			_, b := decimalCastFixture(t, n)
			before, _ := memory.CloneBatch(b)
			out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			checkDecimalCastFixture(t, b, nil, out)

			if m.Used() != w.MemoryBytes() {
				t.Fatal("accounting")
			}
			if n > 0 && !reflect.DeepEqual(before, b) {
				t.Fatal("source changed")
			}
		}
		w.Close()
		w.Close()
		if m.Used() != 0 {
			t.Fatal("close leaked")
		}
	}
	// Verify fixed-width selected fast path separately from string fallback.
	schema := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true})
	p, err := Compile(schema, CastTo(CastDecimal128(Col("i"), 25, 5), memory.TypeInt64))
	if err != nil || !p.SupportsSelection() {
		t.Fatal("selection plan", err)
	}
	_, b := decimalCastFixture(t, 67)
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	out, err := w.EvaluateSelected(context.Background(), b, []int{1, 2, 64, 66}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range []int{1, 2, 64, 66} {
		if out[0].Int64.Values[i] != b.Int64s["i"].Values[row] {
			t.Fatal("selected", row)
		}
	}
}

func TestDecimalCastGuardsTryErrorsAndDictionary(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	strict := CastDecimal128(Col("s"), 5, 2)
	zero := CastDecimal128(I64(0), 5, 2)
	for _, e := range []Expr{If(EQ(Col("s"), Str("bad")), zero, strict), Case([]WhenThen{{When: EQ(Col("s"), Str("bad")), Then: zero}}, strict), Coalesce(zero, strict), If(Bool(false), strict, zero)} {
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		w := evaluationState{}
		if _, err := evaluateValueTest(p, numericCastBatch("bad", "1.20"), &w, testKernelSet()); err != nil {
			t.Fatal("guard", err)
		}
		w.Close()
	}
	for _, e := range []Expr{TryCastDecimal128(CastTo(Str("bad"), memory.TypeInt64), 5, 2), TryCastTo(CastDecimal128(Str("bad"), 5, 2), memory.TypeInt64), CastDecimal128(Add(I64(math.MaxInt64), I64(1)), 38, 0), If(Bool(true), strict, zero)} {
		p, err := Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		w := evaluationState{}
		if _, err := evaluateValueTest(p, numericCastBatch("bad"), &w, testKernelSet()); err == nil {
			t.Fatal("child/strict error suppressed", e)
		}
		w.Close()
	}
	p, err := Compile(schema, TryCastDecimal128(Col("s"), 5, 2))
	if err != nil {
		t.Fatal(err)
	}
	b := numericCastBatch("1.20", "bad", "1234", "1.234", "NULL poison")
	b.Strings["s"].Validity = memory.Bitmap{15}
	w := evaluationState{}
	out, err := evaluateValueTest(p, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i := range b.Rows {
		if out.Decimal.IsValid(i) != (i == 0) {
			t.Fatal("try validity", i)
		}
	}
	w.Close()
	schema.Fields[0].Encoding = memory.EncodingDictionary
	b = memory.NewBatch()
	b.Rows = 5
	b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2, 0, 999}, Offsets: []uint32{0, 4, 7, 13}, Data: []byte("1.20bad1.2340"), Validity: memory.Bitmap{15}, DictionaryDomain: "decimal-cast"}
	p, err = Compile(schema, TryCastDecimal128(Col("s"), 5, 2))
	if err != nil {
		t.Fatal(err)
	}
	out, err = evaluateValueTest(p, b, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i := range b.Rows {
		if out.Decimal.IsValid(i) != (i == 0 || i == 3) {
			t.Fatal("dictionary try", i)
		}
	}
	w.Close()
	b.DictStrings["s"].Indices[0] = 999
	if _, err = evaluateValueTest(p, b, &w, testKernelSet()); err == nil {
		t.Fatal("try swallowed storage corruption")
	}
	w.Close()
}

func TestDecimalCastAdmissionCancellationAndConcurrentWorkspaces(t *testing.T) {
	p, b := decimalCastFixture(t, 67)
	for _, selected := range []bool{false, true} {
		current := p
		if selected {
			var err error
			current, err = Compile(memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64, Nullable: true}), CastDecimal128(Col("i"), 25, 5))
			if err != nil {
				t.Fatal(err)
			}
		}
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, []int{0, 1, 2, 63, 64, 65, 66}, testKernelSet())
			}
			return w.Evaluate(context.Background(), b, testKernelSet())
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, _ := NewWorkspace(context.Background(), current, probe)
		if _, err := run(w); err != nil {
			t.Fatal(err)
		}
		calls := probe.calls
		w.Close()
		for fail := 1; fail <= calls; fail++ {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, _ := NewWorkspace(context.Background(), current, m)
			if _, err := run(w); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("admission %d/%d selected=%v err=%v", fail, calls, selected, err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("failed accounting")
			}
			m.fail = 0
			out, err := run(w)
			if err != nil {
				t.Fatal("retry", err)
			}
			var rows []int
			if selected {
				rows = []int{0, 1, 2, 63, 64, 65, 66}
			}
			if selected {
				for i, row := range rows {
					v := out[0].Decimal
					valid := b.Int64s["i"].IsValid(row)
					if v.IsValid(i) != valid || valid && v.Values[i] != decimal128.FromI64(b.Int64s["i"].Values[row]).Mul(decimal128.GetScaleMultiplier(5)) {
						t.Fatal("selected budget result", row)
					}
				}
			} else {
				checkDecimalCastFixture(t, b, nil, out)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leak")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w, _ := NewWorkspace(context.Background(), p, nil)
	if _, err := w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel", err)
	}
	w.Close()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, _ := NewWorkspace(context.Background(), p, nil)
			defer w.Close()
			for range 3 {
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Error(err)
					return
				}
				checkDecimalCastFixture(t, b, nil, out)
			}
		}()
	}
	wg.Wait()
}
