package expr

import (
	"context"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func decimalDivideOracle(a memory.Decimal128, as int32, b memory.Decimal128, bs, precision, scale int32) (*big.Int, decimalDivideStatus) {
	// Construct numerator and denominator from the two independently scaled
	// operands, without reusing the production exponent or fixed-width helpers.
	if b.BigInt().Sign() == 0 {
		return nil, decimalDivideZero
	}
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	numerator := new(big.Int).Mul(a.BigInt(), pow(bs))
	numerator.Mul(numerator, pow(scale))
	denominator := new(big.Int).Mul(b.BigInt(), pow(as))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if remainder.Sign() != 0 {
		return nil, decimalDivideInexact
	}
	if new(big.Int).Abs(new(big.Int).Set(quotient)).Cmp(pow(precision)) >= 0 {
		return nil, decimalDivideOverflow
	}
	return quotient, decimalDivideOK
}

func TestDecimalDivideBigIntOracle(t *testing.T) {
	check := func(a memory.Decimal128, as int32, b memory.Decimal128, bs, p, s int32) {
		t.Helper()
		want, status := decimalDivideOracle(a, as, b, bs, p, s)
		got, actual := decimalDivideValue(a, as, b, bs, p, s)
		if actual != status || status == decimalDivideOK && got.BigInt().Cmp(want) != 0 {
			t.Fatalf("a=%s/%d b=%s/%d target=%d/%d got=%s/%d want=%v/%d", a.BigInt(), as, b.BigInt(), bs, p, s, got.BigInt(), actual, want, status)
		}
	}
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	values := []memory.Decimal128{{}, decimal128.FromI64(1), decimal128.FromI64(-1), decimal128.FromI64(3), decimal128.FromI64(-8), decimal128.FromI64(1<<53 + 1), decimal128.New(0, ^uint64(0)), decimal128.New(1, 0), max, max.Negate()}
	// Every pair of input scales and every target scale, including exponent
	// -38 and +76. Values and precisions vary separately from those scales.
	for as := int32(0); as <= 38; as++ {
		for bs := int32(0); bs <= 38; bs++ {
			for s := int32(0); s <= 38; s++ {
				for i, a := range values {
					b := values[(i+int(as+bs+s))%len(values)]
					check(a, as, b, bs, 38, s)
					check(a, as, b, bs, max32(s, 1), s)
				}
			}
		}
	}
	// Powers around every decimal precision boundary, both signs, exact and
	// inexact division, and large intermediates that cancel to valid quotients.
	for p := int32(1); p <= 38; p++ {
		for _, sign := range []int64{-1, 1} {
			for _, delta := range []int64{-1, 0, 1} {
				v := decimal128.GetScaleMultiplier(int(p)).Add(decimal128.FromI64(delta))
				if !memory.Decimal128FitsPrecision(v, 38) {
					continue
				}
				if sign < 0 {
					v = v.Negate()
				}
				check(v, 0, decimal128.FromI64(1), 0, p, 0)
				check(v, 0, decimal128.FromI64(3), 0, p, 0)
			}
		}
		check(max, 38, max, 38, p, p)
		check(max, 0, max, 0, p, p-1)
		check(max, 38, max, 0, p, p)
	}
	rng := rand.New(rand.NewSource(382076))
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	for i := 0; i < 20000; i++ {
		a, b := new(big.Int).Rand(rng, limit), new(big.Int).Rand(rng, limit)
		if i%2 == 0 {
			a.Neg(a)
		}
		if i%3 == 0 {
			b.Neg(b)
		}
		p := int32(rng.Intn(38) + 1)
		check(decimal128.FromBigInt(a), int32(rng.Intn(39)), decimal128.FromBigInt(b), int32(rng.Intn(39)), p, int32(rng.Intn(int(p)+1)))
		// High success-rate cases prevent a random test from testing only errors.
		b.Quo(b, big.NewInt(100))
		a.Mul(b, big.NewInt(int64(i%99+1)))
		check(decimal128.FromBigInt(a), 12, decimal128.FromBigInt(b), 12, 38, 20)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		decimalDivideValue(max, 0, max, 0, 38, 37)
		decimalDivideValue(max, 0, decimal128.FromI64(1), 38, 38, 38)
		decimalDivideValue(max, 38, decimal128.FromI64(3), 0, 1, 0)
		decimalDivideValue(decimal128.FromI64(1), 0, memory.Decimal128{}, 0, 1, 0)
	}); allocs != 0 {
		t.Fatal("division coefficient path allocated", allocs)
	}
}

func max32(a, b int32) int32 { return max(a, b) }

func TestDecimalDivideBindingAndIdentity(t *testing.T) {
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	for _, leftNullable := range []bool{false, true} {
		for _, rightNullable := range []bool{false, true} {
			left, _ := memory.Decimal128Field("a", leftNullable, 38, 38)
			right, _ := memory.Decimal128Field("b", rightNullable, 18, 0)
			for p := int32(1); p <= 38; p++ {
				for s := int32(0); s <= p; s++ {
					e := DivideDecimal128(Col("a"), Col("b"), p, s)
					bound, err := Bind(memory.NewSchema(left, right), e)
					if err != nil {
						t.Fatal(err)
					}
					if !bound.MayError || bound.Field.Nullable != (leftNullable || rightNullable) || bound.Field.DecimalPrecision != p || bound.Field.DecimalScale != s || !reflect.DeepEqual(bound.Columns, []string{"a", "b"}) || !reflect.DeepEqual(Columns(e), bound.Columns) {
						t.Fatalf("wrong metadata: %+v", bound)
					}
					program, err := Compile(memory.NewSchema(left, right), bound.Expr)
					if err != nil || !program.SupportsSelection() || !reflect.DeepEqual(program.ResultField(), bound.OutputField("")) {
						t.Fatal("rebind", err)
					}
				}
			}
		}
	}
	for _, e := range []Expr{
		DivideDecimal128(one, one, 0, 0), DivideDecimal128(one, one, 39, 0), DivideDecimal128(one, one, 1, -1), DivideDecimal128(one, one, 1, 2),
		DivideDecimal128(I64(1), one, 38, 0), DivideDecimal128(one, I64(1), 38, 0), DivideDecimal128(F64(1), one, 38, 0), DivideDecimal128(one, Str("1"), 38, 0),
		DivideDecimal128(Col("opaque"), one, 38, 0), DivideDecimal128(one, Col("opaque"), 38, 0), DivideDecimal128(nil, one, 38, 0), DivideDecimal128(one, nil, 38, 0),
		If(Bool(false), DivideDecimal128(one, one, 0, 0), one), DivideDecimal128(Decimal128(decimal128.FromI64(10), 1, 0), one, 38, 0), Div(one, one),
	} {
		if _, err := Compile(memory.NewSchema(memory.Field{Name: "opaque", Type: memory.TypeDecimal}), e); err == nil {
			t.Fatalf("accepted invalid division %v", e)
		}
	}
	if DivideDecimal128(one, one, 2, 1).String() != "DIVIDE_DECIMAL128(DECIMAL128(1,1,0),DECIMAL128(1,1,0),2,1)" {
		t.Fatal("unstable spelling")
	}
	two := Decimal128(decimal128.FromI64(2), 1, 0)
	program, err := CompileExpressions(memory.NewSchema(), []NamedExpr{
		DivideDecimal128(one, two, 2, 1).Alias("a"), DivideDecimal128(two, one, 2, 1).Alias("b"), DivideDecimal128(one, two, 3, 1).Alias("c"), DivideDecimal128(one, two, 3, 2).Alias("d"), DivideDecimal128(one, two, 3, 2).Alias("dup"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		for j := i + 1; j < 4; j++ {
			if program.outputs[i].root == program.outputs[j].root {
				t.Fatal("CSE lost operand order or target metadata")
			}
		}
	}
	if program.outputs[3].root != program.outputs[4].root {
		t.Fatal("duplicate was not shared")
	}
}

func TestDecimalDivideBinaryNullGuardAndErrorOrder(t *testing.T) {
	aField, _ := memory.Decimal128Field("a", true, 38, 3)
	bField, _ := memory.Decimal128Field("b", true, 38, 1)
	schema := memory.NewSchema(aField, bField, memory.Field{Name: "guard", Type: memory.TypeBool})
	batch := memory.NewBatch()
	batch.Rows = 129
	a := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 129), Validity: memory.EnsureBitmap(nil, 129), Precision: 38, Scale: 3}
	b := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 129), Validity: memory.EnsureBitmap(nil, 129), Precision: 38, Scale: 1}
	guard := &memory.Uint32Vector{Values: make([]uint32, 129)}
	for i := range a.Values {
		a.Values[i] = decimal128.FromI64(int64((i - 64) * 6000))
		b.Values[i] = decimal128.FromI64(30)
		memory.Set(a.Validity, i)
		memory.Set(b.Validity, i)
		guard.Values[i] = 1
		if i%7 == 0 {
			a.Validity[i/64] &^= 1 << uint(i%64)
			a.Values[i] = decimal128.New(math.MinInt64, 0)
			b.Values[i] = memory.Decimal128{}
		}
		if i%11 == 0 {
			b.Validity[i/64] &^= 1 << uint(i%64)
			b.Values[i] = decimal128.New(math.MinInt64, 0)
		}
		if i%13 == 0 {
			guard.Values[i] = 0
			a.Values[i] = decimal128.FromI64(1)
			b.Values[i] = memory.Decimal128{}
		}
	}
	batch.Decimals["a"], batch.Decimals["b"], batch.Uint32s["guard"] = a, b, guard
	quotient := DivideDecimal128(Col("a"), Col("b"), 8, 2)
	zero := Decimal128(memory.Decimal128{}, 8, 2)
	program, err := CompileExpressions(schema, []NamedExpr{Named(If(Col("guard"), quotient, zero), "q"), Named(If(Col("guard"), quotient, zero), "dup")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		manager := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), program, manager)
		for _, selected := range []bool{false, true} {
			rows := make([]int, 0)
			for i := 0; i < 129; i++ {
				if !selected || i%2 == 0 {
					rows = append(rows, i)
				}
			}
			var out []ScalarValue
			if selected {
				out, err = w.EvaluateSelected(context.Background(), batch, rows, kernel.New(mode))
			} else {
				out, err = w.Evaluate(context.Background(), batch, kernel.New(mode))
			}
			if err != nil {
				t.Fatal(err)
			}
			for i, row := range rows {
				v := out[0].Decimal
				valid := guard.Values[row] == 0 || a.IsValid(row) && b.IsValid(row)
				if v.IsValid(i) != valid {
					t.Fatal("binary null mismatch", row)
				}
				want := big.NewInt(0)
				if guard.Values[row] != 0 && valid {
					var status decimalDivideStatus
					want, status = decimalDivideOracle(a.Values[row], 3, b.Values[row], 1, 8, 2)
					if status != decimalDivideOK {
						t.Fatal("bad fixture")
					}
				}
				if v.Values[i].BigInt().Cmp(want) != 0 {
					t.Fatal("binary quotient mismatch", row)
				}
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatal("accounting drift")
			}
		}
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("leak")
		}
	}
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	z := Decimal128(memory.Decimal128{}, 1, 0)
	divideZero := DivideDecimal128(one, z, 1, 0)
	inexact := DivideDecimal128(one, Decimal128(decimal128.FromI64(3), 1, 0), 1, 0)
	for _, tc := range []struct {
		e    Expr
		want string
	}{
		{divideZero, "by zero"}, {DivideDecimal128(z, z, 1, 0), "by zero"},
		{DivideDecimal128(inexact, divideZero, 1, 0), "loses fractional digits"},
		{DivideDecimal128(divideZero, inexact, 1, 0), "by zero"},
		{DivideDecimal128(NullDecimal128(1, 0), divideZero, 1, 0), "by zero"},
		{DivideDecimal128(divideZero, NullDecimal128(1, 0), 1, 0), "by zero"},
		{DivideDecimal128(NullDecimal128(1, 0), z, 1, 0), ""}, {DivideDecimal128(z, NullDecimal128(1, 0), 1, 0), ""},
		{If(Bool(false), divideZero, z), ""}, {Case([]WhenThen{{When: Bool(false), Then: divideZero}}, z), ""}, {Coalesce(one, divideZero), ""}, {In(one, one, divideZero), ""},
		{And(Bool(false), EQ(divideZero, z)), "by zero"},
	} {
		p, err := Compile(memory.NewSchema(), tc.e)
		if err != nil {
			t.Fatal(err)
		}
		m := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, m)
		_, err = w.Evaluate(context.Background(), batch, testKernelSet())
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want+" at row 0")) {
			t.Fatalf("%s: %v", tc.e, err)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("failure leak")
		}
	}
}

func TestDecimalDivideWideQuotientRemainder(t *testing.T) {
	rng := rand.New(rand.NewSource(64128384))
	bigValue := func(v decimalDivideMagnitude) *big.Int {
		n := new(big.Int)
		for i := len(v) - 1; i >= 0; i-- {
			n.Lsh(n, 64)
			n.Add(n, new(big.Int).SetUint64(v[i]))
		}
		return n
	}
	for i := 0; i < 5000; i++ {
		var n, d decimalDivideMagnitude
		for j := range n {
			n[j] = rng.Uint64()
			d[j] = rng.Uint64()
		}
		for j := i%6 + 1; j < len(d); j++ {
			d[j] = 0
		}
		d[0] |= 1
		if i%5 == 0 {
			n = d
		}
		q, r := decimalDivideQuoRem(n, d)
		wantQ, wantR := new(big.Int), new(big.Int)
		wantQ.QuoRem(bigValue(n), bigValue(d), wantR)
		if bigValue(q).Cmp(wantQ) != 0 || bigValue(r).Cmp(wantR) != 0 {
			t.Fatalf("case %d quotient or remainder mismatch", i)
		}
	}
}

func TestDecimalDivideTwoInputAdmissionAndSelectedZero(t *testing.T) {
	aField, _ := memory.Decimal128Field("a", true, 8, 2)
	bField, _ := memory.Decimal128Field("b", true, 4, 1)
	schema := memory.NewSchema(aField, bField)
	batch := memory.NewBatch()
	batch.Rows = 67
	a := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 67), Precision: 8, Scale: 2}
	b := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 67), Precision: 4, Scale: 1}
	for i := range a.Values {
		a.Values[i] = decimal128.FromI64(int64(i * 600))
		b.Values[i] = decimal128.FromI64(30)
	}
	batch.Decimals["a"], batch.Decimals["b"] = a, b
	p, err := Compile(schema, DivideDecimal128(Col("a"), Col("b"), 8, 2))
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []bool{false, true} {
		rows := []int{0, 2, 63, 64, 66}
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), batch, rows, testKernelSet())
			}
			return w.Evaluate(context.Background(), batch, testKernelSet())
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, _ := NewWorkspace(context.Background(), p, probe)
		if _, err := run(w); err != nil {
			t.Fatal(err)
		}
		calls := probe.calls
		w.Close()
		for fail := 1; fail <= calls; fail++ {
			manager := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, _ := NewWorkspace(context.Background(), p, manager)
			if _, err := run(w); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal("admission was not rejected", selected, fail, err)
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatal("failed admission accounting")
			}
			manager.fail = 0
			out, err := run(w)
			if err != nil {
				t.Fatal(err)
			}
			for i, got := range out[0].Decimal.Values {
				row := i
				if selected {
					row = rows[i]
				}
				if got != decimal128.FromI64(int64(row*200)) {
					t.Fatal("retry changed result", row)
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("retry leak")
			}
		}
		b.Values[1], b.Values[63] = memory.Decimal128{}, memory.Decimal128{}
		manager := memory.NewBudgetManager(0)
		w, _ = NewWorkspace(context.Background(), p, manager)
		_, err = run(w)
		want := "by zero at row 1"
		if selected {
			want = "by zero at row 2"
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatal("first zero row", err)
		}
		if manager.Used() != 0 {
			t.Fatal("semantic error retained memory")
		}
		w.Close()
		b.Values[1], b.Values[63] = decimal128.FromI64(30), decimal128.FromI64(30)
	}
}

func BenchmarkDecimalDivideCoefficient(b *testing.B) {
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	for _, tc := range []struct {
		name         string
		a, b         memory.Decimal128
		as, bs, p, s int32
	}{
		{"small", decimal128.FromI64(1200), decimal128.FromI64(3), 2, 0, 8, 2},
		{"wide_cancel", max, max, 0, 0, 38, 37},
		{"negative_exponent", decimal128.GetScaleMultiplier(37), decimal128.FromI64(1), 37, 0, 1, 0},
		{"wide_divisor", max, max, 38, 37, 38, 38},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var value memory.Decimal128
			var status decimalDivideStatus
			for b.Loop() {
				value, status = decimalDivideValue(tc.a, tc.as, tc.b, tc.bs, tc.p, tc.s)
			}
			if status != decimalDivideOK {
				b.Fatal(value, status)
			}
		})
	}
}
