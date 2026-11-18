package expr

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Build numerator/denominator from independently scaled operand definitions,
// rather than the production exponent formula. Round the whole remainder once.
func divideRoundBigOracle(a *big.Int, as int32, b *big.Int, bs, p, s int32, mode DecimalRoundingMode) (*big.Int, string) {
	if b.Sign() == 0 {
		return nil, "zero"
	}
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	numerator := new(big.Int).Mul(a, pow(bs))
	numerator.Mul(numerator, pow(s))
	denominator := new(big.Int).Mul(b, pow(as))
	negative := numerator.Sign()*denominator.Sign() < 0
	numerator.Abs(numerator)
	denominator.Abs(denominator)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(numerator, denominator, r)
	inc := false
	if r.Sign() != 0 {
		switch mode {
		case DecimalRoundAwayFromZero:
			inc = true
		case DecimalRoundFloor:
			inc = negative
		case DecimalRoundCeil:
			inc = !negative
		case DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero:
			cmp := new(big.Int).Lsh(r, 1).Cmp(denominator)
			inc = cmp > 0 || (cmp == 0 && (mode == DecimalRoundHalfAwayFromZero || q.Bit(0) == 1))
		}
	}
	if inc {
		q.Add(q, big.NewInt(1))
	}
	if negative {
		q.Neg(q)
	}
	if new(big.Int).Abs(new(big.Int).Set(q)).Cmp(pow(p)) >= 0 {
		return nil, "overflow"
	}
	return q, "ok"
}
func TestDecimalDivideRoundOracleTable(t *testing.T) {
	modes := []DecimalRoundingMode{DecimalRoundTowardZero, DecimalRoundAwayFromZero, DecimalRoundFloor, DecimalRoundCeil, DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero}
	for _, tc := range []struct {
		a, b int64
		want [6]int64
	}{{5, 2, [6]int64{2, 3, 2, 3, 2, 3}}, {-5, 2, [6]int64{-2, -3, -3, -2, -2, -3}}, {5, -2, [6]int64{-2, -3, -3, -2, -2, -3}}, {-5, -2, [6]int64{2, 3, 2, 3, 2, 3}}, {7, 2, [6]int64{3, 4, 3, 4, 4, 4}}, {1, 3, [6]int64{0, 1, 0, 1, 0, 0}}, {-1, 3, [6]int64{0, -1, -1, 0, 0, 0}}} {
		for i, m := range modes {
			got, status := divideRoundBigOracle(big.NewInt(tc.a), 0, big.NewInt(tc.b), 0, 2, 0, m)
			if status != "ok" || got.Cmp(big.NewInt(tc.want[i])) != 0 {
				t.Fatalf("literal oracle %d/%d mode%d got%v/%s", tc.a, tc.b, m, got, status)
			}
		}
	}
}

func divideRoundFixture(n int) (memory.Schema, *memory.Batch) {
	a, _ := memory.Decimal128Field("a", true, 38, 2)
	b, _ := memory.Decimal128Field("b", true, 38, 1)
	batch := memory.NewBatch()
	batch.Rows = n
	av := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: make(memory.Bitmap, memory.WordsFor(n)), Precision: 38, Scale: 2}
	bv := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: make(memory.Bitmap, memory.WordsFor(n)), Precision: 38, Scale: 1}
	for i := 0; i < n; i++ {
		av.Values[i] = decimal128.FromI64(int64(i%97-48)*100 + 25)
		bv.Values[i] = decimal128.FromI64(int64(i%5+1) * 10)
		if i%3 == 0 {
			bv.Values[i] = bv.Values[i].Negate()
		}
		if i%7 != 0 {
			memory.Set(av.Validity, i)
		} else {
			av.Values[i] = decimal128.New(math.MinInt64, 0)
		}
		if i%11 != 0 {
			memory.Set(bv.Validity, i)
		} else {
			bv.Values[i] = decimal128.New(math.MinInt64, 0)
		}
	}
	batch.Decimals["a"], batch.Decimals["b"] = av, bv
	return memory.NewSchema(a, b), batch
}

var divideRoundTestModes = []DecimalRoundingMode{DecimalRoundTowardZero, DecimalRoundAwayFromZero, DecimalRoundFloor, DecimalRoundCeil, DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero}

func TestDecimalDivideRoundBigIntMatrix(t *testing.T) {
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	max := new(big.Int).Sub(pow(38), big.NewInt(1))
	check := func(a *big.Int, as int32, b *big.Int, bs, p, s int32, m DecimalRoundingMode) {
		t.Helper()
		want, status := divideRoundBigOracle(a, as, b, bs, p, s, m)
		got, actual := decimalDivideRoundValue(decimal128.FromBigInt(a), as, decimal128.FromBigInt(b), bs, p, s, m)
		expected := decimalDivideOK
		switch status {
		case "zero":
			expected = decimalDivideZero
		case "overflow":
			expected = decimalDivideOverflow
		}
		if actual != expected || status == "ok" && got.BigInt().Cmp(want) != 0 {
			t.Fatalf("a%s/%d b%s/%d p%d s%d mode%s got%s/%d want%v/%s", a, as, b, bs, p, s, m, got.BigInt(), actual, want, status)
		}
	}
	// Every input-scale pair and every target scale, all modes. A mix of large
	// common factors and small odd denominators prevents an all-overflow matrix.
	vals := []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(-5), big.NewInt(3), max, new(big.Int).Neg(max), new(big.Int).Lsh(big.NewInt(1), 64)}
	for as := int32(0); as <= 38; as++ {
		for bs := int32(0); bs <= 38; bs++ {
			for s := int32(0); s <= 38; s++ {
				a := vals[int(as+bs+s)%len(vals)]
				b := vals[1+int(2*as+bs+s)%6]
				for _, m := range divideRoundTestModes {
					check(a, as, b, bs, 38, s, m)
				}
			}
		}
	}
	for p := int32(1); p <= 38; p++ {
		for s := int32(0); s <= p; s++ {
			for _, m := range divideRoundTestModes {
				check(max, 38, max, 38, p, s, m)
				check(big.NewInt(-5), 1, big.NewInt(2), 0, p, s, m)
				check(max, 0, big.NewInt(1), 38, p, s, m)
			}
		}
	}
	// Literal half/near-half cases for odd/even denominators, both operand signs.
	for _, den := range []int64{2, 3, 5, 10, math.MaxInt64} {
		for _, q := range []int64{0, 1, 2, 9} {
			for _, r := range []int64{0, den / 2, den/2 + 1, den - 1} {
				a := new(big.Int).Add(new(big.Int).Mul(big.NewInt(q), big.NewInt(den)), big.NewInt(r))
				for _, sa := range []int64{-1, 1} {
					for _, sb := range []int64{-1, 1} {
						for _, m := range divideRoundTestModes {
							check(new(big.Int).Mul(a, big.NewInt(sa)), 0, big.NewInt(den*sb), 0, 38, 0, m)
							check(new(big.Int).Mul(a, big.NewInt(sa)), 0, big.NewInt(den*sb), 0, 1, 0, m)
						}
					}
				}
			}
		}
	}
	// Multi-limb denominator half neighborhoods and numerator cancellation.
	for _, digits := range []int32{19, 20, 37, 38} {
		den := new(big.Int).Sub(pow(digits), big.NewInt(2))
		for _, delta := range []int64{-1, 0, 1} {
			a := new(big.Int).Quo(new(big.Int).Set(den), big.NewInt(2))
			a.Add(a, big.NewInt(delta))
			for _, m := range divideRoundTestModes {
				check(a, 0, den, 0, 38, 0, m)
				check(new(big.Int).Neg(a), 0, den, 0, 38, 0, m)
			}
		}
	}
	for p := int32(1); p <= 37; p++ {
		a := new(big.Int).Sub(pow(p+1), big.NewInt(5))
		for _, m := range divideRoundTestModes {
			check(a, 0, big.NewInt(10), 0, p, 0, m)
			check(new(big.Int).Neg(a), 0, big.NewInt(10), 0, p, 0, m)
		}
	}
	for _, m := range divideRoundTestModes {
		check(max, 38, max, 38, 38, 37, m)
		check(max, 0, max, 38, 38, 38, m)
		check(big.NewInt(1), 38, max, 0, 38, 0, m)
		check(big.NewInt(0), 0, big.NewInt(0), 0, 1, 0, m)
		check(big.NewInt(1), 0, big.NewInt(0), 0, 1, 0, m)
		check(big.NewInt(1251), 0, big.NewInt(1000), 0, 2, 1, m)
	}
	rng := rand.New(rand.NewSource(178392))
	limit := pow(38)
	for i := 0; i < 10000; i++ {
		a, b := new(big.Int).Rand(rng, limit), new(big.Int).Rand(rng, limit)
		if i%2 == 0 {
			a.Neg(a)
		}
		if i%3 == 0 {
			b.Neg(b)
		}
		p := int32(rng.Intn(38) + 1)
		check(a, int32(rng.Intn(39)), b, int32(rng.Intn(39)), p, int32(rng.Intn(int(p)+1)), divideRoundTestModes[i%6])
	}
}
func divideRoundProgram(t *testing.T, identity bool) Program {
	t.Helper()
	schema, _ := divideRoundFixture(0)
	es := []NamedExpr{}
	for i, m := range divideRoundTestModes {
		es = append(es, Named(DivideRoundDecimal128(Col("a"), Col("b"), 20, 2, m), fmt.Sprint(i)))
	}
	es = append(es, Named(DivideRoundDecimal128(Col("a"), Col("b"), 20, 2, DecimalRoundHalfEven), "duplicate"))
	p, err := CompileExpressions(schema, es)
	if err != nil {
		t.Fatal(err)
	}
	if identity {
		p.instructions = append([]instruction(nil), p.instructions...)
		for i := range p.instructions {
			p.instructions[i].storageOwner = i + 1
		}
	}
	return p
}
func divideRoundCheck(t *testing.T, b *memory.Batch, rows []int, out []ScalarValue) {
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
		valid := b.Decimals["a"].IsValid(row) && b.Decimals["b"].IsValid(row)
		for j, v := range out {
			mode := DecimalRoundHalfEven
			if j < 6 {
				mode = divideRoundTestModes[j]
			}
			d := v.Decimal
			if d.Precision != 20 || d.Scale != 2 || d.IsValid(i) != valid {
				t.Fatal("metadata/validity", row, j)
			}
			if valid {
				want, status := divideRoundBigOracle(b.Decimals["a"].Values[row].BigInt(), 2, b.Decimals["b"].Values[row].BigInt(), 1, 20, 2, mode)
				if status != "ok" || d.Values[i].BigInt().Cmp(want) != 0 {
					t.Fatal("public quotient oracle", row, j)
				}
			} else if d.Values[i] != (memory.Decimal128{}) {
				t.Fatal("NULL payload")
			}
		}
	}
	if n > 0 && &out[4].Decimal.Values[0] != &out[6].Decimal.Values[0] {
		t.Fatal("same mode CSE")
	}
}
func TestDecimalDivideRoundDenseSelectedStorage(t *testing.T) {
	for _, identity := range []bool{false, true} {
		for _, km := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			p := divideRoundProgram(t, identity)
			if !p.SupportsSelection() {
				t.Fatal("selected missing")
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			for _, n := range []int{0, 1, 63, 64, 65, 130, 2049, 1, 0} {
				_, b := divideRoundFixture(n)
				before, err := memory.CloneBatch(b)
				if err != nil {
					t.Fatal(err)
				}
				out, err := w.Evaluate(context.Background(), b, kernel.New(km))
				if err != nil {
					t.Fatal(err)
				}
				divideRoundCheck(t, b, nil, out)
				for _, step := range []int{1, 2, 65} {
					rows := make([]int, 0)
					for i := 0; i < n; i += step {
						rows = append(rows, i)
					}
					out, err = w.EvaluateSelected(context.Background(), b, rows, kernel.New(km))
					if err != nil {
						t.Fatal(err)
					}
					divideRoundCheck(t, b, rows, out)
				}
				if n > 0 && !reflect.DeepEqual(before, b) {
					t.Fatal("input changed")
				}
				if m.Used() != w.MemoryBytes() {
					t.Fatal("charge drift")
				}
			}
			_, b := divideRoundFixture(3)
			for _, a := range []int64{125, 135, -125, 125} {
				b.Decimals["a"].Values[1] = decimal128.FromI64(a)
				out, err := w.Evaluate(context.Background(), b, kernel.New(km))
				if err != nil {
					t.Fatal(err)
				}
				divideRoundCheck(t, b, nil, out)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("close leak")
			}
		}
	}
}

func TestDecimalDivideRoundBindingAndCSE(t *testing.T) {
	schema, _ := divideRoundFixture(0)
	es := []NamedExpr{}
	for i, mode := range divideRoundTestModes {
		e := DivideRoundDecimal128(Col("a"), Col("b"), 20, 2, mode)
		bound, err := Bind(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		if !bound.MayError || !bound.Field.Nullable || bound.Field.DecimalPrecision != 20 || bound.Field.DecimalScale != 2 || !reflect.DeepEqual(Columns(e), []string{"a", "b"}) {
			t.Fatal("bound metadata")
		}
		if !strings.Contains(e.String(), mode.String()) {
			t.Fatal("mode diagnostic")
		}
		es = append(es, Named(e, fmt.Sprint(i)))
	}
	es = append(es, Named(DivideRoundDecimal128(Col("a"), Col("b"), 20, 2, DecimalRoundHalfEven), "same"), Named(DivideRoundDecimal128(Col("b"), Col("a"), 20, 2, DecimalRoundHalfEven), "reverse"), Named(DivideRoundDecimal128(Col("a"), Col("b"), 21, 2, DecimalRoundHalfEven), "precision"), Named(DivideRoundDecimal128(Col("a"), Col("b"), 20, 3, DecimalRoundHalfEven), "scale"))
	p, err := CompileExpressions(schema, es)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		for j := i + 1; j < 6; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("mode merged")
			}
		}
	}
	if p.outputs[4].root != p.outputs[6].root {
		t.Fatal("duplicate not CSE")
	}
	for _, i := range []int{7, 8, 9} {
		if p.outputs[4].root == p.outputs[i].root {
			t.Fatal("ordered children or target merged")
		}
	}
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	for _, e := range []Expr{DivideRoundDecimal128(one, one, 0, 0, DecimalRoundHalfEven), DivideRoundDecimal128(one, one, 39, 0, DecimalRoundHalfEven), DivideRoundDecimal128(one, one, 1, -1, DecimalRoundHalfEven), DivideRoundDecimal128(one, one, 1, 2, DecimalRoundHalfEven), DivideRoundDecimal128(I64(1), one, 1, 0, DecimalRoundHalfEven), DivideRoundDecimal128(one, F64(1), 1, 0, DecimalRoundHalfEven), DivideRoundDecimal128(nil, one, 1, 0, DecimalRoundHalfEven), DivideRoundDecimal128(one, nil, 1, 0, DecimalRoundHalfEven), If(Bool(false), DivideRoundDecimal128(one, one, 1, 0, 0), zero), If(Bool(false), DivideRoundDecimal128(one, one, 1, 0, 255), zero)} {
		if _, err := Compile(memory.NewSchema(), e); err == nil {
			t.Fatalf("invalid bind accepted %v", e)
		}
	}
}
func TestDecimalDivideRoundGuardsZeroNullAndErrors(t *testing.T) {
	for _, kind := range []string{"zero", "overflow"} {
		t.Run(kind, func(t *testing.T) {
			af, _ := memory.Decimal128Field("a", false, 3, 0)
			bf, _ := memory.Decimal128Field("b", false, 2, 0)
			ff, _ := memory.Decimal128Field("first", true, 1, 0)
			mf, _ := memory.Decimal128Field("match", false, 1, 0)
			schema := memory.NewSchema(af, bf, ff, mf, memory.Field{Name: "keep", Type: memory.TypeBool})
			batch := memory.NewBatch()
			batch.Rows = 3
			av := []memory.Decimal128{decimal128.FromI64(95), decimal128.FromI64(5), decimal128.FromI64(95)}
			bv := []memory.Decimal128{decimal128.FromI64(10), decimal128.FromI64(2), decimal128.FromI64(10)}
			if kind == "zero" {
				bv[0] = memory.Decimal128{}
				bv[2] = memory.Decimal128{}
			}
			batch.Decimals["a"] = &memory.Decimal128Vector{Values: av, Precision: 3, Scale: 0}
			batch.Decimals["b"] = &memory.Decimal128Vector{Values: bv, Precision: 2, Scale: 0}
			batch.Decimals["first"] = &memory.Decimal128Vector{Values: []memory.Decimal128{{}, {}, {}}, Validity: memory.Bitmap{5}, Precision: 1, Scale: 0}
			batch.Decimals["match"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(2), decimal128.FromI64(9), decimal128.FromI64(2)}, Precision: 1, Scale: 0}
			batch.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0}}
			div := DivideRoundDecimal128(Col("a"), Col("b"), 1, 0, DecimalRoundHalfEven)
			zero := Decimal128(memory.Decimal128{}, 1, 0)
			two := Decimal128(decimal128.FromI64(2), 1, 0)
			for name, e := range map[string]Expr{"if": If(Col("keep"), div, zero), "case": Case([]WhenThen{{When: Col("keep"), Then: div}}, zero), "coalesce": Coalesce(Col("first"), div), "in": In(two, Col("match"), div)} {
				p, err := Compile(schema, e)
				if err != nil {
					t.Fatal(err)
				}
				m := memory.NewBudgetManager(0)
				w, _ := NewWorkspace(context.Background(), p, m)
				out, err := w.Evaluate(context.Background(), batch, testKernelSet())
				if err != nil {
					t.Fatal(name, err)
				}
				if name == "in" {
					if !reflect.DeepEqual(out[0].Uint32.Values, []uint32{1, 1, 1}) {
						t.Fatal("IN oracle")
					}
				} else {
					for row, want := range []int64{0, 2, 0} {
						if !out[0].Decimal.IsValid(row) || out[0].Decimal.Values[row] != decimal128.FromI64(want) {
							t.Fatal("guard oracle")
						}
					}
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("guard leak")
				}
			}
			p, err := Compile(schema, div)
			if err != nil {
				t.Fatal(err)
			}
			w, _ := NewWorkspace(context.Background(), p, nil)
			if _, err = w.EvaluateSelected(context.Background(), batch, []int{1}, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			w.Close()
			w, _ = NewWorkspace(context.Background(), p, nil)
			_, err = w.EvaluateSelected(context.Background(), batch, []int{1, 2}, testKernelSet())
			w.Close()
			reason := "overflows at row 1"
			if kind == "zero" {
				reason = "by zero at row 1"
			}
			if err == nil || !strings.Contains(err.Error(), reason) {
				t.Fatalf("compact selected error expected %s got%v", reason, err)
			}
		})
	}
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	for _, e := range []Expr{DivideRoundDecimal128(NullDecimal128(1, 0), zero, 1, 0, DecimalRoundHalfEven), DivideRoundDecimal128(zero, NullDecimal128(1, 0), 1, 0, DecimalRoundHalfEven)} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		b := memory.NewBatch()
		b.Rows = 1
		w, _ := NewWorkspace(context.Background(), p, nil)
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || out[0].Decimal.IsValid(0) || out[0].Decimal.Values[0] != (memory.Decimal128{}) {
			t.Fatal("NULL dominates zero divisor", err)
		}
		w.Close()
	}
	// Existing exact division still rejects1/3, whereas explicit rounding gives0.
	a, b := Decimal128(decimal128.FromI64(1), 1, 0), Decimal128(decimal128.FromI64(3), 1, 0)
	p, err := Compile(memory.NewSchema(), DivideDecimal128(a, b, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 1
	w, _ := NewWorkspace(context.Background(), p, nil)
	_, err = w.Evaluate(context.Background(), batch, testKernelSet())
	w.Close()
	if err == nil || !strings.Contains(err.Error(), "loses fractional digits") {
		t.Fatal("old exact contract changed", err)
	}
	p, err = Compile(memory.NewSchema(), DivideRoundDecimal128(a, b, 1, 0, DecimalRoundHalfEven))
	if err != nil {
		t.Fatal(err)
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	out, err := w.Evaluate(context.Background(), batch, testKernelSet())
	w.Close()
	if err != nil || out[0].Decimal.Values[0] != (memory.Decimal128{}) {
		t.Fatal("explicit round1/3", err)
	}
}

func TestDecimalDivideRoundAdmissions(t *testing.T) {
	p := divideRoundProgram(t, false)
	_, b := divideRoundFixture(130)
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
				t.Fatalf("admission%d err%v", fail, err)
			}
			m.fail = 0
			out, err := run(w)
			if err != nil {
				t.Fatal(err)
			}
			var rows []int
			if selected {
				rows = []int{0, 1, 63, 64, 129}
			}
			divideRoundCheck(t, b, rows, out)
			w.Close()
			if m.Used() != 0 {
				t.Fatal("retry leak")
			}
		}
	}
}

type divideRoundCancelContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *divideRoundCancelContext) Err() error {
	var pcs [24]uintptr
	n := runtime.Callers(2, pcs[:])
	fs := runtime.CallersFrames(pcs[:n])
	for {
		f, more := fs.Next()
		if strings.HasSuffix(f.Function, ".evalDecimalDivideRound") {
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
func TestDecimalDivideRoundExecutingCancellation(t *testing.T) {
	for _, stop := range []int{2, 3} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			const n = 193
			af, _ := memory.Decimal128Field("a", false, 1, 0)
			bf, _ := memory.Decimal128Field("b", false, 1, 0)
			of, _ := memory.Decimal128Field("out", false, 2, 0)
			a := scalarRegister{decimal: make([]memory.Decimal128, n), decimalType: &af}
			b := scalarRegister{decimal: make([]memory.Decimal128, n), decimalType: &bf}
			out := scalarRegister{decimal: make([]memory.Decimal128, n), decimalType: &of, ownedValidity: make(memory.Bitmap, memory.WordsFor(n))}
			sentinel := decimal128.FromI64(-777)
			for i := 0; i < n; i++ {
				a.decimal[i] = decimal128.FromI64(5)
				b.decimal[i] = decimal128.FromI64(2)
				out.decimal[i] = sentinel
			}
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := 0
			out.checkContext = func() error {
				checks++
				if checks == stop {
					cancel()
				}
				return base.Err()
			}
			err := evalDecimalDivideRound(&out, &a, &b, DecimalRoundHalfEven, n)
			if !errors.Is(err, context.Canceled) || checks != stop {
				t.Fatal(err, checks)
			}
			for i, v := range out.decimal {
				want := sentinel
				if i < (stop-1)*64 {
					want = decimal128.FromI64(2)
				}
				if v != want {
					t.Fatal("processed/sentinel row", i)
				}
			}
			batch := memory.NewBatch()
			batch.Rows = n
			batch.Decimals["a"] = &memory.Decimal128Vector{Values: a.decimal, Precision: 1, Scale: 0}
			batch.Decimals["b"] = &memory.Decimal128Vector{Values: b.decimal, Precision: 1, Scale: 0}
			p, err := Compile(memory.NewSchema(af, bf), DivideRoundDecimal128(Col("a"), Col("b"), 2, 0, DecimalRoundHalfEven))
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			ctx, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			running := &divideRoundCancelContext{Context: ctx, cancel: cancel2, stop: stop}
			if _, err = w.Evaluate(running, batch, testKernelSet()); !errors.Is(err, context.Canceled) || running.checks != stop {
				t.Fatal("workspace execution cancellation", err, running.checks)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("cancel budget leak")
			}
		})
	}
}

var divideRoundAllocationValue memory.Decimal128
var divideRoundAllocationStatus decimalDivideStatus

func TestDecimalDivideRoundHelperAllocations(t *testing.T) {
	max := decimal128.FromBigInt(new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1)))
	for _, mode := range divideRoundTestModes {
		t.Run(mode.String(), func(t *testing.T) {
			a := testing.AllocsPerRun(1000, func() {
				divideRoundAllocationValue, divideRoundAllocationStatus = decimalDivideRoundValue(max, 38, max, 38, 38, 37, mode)
				divideRoundAllocationValue, divideRoundAllocationStatus = decimalDivideRoundValue(max, 0, decimal128.FromI64(1), 38, 38, 38, mode)
				divideRoundAllocationValue, divideRoundAllocationStatus = decimalDivideRoundValue(decimal128.FromI64(-5), 0, decimal128.FromI64(2), 0, 1, 0, mode)
				divideRoundAllocationValue, divideRoundAllocationStatus = decimalDivideRoundValue(decimal128.FromI64(1), 38, max, 0, 38, 0, mode)
				divideRoundAllocationValue, divideRoundAllocationStatus = decimalDivideRoundValue(max, 0, memory.Decimal128{}, 0, 38, 0, mode)
			})
			if a != 0 {
				t.Fatal("helper allocations/run", a)
			}
			t.Logf("helper only AllocsPerRun1000=%g", a)
		})
	}
}

func TestDecimalDivideRoundPreCancelled(t *testing.T) {
	p := divideRoundProgram(t, false)
	_, b := divideRoundFixture(65)
	for _, selected := range []bool{false, true} {
		m := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, m)
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var err error
		if selected {
			_, err = w.EvaluateSelected(ctx, b, []int{1, 2}, testKernelSet())
		} else {
			_, err = w.Evaluate(ctx, b, testKernelSet())
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("pre-cancel", err)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("pre-cancel leak")
		}
	}
}

func TestDecimalDivideRoundLowLimbCarry(t *testing.T) {
	// q=2^64-1,r=1 before rounding. Increment must carry into the high limb.
	a := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 65), big.NewInt(1))
	low := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))
	high := new(big.Int).Lsh(big.NewInt(1), 64)
	for _, sa := range []int64{-1, 1} {
		for _, sb := range []int64{-1, 1} {
			for _, mode := range divideRoundTestModes {
				negative := sa != sb
				increment := mode == DecimalRoundAwayFromZero || mode == DecimalRoundHalfEven || mode == DecimalRoundHalfAwayFromZero || (mode == DecimalRoundFloor && negative) || (mode == DecimalRoundCeil && !negative)
				want := new(big.Int).Set(low)
				if increment {
					want.Set(high)
				}
				if negative {
					want.Neg(want)
				}
				got, status := decimalDivideRoundValue(decimal128.FromBigInt(new(big.Int).Mul(a, big.NewInt(sa))), 0, decimal128.FromI64(2*sb), 0, 38, 0, mode)
				if status != decimalDivideOK || got.BigInt().Cmp(want) != 0 {
					t.Fatalf("low-limb carry signs%d/%d mode%s got%s status%d want%s", sa, sb, mode, got.BigInt(), status, want)
				}
			}
		}
	}
}
