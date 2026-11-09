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
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func decimalArithmeticOracle(a memory.Decimal128, as int32, b memory.Decimal128, bs int32, op Op) *big.Int {
	x, y := a.BigInt(), b.BigInt()
	if op == OpMul {
		return x.Mul(x, y)
	}
	if as < bs {
		x.Mul(x, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(bs-as)), nil))
	} else if bs < as {
		y.Mul(y, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(as-bs)), nil))
	}
	if op == OpSub {
		return x.Sub(x, y)
	}
	return x.Add(x, y)
}

func TestDecimalArithmeticBigIntOracle(t *testing.T) {
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	values := []memory.Decimal128{{}, decimal128.FromI64(1), decimal128.FromI64(-1), decimal128.FromI64(1<<53 + 1), decimal128.New(1, 0), decimal128.New(0, ^uint64(0)), max, max.Negate()}
	check := func(a memory.Decimal128, as int32, b memory.Decimal128, bs int32, op Op, p int32) {
		t.Helper()
		want := decimalArithmeticOracle(a, as, b, bs, op)
		limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(p)), nil)
		wantOK := new(big.Int).Abs(new(big.Int).Set(want)).Cmp(limit) < 0
		got, ok := decimalArithmeticValue(a, as, b, bs, op, p)
		if ok != wantOK || ok && got.BigInt().Cmp(want) != 0 {
			t.Fatalf("a=%s/%d b=%s/%d op=%s p=%d got=%s/%v want=%s/%v", a.BigInt(), as, b.BigInt(), bs, op, p, got.BigInt(), ok, want, wantOK)
		}
	}
	for as := int32(0); as <= 38; as++ {
		for bs := int32(0); bs <= 38; bs++ {
			for _, a := range values {
				for _, b := range values {
					for _, op := range []Op{OpAdd, OpSub, OpMul} {
						if op != OpMul || as+bs <= 38 {
							check(a, as, b, bs, op, 38)
						}
					}
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(9017))
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	for i := 0; i < 10000; i++ {
		a, b := new(big.Int).Rand(rng, limit), new(big.Int).Rand(rng, limit)
		if rng.Intn(2) == 0 {
			a.Neg(a)
		}
		if rng.Intn(2) == 0 {
			b.Neg(b)
		}
		as, bs, p := int32(rng.Intn(39)), int32(rng.Intn(39)), int32(1+rng.Intn(38))
		for _, op := range []Op{OpAdd, OpSub, OpMul} {
			if op != OpMul || as+bs <= 38 {
				check(decimal128.FromBigInt(a), as, decimal128.FromBigInt(b), bs, op, p)
			}
		}
	}
	// The aligned value 10^38 does not fit decimal(38), but cancellation does.
	for _, sign := range []int64{-1, 1} {
		a := decimal128.GetScaleMultiplier(37)
		if sign < 0 {
			a = a.Negate()
		}
		b := max
		if sign < 0 {
			b = b.Negate()
		}
		got, ok := decimalArithmeticValue(a, 0, b, 1, OpSub, 38)
		if !ok || got != decimal128.FromI64(sign) {
			t.Fatal("aligned cancellation rejected", got, ok)
		}
	}
	// Cancellation also works after alignment exceeds the signed 128-bit range.
	wide := decimal128.GetScaleMultiplier(36).Mul(decimal128.FromI64(18))
	check(wide, 0, max, 1, OpSub, 38)
	check(wide.Negate(), 0, max, 1, OpAdd, 38)
	if allocs := testing.AllocsPerRun(1000, func() {
		decimalArithmeticValue(max, 0, max, 38, OpAdd, 38)
		decimalArithmeticValue(max, 0, max, 0, OpSub, 38)
		decimalArithmeticValue(max, 0, max, 0, OpMul, 38)
	}); allocs != 0 {
		t.Fatal("coefficient arithmetic allocated", allocs)
	}
}

func TestDecimalArithmeticMetadata(t *testing.T) {
	for _, tc := range []struct {
		ap, as, bp, bs int32
		op             Op
		p, s           int32
	}{
		{5, 2, 7, 4, OpAdd, 8, 4}, {5, 2, 7, 4, OpSub, 8, 4},
		{5, 2, 7, 4, OpMul, 12, 6}, {38, 0, 38, 38, OpAdd, 38, 38},
		{38, 38, 38, 38, OpSub, 38, 38}, {38, 19, 38, 19, OpMul, 38, 38},
	} {
		for _, nullable := range []bool{false, true} {
			a, _ := memory.Decimal128Field("a", nullable, tc.ap, tc.as)
			b, _ := memory.Decimal128Field("b", false, tc.bp, tc.bs)
			schema := memory.NewSchema(a, b)
			e := Binary{Op: tc.op, Left: Col("a"), Right: Col("b")}
			bound, err := Bind(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			f := bound.Field
			if f.DecimalPrecision != tc.p || f.DecimalScale != tc.s || f.Nullable != nullable || !bound.MayError {
				t.Fatalf("metadata: %+v, MayError=%v", f, bound.MayError)
			}
			p, err := Compile(schema, bound.Expr)
			if err != nil || !p.SupportsSelection() || !reflect.DeepEqual(p.ResultField(), bound.OutputField("")) {
				t.Fatal("rebind or selection lost metadata", err)
			}
		}
	}
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	for _, e := range []Expr{Add(one, I64(1)), Add(F64(1), one), Mul(one, Str("1")), Sub(Col("opaque"), one), Div(one, one), Mul(Decimal128(memory.Decimal128{}, 38, 20), Decimal128(memory.Decimal128{}, 38, 19)), If(Bool(false), Mul(NullDecimal128(38, 20), NullDecimal128(38, 19)), NullDecimal128(38, 38))} {
		if _, err := Compile(memory.NewSchema(memory.Field{Name: "opaque", Type: memory.TypeDecimal}), e); err == nil {
			t.Fatalf("invalid decimal arithmetic accepted: %s", e)
		}
	}
}

func decimalArithmeticFixture(t *testing.T, rows int) (Program, *memory.Batch) {
	t.Helper()
	a, _ := memory.Decimal128Field("a", true, 20, 8)
	b, _ := memory.Decimal128Field("b", true, 20, 4)
	batch := memory.NewBatch()
	batch.Rows = rows
	for j, f := range []memory.Field{a, b} {
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Precision: f.DecimalPrecision, Scale: f.DecimalScale}
		for row := range v.Values {
			v.Values[row] = decimal128.FromI64(int64(row+j)*712931 + 1<<53 + 1)
			if row%3 == 0 {
				v.Values[row] = v.Values[row].Negate()
			}
			if (row+j)%7 != 0 {
				memory.Set(v.Validity, row)
			} else {
				v.Values[row] = decimal128.New(math.MinInt64, 0)
			}
		}
		batch.Decimals[f.Name] = v
	}
	add := Add(Col("a"), Col("b"))
	p, err := CompileExpressions(memory.NewSchema(a, b), []NamedExpr{Named(add, "add"), Named(Sub(Col("a"), Col("b")), "sub"), Named(Mul(Col("a"), Col("b")), "mul"), Named(add, "duplicate"), Named(EQ(add, Add(Col("b"), Col("a"))), "equal")})
	if err != nil {
		t.Fatal(err)
	}
	return p, batch
}

func decimalArithmeticCheck(t *testing.T, batch *memory.Batch, rows []int, out []ScalarValue) {
	t.Helper()
	n := batch.Rows
	if rows != nil {
		n = len(rows)
	}
	a, b := batch.Decimals["a"], batch.Decimals["b"]
	for i := 0; i < n; i++ {
		row := i
		if rows != nil {
			row = rows[i]
		}
		valid := a.IsValid(row) && b.IsValid(row)
		for index, op := range []Op{OpAdd, OpSub, OpMul, OpAdd} {
			v := out[index].Decimal
			p, s := int32(25), int32(8)
			if op == OpMul {
				p, s = 38, 12
			}
			if v.Precision != p || v.Scale != s || v.IsValid(i) != valid {
				t.Fatal("result metadata/validity mismatch", row, index)
			}
			if valid && v.Values[i].BigInt().Cmp(decimalArithmeticOracle(a.Values[row], a.Scale, b.Values[row], b.Scale, op)) != 0 {
				t.Fatal("result coefficient mismatch", row, index)
			}
		}
		if out[4].Uint32.IsValid(i) != valid || valid && out[4].Uint32.Values[i] != 1 {
			t.Fatal("computed comparison mismatch")
		}
	}
	if n > 0 && &out[0].Decimal.Values[0] != &out[3].Decimal.Values[0] {
		t.Fatal("identical arithmetic lost CSE")
	}
}

func TestDecimalArithmeticDenseSelectedLifecycle(t *testing.T) {
	p, _ := decimalArithmeticFixture(t, 0)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		manager := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, manager)
		for _, n := range []int{2049, 1, 65, 0, 64, 63, 130} {
			_, batch := decimalArithmeticFixture(t, n)
			before, err := memory.CloneBatch(batch)
			if err != nil {
				t.Fatal(err)
			}
			out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			decimalArithmeticCheck(t, batch, nil, out)
			for _, step := range []int{1, 2, 65} {
				rows := make([]int, 0)
				for row := 0; row < n; row += step {
					rows = append(rows, row)
				}
				out, err = w.EvaluateSelected(context.Background(), batch, rows, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				decimalArithmeticCheck(t, batch, rows, out)
				if manager.Used() != w.MemoryBytes() {
					t.Fatal("accounting drift")
				}
			}
			if n > 0 && !reflect.DeepEqual(before, batch) {
				t.Fatal("arithmetic mutated input")
			}
		}
		w.Close()
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("Close leaked")
		}
	}
}

func TestDecimalArithmeticEveryAdmissionRetry(t *testing.T) {
	p, batch := decimalArithmeticFixture(t, 67)
	for _, selected := range []bool{false, true} {
		var rows []int
		if selected {
			rows = []int{0, 1, 3, 5, 64, 66}
		}
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
				t.Fatalf("selected=%v fail=%d err=%v", selected, fail, err)
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatal("failed admission accounting drift")
			}
			manager.fail = 0
			out, err := run(w)
			if err != nil {
				t.Fatal(err)
			}
			decimalArithmeticCheck(t, batch, rows, out)
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("retry close leaked")
			}
		}
	}
}

func TestDecimalArithmeticGuardsOverflowAndErrorOrder(t *testing.T) {
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	zero := Decimal128(memory.Decimal128{}, 38, 0)
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	bad := Add(Decimal128(max, 38, 0), one)
	for name, e := range map[string]Expr{
		"if":       If(Bool(false), bad, zero),
		"case":     Case([]WhenThen{{When: Bool(false), Then: bad}}, zero),
		"coalesce": Coalesce(zero, bad),
		"in":       In(zero, zero, bad),
		"null":     Add(NullDecimal128(38, 0), Decimal128(max, 38, 0)),
	} {
		t.Run(name, func(t *testing.T) {
			p, err := Compile(memory.NewSchema(), e)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, manager)
			b := memory.NewBatch()
			b.Rows = 67
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal("guarded overflow", err)
			}
			if _, err := w.EvaluateSelected(context.Background(), b, []int{0, 2, 64, 66}, testKernelSet()); err != nil {
				t.Fatal("selected guarded overflow", err)
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("guard leaked")
			}
		})
	}
	// A NULL parent cannot erase an error from an eagerly evaluated child.
	for name, e := range map[string]Expr{"live": bad, "eager_and": And(Bool(false), EQ(bad, zero)), "null_parent": Add(NullDecimal128(38, 0), bad)} {
		t.Run(name, func(t *testing.T) {
			p, err := Compile(memory.NewSchema(), e)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, manager)
			b := memory.NewBatch()
			b.Rows = 0
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal("empty batch raised value error", err)
			}
			b.Rows = 1
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "decimal128(38,0) overflows at row 0") {
				t.Fatal("missing overflow", err)
			}
			if manager.Used() != 0 {
				t.Fatal("error retained memory")
			}
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
				t.Fatal("semantic failure did not close workspace")
			}
		})
	}
	f, _ := memory.Decimal128Field("a", true, 38, 0)
	schema := memory.NewSchema(f, memory.Field{Name: "condition", Type: memory.TypeBool})
	b := memory.NewBatch()
	b.Rows = 67
	b.Decimals["a"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows), Precision: 38}
	b.Uint32s["condition"] = &memory.Uint32Vector{Values: make([]uint32, b.Rows)}
	for row := 0; row < b.Rows; row++ {
		memory.Set(b.Decimals["a"].Validity, row)
		b.Decimals["a"].Values[row] = max
		if row%2 == 0 {
			b.Uint32s["condition"].Values[row] = 1
			b.Decimals["a"].Values[row] = decimal128.FromI64(int64(row))
		}
	}
	b.Decimals["a"].Values[65] = decimal128.New(math.MinInt64, 0)
	b.Decimals["a"].Validity[1] &^= 2
	guard := If(Col("condition"), Add(Col("a"), one), zero)
	p, err := Compile(schema, guard)
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []bool{false, true} {
		w, _ := NewWorkspace(context.Background(), p, nil)
		if selected {
			_, err = w.EvaluateSelected(context.Background(), b, []int{0, 1, 2, 63, 64, 65, 66}, testKernelSet())
		} else {
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
		}
		if err != nil {
			t.Fatal("row guard failed", err)
		}
		w.Close()
	}
	// Lowest demanded row is reported, including selected-domain row numbers.
	p, _ = Compile(schema, Add(Col("a"), one))
	for _, selected := range []bool{false, true} {
		w, _ := NewWorkspace(context.Background(), p, nil)
		if selected {
			_, err = w.EvaluateSelected(context.Background(), b, []int{0, 2, 63, 64, 65, 66}, testKernelSet())
		} else {
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
		}
		want := "row 1"
		if selected {
			want = "row 2"
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatal("first row changed", err)
		}
		w.Close()
	}
	// The left child error at a later row precedes the right child's row zero.
	left := If(Col("condition"), zero, bad)
	right := Mul(Decimal128(max, 38, 0), Decimal128(decimal128.FromI64(2), 1, 0))
	p, _ = Compile(schema, Add(left, right))
	w, _ := NewWorkspace(context.Background(), p, nil)
	_, err = w.Evaluate(context.Background(), b, testKernelSet())
	w.Close()
	if err == nil || !strings.Contains(err.Error(), "checked +") || !strings.Contains(err.Error(), "row 1") {
		t.Fatal("operand error ordering changed", err)
	}
}

func TestDecimalArithmeticConcurrentWorkspaces(t *testing.T) {
	p, b := decimalArithmeticFixture(t, 65)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, _ := NewWorkspace(context.Background(), p, nil)
			defer w.Close()
			for j := 0; j < 5; j++ {
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Error(err)
					return
				}
				decimalArithmeticCheck(t, b, nil, out)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkDecimalArithmeticCoefficient(b *testing.B) {
	for _, tc := range []struct {
		name string
		a    memory.Decimal128
		as   int32
		c    memory.Decimal128
		cs   int32
		op   Op
	}{
		{"same_scale_add", decimal128.New(1, 17), 8, decimal128.New(2, 19), 8, OpAdd},
		{"cross_scale_sub", decimal128.New(1, 17), 8, decimal128.New(2, 19), 4, OpSub},
		{"multiply", decimal128.FromI64(1<<53 + 1), 8, decimal128.FromI64(131), 4, OpMul},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var value memory.Decimal128
			var ok bool
			for b.Loop() {
				value, ok = decimalArithmeticValue(tc.a, tc.as, tc.c, tc.cs, tc.op, 38)
			}
			if !ok {
				b.Fatal(fmt.Sprint(value))
			}
		})
	}
}
