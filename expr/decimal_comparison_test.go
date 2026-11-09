package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func decimalComparisonOracle(a memory.Decimal128, as int32, b memory.Decimal128, bs int32) int {
	// Independent arbitrary-precision cross multiplication, not the production
	// magnitude alignment, limb arithmetic, or Decimal128 rescale helpers.
	left := new(big.Int).Mul(a.BigInt(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(bs)), nil))
	right := new(big.Int).Mul(b.BigInt(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(as)), nil))
	return left.Cmp(right)
}

func TestDecimalComparisonWideOracle(t *testing.T) {
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	values := []memory.Decimal128{{}, decimal128.FromI64(1), decimal128.FromI64(-1), decimal128.FromI64(10), decimal128.FromI64(-10), decimal128.New(0, ^uint64(0)), decimal128.New(1, 0), decimal128.New(1, 1), decimal128.FromI64(1<<53 + 1), max, max.Negate()}
	check := func(a memory.Decimal128, as int32, b memory.Decimal128, bs int32) {
		t.Helper()
		if got, want := compareDecimal128(a, as, b, bs), decimalComparisonOracle(a, as, b, bs); got != want {
			t.Fatalf("(%s scale=%d) cmp (%s scale=%d)=%d want=%d", a.BigInt(), as, b.BigInt(), bs, got, want)
		}
	}
	for as := int32(0); as <= 38; as++ {
		for bs := int32(0); bs <= 38; bs++ {
			for _, a := range values {
				for _, b := range values {
					check(a, as, b, bs)
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(673841))
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	for i := 0; i < 12000; i++ {
		a, b := new(big.Int).Rand(rng, limit), new(big.Int).Rand(rng, limit)
		if i&1 != 0 {
			a.Neg(a)
		}
		if i&2 != 0 {
			b.Neg(b)
		}
		check(decimal128.FromBigInt(a), int32(rng.Intn(39)), decimal128.FromBigInt(b), int32(rng.Intn(39)))
	}
	// Equal values whose representations differ, including negatives and every
	// legal scale gap. Random pairs alone almost never exercise equality.
	for gap := 0; gap <= 37; gap++ {
		a := decimal128.FromI64(1)
		b := decimal128.GetScaleMultiplier(gap)
		check(a, 0, b, int32(gap))
		check(a.Negate(), 0, b.Negate(), int32(gap))
	}
	if allocs := testing.AllocsPerRun(1000, func() { compareDecimal128(max, 0, max, 38) }); allocs != 0 {
		t.Fatalf("comparison allocates %g times per row", allocs)
	}
}

func decimalComparisonFixture(t *testing.T, rows int) (Program, *memory.Batch) {
	t.Helper()
	a, _ := memory.Decimal128Field("a", true, 38, 0)
	b, _ := memory.Decimal128Field("b", true, 38, 38)
	schema := memory.NewSchema(a, b)
	batch := memory.NewBatch()
	batch.Rows = rows
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	vals := []memory.Decimal128{max, max.Negate(), {}, decimal128.FromI64(1), decimal128.FromI64(-1), decimal128.New(1, 1), decimal128.New(-2, ^uint64(0))}
	for k, f := range []memory.Field{a, b} {
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Precision: f.DecimalPrecision, Scale: f.DecimalScale}
		for i := range rows {
			v.Values[i] = vals[(i+k)%len(vals)]
			if (i+k)%5 != 0 {
				memory.Set(v.Validity, i)
			}
		}
		batch.Decimals[f.Name] = v
	}
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	named := make([]NamedExpr, 0)
	for _, op := range []Op{OpEQ, OpLT, OpLE, OpGT, OpGE} {
		named = append(named, Named(Binary{Op: op, Left: Col("a"), Right: Col("b")}, op.String()))
	}
	candidates := []Expr{Col("b"), NullDecimal128(38, 17), zero}
	for i := 0; i < 17; i++ {
		candidates = append(candidates, Decimal128(decimal128.FromI64(int64(i+2)), 4, 2))
	}
	named = append(named, Named(In(Col("a"), candidates...), "in"), Named(Not(In(Col("a"), candidates...)), "not_in"), zero.Alias("zero"), NullDecimal128(38, 38).Alias("null"), Named(Col("a"), "alias"), Named(Col("a"), "duplicate"), Decimal128(max, 38, 0).Alias("max"))
	p, err := CompileExpressions(schema, named)
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("native decimals lost selected execution")
	}
	return p, batch
}

func decimalComparisonCheck(t *testing.T, b *memory.Batch, rows []int, values []ScalarValue) {
	t.Helper()
	if rows == nil {
		rows = make([]int, b.Rows)
		for i := range rows {
			rows[i] = i
		}
	}
	for i, row := range rows {
		a, bv := b.Decimals["a"], b.Decimals["b"]
		valid := a.IsValid(row) && bv.IsValid(row)
		cmp := decimalComparisonOracle(a.Values[row], a.Scale, bv.Values[row], bv.Scale)
		wants := []bool{cmp == 0, cmp < 0, cmp <= 0, cmp > 0, cmp >= 0}
		for out, want := range wants {
			got := values[out].Uint32
			if got.IsValid(i) != valid || valid && (got.Values[i] != 0) != want {
				t.Fatalf("out=%d row=%d compare mismatch", out, row)
			}
		}
		match := a.IsValid(row) && (a.Values[row] == (memory.Decimal128{}) || bv.IsValid(row) && cmp == 0)
		// Every IN has a NULL candidate. A miss is unknown, a match is true.
		if values[5].Uint32.IsValid(i) != match || match && values[5].Uint32.Values[i] != 1 || values[6].Uint32.IsValid(i) != match || match && values[6].Uint32.Values[i] != 0 {
			t.Fatalf("row=%d IN SQL NULL mismatch", row)
		}
		if values[7].Decimal.Values[i] != (memory.Decimal128{}) || !values[7].Decimal.IsValid(i) || values[8].Decimal.IsValid(i) {
			t.Fatal("typed literal/NULL mismatch")
		}
		for _, out := range []int{9, 10} {
			if values[out].Decimal.IsValid(i) != a.IsValid(row) || a.IsValid(row) && values[out].Decimal.Values[i] != a.Values[row] {
				t.Fatal("decimal alias changed")
			}
		}
		if values[11].Decimal.Values[i] != decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1)) {
			t.Fatal("38-digit literal truncated")
		}
	}
	if len(rows) > 0 && &values[9].Decimal.Values[0] != &values[10].Decimal.Values[0] {
		t.Fatal("duplicate alias lost sharing")
	}
}

func TestDecimalComparisonDenseSelectedLifecycle(t *testing.T) {
	p, _ := decimalComparisonFixture(t, 0)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		manager := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, manager)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{257, 1, 65, 0, 64, 63, 130} {
			_, b := decimalComparisonFixture(t, n)
			before, err := memory.CloneBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			values, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			decimalComparisonCheck(t, b, nil, values)
			if n > 0 && &values[9].Decimal.Values[0] != &b.Decimals["a"].Values[0] {
				t.Fatal("dense alias lost borrowing")
			}
			for _, step := range []int{1, 2, 65} {
				rows := make([]int, 0)
				for row := 0; row < n; row += step {
					rows = append(rows, row)
				}
				values, err = w.EvaluateSelected(context.Background(), b, rows, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				decimalComparisonCheck(t, b, rows, values)
				if len(rows) > 0 && &values[9].Decimal.Values[0] == &b.Decimals["a"].Values[0] {
					t.Fatal("selected alias borrowed ungathered input")
				}
				if manager.Used() != w.MemoryBytes() {
					t.Fatalf("used=%d capacity=%d", manager.Used(), w.MemoryBytes())
				}
			}
			if n > 0 && !reflect.DeepEqual(before, b) {
				t.Fatal("comparison mutated source")
			}
		}
		w.Close()
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("Close leaked")
		}
	}
}

func TestDecimalComparisonEveryAdmissionRetry(t *testing.T) {
	p, b := decimalComparisonFixture(t, 67)
	for _, selected := range []bool{false, true} {
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, []int{0, 1, 3, 5, 64, 66}, testKernelSet())
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
			var rows []int
			if selected {
				rows = []int{0, 1, 3, 5, 64, 66}
			}
			decimalComparisonCheck(t, b, rows, out)
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("retry close leaked")
			}
		}
	}
}

func TestDecimalComparisonTypedMetadataAndRejections(t *testing.T) {
	a, _ := memory.Decimal128Field("a", true, 38, 0)
	opaque := memory.Field{Name: "opaque", Type: memory.TypeDecimal, Nullable: true}
	schema := memory.NewSchema(a, opaque)
	one := Decimal128(decimal128.FromI64(1), 1, 0)
	for _, e := range []Expr{
		Decimal128(decimal128.FromI64(10), 1, 0), Decimal128(decimal128.FromI64(-10), 1, 0), Decimal128(decimal128.New(math.MinInt64, 0), 38, 0),
		Decimal128(memory.Decimal128{}, 39, 0), Decimal128(memory.Decimal128{}, 5, 6), Decimal128(memory.Decimal128{}, 5, -1),
		NullDecimal128(0, 0), NullDecimal128(38, 39), Null(memory.TypeDecimal),
		EQ(Col("a"), Col("opaque")), EQ(Col("a"), I64(1)), EQ(F64(1), one), EQ(one, Str("1")), In(one, F64(1)),
		If(Bool(false), Decimal128(decimal128.FromI64(10), 1, 0), one),
		Add(one, I64(1)), Div(one, one), CastTo(one, memory.TypeFloat64), CastTo(one, memory.TypeDecimal),
	} {
		if _, err := Compile(schema, e); err == nil {
			t.Fatalf("unsupported/invalid accepted: %s", e)
		}
	}
	literals := []Expr{one, Decimal128(decimal128.FromI64(1), 2, 0), Decimal128(decimal128.FromI64(1), 2, 1), NullDecimal128(1, 0), NullDecimal128(2, 1)}
	named := make([]NamedExpr, len(literals))
	for i, e := range literals {
		named[i] = Named(e, fmt.Sprint(i))
	}
	p, err := CompileExpressions(schema, named)
	if err != nil {
		t.Fatal(err)
	}
	for i := range literals {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("CSE merged distinct precision/scale or NULL")
			}
		}
	}
	e := Coalesce(Case([]WhenThen{{When: Bool(false), Then: one}}, NullDecimal128(1, 0)), one)
	bound, err := Bind(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := Compile(schema, bound.Expr)
	if err != nil {
		t.Fatal(err)
	}
	if f := rebound.ResultField(); f.DecimalPrecision != 1 || f.DecimalScale != 0 {
		t.Fatal("typed literal metadata lost in rebind")
	}
	if one.String() != "DECIMAL128(1,1,0)" || NullDecimal128(2, 1).String() != "NULL_DECIMAL128(2,1)" {
		t.Fatal("ambiguous decimal explain spelling")
	}
}

func TestDecimalComparisonGuardsAndErrorOrder(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 0)
	schema := memory.NewSchema(d, memory.Field{Name: "f", Type: memory.TypeFloat64})
	b := memory.NewBatch()
	b.Rows = 3
	b.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(1), decimal128.FromI64(2), {}}, Validity: memory.Bitmap{3}, Precision: 38, Scale: 0}
	b.Float64s["f"] = &memory.Float64Vector{Values: []float64{math.NaN(), 0, math.NaN()}}
	one, two := Decimal128(decimal128.FromI64(1), 1, 0), Decimal128(decimal128.FromI64(2), 1, 0)
	dangerous := If(EQ(CastTo(Col("f"), memory.TypeInt64), I64(0)), two, one)
	guarded := In(Col("d"), one, NullDecimal128(1, 0), dangerous)
	p, err := Compile(schema, guarded)
	if err != nil {
		t.Fatal(err)
	}
	if bound, _ := Bind(schema, guarded); !bound.MayError {
		t.Fatal("decimal comparison lost child error effect")
	}
	for _, selected := range []bool{false, true} {
		manager := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, manager)
		run := func() ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), b, []int{0, 1, 2}, testKernelSet())
			}
			return w.Evaluate(context.Background(), b, testKernelSet())
		}
		out, err := run()
		if err != nil {
			t.Fatal("matched/NULL rows did not guard cast", err)
		}
		for row := 0; row < 3; row++ {
			if out[0].Uint32.IsValid(row) != (row < 2) || row < 2 && out[0].Uint32.Values[row] != 1 {
				t.Fatal("guarded IN result")
			}
		}
		b.Float64s["f"].Values[1] = math.Inf(1)
		if _, err = run(); err == nil || !strings.Contains(err.Error(), "row 1") {
			t.Fatalf("expected live candidate row 1 failure, got %v", err)
		}
		if manager.Used() != 0 {
			t.Fatal("semantic failure retained memory")
		}
		b.Float64s["f"].Values[1] = 0
		// Semantic errors permanently close a Workspace; retryable budget
		// failures are covered separately above.
		if _, err = run(); err == nil {
			t.Fatal("semantic failure did not close workspace")
		}
		w, err = NewWorkspace(context.Background(), p, manager)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = run(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation", err)
		}
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("guarded workspace leaked")
		}
	}
	// Ordinary AND retains its existing eager error contract.
	eager := And(Bool(false), EQ(dangerous, one))
	ep, err := Compile(schema, eager)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), ep, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("eager comparison error suppressed")
	}
}

func TestDecimalComparisonConcurrentWorkspaces(t *testing.T) {
	p, b := decimalComparisonFixture(t, 65)
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Error(err)
				return
			}
			defer w.Close()
			for range 10 {
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Error(err)
					return
				}
				decimalComparisonCheck(t, b, nil, out)
			}
		})
	}
	wg.Wait()
}
