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

var decimalRoundModes = []DecimalRoundingMode{DecimalRoundTowardZero, DecimalRoundAwayFromZero, DecimalRoundFloor, DecimalRoundCeil, DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero}

// Independent absolute big.Int quotient/remainder oracle. No production power,
// rescale, division, rounding, or precision helper participates.
func decimalRoundOracle(c *big.Int, from, p, s int32, mode DecimalRoundingMode) (*big.Int, bool) {
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	x := new(big.Int).Abs(new(big.Int).Set(c))
	if s >= from {
		x.Mul(x, pow(s-from))
	} else {
		divisor := pow(from - s)
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(x, divisor, r)
		inc := false
		if r.Sign() != 0 {
			switch mode {
			case DecimalRoundAwayFromZero:
				inc = true
			case DecimalRoundFloor:
				inc = c.Sign() < 0
			case DecimalRoundCeil:
				inc = c.Sign() > 0
			case DecimalRoundHalfEven, DecimalRoundHalfAwayFromZero:
				cmp := new(big.Int).Lsh(r, 1).Cmp(divisor)
				inc = cmp > 0 || (cmp == 0 && (mode == DecimalRoundHalfAwayFromZero || q.Bit(0) != 0))
			}
		}
		if inc {
			q.Add(q, big.NewInt(1))
		}
		x = q
	}
	if c.Sign() < 0 {
		x.Neg(x)
	}
	return x, new(big.Int).Abs(new(big.Int).Set(x)).Cmp(pow(p)) < 0
}

func TestDecimalRoundOracleTieTable(t *testing.T) {
	// Columns use the explicit mode order above; expected literals validate the
	// oracle itself, especially negative ties and both even/odd retained digits.
	for _, tc := range []struct {
		c    int64
		want [6]int64
	}{
		{25, [6]int64{2, 3, 2, 3, 2, 3}}, {-25, [6]int64{-2, -3, -3, -2, -2, -3}},
		{35, [6]int64{3, 4, 3, 4, 4, 4}}, {-35, [6]int64{-3, -4, -4, -3, -4, -4}},
		{24, [6]int64{2, 3, 2, 3, 2, 2}}, {-24, [6]int64{-2, -3, -3, -2, -2, -2}},
		{26, [6]int64{2, 3, 2, 3, 3, 3}}, {-26, [6]int64{-2, -3, -3, -2, -3, -3}},
		{5, [6]int64{0, 1, 0, 1, 0, 1}}, {-5, [6]int64{0, -1, -1, 0, 0, -1}},
	} {
		for i, m := range decimalRoundModes {
			got, ok := decimalRoundOracle(big.NewInt(tc.c), 1, 2, 0, m)
			if !ok || got.Cmp(big.NewInt(tc.want[i])) != 0 {
				t.Fatalf("oracle %d mode%d got%v", tc.c, m, got)
			}
		}
	}
}

func decimalRoundFixture(t *testing.T, n int) (Program, *memory.Batch) {
	t.Helper()
	f, _ := memory.Decimal128Field("d", true, 38, 20)
	b := memory.NewBatch()
	b.Rows = n
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: make(memory.Bitmap, memory.WordsFor(n)), Precision: 38, Scale: 20}
	for i := 0; i < n; i++ {
		v.Values[i] = decimal128.FromI64(int64((i%101)-50)*100000000000000000 + int64(i%3-1))
		if i%7 != 0 {
			memory.Set(v.Validity, i)
		} else {
			v.Values[i] = decimal128.New(math.MinInt64, 0)
		}
	}
	b.Decimals["d"] = v
	es := make([]NamedExpr, 0, 7)
	for i, m := range decimalRoundModes {
		es = append(es, Named(RoundDecimal128(Col("d"), 20, 2, m), fmt.Sprintf("m%d", i)))
	}
	es = append(es, Named(RoundDecimal128(Col("d"), 20, 2, DecimalRoundHalfEven), "duplicate"))
	p, err := CompileExpressions(memory.NewSchema(f), es)
	if err != nil {
		t.Fatal(err)
	}
	return p, b
}
func decimalRoundCheck(t *testing.T, b *memory.Batch, rows []int, out []ScalarValue) {
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
		for j, v := range out {
			m := DecimalRoundHalfEven
			if j < 6 {
				m = decimalRoundModes[j]
			}
			d := v.Decimal
			if d.Precision != 20 || d.Scale != 2 || d.IsValid(i) != b.Decimals["d"].IsValid(row) {
				t.Fatal("metadata/null")
			}
			if d.IsValid(i) {
				want, ok := decimalRoundOracle(b.Decimals["d"].Values[row].BigInt(), 20, 20, 2, m)
				if !ok || d.Values[i].BigInt().Cmp(want) != 0 {
					t.Fatalf("row%d mode%d got%s want%s", row, m, d.Values[i].BigInt(), want)
				}
			} else if d.Values[i] != (memory.Decimal128{}) {
				t.Fatal("NULL payload not cleared")
			}
		}
	}
	if n > 0 && &out[4].Decimal.Values[0] != &out[6].Decimal.Values[0] {
		t.Fatal("identical mode CSE not shared")
	}
}
func TestDecimalRoundDenseSelectedLifecycle(t *testing.T) {
	p, _ := decimalRoundFixture(t, 0)
	if !p.SupportsSelection() {
		t.Fatal("expected existing Decimal selected path")
	}
	for _, km := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		manager := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, manager)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{0, 1, 63, 64, 65, 2049, 1, 130, 0} {
			_, b := decimalRoundFixture(t, n)
			before, err := memory.CloneBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			out, err := w.Evaluate(context.Background(), b, kernel.New(km))
			if err != nil {
				t.Fatal(err)
			}
			decimalRoundCheck(t, b, nil, out)
			for _, step := range []int{1, 2, 65} {
				rows := make([]int, 0)
				for i := 0; i < n; i += step {
					rows = append(rows, i)
				}
				out, err = w.EvaluateSelected(context.Background(), b, rows, kernel.New(km))
				if err != nil {
					t.Fatal(err)
				}
				decimalRoundCheck(t, b, rows, out)
			}
			if n > 0 && !reflect.DeepEqual(before, b) {
				t.Fatal("mutated input")
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatal("accounting drift")
			}
		}
		w.Close()
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("Close leaked")
		}
	}
}

func TestDecimalRoundBigIntOracle(t *testing.T) {
	pow := func(n int32) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }
	max := new(big.Int).Sub(pow(38), big.NewInt(1))
	check := func(c *big.Int, from, p, s int32, m DecimalRoundingMode) {
		t.Helper()
		want, ok := decimalRoundOracle(c, from, p, s, m)
		got, status := decimalRoundValue(decimal128.FromBigInt(c), from, p, s, m)
		if ok {
			if status != decimalRescaleOK || got.BigInt().Cmp(want) != 0 {
				t.Fatalf("c%s from%d p%d s%d m%d got%s/%d want%s", c, from, p, s, m, got.BigInt(), status, want)
			}
		} else if status != decimalRescaleOverflow {
			t.Fatalf("expected overflow c%s from%d p%d s%d m%d got%s/%d", c, from, p, s, m, got.BigInt(), status)
		}
	}
	vals := []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(-1), big.NewInt(25), big.NewInt(-25), max, new(big.Int).Neg(max), new(big.Int).Lsh(big.NewInt(1), 64)}
	for from := int32(0); from <= 38; from++ {
		for p := int32(1); p <= 38; p++ {
			for s := int32(0); s <= p; s++ {
				for _, m := range decimalRoundModes {
					for _, c := range vals {
						check(c, from, p, s, m)
					}
				}
			}
		}
	}
	// Every reduction gap: odd/even quotient, true ties and neighbors. In
	// particular gap20/37/38 retains the low 19-digit remainder information.
	for gap := int32(1); gap <= 38; gap++ {
		d := pow(gap)
		half := new(big.Int).Quo(new(big.Int).Set(d), big.NewInt(2))
		for _, q := range []int64{0, 2, 3, 9} {
			for _, delta := range []int64{-1, 0, 1} {
				c := new(big.Int).Mul(big.NewInt(q), d)
				c.Add(c, half)
				c.Add(c, big.NewInt(delta))
				if c.Cmp(max) > 0 {
					continue
				}
				for _, sign := range []int64{-1, 1} {
					x := new(big.Int).Mul(c, big.NewInt(sign))
					for _, m := range decimalRoundModes {
						check(x, gap, 1, 0, m)
						check(x, gap, 38, 0, m)
					}
				}
			}
		}
	}
	// Carry across each target precision boundary, including final-only overflow.
	for p := int32(1); p <= 37; p++ {
		c := new(big.Int).Sub(pow(p+1), big.NewInt(5))
		for _, sign := range []int64{-1, 1} {
			x := new(big.Int).Mul(c, big.NewInt(sign))
			for _, m := range decimalRoundModes {
				check(x, 1, p, 0, m)
			}
		}
	}
	rng := rand.New(rand.NewSource(382711))
	limit := pow(38)
	for i := 0; i < 10000; i++ {
		c := new(big.Int).Rand(rng, limit)
		if i%2 == 0 {
			c.Neg(c)
		}
		p := int32(rng.Intn(38) + 1)
		check(c, int32(rng.Intn(39)), p, int32(rng.Intn(int(p)+1)), decimalRoundModes[i%6])
	}
}

func TestDecimalRoundBindingCSE(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 20)
	schema := memory.NewSchema(f)
	es := []NamedExpr{}
	for i, m := range decimalRoundModes {
		e := RoundDecimal128(Col("d"), 20, 2, m)
		bound, err := Bind(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		if !bound.MayError || !bound.Field.Nullable || bound.Field.DecimalPrecision != 20 || bound.Field.DecimalScale != 2 || !reflect.DeepEqual(Columns(e), []string{"d"}) {
			t.Fatal("bound contract")
		}
		es = append(es, Named(e, fmt.Sprint(i)))
	}
	es = append(es, Named(RoundDecimal128(Col("d"), 20, 2, DecimalRoundHalfEven), "same"), Named(RoundDecimal128(Col("d"), 21, 2, DecimalRoundHalfEven), "precision"), Named(RoundDecimal128(Col("d"), 20, 3, DecimalRoundHalfEven), "scale"))
	p, err := CompileExpressions(schema, es)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		for j := i + 1; j < 6; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatal("mode lost in CSE")
			}
		}
	}
	if p.outputs[4].root != p.outputs[6].root || p.outputs[4].root == p.outputs[7].root || p.outputs[4].root == p.outputs[8].root {
		t.Fatal("target metadata CSE")
	}
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	for _, e := range []Expr{RoundDecimal128(zero, 0, 0, DecimalRoundHalfEven), RoundDecimal128(zero, 39, 0, DecimalRoundHalfEven), RoundDecimal128(zero, 1, -1, DecimalRoundHalfEven), RoundDecimal128(zero, 1, 2, DecimalRoundHalfEven), RoundDecimal128(I64(1), 1, 0, DecimalRoundHalfEven), RoundDecimal128(nil, 1, 0, DecimalRoundHalfEven), RoundDecimal128(zero, 1, 0, DecimalRoundingMode(0)), RoundDecimal128(zero, 2, 1, DecimalRoundingMode(255)), If(Bool(false), RoundDecimal128(zero, 1, 0, DecimalRoundingMode(0)), zero)} {
		if _, err := Compile(memory.NewSchema(), e); err == nil {
			t.Fatalf("invalid binding accepted %v", e)
		}
	}
}

func TestDecimalRoundGuardsAndSelectedErrors(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 3, 1)
	first, _ := memory.Decimal128Field("first", true, 1, 0)
	schema := memory.NewSchema(f, first, memory.Field{Name: "keep", Type: memory.TypeBool})
	bad := RoundDecimal128(Col("d"), 1, 0, DecimalRoundHalfAwayFromZero)
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	for name, e := range map[string]Expr{"if": If(Col("keep"), bad, zero), "case": Case([]WhenThen{{When: Col("keep"), Then: bad}}, zero), "coalesce": Coalesce(Col("first"), bad)} {
		t.Run(name, func(t *testing.T) {
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			for turn := 0; turn < 3; turn++ {
				b := memory.NewBatch()
				b.Rows = 2
				active := turn % 2
				hidden := 1 - active
				dv := &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(25), decimal128.FromI64(25)}, Precision: 3, Scale: 1}
				dv.Values[hidden] = decimal128.FromI64(95)
				b.Decimals["d"] = dv
				keep := []uint32{0, 0}
				keep[active] = 1
				b.Uint32s["keep"] = &memory.Uint32Vector{Values: keep}
				fv := &memory.Decimal128Vector{Values: []memory.Decimal128{{}, {}}, Validity: make(memory.Bitmap, 1), Precision: 1, Scale: 0}
				memory.Set(fv.Validity, hidden)
				b.Decimals["first"] = fv
				out, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < 2; row++ {
					want := int64(0)
					if row == active {
						want = 3
					}
					if !out[0].Decimal.IsValid(row) || out[0].Decimal.Values[row].BigInt().Cmp(big.NewInt(want)) != 0 {
						t.Fatal("dynamic guard oracle")
					}
				}
				// Unguarded demanded overflow, while selection of the other row succeeds.
				direct, err := Compile(schema, bad)
				if err != nil {
					t.Fatal(err)
				}
				cw, _ := NewWorkspace(context.Background(), direct, m)
				if _, err = cw.EvaluateSelected(context.Background(), b, []int{active}, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				cw.Close()
				cw, _ = NewWorkspace(context.Background(), direct, m)
				if _, err = cw.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "overflow") {
					t.Fatal("demanded rounding carry overflow", err)
				}
				cw.Close()
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("guard leaked")
			}
		})
	}
}

func TestDecimalRoundAdmissionAndCancel(t *testing.T) {
	p, b := decimalRoundFixture(t, 130)
	for _, selected := range []bool{false, true} {
		run := func(w *Workspace, ctx context.Context) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(ctx, b, []int{0, 1, 3, 64, 129}, testKernelSet())
			}
			return w.Evaluate(ctx, b, testKernelSet())
		}
		probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, _ := NewWorkspace(context.Background(), p, probe)
		if _, err := run(w, context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := probe.calls
		w.Close()
		for fail := 1; fail <= calls; fail++ {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, _ := NewWorkspace(context.Background(), p, m)
			if _, err := run(w, context.Background()); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("admission%d: %v", fail, err)
			}
			m.fail = 0
			if _, err := run(w, context.Background()); err != nil {
				t.Fatal(err)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("retry leaked")
			}
		}
		m := memory.NewBudgetManager(0)
		w, _ = NewWorkspace(context.Background(), p, m)
		if _, err := run(w, context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := run(w, ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("cancel leaked")
		}
	}
}

func TestDecimalRoundPublicScaleDirectionsAndReuse(t *testing.T) {
	for _, target := range [][2]int32{{38, 0}, {38, 2}, {38, 20}, {38, 38}} {
		for _, mode := range decimalRoundModes {
			t.Run(fmt.Sprintf("%d_%d/m%d", target[0], target[1], mode), func(t *testing.T) {
				f, _ := memory.Decimal128Field("d", true, 38, 2)
				p, err := Compile(memory.NewSchema(f), RoundDecimal128(Col("d"), target[0], target[1], mode))
				if err != nil {
					t.Fatal(err)
				}
				b := memory.NewBatch()
				b.Rows = 3
				v := &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(25), decimal128.FromI64(-25), decimal128.New(math.MinInt64, 0)}, Validity: memory.Bitmap{3}, Precision: 38, Scale: 2}
				b.Decimals["d"] = v
				w, err := NewWorkspace(context.Background(), p, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				for _, c := range []int64{25, 35, 24, 26, 0, -5} {
					// Mutate the identical vector backing array between evaluations.
					v.Values[0] = decimal128.FromI64(c)
					v.Values[1] = decimal128.FromI64(-c)
					out, err := w.Evaluate(context.Background(), b, testKernelSet())
					if err != nil {
						t.Fatal(err)
					}
					if out[0].Decimal.Precision != target[0] || out[0].Decimal.Scale != target[1] {
						t.Fatal("output metadata")
					}
					for row := 0; row < 2; row++ {
						want, ok := decimalRoundOracle(v.Values[row].BigInt(), 2, target[0], target[1], mode)
						if !ok || out[0].Decimal.Values[row].BigInt().Cmp(want) != 0 || !out[0].Decimal.IsValid(row) {
							t.Fatal("scale direction or stale value")
						}
					}
					if out[0].Decimal.IsValid(2) || out[0].Decimal.Values[2] != (memory.Decimal128{}) {
						t.Fatal("hidden NULL extreme")
					}
				}
			})
		}
	}
}

func TestDecimalRoundINAndSelectedErrorRows(t *testing.T) {
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	for _, mode := range []DecimalRoundingMode{0, 255} {
		if _, err := Compile(memory.NewSchema(), If(Bool(false), RoundDecimal128(zero, 1, 0, mode), zero)); err == nil {
			t.Fatalf("dead invalid mode %d accepted", mode)
		}
	}
	f, _ := memory.Decimal128Field("d", false, 3, 1)
	match, _ := memory.Decimal128Field("match", false, 1, 0)
	schema := memory.NewSchema(f, match)
	b := memory.NewBatch()
	b.Rows = 2
	b.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(95), decimal128.FromI64(25)}, Precision: 3, Scale: 1}
	b.Decimals["match"] = &memory.Decimal128Vector{Values: []memory.Decimal128{{}, decimal128.FromI64(1)}, Precision: 1, Scale: 0}
	round := RoundDecimal128(Col("d"), 1, 0, DecimalRoundHalfEven)
	p, err := Compile(schema, In(zero, Col("match"), round))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out[0].Uint32.Values, []uint32{1, 0}) {
		t.Fatal("IN did not stop after matching item")
	}
	w.Close()
	b.Decimals["match"].Values[0] = decimal128.FromI64(1)
	w, _ = NewWorkspace(context.Background(), p, nil)
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "overflows at row 0") {
		t.Fatal("IN demanded control", err)
	}
	w.Close()
	b.Rows = 67
	xs := make([]memory.Decimal128, 67)
	for i := range xs {
		xs[i] = decimal128.FromI64(25)
	}
	xs[1] = decimal128.FromI64(95)
	xs[63] = decimal128.FromI64(-95)
	b.Decimals["d"] = &memory.Decimal128Vector{Values: xs, Precision: 3, Scale: 1}
	p, err = Compile(memory.NewSchema(f), round)
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []bool{false, true} {
		w, _ = NewWorkspace(context.Background(), p, nil)
		want := "overflows at row 1"
		if selected {
			_, err = w.EvaluateSelected(context.Background(), b, []int{0, 2, 63, 64, 66}, testKernelSet())
			want = "overflows at row 2"
		} else {
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
		}
		w.Close()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("selected=%v want compact-domain %s got%v", selected, want, err)
		}
	}
}

var decimalRoundAllocSink memory.Decimal128
var decimalRoundAllocStatus decimalRescaleStatus

func TestDecimalRoundHelperAllocs(t *testing.T) {
	max := decimal128.FromBigInt(new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1)))
	for _, mode := range decimalRoundModes {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				for _, gap := range []int32{1, 19, 20, 37, 38} {
					decimalRoundAllocSink, decimalRoundAllocStatus = decimalRoundValue(max, gap, 38, 0, mode)
					decimalRoundAllocSink, decimalRoundAllocStatus = decimalRoundValue(max.Negate(), gap, 38, 0, mode)
				}
				decimalRoundAllocSink, decimalRoundAllocStatus = decimalRoundValue(decimal128.FromI64(1), 0, 38, 37, mode)
				decimalRoundAllocSink, decimalRoundAllocStatus = decimalRoundValue(max, 0, 38, 38, mode)
				decimalRoundAllocSink, decimalRoundAllocStatus = decimalRoundValue(decimal128.FromI64(95), 1, 1, 0, mode)
			})
			if allocs != 0 {
				t.Fatalf("coefficient helper allocations/run=%g", allocs)
			}
			t.Logf("helper-only allocations/run=%g (five signed scale-gap pairs and exact/overflow paths)", allocs)
		})
	}
}

// Err counts only checks called from the rounding evaluator, excluding
// workspace entry, binding, and allocation checks. This targets actual
// row64/128 checkpoints without a production hook or timing-dependent sleep.
type decimalRoundRunningContext struct {
	context.Context
	cancel       context.CancelFunc
	stop, checks int
}

func (c *decimalRoundRunningContext) Err() error {
	var pcs [24]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".evalDecimalRound") {
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
func TestDecimalRoundRunningCancellation(t *testing.T) {
	for _, stop := range []int{2, 3} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			const n = 193
			inputField, _ := memory.Decimal128Field("d", false, 3, 1)
			outputField, _ := memory.Decimal128Field("out", false, 3, 0)
			input := scalarRegister{decimal: make([]memory.Decimal128, n), decimalType: &inputField}
			out := scalarRegister{decimal: make([]memory.Decimal128, n), decimalType: &outputField}
			sentinel := decimal128.FromI64(-777)
			for i := 0; i < n; i++ {
				input.decimal[i] = decimal128.FromI64(25)
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
			err := evalDecimalRound(&out, &input, DecimalRoundHalfEven, n)
			if !errors.Is(err, context.Canceled) || checks != stop {
				t.Fatal("direct checkpoint", err, checks)
			}
			processed := (stop - 1) * 64
			for i, v := range out.decimal {
				want := sentinel
				if i < processed {
					want = decimal128.FromI64(2)
				}
				if v != want {
					t.Fatalf("slot%d changed beyond processed%d", i, processed)
				}
			}
			// Actual workspace cancellation at that same evaluator checkpoint must
			// discard scratch and release reservations on Close, not merely return err.
			p, err := Compile(memory.NewSchema(inputField), RoundDecimal128(Col("d"), 3, 0, DecimalRoundHalfEven))
			if err != nil {
				t.Fatal(err)
			}
			b := memory.NewBatch()
			b.Rows = n
			b.Decimals["d"] = &memory.Decimal128Vector{Values: input.decimal, Precision: 3, Scale: 1}
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			running := &decimalRoundRunningContext{Context: ctx, cancel: cancel2, stop: stop}
			if _, err = w.Evaluate(running, b, testKernelSet()); !errors.Is(err, context.Canceled) || running.checks != stop {
				t.Fatalf("running workspace cancellation err%v checks%d", err, running.checks)
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("running cancellation leaked", manager.Used())
			}
		})
	}
}
