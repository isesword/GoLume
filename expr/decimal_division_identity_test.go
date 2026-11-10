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
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Division by one must obey the exact rescale laws across execution paths.
func divisionByOne(value Expr, precision, scale int32) Decimal128DivideExpr {
	return DivideDecimal128(value, Decimal128(decimal128.FromI64(1), 1, 0), precision, scale)
}
func decimalDivideByOneOracle(value memory.Decimal128, from, precision, scale int32) (*big.Int, decimalDivideStatus) {
	return decimalDivideOracle(value, from, decimal128.FromI64(1), 0, precision, scale)
}
func decimalDivideByOneValue(value memory.Decimal128, from, precision, scale int32) (memory.Decimal128, decimalDivideStatus) {
	return decimalDivideValue(value, from, decimal128.FromI64(1), 0, precision, scale)
}

func decimalDivideByOneFixture(t *testing.T, rows int) (Program, *memory.Batch) {
	t.Helper()
	field, _ := memory.Decimal128Field("d", true, 38, 20)
	batch := memory.NewBatch()
	batch.Rows = rows
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Precision: 38, Scale: 20}
	for row := range rows {
		v.Values[row] = decimal128.FromI64(int64(row - 1024)).Mul(decimal128.GetScaleMultiplier(20))
		if row%7 != 0 {
			memory.Set(v.Validity, row)
		} else {
			v.Values[row] = decimal128.New(math.MinInt64, 0)
		}
	}
	batch.Decimals["d"] = v
	down := divisionByOne(Col("d"), 18, 0)
	p, err := CompileExpressions(memory.NewSchema(field), []NamedExpr{
		down.Alias("down"), divisionByOne(down, 38, 20).Alias("back"), divisionByOne(Col("d"), 30, 22).Alias("up"), divisionByOne(Col("d"), 30, 20).Alias("narrow"), down.Alias("duplicate"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, batch
}

func decimalDivideByOneCheck(t *testing.T, batch *memory.Batch, rows []int, out []ScalarValue) {
	t.Helper()
	count := batch.Rows
	if rows != nil {
		count = len(rows)
	}
	for i := 0; i < count; i++ {
		row := i
		if rows != nil {
			row = rows[i]
		}
		for index, target := range [][2]int32{{18, 0}, {38, 20}, {30, 22}, {30, 20}, {18, 0}} {
			v := out[index].Decimal
			if v.Precision != target[0] || v.Scale != target[1] || v.IsValid(i) != batch.Decimals["d"].IsValid(row) {
				t.Fatalf("row %d output %d metadata/NULL mismatch", row, index)
			}
			if v.IsValid(i) {
				want, status := decimalDivideByOneOracle(batch.Decimals["d"].Values[row], 20, target[0], target[1])
				if status != decimalDivideOK || v.Values[i].BigInt().Cmp(want) != 0 {
					t.Fatalf("row %d output %d coefficient mismatch", row, index)
				}
			} else if v.Values[i] != (memory.Decimal128{}) {
				t.Fatal("division did not clear NULL payload")
			}
		}
	}
	if count > 0 && &out[0].Decimal.Values[0] != &out[4].Decimal.Values[0] {
		t.Fatal("duplicate did not share CSE output")
	}
}

func TestDecimalDivideByOneDenseSelectedLifecycle(t *testing.T) {
	p, _ := decimalDivideByOneFixture(t, 0)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		manager := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, manager)
		for _, n := range []int{2049, 1, 65, 0, 64, 63, 130} {
			_, batch := decimalDivideByOneFixture(t, n)
			before, err := memory.CloneBatch(batch)
			if err != nil {
				t.Fatal(err)
			}
			out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			decimalDivideByOneCheck(t, batch, nil, out)
			for _, step := range []int{1, 2, 65} {
				rows := make([]int, 0)
				for row := 0; row < n; row += step {
					rows = append(rows, row)
				}
				out, err := w.EvaluateSelected(context.Background(), batch, rows, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				decimalDivideByOneCheck(t, batch, rows, out)
				if manager.Used() != w.MemoryBytes() {
					t.Fatal("accounting drift")
				}
			}
			if n > 0 && !reflect.DeepEqual(before, batch) {
				t.Fatal("division mutated source")
			}
		}
		w.Close()
		w.Close()
		if manager.Used() != 0 {
			t.Fatal("close leaked")
		}
	}
}

func TestDecimalDivideByOneEveryAdmissionRetry(t *testing.T) {
	p, batch := decimalDivideByOneFixture(t, 67)
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
			decimalDivideByOneCheck(t, batch, rows, out)
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("retry close leaked")
			}
		}
	}
}

func TestDecimalDivideByOneGuardsErrorsAndBatchOrder(t *testing.T) {
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	inexact := divisionByOne(Decimal128(decimal128.FromI64(1), 1, 1), 1, 0)
	overflow := divisionByOne(Decimal128(decimal128.FromI64(10), 2, 0), 1, 0)
	for name, bad := range map[string]Expr{"inexact": inexact, "overflow": overflow} {
		for guard, e := range map[string]Expr{
			"if": If(Bool(false), bad, zero), "case": Case([]WhenThen{{When: Bool(false), Then: bad}}, zero),
			"coalesce": Coalesce(zero, bad), "in": In(zero, zero, bad), "null": divisionByOne(NullDecimal128(38, 38), 1, 0),
		} {
			t.Run(name+"/"+guard, func(t *testing.T) {
				p, err := Compile(memory.NewSchema(), e)
				if err != nil {
					t.Fatal(err)
				}
				m := memory.NewBudgetManager(0)
				w, _ := NewWorkspace(context.Background(), p, m)
				b := memory.NewBatch()
				b.Rows = 67
				if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				if _, err := w.EvaluateSelected(context.Background(), b, []int{0, 2, 64, 66}, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("guard leaked")
				}
			})
		}
		for _, e := range []Expr{bad, And(Bool(false), EQ(bad, zero)), Add(NullDecimal128(1, 0), bad)} {
			p, err := Compile(memory.NewSchema(), e)
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, m)
			b := memory.NewBatch()
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal("empty batch raised value error", err)
			}
			b.Rows = 1
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
			want := "loses fractional digits at row 0"
			if name == "overflow" {
				want = "overflows at row 0"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatal("missing value error", err)
			}
			if m.Used() != 0 {
				t.Fatal("semantic failure retained memory")
			}
			w.Close()
		}
	}
	field, _ := memory.Decimal128Field("d", true, 3, 1)
	schema := memory.NewSchema(field, memory.Field{Name: "c", Type: memory.TypeBool})
	b := memory.NewBatch()
	b.Rows = 67
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows), Precision: 3, Scale: 1}
	c := &memory.Uint32Vector{Values: make([]uint32, b.Rows)}
	for row := range b.Rows {
		memory.Set(v.Validity, row)
		v.Values[row] = decimal128.FromI64(10)
		c.Values[row] = 1
	}
	b.Decimals["d"], b.Uint32s["c"] = v, c
	convert := divisionByOne(Col("d"), 1, 0)
	p, _ := Compile(schema, convert)
	// The same workspace sees large/small/empty batches before a later failure;
	// its failing row is batch-local, not cumulative and not stale from reuse.
	for _, selected := range []bool{false, true} {
		v.Values[1], v.Values[63] = decimal128.FromI64(10), decimal128.FromI64(10)
		m := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, m)
		for _, n := range []int{67, 1, 0, 65, 67} {
			b.Rows = n
			if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
		}
		v.Values[1], v.Values[63] = decimal128.FromI64(11), decimal128.FromI64(11)
		var err error
		want := "row 1"
		if selected {
			_, err = w.EvaluateSelected(context.Background(), b, []int{0, 2, 63, 64, 66}, testKernelSet())
			want = "row 2"
		} else {
			_, err = w.Evaluate(context.Background(), b, testKernelSet())
		}
		if err == nil || !strings.Contains(err.Error(), "loses fractional digits at "+want) {
			t.Fatal("wrong first row", err)
		}
		if m.Used() != 0 {
			t.Fatal("error retained memory")
		}
		w.Close()
	}
	// Guarded lanes and NULL poison must not be divided or checked for overflow.
	v.Values[1], v.Values[63], v.Values[65] = decimal128.FromI64(11), decimal128.FromI64(100), decimal128.New(math.MinInt64, 0)
	v.Validity[1] &^= 2
	c.Values[1], c.Values[63] = 0, 0
	p, _ = Compile(schema, If(Col("c"), convert, zero))
	w, _ := NewWorkspace(context.Background(), p, nil)
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal("row guard failed", err)
	}
	if _, err := w.EvaluateSelected(context.Background(), b, []int{0, 1, 63, 65, 66}, testKernelSet()); err != nil {
		t.Fatal("selected guard failed", err)
	}
	w.Close()
	// A later left-child row error wins over a right child's earlier error.
	p, _ = Compile(schema, Add(If(Col("c"), zero, inexact), overflow))
	w, _ = NewWorkspace(context.Background(), p, nil)
	_, err := w.Evaluate(context.Background(), b, testKernelSet())
	w.Close()
	if err == nil || !strings.Contains(err.Error(), "loses fractional digits at row 1") {
		t.Fatal("operand order changed", err)
	}
	// Exact divisibility is checked before target precision on the same row.
	p, _ = Compile(memory.NewSchema(), divisionByOne(Decimal128(decimal128.FromI64(999), 3, 1), 1, 0))
	w, _ = NewWorkspace(context.Background(), p, nil)
	_, err = w.Evaluate(context.Background(), b, testKernelSet())
	w.Close()
	if err == nil || !strings.Contains(err.Error(), "loses fractional digits") {
		t.Fatal("rounding was hidden by precision check", err)
	}
}

func TestDecimalDivideByOneConcurrentWorkspaces(t *testing.T) {
	p, b := decimalDivideByOneFixture(t, 65)
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
				decimalDivideByOneCheck(t, b, nil, out)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkDecimalDivideByOneCoefficient(b *testing.B) {
	for _, tc := range []struct {
		name       string
		value      memory.Decimal128
		from, p, s int32
	}{
		{"same_scale", decimal128.New(1, 17), 12, 38, 12},
		{"up", decimal128.FromI64(1<<53 + 1), 0, 38, 20},
		{"down_19", decimal128.GetScaleMultiplier(37), 19, 38, 0},
		{"down_37", decimal128.GetScaleMultiplier(37).Negate(), 37, 1, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var value memory.Decimal128
			var status decimalDivideStatus
			for b.Loop() {
				value, status = decimalDivideByOneValue(tc.value, tc.from, tc.p, tc.s)
			}
			if status != decimalDivideOK {
				b.Fatal(fmt.Sprint(value, status))
			}
		})
	}
}

func TestDecimalDivideByOneNestedGuardAdmissionAndValidation(t *testing.T) {
	field, _ := memory.Decimal128Field("d", true, 38, 20)
	schema := memory.NewSchema(field)
	_, batch := decimalDivideByOneFixture(t, 67)
	converted := divisionByOne(Col("d"), 18, 0)
	zero := Decimal128(memory.Decimal128{}, 18, 0)
	guard := If(IsNotNull(Col("d")), Coalesce(Case([]WhenThen{{When: Bool(true), Then: converted}}, nil), zero), zero)
	p, err := Compile(schema, guard)
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []bool{false, true} {
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(context.Background(), batch, []int{0, 1, 63, 64, 65, 66}, testKernelSet())
			}
			return w.Evaluate(context.Background(), batch, testKernelSet())
		}
		check := func(out []ScalarValue) {
			v := out[0].Decimal
			for i := range v.Values {
				row := i
				if selected {
					row = []int{0, 1, 63, 64, 65, 66}[i]
				}
				want := big.NewInt(0)
				if batch.Decimals["d"].IsValid(row) {
					var status decimalDivideStatus
					want, status = decimalDivideByOneOracle(batch.Decimals["d"].Values[row], 20, 18, 0)
					if status != decimalDivideOK {
						t.Fatal("oracle fixture invalid")
					}
				}
				if !v.IsValid(i) || v.Precision != 18 || v.Scale != 0 || v.Values[i].BigInt().Cmp(want) != 0 {
					t.Fatalf("nested retry result mismatch at row %d", row)
				}
			}
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
				t.Fatalf("selected=%v fail=%d: %v", selected, fail, err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("nested failed admission accounting drift")
			}
			m.fail = 0
			out, err := run(w)
			if err != nil {
				t.Fatal("nested admission retry", err)
			}
			check(out)
			if m.Used() != w.MemoryBytes() {
				t.Fatal("nested retry accounting drift")
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("nested guard leaked")
			}
		}
	}
	// Entirely undemanded branches are not read. If the source is read for the
	// condition, invalid source storage is still an error independent of guards.
	p, _ = Compile(schema, If(IsNull(Col("d")), converted, zero))
	for _, corrupt := range []func(*memory.Decimal128Vector){
		func(v *memory.Decimal128Vector) { v.Precision = 37 },
		func(v *memory.Decimal128Vector) { v.Scale = 19 },
		func(v *memory.Decimal128Vector) { v.Validity = memory.Bitmap{1} },
		func(v *memory.Decimal128Vector) { v.Values = v.Values[:1] },
		func(v *memory.Decimal128Vector) { v.Values[1] = decimal128.New(math.MinInt64, 0) },
	} {
		_, b := decimalDivideByOneFixture(t, 67)
		corrupt(b.Decimals["d"])
		m := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, m)
		if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
			t.Fatal("corrupt source hidden by division guard")
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("invalid source retained budget")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, m)
	if _, err := w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation", err)
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("cancel leaked")
	}
}

func TestDecimalDivideByOneSharedNodeDistinctGuardDomains(t *testing.T) {
	field, _ := memory.Decimal128Field("d", true, 3, 1)
	schema := memory.NewSchema(field, memory.Field{Name: "left", Type: memory.TypeBool}, memory.Field{Name: "right", Type: memory.TypeBool})
	shared := divisionByOne(Col("d"), 1, 0)
	zero := Decimal128(memory.Decimal128{}, 1, 0)
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(If(Col("left"), shared, zero), "left"),
		Named(If(Col("right"), Coalesce(NullDecimal128(1, 0), shared), zero), "right"),
	})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, 129), Precision: 3, Scale: 1}
	left, right := &memory.Uint32Vector{Values: make([]uint32, 129)}, &memory.Uint32Vector{Values: make([]uint32, 129)}
	for row := range v.Values {
		v.Values[row] = decimal128.FromI64(10)
		if row%3 == 0 {
			v.Values[row] = decimal128.FromI64(11)
			continue
		}
		if row%4 == 0 {
			left.Values[row] = 1
		}
		if row%5 == 0 {
			right.Values[row] = 1
		}
	}
	b.Decimals["d"], b.Uint32s["left"], b.Uint32s["right"] = v, left, right
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		m := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, m)
		for _, n := range []int{129, 1, 0, 65, 63, 129} {
			b.Rows = n
			check := func(out []ScalarValue, rows []int) {
				for i, row := range rows {
					for index, condition := range []*memory.Uint32Vector{left, right} {
						got := out[index].Decimal
						if !got.IsValid(i) || got.Values[i] != decimal128.FromI64(int64(condition.Values[row])) {
							t.Fatalf("domain leakage row=%d output=%d", row, index)
						}
					}
				}
			}
			rows := make([]int, n)
			for row := range rows {
				rows[row] = row
			}
			out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			check(out, rows)
			selected := make([]int, 0)
			for row := 0; row < n; row += 2 {
				selected = append(selected, row)
			}
			out, err = w.EvaluateSelected(context.Background(), b, selected, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			check(out, selected)
			if m.Used() != w.MemoryBytes() {
				t.Fatal("guard domain accounting drift")
			}
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("domain workspace leaked")
		}
	}
}
