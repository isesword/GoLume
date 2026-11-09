package expr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func decimalExpressionInput(t *testing.T, rows int) (memory.Schema, *memory.Batch) {
	t.Helper()
	a, err := memory.Decimal128Field("a", true, 38, 12)
	if err != nil {
		t.Fatal(err)
	}
	b := a
	b.Name = "b"
	schema := memory.NewSchema(a, b, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	batch := memory.NewBatch()
	batch.Rows = rows
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	values := []memory.Decimal128{max, max.Negate(), decimal128.FromI64(1<<53 + 1), decimal128.FromI64(-1<<53 - 1), {}, decimal128.FromI64(1)}
	for col, name := range []string{"a", "b"} {
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Precision: 38, Scale: 12}
		for row := range rows {
			v.Values[row] = values[(row+col)%len(values)]
			if (row+col)%5 != 0 {
				memory.Set(v.Validity, row)
			}
		}
		batch.Decimals[name] = v
	}
	c := &memory.Uint32Vector{Values: make([]uint32, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
	for row := range rows {
		c.Values[row] = uint32(row % 2)
		if row%3 != 0 {
			memory.Set(c.Validity, row)
		}
	}
	batch.Uint32s["c"] = c
	return schema, batch
}

func decimalExpressionProgram(t *testing.T, schema memory.Schema) Program {
	t.Helper()
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(Col("a"), "alias"), Named(Col("a"), "duplicate"),
		Named(If(Col("c"), Col("a"), Col("b")), "if"),
		Named(Coalesce(Col("a"), Col("b")), "coalesce"),
		Named(Case([]WhenThen{{When: Col("c"), Then: Col("a")}}, nil), "case"),
		Named(IsNull(Col("a")), "null"), Named(IsNotNull(Col("a")), "not_null"),
		Named(If(IsNull(Col("a")), Coalesce(Col("a"), Col("b")), If(Col("c"), Col("a"), Col("b"))), "nested"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("decimal program lost selected execution")
	}
	return p
}

func checkDecimalExpressionRows(t *testing.T, batch *memory.Batch, rows []int, got []ScalarValue) {
	t.Helper()
	if rows == nil {
		rows = make([]int, batch.Rows)
		for i := range rows {
			rows[i] = i
		}
	}
	a, b, c := batch.Decimals["a"], batch.Decimals["b"], batch.Uint32s["c"]
	for col := range got {
		if col == 5 || col == 6 {
			continue
		}
		v := got[col].Decimal
		if v == nil || v.Precision != 38 || v.Scale != 12 || len(v.Values) != len(rows) || got[col].Opaque != nil || got[col].Float64 != nil {
			t.Fatalf("column %d lost native decimal metadata: %+v", col, got[col])
		}
		for output, row := range rows {
			chosen := a
			valid := a.IsValid(row)
			condition := c.IsValid(row) && c.Values[row] != 0
			switch col {
			case 2:
				if !condition {
					chosen = b
				}
				valid = chosen.IsValid(row)
			case 3:
				if !a.IsValid(row) {
					chosen = b
				}
				valid = chosen.IsValid(row)
			case 4:
				valid = condition && a.IsValid(row)
			case 7:
				if !a.IsValid(row) || !condition {
					chosen = b
				}
				valid = chosen.IsValid(row)
			}
			if v.IsValid(output) != valid || valid && v.Values[output] != chosen.Values[row] {
				t.Fatalf("col=%d row=%d invalid exact value", col, row)
			}
		}
	}
	for output, row := range rows {
		want := uint32(0)
		if !a.IsValid(row) {
			want = 1
		}
		if got[5].Uint32.Values[output] != want || got[6].Uint32.Values[output] != 1-want || !got[5].Uint32.IsValid(output) {
			t.Fatal("wrong decimal null check")
		}
	}
	if len(rows) > 0 && &got[0].Decimal.Values[0] != &got[1].Decimal.Values[0] {
		t.Fatal("duplicate root did not share borrowed coefficient view")
	}
}

func TestDecimalExpressionsExactDenseSelectedAndLifetime(t *testing.T) {
	ctx := context.Background()
	schema, _ := decimalExpressionInput(t, 0)
	p := decimalExpressionProgram(t, schema)
	for i := 0; i < 5; i++ {
		f := p.OutputField(i)
		if f.DecimalPrecision != 38 || f.DecimalScale != 12 {
			t.Fatalf("output field %d lost metadata", i)
		}
	}
	// Compiled fields must not depend on the caller's schema or output descriptor.
	schema.Fields[0].DecimalPrecision = 9
	f := p.OutputField(0)
	f.DecimalScale = 0
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		budget := memory.NewBudgetManager(0)
		w, err := NewWorkspace(ctx, p, budget)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{130, 1, 65, 0, 257, 63, 64, 0} {
			_, batch := decimalExpressionInput(t, n)
			before, err := memory.CloneBatch(batch)
			if err != nil {
				t.Fatal(err)
			}
			got, err := w.Evaluate(ctx, batch, kernel.New(mode))
			if err != nil {
				t.Fatal(err)
			}
			checkDecimalExpressionRows(t, batch, nil, got)
			if n > 0 && &got[0].Decimal.Values[0] != &batch.Decimals["a"].Values[0] {
				t.Fatal("plain projection copied input")
			}
			for _, step := range []int{1, 2, 13, 65} {
				rows := make([]int, 0)
				for i := 0; i < n; i += step {
					rows = append(rows, i)
				}
				got, err = w.EvaluateSelected(ctx, batch, rows, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				checkDecimalExpressionRows(t, batch, rows, got)
				if len(rows) > 0 && &got[0].Decimal.Values[0] == &batch.Decimals["a"].Values[0] {
					t.Fatal("selected output borrowed original input")
				}
				if w.MemoryBytes() != budget.Used() {
					t.Fatalf("rows=%d capacity=%d charge=%d", n, w.MemoryBytes(), budget.Used())
				}
			}
			if n > 0 && !reflect.DeepEqual(batch, before) {
				t.Fatal("evaluation mutated input")
			}
		}
		w.Close()
		w.Close()
		if budget.Used() != 0 {
			t.Fatal("workspace leaked")
		}
	}
}

func TestDecimalExpressionAdmissionRetry(t *testing.T) {
	ctx := context.Background()
	schema, batch := decimalExpressionInput(t, 67)
	p := decimalExpressionProgram(t, schema)
	for _, selected := range []bool{false, true} {
		run := func(w *Workspace) ([]ScalarValue, error) {
			if selected {
				return w.EvaluateSelected(ctx, batch, []int{0, 1, 2, 63, 64, 66}, testKernelSet())
			}
			return w.Evaluate(ctx, batch, testKernelSet())
		}
		successful := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
		w, _ := NewWorkspace(ctx, p, successful)
		if _, err := run(w); err != nil {
			t.Fatal(err)
		}
		calls := successful.calls
		w.Close()
		for fail := 1; fail <= calls; fail++ {
			budget := memory.NewBudgetManager(0)
			manager := &selectedFaultManager{Manager: budget, fail: fail}
			w, _ := NewWorkspace(ctx, p, manager)
			if _, err := run(w); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("selected=%v fail=%d err=%v", selected, fail, err)
			}
			if w.MemoryBytes() != budget.Used() {
				t.Fatalf("failed admission capacity=%d charge=%d", w.MemoryBytes(), budget.Used())
			}
			manager.fail = 0
			got, err := run(w)
			if err != nil {
				t.Fatalf("retry fail=%d: %v", fail, err)
			}
			var rows []int
			if selected {
				rows = []int{0, 1, 2, 63, 64, 66}
			}
			checkDecimalExpressionRows(t, batch, rows, got)
			w.Close()
			if budget.Used() != 0 {
				t.Fatal("failed admission leaked")
			}
		}
	}
}

func TestDecimalExpressionRejectsUnsupportedAndMalformed(t *testing.T) {
	schema, _ := decimalExpressionInput(t, 0)
	wrong := schema.Fields[0]
	wrong.Name = "different"
	wrong.DecimalScale = 11
	schema.Fields = append(schema.Fields, wrong)
	for _, e := range []Expr{Add(Col("a"), Col("b")), Mul(Col("a"), I64(2)), Div(Col("a"), F64(2)), GT(Col("a"), I64(0)), CastTo(Col("a"), memory.TypeFloat64), CastTo(Col("a"), memory.TypeDecimal), Null(memory.TypeDecimal), If(Bool(true), Col("a"), Col("different")), Coalesce(Col("a"), I64(1))} {
		if _, err := Compile(schema, e); err == nil {
			t.Fatalf("unsupported decimal expression accepted: %#v", e)
		}
	}
	for _, selected := range []bool{false, true} {
		for _, change := range []func(*memory.Batch){
			func(b *memory.Batch) { b.Decimals["a"].Scale-- },
			func(b *memory.Batch) { b.Decimals["a"].Precision-- },
			func(b *memory.Batch) { b.Decimals["a"].Values = b.Decimals["a"].Values[:1] },
			func(b *memory.Batch) { b.Decimals["a"].Validity = memory.Bitmap{} },
			func(b *memory.Batch) { b.Decimals["a"].Values[1] = decimal128.GetScaleMultiplier(38) },
			func(b *memory.Batch) { delete(b.Decimals, "a") },
		} {
			schema, batch := decimalExpressionInput(t, 65)
			p, err := Compile(schema, Col("a"))
			if err != nil {
				t.Fatal(err)
			}
			change(batch)
			budget := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), p, budget)
			if selected {
				_, err = w.EvaluateSelected(context.Background(), batch, []int{1}, testKernelSet())
			} else {
				_, err = w.Evaluate(context.Background(), batch, testKernelSet())
			}
			if err == nil {
				t.Fatal("malformed native decimal accepted")
			}
			if budget.Used() != 0 {
				t.Fatal("malformed decimal leaked")
			}
		}
	}
}

func TestDecimalProjectionLogicalPrefix(t *testing.T) {
	schema, batch := decimalExpressionInput(t, 130)
	p, err := Compile(schema, Col("a"))
	if err != nil {
		t.Fatal(err)
	}
	batch.Rows = 65
	batch.Decimals["a"].Values[129] = decimal128.GetScaleMultiplier(38)
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	values, err := w.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if values[0].Decimal.Len() != 65 {
		t.Fatal("export retained trailing rows")
	}
}

func TestDecimalProjectionBorrowsWithoutPayloadCharge(t *testing.T) {
	schema, batch := decimalExpressionInput(t, 1025)
	p, err := Compile(schema, Col("a"))
	if err != nil {
		t.Fatal(err)
	}
	zero, err := p.WorkspaceBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	larger, err := p.WorkspaceBytes(batch.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if zero != larger {
		t.Fatal("borrowed decimal column charged dense coefficient storage")
	}
	manager := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, manager)
	values, err := w.Evaluate(context.Background(), batch, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if &values[0].Decimal.Values[0] != &batch.Decimals["a"].Values[0] {
		t.Fatal("plain decimal projection allocated coefficient storage")
	}
	if w.MemoryBytes() != zero+int64(unsafe.Sizeof(ScalarValue{})) || manager.Used() != w.MemoryBytes() {
		t.Fatalf("metadata-only projection charge %d", manager.Used())
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("plain projection leaked")
	}
}
