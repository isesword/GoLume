package expr

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

func TestDecimalReviewOpaqueValidationUsesOpaqueBinding(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDecimal, Nullable: true})
	p, err := Compile(schema, IsNull(Col("d")))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 65
	b.Decimals["d"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, 65), Precision: 38, Scale: 0}
	b.Opaque["d"] = &memory.OpaqueVector{Rows: 65, Type: memory.TypeDecimal, Validity: memory.Bitmap{1}}
	m := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("malformed opaque storage bypassed validation and panicked: %v", r)
		}
	}()
	if _, err := w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("malformed opaque validity accepted because a native binding was present")
	}
	if m.Used() != 0 {
		t.Fatalf("semantic error retained %d bytes", m.Used())
	}
}

func TestDecimalReviewCaseNullMetadataIdentity(t *testing.T) {
	a, _ := memory.Decimal128Field("a", true, 12, 2)
	b, _ := memory.Decimal128Field("b", true, 38, 20)
	p, err := CompileExpressions(memory.NewSchema(a, b), []NamedExpr{
		Named(Case([]WhenThen{{When: Bool(false), Then: Col("a")}}, nil), "a_null"),
		Named(Case([]WhenThen{{When: Bool(false), Then: Col("b")}}, nil), "b_null"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range p.outputs {
		conditional := p.instructions[output.root]
		otherwise := conditional.Regions[len(conditional.Regions)-1]
		field := otherwise.instructions[otherwise.root].Decimal
		if field == nil || field.DecimalPrecision != output.Field.DecimalPrecision || field.DecimalScale != output.Field.DecimalScale {
			t.Fatalf("%s NULL arm metadata=%+v, output=%+v", output.Name, field, output.Field)
		}
	}
}

func TestDecimalReviewImplicitNestedCaseCanRebind(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 20)
	schema := memory.NewSchema(f)
	inner := Case([]WhenThen{{When: Bool(true), Then: Col("d")}}, nil)
	e := Coalesce(Case([]WhenThen{{When: Bool(true), Then: inner}}, nil), Col("d"))
	bound, err := Bind(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Compile(schema, bound.Expr)
	if err != nil {
		t.Fatalf("normalized implicit CASE cannot be compiled again: %v", err)
	}
	if p.ResultField().DecimalPrecision != 38 || p.ResultField().DecimalScale != 20 {
		t.Fatalf("rebound metadata=%+v", p.ResultField())
	}
}

func decimalReviewFixture(t *testing.T, rows int) (Program, *memory.Batch) {
	t.Helper()
	a, _ := memory.Decimal128Field("a", true, 38, 20)
	b, _ := memory.Decimal128Field("b", true, 38, 20)
	schema := memory.NewSchema(a, b, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	e := If(Col("c"), Coalesce(Col("a"), Col("b")), Case([]WhenThen{{When: IsNotNull(Col("b")), Then: Col("b")}}, nil))
	p, err := CompileExpressions(schema, []NamedExpr{Named(e, "value"), Named(Col("a"), "alias")})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = rows
	for index, name := range []string{"a", "b"} {
		v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows)), Precision: 38, Scale: 20}
		for row := range rows {
			v.Values[row] = decimal128.New(int64((row+1)*(index+1)), uint64(1)<<53+uint64(row+1))
			if row%(3+index) != 0 {
				memory.Set(v.Validity, row)
			}
		}
		batch.Decimals[name] = v
	}
	c := &memory.Uint32Vector{Values: make([]uint32, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
	for row := range rows {
		c.Values[row] = uint32(row % 2)
		if row%5 != 0 {
			memory.Set(c.Validity, row)
		}
	}
	batch.Uint32s["c"] = c
	return p, batch
}

func decimalReviewCheck(t *testing.T, values []ScalarValue, b *memory.Batch, rows []int) {
	t.Helper()
	for index, row := range rows {
		want := b.Decimals["b"]
		if b.Uint32s["c"].IsValid(row) && b.Uint32s["c"].Values[row] != 0 && b.Decimals["a"].IsValid(row) {
			want = b.Decimals["a"]
		}
		for output, expected := range []*memory.Decimal128Vector{want, b.Decimals["a"]} {
			got := values[output].Decimal
			if got == nil || got.Precision != 38 || got.Scale != 20 || got.IsValid(index) != expected.IsValid(row) || got.IsValid(index) && got.Values[index] != expected.Values[row] {
				t.Fatalf("output=%d index=%d source=%d got=%+v", output, index, row, got)
			}
		}
	}
}

func TestDecimalReviewSelectedNestedAdmissionRetry(t *testing.T) {
	p, b := decimalReviewFixture(t, 257)
	rows := make([]int, 0)
	for row := 0; row < b.Rows; row += 2 {
		rows = append(rows, row)
	}
	for fail := 1; fail <= 50; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if _, err := w.EvaluateSelected(context.Background(), b, rows, testKernelSet()); err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatalf("after failure used=%d retained=%d", m.Used(), w.MemoryBytes())
			}
			m.fail = 0
			for _, selected := range [][]int{rows, {0}, {}, rows[:65], rows} {
				values, err := w.EvaluateSelected(context.Background(), b, selected, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				decimalReviewCheck(t, values, b, selected)
				if m.Used() != w.MemoryBytes() {
					t.Fatalf("used=%d retained=%d", m.Used(), w.MemoryBytes())
				}
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatalf("Close retained %d bytes", m.Used())
			}
		})
	}
}
