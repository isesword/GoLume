package expr

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
)

// Exercise the all-literal threshold independently of the column-candidate IN
// fixture. Decimal values must not enter the integer/float sorted-set path.
func TestDecimalComparisonReviewLiteralInDomains(t *testing.T) {
	f, err := memory.Decimal128Field("d", true, 38, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidates := make([]Expr, 0, 18)
	for i := int64(1); i <= 16; i++ {
		candidates = append(candidates, Decimal128(decimal128.FromI64(i), 2, 0))
	}
	candidates = append(candidates, Decimal128(decimal128.FromI64(-1250), 4, 3))
	withNull := append(append([]Expr(nil), candidates...), NullDecimal128(38, 38))
	p, err := CompileExpressions(memory.NewSchema(f), []NamedExpr{
		Named(In(Col("d"), candidates...), "in"),
		Named(Not(In(Col("d"), candidates...)), "not_in"),
		Named(In(Col("d"), withNull...), "in_null"),
		Named(Not(In(Col("d"), withNull...)), "not_in_null"),
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 7
	// Row 6 deliberately has an invalid coefficient behind a NULL bit.
	batch.Decimals["d"] = &memory.Decimal128Vector{
		Values:   []memory.Decimal128{decimal128.FromI64(100), decimal128.FromI64(1600), decimal128.FromI64(-125), decimal128.FromI64(101), {}, decimal128.FromI64(-126), decimal128.New(-1<<63, 0)},
		Validity: memory.Bitmap{63}, Precision: 38, Scale: 2,
	}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, rows := range [][]int{nil, {0, 2, 3, 6}, {}, {1, 4, 5}, nil} {
		got, err := w.EvaluateSelected(context.Background(), batch, rows, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		sourceRows := rows
		if sourceRows == nil {
			sourceRows = []int{0, 1, 2, 3, 4, 5, 6}
		}
		for i, row := range sourceRows {
			match := row < 3
			for col := 0; col < 4; col++ {
				valid := row < 6 && (col < 2 || match)
				truth := match
				if col%2 == 1 {
					truth = !truth
				}
				v := got[col].Uint32
				if v.IsValid(i) != valid || valid && (v.Values[i] != 0) != truth {
					t.Fatalf("rows=%v row=%d output=%d: validity=%v value=%d", rows, row, col, v.IsValid(i), v.Values[i])
				}
			}
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("literal IN workspace leaked")
	}
}

func TestDecimalComparisonReviewLiteralMetadataReuse(t *testing.T) {
	literals := []Expr{
		Decimal128(decimal128.FromI64(123), 3, 0),
		Decimal128(decimal128.FromI64(123), 6, 0),
		Decimal128(decimal128.FromI64(123), 6, 3),
		NullDecimal128(3, 0), NullDecimal128(6, 3),
	}
	named := make([]NamedExpr, len(literals))
	for i, e := range literals {
		named[i] = Named(e, string(rune('a'+i)))
	}
	p, err := CompileExpressions(memory.NewSchema(), named)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, n := range []int{129, 0, 1, 65, 2, 257} {
		batch := memory.NewBatch()
		batch.Rows = n
		got, err := w.Evaluate(context.Background(), batch, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		for col, want := range []struct{ precision, scale int32 }{{3, 0}, {6, 0}, {6, 3}, {3, 0}, {6, 3}} {
			v := got[col].Decimal
			if v == nil || v.Precision != want.precision || v.Scale != want.scale || len(v.Values) != n {
				t.Fatalf("n=%d col=%d wrong decimal metadata: %+v", n, col, v)
			}
			for row := 0; row < n; row++ {
				if v.IsValid(row) != (col < 3) || col < 3 && v.Values[row] != decimal128.FromI64(123) {
					t.Fatalf("n=%d col=%d row=%d wrong literal", n, col, row)
				}
			}
		}
	}
}

func TestDecimalComparisonReviewMalformedVector(t *testing.T) {
	f, _ := memory.Decimal128Field("d", false, 3, 2)
	p, err := Compile(memory.NewSchema(f), EQ(Col("d"), Decimal128(decimal128.FromI64(123), 3, 2)))
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]*memory.Decimal128Vector{
		"precision":         {Values: []memory.Decimal128{decimal128.FromI64(123)}, Precision: 4, Scale: 2},
		"scale":             {Values: []memory.Decimal128{decimal128.FromI64(123)}, Precision: 3, Scale: 1},
		"overflow":          {Values: []memory.Decimal128{decimal128.FromI64(1000)}, Precision: 3, Scale: 2},
		"negative_overflow": {Values: []memory.Decimal128{decimal128.FromI64(-1000)}, Precision: 3, Scale: 2},
		"short":             {Precision: 3, Scale: 2},
		"short_validity":    {Values: []memory.Decimal128{decimal128.FromI64(123)}, Validity: memory.Bitmap{}, Precision: 3, Scale: 2},
		"unexpected_null":   {Values: []memory.Decimal128{{}}, Validity: memory.Bitmap{0}, Precision: 3, Scale: 2},
	} {
		for _, selected := range []bool{false, true} {
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			batch := memory.NewBatch()
			batch.Rows = 1
			batch.Decimals["d"] = v
			var rows []int
			if selected {
				rows = []int{0}
			}
			if _, err := w.EvaluateSelected(context.Background(), batch, rows, testKernelSet()); err == nil {
				t.Errorf("%s selected=%v accepted malformed decimal", name, selected)
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatalf("%s selected=%v leaked", name, selected)
			}
		}
	}
}
