package physical

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestDecimalReviewPhysicalSelectedPath(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 20)
	schema := memory.NewSchema(f, memory.Field{Name: "keep", Type: memory.TypeInt64}, memory.Field{Name: "padding", Type: memory.TypeInt64})
	b := memory.NewBatch()
	b.Rows = 130
	d := &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Validity: make(memory.Bitmap, memory.WordsFor(b.Rows)), Precision: 38, Scale: 20}
	keep := &memory.Int64Vector{Values: make([]int64, b.Rows)}
	for row := range b.Rows {
		d.Values[row] = decimal128.New(int64(row+1), uint64(1)<<53+uint64(row+1))
		if row%7 != 0 {
			memory.Set(d.Validity, row)
		}
		keep.Values[row] = int64(row % 2)
	}
	b.Decimals["d"] = d
	b.Int64s["keep"] = keep
	b.Int64s["padding"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	input, err := inmemory.New(schema, b, b)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(input, expr.EQ(expr.Col("keep"), expr.I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("d"), "result")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !projected.selectionEnabled {
		t.Fatal("native decimal filter/project edge did not enable selected evaluation")
	}
	budget := memory.NewBudgetManager(1 << 20)
	opts := testReadOptions()
	opts.MemoryManager = budget
	stream, err := source.OpenBatchStream(context.Background(), projected, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	count := 0
	for {
		out := memory.NewBatch()
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		v := out.Decimals["result"]
		if v == nil || v.Precision != 38 || v.Scale != 20 {
			t.Fatalf("wrong result metadata: %+v", v)
		}
		for row := range out.Rows {
			sourceRow := (count%65)*2 + 1
			if v.IsValid(row) != d.IsValid(sourceRow) || v.IsValid(row) && v.Values[row] != d.Values[sourceRow] {
				t.Fatalf("row=%d source=%d mismatch", count, sourceRow)
			}
			count++
		}
	}
	if count != 130 {
		t.Fatalf("rows=%d want=130", count)
	}
	stream.Close()
	if budget.Used() != 0 {
		t.Fatalf("stream retained %d bytes", budget.Used())
	}
}

func TestDecimalReviewOpaquePayloadKeepsMaterializedBoundary(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 20)
	input, err := inmemory.New(memory.NewSchema(f, memory.Field{Name: "opaque", Type: memory.TypeDecimal}))
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(input, expr.Bool(true))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("d"), "result")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if projected.selectionEnabled {
		t.Fatal("opaque decimal payload entered native selected path")
	}
}
