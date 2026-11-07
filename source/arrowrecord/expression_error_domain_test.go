package arrowrecord

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestPushdownRetainsFalliblePredicateWithFalseConjunct(t *testing.T) {
	builder := array.NewInt64Builder(arrowmem.NewGoAllocator())
	builder.AppendValues([]int64{1, 300}, nil)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{values}, 2)
	defer record.Release()
	src, err := NewSource(record)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	fallible := expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0))
	neverTrue := expr.LT(expr.Col("x"), expr.I64(-1))
	for _, predicate := range []expr.Expr{expr.And(fallible, neverTrue), expr.And(neverTrue, fallible)} {
		_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		if report.PrunedPartitions != 0 || report.SelectedPartitions != 1 {
			t.Fatalf("fallible predicate was metadata-pruned: %+v", report)
		}
	}
	_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: neverTrue})
	if err != nil || report.PrunedPartitions != 1 {
		t.Fatalf("safe predicate lost pruning: %+v, %v", report, err)
	}
}
