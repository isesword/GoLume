package physical

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestListLengthKeepsMaterializedSelectionBoundary(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{1}}
	b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 0}, Values: &memory.Int64Vector{}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.ListInt64Field("xs", false, false)), b)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(src, expr.GT(expr.Col("id"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.ListLength(expr.Col("xs")), "n")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if projected.selectionEnabled {
		t.Fatal("nested input entered fixed-width selection path")
	}
}
