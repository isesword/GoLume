package physical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestProjectOpaqueAliasUsesCanonicalValueAndPreservesOwner(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	builder := array.NewInt64Builder(alloc)
	builder.AppendValues([]int64{7, 0}, []bool{true, false})
	values := builder.NewArray()
	builder.Release()
	batch := memory.NewBatch()
	batch.Rows = 2
	batch.Opaque["raw"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: "opaque-test", Value: values, Validity: memory.Bitmap{1}, Owned: true}
	defer batch.ReleaseOwned()
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "opaque-test", Nullable: true}), batch)
	if err != nil {
		t.Fatal(err)
	}
	project, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Col("raw"), "alias"), expr.Named(expr.IsNull(expr.Col("raw")), "missing")}, false)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := project.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next ok=%v err=%v", ok, err)
	}
	alias := out.Opaque["alias"]
	if alias == nil || alias.Owned || alias.Rows != 2 || out.Uint32s["missing"].Values[0] != 0 || out.Uint32s["missing"].Values[1] != 1 {
		t.Fatal("opaque alias or ordinary NULL predicate was not exported")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if !batch.Opaque["raw"].Owned || batch.Opaque["raw"].Value != values || values.(*array.Int64).Value(0) != 7 {
		t.Fatal("projection released or mutated the source's opaque owner")
	}
	// Include-input passthrough must also protect an independently owned input
	// slice, even when no expression directly reads that opaque column.
	passthrough, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.I64(1), "one")}, true)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := expr.NewWorkspace(context.Background(), passthrough.program, memory.NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if err := applyProjectBatch(context.Background(), passthrough, batch, out, workspace, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if out.Opaque["raw"] == batch.Opaque["raw"] || out.Opaque["raw"].Owned {
		t.Fatal("include-input projection shared an owned opaque header")
	}
	out.ClearColumns()
	if !batch.Opaque["raw"].Owned || values.(*array.Int64).Value(0) != 7 {
		t.Fatal("clearing passthrough output stole source ownership")
	}
}
