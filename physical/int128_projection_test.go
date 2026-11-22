package physical_test

import (
	"strings"
	"testing"

	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/source/inmemory"
)

func TestInt128WorkspaceResultIsNotMaterialized(t *testing.T) {
	s := memory.NewSchema(memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "u", Type: memory.TypeUint64})
	base, err := inmemory.New(s)
	if err != nil {
		t.Fatal(err)
	}
	named := []expr.NamedExpr{expr.Named(expr.AddInt128(expr.Col("i"), expr.Col("u")), "wide")}
	scan := &logical.Scan{Source: base}
	_, logicalErr := logical.InferSchema(&logical.ExprProjection{Input: scan, Exprs: named})
	_, physicalErr := physical.NewProjectSource(base, named, false)
	keys := []logical.SortKey{{Expr: named[0].Expr}}
	_, sortErr := logical.InferSchema(&logical.Sort{Input: scan, By: keys})
	_, topKErr := logical.InferSchema(&logical.TopK{Input: scan, By: keys, K: 1})
	for _, err := range []error{logicalErr, physicalErr, sortErr, topKErr} {
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "int128") {
			t.Fatalf("materialization or sort did not reject Int128 at bind time: %v", err)
		}
	}
}
