package optimizer

import (
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestConfiguredWindowPruningKeepsBucketAlignment(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeString}, memory.Field{Name: "unused", Type: memory.TypeInt64})
	n := &logical.ConfiguredWindow{Input: &logical.Scan{Source: &testSource{schema: schema}}, Spec: logical.WindowSpec{By: []logical.SortKey{{Expr: expr.Col("k")}}}, Exprs: []logical.ConfiguredWindowExpr{
		{Expr: logical.WindowExpr{Function: logical.WindowNTile, Alias: "drop"}, Buckets: 2},
		{Expr: logical.WindowExpr{Function: logical.WindowNTile, Alias: "a"}, Buckets: 7},
		{Expr: logical.WindowExpr{Function: logical.WindowRank, Alias: "rank"}},
		{Expr: logical.WindowExpr{Function: logical.WindowNTile, Alias: "keep"}, Buckets: 3},
	}}
	top := &logical.Projection{Input: &logical.Filter{Input: n, Predicate: expr.GT(expr.Col("a"), expr.Lit(1))}, Columns: []string{"keep", "a", "rank"}}
	before, err := logical.InferSchema(top)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Optimize(top)
	if err != nil {
		t.Fatal(err)
	}
	after, err := logical.InferSchema(result.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Fields, after.Fields) {
		t.Fatal("replacement order", before, after)
	}
	var found *logical.ConfiguredWindow
	filterAbove := false
	var walk func(logical.Plan)
	walk = func(p logical.Plan) {
		switch v := p.(type) {
		case *logical.Projection:
			walk(v.Input)
		case *logical.Filter:
			filterAbove = true
			walk(v.Input)
		case *logical.ConfiguredWindow:
			found = v
		}
	}
	walk(result.Plan)
	if found == nil || !filterAbove {
		t.Fatal("window/filter lost")
	}
	if len(found.Exprs) != 3 {
		t.Fatal(found.Exprs)
	}
	want := []int64{7, 0, 3}
	for i, e := range found.Exprs {
		if e.Buckets != want[i] {
			t.Fatal("index configuration drift", found.Exprs)
		}
	}
	found.Exprs[0].Buckets = 99
	if n.Exprs[1].Buckets != 7 {
		t.Fatal("mutated original configuration")
	}
	if scan, ok := found.Input.(*logical.Scan); ok && scan.PushedPredicate != nil {
		t.Fatal("predicate crossed window")
	}
}
