package logical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strings"
	"testing"
)

type ntileLogicalSource struct{}

func (*ntileLogicalSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64})
}
func (*ntileLogicalSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (*ntileLogicalSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (*ntileLogicalSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return nil, nil
}

func TestNTileConfiguredLogicalContract(t *testing.T) {
	if WindowNTile != 15 || WindowCumeDist != 14 {
		t.Fatal("enum compatibility")
	}
	old := WindowExpr{WindowRank, "", RowsFrame{}, "rank"}
	good := ConfiguredWindowExpr{Expr: WindowExpr{Function: WindowNTile, Alias: "tile"}, Buckets: math.MaxInt64}
	n := &ConfiguredWindow{Input: &Scan{Source: &ntileLogicalSource{}}, Spec: WindowSpec{By: []SortKey{{Expr: expr.Col("k")}}}, Exprs: []ConfiguredWindowExpr{{Expr: old}, good}}
	schema, err := InferSchema(n)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := schema.Field("tile")
	if !ok || f.Type != memory.TypeInt64 || f.Nullable {
		t.Fatal(f)
	}
	if !strings.Contains(Explain(n), "ntile(9223372036854775807)") {
		t.Fatal(Explain(n))
	}
	cloned := n.WindowExpressions()
	cloned[0].Alias = "changed"
	if n.Exprs[0].Expr.Alias != "rank" {
		t.Fatal("alias mutation")
	}
	for _, bad := range []ConfiguredWindowExpr{
		{Expr: good.Expr}, {Expr: good.Expr, Buckets: -1}, {Expr: old, Buckets: 1},
		{Expr: WindowExpr{Function: WindowNTile, Column: "k", Alias: "tile"}, Buckets: 2},
		{Expr: WindowExpr{Function: WindowNTile, Frame: RowsFrame{Start: RowBound{Offset: 1}}, Alias: "tile"}, Buckets: 2},
	} {
		n.Exprs = []ConfiguredWindowExpr{bad}
		if _, err := InferSchema(n); err == nil {
			t.Fatal("accepted", bad)
		}
	}
	n.Exprs = []ConfiguredWindowExpr{good}
	n.Spec.By = nil
	if _, err := InferSchema(n); err == nil {
		t.Fatal("no ORDER BY")
	}
	if _, err := inferWindowField(memory.NewSchema(), good.Expr); err == nil {
		t.Fatal("unconfigured NTILE accepted")
	}
}
