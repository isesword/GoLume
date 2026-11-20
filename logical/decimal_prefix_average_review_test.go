package logical

import (
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

type n28ReviewSource struct {
	n14SchemaSource
	order []source.OrderingColumn
	plan  source.StreamPlanNode
}

func (s n28ReviewSource) Ordering() []source.OrderingColumn { return s.order }
func (s n28ReviewSource) StreamPlan() source.StreamPlanNode { return s.plan }
func n28ReviewNode(t *testing.T) *DecimalPrefixAverage {
	t.Helper()
	d, e := memory.Decimal128Field("d", true, 38, 7)
	if e != nil {
		t.Fatal(e)
	}
	src := n28ReviewSource{n14SchemaSource: n14SchemaSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, d)}, order: []source.OrderingColumn{{Name: "key"}}, plan: source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}}
	return &DecimalPrefixAverage{Input: &Scan{Source: src}, Column: "d", OrderBy: "key", Alias: "mean", Precision: 17, Scale: 4, Rounding: aggregate.DecimalAverageHalfEven}
}
func TestN28ReviewLogicalSchemaAndExplain(t *testing.T) {
	n := n28ReviewNode(t)
	schema, e := InferSchema(n)
	if e != nil {
		t.Fatal(e)
	}
	if len(schema.Fields) != 3 {
		t.Fatal(schema)
	}
	out := schema.Fields[2]
	if out.Name != "mean" || out.Type != memory.TypeDecimal || !out.Nullable || out.DecimalPrecision != 17 || out.DecimalScale != 4 {
		t.Fatal(out)
	}
	if schema.Fields[1].DecimalPrecision != 38 || schema.Fields[1].DecimalScale != 7 {
		t.Fatal("input metadata changed")
	}
	explanation := Explain(n)
	for _, token := range []string{"DECIMAL_PREFIX_AVG", "d", "key", "mean", "17", "4"} {
		if !strings.Contains(explanation, token) {
			t.Fatal("explain omitted config", explanation)
		}
	}
	if strings.Contains(explanation, "WINDOW") || strings.Contains(explanation, "SORT ") {
		t.Fatal("misleading lowering", explanation)
	}
	// Schema inference must own its field slice and not mutate scan metadata.
	schema.Fields[0].Name = "poison"
	again, e := InferSchema(n)
	if e != nil || again.Fields[0].Name != "key" {
		t.Fatal("schema alias", again, e)
	}
}
func TestN28ReviewLogicalInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*DecimalPrefixAverage)
	}{
		{"missingvalue", func(n *DecimalPrefixAverage) { n.Column = "absent" }},
		{"missingorder", func(n *DecimalPrefixAverage) { n.OrderBy = "absent" }},
		{"sameinput", func(n *DecimalPrefixAverage) { n.Column = "key" }},
		{"emptyalias", func(n *DecimalPrefixAverage) { n.Alias = "" }},
		{"keyalias", func(n *DecimalPrefixAverage) { n.Alias = "key" }},
		{"valuealias", func(n *DecimalPrefixAverage) { n.Alias = "d" }},
		{"precisionzero", func(n *DecimalPrefixAverage) { n.Precision = 0 }},
		{"precision39", func(n *DecimalPrefixAverage) { n.Precision = 39 }},
		{"negativescale", func(n *DecimalPrefixAverage) { n.Scale = -1 }},
		{"scalelarge", func(n *DecimalPrefixAverage) { n.Scale = 18 }},
		{"roundzero", func(n *DecimalPrefixAverage) { n.Rounding = 0 }},
		{"roundunknown", func(n *DecimalPrefixAverage) { n.Rounding = 255 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := n28ReviewNode(t)
			tc.mutate(n)
			if _, e := InferSchema(n); e == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
func TestN28ReviewLogicalInputProof(t *testing.T) {
	for _, mode := range []string{"nullable-key", "wrong-key", "descending", "wrong-order", "no-order", "opaque-value", "extra", "filter", "pushed-filter", "limit", "sort", "expression", "blocking", "spilling-child"} {
		t.Run(mode, func(t *testing.T) {
			n := n28ReviewNode(t)
			scan := n.Input.(*Scan)
			src := scan.Source.(n28ReviewSource)
			switch mode {
			case "nullable-key":
				src.schema.Fields[0].Nullable = true
			case "wrong-key":
				src.schema.Fields[0].Type = memory.TypeUint64
			case "descending":
				src.order[0].Descending = true
			case "wrong-order":
				src.order[0].Name = "d"
			case "no-order":
				src.order = nil
			case "opaque-value":
				src.schema.Fields[1].DecimalPrecision = 0
			case "extra":
				src.schema = memory.NewSchema(append(src.schema.Fields, memory.Field{Name: "extra", Type: memory.TypeInt64})...)
			case "filter":
				n.Input = &Filter{Input: scan, Predicate: expr.Bool(true)}
			case "pushed-filter":
				scan.PushedPredicate = expr.Bool(true)
			case "limit":
				n.Input = &Limit{Input: scan, N: 1}
			case "sort":
				n.Input = &Sort{Input: scan, By: []SortKey{{Expr: expr.Col("key")}}}
			case "expression":
				n.Input = &ExprProjection{Input: scan, Exprs: []expr.NamedExpr{expr.Col("key").Alias("key"), expr.Col("d").Alias("d")}}
			case "blocking":
				src.plan.Class = source.StreamBlocking
			case "spilling-child":
				src.plan.Children = []source.StreamPlanNode{{Operator: "hidden-spill", Class: source.StreamSpillable}}
			}
			scan.Source = src
			if _, e := InferSchema(n); e == nil {
				t.Fatal("unproven input accepted")
			}
		})
	}
}
func TestN28ReviewLogicalSafeProjection(t *testing.T) {
	n := n28ReviewNode(t)
	scan := n.Input.(*Scan)
	src := scan.Source.(n28ReviewSource)
	src.schema = memory.NewSchema(append(src.schema.Fields, memory.Field{Name: "unused", Type: memory.TypeInt64})...)
	scan.Source = src
	n.Input = &Projection{Input: &Projection{Input: scan, Columns: []string{"key", "d", "unused"}}, Columns: []string{"d", "key"}}
	s, e := InferSchema(n)
	if e != nil || len(s.Fields) != 3 || s.Fields[0].Name != "d" || s.Fields[1].Name != "key" {
		t.Fatal(s, e)
	}
}
