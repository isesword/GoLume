package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func n39ReviewFixture() (memory.Schema, *memory.Batch) {
	f := memory.Field{Name: "item", Type: memory.TypeInt64, Nullable: true, Encoding: memory.EncodingPlain}
	n := &memory.ArrayNode{Kind: memory.TypeInt64, Encoding: memory.EncodingView, Int64: &memory.Int64Vector{Values: []int64{7, 8, 9}, Encoding: memory.EncodingView}}
	for i := 0; i < 3; i++ {
		f = memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{f}}
		n = &memory.ArrayNode{Kind: memory.TypeList, Encoding: memory.EncodingView, Offsets: []uint32{0, 1, 2, 3}, Child: n}
	}
	f.Name = "v"
	b := memory.NewBatch()
	b.Rows = 3
	b.Arrays["v"] = n
	return memory.NewSchema(f), b
}
func TestN39ReviewAccessBindingAndRepeatedOwner(t *testing.T) {
	sc, b := n39ReviewFixture()
	get := ListGet(Col("v"), I64(0))
	for _, e := range []Expr{ListLength(Col("v")), get} {
		bound, err := Bind(sc, e)
		if err != nil {
			t.Fatal(err)
		}
		if bound.MayError {
			t.Fatal("storage errors changed row effect")
		}
	}
	if _, err := Bind(sc, IsNull(Col("v"))); err == nil {
		t.Fatal("blanket depth3 computation admitted")
	}
	p, e := CompileExpressions(sc, []NamedExpr{get.Alias("a"), get.Alias("b"), ListGet(ListGet(get, I64(0)), I64(0)).Alias("leaf"), ListLength(Col("v")).Alias("len")})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(1 << 20)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	for run := 0; run < 4; run++ {
		got, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
		if e != nil {
			t.Fatal(e)
		}
		if got[0].Array == nil || got[1].Array == nil {
			t.Fatal("depth2 output not typed array")
		}
		for r := 0; r < 3; r++ {
			if got[2].Int64.Values[r] != int64(7+r+run*10) || got[3].Int64.Values[r] != 1 {
				t.Fatal("chain oracle", run, r)
			}
		}
		leaf := b.Arrays["v"].Child.Child.Child.Int64
		for r := range leaf.Values {
			leaf.Values[r] += 10
		}
		if got[0].Array.Child.Child.Int64.Values[0] != int64(7+run*10) {
			t.Fatal("output borrowed source")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = w.Evaluate(ctx, b, kernel.New(kernel.ModeScalar)); !errors.Is(e, context.Canceled) {
		t.Fatal("cancel", e)
	}
	w.Close()
	w.Close()
	if m.Used() != 0 {
		t.Fatal("workspace leak", m.Used())
	}
}
func TestN39ReviewRequiredParentAndHiddenStructure(t *testing.T) {
	sc, b := n39ReviewFixture()
	sc.Fields[0].Nullable = false
	b.Arrays["v"].Validity = memory.Bitmap{6}
	p, e := CompileExpressions(sc, []NamedExpr{ListGet(Col("v"), I64(0)).Alias("a")})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(1 << 20)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar)); e == nil {
		t.Fatal("required source root NULL accepted")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("error leak")
	}
}
