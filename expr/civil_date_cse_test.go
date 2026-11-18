package expr

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestCivilDateCSEStrongIdentity(t *testing.T) {
	z := civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, ""))
	r := civilTestResolver(t, z, FoldReject)
	copied := r
	reload := civilTestResolver(t, z, FoldReject)
	other := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, "")), FoldReject)
	policy := civilTestResolver(t, z, FoldLater)
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeDate32}, memory.Field{Name: "y", Type: memory.TypeDate32})
	p, e := CompileExpressions(schema, []NamedExpr{DateAtMidnight(Col("x"), r).Alias("a"), DateAtMidnight(Col("x"), copied).Alias("copy"), DateAtMidnight(Col("x"), reload).Alias("reload"), DateAtMidnight(Col("x"), other).Alias("other"), DateAtMidnight(Col("x"), policy).Alias("policy"), TryDateAtMidnight(Col("x"), r).Alias("try"), DateAtMidnight(Col("y"), r).Alias("different_input")})
	if e != nil {
		t.Fatal(e)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("copied resolver did not CSE")
	}
	for i := 2; i < len(p.outputs); i++ {
		for j := 0; j < i; j++ {
			if p.outputs[i].root == p.outputs[j].root {
				t.Fatalf("distinct identities/mode/input merged %d %d", i, j)
			}
		}
	}
	b := memory.NewBatch()
	b.Reset(1)
	b.Int32s["x"] = &memory.Int32Vector{Values: []int32{0}}
	b.Int32s["y"] = &memory.Int32Vector{Values: []int32{1}}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	for i, want := range []int64{0, 0, 0, -3600e9, 0, 0, 86400e9} {
		if !out[i].Int64.IsValid(0) || out[i].Int64.Values[0] != want {
			t.Fatal(i, out[i], want)
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}
