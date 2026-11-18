package expr

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestCivilTimestampCSEStrongIdentity(t *testing.T) {
	z := civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, ""))
	r := civilTestResolver(t, z, FoldReject)
	copied := r
	reload := civilTestResolver(t, z, FoldReject)
	other := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{3600}, nil, nil, nil, "")), FoldReject)
	policy := civilTestResolver(t, z, FoldLater)
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString}, memory.Field{Name: "y", Type: memory.TypeString})
	p, e := CompileExpressions(schema, []NamedExpr{ParseCivilTimestamp(Col("x"), r).Alias("a"), ParseCivilTimestamp(Col("x"), copied).Alias("copy"), ParseCivilTimestamp(Col("x"), reload).Alias("reload"), ParseCivilTimestamp(Col("x"), other).Alias("other"), ParseCivilTimestamp(Col("x"), policy).Alias("policy"), TryParseCivilTimestamp(Col("x"), r).Alias("try"), ParseCivilTimestamp(Col("y"), r).Alias("different_input")})
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
	b.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, 19}, Data: []byte("1970-01-01T00:00:00")}
	b.Strings["y"] = &memory.StringViewVector{Offsets: []uint32{0, 19}, Data: []byte("1970-01-01T00:00:01")}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	for i, want := range []int64{0, 0, 0, -3600e9, 0, 0, 1e9} {
		if !out[i].Int64.IsValid(0) || out[i].Int64.Values[0] != want {
			t.Fatal(i, out[i], want)
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}
