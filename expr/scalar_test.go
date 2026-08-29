package expr

import (
	"gocolumn/memory"
	"testing"
)

func TestScalarProgramArithmeticAndNulls(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true},
		memory.Field{Name: "b", Type: memory.TypeInt64},
	)
	p, err := CompileScalar(schema, Col("a").Mul(Lit(2)).Add(Col("b")))
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{2, 100, 4}, Validity: valid}
	batch.Int64s["b"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	batch.Reset(3)
	var w ScalarWorkspace
	got, err := p.EvaluateInt64(batch, &w, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got.Values[0] != 5 || got.Values[2] != 11 {
		t.Fatalf("values=%v", got.Values)
	}
	if got.IsValid(1) {
		t.Fatalf("NULL input must propagate")
	}
}
