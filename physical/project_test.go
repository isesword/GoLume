package physical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func TestProjectSourceWithColumnsAndNullPropagation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt64})
	b := memory.NewBatch()
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	b.Int64s["a"] = &memory.Int64Vector{Values: []int64{2, 99, 4}, Validity: valid}
	b.Int64s["b"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	b.Reset(3)
	src, _ := inmemory.New(schema, b)
	ps, err := NewProjectSource(src, []expr.NamedExpr{expr.Col("a").Mul(expr.Lit(2)).Add(expr.Col("b")).Alias("score")}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := openTestReader(context.Background(), ps, testReadOptions())
	defer r.Close()
	parts, _ := ps.Partitions(context.Background())
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	v, err := out.Int64("score")
	if err != nil {
		t.Fatal(err)
	}
	if v.Values[0] != 5 || v.Values[2] != 11 || v.IsValid(1) {
		t.Fatalf("score=%v validity=%v", v.Values, v.Validity)
	}
	if _, err := out.Int64("a"); err != nil {
		t.Fatal("WithColumns dropped input")
	}
}
