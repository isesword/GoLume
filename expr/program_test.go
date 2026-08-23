package expr

import (
	"gocolumn/memory"
	"testing"
)

func TestProgramThreeValuedLogic(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true})
	// x=[5,15,NULL,35]
	valid := make(memory.Bitmap, 1)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 3)
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{5, 15, 0, 35}, Validity: valid}
	p, _, err := CompilePredicate(schema, And(GT(Col("x"), I64(10)), LT(Col("x"), I64(40))))
	if err != nil {
		t.Fatal(err)
	}
	var w EvalWorkspace
	v, err := p.Evaluate(b, &w, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(v.Truth, 1) || !memory.IsSet(v.Truth, 3) || memory.CountBitmap(v.Truth) != 2 {
		t.Fatalf("truth=%064b", v.Truth[0])
	}
	if memory.IsSet(v.Validity, 2) {
		t.Fatalf("NULL comparison must remain unknown")
	}
	p2, _, err := CompilePredicate(schema, Or(IsNull(Col("x")), GT(Col("x"), I64(30))))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := p2.Evaluate(b, &w, nil)
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(v2.Truth) != 2 || !memory.IsSet(v2.Truth, 2) || !memory.IsSet(v2.Truth, 3) {
		t.Fatalf("OR truth=%064b", v2.Truth[0])
	}
	for i := 0; i < 4; i++ {
		if !memory.IsSet(v2.Validity, i) {
			t.Fatalf("IS NULL OR comparison should be known at row %d", i)
		}
	}
}

func TestCompilePredicateNilMeansAllRows(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	program, simple, err := CompilePredicate(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if simple != nil || program.Complexity != 1 {
		t.Fatalf("program=%+v simple=%+v", program, simple)
	}
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	var workspace EvalWorkspace
	out, err := program.Evaluate(batch, &workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	if memory.CountBitmap(out.Truth) != 3 || memory.CountBitmap(out.Validity) != 3 {
		t.Fatalf("truth=%v validity=%v", out.Truth, out.Validity)
	}
}

func TestOpaqueColumnNullPredicate(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "nested", Type: memory.TypeArrow, Nullable: true, ArrowType: "list<item: int32>"})
	program, _, err := CompilePredicate(schema, IsNotNull(Col("nested")))
	if err != nil {
		t.Fatal(err)
	}
	valid := memory.EnsureBitmap(nil, 3)
	memory.Set(valid, 0)
	memory.Set(valid, 2)
	b := memory.NewBatch()
	b.Rows = 3
	b.Opaque["nested"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeArrow, Validity: valid, Value: struct{}{}}
	var ws EvalWorkspace
	got, err := program.Evaluate(b, &ws, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !memory.IsSet(got.Truth, 0) || memory.IsSet(got.Truth, 1) || !memory.IsSet(got.Truth, 2) {
		t.Fatalf("truth=%v", got.Truth)
	}
}
