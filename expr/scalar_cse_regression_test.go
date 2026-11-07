package expr

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestProjectionCSEFramesColumnNames(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "a:c:b", Type: memory.TypeInt64},
		memory.Field{Name: "c", Type: memory.TypeInt64},
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b:c:c", Type: memory.TypeInt64},
	)
	left := Add(Col("a:c:b"), Col("c"))
	right := Add(Col("a"), Col("b:c:c"))
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(left, "left"), Named(right, "right"), Named(left, "shared"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root == p.outputs[1].root {
		t.Fatal("distinct expressions share a register")
	}
	if p.outputs[0].root != p.outputs[2].root {
		t.Fatal("identical expressions no longer share a register")
	}
	batch := memory.NewBatch()
	batch.Int64s["a:c:b"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Int64s["c"] = &memory.Int64Vector{Values: []int64{2}}
	batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{10}}
	batch.Int64s["b:c:c"] = &memory.Int64Vector{Values: []int64{20}}
	batch.Reset(1)
	var w evaluationState
	defer w.Close()
	if err := evaluateInstructions(p.instructions, batch, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{3, 30, 3} {
		if got := w.regs[p.outputs[i].root].i64[0]; got != want {
			t.Fatalf("output %d = %d, want %d", i, got, want)
		}
	}
}

func TestScalarCSEPreservesFloatBits(t *testing.T) {
	values := []float64{0, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002)}
	c := newScalarCompiler(memory.NewSchema())
	seen := make(map[int]bool)
	for _, value := range values {
		root, _, err := c.compile(F64(value))
		if err != nil {
			t.Fatal(err)
		}
		if seen[root] {
			t.Fatalf("float bits %x incorrectly share a register", math.Float64bits(value))
		}
		seen[root] = true
		if got := math.Float64bits(c.instructions[root].F64); got != math.Float64bits(value) {
			t.Fatalf("literal bits = %x, want %x", got, math.Float64bits(value))
		}
	}
}

func TestScalarCSEIncludesJSONChildSchema(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "json", Type: memory.TypeString})
	c := newScalarCompiler(schema)
	optional := JSONDecode(Col("json"), memory.NewSchema(memory.ListStringField("items", false, true)))
	required := JSONDecode(Col("json"), memory.NewSchema(memory.ListStringField("items", false, false)))
	a, _, err := c.compile(optional)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := c.compile(required)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("JSON decoders with different child nullability share a register")
	}
	if c.instructions[a].A != c.instructions[b].A {
		t.Fatal("JSON decoders should still share the input column")
	}
}

func TestScalarCSEFramesJSONFields(t *testing.T) {
	// The old key encoded both schemas as :a:int64:false:b:int64:false.
	one := JSONDecode(Col("json"), memory.NewSchema(memory.Field{Name: "a:int64:false:b", Type: memory.TypeInt64}))
	two := JSONDecode(Col("json"), memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt64},
		memory.Field{Name: "b", Type: memory.TypeInt64},
	))
	if scalarExprKey(one) == scalarExprKey(two) {
		t.Fatal("JSON schemas with different field boundaries have the same key")
	}
}
