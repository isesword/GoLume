package expr

import (
	"context"
	"testing"

	"gocolumn/memory"
)

func TestStructuralInternerExactFieldMetadata(t *testing.T) {
	base := memory.Field{Name: "value", Type: memory.TypeString}
	variants := []memory.Field{base, base, base, base, base, base, base}
	variants[0].Name = "value:1:2"
	variants[1].Type = memory.TypeInt64
	variants[2].Nullable = true
	variants[3].Encoding = memory.EncodingView
	variants[4].DictionaryDomain = "domain:1:2"
	variants[5].ArrowType = "utf8"
	variants[6].Children = []memory.Field{{Name: "nested", Type: memory.TypeString}}
	s := newStructuralInterner()
	baseID := s.field(base)
	seen := map[int]bool{baseID: true}
	for _, field := range variants {
		id := s.field(field)
		if seen[id] {
			t.Fatalf("distinct field metadata merged: %#v", field)
		}
		seen[id] = true
		if s.field(field) != id {
			t.Fatal("identical metadata did not intern")
		}
	}
	a := memory.Field{Name: "parent", Type: memory.TypeStruct, Children: []memory.Field{base, variants[0]}}
	b := a
	b.Children = []memory.Field{variants[0], base}
	if s.field(a) == s.field(b) {
		t.Fatal("ordered child fields merged")
	}
	b = a
	b.Children = []memory.Field{variants[2], variants[0]}
	if s.field(a) == s.field(b) {
		t.Fatal("nested nullability merged")
	}
}

func TestStructuralInternerIndependentTreesAndCompactKeys(t *testing.T) {
	const depth = 512
	s := newStructuralInterner()
	bind := func(e Expr) *BoundExpr {
		b, err := Bind(livenessSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	first := s.expression(bind(livenessChain(Col("x"), depth, false)))
	keys := len(s.keys)
	if got := s.expression(bind(livenessChain(Col("x"), depth, false))); got != first {
		t.Fatal("separately bound equal tree has another ID")
	}
	if len(s.keys) != keys {
		t.Fatal("duplicate tree expanded structural table")
	}
	// The longest descriptor names only its own node and integer child IDs.
	// Recursive whole-subtree serialization would exceed this bound by orders
	// of magnitude. This is a size invariant, not a wall-clock performance test.
	for key := range s.keys {
		if len(key) > 128 {
			t.Fatalf("non-local descriptor has %d bytes", len(key))
		}
	}
	if s.expression(bind(Sub(Col("x"), I64(1)))) == s.expression(bind(Sub(I64(1), Col("x")))) {
		t.Fatal("ordered operands merged")
	}
}

func TestStructuralInternerDoesNotShareRegistersAcrossRegions(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	partial := CastTo(Col("s"), memory.TypeTimestamp)
	safe := CastTo(I64(7), memory.TypeTimestamp)
	// The fallible tree is interned first in a dead region, then independently
	// in a live region. A global register memo would suppress or misroute it.
	p, err := CompileExpressions(schema, []NamedExpr{
		Named(If(Bool(false), partial, safe), "dead"),
		Named(If(Bool(true), partial, safe), "live"),
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Strings["s"] = testStringVector([]string{"invalid timestamp"}, nil)
	w, err := NewWorkspace(context.Background(), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), batch, testKernelSet()); err == nil {
		t.Fatal("live region lost its fallible expression")
	}
}
