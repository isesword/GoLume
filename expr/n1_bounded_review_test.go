package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestN1BoundedMetadataRejections(t *testing.T) {
	scalar := memory.Field{Name: "x", Type: memory.TypeInt64, Children: []memory.Field{{Name: "hidden", Type: memory.TypeInt64}}}
	list := memory.ListInt64Field("xs", true, true)
	list.Children[0].Children = []memory.Field{{Name: "hidden", Type: memory.TypeInt64}}
	decimal, _ := memory.Decimal128Field("d", false, 10, 2)
	for _, child := range []memory.Field{scalar, list, decimal} {
		parent := memory.StructField("r", true, child)
		if _, err := Compile(memory.NewSchema(parent), StructGet(Col("r"), child.Name)); err == nil {
			t.Fatalf("unsupported bounded child accepted: %+v", child)
		}
	}
	// ListGet must not become a route around bounded admission.
	if _, err := Compile(memory.NewSchema(list), ListGet(Col("xs"), I64(0))); err == nil {
		t.Fatal("primitive ListGet accepted recursive metadata")
	}
	if _, err := Compile(memory.NewSchema(decimal), StructFrom(Col("d").Alias("d"))); err == nil {
		t.Fatal("top-level Decimal support leaked into StructFrom")
	}
}

func TestN1StructGetBoolCacheReuseAndAccounting(t *testing.T) {
	schema, _ := structGetFixture(structGetKinds[10], 0, true, true)
	get := StructGet(Col("r"), "a.b:数")
	p, err := CompileExpressions(schema, []NamedExpr{get.Alias("a"), get.Alias("copy"), Named(If(get, Bool(false), Bool(true)), "negated")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("Bool CSE not shared")
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var warmBytes int64
			for step, n := range []int{0, 1, 65, 513, 65, 65, 65, 1, 0} {
				_, batch := structGetFixture(structGetKinds[10], n, true, true)
				parent := batch.Structs["r"]
				values := parent.Values.Uint32s["a.b:数"]
				for row := 0; row < n; row++ {
					values.Values[row] = uint32((row + step) % 2)
				}
				out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < n; row++ {
					valid := parent.IsValid(row) && values.IsValid(row)
					for col := 0; col < 2; col++ {
						if out[col].Uint32.IsValid(row) != valid || valid && out[col].Uint32.Values[row] != values.Values[row] {
							t.Fatalf("stale Bool cache step%d row%d col%d", step, row, col)
						}
					}
					want := uint32(1)
					if valid && values.Values[row] != 0 {
						want = 0
					}
					if !out[2].Uint32.IsValid(row) || out[2].Uint32.Values[row] != want {
						t.Fatalf("Bool guard result step%d row%d", step, row)
					}
				}
				if n > 0 && &out[0].Uint32.Values[0] != &out[1].Uint32.Values[0] {
					t.Fatal("duplicate Bool output cache not shared")
				}
				if manager.Used() != w.MemoryBytes() {
					t.Fatalf("Bool accounting step%d used%d workspace%d", step, manager.Used(), w.MemoryBytes())
				}
				if step == 4 {
					warmBytes = manager.Used()
				}
				if step == 5 || step == 6 {
					if manager.Used() != warmBytes {
						t.Fatalf("same-size Bool cache keeps allocating: step%d used%d warm%d", step, manager.Used(), warmBytes)
					}
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("Bool Close balance")
			}
		})
	}
}
