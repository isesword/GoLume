package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

// Exercise the dedicated owners inside guarded regions, not merely as an
// unconditional predicate feeding a guard. Keep the public Bool output path too.
func TestN1IndependentGuardedOwnersAccounting(t *testing.T) {
	schema, _ := structGetFixture(structGetKinds[10], 0, true, true)
	get := StructGet(Col("r"), "a.b:数")
	rebuilt := StructGet(StructFrom(get.Alias("kept")), "kept")
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			p, err := CompileExpressions(schema, []NamedExpr{
				If(get, rebuilt, Bool(false)).Alias("guarded"),
				If(get, Bool(false), If(Bool(true), rebuilt, Bool(true))).Alias("nested"),
				get.Alias("public"),
			})
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for step, n := range []int{1, 65, 513, 65, 65, 0, 1} {
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
					truth := valid && values.Values[row] != 0
					want := uint32(0)
					if truth {
						want = 1
					}
					if !out[0].Uint32.IsValid(row) || out[0].Uint32.Values[row] != want {
						t.Fatalf("guarded step%d row%d", step, row)
					}
					// In the false arm, reconstructing a null source remains null.
					if out[1].Uint32.IsValid(row) != valid || valid && out[1].Uint32.Values[row] != 0 {
						t.Fatalf("nested step%d row%d", step, row)
					}
					if out[2].Uint32.IsValid(row) != valid || valid && out[2].Uint32.Values[row] != values.Values[row] {
						t.Fatalf("public step%d row%d", step, row)
					}
				}
				if manager.Used() != w.MemoryBytes() {
					t.Fatalf("guard accounting step%d used%d bytes%d", step, manager.Used(), w.MemoryBytes())
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("guard Close balance")
			}
		})
	}
}
