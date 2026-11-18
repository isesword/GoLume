package expr

import (
	"gocolumn/memory"
	"testing"
)

// A directly executed all-inactive constructor still checks structural storage.
// This does not change public short circuiting of entirely unexecuted branches.
func TestN7PureContainerAllInactiveStructuralBoundary(t *testing.T) {
	in, field := n7StringInput(128)
	manager := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	out := n7Output(manager)
	active := make(memory.Bitmap, memory.WordsFor(4))
	if err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{field}, 4, active); err != nil {
		t.Fatal(err)
	}
	old, used := out.strct, manager.Used()
	for row := 0; row < 4; row++ {
		if old.IsValid(row) {
			t.Fatal("inactive parent published as valid")
		}
	}
	in.strct.Values.Strings["s"].Offsets[4] = 999999
	if err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{field}, 4, active); err == nil || out.strct != old || manager.Used() != used {
		t.Fatal("all-inactive malformed storage bypassed or replaced owner", err)
	}
	out.structFromReservation.Release()
	if manager.Used() != 0 {
		t.Fatal("owner leak")
	}
}
