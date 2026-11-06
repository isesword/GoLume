package expr

import (
	"context"
	"testing"

	"gocolumn/memory"
)

func TestScalarWorkspaceCloseDropsAllRegisters(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	w := &ScalarWorkspace{}
	w.SetMemoryManager(context.Background(), manager)
	w.Ensure(1, 2)
	if err := w.reserve(128); err != nil {
		t.Fatal(err)
	}
	w.regs[0].i64 = make([]int64, 8)
	w.regs[1].data = make([]byte, 64)
	// A shorter subsequent program must not hide retained registers from Close.
	w.Ensure(1, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.regs != nil || w.reservations != nil || manager.Used() != 0 {
		t.Fatalf("Close retained buffers or reservations: regs=%d used=%d", cap(w.regs), manager.Used())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w.Ensure(1, 1)
	before := manager.Used()
	if err := w.reserve(64); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != before+64 {
		t.Fatalf("reused workspace charge=%d, want metadata plus 64", manager.Used())
	}
	_ = w.Close()
}
