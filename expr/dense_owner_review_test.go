package expr

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
)

func TestDenseOwnerReplacementAdmissionRetry(t *testing.T) {
	oldProgram, err := Compile(livenessSchema(), livenessChain(Col("x"), 8, false))
	if err != nil {
		t.Fatal(err)
	}
	nextProgram, err := Compile(livenessSchema(), livenessChain(Col("x"), 128, false))
	if err != nil {
		t.Fatal(err)
	}
	oldBytes, err := oldProgram.WorkspaceBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	nextBytes, err := nextProgram.WorkspaceBytes(65)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(oldBytes + nextBytes)
	w := &evaluationState{ctx: context.Background(), manager: manager}
	defer w.Close()
	if err = w.prepareProgram(oldProgram.instructions, 1); err != nil {
		t.Fatal(err)
	}
	blocker, err := manager.Reserve(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	if err = w.prepareProgram(nextProgram.instructions, 65); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("replacement admission: %v", err)
	}
	if manager.Used() != oldBytes+1 || w.MemoryBytes() != oldBytes || &w.program[0] != &oldProgram.instructions[0] {
		t.Fatal("failed replacement changed retained program or ownership")
	}
	if err = evaluateInstructions(oldProgram.instructions, livenessBatch(1, false), w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if w.regs[oldProgram.root].i64[0] != 8 {
		t.Fatal("old program unavailable after failed replacement")
	}
	blocker.Release()
	if err = w.prepareProgram(nextProgram.instructions, 65); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != nextBytes || w.MemoryBytes() != nextBytes {
		t.Fatalf("retry accounting: charged=%d retained=%d want=%d", manager.Used(), w.MemoryBytes(), nextBytes)
	}
	if len(w.owned) != storageOwnerCount(nextProgram.instructions) {
		t.Fatal("retry allocated logical-count owners")
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatal("replacement leaked reservations")
	}
}
