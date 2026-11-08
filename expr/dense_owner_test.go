package expr

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	"gocolumn/memory"
)

func TestDensePhysicalOwnerMetadata(t *testing.T) {
	for _, boolean := range []bool{false, true} {
		t.Run(fmt.Sprint(boolean), func(t *testing.T) {
			input := Col("x")
			if boolean {
				input = Col("c")
			}
			p, err := Compile(livenessSchema(), livenessChain(input, 128, boolean))
			if err != nil {
				t.Fatal(err)
			}
			seen := map[int]bool{}
			for i := range p.instructions {
				owner := storageIndex(p.instructions, i)
				if !seen[owner] && owner != len(seen) {
					t.Fatalf("instruction %d introduces non-dense owner %d after %d owners", i, owner, len(seen))
				}
				seen[owner] = true
			}
			owners := storageOwnerCount(p.instructions)
			if owners != len(seen) || owners > 4 {
				t.Fatalf("chain uses %d owners for %d instructions", owners, len(p.instructions))
			}
			wantMetadata := int64(len(p.instructions))*int64(unsafe.Sizeof((*scalarRegister)(nil))) + int64(owners+borrowedRegisterCount(p.instructions))*int64(unsafe.Sizeof(scalarRegister{}))
			if got, err := p.WorkspaceBytes(0); err != nil || got != wantMetadata {
				t.Fatalf("zero-row metadata = %d, %v; want %d", got, err, wantMetadata)
			}
			manager := memory.NewBudgetManager(0)
			w := &evaluationState{ctx: context.Background(), manager: manager}
			for _, rows := range []int{0, 65, 1, 129, 0} {
				if err := evaluateInstructions(p.instructions, livenessBatch(rows, true), w, testKernelSet()); err != nil {
					t.Fatal(err)
				}
				if len(w.owned) != owners || cap(w.owned) != owners || len(w.regs) != len(p.instructions) {
					t.Fatalf("register capacities: logical=%d physical=%d want=%d", cap(w.regs), cap(w.owned), owners)
				}
				if got := manager.Used(); got != w.MemoryBytes() {
					t.Fatalf("charged %d retained %d", got, w.MemoryBytes())
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatalf("close leaked %d", manager.Used())
			}
		})
	}
}

func TestDenseOwnersDedicatedProgramCompatibility(t *testing.T) {
	p, err := Compile(livenessSchema(), livenessChain(Col("x"), 32, false))
	if err != nil {
		t.Fatal(err)
	}
	for i := range p.instructions {
		p.instructions[i].storageOwner = 0
	}
	if got := storageOwnerCount(p.instructions); got != len(p.instructions) {
		t.Fatalf("unplanned owners %d", got)
	}
	w := &evaluationState{}
	defer w.Close()
	if err := evaluateInstructions(p.instructions, livenessBatch(65, false), w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if len(w.owned) != len(p.instructions) {
		t.Fatal("unplanned instructions must retain dedicated owners")
	}
	if got := w.regs[p.root].i64[1]; got != 33 {
		t.Fatalf("result %d", got)
	}
}

func TestLogicalRegisterMetadataScalesByPointers(t *testing.T) {
	shallow, err := Compile(livenessSchema(), livenessChain(Col("x"), 8, false))
	if err != nil {
		t.Fatal(err)
	}
	deep, err := Compile(livenessSchema(), livenessChain(Col("x"), 128, false))
	if err != nil {
		t.Fatal(err)
	}
	if storageOwnerCount(shallow.instructions) != storageOwnerCount(deep.instructions) || borrowedRegisterCount(shallow.instructions) != borrowedRegisterCount(deep.instructions) {
		t.Fatal("fixture changed owner or input counts")
	}
	a, err := shallow.WorkspaceBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := deep.WorkspaceBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(len(deep.instructions)-len(shallow.instructions)) * int64(unsafe.Sizeof((*scalarRegister)(nil)))
	if b-a != want {
		t.Fatalf("additional logical metadata = %d, want %d", b-a, want)
	}
}
