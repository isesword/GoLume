package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
)

func TestJoinScratchRejectsBeforeAllocation(t *testing.T) {
	manager := memory.NewBudgetManager(1)
	matched, reservation, err := newJoinMatchedRows(context.Background(), manager, 1024)
	if !errors.Is(err, memory.ErrBudgetExceeded) || matched != nil || reservation != nil {
		t.Fatalf("matched scratch: len=%d reservation=%v err=%v", len(matched), reservation, err)
	}
	keys, masks, reservation, err := newJoinProbeKeys(context.Background(), manager, 1024)
	if !errors.Is(err, memory.ErrBudgetExceeded) || keys != nil || masks != nil || reservation != nil {
		t.Fatalf("probe scratch: keys=%d masks=%d reservation=%v err=%v", len(keys), len(masks), reservation, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("failed reservation leaked %d bytes", manager.Used())
	}
	// A budget error alone is insufficient: the rejected path must not first
	// allocate arrays proportional to the partition and then discard them.
	allocations := testing.AllocsPerRun(10, func() {
		_, _, _ = newJoinMatchedRows(context.Background(), manager, 1024)
		_, _, _, _ = newJoinProbeKeys(context.Background(), manager, 1024)
	})
	if allocations != 0 {
		t.Fatalf("rejected scratch allocated %g objects", allocations)
	}
}

func TestJoinScratchReservationLifetime(t *testing.T) {
	manager := memory.NewBudgetManager(36)
	matched, matchedReservation, err := newJoinMatchedRows(context.Background(), manager, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer matchedReservation.Release()
	keys, masks, scratch, err := newJoinProbeKeys(context.Background(), manager, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer scratch.Release()
	if len(matched) != 2 || len(keys) != 2 || len(masks) != 2 || manager.Used() != 36 {
		t.Fatalf("scratch lengths/accounting: matched=%d keys=%d masks=%d used=%d", len(matched), len(keys), len(masks), manager.Used())
	}
	scratch.Release()
	if manager.Used() != 2 {
		t.Fatalf("after probe release: used=%d", manager.Used())
	}
	matchedReservation.Release()
	if manager.Used() != 0 {
		t.Fatalf("after all releases: used=%d", manager.Used())
	}
}

func TestJoinScratchRejectsInvalidRows(t *testing.T) {
	manager := memory.NewBudgetManager(1)
	if _, _, err := newJoinMatchedRows(context.Background(), manager, -1); err == nil {
		t.Fatal("negative matched row count accepted")
	}
	if _, _, _, err := newJoinProbeKeys(context.Background(), manager, -1); err == nil {
		t.Fatal("negative probe row count accepted")
	}
	if manager.Used() != 0 {
		t.Fatalf("invalid rows leaked %d bytes", manager.Used())
	}
}
