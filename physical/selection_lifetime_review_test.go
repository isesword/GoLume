package physical

import (
	"context"
	"errors"
	"testing"

	"gocolumn/memory"
)

func TestSelectionProjectReleasesBorrowedIndicesOnTerminalPaths(t *testing.T) {
	for _, terminal := range []string{"close", "eof", "cancel"} {
		t.Run(terminal, func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			p := selectionProjection(t, selectionFixture(t, 65, 3, 1, false, ""), 33, true, false, manager)
			opts := testReadOptions()
			opts.MemoryManager = manager
			stream, err := p.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			native := stream.(*projectBatchStream)
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("first %t %v", ok, err)
			}
			if len(native.selected) == 0 || native.selection == nil {
				t.Fatal("test did not acquire a selection borrow")
			}
			switch terminal {
			case "close":
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
			case "eof":
				if ok, err := stream.Next(context.Background(), out); ok || err != nil {
					t.Fatalf("EOF %t %v", ok, err)
				}
			case "cancel":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel %t %v", ok, err)
				}
			}
			if native.selected != nil || native.selection != nil || native.selectedCount != 0 {
				t.Fatal("terminal stream retained released filter selection")
			}
			if manager.Used() != 0 {
				t.Fatalf("terminal stream retained %d bytes", manager.Used())
			}
		})
	}
}
