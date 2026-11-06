package engine

import (
	"context"
	"reflect"
	"testing"

	"gocolumn/memory"
)

func TestQueryWorkspaceCloseDropsRetainedState(t *testing.T) {
	manager := memory.NewBudgetManager(1024)
	r, err := manager.Reserve(context.Background(), 128)
	if err != nil {
		t.Fatal(err)
	}
	w := &queryWorkspace{
		owner:        &PreparedQuery{},
		workers:      make([]workerWorkspace, 1),
		reservations: []memory.Reservation{r},
		selectedKeys: make([]uint64, 8),
		selectedVals: make([]int64, 8),
	}
	w.workers[0].batch = memory.NewBatch()
	// Model an idle pool retaining the workspace after its owner is closed.
	pool := newWorkspacePool(1, nil)
	pool.all[w] = struct{}{}
	pool.idle <- w
	if err := pool.close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 || !reflect.DeepEqual(*w, queryWorkspace{}) {
		t.Fatalf("closed workspace retained state: used=%d", manager.Used())
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
}
