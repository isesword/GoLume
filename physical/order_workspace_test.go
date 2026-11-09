package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"sync"
	"testing"
)

func TestSortWorkspaceCreditAndLiveClose(t *testing.T) {
	ctx := context.Background()
	parent := memory.NewBudgetManager(4096)
	workspace := &sortWorkspace{parent: parent}
	if err := workspace.ensure(ctx, 1024); err != nil {
		t.Fatal(err)
	}
	charged := parent.Used()
	first, err := workspace.Reserve(ctx, 600)
	if err != nil {
		t.Fatal(err)
	}
	if parent.Used() != charged || workspace.Used() != 600 {
		t.Fatal("child charge duplicated", parent.Used(), workspace.Used())
	}
	second, err := workspace.Reserve(ctx, 700)
	if err != nil {
		t.Fatal(err)
	}
	if workspace.Limit() != 1300 || parent.Used() != 1300+2*sortWorkspaceCreditOverhead {
		t.Fatal(workspace.Limit(), parent.Used())
	}
	workspace.close()
	if parent.Used() == 0 {
		t.Fatal("Close released live backing credit")
	}
	if _, err = workspace.Reserve(ctx, 1); !errors.Is(err, errSortWorkspaceClosed) {
		t.Fatal(err)
	}
	first.Release()
	first.Release()
	if parent.Used() == 0 {
		t.Fatal("other live child lost backing credit")
	}
	second.Release()
	if parent.Used() != 0 || workspace.Used() != 0 {
		t.Fatal(parent.Used(), workspace.Used())
	}
	workspace.close()
}

func TestSortWorkspaceRejectsUnadmittedGrowthAndCancellation(t *testing.T) {
	ctx := context.Background()
	parent := memory.NewBudgetManager(1024)
	workspace := &sortWorkspace{parent: parent}
	defer workspace.close()
	if err := workspace.ensure(ctx, 512); err != nil {
		t.Fatal(err)
	}
	before := parent.Used()
	if _, err := workspace.Reserve(ctx, 1024); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if parent.Used() != before || workspace.Used() != 0 || workspace.Limit() != 512 {
		t.Fatal("failed growth changed ownership")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := workspace.ensure(cancelled, 600); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := workspace.Reserve(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSortWorkspaceConcurrentTokensAndClose(t *testing.T) {
	for repeat := 0; repeat < 25; repeat++ {
		ctx := context.Background()
		parent := memory.NewBudgetManager(1 << 20)
		workspace := &sortWorkspace{parent: parent}
		if err := workspace.ensure(ctx, 256); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := workspace.Reserve(ctx, 128)
				if err == nil {
					r.Release()
					r.Release()
				} else if !errors.Is(err, errSortWorkspaceClosed) {
					t.Error(err)
				}
			}()
		}
		workspace.close()
		wg.Wait()
		if parent.Used() != 0 || workspace.Used() != 0 {
			t.Fatal(parent.Used(), workspace.Used())
		}
	}
}

func TestSortWorkspaceTrimAndTokenGrowth(t *testing.T) {
	ctx := context.Background()
	parent := memory.NewBudgetManager(32 << 10)
	w := &sortWorkspace{parent: parent}
	defer w.close()
	if err := w.ensure(ctx, 16<<10); err != nil {
		t.Fatal(err)
	}
	lease, err := w.Reserve(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	r := lease.(*sortWorkspaceReservation)
	if err = r.grow(ctx, 6000); err != nil {
		t.Fatal(err)
	}
	w.trim(0)
	if r.Bytes() != 7000 || w.Used() != 7000 || w.Limit() < 7000 || w.Limit() >= 7000+4096 {
		t.Fatal(r.Bytes(), w.Used(), w.Limit())
	}
	if parent.Used() < 7000 || parent.Used() >= 16<<10 {
		t.Fatal(parent.Used())
	}
	r.Release()
	if err = r.grow(ctx, 1); !errors.Is(err, errSortWorkspaceClosed) {
		t.Fatal(err)
	}
	w.trim(0)
	if parent.Used() != 0 {
		t.Fatal(parent.Used())
	}
}

func TestSortWorkspaceRollsBackPartialCredits(t *testing.T) {
	parent := memory.NewBudgetManager(12 << 10)
	w := &sortWorkspace{parent: parent}
	defer w.close()
	if err := w.ensure(context.Background(), 1024); err != nil {
		t.Fatal(err)
	}
	before := parent.Used()
	if err := w.ensure(context.Background(), 16<<10); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if parent.Used() != before || w.Limit() != 1024 {
		t.Fatal("partial growth leaked", parent.Used(), w.Limit())
	}
}

func TestSortWorkspaceConcurrentGrowReleaseClose(t *testing.T) {
	for repeat := 0; repeat < 100; repeat++ {
		parent := memory.NewBudgetManager(64 << 10)
		w := &sortWorkspace{parent: parent}
		r, err := w.Reserve(context.Background(), 32)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			err := r.(*sortWorkspaceReservation).grow(context.Background(), 8192)
			if err != nil && !errors.Is(err, errSortWorkspaceClosed) {
				t.Error(err)
			}
		}()
		go func() { defer wg.Done(); r.Release() }()
		go func() { defer wg.Done(); w.close() }()
		wg.Wait()
		if parent.Used() != 0 || w.Used() != 0 {
			t.Fatal(parent.Used(), w.Used())
		}
	}
}
