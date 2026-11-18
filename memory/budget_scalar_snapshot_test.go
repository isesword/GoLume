package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

var scalarSnapshotTestSink BudgetScalarSnapshot

func TestBudgetScalarSnapshotOperatorsClosedAndNoAlloc(t *testing.T) {
	var nilQuery *QueryBudget
	if got := nilQuery.ScalarSnapshot(); got != (BudgetScalarSnapshot{}) {
		t.Fatalf("nil: %+v", got)
	}
	q, err := NewEngineBudget(1 << 20).BeginQuery(BudgetOptions{SoftLimitBytes: 800000, HardLimitBytes: 900000})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var held []Reservation
	for i := 0; i < 128; i++ {
		r, err := q.Operator(fmt.Sprintf("operator-%d", i), MemoryRetainedState).Reserve(context.Background(), int64(i+1))
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, r)
	}
	defer func() {
		for _, r := range held {
			r.Release()
		}
	}()
	q.RecordSpilledBytes(321)
	want := q.Snapshot()
	got := q.ScalarSnapshot()
	if got.UsedBytes != want.UsedBytes || got.PeakBytes != want.PeakBytes || got.SpilledBytes != 321 || got.SpillRequests != want.SpillRequests || got.SoftLimitBytes != 800000 || got.HardLimitBytes != 900000 || got.Closed {
		t.Fatalf("snapshot mismatch: %+v / %+v", got, want)
	}
	if allocs := testing.AllocsPerRun(1000, func() { scalarSnapshotTestSink = q.ScalarSnapshot() }); allocs != 0 {
		t.Fatalf("allocated %g", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() { scalarSnapshotTestSink = nilQuery.ScalarSnapshot() }); allocs != 0 {
		t.Fatalf("nil allocated %g", allocs)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	closed := q.ScalarSnapshot()
	if !closed.Closed || closed.UsedBytes != got.UsedBytes {
		t.Fatalf("close invalidated live output: %+v", closed)
	}
	for _, r := range held {
		r.Release()
	}
	if end := q.ScalarSnapshot(); end.UsedBytes != 0 || end.PeakBytes != got.PeakBytes || !end.Closed {
		t.Fatalf("release: %+v", end)
	}
	if allocs := testing.AllocsPerRun(1000, func() { scalarSnapshotTestSink = q.ScalarSnapshot() }); allocs != 0 {
		t.Fatalf("closed allocated %g", allocs)
	}
}

func TestBudgetScalarSnapshotConcurrentClose(t *testing.T) {
	q, err := NewEngineBudget(1 << 20).BeginQuery(BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var workers sync.WaitGroup
	start := make(chan struct{})
	ready := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		op := q.Operator(fmt.Sprintf("worker-%d", i), MemoryWorkspace)
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for j := 0; j < 1000; j++ {
				r, e := op.Reserve(context.Background(), 64)
				if e != nil {
					if !errors.Is(e, ErrQueryBudgetClosed) {
						t.Errorf("reserve: %v", e)
					}
					return
				}
				q.RecordSpilledBytes(1)
				r.Release()
				if j == 0 {
					ready <- struct{}{}
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		var prevPeak, prevSpill int64
		seenClosed := false
		for i := 0; i < 2000; i++ {
			s := q.ScalarSnapshot()
			if s.UsedBytes < 0 || s.UsedBytes > s.PeakBytes || s.PeakBytes < prevPeak || s.SpilledBytes < prevSpill || seenClosed && !s.Closed {
				t.Errorf("inconsistent sample: %+v", s)
				return
			}
			prevPeak, prevSpill, seenClosed = s.PeakBytes, s.SpilledBytes, s.Closed
		}
	}()
	close(start)
	for i := 0; i < 4; i++ {
		<-ready
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if s := q.ScalarSnapshot(); !s.Closed || s.UsedBytes != 0 {
		t.Fatalf("final: %+v", s)
	}
}
