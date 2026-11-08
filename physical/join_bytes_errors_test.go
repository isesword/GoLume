package physical

import (
	"context"
	"errors"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

func TestReviewGenericChildClosedErrorCleanup(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	closeErr := errors.New("child close")
	src := genericJoinSource(t, []genericJoinRow{{s: "x"}}, false, 1)
	left := &filterProbeSource{schema: src.Schema()}
	left.next = func(context.Context, *memory.Batch) (bool, error) { return false, source.ErrBatchStreamClosed }
	left.close = func() error { return closeErr }
	owner, err := NewJoinSource(ctx, left, src, join.Inner, []JoinKey{{"s", "s"}}, "", nil, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	stream, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, err = stream.Next(ctx, memory.NewBatch())
	if !errors.Is(err, source.ErrBatchStreamClosed) || !errors.Is(err, closeErr) || left.closes != 1 {
		t.Fatalf("child closed sentinel skips terminal cleanup: err=%v closes=%d", err, left.closes)
	}
}
func TestReviewGenericMatchedAdmissionKeepsCloseError(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	closeErr := errors.New("left cleanup failure")
	src := genericJoinSource(t, []genericJoinRow{{s: "x"}, {s: "y"}}, false, 1)
	left := &filterProbeSource{schema: src.Schema(), close: func() error { return closeErr }}
	left.next = func(context.Context, *memory.Batch) (bool, error) { return false, nil }
	owner, err := NewJoinSource(ctx, left, src, join.Right, []JoinKey{{"s", "s"}}, "", nil, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	q, _ := memory.NewEngineBudget(1).BeginQuery(memory.BudgetOptions{HardLimitBytes: 1})
	defer q.Close()
	ctx = memory.WithQueryBudget(ctx, q)
	_, err = owner.NewBatchStream(ctx, source.ReadOptions{Kernels: k})
	if !errors.Is(err, memory.ErrBudgetExceeded) || !errors.Is(err, closeErr) || left.closes != 1 {
		t.Fatalf("matched-state admission dropped cleanup error: err=%v closes=%d", err, left.closes)
	}
}
