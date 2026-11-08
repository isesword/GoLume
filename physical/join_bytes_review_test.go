package physical

import (
	"context"
	"errors"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

func TestReviewGenericQueryScratchCharge(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	left := genericJoinSource(t, []genericJoinRow{{s: strings.Repeat("x", 4096)}}, false, 1)
	right := genericJoinSource(t, []genericJoinRow{{s: "r"}}, false, 1)
	owner, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{"s", "s"}}, "", nil, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	q, _ := memory.NewEngineBudget(1200).BeginQuery(memory.BudgetOptions{HardLimitBytes: 1200})
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	stream, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	ok, err := stream.Next(ctx, out)
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("4096-byte probe scratch under 1200-byte hard limit accepted: ok=%v rows=%d err=%v peak=%d", ok, out.Rows, err, q.Snapshot().PeakBytes)
	}
}

func TestReviewGenericOutputInvalidated(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	m := memory.NewBudgetManager(1 << 20)
	src := genericJoinSource(t, []genericJoinRow{{s: "x"}}, false, 1)
	owner, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"s", "s"}}, "", m, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	base := m.Used()
	stream, err := owner.NewBatchStream(context.Background(), source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatal(ok, err)
	}
	stream.Close()
	if out.Rows != 0 && m.Used() == base {
		t.Fatalf("Close returned output/pair charge but kept %d rows live", out.Rows)
	}
}

func TestReviewGenericErrorTerminal(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	m := memory.NewBudgetManager(1 << 20)
	src := genericJoinSource(t, []genericJoinRow{{s: strings.Repeat("z", 1024)}}, false, 1)
	owner, err := NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", m, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	hold, err := m.Reserve(ctx, m.Limit()-m.Used()-3000)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	_, first := stream.Next(ctx, memory.NewBatch())
	if !errors.Is(first, memory.ErrBudgetExceeded) {
		t.Fatal(first)
	}
	hold.Release()
	ok, again := stream.Next(ctx, memory.NewBatch())
	if again != first {
		t.Fatalf("failed stream silently advances past consumed rows: next=(%v,%v), original=%v", ok, again, first)
	}
}

func TestReviewGenericOuterCancel(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	l := genericJoinSource(t, nil, false, 1)
	rows := make([]genericJoinRow, 4097)
	for i := range rows {
		rows[i].s = "right"
	}
	r := genericJoinSource(t, rows, false, 4097)
	owner, err := NewJoinSource(ctx, l, r, join.Right, []JoinKey{{"s", "s"}}, "", nil, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	stream, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(ctx, out); !ok || err != nil || out.Rows != 4096 {
		t.Fatal(ok, err, out.Rows)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	ok, err := stream.Next(canceled, out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled unmatched-right output emitted: ok=%v rows=%d err=%v", ok, out.Rows, err)
	}
}

func TestReviewGenericDictionaryReleased(t *testing.T) {
	ctx := context.Background()
	k := kernel.New(kernel.ModeScalar)
	m := memory.NewBudgetManager(1 << 20)
	l := genericJoinSource(t, []genericJoinRow{{s: "key"}}, false, 1)
	r := genericJoinSource(t, []genericJoinRow{{s: "key"}}, true, 1)
	owner, err := NewJoinSource(ctx, l, r, join.Inner, []JoinKey{{"s", "s"}}, "", m, k)
	if err != nil {
		t.Fatal(err)
	}
	owner.Close()
	for _, b := range owner.bindings {
		if len(b.dict.Data) != 0 {
			t.Fatalf("closed JoinSource retains %d newly copied dictionary bytes with Used=%d", len(b.dict.Data), m.Used())
		}
	}
}

func TestReviewGenericQueryContextPinned(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	src := genericJoinSource(t, []genericJoinRow{{s: "x"}, {s: "y"}}, false, 1)
	owner, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"s", "s"}}, "", nil, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	engine := memory.NewEngineBudget(1 << 20)
	q1, _ := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
	defer q1.Close()
	q2, _ := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
	defer q2.Close()
	stream, err := owner.NewBatchStream(memory.WithQueryBudget(context.Background(), q1), source.ReadOptions{Kernels: k, MemoryManager: memory.NewBudgetManager(0)})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if q1.Snapshot().UsedBytes == 0 {
		t.Fatal("lost opening query budget on plain Next context")
	}
	_, err = stream.Next(memory.WithQueryBudget(context.Background(), q2), out)
	if err == nil || !strings.Contains(err.Error(), "cannot change query ownership") {
		t.Fatal(err)
	}
	if q1.Snapshot().UsedBytes != 0 || q2.Snapshot().UsedBytes != 0 || out.Rows != 0 {
		t.Fatalf("query drift leaked: %d %d rows=%d", q1.Snapshot().UsedBytes, q2.Snapshot().UsedBytes, out.Rows)
	}
}

func TestReviewGenericNilDestinationCleansOutput(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	m := memory.NewBudgetManager(1 << 20)
	src := genericJoinSource(t, []genericJoinRow{{s: "x"}}, false, 1)
	owner, err := NewJoinSource(context.Background(), src, src, join.Inner, []JoinKey{{"s", "s"}}, "", m, k)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	base := m.Used()
	stream, err := owner.NewBatchStream(context.Background(), source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if _, err := stream.Next(context.Background(), nil); err == nil {
		t.Fatal("nil dst accepted")
	}
	if out.Rows != 0 || m.Used() != base {
		t.Fatalf("nil dst retained rows=%d charge=%d", out.Rows, m.Used()-base)
	}
}

func TestReviewGenericBuildContextBudgetWins(t *testing.T) {
	k := kernel.New(kernel.ModeScalar)
	src := genericJoinSource(t, []genericJoinRow{{s: strings.Repeat("x", 4096)}}, false, 1)
	for _, manager := range []memory.Manager{nil, memory.NewBudgetManager(0)} {
		q, err := memory.NewEngineBudget(1).BeginQuery(memory.BudgetOptions{HardLimitBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), q)
		owner, err := NewJoinSource(ctx, src, src, join.Inner, []JoinKey{{"s", "s"}}, "", manager, k)
		if owner != nil {
			owner.Close()
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("preparation bypassed query budget: %v", err)
		}
		if q.Snapshot().UsedBytes != 0 {
			t.Fatal("preparation leaked", q.Snapshot())
		}
		q.Close()
	}
}
