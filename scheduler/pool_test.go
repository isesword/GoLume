package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestPoolParallelFor(t *testing.T) {
	p := NewPool(4)
	defer p.Close()
	var sum atomic.Int64
	if err := p.ParallelFor(context.Background(), 100, 4, func(i int) error { sum.Add(int64(i + 1)); return nil }); err != nil {
		t.Fatal(err)
	}
	if got, want := sum.Load(), int64(5050); got != want {
		t.Fatalf("sum=%d want=%d", got, want)
	}
}
