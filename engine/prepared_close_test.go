package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEveryConcurrentPreparedCloseWaitsForActiveCleanup(t *testing.T) {
	prepared := &PreparedQuery{}
	if err := prepared.begin(); err != nil {
		t.Fatal(err)
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- prepared.Close() }()
	deadline := time.After(time.Second)
	for {
		prepared.lifeMu.Lock()
		closing := prepared.closed
		prepared.lifeMu.Unlock()
		if closing {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close did not begin")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	go func() { second <- prepared.Close() }()
	select {
	case err := <-second:
		t.Fatalf("second Close returned before active cleanup: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := prepared.begin(); !errors.Is(err, ErrPreparedClosed) {
		t.Fatalf("new execution after Close: %v", err)
	}
	prepared.end()
	for _, done := range []chan error{first, second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not finish after cleanup")
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

// Done is first evaluated by Execute's concurrency-slot select, after begin
// accepted the execution. It provides a deterministic queued-execution barrier.
type queuedExecuteContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *queuedExecuteContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestAcceptedQueuedExecutionCompletesDuringPreparedClose(t *testing.T) {
	for _, kind := range []string{"resident", "sorted", "spill"} {
		t.Run(kind, func(t *testing.T) {
			opts := streamTestOptions()
			src := &generatedStreamSource{rows: 64, groups: 8, sorted: kind == "sorted"}
			query := streamTestQuery()
			if kind != "resident" {
				query.Hints.ForceStrategy = ""
			}
			if kind == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			}
			eng := New(opts)
			defer eng.Close()
			prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{MaxConcurrency: 1})
			if err != nil {
				t.Fatal(err)
			}
			prepared.executions <- struct{}{}
			ctx := &queuedExecuteContext{Context: context.Background(), entered: make(chan struct{})}
			executed := make(chan error, 1)
			go func() {
				result, err := prepared.Execute(ctx, ExecuteOptions{})
				if err == nil && (result.SelectedRows != 64 || result.Checksum != 64 || result.GroupCount != 8) {
					err = fmt.Errorf("result=%+v", result)
				}
				executed <- err
			}()
			<-ctx.entered
			closed := make(chan error, 1)
			go func() { closed <- prepared.Close() }()
			for {
				prepared.lifeMu.Lock()
				closing := prepared.closed
				prepared.lifeMu.Unlock()
				if closing {
					break
				}
				time.Sleep(time.Millisecond)
			}
			<-prepared.executions
			select {
			case err := <-executed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("accepted execution did not finish")
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if eng.Memory().Used() != 0 || src.opened.Load() != src.closed.Load() {
				t.Fatal("Close returned before execution cleanup")
			}
		})
	}
}
