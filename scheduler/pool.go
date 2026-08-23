package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrClosed is returned when work is submitted after a scheduler has been closed.
var ErrClosed = errors.New("scheduler closed")

// Scheduler executes independent integer-indexed tasks with bounded parallelism.
type Scheduler interface {
	MaxWorkers() int
	ParallelFor(ctx context.Context, tasks, parallelism int, fn func(task int) error) error
	Close() error
}

type job struct {
	ctx   context.Context
	fn    func() error
	state *jobState
}

type jobState struct {
	wg   sync.WaitGroup
	once sync.Once
	err  error
}

// Pool is a persistent worker pool used to avoid per-operator goroutine creation.
// A Pool is safe for concurrent ParallelFor calls.
type Pool struct {
	workers int
	queue   chan job
	closed  atomic.Bool
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// NewPool starts a persistent scheduler with workers goroutines.
func NewPool(workers int) *Pool {
	if workers < 1 {
		workers = 1
	}
	p := &Pool{workers: workers, queue: make(chan job, workers*8)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for j := range p.queue {
		if j.ctx.Err() == nil {
			if err := j.fn(); err != nil {
				j.state.once.Do(func() { j.state.err = err })
			}
		} else {
			j.state.once.Do(func() { j.state.err = j.ctx.Err() })
		}
		j.state.wg.Done()
	}
}

// MaxWorkers returns the maximum parallelism supported by the pool.
func (p *Pool) MaxWorkers() int { return p.workers }

// ParallelFor executes fn once for each task index in [0, tasks) with bounded parallelism.
// It returns the first observed task or context error after all submitted worker loops finish.
func (p *Pool) ParallelFor(ctx context.Context, tasks, parallelism int, fn func(task int) error) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if tasks <= 0 {
		return nil
	}
	if p.closed.Load() {
		return ErrClosed
	}
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > p.workers {
		parallelism = p.workers
	}
	if parallelism > tasks {
		parallelism = tasks
	}

	// Use parallel worker loops rather than enqueueing every tiny task.
	var next atomic.Int64
	state := &jobState{}
	state.wg.Add(parallelism)
	for w := 0; w < parallelism; w++ {
		j := job{ctx: ctx, state: state}
		j.fn = func() error {
			for {
				i := int(next.Add(1) - 1)
				if i >= tasks {
					return nil
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := fn(i); err != nil {
					return fmt.Errorf("task %d: %w", i, err)
				}
			}
		}
		select {
		case p.queue <- j:
		case <-ctx.Done():
			state.wg.Done()
			state.once.Do(func() { state.err = ctx.Err() })
		}
	}
	state.wg.Wait()
	return state.err
}

// Close stops all persistent workers after in-flight ParallelFor calls release the pool read lock.
// Close is idempotent.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.CompareAndSwap(false, true) {
		close(p.queue)
		p.wg.Wait()
	}
	return nil
}
