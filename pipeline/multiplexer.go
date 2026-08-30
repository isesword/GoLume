package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// OpenFanOut opens independent rescans for stable sources and a single-scan,
// byte-bounded multiplexer for unknown/non-replayable sources.
func OpenFanOut(ctx context.Context, src source.Source, readOpts source.ReadOptions, parts []source.Partition, manager memory.Manager, opts Options, branches int, sink observer.Observer) ([]source.BatchStream, error) {
	plan, err := PlanFanOut(src, branches, opts.MaxInFlightBytes)
	if err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if plan.Strategy == FanOutRescan {
		streams := make([]source.BatchStream, 0, branches)
		for branch := 0; branch < branches; branch++ {
			stream, openErr := source.OpenBatchStreamWithPartitions(ctx, src, readOpts, parts)
			if openErr != nil {
				for _, opened := range streams {
					_ = opened.Close()
				}
				return nil, openErr
			}
			streams = append(streams, stream)
		}
		return streams, nil
	}
	perBranchBytes := opts.MaxInFlightBytes / int64(branches)
	if perBranchBytes < opts.TargetMorselBytes {
		return nil, fmt.Errorf("bounded multiplexer bytes per branch %d is below target morsel bytes %d", perBranchBytes, opts.TargetMorselBytes)
	}
	branchOpts := opts
	branchOpts.Workers = 1
	branchOpts.MaxInFlightBytes = perBranchBytes
	if branchOpts.MaxMorselBytes > perBranchBytes {
		branchOpts.MaxMorselBytes = perBranchBytes
	}
	if err := branchOpts.Validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		sink = observer.Nop{}
	}
	producerCtx, producerCancel := context.WithCancelCause(ctx)
	mux := &boundedMultiplexer{
		src: src, readOpts: readOpts, parts: parts, manager: manager,
		opts: branchOpts, observer: sink, ctx: producerCtx, cancel: producerCancel,
		started: time.Now(),
	}
	mux.active.Store(int32(branches))
	streams := make([]source.BatchStream, branches)
	mux.branches = make([]*fanOutBranch, branches)
	for branch := 0; branch < branches; branch++ {
		exchange, err := NewExchange(manager, perBranchBytes, 2, 1)
		if err != nil {
			producerCancel(err)
			return nil, err
		}
		branchCtx, branchCancel := context.WithCancel(producerCtx)
		state := &fanOutBranch{owner: mux, id: branch, exchange: exchange, ctx: branchCtx, cancel: branchCancel}
		state.active.Store(true)
		mux.branches[branch] = state
		streams[branch] = state
	}
	mux.wg.Add(1)
	go mux.run()
	return streams, nil
}

type boundedMultiplexer struct {
	src      source.Source
	readOpts source.ReadOptions
	parts    []source.Partition
	manager  memory.Manager
	opts     Options
	observer observer.Observer

	ctx      context.Context
	cancel   context.CancelCauseFunc
	active   atomic.Int32
	branches []*fanOutBranch
	wg       sync.WaitGroup

	errOnce sync.Once
	errMu   sync.Mutex
	err     error
	started time.Time
	rows    int64
	bytes   int64
	morsels int64
	observe sync.Once
}

func (m *boundedMultiplexer) run() {
	defer m.wg.Done()
	defer func() {
		for _, branch := range m.branches {
			close(branch.exchange.items)
		}
		m.observeOnce()
	}()
	stream, err := source.OpenBatchStreamWithPartitions(m.ctx, m.src, m.readOpts, m.parts)
	if err != nil {
		m.fail(err)
		return
	}
	sizer, err := NewByteSizer(m.src.Schema(), m.opts)
	if err != nil {
		m.fail(errors.Join(err, stream.Close()))
		return
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	var index uint64
	for {
		ok, nextErr := stream.Next(m.ctx, batch)
		if nextErr != nil {
			m.fail(errors.Join(nextErr, stream.Close()))
			return
		}
		if !ok {
			if closeErr := stream.Close(); closeErr != nil {
				m.fail(closeErr)
				return
			}
			for _, branch := range m.branches {
				branch.sendMu.Lock()
				if branch.active.Load() {
					if err := branch.exchange.enqueueEnd(branch.ctx, 0, index); err != nil && branch.active.Load() {
						branch.sendMu.Unlock()
						m.fail(err)
						return
					}
				}
				branch.sendMu.Unlock()
			}
			return
		}
		ranges, err := sizer.Split(batch)
		if err != nil {
			m.fail(errors.Join(err, stream.Close()))
			return
		}
		for _, morsel := range ranges {
			active := 0
			for _, branch := range m.branches {
				branch.sendMu.Lock()
				if !branch.active.Load() {
					branch.sendMu.Unlock()
					continue
				}
				active++
				err := branch.exchange.enqueueRange(branch.ctx, Sequence{Index: index}, batch, morsel)
				stillActive := branch.active.Load()
				branch.sendMu.Unlock()
				if err != nil && stillActive {
					m.fail(errors.Join(err, stream.Close()))
					return
				}
			}
			if active == 0 {
				_ = stream.Close()
				return
			}
			m.rows += int64(morsel.Rows)
			m.bytes += morsel.Bytes
			m.morsels++
			index++
		}
	}
}

func (m *boundedMultiplexer) fail(err error) {
	if err == nil {
		return
	}
	m.errOnce.Do(func() {
		m.errMu.Lock()
		m.err = err
		m.errMu.Unlock()
		m.cancel(err)
	})
}

func (m *boundedMultiplexer) failure() error {
	m.errMu.Lock()
	defer m.errMu.Unlock()
	return m.err
}

func (m *boundedMultiplexer) observeOnce() {
	m.observe.Do(func() {
		peak := int64(0)
		blocked := time.Duration(0)
		for _, branch := range m.branches {
			stats := branch.exchange.Stats()
			peak += stats.PeakInFlightBytes
			blocked += stats.ProducerBlocked
		}
		m.observer.Observe(observer.Event{
			Stage: "multiplexer", Duration: time.Since(m.started), Rows: m.rows, Bytes: m.bytes,
			Workers: 1,
			Attributes: map[string]string{
				"branches":             strconv.Itoa(len(m.branches)),
				"morsels":              strconv.FormatInt(m.morsels, 10),
				"peak_in_flight_bytes": strconv.FormatInt(peak, 10),
				"producer_blocked_ns":  strconv.FormatInt(int64(blocked), 10),
			},
		})
	})
}

type fanOutBranch struct {
	owner    *boundedMultiplexer
	id       int
	exchange *Exchange
	ctx      context.Context
	cancel   context.CancelFunc
	active   atomic.Bool
	sendMu   sync.Mutex

	current    exchangeItem
	currentDst *memory.Batch
	closed     bool
	exhausted  bool
	terminal   error
}

// Next returns the next Batch for one bounded fan-out branch.
func (b *fanOutBranch) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("batch stream destination is nil")
	}
	if ctx == nil {
		dst.ClearColumns()
		return false, fmt.Errorf("batch stream context is nil")
	}
	if b.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if b.terminal != nil {
		dst.ClearColumns()
		return false, b.terminal
	}
	if b.exhausted {
		dst.ClearColumns()
		return false, nil
	}
	b.releaseCurrent()
	select {
	case <-ctx.Done():
		b.terminal = ctx.Err()
		dst.ClearColumns()
		return false, b.terminal
	case item, ok := <-b.exchange.items:
		if !ok {
			if err := b.owner.failure(); err != nil {
				b.terminal = err
				return false, err
			}
			b.exhausted = true
			dst.ClearColumns()
			return false, nil
		}
		if item.end {
			b.exhausted = true
			dst.ClearColumns()
			return false, nil
		}
		b.bindCurrent(dst, item)
		return true, nil
	}
}

func (b *fanOutBranch) bindCurrent(dst *memory.Batch, item exchangeItem) {
	dst.ClearColumns()
	old := *dst
	*dst = *item.batch
	*item.batch = old
	item.batch.ClearColumns()
	item.batch = nil
	b.current, b.currentDst = item, dst
}

func (b *fanOutBranch) releaseCurrent() {
	if b.currentDst != nil {
		b.currentDst.ClearColumns()
		b.currentDst = nil
	}
	if b.current.bytes != 0 || b.current.reservation != nil || b.current.batch != nil {
		b.exchange.release(b.current)
		b.current = exchangeItem{}
	}
}

// Close removes this branch from producer backpressure, releases queued
// morsels, and cancels the shared scan when the last branch closes.
func (b *fanOutBranch) Close() error {
	if b == nil || b.closed {
		return nil
	}
	b.closed = true
	if b.active.CompareAndSwap(true, false) {
		b.cancel()
		b.sendMu.Lock()
		b.sendMu.Unlock()
		b.exchange.closeSignal()
		b.releaseCurrent()
		for {
			select {
			case item, ok := <-b.exchange.items:
				if !ok {
					goto drained
				}
				b.exchange.release(item)
			default:
				goto drained
			}
		}
	}
drained:
	if b.owner.active.Add(-1) == 0 {
		b.owner.cancel(context.Canceled)
		b.owner.wg.Wait()
	}
	return nil
}

var _ source.BatchStream = (*fanOutBranch)(nil)
