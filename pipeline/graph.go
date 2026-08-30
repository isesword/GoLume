package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// StreamStats summarizes one query-owned parallel pipeline.
type StreamStats struct {
	Exchange ExchangeStats
	Workers  int
	Morsels  int64
	Rows     int64
	Bytes    int64
}

// StreamStatsProvider is implemented by parallel pipeline streams.
type StreamStatsProvider interface {
	PipelineStats() StreamStats
}

// OpenBatchStream opens a direct stream for one worker or a query-owned
// parallel graph for partition-parallel plans. No worker survives Close.
func OpenBatchStream(ctx context.Context, src source.Source, readOpts source.ReadOptions, parts []source.Partition, manager memory.Manager, opts Options, sink observer.Observer) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pipeline context is nil")
	}
	if src == nil {
		return nil, fmt.Errorf("pipeline source is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	workers := EffectiveWorkers(src, parts, opts)
	if workers <= 1 {
		return source.OpenBatchStreamWithPartitions(ctx, src, readOpts, parts)
	}
	exchange, err := NewExchange(manager, opts.MaxInFlightBytes, workers*2, workers)
	if err != nil {
		return nil, err
	}
	if sink == nil {
		sink = observer.Nop{}
	}
	queryCtx, cancel := context.WithCancelCause(ctx)
	p := &parallelBatchStream{
		src:      src,
		readOpts: readOpts,
		parts:    parts,
		opts:     opts,
		observer: sink,
		workers:  workers,
		ctx:      queryCtx,
		cancel:   cancel,
		exchange: exchange,
		pending:  make(map[uint32]*pendingGroup, workers),
		started:  time.Now(),
	}
	p.remaining.Store(int32(workers))
	p.wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		start := worker * len(parts) / workers
		end := (worker + 1) * len(parts) / workers
		go p.runWorker(uint32(worker), parts[start:end])
	}
	return p, nil
}

// EffectiveWorkers returns the safe partition parallelism after plan,
// partition-size, partition-count, and per-worker byte-quota constraints are
// applied. Workers is a ceiling; tiny partitions remain on the direct path.
func EffectiveWorkers(src source.Source, parts []source.Partition, opts Options) int {
	partitions := len(parts)
	if src == nil || partitions < 2 || !source.CanParallelizePartitions(source.StreamPlanOf(src)) {
		return 1
	}
	// A parallel exchange owns and clones every emitted morsel. For sources with
	// tiny physical partitions that fixed cost dominates useful work, so Workers
	// is a ceiling rather than a command to parallelize. Larger batches can
	// amortize the exchange and remain eligible for partition parallelism.
	if estimatedAveragePartitionBytes(src, parts) < opts.TargetMorselBytes/4 {
		return 1
	}
	workers := opts.Workers
	if workers > partitions {
		workers = partitions
	}
	byteWorkers := int(opts.MaxInFlightBytes / opts.MaxMorselBytes)
	if workers > byteWorkers {
		workers = byteWorkers
	}
	if workers < 1 {
		return 1
	}
	return workers
}

func estimatedAveragePartitionBytes(src source.Source, parts []source.Partition) int64 {
	if src == nil || len(parts) < 1 {
		return 0
	}
	var rows int64
	for _, part := range parts {
		if part.Rows < 0 || rows > math.MaxInt64-part.Rows {
			return math.MaxInt64
		}
		rows += part.Rows
	}
	averageRows := rows / int64(len(parts))
	rowBytes := EstimateSchemaRowBytes(src.Schema())
	if averageRows > 0 && rowBytes > math.MaxInt64/averageRows {
		return math.MaxInt64
	}
	return averageRows * rowBytes
}

type pendingGroup struct {
	items map[uint64]exchangeItem
	done  bool
	total uint64
}

type parallelBatchStream struct {
	src      source.Source
	readOpts source.ReadOptions
	parts    []source.Partition
	opts     Options
	observer observer.Observer
	workers  int

	ctx       context.Context
	cancel    context.CancelCauseFunc
	exchange  *Exchange
	remaining atomic.Int32
	wg        sync.WaitGroup

	errOnce  sync.Once
	errMu    sync.Mutex
	firstErr error
	closing  atomic.Bool

	pending       map[uint32]*pendingGroup
	expectedGroup uint32
	expectedIndex uint64
	current       exchangeItem
	currentDst    *memory.Batch

	started     time.Time
	outputRows  int64
	outputBytes int64
	morsels     int64

	cleanupOnce sync.Once
	observeOnce sync.Once
	closed      bool
	exhausted   bool
	terminalErr error
	closeErr    error
}

func (p *parallelBatchStream) runWorker(group uint32, parts []source.Partition) {
	defer func() {
		p.wg.Done()
		if p.remaining.Add(-1) == 0 {
			close(p.exchange.items)
		}
	}()
	stream, err := source.OpenBatchStreamWithPartitions(p.ctx, p.src, p.readOpts, parts)
	if err != nil {
		p.recordFailure(err)
		return
	}
	sizer, err := NewByteSizer(p.src.Schema(), p.opts)
	if err != nil {
		p.recordWorkerError(err, stream.Close())
		return
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	var index uint64
	for {
		ok, nextErr := stream.Next(p.ctx, batch)
		if nextErr != nil {
			closeErr := stream.Close()
			p.recordWorkerError(nextErr, closeErr)
			return
		}
		if !ok {
			if closeErr := stream.Close(); closeErr != nil {
				p.recordFailure(closeErr)
				return
			}
			if err := p.exchange.enqueueEnd(p.ctx, group, index); err != nil && !p.ignoreShutdownError(err) {
				p.recordFailure(err)
			}
			return
		}
		ranges, splitErr := sizer.Split(batch)
		if splitErr != nil {
			p.recordWorkerError(splitErr, stream.Close())
			return
		}
		for _, morsel := range ranges {
			if err := p.exchange.enqueueRange(p.ctx, Sequence{Group: group, Index: index}, batch, morsel); err != nil {
				p.recordWorkerError(err, stream.Close())
				return
			}
			index++
		}
	}
}

func (p *parallelBatchStream) recordWorkerError(primary, closeErr error) {
	if primary != nil && p.ignoreShutdownError(primary) {
		if closeErr != nil && !p.ignoreShutdownError(closeErr) {
			p.recordFailure(closeErr)
		}
		return
	}
	p.recordFailure(errors.Join(primary, closeErr))
}

func (p *parallelBatchStream) ignoreShutdownError(err error) bool {
	return p.closing.Load() && (errors.Is(err, context.Canceled) || errors.Is(err, errExchangeClosed))
}

func (p *parallelBatchStream) recordFailure(err error) {
	if err == nil || p.ignoreShutdownError(err) {
		return
	}
	p.errOnce.Do(func() {
		p.errMu.Lock()
		p.firstErr = err
		p.errMu.Unlock()
		p.cancel(err)
	})
}

func (p *parallelBatchStream) failure() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.firstErr
}

// Next returns the next stable or relaxed fan-in morsel.
func (p *parallelBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("batch stream destination is nil")
	}
	if ctx == nil {
		dst.ClearColumns()
		return false, fmt.Errorf("batch stream context is nil")
	}
	if p.closed {
		dst.ClearColumns()
		return false, source.ErrBatchStreamClosed
	}
	if p.terminalErr != nil {
		dst.ClearColumns()
		return false, p.terminalErr
	}
	if p.exhausted {
		dst.ClearColumns()
		return false, nil
	}
	p.releaseCurrent()
	if err := ctx.Err(); err != nil {
		return false, p.fail(err)
	}
	for {
		if err := p.failure(); err != nil {
			return false, p.fail(err)
		}
		if p.opts.Ordering == OrderingStable {
			if item, ok, err := p.readyStable(); err != nil {
				return false, p.fail(err)
			} else if ok {
				p.bindCurrent(dst, item)
				return true, nil
			}
		}
		select {
		case <-ctx.Done():
			return false, p.fail(ctx.Err())
		case item, ok := <-p.exchange.items:
			if !ok {
				if err := p.failure(); err != nil {
					return false, p.fail(err)
				}
				if p.opts.Ordering == OrderingStable {
					if item, ready, err := p.readyStable(); err != nil {
						return false, p.fail(err)
					} else if ready {
						p.bindCurrent(dst, item)
						return true, nil
					}
					if p.expectedGroup != uint32(p.workers) {
						return false, p.fail(fmt.Errorf("stable fan-in ended at group %d of %d", p.expectedGroup, p.workers))
					}
				}
				p.finish()
				dst.ClearColumns()
				return false, nil
			}
			if p.opts.Ordering == OrderingRelaxed {
				if item.end {
					continue
				}
				p.bindCurrent(dst, item)
				return true, nil
			}
			if err := p.bufferStable(item); err != nil {
				p.exchange.release(item)
				return false, p.fail(err)
			}
		}
	}
}

func (p *parallelBatchStream) bufferStable(item exchangeItem) error {
	group := item.sequence.Group
	if group < p.expectedGroup || group >= uint32(p.workers) {
		return fmt.Errorf("stable fan-in received invalid group %d", group)
	}
	pending := p.pending[group]
	if pending == nil {
		pending = &pendingGroup{items: make(map[uint64]exchangeItem)}
		p.pending[group] = pending
	}
	if item.end {
		if pending.done {
			return fmt.Errorf("stable fan-in received duplicate end for group %d", group)
		}
		pending.done, pending.total = true, item.total
		return nil
	}
	if _, exists := pending.items[item.sequence.Index]; exists {
		return fmt.Errorf("stable fan-in received duplicate sequence %d:%d", group, item.sequence.Index)
	}
	pending.items[item.sequence.Index] = item
	return nil
}

func (p *parallelBatchStream) readyStable() (exchangeItem, bool, error) {
	for p.expectedGroup < uint32(p.workers) {
		group := p.pending[p.expectedGroup]
		if group == nil {
			return exchangeItem{}, false, nil
		}
		if item, ok := group.items[p.expectedIndex]; ok {
			delete(group.items, p.expectedIndex)
			p.expectedIndex++
			return item, true, nil
		}
		if !group.done {
			return exchangeItem{}, false, nil
		}
		if p.expectedIndex != group.total {
			return exchangeItem{}, false, fmt.Errorf("stable fan-in group %d ended with %d batches, next is %d", p.expectedGroup, group.total, p.expectedIndex)
		}
		delete(p.pending, p.expectedGroup)
		p.expectedGroup++
		p.expectedIndex = 0
	}
	return exchangeItem{}, false, nil
}

func (p *parallelBatchStream) bindCurrent(dst *memory.Batch, item exchangeItem) {
	dst.ClearColumns()
	old := *dst
	*dst = *item.batch
	*item.batch = old
	item.batch.ClearColumns()
	item.batch = nil
	p.current, p.currentDst = item, dst
	p.outputRows += int64(dst.Rows)
	p.outputBytes += item.bytes
	p.morsels++
}

func (p *parallelBatchStream) releaseCurrent() {
	if p.currentDst != nil {
		p.currentDst.ClearColumns()
		p.currentDst = nil
	}
	if p.current.bytes != 0 || p.current.reservation != nil || p.current.batch != nil {
		p.exchange.release(p.current)
		p.current = exchangeItem{}
	}
}

func (p *parallelBatchStream) fail(err error) error {
	if p.terminalErr != nil {
		return p.terminalErr
	}
	p.recordFailure(err)
	p.terminalErr = errors.Join(err, p.failure())
	p.cancel(p.terminalErr)
	p.cleanup(true)
	return p.terminalErr
}

func (p *parallelBatchStream) finish() {
	p.exhausted = true
	p.cleanup(false)
}

func (p *parallelBatchStream) cleanup(cancel bool) {
	p.cleanupOnce.Do(func() {
		if cancel {
			p.cancel(p.terminalErr)
		}
		p.exchange.closeSignal()
		p.wg.Wait()
		p.releaseCurrent()
		for _, group := range p.pending {
			for _, item := range group.items {
				p.exchange.release(item)
			}
		}
		p.pending = make(map[uint32]*pendingGroup)
		for item := range p.exchange.items {
			p.exchange.release(item)
		}
		p.observe()
	})
}

func (p *parallelBatchStream) observe() {
	p.observeOnce.Do(func() {
		stats := p.exchange.Stats()
		p.observer.Observe(observer.Event{
			Stage:    "pipeline",
			Duration: time.Since(p.started),
			Rows:     p.outputRows,
			Bytes:    p.outputBytes,
			Workers:  p.workers,
			Attributes: map[string]string{
				"morsels":              strconv.FormatInt(p.morsels, 10),
				"ordering":             p.opts.Ordering.String(),
				"target_morsel_bytes":  strconv.FormatInt(p.opts.TargetMorselBytes, 10),
				"peak_in_flight_bytes": strconv.FormatInt(stats.PeakInFlightBytes, 10),
				"producer_blocked_ns":  strconv.FormatInt(int64(stats.ProducerBlocked), 10),
				"queue_high_water":     strconv.FormatInt(stats.QueueHighWater, 10),
			},
		})
	})
}

// PipelineStats returns a stage-level metrics snapshot.
func (p *parallelBatchStream) PipelineStats() StreamStats {
	if p == nil {
		return StreamStats{}
	}
	return StreamStats{Exchange: p.exchange.Stats(), Workers: p.workers, Morsels: p.morsels, Rows: p.outputRows, Bytes: p.outputBytes}
}

// Close cancels all query-owned workers, releases queued reservations, and
// waits until no pipeline goroutine remains. It is idempotent.
func (p *parallelBatchStream) Close() error {
	if p == nil || p.closed {
		return p.closeErr
	}
	p.closed = true
	p.closing.Store(true)
	p.cancel(context.Canceled)
	p.cleanup(true)
	if err := p.failure(); err != nil && !errors.Is(err, context.Canceled) {
		p.closeErr = err
	}
	return p.closeErr
}

var _ source.BatchStream = (*parallelBatchStream)(nil)
var _ StreamStatsProvider = (*parallelBatchStream)(nil)
