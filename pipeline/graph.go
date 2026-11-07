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
	if err := readOpts.Validate(); err != nil {
		return nil, err
	}
	readOpts = scanReadOptions(ctx, readOpts, manager, opts)
	workers := EffectiveWorkers(src, parts, opts)
	if workers <= 1 {
		return openWholeStream(ctx, src, readOpts, parts)
	}
	// Multi-row batches leave room for rebased range metadata. A single
	// whole-batch view allocates no such metadata, so its payload may use the
	// full per-worker quota. Source-owned allocations retain their own limits.
	perWorkerBytes := opts.MaxInFlightBytes / int64(workers)
	if readOpts.BatchPayloadBytes == 0 || readOpts.BatchPayloadBytes > perWorkerBytes {
		readOpts.BatchPayloadBytes = perWorkerBytes
	}
	target := perWorkerBytes / 2
	if target < 1 {
		target = 1
	}
	if target > readOpts.BatchPayloadBytes {
		target = readOpts.BatchPayloadBytes
	}
	if readOpts.BatchPayloadTargetBytes == 0 || readOpts.BatchPayloadTargetBytes > target {
		readOpts.BatchPayloadTargetBytes = target
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

// scanReadOptions applies scan limits before decoding, not just when an
// exchange later splits an already decoded batch. Source buffers and range
// metadata have distinct ownership and are charged separately to the same query.
func scanReadOptions(ctx context.Context, readOpts source.ReadOptions, manager memory.Manager, opts Options) source.ReadOptions {
	readOpts = readOpts.Normalize()
	if readOpts.BatchRows > opts.MaxMorselRows {
		readOpts.BatchRows = opts.MaxMorselRows
	}
	if readOpts.BatchBytes > opts.MaxMorselBytes {
		readOpts.BatchBytes = opts.MaxMorselBytes
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil {
		readOpts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
	} else if readOpts.MemoryManager == nil {
		readOpts.MemoryManager = memory.ClassifiedManager(manager, "SourceScan", memory.MemoryWorkspace)
	}
	return readOpts
}

// EffectiveWorkers returns the safe partition parallelism after plan,
// partition-size, partition-count, and per-worker byte-quota constraints are
// applied. Workers is a ceiling; tiny partitions remain on the direct path.
func EffectiveWorkers(src source.Source, parts []source.Partition, opts Options) int {
	partitions := len(parts)
	if src == nil || partitions < 2 || !source.CanParallelizePartitions(source.StreamPlanOf(src)) {
		return 1
	}
	// Worker startup and handoff dominate tiny physical partitions, so Workers
	// remains a ceiling rather than a command to parallelize.
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
	currentRange  int

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
			var payloadErr *memory.BatchPayloadTooLargeError
			if errors.As(nextErr, &payloadErr) {
				nextErr = errors.Join(nextErr, &MorselTooLargeError{Row: payloadErr.Row, Bytes: payloadErr.Bytes, LimitBytes: payloadErr.LimitBytes})
			}
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
		if len(ranges) == 0 {
			continue
		}
		payloadBytes, err := memory.EstimateBatchRangeBytes(batch, 0, batch.Rows)
		if err != nil {
			p.recordWorkerError(err, stream.Close())
			return
		}
		if retained := memory.BatchBytes(batch); retained > payloadBytes {
			payloadBytes = retained
		}
		if payloadBytes < 1 {
			payloadBytes = 1
		}
		metadataBytes := int64(0)
		for _, morsel := range ranges {
			bytes, err := memory.BorrowedRangeMetadataBytes(batch, morsel.Offset, morsel.Rows)
			if err != nil {
				p.recordWorkerError(err, stream.Close())
				return
			}
			if bytes > metadataBytes {
				metadataBytes = bytes
			}
		}
		lease, err := p.exchange.enqueueBorrowed(p.ctx, Sequence{Group: group, Index: index}, batch, ranges, payloadBytes, metadataBytes)
		if err != nil {
			p.recordWorkerError(err, stream.Close())
			return
		}
		// A published batch cannot be revoked by cancellation: the consumer
		// may still have hash tasks reading its borrowed vectors.
		started := time.Now()
		<-lease.released
		p.exchange.blockedNanos.Add(int64(time.Since(started)))
		index++
		if err := p.ctx.Err(); err != nil {
			p.recordWorkerError(err, stream.Close())
			return
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
	p.releaseCurrentView()
	dst.ClearColumns()
	if err := ctx.Err(); err != nil {
		return false, p.fail(err)
	}
	for {
		if err := p.failure(); err != nil {
			return false, p.fail(err)
		}
		if p.ctx.Err() != nil {
			return false, p.fail(context.Cause(p.ctx))
		}
		if p.current.borrowed != nil {
			if p.currentRange < len(p.current.borrowed.ranges) {
				if err := p.bindRange(dst); err != nil {
					return false, p.fail(err)
				}
				return true, nil
			}
			p.releaseCurrent()
		}
		if p.opts.Ordering == OrderingStable {
			if item, ok, err := p.readyStable(); err != nil {
				return false, p.fail(err)
			} else if ok {
				if err := p.bindCurrent(dst, item); err != nil {
					return false, p.fail(err)
				}
				return true, nil
			}
		}
		select {
		case <-p.ctx.Done():
			return false, p.fail(context.Cause(p.ctx))
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
						if err := p.bindCurrent(dst, item); err != nil {
							return false, p.fail(err)
						}
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
				if err := p.bindCurrent(dst, item); err != nil {
					return false, p.fail(err)
				}
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

func (p *parallelBatchStream) bindCurrent(dst *memory.Batch, item exchangeItem) error {
	p.current = item
	p.currentRange = 0
	return p.bindRange(dst)
}

func (p *parallelBatchStream) bindRange(dst *memory.Batch) error {
	item := p.current
	if item.borrowed == nil || p.currentRange >= len(item.borrowed.ranges) {
		return fmt.Errorf("invalid borrowed batch lease")
	}
	r := item.borrowed.ranges[p.currentRange]
	view, err := memory.BorrowBatchRange(item.batch, r.Offset, r.Rows)
	if err != nil {
		return err
	}
	dst.ClearColumns()
	// Move only view-owned maps; never clear or swap the producer's batch.
	old := *dst
	*dst = *view
	*view = old
	p.currentDst = dst
	p.currentRange++
	p.outputRows += int64(r.Rows)
	p.outputBytes += r.Bytes
	p.morsels++
	return nil
}

func (p *parallelBatchStream) releaseCurrentView() {
	if p.currentDst != nil {
		p.currentDst.ClearColumns()
		p.currentDst = nil
	}
}

func (p *parallelBatchStream) releaseCurrent() {
	p.releaseCurrentView()
	if p.current.borrowed != nil || p.current.bytes != 0 || p.current.reservation != nil || p.current.batch != nil {
		p.exchange.release(p.current)
		p.current = exchangeItem{}
		p.currentRange = 0
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
		p.releaseCurrent()
		for _, group := range p.pending {
			for _, item := range group.items {
				p.exchange.release(item)
			}
		}
		p.pending = make(map[uint32]*pendingGroup)
		// Drain before waiting: producers with published leases wait for ACK
		// even after cancellation, to protect consumer-visible buffers.
		for item := range p.exchange.items {
			p.exchange.release(item)
		}
		p.wg.Wait()
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

// openWholeStream opens the complete input, preserving prepared partitions only
// when the source explicitly implements the partition contract. Global operators
// have one stream and must never be mistaken for partition-subset providers.
func openWholeStream(ctx context.Context, src source.Source, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if _, ok := src.(source.PreparedBatchStreamProvider); ok && source.CanParallelizePartitions(source.StreamPlanOf(src)) {
		return source.OpenBatchStreamWithPartitions(ctx, src, opts, parts)
	}
	return source.OpenBatchStream(ctx, src, opts)
}
