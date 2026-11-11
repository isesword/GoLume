package pipeline

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"golang.org/x/sync/semaphore"
	"sync"
	"sync/atomic"
	"time"
)

var errExchangeClosed = errors.New("pipeline exchange closed")

// Sequence identifies one stable fan-in group and the morsel ordinal within
// that group.
type Sequence struct {
	Group uint32
	Index uint64
}

// ExchangeStats reports stage-level backpressure without row-loop callbacks.
// In-flight bytes are accounting charges, including legacy Opaque logical bytes,
// not a complete Arrow-retention or process-memory bound.
type ExchangeStats struct {
	CurrentInFlightBytes int64
	PeakInFlightBytes    int64
	ProducerBlocked      time.Duration
	QueueHighWater       int64
	// LegacyOpaqueItems and LegacyOpaqueBytes are cumulative successfully
	// published owned items and their legacy Opaque logical charge. This charge
	// is not a bound on retained Arrow backing or private extension metadata.
	LegacyOpaqueItems int64
	LegacyOpaqueBytes int64
}

// Exchange is a byte-bounded query-owned handoff. Producers acquire byte
// capacity before publication. Fan-out owns cloned morsels; partition fan-in
// borrows source payloads until the consumer acknowledges the whole batch.
type Exchange struct {
	maxBytes   int64
	bytes      *semaphore.Weighted
	groupBytes []*semaphore.Weighted
	groupMax   int64
	manager    memory.Manager
	items      chan exchangeItem
	closed     chan struct{}
	closeOnce  sync.Once

	currentBytes      atomic.Int64
	peakBytes         atomic.Int64
	blockedNanos      atomic.Int64
	queueHigh         atomic.Int64
	legacyOpaqueItems atomic.Int64
	legacyOpaqueBytes atomic.Int64
}

// borrowedBatchLease prevents source reuse until every emitted range is consumed.
// Cancellation stops publication, but cannot revoke a published lease.
type borrowedBatchLease struct {
	released chan struct{}
	once     sync.Once
	ranges   []MorselRange
}

type exchangeItem struct {
	shared      *preparedSharedItem
	borrowed    *borrowedBatchLease
	sequence    Sequence
	batch       *memory.Batch
	bytes       int64
	reservation memory.Reservation
	end         bool
	total       uint64
}

// NewExchange constructs a byte-bounded exchange. queueSlots bounds metadata
// and zero-byte completion markers in addition to the byte semaphore.
func NewExchange(manager memory.Manager, maxBytes int64, queueSlots, groups int) (*Exchange, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("exchange max bytes must be positive")
	}
	if queueSlots < 1 {
		return nil, fmt.Errorf("exchange queue slots must be positive")
	}
	if groups < 1 {
		return nil, fmt.Errorf("exchange groups must be positive")
	}
	groupMax := maxBytes / int64(groups)
	if groupMax < 1 {
		return nil, fmt.Errorf("exchange max bytes %d cannot provide capacity to %d groups", maxBytes, groups)
	}
	exchange := &Exchange{
		maxBytes: maxBytes,
		bytes:    semaphore.NewWeighted(maxBytes),
		groupMax: groupMax,
		manager:  manager,
		items:    make(chan exchangeItem, queueSlots),
		closed:   make(chan struct{}),
	}
	exchange.groupBytes = make([]*semaphore.Weighted, groups)
	for group := range exchange.groupBytes {
		exchange.groupBytes[group] = semaphore.NewWeighted(groupMax)
	}
	return exchange, nil
}

// Stats returns an atomic snapshot of byte pressure and producer blocking.
func (e *Exchange) Stats() ExchangeStats {
	if e == nil {
		return ExchangeStats{}
	}
	return ExchangeStats{
		CurrentInFlightBytes: e.currentBytes.Load(),
		PeakInFlightBytes:    e.peakBytes.Load(),
		ProducerBlocked:      time.Duration(e.blockedNanos.Load()),
		QueueHighWater:       e.queueHigh.Load(),
		LegacyOpaqueItems:    e.legacyOpaqueItems.Load(),
		LegacyOpaqueBytes:    e.legacyOpaqueBytes.Load(),
	}
}

func (e *Exchange) enqueueRange(ctx context.Context, sequence Sequence, src *memory.Batch, r MorselRange) error {
	if e == nil {
		return fmt.Errorf("exchange is nil")
	}
	// The producer's range is advisory. Recompute the owned admission model
	// before acquiring capacity or copying, so a logical/stale Bytes value cannot
	// under-reserve native maps, metadata, capacity or clone growth overlap.
	// Opaque remains on its legacy logical estimate and clone path; its private
	// backing/metadata is outside the native-copy capacity guarantee.
	estimate, estimateErr := memory.EstimateNativeOwnedBatchRangeBytes(src, r.Offset, r.Rows)
	if estimateErr != nil {
		return estimateErr
	}
	r.Bytes = estimate.Bytes
	// Preserve the legacy one-token minimum for zero-payload Opaque items.
	if r.Bytes == 0 {
		r.Bytes = 1
	}
	if r.Bytes <= 0 {
		return fmt.Errorf("morsel %v has non-positive byte reservation %d", sequence, r.Bytes)
	}
	if r.Bytes > e.maxBytes {
		return &MorselTooLargeError{Row: r.Offset, Bytes: r.Bytes, LimitBytes: e.maxBytes}
	}
	if int(sequence.Group) >= len(e.groupBytes) {
		return fmt.Errorf("morsel group %d exceeds exchange groups %d", sequence.Group, len(e.groupBytes))
	}
	if r.Bytes > e.groupMax {
		return fmt.Errorf("morsel %v requires %d bytes, exceeding per-group exchange capacity %d", sequence, r.Bytes, e.groupMax)
	}
	started := time.Now()
	groupBytes := e.groupBytes[sequence.Group]
	if err := groupBytes.Acquire(ctx, r.Bytes); err != nil {
		return err
	}
	releaseGroup := true
	defer func() {
		if releaseGroup {
			groupBytes.Release(r.Bytes)
		}
	}()
	if err := e.bytes.Acquire(ctx, r.Bytes); err != nil {
		return err
	}
	e.blockedNanos.Add(int64(time.Since(started)))
	current := e.currentBytes.Add(r.Bytes)
	updateMax(&e.peakBytes, current)
	releaseBytes := true
	defer func() {
		if releaseBytes {
			e.currentBytes.Add(-r.Bytes)
			e.bytes.Release(r.Bytes)
		}
	}()
	select {
	case <-e.closed:
		return errExchangeClosed
	default:
	}
	var reservation memory.Reservation
	var err error
	if e.manager != nil || memory.QueryBudgetFromContext(ctx) != nil {
		reservation, err = e.reserve(ctx, r.Bytes)
		if err != nil {
			return err
		}
	}
	owned, err := memory.CloneBatchRange(src, r.Offset, r.Rows)
	if err != nil {
		if reservation != nil {
			reservation.Release()
		}
		return err
	}
	if actual := memory.BatchBytes(owned); actual > r.Bytes {
		owned.ReleaseOwned()
		if reservation != nil {
			reservation.Release()
		}
		return fmt.Errorf("morsel clone used %d bytes after reserving %d", actual, r.Bytes)
	}
	item := exchangeItem{sequence: sequence, batch: owned, bytes: r.Bytes, reservation: reservation}
	select {
	case e.items <- item:
		if estimate.OpaquePresent {
			e.legacyOpaqueItems.Add(1)
			e.legacyOpaqueBytes.Add(estimate.LegacyOpaqueBytes)
		}
		releaseBytes = false
		releaseGroup = false
		updateMax(&e.queueHigh, int64(len(e.items)))
		return nil
	case <-ctx.Done():
		owned.ReleaseOwned()
		if reservation != nil {
			reservation.Release()
		}
		return ctx.Err()
	case <-e.closed:
		owned.ReleaseOwned()
		if reservation != nil {
			reservation.Release()
		}
		return errExchangeClosed
	}
}

func (e *Exchange) enqueueEnd(ctx context.Context, group uint32, total uint64) error {
	item := exchangeItem{sequence: Sequence{Group: group}, end: true, total: total}
	select {
	case e.items <- item:
		updateMax(&e.queueHigh, int64(len(e.items)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return errExchangeClosed
	}
}

func (e *Exchange) release(item exchangeItem) {
	if item.shared != nil {
		// Remove consumer/pending graph references before acknowledging the lease.
		if item.batch != nil {
			item.batch.ClearColumns()
			*item.batch = memory.Batch{}
			item.batch = nil
		}
		binding := item.shared
		item.shared = nil
		binding.release()
		e.releaseSharedCredit(item.bytes)
		return
	}

	if item.borrowed != nil {
		item.borrowed.once.Do(func() {
			if item.reservation != nil {
				item.reservation.Release()
			}
			if item.bytes > 0 {
				e.currentBytes.Add(-item.bytes)
				e.bytes.Release(item.bytes)
				e.groupBytes[item.sequence.Group].Release(item.bytes)
			}
			close(item.borrowed.released)
		})
		return
	}

	if item.batch != nil {
		item.batch.ReleaseOwned()
	}
	if item.reservation != nil {
		item.reservation.Release()
	}
	if item.bytes > 0 {
		e.currentBytes.Add(-item.bytes)
		e.bytes.Release(item.bytes)
		e.groupBytes[item.sequence.Group].Release(item.bytes)
	}
}

func (e *Exchange) closeSignal() {
	e.closeOnce.Do(func() { close(e.closed) })
}

func updateMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

// enqueueBorrowed publishes a whole source-owned batch. Capacity covers all
// retained payload plus the largest live range's allocated metadata. Source
// payload memory is already reserved by SourceScan; only view metadata requires
// a new engine reservation. The caller must await released after success.
func (e *Exchange) enqueueBorrowed(ctx context.Context, sequence Sequence, batch *memory.Batch, ranges []MorselRange, payloadBytes, metadataBytes int64) (*borrowedBatchLease, error) {
	if payloadBytes < 1 || metadataBytes < 0 || payloadBytes > e.groupMax || metadataBytes > e.groupMax-payloadBytes {
		return nil, fmt.Errorf("borrowed batch %v requires payload=%d metadata=%d bytes, exceeding per-worker capacity %d; source must honor scan byte limits", sequence, payloadBytes, metadataBytes, e.groupMax)
	}
	bytes := payloadBytes + metadataBytes
	if int(sequence.Group) >= len(e.groupBytes) {
		return nil, fmt.Errorf("borrowed batch group %d exceeds exchange groups", sequence.Group)
	}
	started := time.Now()
	group := e.groupBytes[sequence.Group]
	if err := group.Acquire(ctx, bytes); err != nil {
		return nil, err
	}
	if err := e.bytes.Acquire(ctx, bytes); err != nil {
		group.Release(bytes)
		return nil, err
	}
	e.blockedNanos.Add(int64(time.Since(started)))
	current := e.currentBytes.Add(bytes)
	updateMax(&e.peakBytes, current)
	var reservation memory.Reservation
	if metadataBytes > 0 && (e.manager != nil || memory.QueryBudgetFromContext(ctx) != nil) {
		var err error
		reservation, err = e.reserve(ctx, metadataBytes)
		if err != nil {
			e.currentBytes.Add(-bytes)
			e.bytes.Release(bytes)
			group.Release(bytes)
			return nil, err
		}
	}
	lease := &borrowedBatchLease{released: make(chan struct{}), ranges: ranges}
	item := exchangeItem{sequence: sequence, batch: batch, bytes: bytes, reservation: reservation, borrowed: lease}
	select {
	case e.items <- item:
		updateMax(&e.queueHigh, int64(len(e.items)))
		return lease, nil
	case <-ctx.Done():
		e.release(item)
		return nil, ctx.Err()
	case <-e.closed:
		e.release(item)
		return nil, errExchangeClosed
	}
}

// The active query is authoritative, even when a prepared plan carried a
// manager from a different engine root. Queryless callers retain injection.
func (e *Exchange) reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	if query := memory.QueryBudgetFromContext(ctx); query != nil {
		return query.Operator("PipelineExchange", memory.MemoryInFlightBatch).Reserve(ctx, bytes)
	}
	if e.manager != nil {
		return e.manager.Reserve(ctx, bytes)
	}
	return nil, nil
}
