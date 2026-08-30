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
type ExchangeStats struct {
	CurrentInFlightBytes int64
	PeakInFlightBytes    int64
	ProducerBlocked      time.Duration
	QueueHighWater       int64
}

// Exchange is a byte-bounded query-owned handoff. Producers acquire byte
// capacity and engine memory before cloning, then enqueue the owned morsel.
type Exchange struct {
	maxBytes   int64
	bytes      *semaphore.Weighted
	groupBytes []*semaphore.Weighted
	groupMax   int64
	manager    memory.Manager
	items      chan exchangeItem
	closed     chan struct{}
	closeOnce  sync.Once

	currentBytes atomic.Int64
	peakBytes    atomic.Int64
	blockedNanos atomic.Int64
	queueHigh    atomic.Int64
}

type exchangeItem struct {
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
	}
}

func (e *Exchange) enqueueRange(ctx context.Context, sequence Sequence, src *memory.Batch, r MorselRange) error {
	if e == nil {
		return fmt.Errorf("exchange is nil")
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
	if e.manager != nil {
		reservation, err = e.manager.Reserve(ctx, r.Bytes)
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
