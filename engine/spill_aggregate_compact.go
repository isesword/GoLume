package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"unsafe"
)

// aggregateCompactionBuffer limits the complete serialized intermediate state,
// not its group count. All aggregate codecs (including byte endpoints) size the
// full run before their single Reserve call. A compacted run therefore needs at
// most maxState/2 plus the codec name when it is restored in full later.
// Embedding a GrowthManager also prevents admission from reentering Spill while
// the stream mutex is held.
type aggregateCompactionBuffer struct {
	memory.Manager
	maxPayload int64
}

// Reserve admits the complete encoded payload before its allocation.
func (m *aggregateCompactionBuffer) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bytes > m.maxPayload {
		return nil, errAggregateStateTarget
	}
	return m.Manager.Reserve(ctx, bytes)
}

// aggregateCompactionScratch accounts the fixed owners and callback captures.
// Backing arrays, keys, state lanes, restored payloads, encoded payloads and
// Store handles are admitted separately by their existing owners.
type aggregateCompactionScratch struct {
	table       *aggregateTableOwner
	encoded     *encodedAggregateRun
	replacement spillmeta.Slice[aggregateRunRef]
	handle      memory.SpillHandle
	buffer      aggregateCompactionBuffer
	merge       func(aggregateRecord) error
	visit       aggregateRecordVisitor
}

func aggregateCompactionMetadataBytes() int64 {
	// Only one scalar table or one multi table is live at a time. Count the
	// largest implementation rather than charging every unused variant.
	scalar := max(unsafe.Sizeof(hashagg.I32Table{}), unsafe.Sizeof(hashagg.U64Table{}),
		unsafe.Sizeof(hashagg.U32Table{}), unsafe.Sizeof(hashagg.PairTable{}), unsafe.Sizeof(hashagg.StringTable{}))
	multi := unsafe.Sizeof(multiAggregateState{}) + unsafe.Sizeof(hashagg.GroupIndex{}) +
		max(unsafe.Sizeof(aggregate.Int64Bundle{}), unsafe.Sizeof(aggregate.NumericBundle{})) +
		unsafe.Sizeof(aggregate.StatisticsBundle{})
	reader := unsafe.Sizeof(aggregateRunReader{}) + max(unsafe.Sizeof(multiAggregateRunReader{}),
		unsafe.Sizeof(numericAggregateRunReader{}), unsafe.Sizeof(statisticsAggregateRunReader{}),
		unsafe.Sizeof(bytesStatisticsAggregateRunReader{}))
	// The two-record allowance bounds the captured callback environments and
	// the local run/record descriptors. 2x covers allocator size-class rounding;
	// 128 covers this reservation and the small GrowthManager interface boxes.
	return 128 + 2*int64(unsafe.Sizeof(aggregateCompactionScratch{})+
		unsafe.Sizeof(aggregateTableOwner{})+unsafe.Sizeof(aggregateStateTable{})+
		max(scalar, multi)+reader+unsafe.Sizeof(memory.RestoredSpill{})+
		unsafe.Sizeof(encodedAggregateRun{})+unsafe.Sizeof(aggregateRunWriter{})+
		2*unsafe.Sizeof(aggregateRecord{})+2*unsafe.Sizeof(aggregateRunRef{}))
}

// aggregateCompactionPressure accepts only resource-admission pressure, including
// a disk quota that cannot fit the optional third file while both sources live.
// In particular a joined cleanup/cancellation error must never become a skip.
func aggregateCompactionPressure(err error) bool {
	if err == nil {
		return false
	}
	if err == memory.ErrBudgetExceeded || err == errAggregateStateTarget || err == memory.ErrSpillQuotaExceeded {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !aggregateCompactionPressure(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return aggregateCompactionPressure(wrapped.Unwrap())
	}
	return false
}

// compactInputRuns must be called after Spill has returned, so its table,
// partition directory and temporary publication list have all been released.
// Each call tries only the oldest two runs in each partition, at most sixteen
// attempts. A non-fitting union stays untouched for the EOF radix merge; there
// is no retry loop, recursive spill, or publication of a partially merged table.
func (s *spillableAggregateStream) compactInputRuns(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("aggregate compaction context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalErr != nil {
		return s.terminalErr
	}
	if s.table != nil || s.residentOutput || s.closed || s.exhausted {
		return nil
	}
	for partition := range s.partitions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(s.partitions[partition].Values) < 2 {
			continue
		}
		if err := s.compactInputPrefixLocked(ctx, partition); err != nil {
			return err
		}
	}
	return nil
}

func (s *spillableAggregateStream) compactInputPrefixLocked(ctx context.Context, partition int) (err error) {
	lease, err := s.metadata.Reserve(ctx, aggregateCompactionMetadataBytes())
	if err != nil {
		if aggregateCompactionPressure(err) {
			return nil
		}
		return err
	}
	var work aggregateCompactionScratch
	committed := false
	defer func() {
		// Clear every callback/borrowed reference before its owner's release.
		work.merge, work.visit = nil, nil
		if work.table != nil {
			work.table.close()
			work.table = nil
		}
		if work.encoded != nil {
			work.encoded.close()
			work.encoded = nil
		}
		clear(work.replacement.Values)
		work.replacement.Release()
		var cleanupErr error
		if !committed && work.handle != nil {
			cleanupErr = s.store.Delete(context.Background(), work.handle)
		}
		work.handle = nil
		work.buffer = aggregateCompactionBuffer{}
		lease.Release()
		// Pressure is a best-effort skip only before publication. Any failed
		// cleanup still returns an error and remains owned by the Store.
		if !committed && aggregateCompactionPressure(err) {
			err = nil
		}
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	work.table, err = newAggregateTableOwner(ctx, s.query, s.state, s.maxState)
	if err != nil {
		return err
	}
	work.merge = func(record aggregateRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := work.table.table.merge(record)
		return err
	}
	for index := 0; index < 2; index++ {
		if s.partitions[partition].Values[index].partition != partition {
			return fmt.Errorf("%w: aggregate compaction partition mismatch", memory.ErrSpillCorrupt)
		}
		if err = s.restoreRunWithManager(ctx, s.partitions[partition].Values[index],
			spillmeta.GrowthManager(s.restorer), work.merge); err != nil {
			return err
		}
	}
	work.merge = nil
	work.buffer = aggregateCompactionBuffer{Manager: spillmeta.GrowthManager(s.encoder), maxPayload: s.maxState / 2}
	work.visit = func(fn func(aggregateRecord) error) error {
		return work.table.table.visit(func(record aggregateRecord) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fn(record)
		})
	}
	work.encoded, err = s.query.encodeAggregateRun(ctx, work.visit, nil, &work.buffer)
	work.visit = nil
	if err != nil {
		return err
	}
	// The buffer owns its serialized state. Do not retain the table across
	// file metadata admission or the replacement reference-array allocation.
	work.table.close()
	work.table = nil
	if work.encoded.groups == 0 {
		return fmt.Errorf("%w: aggregate compaction produced an empty run", memory.ErrSpillCorrupt)
	}
	groups := work.encoded.groups
	work.handle, err = s.store.Spill(ctx, memory.SpillObject{
		Kind: memory.SpillAggregateState, Partition: int32(partition), Codec: s.query.aggregateRunCodec(),
		SchemaFingerprint: s.fingerprint, Payload: work.encoded.payload,
	})
	work.encoded.close()
	work.encoded = nil
	if work.handle != nil {
		// Count actual committed I/O, even if later publication is rejected.
		s.spillRuns++
		s.spillBytes += work.handle.Bytes()
	}
	if err != nil {
		return err
	}
	if work.handle == nil {
		return fmt.Errorf("aggregate compaction spill returned a nil handle")
	}

	// Allocate the full replacement while the old owner remains authoritative.
	// Its first entry is a temporary placeholder; the unchanged suffix retains
	// its original order. No borrowed slice header survives the owner move.
	if err = work.replacement.Append(ctx, s.metadata, s.partitions[partition].Values[1:]...); err != nil {
		return err
	}
	work.replacement.Values[0] = aggregateRunRef{handle: work.handle, groups: groups, partition: partition}
	if err = ctx.Err(); err != nil {
		return err
	}
	old := s.partitions[partition]
	s.partitions[partition] = work.replacement
	work.replacement = spillmeta.Slice[aggregateRunRef]{}
	work.handle = nil
	committed = true

	// Publication cannot be rolled back after any deletion. The new owner is
	// authoritative even when only one old file is removed. Store retains the
	// failed file and its real metadata charge until deletion succeeds later.
	var deleteErr error
	for index := 0; index < 2; index++ {
		if e := s.store.Delete(ctx, old.Values[index].handle); e != nil && deleteErr == nil {
			deleteErr = e
		}
		old.Values[index] = aggregateRunRef{}
	}
	clear(old.Values)
	old.Release()
	return deleteErr
}
