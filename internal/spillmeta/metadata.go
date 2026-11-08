// Package spillmeta owns budgeted operator-side spill references. Store handles
// and their paths are separately admitted by memory.MetadataSpillManager.
package spillmeta

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"math"
	"sync"
	"unsafe"
)

// GrowthManager prevents admission from reentering a mutating spill operator.
// The query hard limit still applies to every reservation.
func GrowthManager(operator *memory.OperatorBudget) memory.Manager {
	return growthManager{operator}
}

type growthManager struct{ *memory.OperatorBudget }

// Reserve admits metadata without entering the spill arbiter.
func (m growthManager) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	return m.ReserveGrowth(ctx, bytes)
}

// Slice owns an admitted backing array. It may be moved, but must not be copied
// into multiple owners. Values may be borrowed only until Release or growth.
// Removing values does not release backing-array accounting; Release does.
type Slice[T any] struct {
	Values      []T
	reservation memory.Reservation
}

// Append reserves the full replacement while the old array remains live.
// Admission failure leaves the existing values and capacity unchanged.
func (s *Slice[T]) Append(ctx context.Context, manager memory.Manager, values ...T) error {
	if ctx == nil {
		return fmt.Errorf("spill reference context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(values) > math.MaxInt-len(s.Values) {
		return memory.ErrBudgetExceeded
	}
	need := len(s.Values) + len(values)
	if need > cap(s.Values) {
		capacity := cap(s.Values)
		if capacity > math.MaxInt/2 {
			capacity = math.MaxInt
		} else {
			capacity *= 2
		}
		if capacity < need {
			capacity = need
		}
		width := int64(unsafe.Sizeof(*new(T)))
		if width == 0 {
			width = 1
		}
		// Include allocator size-class slack and the reservation itself. 2x the
		// exact requested bytes bounds Go's rounded backing allocation on all
		// supported element widths, including small and zero-sized elements.
		// References may still point to a deleted, detached handle descriptor
		// until the containing run list is released. Include that fixed object
		// here; Delete clears its variable-size underlying handle/path.
		if width > (math.MaxInt64-256)/2 {
			return memory.ErrBudgetExceeded
		}
		perElement := 2*width + 256
		if int64(capacity) > (math.MaxInt64-128)/perElement {
			return memory.ErrBudgetExceeded
		}
		if manager == nil {
			return fmt.Errorf("spill references require a memory manager")
		}
		reservation, err := manager.Reserve(ctx, 128+int64(capacity)*perElement)
		if err != nil {
			return err
		}
		replacement := make([]T, len(s.Values), capacity)
		copy(replacement, s.Values)
		old := s.reservation
		s.Values, s.reservation = replacement, reservation
		if old != nil {
			old.Release()
		}
	}
	s.Values = append(s.Values, values...)
	return nil
}

// Clear forgets values while keeping the admitted array for reuse.
func (s *Slice[T]) Clear() { clear(s.Values); s.Values = s.Values[:0] }

// Release drops the backing array before returning its reservation.
func (s *Slice[T]) Release() {
	s.Values = nil
	if s.reservation != nil {
		s.reservation.Release()
		s.reservation = nil
	}
}

// Store owns an operator's spill handles without an input-sized ownership map.
// Its linked nodes have individual reservations. Close never closes the shared
// underlying store, which remains responsible for any failed file deletions.
// Callers must release their Slice owners before Close.
type Store struct {
	base      memory.SpillManager
	manager   memory.Manager
	mu        sync.Mutex
	first     *handle
	closed    bool
	closeErr  error
	inflight  sync.WaitGroup
	closeDone chan struct{}
}

type handle struct {
	owner       *Store
	base        memory.SpillHandle
	id          string
	bytes       int64
	kind        memory.SpillKind
	prev, next  *handle
	reservation memory.Reservation
	deleted     bool
}

// ID returns the immutable query-local identifier.
func (h *handle) ID() string { return h.id }

// Bytes returns committed disk bytes.
func (h *handle) Bytes() int64 { return h.bytes }

// Kind returns the payload family.
func (h *handle) Kind() memory.SpillKind { return h.kind }

// NewStore creates a private handle owner over a shared query store. The
// caller's fixed metadata admission covers the Store descriptor itself.
func NewStore(base memory.SpillManager, manager memory.Manager) *Store {
	return &Store{base: base, manager: manager, closeDone: make(chan struct{})}
}

// Spill admits operator references and all store metadata before writing.
func (s *Store) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	if s == nil || s.base == nil {
		return nil, memory.ErrSpillUnsupported
	}
	if ctx == nil {
		return nil, fmt.Errorf("spill context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, memory.ErrSpillClosed
	}
	s.inflight.Add(1)
	s.mu.Unlock()
	defer s.inflight.Done()
	provider, ok := s.base.(memory.MetadataSpillManager)
	if !ok {
		return nil, fmt.Errorf("operator requires budgeted spill metadata: %w", memory.ErrSpillUnsupported)
	}
	if s.manager == nil {
		return nil, fmt.Errorf("spill metadata requires a memory manager")
	}
	reservation, err := s.manager.Reserve(ctx, 256)
	if err != nil {
		return nil, err
	}
	underlying, err := provider.SpillWithMetadata(ctx, object, s.manager)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		err := s.base.Delete(context.Background(), underlying)
		if s.closeErr == nil {
			s.closeErr = err
		}
		reservation.Release()
		return nil, errors.Join(memory.ErrSpillClosed, err)
	}
	h := &handle{owner: s, base: underlying, id: underlying.ID(), bytes: underlying.Bytes(), kind: underlying.Kind(), reservation: reservation, next: s.first}
	if s.first != nil {
		s.first.prev = h
	}
	s.first = h
	return h, nil
}

// Restore validates the private owner before forwarding to the query store.
func (s *Store) Restore(ctx context.Context, ref memory.SpillHandle, schema [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	h, ok := ref.(*handle)
	if !ok || h == nil || h.owner != s {
		return nil, memory.ErrInvalidSpillHandle
	}
	s.mu.Lock()
	underlying := h.base
	s.mu.Unlock()
	if underlying == nil {
		return nil, memory.ErrInvalidSpillHandle
	}
	return s.base.Restore(ctx, underlying, schema, manager)
}

func (s *Store) unlink(h *handle) {
	if h.prev == nil {
		s.first = h.next
	} else {
		h.prev.next = h.next
	}
	if h.next != nil {
		h.next.prev = h.prev
	}
	h.prev, h.next = nil, nil
	h.deleted = true
	h.base = nil
	h.reservation.Release()
	h.reservation = nil
}

// Delete keeps ownership and accounting on a failed delete. It is idempotent.
func (s *Store) Delete(ctx context.Context, ref memory.SpillHandle) error {
	h, ok := ref.(*handle)
	if !ok || h == nil || h.owner != s {
		return memory.ErrInvalidSpillHandle
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if h.deleted {
		return nil
	}
	if err := s.base.Delete(ctx, h.base); err != nil {
		return err
	}
	s.unlink(h)
	return nil
}

// Close deletes every still-owned run, including partial recursive outputs.
// Failed files stay owned and admitted by the underlying query store. The
// private list is discarded, so its own metadata reservations are released.
// Only the first cleanup error is retained, avoiding a run-sized error list.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
	}
	s.mu.Unlock()
	// Admission/writes run without mu. Once closed, no new operation can join
	// this wait group; existing writers clean committed files before Done.
	s.inflight.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.first != nil {
		h := s.first
		if err := s.base.Delete(context.Background(), h.base); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		s.unlink(h)
	}
	close(s.closeDone)
	return s.closeErr
}

var _ memory.SpillManager = (*Store)(nil)
