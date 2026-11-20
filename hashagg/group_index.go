package hashagg

import (
	"bytes"
	"fmt"
	"gocolumn/memory"
	"unsafe"
)

type groupIndexEntry struct {
	hash   uint64
	offset int
	length int
}

const groupIndexSlotBytes = int64(unsafe.Sizeof(int(0)) + unsafe.Sizeof(groupIndexEntry{}))

// GroupIndex maps canonical byte keys to stable, dense group IDs. IDs are
// assigned in insertion order and do not change when the index grows. Keys
// are opaque: callers encode type, NULL and composite-key boundaries before
// insertion. An empty key is valid and distinct from every non-empty key.
//
// The index owns a single key arena, a hash-slot array and a descriptor array.
// It is not safe for concurrent use. Each worker must own its own index.
type GroupIndex struct {
	slots      []int // Zero is empty; occupied slots contain group ID + 1.
	entries    []groupIndexEntry
	arena      []byte
	size       int
	maxEntries int
	reserve    ReserveFunc
	slotLease  memory.Reservation
	arenaLease memory.Reservation
	closed     bool
}

// NewGroupIndex creates an index with room for the estimated cardinality.
// Each backing allocation is reserved before construction. A nil reserve
// callback opts out of external accounting, as for the other hash tables.
func NewGroupIndex(cardinalityHint int, reserve ReserveFunc) (*GroupIndex, error) {
	n, err := checkedTableCapacity(cardinalityHint, groupIndexSlotBytes)
	if err != nil {
		return nil, err
	}
	lease, err := reserveAllocation(reserve, int64(n)*groupIndexSlotBytes)
	if err != nil {
		return nil, err
	}
	return &GroupIndex{
		slots: make([]int, n), entries: make([]groupIndexEntry, n),
		maxEntries: loadLimit(n, defaultMaxLoadFactor),
		reserve:    reserve, slotLease: lease,
	}, nil
}

// Lookup returns the ID for key without mutating the index or reserving
// memory. It returns false for an absent key, a nil index or a closed index.
func (t *GroupIndex) Lookup(key []byte) (id int, ok bool) {
	id, ok, _ = t.LookupWithCheck(key, nil)
	return
}

// LookupWithCheck is Lookup with cooperative cancellation during probing.
// bytes.Equal is not preemptible per byte; callers bound their key size.
func (t *GroupIndex) LookupWithCheck(key []byte, check func() error) (int, bool, error) {
	if check != nil {
		if e := check(); e != nil {
			return 0, false, e
		}
	}
	if t == nil || t.closed || len(t.slots) == 0 {
		return 0, false, nil
	}
	slot, found, e := t.findWithCheck(key, HashBytes(key), check)
	if e != nil || !found {
		return 0, false, e
	}
	return t.slots[slot] - 1, true, nil
}

// LookupOrInsert returns the existing ID or copies key into the arena and
// assigns the next ID. inserted is true only for a newly committed key. An
// error never inserts a key or consumes an ID; successful capacity growth may
// be retained if a later allocation is denied. Existing IDs remain usable.
// Calling LookupOrInsert on a closed or nil index returns an error.
func (t *GroupIndex) LookupOrInsert(key []byte) (int, bool, error) {
	return t.LookupOrInsertWithCheck(key, nil)
}

// LookupOrInsertWithCheck preserves insertion atomicity. Successful capacity
// growth can remain after a later failure, as in LookupOrInsert.
func (t *GroupIndex) LookupOrInsertWithCheck(key []byte, check func() error) (id int, inserted bool, err error) {
	if check != nil {
		if e := check(); e != nil {
			return 0, false, e
		}
	}

	if t == nil || t.closed || len(t.slots) == 0 {
		return 0, false, fmt.Errorf("group index is closed or uninitialized")
	}
	hash := HashBytes(key)
	slot, found, e := t.findWithCheck(key, hash, check)
	if e != nil {
		return 0, false, e
	}
	if found {
		return t.slots[slot] - 1, false, nil
	}
	if t.size >= t.maxEntries {
		if err := t.growWithCheck(check); err != nil {
			return 0, false, err
		}
		slot, _, e = t.findWithCheck(key, hash, check)
		if e != nil {
			return 0, false, e
		}
	}
	offset, err := t.appendKeyWithCheck(key, check)
	if err != nil {
		return 0, false, err
	}
	id = t.size
	t.entries[id] = groupIndexEntry{hash: hash, offset: offset, length: len(key)}
	t.slots[slot] = id + 1
	t.size++
	return id, true, nil
}

func (t *GroupIndex) find(key []byte, hash uint64) (int, bool) {
	i, ok, _ := t.findWithCheck(key, hash, nil)
	return i, ok
}
func (t *GroupIndex) findWithCheck(key []byte, hash uint64, check func() error) (int, bool, error) {
	mask := uint64(len(t.slots) - 1)
	slot := int(hash & mask)
	steps := 0
	for t.slots[slot] != 0 {
		if steps%64 == 0 && check != nil {
			if e := check(); e != nil {
				return 0, false, e
			}
		}
		steps++
		entry := t.entries[t.slots[slot]-1]
		if entry.hash == hash && entry.length == len(key) && bytes.Equal(t.arena[entry.offset:entry.offset+entry.length], key) {
			return slot, true, nil
		}
		slot = int((uint64(slot) + 1) & mask)
	}
	return slot, false, nil
}

func (t *GroupIndex) grow() error { return t.growWithCheck(nil) }
func (t *GroupIndex) growWithCheck(check func() error) error {
	n, err := checkedGrowCapacity(len(t.slots), groupIndexSlotBytes)
	if err != nil {
		return err
	}
	lease, err := reserveAllocation(t.reserve, int64(n)*groupIndexSlotBytes)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	if check != nil {
		if e := check(); e != nil {
			return e
		}
	}
	slots := make([]int, n)
	entries := make([]groupIndexEntry, n)
	for start := 0; start < t.size; start += 64 {
		if check != nil {
			if e := check(); e != nil {
				return e
			}
		}
		copy(entries[start:min(start+64, t.size)], t.entries[start:min(start+64, t.size)])
	}
	mask := uint64(n - 1)
	for id := 0; id < t.size; id++ {
		if id%64 == 0 && check != nil {
			if e := check(); e != nil {
				return e
			}
		}
		slot := int(entries[id].hash & mask)
		steps := 0
		for slots[slot] != 0 {
			if steps%64 == 0 && check != nil {
				if e := check(); e != nil {
					return e
				}
			}
			steps++
			slot = int((uint64(slot) + 1) & mask)
		}
		slots[slot] = id + 1
	}
	if check != nil {
		if e := check(); e != nil {
			return e
		}
	}
	previous := t.slotLease
	t.slots, t.entries, t.slotLease = slots, entries, lease
	t.maxEntries = loadLimit(n, defaultMaxLoadFactor)
	committed = true
	releaseAllocation(previous)
	return nil
}

func (t *GroupIndex) appendKey(key []byte) (int, error) { return t.appendKeyWithCheck(key, nil) }
func (t *GroupIndex) appendKeyWithCheck(key []byte, check func() error) (int, error) {
	offset := len(t.arena)
	maxInt := int(^uint(0) >> 1)
	if len(key) > maxInt-offset {
		return 0, fmt.Errorf("group index key arena size overflow")
	}
	need := offset + len(key)
	if need <= cap(t.arena) {
		if e := groupIndexCopyWithCheck(t.arena[:need][offset:], key, check); e != nil {
			return 0, e
		}
		t.arena = t.arena[:need]
		return offset, nil
	}
	capacity := cap(t.arena)
	if capacity < 64 {
		capacity = 64
	}
	for capacity < need {
		if capacity > maxInt/2 {
			capacity = need
			break
		}
		capacity *= 2
	}
	lease, err := reserveAllocation(t.reserve, int64(capacity))
	if err != nil {
		return 0, err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	arena := make([]byte, need, capacity)
	if e := groupIndexCopyWithCheck(arena, t.arena, check); e != nil {
		return 0, e
	}
	// key may itself borrow the previous arena. Copy it before releasing that
	// arena's reservation, so replacement overlap is fully accounted for.
	if e := groupIndexCopyWithCheck(arena[offset:], key, check); e != nil {
		return 0, e
	}
	previous := t.arenaLease
	t.arena, t.arenaLease = arena, lease
	committed = true
	releaseAllocation(previous)
	return offset, nil
}

// Key returns a read-only borrowed key for id, or nil for an invalid ID. The
// view must not be modified and is valid only until the next index mutation
// or Close. Callers that retain keys beyond that point must copy them.
func (t *GroupIndex) Key(id int) []byte {
	if t == nil || id < 0 || id >= t.size {
		return nil
	}
	entry := t.entries[id]
	end := entry.offset + entry.length
	return t.arena[entry.offset:end:end]
}

// Len returns the number of distinct keys. It returns zero after Close.
func (t *GroupIndex) Len() int {
	if t == nil {
		return 0
	}
	return t.size
}

// RetainedBytes returns the exact backing-array capacities owned by the index,
// excluding the fixed-size Go object and allocator overhead.
func (t *GroupIndex) RetainedBytes() int64 {
	if t == nil {
		return 0
	}
	return int64(len(t.slots))*groupIndexSlotBytes + int64(cap(t.arena))
}

// Close releases all owned buffers and reservations. It is idempotent.
func (t *GroupIndex) Close() {
	if t == nil {
		return
	}
	slots, arena := t.slotLease, t.arenaLease
	*t = GroupIndex{closed: true}
	releaseAllocation(slots)
	releaseAllocation(arena)
}

func groupIndexCopyWithCheck(dst, src []byte, check func() error) error {
	if check == nil {
		copy(dst, src)
		return nil
	}
	for i := 0; i < len(src); i += 4096 {
		if e := check(); e != nil {
			return e
		}
		copy(dst[i:min(i+4096, len(src))], src[i:min(i+4096, len(src))])
	}
	return check()
}
