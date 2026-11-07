package hashagg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/bits"
)

// GroupString is a materialized variable-width string group. Valid is false
// for the SQL NULL group.
type GroupString struct {
	Key   string
	Valid bool
	Sum   int64
	Count uint64
	State aggregate.Int64Result
}

// StringTable is an adaptive open-addressing table for offset+bytes string
// keys. First-seen group keys are copied into an owned arena so input batches
// may be reused after aggregation.
type StringTable struct {
	off        []uint32
	lens       []uint32
	state      aggregate.Int64Vector
	epoch      []uint32
	gen        uint32
	mask       uint64
	size       int
	maxEntries int
	arena      []byte
	slotLease  memory.Reservation
	nullLease  memory.Reservation
	arenaLease memory.Reservation
	opts       TableOptions
	nullSet    bool
	nullState  aggregate.Int64Vector
}

// NewStringTable creates a string table with default table growth policy.
func NewStringTable(cardinalityHint, arenaHint int, reserve ReserveFunc) (*StringTable, error) {
	return NewStringTableWithOptions(cardinalityHint, arenaHint, TableOptions{Reserve: reserve})
}

// NewStringTableWithOptions creates an adaptive string aggregation table.
// All backing allocations are reserved before construction. On failure,
// successfully acquired reservations are released and no table is returned.
func NewStringTableWithOptions(cardinalityHint, arenaHint int, opts TableOptions) (*StringTable, error) {
	opts = normalizeTableOptions(opts)
	n, err := checkedTableCapacity(cardinalityHint, stringTableSlotBytesForCapacity(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	if arenaHint < 0 {
		arenaHint = 0
	}
	if uint64(arenaHint) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("string arena hint exceeds uint32 offsets")
	}
	t := &StringTable{opts: opts}
	committed := false
	defer func() {
		if !committed {
			t.Close()
		}
	}()
	t.slotLease, err = reserveAllocation(opts.Reserve, stringTableSlotBytesForCapacity(n, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t.nullLease, err = reserveAllocation(opts.Reserve, aggregate.Int64VectorBytesForSlots(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t.arenaLease, err = reserveAllocation(opts.Reserve, int64(arenaHint))
	if err != nil {
		return nil, err
	}
	t.off, t.lens = make([]uint32, n), make([]uint32, n)
	t.state = aggregate.NewInt64Vector(n, opts.Aggregates)
	t.epoch = make([]uint32, n)
	t.mask, t.maxEntries = uint64(n-1), loadLimit(n, opts.MaxLoadFactor)
	t.arena = make([]byte, 0, arenaHint)
	t.nullState = aggregate.NewInt64Vector(1, opts.Aggregates)
	// Epoch zero belongs to freshly allocated, unoccupied slots.
	t.gen = 1
	committed = true
	return t, nil
}

// Close releases all table-owned buffers and reservations. It is idempotent.
// The table must not be used afterward, except for Close and size inspection.
func (t *StringTable) Close() {
	if t == nil {
		return
	}
	slots, null, arena := t.slotLease, t.nullLease, t.arenaLease
	*t = StringTable{}
	releaseAllocation(slots)
	releaseAllocation(null)
	releaseAllocation(arena)
}

// Reset clears logical table contents and arena length while retaining capacity.
func (t *StringTable) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.size = 0
	t.arena = t.arena[:0]
	t.nullSet = false
}

// ArenaCap returns retained key-arena capacity in bytes.
func (t *StringTable) ArenaCap() int { return cap(t.arena) }

func (t *StringTable) ensureArena(extra int) error {
	if extra < 0 || extra > int(^uint(0)>>1)-len(t.arena) {
		return fmt.Errorf("string arena size overflow")
	}
	if uint64(len(t.arena))+uint64(extra) > uint64(^uint32(0)) {
		return fmt.Errorf("string arena length exceeds uint32 offsets")
	}
	need := len(t.arena) + extra
	if need <= cap(t.arena) {
		return nil
	}
	newCap := nextPow2Int(need)
	if newCap <= 0 {
		return fmt.Errorf("string arena capacity overflow")
	}
	if newCap < 64 {
		newCap = 64
	}
	lease, err := reserveAllocation(t.opts.Reserve, int64(newCap))
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	b := make([]byte, len(t.arena), newCap)
	copy(b, t.arena)
	t.arena = b
	oldLease := t.arenaLease
	t.arenaLease = lease
	committed = true
	releaseAllocation(oldLease)
	return nil
}

func (t *StringTable) grow() error {
	oldCap := len(t.off)
	newCap, err := checkedGrowCapacity(oldCap, stringTableSlotBytesForCapacity(1, t.opts.Aggregates))
	if err != nil {
		return err
	}
	lease, err := reserveAllocation(t.opts.Reserve, stringTableSlotBytesForCapacity(newCap, t.opts.Aggregates))
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	off := make([]uint32, newCap)
	lens := make([]uint32, newCap)
	state := aggregate.NewInt64Vector(newCap, t.opts.Aggregates)
	epoch := make([]uint32, newCap)
	mask := uint64(newCap - 1)
	for i := range t.epoch {
		if t.epoch[i] != t.gen {
			continue
		}
		lo, ln := t.off[i], t.lens[i]
		key := t.arena[uint64(lo) : uint64(lo)+uint64(ln)]
		p := HashBytes(key) & mask
		for epoch[p] == t.gen {
			p = (p + 1) & mask
		}
		epoch[p] = t.gen
		off[p], lens[p] = lo, ln
		t.state.Copy(&state, i, int(p))
	}
	t.off, t.lens, t.state, t.epoch = off, lens, state, epoch
	t.mask = mask
	t.maxEntries = loadLimit(newCap, t.opts.MaxLoadFactor)
	oldLease := t.slotLease
	t.slotLease = lease
	committed = true
	releaseAllocation(oldLease)
	return nil
}

// Add updates one non-NULL string group with a non-NULL aggregate value.
func (t *StringTable) Add(key []byte, value int64) (bool, error) {
	return t.AddValue(key, value, true)
}

// AddValue updates one non-NULL string key while preserving aggregate-value NULL semantics.
func (t *StringTable) AddValue(key []byte, value int64, valueValid bool) (bool, error) {
	for {
		h := HashBytes(key)
		p := h & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				if err := t.ensureArena(len(key)); err != nil {
					return false, err
				}
				o := len(t.arena)
				t.arena = append(t.arena, key...)
				t.epoch[p] = t.gen
				t.off[p], t.lens[p] = uint32(o), uint32(len(key))
				t.state.InitValue(int(p), value, valueValid)
				t.size++
				return true, nil
			}
			o, ln := t.off[p], t.lens[p]
			if uint64(o)+uint64(ln) <= uint64(len(t.arena)) && bytes.Equal(t.arena[uint64(o):uint64(o)+uint64(ln)], key) {
				t.state.AddValue(int(p), value, valueValid)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// AddNullable updates a nullable string key with a non-NULL aggregate value.
func (t *StringTable) AddNullable(key []byte, valid bool, value int64) (bool, error) {
	return t.AddNullableValue(key, valid, value, true)
}

// AddNullableValue updates string-key and aggregate-value validity using SQL semantics.
func (t *StringTable) AddNullableValue(key []byte, keyValid bool, value int64, valueValid bool) (bool, error) {
	if keyValid {
		return t.AddValue(key, value, valueValid)
	}
	created := !t.nullSet
	if created {
		t.nullSet = true
		t.nullState.InitValue(0, value, valueValid)
	} else {
		t.nullState.AddValue(0, value, valueValid)
	}
	return created, nil
}

// TryMergeNullable combines a mergeable partial state into a nullable string key.
func (t *StringTable) TryMergeNullable(key []byte, keyValid bool, state aggregate.Int64Result) (bool, error) {
	if !keyValid {
		created := !t.nullSet
		if created {
			t.nullSet = true
			t.nullState.InitResult(0, state)
		} else {
			t.nullState.MergeResult(0, state)
		}
		return created, nil
	}
	for {
		h := HashBytes(key)
		p := h & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				if err := t.ensureArena(len(key)); err != nil {
					return false, err
				}
				o := len(t.arena)
				t.arena = append(t.arena, key...)
				t.epoch[p] = t.gen
				t.off[p], t.lens[p] = uint32(o), uint32(len(key))
				t.state.InitResult(int(p), state)
				t.size++
				return true, nil
			}
			o, ln := t.off[p], t.lens[p]
			if uint64(o)+uint64(ln) <= uint64(len(t.arena)) && bytes.Equal(t.arena[uint64(o):uint64(o)+uint64(ln)], key) {
				t.state.MergeResult(int(p), state)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// Materialize appends all current groups, including the SQL NULL group, to dst.
func (t *StringTable) Materialize(dst []GroupString) []GroupString {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			o, ln := t.off[i], t.lens[i]
			st := t.state.Result(i)
			dst = append(dst, GroupString{Key: string(t.arena[uint64(o) : uint64(o)+uint64(ln)]), Valid: true, Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	if t.nullSet {
		st := t.nullState.Result(0)
		dst = append(dst, GroupString{Valid: false, Sum: st.Sum, Count: st.CountRows, State: st})
	}
	return dst
}

// Visit calls fn once for every current group without materializing keys.
// key is borrowed from the table arena and is valid only during the callback.
func (t *StringTable) Visit(fn func(key []byte, valid bool, state aggregate.Int64Result) error) error {
	if fn == nil {
		return nil
	}
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			o, ln := t.off[i], t.lens[i]
			if err := fn(t.arena[uint64(o):uint64(o)+uint64(ln)], true, t.state.Result(i)); err != nil {
				return err
			}
		}
	}
	if t.nullSet {
		return fn(nil, false, t.nullState.Result(0))
	}
	return nil
}

// VisitChunk visits at most limit groups and advances cursor. Keys remain
// borrowed from the table arena for the duration of fn.
func (t *StringTable) VisitChunk(cursor *int, limit int, fn func(key []byte, valid bool, state aggregate.Int64Result) error) (bool, error) {
	if cursor == nil || limit < 1 || fn == nil {
		return false, nil
	}
	emitted := 0
	for *cursor < len(t.epoch) {
		i := *cursor
		*cursor = i + 1
		if t.epoch[i] != t.gen {
			continue
		}
		o, ln := t.off[i], t.lens[i]
		if err := fn(t.arena[uint64(o):uint64(o)+uint64(ln)], true, t.state.Result(i)); err != nil {
			return false, err
		}
		emitted++
		if emitted == limit {
			return !t.nullSet && *cursor >= len(t.epoch), nil
		}
	}
	if t.nullSet && *cursor == len(t.epoch) {
		*cursor++
		if err := fn(nil, false, t.nullState.Result(0)); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Capacity returns the current number of hash slots.
func (t *StringTable) Capacity() int { return len(t.off) }

// Len returns the number of logical groups, including NULL when present.
func (t *StringTable) Len() int {
	n := t.size
	if t.nullSet {
		n++
	}
	return n
}

// RetainedBytes returns slot/state bytes plus the owned key-arena capacity.
func (t *StringTable) RetainedBytes() int64 {
	if t == nil || t.Capacity() == 0 {
		return 0
	}
	return stringTableSlotBytesForCapacity(len(t.off), t.opts.Aggregates) + int64(cap(t.arena)) + aggregate.Int64VectorBytesForSlots(1, t.opts.Aggregates)
}

func stringTableSlotBytesForCapacity(capacity int, layout aggregate.Layout) int64 {
	return int64(capacity)*(4+4+4) + aggregate.Int64VectorBytesForSlots(capacity, layout)
}

// EstimateStringTableBytes estimates initial retained bytes including arenaHint.
func EstimateStringTableBytes(cardinalityHint, arenaHint int) int64 {
	return EstimateStringTableBytesWithOptions(cardinalityHint, arenaHint, TableOptions{})
}

// EstimateStringTableBytesWithOptions estimates retained slot+arena bytes for the configured aggregate layout.
func EstimateStringTableBytesWithOptions(cardinalityHint, arenaHint int, opts TableOptions) int64 {
	opts = normalizeTableOptions(opts)
	return stringTableSlotBytesForCapacity(tableCapacity(cardinalityHint), opts.Aggregates) + int64(arenaHint) + aggregate.Int64VectorBytesForSlots(1, opts.Aggregates)
}

// HashBytes returns the scalar variable-width key hash used by StringTable.
func HashBytes(b []byte) uint64 {
	const p1 uint64 = 11400714785074694791
	const p2 uint64 = 14029467366897019727
	const p3 uint64 = 1609587929392839161
	const p4 uint64 = 9650029242287828579
	const p5 uint64 = 2870177450012600261
	i := 0
	h := p5 + uint64(len(b)) + 0x9e3779b97f4a7c15
	for i+8 <= len(b) {
		k := binary.LittleEndian.Uint64(b[i:])
		k *= p2
		k = bits.RotateLeft64(k, 31)
		k *= p1
		h ^= k
		h = bits.RotateLeft64(h, 27)*p1 + p4
		i += 8
	}
	if i+4 <= len(b) {
		k := uint64(binary.LittleEndian.Uint32(b[i:]))
		h ^= k * p1
		h = bits.RotateLeft64(h, 23)*p2 + p3
		i += 4
	}
	for i < len(b) {
		h ^= uint64(b[i]) * p5
		h = bits.RotateLeft64(h, 11) * p1
		i++
	}
	h ^= h >> 33
	h *= p2
	h ^= h >> 29
	h *= p3
	h ^= h >> 32
	return h
}

func tableCapacity(cardinalityHint int) int {
	if cardinalityHint < 8 {
		cardinalityHint = 8
	}
	n := 1
	for n < cardinalityHint*2 {
		n <<= 1
	}
	return n
}

func nextPow2Int(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}
