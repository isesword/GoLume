package hashagg

import (
	"bytes"
	"encoding/binary"
	"gocolumn/aggregate"
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
	reserve    ReserveFunc
	opts       TableOptions
	nullSet    bool
	nullState  aggregate.Int64Vector
}

// NewStringTable creates a string table with default table growth policy.
func NewStringTable(cardinalityHint, arenaHint int, reserve ReserveFunc) *StringTable {
	return NewStringTableWithOptions(cardinalityHint, arenaHint, TableOptions{Reserve: reserve})
}

// NewStringTableWithOptions creates an adaptive string aggregation table.
func NewStringTableWithOptions(cardinalityHint, arenaHint int, opts TableOptions) *StringTable {
	opts = normalizeTableOptions(opts)
	n := tableCapacity(cardinalityHint)
	if arenaHint < 0 {
		arenaHint = 0
	}
	return &StringTable{off: make([]uint32, n), lens: make([]uint32, n), state: aggregate.NewInt64Vector(n, opts.Aggregates), epoch: make([]uint32, n), mask: uint64(n - 1), maxEntries: loadLimit(n, opts.MaxLoadFactor), arena: make([]byte, 0, arenaHint), reserve: opts.Reserve, opts: opts, nullState: aggregate.NewInt64Vector(1, opts.Aggregates)}
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
	need := len(t.arena) + extra
	if need <= cap(t.arena) {
		return nil
	}
	newCap := nextPow2Int(need)
	if newCap < 64 {
		newCap = 64
	}
	delta := newCap - cap(t.arena)
	if t.reserve != nil {
		if err := t.reserve(int64(delta)); err != nil {
			return err
		}
	}
	b := make([]byte, len(t.arena), newCap)
	copy(b, t.arena)
	t.arena = b
	return nil
}

func (t *StringTable) grow() error {
	oldCap := len(t.off)
	newCap := oldCap << 1
	if err := reserveGrowth(t.opts.Reserve, stringTableSlotBytesForCapacity(oldCap, t.opts.Aggregates), stringTableSlotBytesForCapacity(newCap, t.opts.Aggregates)); err != nil {
		return err
	}
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
		key := t.arena[lo : lo+ln]
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
			if int(o+ln) <= len(t.arena) && bytes.Equal(t.arena[o:o+ln], key) {
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

// Materialize appends all current groups, including the SQL NULL group, to dst.
func (t *StringTable) Materialize(dst []GroupString) []GroupString {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			o, ln := t.off[i], t.lens[i]
			st := t.state.Result(i)
			dst = append(dst, GroupString{Key: string(t.arena[o : o+ln]), Valid: true, Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	if t.nullSet {
		st := t.nullState.Result(0)
		dst = append(dst, GroupString{Valid: false, Sum: st.Sum, Count: st.CountRows, State: st})
	}
	return dst
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
