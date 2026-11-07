package hashagg

import (
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/bits"
)

// GroupU64 is a materialized uint64 aggregation group. Valid is false for the
// SQL NULL group. State contains the finalized physical aggregate state.
type GroupU64 struct {
	Key   uint64
	Valid bool
	Sum   int64
	Count uint64
	State aggregate.Int64Result
}

// U64Table is an adaptive open-addressing aggregation table for uint64 keys.
type U64Table struct {
	slotLease  memory.Reservation
	nullLease  memory.Reservation
	keys       []uint64
	state      aggregate.Int64Vector
	epoch      []uint32
	gen        uint32
	mask       uint64
	size       int
	maxEntries int
	opts       TableOptions
	nullSet    bool
	nullState  aggregate.Int64Vector
}

// NewU64Table creates a uint64 aggregation table using default growth policy.
func NewU64Table(cardinalityHint int) *U64Table {
	table, err := NewU64TableWithOptions(cardinalityHint, TableOptions{})
	if err != nil {
		panic(err)
	}
	return table
}

// NewU64TableWithOptions creates an adaptive uint64 aggregation table.
// All backing allocations are reserved before construction. On failure,
// successfully acquired reservations are released and no table is returned.
func NewU64TableWithOptions(cardinalityHint int, opts TableOptions) (*U64Table, error) {
	opts = normalizeTableOptions(opts)
	n, err := checkedTableCapacity(cardinalityHint, u64TableBytesForCapacity(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t := &U64Table{opts: opts}
	committed := false
	defer func() {
		if !committed {
			t.Close()
		}
	}()
	t.slotLease, err = reserveAllocation(opts.Reserve, u64TableBytesForCapacity(n, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t.nullLease, err = reserveAllocation(opts.Reserve, aggregate.Int64VectorBytesForSlots(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	*t = U64Table{slotLease: t.slotLease, nullLease: t.nullLease,
		keys:  make([]uint64, n),
		state: aggregate.NewInt64Vector(n, opts.Aggregates),
		epoch: make([]uint32, n), mask: uint64(n - 1),
		maxEntries: loadLimit(n, opts.MaxLoadFactor), opts: opts,
		nullState: aggregate.NewInt64Vector(1, opts.Aggregates),
	}
	// Epoch zero belongs to freshly allocated, unoccupied slots.
	t.gen = 1
	committed = true
	return t, nil
}

// Close releases all table-owned buffers and reservations. It is idempotent.
// The table must not be used afterward, except for Close and size inspection.
func (t *U64Table) Close() {
	if t == nil {
		return
	}
	slots := t.slotLease
	null := t.nullLease
	*t = U64Table{}
	releaseAllocation(slots)
	releaseAllocation(null)
}

// Reset clears logical table contents while retaining allocated capacity.
func (t *U64Table) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.size = 0
	t.nullSet = false
}

func (t *U64Table) grow() error {
	oldCap := len(t.keys)
	newCap, err := checkedGrowCapacity(oldCap, u64TableBytesForCapacity(1, t.opts.Aggregates))
	if err != nil {
		return err
	}
	lease, err := reserveAllocation(t.opts.Reserve, u64TableBytesForCapacity(newCap, t.opts.Aggregates))
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	keys := make([]uint64, newCap)
	state := aggregate.NewInt64Vector(newCap, t.opts.Aggregates)
	epoch := make([]uint32, newCap)
	mask := uint64(newCap - 1)
	for i := range t.epoch {
		if t.epoch[i] != t.gen {
			continue
		}
		key := t.keys[i]
		p := Hash64(key) & mask
		for epoch[p] == t.gen {
			p = (p + 1) & mask
		}
		epoch[p] = t.gen
		keys[p] = key
		t.state.Copy(&state, i, int(p))
	}
	t.keys, t.state, t.epoch = keys, state, epoch
	t.mask = mask
	t.maxEntries = loadLimit(newCap, t.opts.MaxLoadFactor)
	oldLease := t.slotLease
	t.slotLease = lease
	committed = true
	releaseAllocation(oldLease)
	return nil
}

// TryAdd updates one non-NULL key with a non-NULL aggregate value.
func (t *U64Table) TryAdd(key uint64, value int64) (bool, error) {
	return t.TryAddValue(key, value, true)
}

// TryAddValue updates one non-NULL key while preserving aggregate-value NULL semantics.
func (t *U64Table) TryAddValue(key uint64, value int64, valueValid bool) (bool, error) {
	slot, exists := t.lookupSlot(key)
	if exists {
		t.state.AddValue(slot, value, valueValid)
		return false, nil
	}
	return t.insertValueAt(key, slot, value, valueValid)
}

// insertValueAt consumes an empty slot found by lookupSlot. Scalar and vector
// inputs share this insertion and reservation path. A resize invalidates the
// slot, so only that uncommon path probes again against the new backing arrays.
func (t *U64Table) insertValueAt(key uint64, slot int, value int64, valueValid bool) (bool, error) {
	if t.size+1 > t.maxEntries {
		for t.size+1 > t.maxEntries {
			if err := t.grow(); err != nil {
				return false, err
			}
		}
		slot, _ = t.lookupSlot(key)
	}
	t.epoch[slot], t.keys[slot] = t.gen, key
	t.state.InitValue(slot, value, valueValid)
	t.size++
	return true, nil
}

// Add updates one non-NULL group and panics if configured growth is rejected.
func (t *U64Table) Add(key uint64, value int64) bool {
	created, err := t.TryAdd(key, value)
	if err != nil {
		panic(err)
	}
	return created
}

// TryAddNullable updates a nullable key with a non-NULL aggregate value.
func (t *U64Table) TryAddNullable(key uint64, valid bool, value int64) (bool, error) {
	return t.TryAddNullableValue(key, valid, value, true)
}

// TryAddNullableValue updates key/value validity using SQL GROUP BY and aggregate NULL semantics.
func (t *U64Table) TryAddNullableValue(key uint64, keyValid bool, value int64, valueValid bool) (bool, error) {
	if keyValid {
		return t.TryAddValue(key, value, valueValid)
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

// TryMergeNullable combines a mergeable partial state into key.
func (t *U64Table) TryMergeNullable(key uint64, keyValid bool, state aggregate.Int64Result) (bool, error) {
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
		p := Hash64(key) & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				t.epoch[p], t.keys[p] = t.gen, key
				t.state.InitResult(int(p), state)
				t.size++
				return true, nil
			}
			if t.keys[p] == key {
				t.state.MergeResult(int(p), state)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// AggregateSelected aggregates selected non-NULL keys and non-NULL values.
func (t *U64Table) AggregateSelected(keys []int64, vals []int64, sel memory.Bitmap) Summary {
	out, err := t.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
	if err != nil {
		panic(err)
	}
	return out
}

// AggregateSelectedNullable aggregates selected rows with nullable keys and non-NULL values.
func (t *U64Table) AggregateSelectedNullable(keys []int64, keyValidity memory.Bitmap, vals []int64, sel memory.Bitmap) (Summary, error) {
	return t.AggregateSelectedNullableValues(keys, keyValidity, vals, nil, sel)
}

// AggregateSelectedNullableValues aggregates selected rows while preserving key and value validity.
func (t *U64Table) AggregateSelectedNullableValues(keys []int64, keyValidity memory.Bitmap, vals []int64, valueValidity memory.Bitmap, sel memory.Bitmap) (Summary, error) {
	updater, sumCount := t.state.SumCountUpdater()
	// Nullability is constant for a vector. Keep the common all-valid loop
	// free of row-level validity checks and nullable-key dispatch.
	if keyValidity == nil && valueValidity == nil {
		var out Summary
		for wi, word := range sel {
			base := wi << 6
			for word != 0 {
				i := base + bits.TrailingZeros64(word)
				if i >= len(keys) {
					break
				}
				value := vals[i]
				key := uint64(keys[i])
				if slot, exists := t.lookupSlot(key); exists {
					if sumCount {
						updater.AddValue(slot, value, true)
					} else {
						t.state.AddValue(slot, value, true)
					}
				} else {
					// Insertions, rehashing, and reservation failures have a
					// single implementation shared with the scalar path.
					created, err := t.insertValueAt(key, slot, value, true)
					if err != nil {
						return out, err
					}
					if created {
						out.Groups++
					}
				}
				out.Checksum += value
				out.Rows++
				word &= word - 1
			}
		}
		return out, nil
	}

	var out Summary
	for wi, word := range sel {
		base := wi << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			i := base + bit
			if i >= len(keys) {
				break
			}
			k := uint64(keys[i])
			keyValid := keyValidity == nil || memory.IsSet(keyValidity, i)
			valueValid := valueValidity == nil || memory.IsSet(valueValidity, i)
			var created bool
			var err error
			if keyValid {
				if slot, exists := t.lookupSlot(k); exists {
					if sumCount {
						updater.AddValue(slot, vals[i], valueValid)
					} else {
						t.state.AddValue(slot, vals[i], valueValid)
					}
				} else {
					created, err = t.insertValueAt(k, slot, vals[i], valueValid)
				}
			} else {
				created, err = t.TryAddNullableValue(k, false, vals[i], valueValid)
			}
			if err != nil {
				return out, err
			}
			if created {
				out.Groups++
			}
			if valueValid {
				out.Checksum += vals[i]
			}
			out.Rows++
			word &= word - 1
		}
	}
	return out, nil
}

// AggregateIndexedNullableValues aggregates rows in index order, including
// repeated indices, preserving key and value NULL semantics. Indices must be
// valid for keys and vals; validity bitmaps, when present, must cover those rows.
func (t *U64Table) AggregateIndexedNullableValues(keys []int64, keyValidity memory.Bitmap, vals []int64, valueValidity memory.Bitmap, indices []int) (Summary, error) {
	updater, sumCount := t.state.SumCountUpdater()
	var out Summary
	for _, i := range indices {
		key := uint64(keys[i])
		value := vals[i]
		keyValid := keyValidity == nil || memory.IsSet(keyValidity, i)
		valueValid := valueValidity == nil || memory.IsSet(valueValidity, i)
		var created bool
		var err error
		if keyValid {
			if slot, exists := t.lookupSlot(key); exists {
				if sumCount {
					updater.AddValue(slot, value, valueValid)
				} else {
					t.state.AddValue(slot, value, valueValid)
				}
			} else {
				created, err = t.insertValueAt(key, slot, value, valueValid)
			}
		} else {
			created, err = t.TryAddNullableValue(key, false, value, valueValid)
		}
		if err != nil {
			return out, err
		}
		if created {
			out.Groups++
		}
		if valueValid {
			out.Checksum += value
		}
		out.Rows++
	}
	return out, nil
}

// lookupSlot returns the existing key slot or the first empty insertion slot.
// The returned slot remains valid until the next resize or Reset.
func (t *U64Table) lookupSlot(key uint64) (int, bool) {
	p := Hash64(key) & t.mask
	for t.epoch[p] == t.gen {
		if t.keys[p] == key {
			return int(p), true
		}
		p = (p + 1) & t.mask
	}
	return int(p), false
}

// Materialize appends all current groups, including the SQL NULL group, to dst.
func (t *U64Table) Materialize(dst []GroupU64) []GroupU64 {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			st := t.state.Result(i)
			dst = append(dst, GroupU64{Key: t.keys[i], Valid: true, Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	if t.nullSet {
		st := t.nullState.Result(0)
		dst = append(dst, GroupU64{Valid: false, Sum: st.Sum, Count: st.CountRows, State: st})
	}
	return dst
}

// Visit calls fn once for every current group without materializing a slice.
func (t *U64Table) Visit(fn func(key uint64, valid bool, state aggregate.Int64Result) error) error {
	if fn == nil {
		return nil
	}
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			if err := fn(t.keys[i], true, t.state.Result(i)); err != nil {
				return err
			}
		}
	}
	if t.nullSet {
		return fn(0, false, t.nullState.Result(0))
	}
	return nil
}

// VisitChunk visits at most limit groups and advances cursor.
func (t *U64Table) VisitChunk(cursor *int, limit int, fn func(key uint64, valid bool, state aggregate.Int64Result) error) (bool, error) {
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
		if err := fn(t.keys[i], true, t.state.Result(i)); err != nil {
			return false, err
		}
		emitted++
		if emitted == limit {
			return !t.nullSet && *cursor >= len(t.epoch), nil
		}
	}
	if t.nullSet && *cursor == len(t.epoch) {
		*cursor++
		if err := fn(0, false, t.nullState.Result(0)); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Capacity returns the current number of hash slots.
func (t *U64Table) Capacity() int { return len(t.keys) }

// Len returns the number of logical groups, including NULL when present.
func (t *U64Table) Len() int {
	n := t.size
	if t.nullSet {
		n++
	}
	return n
}

// RetainedBytes returns the table-owned key, epoch, and aggregate-state bytes.
func (t *U64Table) RetainedBytes() int64 {
	if t == nil || t.Capacity() == 0 {
		return 0
	}
	return u64TableBytesForCapacity(len(t.keys), t.opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, t.opts.Aggregates)
}

func u64TableBytesForCapacity(capacity int, layout aggregate.Layout) int64 {
	return int64(capacity)*(8+4) + aggregate.Int64VectorBytesForSlots(capacity, layout)
}

// EstimateU64TableBytes estimates initial retained bytes for cardinalityHint using the default aggregate layout.
func EstimateU64TableBytes(cardinalityHint int) int64 {
	return EstimateU64TableBytesWithOptions(cardinalityHint, TableOptions{})
}

// EstimateU64TableBytesWithOptions estimates retained bytes for the configured aggregate layout.
func EstimateU64TableBytesWithOptions(cardinalityHint int, opts TableOptions) int64 {
	opts = normalizeTableOptions(opts)
	return u64TableBytesForCapacity(tableCapacity(cardinalityHint), opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, opts.Aggregates)
}
