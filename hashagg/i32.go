package hashagg

import (
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/bits"
)

// GroupI32 is a materialized int32 aggregation group. Valid is false for the
// SQL NULL group. State contains the finalized physical aggregate state.
type GroupI32 struct {
	Key   int32
	Valid bool
	Sum   int64
	Count uint64
	State aggregate.Int64Result
}

// I32Table is an adaptive open-addressing aggregation table for int32 keys.
type I32Table struct {
	slotLease  memory.Reservation
	nullLease  memory.Reservation
	keys       []int32
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

// NewI32Table creates a int32 aggregation table using default growth policy.
func NewI32Table(cardinalityHint int) *I32Table {
	table, err := NewI32TableWithOptions(cardinalityHint, TableOptions{})
	if err != nil {
		panic(err)
	}
	return table
}

// NewI32TableWithOptions creates an adaptive int32 aggregation table.
// All backing allocations are reserved before construction. On failure,
// successfully acquired reservations are released and no table is returned.
func NewI32TableWithOptions(cardinalityHint int, opts TableOptions) (*I32Table, error) {
	opts = normalizeTableOptions(opts)
	n, err := checkedTableCapacity(cardinalityHint, i32TableBytesForCapacity(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t := &I32Table{opts: opts}
	committed := false
	defer func() {
		if !committed {
			t.Close()
		}
	}()
	t.slotLease, err = reserveAllocation(opts.Reserve, i32TableBytesForCapacity(n, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	t.nullLease, err = reserveAllocation(opts.Reserve, aggregate.Int64VectorBytesForSlots(1, opts.Aggregates))
	if err != nil {
		return nil, err
	}
	*t = I32Table{slotLease: t.slotLease, nullLease: t.nullLease,
		keys:  make([]int32, n),
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
func (t *I32Table) Close() {
	if t == nil {
		return
	}
	slots := t.slotLease
	null := t.nullLease
	*t = I32Table{}
	releaseAllocation(slots)
	releaseAllocation(null)
}

// Reset clears logical table contents while retaining allocated capacity.
func (t *I32Table) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.size = 0
	t.nullSet = false
}

func (t *I32Table) grow() error {
	oldCap := len(t.keys)
	newCap, err := checkedGrowCapacity(oldCap, i32TableBytesForCapacity(1, t.opts.Aggregates))
	if err != nil {
		return err
	}
	lease, err := reserveAllocation(t.opts.Reserve, i32TableBytesForCapacity(newCap, t.opts.Aggregates))
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			releaseAllocation(lease)
		}
	}()
	keys := make([]int32, newCap)
	state := aggregate.NewInt64Vector(newCap, t.opts.Aggregates)
	epoch := make([]uint32, newCap)
	mask := uint64(newCap - 1)
	for i := range t.epoch {
		if t.epoch[i] != t.gen {
			continue
		}
		key := t.keys[i]
		p := Hash64(uint64(uint32(key))) & mask
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
func (t *I32Table) TryAdd(key int32, value int64) (bool, error) {
	return t.TryAddValue(key, value, true)
}

// TryAddValue updates one non-NULL key while preserving aggregate-value NULL semantics.
func (t *I32Table) TryAddValue(key int32, value int64, valueValid bool) (bool, error) {
	slot, exists := lookupI32Slot(t.keys, t.epoch, t.gen, t.mask, key)
	if exists {
		t.state.AddValue(slot, value, valueValid)
		return false, nil
	}
	return t.insertValueAt(key, slot, value, valueValid)
}

// insertValueAt consumes an empty slot found by lookupSlot. Scalar and vector
// inputs share this insertion and reservation path. A resize invalidates the
// slot, so only that uncommon path probes again against the new backing arrays.
func (t *I32Table) insertValueAt(key int32, slot int, value int64, valueValid bool) (bool, error) {
	if t.size+1 > t.maxEntries {
		for t.size+1 > t.maxEntries {
			if err := t.grow(); err != nil {
				return false, err
			}
		}
		slot, _ = lookupI32Slot(t.keys, t.epoch, t.gen, t.mask, key)
	}
	t.epoch[slot], t.keys[slot] = t.gen, key
	t.state.InitValue(slot, value, valueValid)
	t.size++
	return true, nil
}

// Add updates one non-NULL group and panics if configured growth is rejected.
func (t *I32Table) Add(key int32, value int64) bool {
	created, err := t.TryAdd(key, value)
	if err != nil {
		panic(err)
	}
	return created
}

// TryAddNullable updates a nullable key with a non-NULL aggregate value.
func (t *I32Table) TryAddNullable(key int32, valid bool, value int64) (bool, error) {
	return t.TryAddNullableValue(key, valid, value, true)
}

// TryAddNullableValue updates key/value validity using SQL GROUP BY and aggregate NULL semantics.
func (t *I32Table) TryAddNullableValue(key int32, keyValid bool, value int64, valueValid bool) (bool, error) {
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
func (t *I32Table) TryMergeNullable(key int32, keyValid bool, state aggregate.Int64Result) (bool, error) {
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
		p := Hash64(uint64(uint32(key))) & t.mask
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
func (t *I32Table) AggregateSelected(keys []int32, vals []int64, sel memory.Bitmap) Summary {
	out, err := t.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
	if err != nil {
		panic(err)
	}
	return out
}

// AggregateSelectedNullable aggregates selected rows with nullable keys and non-NULL values.
func (t *I32Table) AggregateSelectedNullable(keys []int32, keyValidity memory.Bitmap, vals []int64, sel memory.Bitmap) (Summary, error) {
	return t.AggregateSelectedNullableValues(keys, keyValidity, vals, nil, sel)
}

// AggregateSelectedNullableValues aggregates selected rows while preserving key and value validity.
func (t *I32Table) AggregateSelectedNullableValues(keys []int32, keyValidity memory.Bitmap, vals []int64, valueValidity memory.Bitmap, sel memory.Bitmap) (Summary, error) {
	updater, sumCount := t.state.SumCountUpdater()
	// Nullability is constant for a vector. Keep the common all-valid loop
	// free of row-level validity checks and nullable-key dispatch.
	if keyValidity == nil && valueValidity == nil {
		if sumCount {
			return t.aggregateSelectedSumCount(keys, vals, sel)
		}
		var out Summary
		for wi, word := range sel {
			base := wi << 6
			for word != 0 {
				i := base + bits.TrailingZeros64(word)
				if i >= len(keys) {
					break
				}
				value := vals[i]
				key := keys[i]
				if slot, exists := lookupI32Slot(t.keys, t.epoch, t.gen, t.mask, key); exists {
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
			k := keys[i]
			keyValid := keyValidity == nil || memory.IsSet(keyValidity, i)
			valueValid := valueValidity == nil || memory.IsSet(valueValidity, i)
			var created bool
			var err error
			if keyValid {
				if slot, exists := lookupI32Slot(t.keys, t.epoch, t.gen, t.mask, k); exists {
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

// aggregateSelectedSumCount keeps reservation and resizing outside the batch
// kernel. A full table can still consume existing keys; only the first actual
// insertion requiring growth ends a run. The stopped row remains unconsumed.
func (t *I32Table) aggregateSelectedSumCount(keys []int32, vals []int64, sel memory.Bitmap) (Summary, error) {
	var out Summary
	for start := 0; start < len(keys); {
		part, next := t.aggregateSelectedSumCountNoGrow(keys, vals, sel, start)
		out.Groups += part.Groups
		out.Checksum += part.Checksum
		out.Rows += part.Rows
		if next == len(keys) {
			break
		}
		// Use the existing growth policy and old+new reservation accounting.
		// Very low load factors can require more than one doubling.
		for t.size+1 > t.maxEntries {
			if err := t.grow(); err != nil {
				return out, err
			}
		}
		start = next
	}
	return out, nil
}

// aggregateSelectedSumCountNoGrow consumes selected, non-NULL SUM+COUNT rows
// with fixed backing arrays. It returns the logical index of the first row
// needing growth, or len(keys) at EOF, and commits exactly the consumed prefix.
// The checked layout dispatch is performed by AggregateSelectedNullableValues.
func (t *I32Table) aggregateSelectedSumCountNoGrow(keys []int32, vals []int64, sel memory.Bitmap, start int) (Summary, int) {
	tableKeys, epochs := t.keys, t.epoch
	mask, gen := t.mask, t.gen
	room := t.maxEntries - t.size
	// Borrow only inside this fixed-backing run. No operation below can grow
	// or release the table; the outer loop discards this view before resizing.
	var state aggregate.SumCountState
	t.state.BorrowSumCountState(&state)
	var out Summary
	for wi := start >> 6; wi < len(sel); wi++ {
		word := sel[wi]
		if wi == start>>6 {
			word &= ^uint64(0) << uint(start&63)
		}
		for word != 0 {
			i := wi<<6 + bits.TrailingZeros64(word)
			if i >= len(keys) {
				t.size += out.Groups
				return out, len(keys)
			}
			key, value := keys[i], vals[i]
			p, exists := lookupI32Slot(tableKeys, epochs, gen, mask, key)
			if exists {
				state.AddValue(p, value, true)
			} else {
				if out.Groups == room {
					t.size += out.Groups
					return out, i
				}
				epochs[p], tableKeys[p] = gen, key
				state.InitValue(p, value, true)
				out.Groups++
			}
			out.Checksum += value
			out.Rows++
			word &= word - 1
		}
	}
	t.size += out.Groups
	return out, len(keys)
}

// AggregateValues aggregates every key/value row in order. Keys and values
// must be non-NULL and vals must cover keys. It returns exactly the successful
// prefix summary if growth fails, leaving the failed row unconsumed.
func (t *I32Table) AggregateValues(keys []int32, vals []int64) (Summary, error) {
	if _, sumCount := t.state.SumCountUpdater(); !sumCount {
		var out Summary
		for i, key := range keys {
			created, err := t.TryAddValue(key, vals[i], true)
			if err != nil {
				return out, err
			}
			if created {
				out.Groups++
			}
			out.Checksum += vals[i]
			out.Rows++
		}
		return out, nil
	}
	var out Summary
	for start := 0; start < len(keys); {
		part, next := t.aggregateSumCountNoGrow(keys, vals, start)
		out.Groups += part.Groups
		out.Checksum += part.Checksum
		out.Rows += part.Rows
		if next == len(keys) {
			break
		}
		for t.size+1 > t.maxEntries {
			if err := t.grow(); err != nil {
				return out, err
			}
		}
		start = next
	}
	return out, nil
}

// aggregateSumCountNoGrow is the dense traversal of the shared I32 probe and
// SUM+COUNT transitions. Its borrowed state ends before any growth or release.
func (t *I32Table) aggregateSumCountNoGrow(keys []int32, vals []int64, start int) (Summary, int) {
	tableKeys, epochs := t.keys, t.epoch
	mask, gen := t.mask, t.gen
	room := t.maxEntries - t.size
	var state aggregate.SumCountState
	t.state.BorrowSumCountState(&state)
	var out Summary
	for i := start; i < len(keys); i++ {
		key, value := keys[i], vals[i]
		p, exists := lookupI32Slot(tableKeys, epochs, gen, mask, key)
		if exists {
			state.AddValue(p, value, true)
		} else {
			if out.Groups == room {
				t.size += out.Groups
				return out, i
			}
			epochs[p], tableKeys[p] = gen, key
			state.InitValue(p, value, true)
			out.Groups++
		}
		out.Checksum += value
		out.Rows++
	}
	t.size += out.Groups
	return out, len(keys)
}

// AggregateIndexedNullableValues aggregates rows in index order, including
// repeated indices, preserving key and value NULL semantics. Indices must be
// valid for keys and vals; validity bitmaps, when present, must cover those rows.
func (t *I32Table) AggregateIndexedNullableValues(keys []int32, keyValidity memory.Bitmap, vals []int64, valueValidity memory.Bitmap, indices []int) (Summary, error) {
	updater, sumCount := t.state.SumCountUpdater()
	var out Summary
	for _, i := range indices {
		key := keys[i]
		value := vals[i]
		keyValid := keyValidity == nil || memory.IsSet(keyValidity, i)
		valueValid := valueValidity == nil || memory.IsSet(valueValidity, i)
		var created bool
		var err error
		if keyValid {
			if slot, exists := lookupI32Slot(t.keys, t.epoch, t.gen, t.mask, key); exists {
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

// lookupI32Slot is the shared fixed-backing probe for scalar and batch paths.
func lookupI32Slot(keys []int32, epochs []uint32, gen uint32, mask uint64, key int32) (int, bool) {
	p := Hash64(uint64(uint32(key))) & mask
	for epochs[p] == gen {
		if keys[p] == key {
			return int(p), true
		}
		p = (p + 1) & mask
	}
	return int(p), false
}

// Materialize appends all current groups, including the SQL NULL group, to dst.
func (t *I32Table) Materialize(dst []GroupI32) []GroupI32 {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			st := t.state.Result(i)
			dst = append(dst, GroupI32{Key: t.keys[i], Valid: true, Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	if t.nullSet {
		st := t.nullState.Result(0)
		dst = append(dst, GroupI32{Valid: false, Sum: st.Sum, Count: st.CountRows, State: st})
	}
	return dst
}

// Visit calls fn once for every current group without materializing a slice.
func (t *I32Table) Visit(fn func(key int32, valid bool, state aggregate.Int64Result) error) error {
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

// VisitChunk visits at most limit groups and advances cursor without
// materializing a group slice. cursor must start at zero and belongs to this
// table generation. done reports that no groups remain.
func (t *I32Table) VisitChunk(cursor *int, limit int, fn func(key int32, valid bool, state aggregate.Int64Result) error) (done bool, err error) {
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
func (t *I32Table) Capacity() int { return len(t.keys) }

// Len returns the number of logical groups, including NULL when present.
func (t *I32Table) Len() int {
	n := t.size
	if t.nullSet {
		n++
	}
	return n
}

// RetainedBytes returns the table-owned key, epoch, and aggregate-state bytes.
func (t *I32Table) RetainedBytes() int64 {
	if t == nil || t.Capacity() == 0 {
		return 0
	}
	return i32TableBytesForCapacity(len(t.keys), t.opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, t.opts.Aggregates)
}

func i32TableBytesForCapacity(capacity int, layout aggregate.Layout) int64 {
	return int64(capacity)*(4+4) + aggregate.Int64VectorBytesForSlots(capacity, layout)
}

// EstimateI32TableBytes estimates initial retained bytes for cardinalityHint using the default aggregate layout.
func EstimateI32TableBytes(cardinalityHint int) int64 {
	return EstimateI32TableBytesWithOptions(cardinalityHint, TableOptions{})
}

// EstimateI32TableBytesWithOptions estimates retained bytes for the configured aggregate layout.
func EstimateI32TableBytesWithOptions(cardinalityHint int, opts TableOptions) int64 {
	opts = normalizeTableOptions(opts)
	return i32TableBytesForCapacity(tableCapacity(cardinalityHint), opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, opts.Aggregates)
}
