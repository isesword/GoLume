package hashagg

import (
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/bits"
)

// GroupU32 is a materialized uint32 aggregation group. Valid is false for the
// SQL NULL group. State contains the finalized physical aggregate state.
type GroupU32 struct {
	Key   uint32
	Valid bool
	Sum   int64
	Count uint64
	State aggregate.Int64Result
}

// U32Table is an adaptive open-addressing aggregation table for uint32 keys.
type U32Table struct {
	keys       []uint32
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

// NewU32Table creates a uint32 aggregation table using default growth policy.
func NewU32Table(cardinalityHint int) *U32Table {
	return NewU32TableWithOptions(cardinalityHint, TableOptions{})
}

// NewU32TableWithOptions creates an adaptive uint32 aggregation table.
func NewU32TableWithOptions(cardinalityHint int, opts TableOptions) *U32Table {
	opts = normalizeTableOptions(opts)
	n := tableCapacity(cardinalityHint)
	return &U32Table{
		keys:  make([]uint32, n),
		state: aggregate.NewInt64Vector(n, opts.Aggregates),
		epoch: make([]uint32, n), mask: uint64(n - 1),
		maxEntries: loadLimit(n, opts.MaxLoadFactor), opts: opts,
		nullState: aggregate.NewInt64Vector(1, opts.Aggregates),
	}
}

// Reset clears logical table contents while retaining allocated capacity.
func (t *U32Table) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.size = 0
	t.nullSet = false
}

func (t *U32Table) grow() error {
	oldCap := len(t.keys)
	newCap := oldCap << 1
	if err := reserveGrowth(t.opts.Reserve, u32TableBytesForCapacity(oldCap, t.opts.Aggregates), u32TableBytesForCapacity(newCap, t.opts.Aggregates)); err != nil {
		return err
	}
	keys := make([]uint32, newCap)
	state := aggregate.NewInt64Vector(newCap, t.opts.Aggregates)
	epoch := make([]uint32, newCap)
	mask := uint64(newCap - 1)
	for i := range t.epoch {
		if t.epoch[i] != t.gen {
			continue
		}
		key := t.keys[i]
		p := Hash64(uint64(key)) & mask
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
	return nil
}

// TryAdd updates one non-NULL key with a non-NULL aggregate value.
func (t *U32Table) TryAdd(key uint32, value int64) (bool, error) {
	return t.TryAddValue(key, value, true)
}

// TryAddValue updates one non-NULL key while preserving aggregate-value NULL semantics.
func (t *U32Table) TryAddValue(key uint32, value int64, valueValid bool) (bool, error) {
	for {
		p := Hash64(uint64(key)) & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				t.epoch[p], t.keys[p] = t.gen, key
				t.state.InitValue(int(p), value, valueValid)
				t.size++
				return true, nil
			}
			if t.keys[p] == key {
				t.state.AddValue(int(p), value, valueValid)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// Add updates one non-NULL group and panics if configured growth is rejected.
func (t *U32Table) Add(key uint32, value int64) bool {
	created, err := t.TryAdd(key, value)
	if err != nil {
		panic(err)
	}
	return created
}

// TryAddNullable updates a nullable key with a non-NULL aggregate value.
func (t *U32Table) TryAddNullable(key uint32, valid bool, value int64) (bool, error) {
	return t.TryAddNullableValue(key, valid, value, true)
}

// TryAddNullableValue updates key/value validity using SQL GROUP BY and aggregate NULL semantics.
func (t *U32Table) TryAddNullableValue(key uint32, keyValid bool, value int64, valueValid bool) (bool, error) {
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
func (t *U32Table) TryMergeNullable(key uint32, keyValid bool, state aggregate.Int64Result) (bool, error) {
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
		p := Hash64(uint64(key)) & t.mask
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
func (t *U32Table) AggregateSelected(keys []uint32, vals []int64, sel memory.Bitmap) Summary {
	out, err := t.AggregateSelectedNullableValues(keys, nil, vals, nil, sel)
	if err != nil {
		panic(err)
	}
	return out
}

// AggregateSelectedNullable aggregates selected rows with nullable keys and non-NULL values.
func (t *U32Table) AggregateSelectedNullable(keys []uint32, keyValidity memory.Bitmap, vals []int64, sel memory.Bitmap) (Summary, error) {
	return t.AggregateSelectedNullableValues(keys, keyValidity, vals, nil, sel)
}

// AggregateSelectedNullableValues aggregates selected rows while preserving key and value validity.
func (t *U32Table) AggregateSelectedNullableValues(keys []uint32, keyValidity memory.Bitmap, vals []int64, valueValidity memory.Bitmap, sel memory.Bitmap) (Summary, error) {
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
			created, err := t.TryAddNullableValue(k, keyValid, vals[i], valueValid)
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

// Materialize appends all current groups, including the SQL NULL group, to dst.
func (t *U32Table) Materialize(dst []GroupU32) []GroupU32 {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			st := t.state.Result(i)
			dst = append(dst, GroupU32{Key: t.keys[i], Valid: true, Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	if t.nullSet {
		st := t.nullState.Result(0)
		dst = append(dst, GroupU32{Valid: false, Sum: st.Sum, Count: st.CountRows, State: st})
	}
	return dst
}

// Visit calls fn once for every current group without materializing a slice.
func (t *U32Table) Visit(fn func(key uint32, valid bool, state aggregate.Int64Result) error) error {
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
func (t *U32Table) VisitChunk(cursor *int, limit int, fn func(key uint32, valid bool, state aggregate.Int64Result) error) (bool, error) {
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
func (t *U32Table) Capacity() int { return len(t.keys) }

// Len returns the number of logical groups, including NULL when present.
func (t *U32Table) Len() int {
	n := t.size
	if t.nullSet {
		n++
	}
	return n
}

// RetainedBytes returns the table-owned key, epoch, and aggregate-state bytes.
func (t *U32Table) RetainedBytes() int64 {
	return u32TableBytesForCapacity(len(t.keys), t.opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, t.opts.Aggregates)
}

func u32TableBytesForCapacity(capacity int, layout aggregate.Layout) int64 {
	return int64(capacity)*(4+4) + aggregate.Int64VectorBytesForSlots(capacity, layout)
}

// EstimateU32TableBytes estimates initial retained bytes for cardinalityHint using the default aggregate layout.
func EstimateU32TableBytes(cardinalityHint int) int64 {
	return EstimateU32TableBytesWithOptions(cardinalityHint, TableOptions{})
}

// EstimateU32TableBytesWithOptions estimates retained bytes for the configured aggregate layout.
func EstimateU32TableBytesWithOptions(cardinalityHint int, opts TableOptions) int64 {
	opts = normalizeTableOptions(opts)
	return u32TableBytesForCapacity(tableCapacity(cardinalityHint), opts.Aggregates) + aggregate.Int64VectorBytesForSlots(1, opts.Aggregates)
}
