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
	return NewU64TableWithOptions(cardinalityHint, TableOptions{})
}

// NewU64TableWithOptions creates an adaptive uint64 aggregation table.
func NewU64TableWithOptions(cardinalityHint int, opts TableOptions) *U64Table {
	opts = normalizeTableOptions(opts)
	n := tableCapacity(cardinalityHint)
	return &U64Table{
		keys:  make([]uint64, n),
		state: aggregate.NewInt64Vector(n, opts.Aggregates),
		epoch: make([]uint32, n), mask: uint64(n - 1),
		maxEntries: loadLimit(n, opts.MaxLoadFactor), opts: opts,
		nullState: aggregate.NewInt64Vector(1, opts.Aggregates),
	}
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
	newCap := oldCap << 1
	if err := reserveGrowth(t.opts.Reserve, u64TableBytesForCapacity(oldCap, t.opts.Aggregates), u64TableBytesForCapacity(newCap, t.opts.Aggregates)); err != nil {
		return err
	}
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
	return nil
}

// TryAdd updates one non-NULL key with a non-NULL aggregate value.
func (t *U64Table) TryAdd(key uint64, value int64) (bool, error) {
	return t.TryAddValue(key, value, true)
}

// TryAddValue updates one non-NULL key while preserving aggregate-value NULL semantics.
func (t *U64Table) TryAddValue(key uint64, value int64, valueValid bool) (bool, error) {
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
