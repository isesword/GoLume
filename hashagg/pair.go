package hashagg

import (
	"gocolumn/aggregate"
	"gocolumn/internal/hashmix"
	"gocolumn/memory"
	"math/bits"
)

// Key128 stores two fixed-width key components. Values for invalid nullable
// components are normalized to zero before hashing or insertion.
type Key128 struct{ A, B uint64 }

// Group128 is a materialized 128-bit aggregation group. ValidMask uses bit 0
// for A and bit 1 for B; cleared bits represent SQL NULL components.
type Group128 struct {
	Key       Key128
	ValidMask uint8
	Sum       int64
	Count     uint64
	State     aggregate.Int64Result
}

// Hash128 returns the fixed-width pair hash for fully valid keys.
func Hash128(k Key128) uint64 { return hashmix.Mix64(hashmix.PairValues(k.A, k.B)) }

// Hash128Nullable returns a fixed-width pair hash that includes key validity.
func Hash128Nullable(k Key128, validMask uint8) uint64 {
	return hashmix.Pair(k.A, k.B, validMask)
}

// PairTable is an adaptive open-addressing aggregation table for two fixed
// 64-bit key parts, including independent SQL NULL validity for each part.
type PairTable struct {
	a, b       []uint64
	validMask  []uint8
	state      aggregate.Int64Vector
	epoch      []uint32
	gen        uint32
	mask       uint64
	size       int
	maxEntries int
	opts       TableOptions
}

// NewPairTable creates a pair aggregation table using default growth policy.
func NewPairTable(cardinalityHint int) *PairTable {
	return NewPairTableWithOptions(cardinalityHint, TableOptions{})
}

// NewPairTableWithOptions creates an adaptive pair aggregation table.
func NewPairTableWithOptions(cardinalityHint int, opts TableOptions) *PairTable {
	opts = normalizeTableOptions(opts)
	n := tableCapacity(cardinalityHint)
	return &PairTable{
		a: make([]uint64, n), b: make([]uint64, n), validMask: make([]uint8, n),
		state: aggregate.NewInt64Vector(n, opts.Aggregates), epoch: make([]uint32, n),
		mask: uint64(n - 1), maxEntries: loadLimit(n, opts.MaxLoadFactor), opts: opts,
	}
}

// Reset clears logical table contents while retaining allocated capacity.
func (t *PairTable) Reset() {
	t.gen++
	if t.gen == 0 {
		clear(t.epoch)
		t.gen = 1
	}
	t.size = 0
}

func (t *PairTable) grow() error {
	oldCap := len(t.a)
	newCap := oldCap << 1
	if err := reserveGrowth(t.opts.Reserve, pairTableBytesForCapacity(oldCap, t.opts.Aggregates), pairTableBytesForCapacity(newCap, t.opts.Aggregates)); err != nil {
		return err
	}
	a := make([]uint64, newCap)
	b := make([]uint64, newCap)
	validMask := make([]uint8, newCap)
	state := aggregate.NewInt64Vector(newCap, t.opts.Aggregates)
	epoch := make([]uint32, newCap)
	mask := uint64(newCap - 1)
	for i := range t.epoch {
		if t.epoch[i] != t.gen {
			continue
		}
		k := Key128{A: t.a[i], B: t.b[i]}
		vm := t.validMask[i]
		p := Hash128Nullable(k, vm) & mask
		for epoch[p] == t.gen {
			p = (p + 1) & mask
		}
		epoch[p] = t.gen
		a[p], b[p], validMask[p] = k.A, k.B, vm
		t.state.Copy(&state, i, int(p))
	}
	t.a, t.b, t.validMask, t.state, t.epoch = a, b, validMask, state, epoch
	t.mask = mask
	t.maxEntries = loadLimit(newCap, t.opts.MaxLoadFactor)
	return nil
}

// TryAdd updates one fully valid pair group with a non-NULL aggregate value.
func (t *PairTable) TryAdd(k Key128, v int64) (bool, error) {
	return t.TryAddNullableValue(k, 3, v, true)
}

// TryAddNullable updates a pair group with a non-NULL aggregate value. Bits
// 0/1 in validMask describe the validity of A/B.
func (t *PairTable) TryAddNullable(k Key128, validMask uint8, v int64) (bool, error) {
	return t.TryAddNullableValue(k, validMask, v, true)
}

// TryAddNullableValue updates pair-key and aggregate-value validity using SQL
// GROUP BY NULL and aggregate NULL semantics.
func (t *PairTable) TryAddNullableValue(k Key128, validMask uint8, v int64, valueValid bool) (bool, error) {
	validMask &= 3
	if validMask&1 == 0 {
		k.A = 0
	}
	if validMask&2 == 0 {
		k.B = 0
	}
	for {
		p := Hash128Nullable(k, validMask) & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				t.epoch[p] = t.gen
				t.a[p], t.b[p], t.validMask[p] = k.A, k.B, validMask
				t.state.InitValue(int(p), v, valueValid)
				t.size++
				return true, nil
			}
			if t.validMask[p] == validMask && t.a[p] == k.A && t.b[p] == k.B {
				t.state.AddValue(int(p), v, valueValid)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// TryMergeNullable combines a mergeable partial state into a nullable pair.
func (t *PairTable) TryMergeNullable(k Key128, validMask uint8, state aggregate.Int64Result) (bool, error) {
	validMask &= 3
	if validMask&1 == 0 {
		k.A = 0
	}
	if validMask&2 == 0 {
		k.B = 0
	}
	for {
		p := Hash128Nullable(k, validMask) & t.mask
		for {
			if t.epoch[p] != t.gen {
				if t.size+1 > t.maxEntries {
					if err := t.grow(); err != nil {
						return false, err
					}
					break
				}
				t.epoch[p] = t.gen
				t.a[p], t.b[p], t.validMask[p] = k.A, k.B, validMask
				t.state.InitResult(int(p), state)
				t.size++
				return true, nil
			}
			if t.validMask[p] == validMask && t.a[p] == k.A && t.b[p] == k.B {
				t.state.MergeResult(int(p), state)
				return false, nil
			}
			p = (p + 1) & t.mask
		}
	}
}

// Add updates one fully valid pair group and panics if configured growth is rejected.
func (t *PairTable) Add(k Key128, v int64) bool {
	created, err := t.TryAdd(k, v)
	if err != nil {
		panic(err)
	}
	return created
}

// AggregateSelected aggregates selected fully valid pair keys and non-NULL values.
func (t *PairTable) AggregateSelected(keys []Key128, vals []int64, sel memory.Bitmap) Summary {
	var out Summary
	for wi, word := range sel {
		base := wi << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			i := base + bit
			if i >= len(keys) {
				break
			}
			created, err := t.TryAdd(keys[i], vals[i])
			if err != nil {
				panic(err)
			}
			if created {
				out.Groups++
			}
			out.Checksum += vals[i]
			out.Rows++
			word &= word - 1
		}
	}
	return out
}

// Materialize appends all current groups to dst.
func (t *PairTable) Materialize(dst []Group128) []Group128 {
	dst = dst[:0]
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			st := t.state.Result(i)
			dst = append(dst, Group128{Key: Key128{A: t.a[i], B: t.b[i]}, ValidMask: t.validMask[i], Sum: st.Sum, Count: st.CountRows, State: st})
		}
	}
	return dst
}

// Visit calls fn once for every current group without materializing a slice.
func (t *PairTable) Visit(fn func(key Key128, validMask uint8, state aggregate.Int64Result) error) error {
	if fn == nil {
		return nil
	}
	for i := range t.epoch {
		if t.epoch[i] == t.gen {
			if err := fn(Key128{A: t.a[i], B: t.b[i]}, t.validMask[i], t.state.Result(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Capacity returns the current number of hash slots.
func (t *PairTable) Capacity() int { return len(t.a) }

// Len returns the number of logical groups.
func (t *PairTable) Len() int { return t.size }

// RetainedBytes returns the table-owned key, validity, epoch, and state bytes.
func (t *PairTable) RetainedBytes() int64 {
	return pairTableBytesForCapacity(len(t.a), t.opts.Aggregates)
}

func pairTableBytesForCapacity(capacity int, layout aggregate.Layout) int64 {
	return int64(capacity)*(8+8+1+4) + aggregate.Int64VectorBytesForSlots(capacity, layout)
}

// EstimatePairTableBytes estimates initial retained bytes using the default aggregate layout.
func EstimatePairTableBytes(cardinalityHint int) int64 {
	return EstimatePairTableBytesWithOptions(cardinalityHint, TableOptions{})
}

// EstimatePairTableBytesWithOptions estimates retained bytes for the configured aggregate layout.
func EstimatePairTableBytesWithOptions(cardinalityHint int, opts TableOptions) int64 {
	opts = normalizeTableOptions(opts)
	return pairTableBytesForCapacity(tableCapacity(cardinalityHint), opts.Aggregates)
}
