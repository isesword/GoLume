// Package aggregate defines reusable aggregate-state contracts and hot-path
// state vectors for columnar execution.
package aggregate

import "fmt"

// Kind identifies a built-in aggregate state implementation.
type Kind uint8

const (
	// KindSum computes SQL SUM over non-NULL int64 values.
	KindSum Kind = iota + 1
	// KindCountRows computes COUNT(*).
	KindCountRows
	// KindCountValues computes COUNT(value), ignoring NULL values.
	KindCountValues
	// KindMin computes SQL MIN over non-NULL int64 values.
	KindMin
	// KindMax computes SQL MAX over non-NULL int64 values.
	KindMax
	// KindAvg computes SQL AVG over non-NULL int64 values.
	KindAvg
)

// Value is the finalized scalar value of an aggregate state.
type Value struct {
	Valid   bool
	Int64   int64
	Float64 float64
	IsFloat bool
}

// State is the control-plane lifecycle contract for one scalar accumulator.
// Hash aggregation hot loops use specialized state vectors instead of dynamic
// interface dispatch on every row.
type State interface {
	// Reset initializes the state for reuse.
	Reset()
	// Add consumes one row. valid is false for SQL NULL input values.
	Add(value int64, valid bool)
	// Merge combines another compatible worker-local state.
	Merge(other State) error
	// Finalize returns the SQL result value.
	Finalize() Value
}

// NewState constructs a built-in int64 aggregate state.
func NewState(kind Kind) (State, error) {
	switch kind {
	case KindSum:
		return &SumState{}, nil
	case KindCountRows:
		return &CountState{rows: true}, nil
	case KindCountValues:
		return &CountState{}, nil
	case KindMin:
		return &MinState{}, nil
	case KindMax:
		return &MaxState{}, nil
	case KindAvg:
		return &AvgState{}, nil
	default:
		return nil, fmt.Errorf("aggregate: unsupported kind %d", kind)
	}
}

// SumState implements SQL SUM for int64 input.
type SumState struct {
	sum   int64
	valid bool
}

// Reset clears the accumulator.
func (s *SumState) Reset() { s.sum, s.valid = 0, false }

// Add adds one non-NULL value.
func (s *SumState) Add(value int64, valid bool) {
	if valid {
		s.sum += value
		s.valid = true
	}
}

// Merge combines another SumState.
func (s *SumState) Merge(other State) error {
	o, ok := other.(*SumState)
	if !ok {
		return fmt.Errorf("aggregate: cannot merge %T into SumState", other)
	}
	if o.valid {
		s.sum += o.sum
		s.valid = true
	}
	return nil
}

// Finalize returns NULL when no non-NULL value was observed.
func (s *SumState) Finalize() Value { return Value{Valid: s.valid, Int64: s.sum} }

// CountState implements COUNT(*) or COUNT(value).
type CountState struct {
	count uint64
	rows  bool
}

// NewCountRowsState creates a COUNT(*) accumulator.
func NewCountRowsState() *CountState { return &CountState{rows: true} }

// NewCountValuesState creates a COUNT(value) accumulator.
func NewCountValuesState() *CountState { return &CountState{} }

// Reset clears the count.
func (s *CountState) Reset() { s.count = 0 }

// Add consumes one row according to COUNT(*) or COUNT(value) semantics.
func (s *CountState) Add(_ int64, valid bool) {
	if s.rows || valid {
		s.count++
	}
}

// Merge combines a compatible CountState.
func (s *CountState) Merge(other State) error {
	o, ok := other.(*CountState)
	if !ok || o.rows != s.rows {
		return fmt.Errorf("aggregate: incompatible count state %T", other)
	}
	s.count += o.count
	return nil
}

// Finalize returns the count, which is never SQL NULL.
func (s *CountState) Finalize() Value { return Value{Valid: true, Int64: int64(s.count)} }

// MinState implements SQL MIN for int64 input.
type MinState struct {
	value int64
	valid bool
}

// Reset clears the accumulator.
func (s *MinState) Reset() { s.value, s.valid = 0, false }

// Add retains the smallest non-NULL value.
func (s *MinState) Add(value int64, valid bool) {
	if valid && (!s.valid || value < s.value) {
		s.value, s.valid = value, true
	}
}

// Merge combines another MinState.
func (s *MinState) Merge(other State) error {
	o, ok := other.(*MinState)
	if !ok {
		return fmt.Errorf("aggregate: cannot merge %T into MinState", other)
	}
	if o.valid {
		s.Add(o.value, true)
	}
	return nil
}

// Finalize returns NULL when no non-NULL value was observed.
func (s *MinState) Finalize() Value { return Value{Valid: s.valid, Int64: s.value} }

// MaxState implements SQL MAX for int64 input.
type MaxState struct {
	value int64
	valid bool
}

// Reset clears the accumulator.
func (s *MaxState) Reset() { s.value, s.valid = 0, false }

// Add retains the largest non-NULL value.
func (s *MaxState) Add(value int64, valid bool) {
	if valid && (!s.valid || value > s.value) {
		s.value, s.valid = value, true
	}
}

// Merge combines another MaxState.
func (s *MaxState) Merge(other State) error {
	o, ok := other.(*MaxState)
	if !ok {
		return fmt.Errorf("aggregate: cannot merge %T into MaxState", other)
	}
	if o.valid {
		s.Add(o.value, true)
	}
	return nil
}

// Finalize returns NULL when no non-NULL value was observed.
func (s *MaxState) Finalize() Value { return Value{Valid: s.valid, Int64: s.value} }

// AvgState implements SQL AVG for int64 input and finalizes to float64.
type AvgState struct {
	sum   int64
	count uint64
}

// Reset clears the accumulator.
func (s *AvgState) Reset() { s.sum, s.count = 0, 0 }

// Add consumes one non-NULL value.
func (s *AvgState) Add(value int64, valid bool) {
	if valid {
		s.sum += value
		s.count++
	}
}

// Merge combines another AvgState.
func (s *AvgState) Merge(other State) error {
	o, ok := other.(*AvgState)
	if !ok {
		return fmt.Errorf("aggregate: cannot merge %T into AvgState", other)
	}
	s.sum += o.sum
	s.count += o.count
	return nil
}

// Finalize returns NULL when no non-NULL value was observed.
func (s *AvgState) Finalize() Value {
	if s.count == 0 {
		return Value{}
	}
	return Value{Valid: true, Float64: float64(s.sum) / float64(s.count), IsFloat: true}
}

// Layout describes the aggregate state arrays required by one physical hash
// aggregation operator. The layout is selected once during query compilation;
// row hot paths update only the arrays enabled by this layout.
type Layout struct {
	Sum         bool
	CountRows   bool
	CountValues bool
	Min         bool
	Max         bool
	Avg         bool
}

// DefaultLayout returns the Phase-1 compatible SUM(value)+COUNT(*) layout.
func DefaultLayout() Layout { return Layout{Sum: true, CountRows: true} }

// IsZero reports whether no aggregate state has been requested.
func (l Layout) IsZero() bool {
	return !l.Sum && !l.CountRows && !l.CountValues && !l.Min && !l.Max && !l.Avg
}

// IsSumCountOnly reports whether the layout can use the shared
// SUM+COUNT specialization without additional aggregate state.
func (l Layout) IsSumCountOnly() bool {
	return l.Sum && l.CountRows && !l.CountValues && !l.Min && !l.Max && !l.Avg
}

func normalizeLayout(l Layout) Layout {
	if l.IsZero() {
		return DefaultLayout()
	}
	return l
}

// Int64Result is the finalized state of built-in int64 aggregate functions for
// one group. Valid flags preserve SQL NULL results for empty non-NULL inputs.
type Int64Result struct {
	Sum         int64
	SumValid    bool
	CountRows   uint64
	CountValues uint64
	Min         int64
	MinValid    bool
	Max         int64
	MaxValid    bool
	Avg         float64
	AvgValid    bool
}

// Int64Vector stores structure-of-arrays aggregate state for hash-table slots.
// It avoids interface dispatch in row loops while retaining a generic
// control-plane Layout selected by the planner.
type Int64Vector struct {
	layout       Layout
	sumCountOnly bool
	sums         []int64
	sumValid     []uint8
	countRows    []uint64
	countValues  []uint64
	mins         []int64
	minValid     []uint8
	maxs         []int64
	maxValid     []uint8
}

// SumCountUpdater is a checked, non-owning update view for a SUM+COUNT(*)
// vector. It must come from Int64Vector.SumCountUpdater and remains valid only
// while that vector keeps the same layout and has not been released. It owns
// no buffers and observes replacement backing arrays through the vector.
type SumCountUpdater struct{ vector *Int64Vector }

// SumCountUpdater selects the narrow state transition once outside a batch
// loop. Other layouts return false and must use the general vector methods.
func (v *Int64Vector) SumCountUpdater() (SumCountUpdater, bool) {
	if v == nil || !v.sumCountOnly {
		return SumCountUpdater{}, false
	}
	return SumCountUpdater{vector: v}, true
}

// AddValue updates an occupied slot, counting NULL rows without changing SUM.
// Slot bounds and initialization follow the Int64Vector.AddValue contract.
func (u SumCountUpdater) AddValue(slot int, value int64, valid bool) {
	u.vector.addSumCountValue(slot, value, valid)
}

// NewInt64Vector allocates aggregate state for slots according to layout.
func NewInt64Vector(slots int, layout Layout) Int64Vector {
	layout = normalizeLayout(layout)
	v := Int64Vector{layout: layout, sumCountOnly: layout.IsSumCountOnly()}
	if layout.Sum || layout.Avg {
		v.sums = make([]int64, slots)
	}
	if layout.Sum && !layout.Avg {
		v.sumValid = make([]uint8, slots)
	}
	if layout.CountRows {
		v.countRows = make([]uint64, slots)
	}
	if layout.CountValues || layout.Avg {
		v.countValues = make([]uint64, slots)
	}
	if layout.Min {
		v.mins = make([]int64, slots)
		v.minValid = make([]uint8, slots)
	}
	if layout.Max {
		v.maxs = make([]int64, slots)
		v.maxValid = make([]uint8, slots)
	}
	return v
}

// Layout returns the immutable aggregate layout used by the vector.
func (v *Int64Vector) Layout() Layout { return v.layout }

// Init initializes a newly occupied slot with a non-NULL input value.
func (v *Int64Vector) Init(slot int, value int64) { v.InitValue(slot, value, true) }

// InitValue initializes a newly occupied slot and applies SQL aggregate NULL
// semantics to the first row in the group.
func (v *Int64Vector) InitValue(slot int, value int64, valid bool) {
	if v.layout.Sum || v.layout.Avg {
		if valid {
			v.sums[slot] = value
		} else {
			v.sums[slot] = 0
		}
	}
	if v.layout.Sum && !v.layout.Avg {
		if valid {
			v.sumValid[slot] = 1
		} else {
			v.sumValid[slot] = 0
		}
	}
	if v.layout.CountRows {
		v.countRows[slot] = 1
	}
	if v.layout.CountValues || v.layout.Avg {
		if valid {
			v.countValues[slot] = 1
		} else {
			v.countValues[slot] = 0
		}
	}
	if v.layout.Min {
		if valid {
			v.mins[slot], v.minValid[slot] = value, 1
		} else {
			v.mins[slot], v.minValid[slot] = 0, 0
		}
	}
	if v.layout.Max {
		if valid {
			v.maxs[slot], v.maxValid[slot] = value, 1
		} else {
			v.maxs[slot], v.maxValid[slot] = 0, 0
		}
	}
}

// Add updates an existing slot with a non-NULL input value.
func (v *Int64Vector) Add(slot int, value int64) { v.AddValue(slot, value, true) }

// AddValue updates an existing slot and applies SQL aggregate NULL semantics.
func (v *Int64Vector) AddValue(slot int, value int64, valid bool) {
	if v.sumCountOnly {
		v.addSumCountValue(slot, value, valid)
		return
	}
	if v.layout.CountRows {
		v.countRows[slot]++
	}
	if !valid {
		return
	}
	if v.layout.Sum || v.layout.Avg {
		v.sums[slot] += value
	}
	if v.layout.Sum && !v.layout.Avg {
		v.sumValid[slot] = 1
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot]++
	}
	if v.layout.Min && (v.minValid[slot] == 0 || value < v.mins[slot]) {
		v.mins[slot], v.minValid[slot] = value, 1
	}
	if v.layout.Max && (v.maxValid[slot] == 0 || value > v.maxs[slot]) {
		v.maxs[slot], v.maxValid[slot] = value, 1
	}
}

// addSumCountValue is the shared narrow state transition for SUM+COUNT(*).
// It counts NULL rows while leaving SUM invalid until a non-NULL value arrives.
func (v *Int64Vector) addSumCountValue(slot int, value int64, valid bool) {
	v.countRows[slot]++
	if valid {
		v.sums[slot] += value
		v.sumValid[slot] = 1
	}
}

// InitResult initializes a newly occupied slot from a mergeable partial
// result. Avg uses Sum and CountValues as its non-finalized state.
func (v *Int64Vector) InitResult(slot int, result Int64Result) {
	if v.layout.Sum || v.layout.Avg {
		v.sums[slot] = result.Sum
	}
	if v.layout.Sum && !v.layout.Avg {
		if result.SumValid {
			v.sumValid[slot] = 1
		} else {
			v.sumValid[slot] = 0
		}
	}
	if v.layout.CountRows {
		v.countRows[slot] = result.CountRows
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot] = result.CountValues
	}
	if v.layout.Min {
		v.mins[slot] = result.Min
		if result.MinValid {
			v.minValid[slot] = 1
		} else {
			v.minValid[slot] = 0
		}
	}
	if v.layout.Max {
		v.maxs[slot] = result.Max
		if result.MaxValid {
			v.maxValid[slot] = 1
		} else {
			v.maxValid[slot] = 0
		}
	}
}

// MergeResult combines one partial result into an occupied slot without
// finalizing Avg or dispatching through per-row interfaces.
func (v *Int64Vector) MergeResult(slot int, result Int64Result) {
	if v.layout.Sum || v.layout.Avg {
		v.sums[slot] += result.Sum
	}
	if v.layout.Sum && !v.layout.Avg && result.SumValid {
		v.sumValid[slot] = 1
	}
	if v.layout.CountRows {
		v.countRows[slot] += result.CountRows
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot] += result.CountValues
	}
	if v.layout.Min && result.MinValid && (v.minValid[slot] == 0 || result.Min < v.mins[slot]) {
		v.mins[slot], v.minValid[slot] = result.Min, 1
	}
	if v.layout.Max && result.MaxValid && (v.maxValid[slot] == 0 || result.Max > v.maxs[slot]) {
		v.maxs[slot], v.maxValid[slot] = result.Max, 1
	}
}

// UpdateBatch applies already-grouped input values to occupied slots. A nil
// validity slice means every value is valid; otherwise zero represents NULL.
func (v *Int64Vector) UpdateBatch(slots []int, values []int64, validity []uint8) error {
	if len(slots) != len(values) || validity != nil && len(validity) != len(values) {
		return fmt.Errorf("aggregate: batch update length mismatch slots=%d values=%d validity=%d", len(slots), len(values), len(validity))
	}
	for i, slot := range slots {
		if slot < 0 || slot >= v.slots() {
			return fmt.Errorf("aggregate: update slot %d out of range", slot)
		}
		v.AddValue(slot, values[i], validity == nil || validity[i] != 0)
	}
	return nil
}

// MergeBatch combines partial states into occupied destination slots.
func (v *Int64Vector) MergeBatch(slots []int, states []Int64Result) error {
	if len(slots) != len(states) {
		return fmt.Errorf("aggregate: merge batch length mismatch slots=%d states=%d", len(slots), len(states))
	}
	for i, slot := range slots {
		if slot < 0 || slot >= v.slots() {
			return fmt.Errorf("aggregate: merge slot %d out of range", slot)
		}
		v.MergeResult(slot, states[i])
	}
	return nil
}

// FinalizeBatch appends finalized results for slots to dst.
func (v *Int64Vector) FinalizeBatch(dst []Int64Result, slots []int) ([]Int64Result, error) {
	dst = dst[:0]
	for _, slot := range slots {
		if slot < 0 || slot >= v.slots() {
			return nil, fmt.Errorf("aggregate: finalize slot %d out of range", slot)
		}
		dst = append(dst, v.Result(slot))
	}
	return dst, nil
}

func (v *Int64Vector) slots() int {
	switch {
	case len(v.sums) != 0:
		return len(v.sums)
	case len(v.countRows) != 0:
		return len(v.countRows)
	case len(v.countValues) != 0:
		return len(v.countValues)
	case len(v.mins) != 0:
		return len(v.mins)
	case len(v.maxs) != 0:
		return len(v.maxs)
	default:
		return 0
	}
}

// Copy copies one occupied slot into dst during hash-table rehash.
func (v *Int64Vector) Copy(dst *Int64Vector, srcSlot, dstSlot int) {
	if v.layout != dst.layout {
		panic("aggregate: incompatible vector layout during rehash")
	}
	if v.layout.Sum || v.layout.Avg {
		dst.sums[dstSlot] = v.sums[srcSlot]
	}
	if v.layout.Sum && !v.layout.Avg {
		dst.sumValid[dstSlot] = v.sumValid[srcSlot]
	}
	if v.layout.CountRows {
		dst.countRows[dstSlot] = v.countRows[srcSlot]
	}
	if v.layout.CountValues || v.layout.Avg {
		dst.countValues[dstSlot] = v.countValues[srcSlot]
	}
	if v.layout.Min {
		dst.mins[dstSlot], dst.minValid[dstSlot] = v.mins[srcSlot], v.minValid[srcSlot]
	}
	if v.layout.Max {
		dst.maxs[dstSlot], dst.maxValid[dstSlot] = v.maxs[srcSlot], v.maxValid[srcSlot]
	}
}

// Result finalizes all configured aggregate states for one occupied slot.
func (v *Int64Vector) Result(slot int) Int64Result {
	var r Int64Result
	if v.layout.Sum || v.layout.Avg {
		r.Sum = v.sums[slot]
		if v.layout.Avg {
			r.SumValid = v.countValues[slot] > 0
		} else if v.layout.Sum {
			r.SumValid = v.sumValid[slot] != 0
		}
	}
	if v.layout.CountRows {
		r.CountRows = v.countRows[slot]
	}
	if v.layout.CountValues || v.layout.Avg {
		r.CountValues = v.countValues[slot]
	}
	if v.layout.Min {
		r.Min, r.MinValid = v.mins[slot], v.minValid[slot] != 0
	}
	if v.layout.Max {
		r.Max, r.MaxValid = v.maxs[slot], v.maxValid[slot] != 0
	}
	if v.layout.Avg && v.countValues[slot] != 0 {
		r.Avg = float64(v.sums[slot]) / float64(v.countValues[slot])
		r.AvgValid = true
	}
	return r
}

// Int64VectorBytesForSlots estimates retained state bytes for slots.
func Int64VectorBytesForSlots(slots int, layout Layout) int64 {
	layout = normalizeLayout(layout)
	var per int64
	if layout.Sum || layout.Avg {
		per += 8
	}
	if layout.Sum && !layout.Avg {
		per += 1
	}
	if layout.CountRows {
		per += 8
	}
	if layout.CountValues || layout.Avg {
		per += 8
	}
	if layout.Min {
		per += 8 + 1
	}
	if layout.Max {
		per += 8 + 1
	}
	return int64(slots) * per
}
