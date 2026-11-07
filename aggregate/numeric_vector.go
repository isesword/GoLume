package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// NumericResult is a mergeable native numeric aggregate state. Type is the input
// type, not the SUM/AVG output type. Layout must match the destination vector.
// Signed integers use two's-complement bits; Float32 uses the low 32 IEEE bits.
// AvgSum is an unfinalized Float64 total for non-Int64 averages. Int64 averages
// deliberately retain the legacy wrapped SumBits/CountValues implementation.
// Spill codecs may reconstruct Type and Layout from validated lane descriptors.
type NumericResult struct {
	Type                         memory.DataType
	Layout                       Layout
	SumBits, MinBits, MaxBits    uint64
	SumValid, MinValid, MaxValid bool
	CountRows, CountValues       uint64
	AvgSum                       float64
}

// NumericAggregateType returns the output type for a supported reduction.
// Counts retain GoLume's Uint64 representation. Small integer SUM widens to
// Int64; other numeric SUM/MIN/MAX preserve their native input type. Integer
// AVG returns Float64, while floating AVG preserves Float32 or Float64.
func NumericAggregateType(input memory.DataType, kind Kind) (memory.DataType, error) {
	if input <= memory.TypeInvalid || input > memory.TypeStruct {
		return memory.TypeInvalid, fmt.Errorf("aggregate: invalid input type %d", input)
	}
	if kind == KindCountRows || kind == KindCountValues {
		return memory.TypeUint64, nil
	}
	if !isAggregateNumeric(input) {
		return memory.TypeInvalid, fmt.Errorf("aggregate: %s does not support numeric reductions", input)
	}
	switch kind {
	case KindSum:
		switch input {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16:
			return memory.TypeInt64, nil
		}
		return input, nil
	case KindMin, KindMax:
		return input, nil
	case KindAvg:
		if input == memory.TypeFloat32 {
			return memory.TypeFloat32, nil
		}
		return memory.TypeFloat64, nil
	default:
		return memory.TypeInvalid, fmt.Errorf("aggregate: unsupported kind %d", kind)
	}
}

// Value finalizes one configured reduction without changing mergeable state.
// The returned bits have NumericAggregateType(r.Type, kind)'s representation.
// Missing reductions and NULL results return valid=false. Count values remain
// valid for empty input. Floating SUM/AVG propagate NaN and IEEE infinities;
// floating MIN/MAX ignore NaNs unless every non-NULL input is NaN.
func (r NumericResult) Value(kind Kind) (bits uint64, valid bool) {
	switch kind {
	case KindSum:
		if r.Layout.Sum {
			return r.SumBits, r.SumValid
		}
	case KindCountRows:
		if r.Layout.CountRows {
			return r.CountRows, true
		}
	case KindCountValues:
		if r.Layout.CountValues {
			return r.CountValues, true
		}
	case KindMin:
		if r.Layout.Min {
			return r.MinBits, r.MinValid
		}
	case KindMax:
		if r.Layout.Max {
			return r.MaxBits, r.MaxValid
		}
	case KindAvg:
		if !r.Layout.Avg || r.CountValues == 0 {
			return 0, false
		}
		var mean float64
		if r.Type == memory.TypeInt64 {
			mean = float64(int64(r.SumBits)) / float64(r.CountValues)
		} else {
			mean = r.AvgSum / float64(r.CountValues)
		}
		if r.Type == memory.TypeFloat32 {
			return uint64(math.Float32bits(float32(mean))), true
		}
		return math.Float64bits(mean), true
	}
	return 0, false
}

// NumericVector stores one native input lane as structure-of-arrays state.
// It contains no interfaces in row update loops. Int64 delegates to Int64Vector
// so existing overflow and rounding behavior does not depend on other columns.
// Other integer averages use a separate Float64 total, avoiding narrow-integer
// SUM overflow. Float32 SUM rounds after each addition; AVG uses a Float64 total
// and rounds its final output. Floating merge order can change rounding, and
// callers must compare finite distributed results with suitable tolerances.
// Float64 AVG totals are ordinary IEEE additions, not compensated or scaled:
// a finite total may overflow even when the mathematical mean is finite, and
// cancellation depends on encounter/merge order. Opposite infinities yield NaN.
// The vector owns no budget token: reserve NumericVectorBytesForSlots before
// construction and retain that token until all backing arrays are released.
type NumericVector struct {
	typ                          memory.DataType
	layout                       Layout
	slots                        int
	legacy                       Int64Vector
	sums, mins, maxs             []uint64
	sumValid, minValid, maxValid []uint8
	countRows, countValues       []uint64
	avgSums                      []float64
}

func isAggregateNumeric(t memory.DataType) bool {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
		return true
	}
	return false
}

func validateNumericLayout(typ memory.DataType, layout Layout) error {
	if typ <= memory.TypeInvalid || typ > memory.TypeStruct {
		return fmt.Errorf("aggregate: invalid input type %d", typ)
	}
	if layout.IsZero() {
		return fmt.Errorf("aggregate: numeric layout must not be empty")
	}
	if (layout.Sum || layout.Min || layout.Max || layout.Avg) && !isAggregateNumeric(typ) {
		return fmt.Errorf("aggregate: numeric reductions do not support %s", typ)
	}
	return nil
}

// NumericVectorBytesForSlots returns exact requested backing-array bytes,
// excluding the NumericVector header itself and allocator/runtime overhead.
// It rejects negative capacities, unsupported type/layout pairs and overflow.
func NumericVectorBytesForSlots(slots int, typ memory.DataType, layout Layout) (int64, error) {
	if slots < 0 {
		return 0, fmt.Errorf("aggregate: negative numeric state capacity")
	}
	if err := validateNumericLayout(typ, layout); err != nil {
		return 0, err
	}
	var per int64
	if typ == memory.TypeInt64 {
		per = Int64VectorBytesForSlots(1, layout)
	} else {
		if layout.Sum {
			per += 9
		}
		if layout.Min {
			per += 9
		}
		if layout.Max {
			per += 9
		}
		if layout.CountRows {
			per += 8
		}
		if layout.CountValues || layout.Avg {
			per += 8
		}
		if layout.Avg {
			per += 8
		}
	}
	if per > 0 && uint64(slots) > uint64(math.MaxInt64/per) {
		return 0, fmt.Errorf("aggregate: numeric state size overflow")
	}
	bytes := int64(slots) * per
	if uint64(bytes) > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("aggregate: numeric state exceeds addressable memory")
	}
	return bytes, nil
}

// NewNumericVector constructs an explicitly configured native numeric lane.
// New slots contain empty identities, not synthetic rows. The caller must have
// reserved the bytes reported by NumericVectorBytesForSlots before allocation.
func NewNumericVector(slots int, typ memory.DataType, layout Layout) (NumericVector, error) {
	if _, err := NumericVectorBytesForSlots(slots, typ, layout); err != nil {
		return NumericVector{}, err
	}
	v := NumericVector{typ: typ, layout: layout, slots: slots}
	if typ == memory.TypeInt64 {
		v.legacy = NewInt64Vector(slots, layout)
		return v, nil
	}
	if layout.Sum {
		v.sums = make([]uint64, slots)
		v.sumValid = make([]uint8, slots)
	}
	if layout.Min {
		v.mins = make([]uint64, slots)
		v.minValid = make([]uint8, slots)
	}
	if layout.Max {
		v.maxs = make([]uint64, slots)
		v.maxValid = make([]uint8, slots)
	}
	if layout.CountRows {
		v.countRows = make([]uint64, slots)
	}
	if layout.CountValues || layout.Avg {
		v.countValues = make([]uint64, slots)
	}
	if layout.Avg {
		v.avgSums = make([]float64, slots)
	}
	return v, nil
}

// Type returns the immutable input type of the lane.
func (v *NumericVector) Type() memory.DataType { return v.typ }

// Layout returns the immutable enabled reductions of the lane.
func (v *NumericVector) Layout() Layout { return v.layout }

// Slots returns the number of addressable state slots.
func (v *NumericVector) Slots() int { return v.slots }

func normalizeNumericBits(typ memory.DataType, bits uint64) uint64 {
	switch typ {
	case memory.TypeInt8:
		return uint64(int64(int8(bits)))
	case memory.TypeInt16:
		return uint64(int64(int16(bits)))
	case memory.TypeInt32:
		return uint64(int64(int32(bits)))
	case memory.TypeUint8:
		return uint64(uint8(bits))
	case memory.TypeUint16:
		return uint64(uint16(bits))
	case memory.TypeUint32, memory.TypeFloat32:
		return uint64(uint32(bits))
	default:
		return bits
	}
}

func numericAsFloat(typ memory.DataType, bits uint64) float64 {
	switch typ {
	case memory.TypeFloat32:
		return float64(math.Float32frombits(uint32(bits)))
	case memory.TypeFloat64:
		return math.Float64frombits(bits)
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		return float64(int64(bits))
	default:
		return float64(bits)
	}
}

func numericSum(typ memory.DataType, left, right uint64) uint64 {
	switch typ {
	case memory.TypeFloat32:
		return uint64(math.Float32bits(math.Float32frombits(uint32(left)) + math.Float32frombits(uint32(right))))
	case memory.TypeFloat64:
		return math.Float64bits(math.Float64frombits(left) + math.Float64frombits(right))
	case memory.TypeInt32:
		return uint64(int64(int32(left + right)))
	case memory.TypeUint32:
		return uint64(uint32(left + right))
	default:
		return left + right
	}
}

func numericExtreme(typ memory.DataType, left, right uint64, min bool) uint64 {
	switch typ {
	case memory.TypeFloat32, memory.TypeFloat64:
		a, b := numericAsFloat(typ, left), numericAsFloat(typ, right)
		if math.IsNaN(a) {
			return right
		}
		if math.IsNaN(b) {
			return left
		}
		var result float64
		if min {
			result = math.Min(a, b)
		} else {
			result = math.Max(a, b)
		}
		if typ == memory.TypeFloat32 {
			return uint64(math.Float32bits(float32(result)))
		}
		return math.Float64bits(result)
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		if (min && int64(right) < int64(left)) || (!min && int64(right) > int64(left)) {
			return right
		}
	default:
		if (min && right < left) || (!min && right > left) {
			return right
		}
	}
	return left
}

// AddBits consumes one input value in its native bit representation. Slot must
// be in [0, Slots()). Non-numeric count-only lanes ignore valueBits completely.
// The caller controls partition ownership; updates are not synchronized.
func (v *NumericVector) AddBits(slot int, valueBits uint64, valid bool) {
	if v.typ == memory.TypeInt64 {
		v.legacy.AddValue(slot, int64(valueBits), valid)
		return
	}
	if v.layout.CountRows {
		v.countRows[slot]++
	}
	if !valid {
		return
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot]++
	}
	if !(v.layout.Sum || v.layout.Min || v.layout.Max || v.layout.Avg) {
		return
	}
	valueBits = normalizeNumericBits(v.typ, valueBits)
	if v.layout.Sum {
		v.sums[slot] = numericSum(v.typ, v.sums[slot], valueBits)
		v.sumValid[slot] = 1
	}
	if v.layout.Avg {
		v.avgSums[slot] += numericAsFloat(v.typ, valueBits)
	}
	if v.layout.Min {
		if v.minValid[slot] == 0 {
			v.mins[slot] = valueBits
		} else {
			v.mins[slot] = numericExtreme(v.typ, v.mins[slot], valueBits, true)
		}
		v.minValid[slot] = 1
	}
	if v.layout.Max {
		if v.maxValid[slot] == 0 {
			v.maxs[slot] = valueBits
		} else {
			v.maxs[slot] = numericExtreme(v.typ, v.maxs[slot], valueBits, false)
		}
		v.maxValid[slot] = 1
	}
}

// Result snapshots one mergeable state without allocating or modifying it.
func (v *NumericVector) Result(slot int) NumericResult {
	r := NumericResult{Type: v.typ, Layout: v.layout}
	if v.typ == memory.TypeInt64 {
		old := v.legacy.Result(slot)
		r.SumBits, r.SumValid = uint64(old.Sum), old.SumValid
		r.MinBits, r.MinValid = uint64(old.Min), old.MinValid
		r.MaxBits, r.MaxValid = uint64(old.Max), old.MaxValid
		r.CountRows, r.CountValues = old.CountRows, old.CountValues
		return r
	}
	if v.layout.Sum {
		r.SumBits, r.SumValid = v.sums[slot], v.sumValid[slot] != 0
	}
	if v.layout.Min {
		r.MinBits, r.MinValid = v.mins[slot], v.minValid[slot] != 0
	}
	if v.layout.Max {
		r.MaxBits, r.MaxValid = v.maxs[slot], v.maxValid[slot] != 0
	}
	if v.layout.CountRows {
		r.CountRows = v.countRows[slot]
	}
	if v.layout.CountValues || v.layout.Avg {
		r.CountValues = v.countValues[slot]
	}
	if v.layout.Avg {
		r.AvgSum = v.avgSums[slot]
	}
	return r
}

func (r NumericResult) int64State() Int64Result {
	return Int64Result{Sum: int64(r.SumBits), SumValid: r.SumValid, CountRows: r.CountRows, CountValues: r.CountValues, Min: int64(r.MinBits), MinValid: r.MinValid, Max: int64(r.MaxBits), MaxValid: r.MaxValid}
}

func (v *NumericVector) compatible(r NumericResult) error {
	if r.Type != v.typ || r.Layout != v.layout {
		return fmt.Errorf("aggregate: numeric state type/layout mismatch")
	}
	return nil
}

// InitResult replaces a slot with a compatible raw state. Type or layout
// mismatches leave the destination unchanged. To clear a group, pass a zero
// NumericResult carrying the destination Type and Layout.
func (v *NumericVector) InitResult(slot int, r NumericResult) error {
	if err := v.compatible(r); err != nil {
		return err
	}
	if v.typ == memory.TypeInt64 {
		v.legacy.InitResult(slot, r.int64State())
		return nil
	}
	if v.layout.Sum {
		v.sums[slot] = r.SumBits
		v.sumValid[slot] = 0
		if r.SumValid {
			v.sumValid[slot] = 1
		}
	}
	if v.layout.Min {
		v.mins[slot] = normalizeNumericBits(v.typ, r.MinBits)
		v.minValid[slot] = 0
		if r.MinValid {
			v.minValid[slot] = 1
		}
	}
	if v.layout.Max {
		v.maxs[slot] = normalizeNumericBits(v.typ, r.MaxBits)
		v.maxValid[slot] = 0
		if r.MaxValid {
			v.maxValid[slot] = 1
		}
	}
	if v.layout.CountRows {
		v.countRows[slot] = r.CountRows
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot] = r.CountValues
	}
	if v.layout.Avg {
		v.avgSums[slot] = r.AvgSum
	}
	return nil
}

// MergeResult combines a compatible raw state without averaging averages.
// Type or layout mismatches leave the destination unchanged. Floating totals
// follow IEEE arithmetic and may round differently under another merge order.
func (v *NumericVector) MergeResult(slot int, r NumericResult) error {
	if err := v.compatible(r); err != nil {
		return err
	}
	if v.typ == memory.TypeInt64 {
		v.legacy.MergeResult(slot, r.int64State())
		return nil
	}
	if v.layout.Sum && r.SumValid {
		v.sums[slot] = numericSum(v.typ, v.sums[slot], r.SumBits)
		v.sumValid[slot] = 1
	}
	if v.layout.Min && r.MinValid {
		bits := normalizeNumericBits(v.typ, r.MinBits)
		if v.minValid[slot] == 0 {
			v.mins[slot] = bits
		} else {
			v.mins[slot] = numericExtreme(v.typ, v.mins[slot], bits, true)
		}
		v.minValid[slot] = 1
	}
	if v.layout.Max && r.MaxValid {
		bits := normalizeNumericBits(v.typ, r.MaxBits)
		if v.maxValid[slot] == 0 {
			v.maxs[slot] = bits
		} else {
			v.maxs[slot] = numericExtreme(v.typ, v.maxs[slot], bits, false)
		}
		v.maxValid[slot] = 1
	}
	if v.layout.CountRows {
		v.countRows[slot] += r.CountRows
	}
	if v.layout.CountValues || v.layout.Avg {
		v.countValues[slot] += r.CountValues
	}
	if v.layout.Avg {
		v.avgSums[slot] += r.AvgSum
	}
	return nil
}

// Copy transfers one state to a compatible destination, preserving raw bits.
// Mismatches return an error without modifying the destination.
func (v *NumericVector) Copy(dst *NumericVector, srcSlot, dstSlot int) error {
	if dst == nil {
		return fmt.Errorf("aggregate: numeric destination is nil")
	}
	return dst.InitResult(dstSlot, v.Result(srcSlot))
}
