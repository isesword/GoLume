package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/bits"
)

// QuantileMode selects the interpolation between adjacent exact order statistics.
type QuantileMode uint8

const (
	// QuantileNearest selects the nearest rank, rounding half ranks upward.
	QuantileNearest QuantileMode = iota
	// QuantileLower selects the lower rank.
	QuantileLower
	// QuantileHigher selects the higher rank.
	QuantileHigher
	// QuantileMidpoint averages the bounding ranks.
	QuantileMidpoint
	// QuantileLinear linearly interpolates between the bounding ranks.
	QuantileLinear
	// QuantileEquiprobable selects ceil(n*q)-1, clamped at zero.
	QuantileEquiprobable
)

// QuantileRanks describes the zero-based endpoints and their interpolation weight.
// Valid is false for an empty input. Otherwise Lower and Upper are below n.
type QuantileRanks struct {
	Lower, Upper uint64
	Weight       float64
	Valid        bool
}

// PlanQuantile computes ranks without rounding the input count to Float64.
// q is interpreted as its exact binary Float64 value. No memory is allocated.
func PlanQuantile(n uint64, q float64, mode QuantileMode) (QuantileRanks, error) {
	if math.IsNaN(q) || q < 0 || q > 1 || mode > QuantileEquiprobable {
		return QuantileRanks{}, fmt.Errorf("aggregate: invalid quantile probability or mode")
	}
	if n == 0 {
		return QuantileRanks{}, nil
	}
	count := n - 1
	if mode == QuantileEquiprobable {
		count = n
	}
	floor, fraction, nonzero, half := quantileProduct(count, q)
	r := QuantileRanks{Lower: floor, Upper: floor, Valid: true}
	if mode == QuantileEquiprobable {
		if nonzero {
			floor++
		}
		if floor > 0 {
			floor--
		}
		r.Lower = floor
		r.Upper = floor
		return r, nil
	}
	switch mode {
	case QuantileNearest:
		if half {
			r.Lower++
			r.Upper++
		}
	case QuantileHigher:
		if nonzero {
			r.Lower++
			r.Upper++
		}
	case QuantileMidpoint:
		if nonzero {
			r.Upper++
			r.Weight = .5
		}
	case QuantileLinear:
		if nonzero {
			r.Upper++
			r.Weight = fraction
		}
	}
	return r, nil
}

// quantileProduct returns floor(n*q), fractional approximation, remainder != 0,
// and remainder >= 1/2. The latter two are exact even for subnormal q.
func quantileProduct(n uint64, q float64) (uint64, float64, bool, bool) {
	if q == 1 {
		return n, 0, false, false
	}
	if q == 0 || n == 0 {
		return 0, 0, false, false
	}
	b := math.Float64bits(q)
	exp := int((b >> 52) & 2047)
	mant := b & ((1 << 52) - 1)
	shift := 1074
	if exp != 0 {
		mant |= 1 << 52
		shift = 1075 - exp
	}
	hi, lo := bits.Mul64(n, mant)
	var whole, rhi, rlo uint64
	if shift < 64 {
		whole = hi<<(64-shift) | lo>>shift
		rlo = lo & ((uint64(1) << shift) - 1)
	} else if shift == 64 {
		whole = hi
		rlo = lo
	} else if shift < 128 {
		whole = hi >> (shift - 64)
		rhi = hi & ((uint64(1) << (shift - 64)) - 1)
		rlo = lo
	} else {
		rhi = hi
		rlo = lo
	}
	nonzero := rhi != 0 || rlo != 0
	half := false
	if shift <= 64 {
		half = rlo>>(shift-1) != 0
	} else if shift <= 128 {
		half = rhi>>(shift-65) != 0
	}
	fraction := math.Ldexp(float64(rhi), 64-shift) + math.Ldexp(float64(rlo), -shift)
	if fraction >= 1 {
		fraction = math.Nextafter(1, 0)
	}
	return whole, fraction, nonzero, half
}

// QuantileOutputType returns Float32 for Float32 input and Float64 for all
// other numeric inputs. Non-numeric input is rejected.
func QuantileOutputType(t memory.DataType) (memory.DataType, error) {
	if !isAggregateNumeric(t) {
		return memory.TypeInvalid, fmt.Errorf("aggregate: quantile requires numeric input, got %s", t)
	}
	if t == memory.TypeFloat32 {
		return t, nil
	}
	return memory.TypeFloat64, nil
}

// InterpolateQuantile finalizes native endpoint bits selected by PlanQuantile.
// Values must be ordered and normalized to their input type. Interpolation uses
// Float64 arithmetic with overflow-safe finite convex combinations; output is
// rounded to Float32 only at finalization. NaNs remain valid values.
func InterpolateQuantile(t memory.DataType, lower, upper uint64, plan QuantileRanks) (uint64, bool, error) {
	out, err := QuantileOutputType(t)
	if err != nil {
		return 0, false, err
	}
	if !plan.Valid {
		return 0, false, nil
	}
	if lower != normalizeNumericBits(t, lower) || upper != normalizeNumericBits(t, upper) || math.IsNaN(plan.Weight) || plan.Weight < 0 || plan.Weight > 1 || plan.Lower > plan.Upper {
		return 0, false, fmt.Errorf("aggregate: invalid quantile endpoints or plan")
	}
	a, b := numericAsFloat(t, lower), numericAsFloat(t, upper)
	v := a
	if plan.Lower != plan.Upper && plan.Weight != 0 {
		w := plan.Weight
		switch {
		case w == 1:
			v = b
		case math.IsNaN(a) || math.IsNaN(b):
			v = math.NaN()
		case t != memory.TypeFloat32 && t != memory.TypeFloat64:
			v = interpolateQuantileIntegers(t, lower, upper, w)
		case a == b:
			v = a
		case math.IsInf(a, 0) || math.IsInf(b, 0):
			v = a*(1-w) + b*w
		case math.Signbit(a) == math.Signbit(b):
			v = a + (b-a)*w
		default:
			v = interpolateQuantileOpposite(a, b, w)
		}
	}
	if math.IsNaN(v) {
		v = math.NaN()
	}
	if out == memory.TypeFloat32 {
		return uint64(math.Float32bits(float32(v))), true, nil
	}
	return math.Float64bits(v), true, nil
}

// QuantileOrderKey returns an unsigned sort key preserving native numeric order.
// Signed zeros compare equal; all NaNs compare equal after positive infinity.
// Callers append a stable source ordinal to break ties without losing duplicates.
func QuantileOrderKey(t memory.DataType, value uint64) (uint64, error) {
	if !isAggregateNumeric(t) || value != normalizeNumericBits(t, value) {
		return 0, fmt.Errorf("aggregate: invalid native quantile value")
	}
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		return value ^ (uint64(1) << 63), nil
	case memory.TypeFloat32, memory.TypeFloat64:
		f := numericAsFloat(t, value)
		if math.IsNaN(f) {
			return math.MaxUint64, nil
		}
		if f == 0 {
			f = 0
		}
		v := math.Float64bits(f)
		if v>>63 != 0 {
			return ^v, nil
		}
		return v ^ (uint64(1) << 63), nil
	default:
		return value, nil
	}
}

// Integer endpoints are split exactly before interpolation, avoiding both
// unsigned subtraction overflow and loss of a small residual when opposite
// Int64 extrema round to symmetric Float64 values.
func quantileIntegerParts(t memory.DataType, value uint64) (float64, float64) {
	hi := numericAsFloat(t, value)
	var top float64
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		top = float64(int64(value)>>32) * 4294967296
	default:
		top = float64(value>>32) * 4294967296
	}
	return hi, (top - hi) + float64(uint32(value))
}
func quantileTwoSum(a, b float64) (float64, float64) {
	s := a + b
	z := s - a
	return s, (a - (s - z)) + (b - z)
}
func interpolateQuantileIntegers(t memory.DataType, a, b uint64, w float64) float64 {
	ah, al := quantileIntegerParts(t, a)
	bh, bl := quantileIntegerParts(t, b)
	d, de := quantileTwoSum(bh, -ah)
	p := d * w
	pe := math.FMA(d, w, -p) + (de+bl-al)*w
	sum, se := quantileTwoSum(ah, p)
	return sum + (se + al + pe)
}

// Recover both product roundoff and the exact complement of w. Computing
// (1-w) alone loses a bit near half and destroys small cancellation residuals.
func interpolateQuantileOpposite(a, b, w float64) float64 {
	complement, ce := quantileTwoSum(1, -w)
	left, right := a*complement, b*w
	le := math.FMA(a, complement, -left) + a*ce
	re := math.FMA(b, w, -right)
	sum, se := quantileTwoSum(left, right)
	return sum + (se + le + re)
}
