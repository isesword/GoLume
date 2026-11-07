package aggregate

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// MomentsResult is the mergeable, fixed-size state for numeric variance. Mean
// is relative to AnchorBits unless Uncentered is true. The unnormalized squared
// deviation total is Scale*Scale*ScaledM2; keeping it scaled avoids intermediate
// overflow. Codecs must validate a decoded result with RestoreMoments.
type MomentsResult struct {
	Type                  memory.DataType
	Count                 uint64
	AnchorBits            uint64
	Mean, Scale, ScaledM2 float64
	Uncentered, NonFinite bool
}

// MomentsState accumulates numeric variance without allocating. Integer inputs
// are differenced exactly before conversion to Float64, preserving small
// differences near the ends of the Int64 and UInt64 ranges. It is worker-local.
type MomentsState struct{ result MomentsResult }

// NewMomentsState creates an empty state for a native numeric input type.
func NewMomentsState(typ memory.DataType) (MomentsState, error) {
	if !isAggregateNumeric(typ) {
		return MomentsState{}, fmt.Errorf("aggregate: moments require numeric input, got %s", typ)
	}
	return MomentsState{result: MomentsResult{Type: typ}}, nil
}

// RestoreMoments validates a decoded intermediate state before accepting it.
func RestoreMoments(r MomentsResult) (MomentsState, error) {
	if !isAggregateNumeric(r.Type) || r.AnchorBits != normalizeNumericBits(r.Type, r.AnchorBits) ||
		math.IsNaN(r.Mean) || math.IsInf(r.Mean, 0) || math.IsNaN(r.Scale) || math.IsInf(r.Scale, 0) ||
		math.IsNaN(r.ScaledM2) || math.IsInf(r.ScaledM2, 0) || r.Scale < 0 || r.ScaledM2 < 0 ||
		(r.Scale == 0) != (r.ScaledM2 == 0) || ((r.Uncentered || r.NonFinite) && !momentsFloat(r.Type)) ||
		(r.Count <= 1 && r.Scale != 0) {
		return MomentsState{}, fmt.Errorf("aggregate: invalid moments state")
	}
	if r.Count == 0 && (r.AnchorBits != 0 || r.Mean != 0 || r.Scale != 0 || r.NonFinite || r.Uncentered) {
		return MomentsState{}, fmt.Errorf("aggregate: nonempty moments payload with zero count")
	}
	if r.Count > 0 && momentsFloat(r.Type) && !finiteMoment(numericAsFloat(r.Type, r.AnchorBits)) {
		return MomentsState{}, fmt.Errorf("aggregate: nonfinite moments anchor")
	}
	return MomentsState{result: r}, nil
}

// Snapshot returns raw mergeable state, not finalized variance.
func (s MomentsState) Snapshot() MomentsResult { return s.result }

// Reset clears accumulated rows while retaining the input type.
func (s *MomentsState) Reset() { s.result = MomentsResult{Type: s.result.Type} }

func momentsFloat(t memory.DataType) bool { return t == memory.TypeFloat32 || t == memory.TypeFloat64 }
func finiteMoment(x float64) bool         { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// integerAnchorDelta subtracts before rounding to Float64. Unsigned subtraction
// after ordering also handles opposite-sign signed values without overflow.
func integerAnchorDelta(t memory.DataType, x, anchor uint64) float64 {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		if int64(x) >= int64(anchor) {
			return float64(x - anchor)
		}
		return -float64(anchor - x)
	default:
		if x >= anchor {
			return float64(x - anchor)
		}
		return -float64(anchor - x)
	}
}

func (r *MomentsResult) addScaled(scale, amount float64) {
	if scale == 0 || amount == 0 {
		return
	}
	if r.Scale < scale {
		ratio := r.Scale / scale
		r.ScaledM2 = amount + r.ScaledM2*ratio*ratio
		r.Scale = scale
	} else {
		ratio := scale / r.Scale
		r.ScaledM2 += amount * ratio * ratio
	}
}

// addDelta adds weight*(x-y)^2 without overflowing the difference or square.
func (r *MomentsResult) addDelta(x, y, weight float64) {
	delta := x - y
	if finiteMoment(delta) {
		r.addScaled(math.Abs(delta), weight)
		return
	}
	r.addScaled(math.Abs(x/2-y/2), 4*weight)
}

func (r MomentsResult) absoluteMean() float64 {
	if r.Uncentered {
		return r.Mean
	}
	return numericAsFloat(r.Type, r.AnchorBits) + r.Mean
}

// AddBits consumes one typed numeric row. Null payloads are never inspected.
// A count-overflow error leaves the state unchanged. Native bits follow the
// NumericVector contract. NaN and infinity count as values and poison variance.
func (s *MomentsState) AddBits(bits uint64, valid bool) error {
	r := &s.result
	if !isAggregateNumeric(r.Type) {
		return fmt.Errorf("aggregate: uninitialized moments state")
	}
	if !valid {
		return nil
	}
	if r.Count == math.MaxUint64 {
		return fmt.Errorf("aggregate: moments count overflow")
	}
	bits = normalizeNumericBits(r.Type, bits)
	n := r.Count + 1
	x := numericAsFloat(r.Type, bits)
	if momentsFloat(r.Type) && !finiteMoment(x) {
		r.Count = n
		r.NonFinite = true
		return nil
	}
	if r.NonFinite {
		r.Count = n
		return nil
	}
	if r.Count == 0 {
		r.Count, r.AnchorBits = 1, bits
		return nil
	}
	coordinate := x
	if !r.Uncentered {
		if momentsFloat(r.Type) {
			coordinate = x - numericAsFloat(r.Type, r.AnchorBits)
		} else {
			coordinate = integerAnchorDelta(r.Type, bits, r.AnchorBits)
		}
		if !finiteMoment(coordinate) {
			r.Mean, r.Uncentered = r.absoluteMean(), true
			coordinate = x
		}
	}
	weight := float64(r.Count) / float64(n)
	r.addDelta(coordinate, r.Mean, weight)
	delta := coordinate - r.Mean
	if finiteMoment(delta) {
		r.Mean += delta / float64(n)
	} else {
		r.Mean = r.Mean*weight + coordinate/float64(n)
	}
	r.Count = n
	return nil
}

// Merge combines compatible partial states using scaled Chan statistics.
// Count overflow and incompatible types fail without modifying the receiver.
func (s *MomentsState) Merge(other MomentsState) error {
	a, b := s.result, other.result
	if !isAggregateNumeric(a.Type) || a.Type != b.Type {
		return fmt.Errorf("aggregate: incompatible moments types")
	}
	if b.Count > math.MaxUint64-a.Count {
		return fmt.Errorf("aggregate: moments count overflow")
	}
	if b.Count == 0 {
		return nil
	}
	if a.Count == 0 {
		s.result = b
		return nil
	}
	n := a.Count + b.Count
	if a.NonFinite || b.NonFinite {
		a.Count, a.NonFinite = n, true
		s.result = a
		return nil
	}
	right := b.Mean
	if !momentsFloat(a.Type) {
		right += integerAnchorDelta(a.Type, b.AnchorBits, a.AnchorBits)
	} else if a.Uncentered || b.Uncentered {
		a.Mean, right, a.Uncentered = a.absoluteMean(), b.absoluteMean(), true
	} else {
		right += numericAsFloat(b.Type, b.AnchorBits) - numericAsFloat(a.Type, a.AnchorBits)
		if !finiteMoment(right) {
			a.Mean, right, a.Uncentered = a.absoluteMean(), b.absoluteMean(), true
		}
	}
	fraction := float64(b.Count) / float64(n)
	a.addScaled(b.Scale, b.ScaledM2)
	a.addDelta(right, a.Mean, float64(a.Count)*fraction)
	delta := right - a.Mean
	if finiteMoment(delta) {
		a.Mean += delta * fraction
	} else {
		a.Mean = a.Mean*(float64(a.Count)/float64(n)) + right*fraction
	}
	a.Count = n
	s.result = a
	return nil
}

// Variance finalizes with divisor n-ddof. Empty and insufficient samples are
// NULL. The caller chooses ddof explicitly; sample variance convention is one.
// Float32 consumers should round only this final result to their output type.
func (s MomentsState) Variance(ddof uint64) (float64, bool) {
	r := s.result
	if r.Count == 0 || r.Count <= ddof {
		return 0, false
	}
	if r.NonFinite {
		return math.NaN(), true
	}
	if r.Scale == 0 {
		return 0, true
	}
	m, exponent := math.Frexp(r.Scale)
	return math.Ldexp(m*m*(r.ScaledM2/float64(r.Count-ddof)), 2*exponent), true
}

// StdDev finalizes standard deviation without first overflowing variance.
// This can remain finite when the representable variance itself is +Inf.
func (s MomentsState) StdDev(ddof uint64) (float64, bool) {
	r := s.result
	if r.Count == 0 || r.Count <= ddof {
		return 0, false
	}
	if r.NonFinite {
		return math.NaN(), true
	}
	return r.Scale * math.Sqrt(r.ScaledM2/float64(r.Count-ddof)), true
}

// SourceOrdinal is a stable logical input position. Partition is the logical
// partition's source-order index, not a worker ID; Row is its original row
// offset. Producers must preserve these positions through filtering/spilling.
type SourceOrdinal struct{ Partition, Row uint64 }

func ordinalLess(a, b SourceOrdinal) bool {
	return a.Partition < b.Partition || a.Partition == b.Partition && a.Row < b.Row
}

// EndpointResult stores FIRST/LAST fixed-width values, including endpoint NULL.
// Seen distinguishes empty input from an observed NULL. Decoded state must pass
// RestoreEndpoints before being used. Variable-length values need owned state.
type EndpointResult struct {
	Type                      memory.DataType
	Seen                      bool
	FirstOrdinal, LastOrdinal SourceOrdinal
	FirstBits, LastBits       uint64
	FirstValid, LastValid     bool
}

// EndpointState chooses fixed-width endpoints by logical ordinal rather than
// encounter or worker merge order. It owns no buffers and performs no allocation.
type EndpointState struct{ result EndpointResult }

func endpointType(t memory.DataType) bool {
	if isAggregateNumeric(t) {
		return true
	}
	switch t {
	case memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
		return true
	}
	return false
}
func endpointBits(t memory.DataType, bits uint64) uint64 {
	if t == memory.TypeDate32 || t == memory.TypeTime32 {
		return uint64(int64(int32(bits)))
	}
	return normalizeNumericBits(t, bits)
}

// NewEndpointState creates a fixed-width FIRST/LAST state.
func NewEndpointState(typ memory.DataType) (EndpointState, error) {
	if !endpointType(typ) {
		return EndpointState{}, fmt.Errorf("aggregate: endpoints require supported fixed-width input, got %s", typ)
	}
	return EndpointState{result: EndpointResult{Type: typ}}, nil
}

// RestoreEndpoints validates decoded endpoint ordering and typed payloads.
func RestoreEndpoints(r EndpointResult) (EndpointState, error) {
	if !endpointType(r.Type) || ordinalLess(r.LastOrdinal, r.FirstOrdinal) ||
		(!r.FirstValid && r.FirstBits != 0) || (!r.LastValid && r.LastBits != 0) ||
		r.FirstBits != endpointBits(r.Type, r.FirstBits) || r.LastBits != endpointBits(r.Type, r.LastBits) ||
		(r.Type == memory.TypeBool && (r.FirstBits > 1 || r.LastBits > 1)) {
		return EndpointState{}, fmt.Errorf("aggregate: invalid endpoint state")
	}
	if !r.Seen && (r.FirstValid || r.LastValid || r.FirstOrdinal != (SourceOrdinal{}) || r.LastOrdinal != (SourceOrdinal{})) {
		return EndpointState{}, fmt.Errorf("aggregate: nonempty endpoint payload")
	}
	if r.Seen && r.FirstOrdinal == r.LastOrdinal && (r.FirstBits != r.LastBits || r.FirstValid != r.LastValid) {
		return EndpointState{}, fmt.Errorf("aggregate: conflicting endpoint ordinal")
	}
	return EndpointState{result: r}, nil
}

// Snapshot returns a copied raw endpoint state.
func (s EndpointState) Snapshot() EndpointResult { return s.result }

// Reset clears endpoints while preserving the input type.
func (s *EndpointState) Reset() { s.result = EndpointResult{Type: s.result.Type} }

// AddBits considers an original input row, including NULL rows. Reusing an
// endpoint ordinal with a different value is an error and leaves state unchanged.
func (s *EndpointState) AddBits(bits uint64, valid bool, ordinal SourceOrdinal) error {
	r := s.result
	if !endpointType(r.Type) {
		return fmt.Errorf("aggregate: uninitialized endpoint state")
	}
	if !valid {
		bits = 0
	} else {
		bits = endpointBits(r.Type, bits)
	}
	if r.Type == memory.TypeBool && bits > 1 {
		return fmt.Errorf("aggregate: invalid boolean endpoint")
	}
	if !r.Seen {
		r.Seen = true
		r.FirstBits, r.LastBits = bits, bits
		r.FirstValid, r.LastValid = valid, valid
		r.FirstOrdinal, r.LastOrdinal = ordinal, ordinal
	} else {
		if ordinal == r.FirstOrdinal && (bits != r.FirstBits || valid != r.FirstValid) || ordinal == r.LastOrdinal && (bits != r.LastBits || valid != r.LastValid) {
			return fmt.Errorf("aggregate: conflicting endpoint ordinal")
		}
		if ordinalLess(ordinal, r.FirstOrdinal) {
			r.FirstOrdinal, r.FirstBits, r.FirstValid = ordinal, bits, valid
		}
		if ordinalLess(r.LastOrdinal, ordinal) {
			r.LastOrdinal, r.LastBits, r.LastValid = ordinal, bits, valid
		}
	}
	s.result = r
	return nil
}

// Merge combines endpoint states independent of worker arrival order.
func (s *EndpointState) Merge(other EndpointState) error {
	if !endpointType(s.result.Type) || s.result.Type != other.result.Type {
		return fmt.Errorf("aggregate: incompatible endpoint types")
	}
	if !other.result.Seen {
		return nil
	}
	copy := *s
	if err := copy.AddBits(other.result.FirstBits, other.result.FirstValid, other.result.FirstOrdinal); err != nil {
		return err
	}
	if err := copy.AddBits(other.result.LastBits, other.result.LastValid, other.result.LastOrdinal); err != nil {
		return err
	}
	*s = copy
	return nil
}

// First returns the first logical row, or NULL for empty input or a NULL endpoint.
func (s EndpointState) First() (bits uint64, valid bool) {
	return s.result.FirstBits, s.result.Seen && s.result.FirstValid
}

// Last returns the last logical row, or NULL for empty input or a NULL endpoint.
func (s EndpointState) Last() (bits uint64, valid bool) {
	return s.result.LastBits, s.result.Seen && s.result.LastValid
}
