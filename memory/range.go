package memory

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"math"
)

// EstimateBatchRangeBytes returns the historical logical payload charge for a
// contiguous row range. It is not a complete owned-allocation bound. Native
// owned fan-out uses EstimateNativeOwnedBatchRangeBytes; Opaque retains this
// legacy sizing contract.
func EstimateBatchRangeBytes(src *Batch, offset, length int) (int64, error) {
	return estimateBatchRangeBytes(src, offset, length, false)
}

// inheritedNulls accounts for validity materialized by canonical struct clone
// when an ancestor is NULL, even if the original child has no validity buffer.
func estimateBatchRangeBytes(src *Batch, offset, length int, inheritedNulls bool) (int64, error) {
	return estimateBatchRangeBytesMode(src, offset, length, inheritedNulls, true)
}

// includeOpaque=false isolates native logical payload without inspecting Opaque
// arrays, datatypes, caches or extension implementations.
func estimateBatchRangeBytesMode(src *Batch, offset, length int, inheritedNulls, includeOpaque bool) (int64, error) {
	return estimateBatchRangeBytesComponents(src, offset, length, inheritedNulls, includeOpaque, nil)
}

// legacyOpaque optionally collects only the historical Opaque entry charges.
// Recursive child totals already contain these entries, so only the entry loop
// increments the shared accumulator; parents must not add child totals to it.
func estimateBatchRangeBytesComponents(src *Batch, offset, length int, inheritedNulls, includeOpaque bool, legacyOpaque *int64) (int64, error) {
	return estimateBatchRangeBytesAccounting(src, offset, length, inheritedNulls, includeOpaque, legacyOpaque, nil, true)
}

// bulkNative counts only pointer-free native payload copied in one allocation
// per backing. Non-empty Struct children use row append and never enter it.
func estimateBatchRangeBytesAccounting(src *Batch, offset, length int, inheritedNulls, includeOpaque bool, legacyOpaque, bulkNative *int64, bulkMode bool) (int64, error) {
	if src == nil {
		return 0, fmt.Errorf("cannot estimate range from nil batch")
	}
	if offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return 0, fmt.Errorf("batch range offset=%d length=%d out of rows=%d", offset, length, src.Rows)
	}
	end := offset + length
	var total int64
	add := func(bytes int64) error {
		if bytes < 0 || total > math.MaxInt64-bytes {
			return fmt.Errorf("batch range byte estimate overflows int64")
		}
		total += bytes
		return nil
	}
	addNative := func(bytes int64) error {
		if err := add(bytes); err != nil {
			return err
		}
		if bulkNative != nil && bulkMode {
			if *bulkNative > math.MaxInt64-bytes {
				return fmt.Errorf("batch range byte estimate overflows int64")
			}
			*bulkNative += bytes
		}
		return nil
	}

	validityBytes := func(validity Bitmap) int64 {
		if (validity == nil && !inheritedNulls) || length == 0 {
			return 0
		}
		return int64(WordsFor(length)) * 8
	}
	for name, v := range src.Int8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("int8 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*1 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("int16 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*2 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("uint8 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*1 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("uint16 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*2 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Float32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("float32 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*4 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("int64 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*8 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("int32 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*4 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("uint32 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*4 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Uint64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("uint64 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*8 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Decimals {
		if err := v.validateRange(offset, length); err != nil {
			return 0, fmt.Errorf("decimal column %q: %w", name, err)
		}
		if int64(length) > math.MaxInt64/16 {
			return 0, fmt.Errorf("decimal range byte estimate overflows int64")
		}
		if err := addNative(int64(length)*16 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Float64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("float64 column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*8 + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Strings {
		lo, hi, err := stringRangeBounds(v, offset, length)
		if err != nil {
			return 0, fmt.Errorf("string column %q: %w", name, err)
		}
		if err := addNative(int64(length+1)*4 + int64(hi-lo) + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	for name, v := range src.DictStrings {
		if v == nil || len(v.Indices) < end {
			return 0, fmt.Errorf("dictionary column %q is shorter than range", name)
		}
		if err := addNative(int64(length)*4 + validityBytes(v.Validity) + int64(len(v.Offsets))*4 + int64(len(v.Data))); err != nil {
			return 0, err
		}
	}
	for name, v := range src.StringLists {
		valueStart, valueEnd, dataStart, dataEnd, err := stringListRangeBounds(v, offset, length)
		if err != nil {
			return 0, fmt.Errorf("list column %q: %w", name, err)
		}
		childLength := int64(valueEnd - valueStart)
		childValidity := int64(0)
		if v.Values.Validity != nil && childLength > 0 {
			childValidity = int64(WordsFor(int(childLength))) * 8
		}
		bytes := int64(length+1)*4 + validityBytes(v.Validity) + (childLength+1)*4 + int64(dataEnd-dataStart) + childValidity
		if err := addNative(bytes); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Int64Lists {
		lo, hi, err := int64ListRangeBounds(v, offset, length)
		if err != nil {
			return 0, fmt.Errorf("list %q: %w", name, err)
		}
		childLength := int(hi - lo)
		childValidity := int64(0)
		if v.Values.Validity != nil && childLength > 0 {
			childValidity = int64(WordsFor(childLength)) * 8
		}
		if err := addNative(int64(length+1)*4 + validityBytes(v.Validity) + int64(childLength)*8 + childValidity); err != nil {
			return 0, err
		}
	}
	for name, v := range src.Structs {
		if v == nil || v.Values == nil || v.Rows < end {
			return 0, fmt.Errorf("struct column %q is shorter than range", name)
		}
		childNulls := inheritedNulls
		if v.Validity != nil {
			if len(v.Validity) < WordsFor(end) {
				return 0, fmt.Errorf("struct column %q validity is shorter than range", name)
			}
			for row := offset; row < end && !childNulls; row++ {
				if !IsSet(v.Validity, row) {
					childNulls = true
				}
			}
		}
		childBytes, err := estimateBatchRangeBytesAccounting(v.Values, offset, length, childNulls, includeOpaque, legacyOpaque, bulkNative, bulkMode && length == 0)
		if err != nil {
			return 0, fmt.Errorf("struct column %q: %w", name, err)
		}
		if err := add(childBytes + validityBytes(v.Validity)); err != nil {
			return 0, err
		}
	}
	if includeOpaque {
		for name, v := range src.Opaque {
			if v == nil || v.Value == nil || v.Rows < end {
				return 0, fmt.Errorf("opaque column %q is shorter than range", name)
			}
			a, ok := v.Value.(arrow.Array)
			if !ok {
				return 0, fmt.Errorf("opaque column %q has no byte sizing contract", name)
			}
			size := a.Data().SizeInBytes()
			if size > math.MaxInt64 {
				return 0, fmt.Errorf("opaque column %q byte size overflows int64", name)
			}
			charge := int64(size) + validityBytes(v.Validity)
			if err := add(charge); err != nil {
				return 0, err
			}
			if legacyOpaque != nil {
				if *legacyOpaque > math.MaxInt64-charge {
					return 0, fmt.Errorf("batch range byte estimate overflows int64")
				}
				*legacyOpaque += charge
			}
		}
	}
	return total, nil
}

// CloneBatchRange creates an owned copy of the contiguous row range
// [offset, offset+length). Specialized vectors are copied and opaque Arrow
// arrays are retained through a zero-copy Arrow slice. Empty string vectors,
// including list children, own a canonical zero offset even when the source
// represents zero values with no offsets. NULL validity remains distinct from
// an empty non-NULL value.
func CloneBatchRange(src *Batch, offset, length int) (*Batch, error) {
	if src == nil {
		return nil, fmt.Errorf("cannot clone range from nil batch")
	}
	if offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return nil, fmt.Errorf("batch range offset=%d length=%d out of rows=%d", offset, length, src.Rows)
	}
	end := offset + length
	dst := NewBatch()
	dst.Reset(length)
	fail := func(column string, err error) (*Batch, error) {
		dst.ReleaseOwned()
		return nil, fmt.Errorf("clone range column %q: %w", column, err)
	}
	for name, v := range src.Int8s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("int8 vector shorter than range"))
		}
		dst.Int8s[name] = &Int8Vector{Values: append([]int8(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int16s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("int16 vector shorter than range"))
		}
		dst.Int16s[name] = &Int16Vector{Values: append([]int16(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint8s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("uint8 vector shorter than range"))
		}
		dst.Uint8s[name] = &Uint8Vector{Values: append([]uint8(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint16s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("uint16 vector shorter than range"))
		}
		dst.Uint16s[name] = &Uint16Vector{Values: append([]uint16(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Float32s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("float32 vector shorter than range"))
		}
		dst.Float32s[name] = &Float32Vector{Values: append([]float32(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int64s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("int64 vector shorter than range"))
		}
		dst.Int64s[name] = &Int64Vector{Values: append([]int64(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int32s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("int32 vector shorter than range"))
		}
		dst.Int32s[name] = &Int32Vector{Values: append([]int32(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint32s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("uint32 vector shorter than range"))
		}
		dst.Uint32s[name] = &Uint32Vector{Values: append([]uint32(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint64s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("uint64 vector shorter than range"))
		}
		dst.Uint64s[name] = &Uint64Vector{Values: append([]uint64(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Decimals {
		if err := v.validateRange(offset, length); err != nil {
			return fail(name, err)
		}
		dst.Decimals[name] = &Decimal128Vector{Values: append([]Decimal128(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Precision: v.Precision, Scale: v.Scale}
	}
	for name, v := range src.Float64s {
		if v == nil || len(v.Values) < end {
			return fail(name, fmt.Errorf("float64 vector shorter than range"))
		}
		dst.Float64s[name] = &Float64Vector{Values: append([]float64(nil), v.Values[offset:end]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Strings {
		lo, hi, err := stringRangeBounds(v, offset, length)
		if err != nil {
			return fail(name, err)
		}
		offsets, err := cloneRangeOffsets(v.Offsets, offset, length, lo)
		if err != nil {
			return fail(name, err)
		}
		dst.Strings[name] = &StringViewVector{Offsets: offsets, Data: append([]byte(nil), v.Data[lo:hi]...), Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.DictStrings {
		if v == nil || len(v.Indices) < end {
			return fail(name, fmt.Errorf("dictionary indices shorter than range"))
		}
		dst.DictStrings[name] = &DictionaryStringVector{
			Indices:          append([]uint32(nil), v.Indices[offset:end]...),
			Validity:         cloneValidityRange(v.Validity, offset, length),
			Offsets:          append([]uint32(nil), v.Offsets...),
			Data:             append([]byte(nil), v.Data...),
			DictionaryDomain: v.DictionaryDomain,
			Encoding:         v.Encoding,
		}
	}
	for name, v := range src.StringLists {
		valueStart, valueEnd, dataStart, dataEnd, err := stringListRangeBounds(v, offset, length)
		if err != nil {
			return fail(name, err)
		}
		parentOffsets, err := cloneRangeOffsets(v.Offsets, offset, length, valueStart)
		if err != nil {
			return fail(name, err)
		}
		childLength := int(valueEnd - valueStart)
		childOffsets, err := cloneRangeOffsets(v.Values.Offsets, int(valueStart), childLength, dataStart)
		if err != nil {
			return fail(name, err)
		}
		dst.StringLists[name] = &StringListVector{
			Offsets:  parentOffsets,
			Validity: cloneValidityRange(v.Validity, offset, length),
			Encoding: v.Encoding,
			Values: &StringViewVector{
				Offsets:  childOffsets,
				Data:     append([]byte(nil), v.Values.Data[dataStart:dataEnd]...),
				Validity: cloneValidityRange(v.Values.Validity, int(valueStart), childLength),
				Encoding: v.Values.Encoding,
			},
		}
	}
	for name, v := range src.Int64Lists {
		lo, hi, err := int64ListRangeBounds(v, offset, length)
		if err != nil {
			return fail(name, err)
		}
		offsets, err := cloneRangeOffsets(v.Offsets, offset, length, lo)
		if err != nil {
			return fail(name, err)
		}
		dst.Int64Lists[name] = &Int64ListVector{Offsets: offsets, Validity: cloneValidityRange(v.Validity, offset, length), Encoding: v.Encoding, Values: &Int64Vector{Values: append([]int64(nil), v.Values.Values[lo:hi]...), Validity: cloneValidityRange(v.Values.Validity, int(lo), int(hi-lo)), Encoding: v.Values.Encoding}}
	}

	for name, v := range src.Structs {
		cloned, err := CloneStructRange(v, offset, length)
		if err != nil {
			return fail(name, err)
		}
		dst.Structs[name] = cloned
	}
	for name, v := range src.Opaque {
		if v == nil || v.Value == nil || v.Rows < end {
			return fail(name, fmt.Errorf("opaque vector shorter than range"))
		}
		if a, ok := v.Value.(arrow.Array); ok {
			sliced := array.NewSlice(a, int64(offset), int64(end))
			dst.Opaque[name] = &OpaqueVector{Rows: length, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: cloneValidityRange(v.Validity, offset, length), Value: sliced, Owned: true}
			continue
		}
		if offset != 0 || length != src.Rows {
			return fail(name, fmt.Errorf("non-Arrow opaque value cannot be sliced"))
		}
		rr, ok := v.Value.(interface {
			Retain()
			Release()
		})
		if !ok {
			return fail(name, fmt.Errorf("opaque value does not expose Retain/Release"))
		}
		rr.Retain()
		dst.Opaque[name] = &OpaqueVector{Rows: length, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: cloneValidityRange(v.Validity, offset, length), Value: v.Value, Owned: true}
	}
	return dst, nil
}

// CloneStructRange creates an owned contiguous range of a StructVector.
func CloneStructRange(src *StructVector, offset, length int) (*StructVector, error) {
	if src == nil || src.Values == nil {
		return nil, fmt.Errorf("cannot clone nil struct range")
	}
	if offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return nil, fmt.Errorf("struct range offset=%d length=%d out of rows=%d", offset, length, src.Rows)
	}
	if length == 0 {
		// No row append will initialize the children. Preserve their physical
		// types, dictionary metadata and empty offset sentinels explicitly.
		children, err := CloneBatchRange(src.Values, offset, 0)
		if err != nil {
			return nil, err
		}
		return &StructVector{Fields: append([]Field(nil), src.Fields...), Values: children, Encoding: EncodingView}, nil
	}
	out := &StructVector{Fields: append([]Field(nil), src.Fields...), Values: NewBatch(), Encoding: EncodingView}
	for row := offset; row < offset+length; row++ {
		if err := AppendStructRow(out, src, row, src.IsValid(row)); err != nil {
			out.Values.ReleaseOwned()
			return nil, err
		}
	}
	return out, nil
}

func cloneValidityRange(src Bitmap, offset, length int) Bitmap {
	if src == nil || length == 0 {
		return nil
	}
	out := make(Bitmap, WordsFor(length))
	for row := 0; row < length; row++ {
		if IsSet(src, offset+row) {
			Set(out, row)
		}
	}
	return out
}

// rangeOffsetBounds checks endpoints without allocation. Cloning checks
// interior offsets while normalizing them; estimation stays constant-time per column
// because callers repeatedly probe ranges during byte-bounded splitting.
// Missing offsets represent only an empty string vector, never a list parent.
func rangeOffsetBounds(offsets []uint32, offset, length, limit int, emptyString bool) (uint32, uint32, error) {
	if emptyString && len(offsets) == 0 && offset == 0 && length == 0 {
		return 0, 0, nil
	}
	if offset < 0 || length < 0 || offset >= len(offsets) || length >= len(offsets)-offset {
		return 0, 0, fmt.Errorf("offsets are shorter than range")
	}
	lo, hi := offsets[offset], offsets[offset+length]
	if lo > hi || uint64(hi) > uint64(limit) {
		return 0, 0, fmt.Errorf("range offsets exceed bounds")
	}
	return lo, hi, nil
}

func rangeValidityBounds(validity Bitmap, offset, length int) error {
	if validity != nil && length > 0 && len(validity) < WordsFor(offset+length) {
		return fmt.Errorf("validity is shorter than range")
	}
	return nil
}

func stringRangeBounds(v *StringViewVector, offset, length int) (uint32, uint32, error) {
	if v == nil {
		return 0, 0, fmt.Errorf("string vector is nil")
	}
	lo, hi, err := rangeOffsetBounds(v.Offsets, offset, length, len(v.Data), true)
	if err != nil {
		return 0, 0, err
	}
	if err := rangeValidityBounds(v.Validity, offset, length); err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

func stringListRangeBounds(v *StringListVector, offset, length int) (uint32, uint32, uint32, uint32, error) {
	if v == nil || v.Values == nil {
		return 0, 0, 0, 0, fmt.Errorf("list vector or child is nil")
	}
	lo, hi, err := rangeOffsetBounds(v.Offsets, offset, length, v.Values.Len(), false)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err := rangeValidityBounds(v.Validity, offset, length); err != nil {
		return 0, 0, 0, 0, err
	}
	dataLo, dataHi, err := stringRangeBounds(v.Values, int(lo), int(hi-lo))
	return lo, hi, dataLo, dataHi, err
}

// cloneRangeOffsets validates interiors in the same pass that normalizes them.
// Endpoints have already been checked by rangeOffsetBounds. No partial offsets
// are exposed on failure, and no source buffer is mutated.
func cloneRangeOffsets(src []uint32, offset, length int, base uint32) ([]uint32, error) {
	out := make([]uint32, length+1)
	if len(src) != 0 {
		previous, end := base, src[offset+length]
		for i := range out {
			next := src[offset+i]
			if next < previous || next > end {
				return nil, fmt.Errorf("range offsets are not monotonic")
			}
			out[i] = next - base
			previous = next
		}
	}
	return out, nil
}
