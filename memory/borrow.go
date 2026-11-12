package memory

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// BorrowBatchRange binds a contiguous range without copying column payloads.
// The owner must keep src valid until the returned batch has been cleared.
// Returned maps/vector headers are independent; misaligned validity bits and rebased offsets
// and Arrow slice headers allocate range storage. ClearColumns releases any
// retained Arrow slices, never source-owned bindings.
func BorrowBatchRange(src *Batch, offset, length int) (*Batch, error) {
	if _, err := BorrowedRangeMetadataBytes(src, offset, length); err != nil {
		return nil, err
	}
	dst := NewBatch()
	dst.Rows = length
	end := offset + length
	fail := func(err error) (*Batch, error) { dst.ClearColumns(); return nil, err }
	for name, v := range src.Int8s {
		dst.Int8s[name] = &Int8Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int16s {
		dst.Int16s[name] = &Int16Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int32s {
		dst.Int32s[name] = &Int32Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int64s {
		dst.Int64s[name] = &Int64Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint8s {
		dst.Uint8s[name] = &Uint8Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint16s {
		dst.Uint16s[name] = &Uint16Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint32s {
		dst.Uint32s[name] = &Uint32Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint64s {
		dst.Uint64s[name] = &Uint64Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Float32s {
		dst.Float32s[name] = &Float32Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Decimals {
		dst.Decimals[name] = &Decimal128Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Precision: v.Precision, Scale: v.Scale}
	}
	for name, v := range src.Float64s {
		dst.Float64s[name] = &Float64Vector{Values: v.Values[offset:end], Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Strings {
		dst.Strings[name] = borrowStringRange(v, offset, length)
	}
	for name, v := range src.DictStrings {
		dst.DictStrings[name] = &DictionaryStringVector{Indices: v.Indices[offset:end], Offsets: v.Offsets, Data: v.Data, Validity: borrowValidityRange(v.Validity, offset, length), DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
	}
	for name, v := range src.StringLists {
		lo, hi := v.Offsets[offset], v.Offsets[end]
		dst.StringLists[name] = &StringListVector{Offsets: borrowOffsets(v.Offsets, offset, length), Values: borrowStringRange(v.Values, int(lo), int(hi-lo)), Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int64Lists {
		lo, hi := v.Offsets[offset], v.Offsets[end]
		dst.Int64Lists[name] = &Int64ListVector{Offsets: borrowOffsets(v.Offsets, offset, length), Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding, Values: &Int64Vector{Values: v.Values.Values[lo:hi], Validity: borrowValidityRange(v.Values.Validity, int(lo), int(hi-lo)), Encoding: v.Values.Encoding}}
	}
	for name, v := range src.Structs {
		children, err := BorrowBatchRange(v.Values, offset, length)
		if err != nil {
			return fail(err)
		}
		dst.Structs[name] = &StructVector{Rows: length, Fields: v.Fields, Values: children, Validity: borrowValidityRange(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Opaque {
		view := &OpaqueVector{Rows: length, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: borrowValidityRange(v.Validity, offset, length), Value: v.Value}
		if offset != 0 || length != src.Rows {
			a, ok := v.Value.(arrow.Array)
			if !ok {
				return fail(fmt.Errorf("non-Arrow opaque column %q cannot be sliced", name))
			}
			view.Value = array.NewSlice(a, int64(offset), int64(end))
			view.Owned = true
		}
		dst.Opaque[name] = view
	}
	return dst, nil
}

// BorrowedRangeMetadataBytes sizes newly allocated validity and offset buffers.
// Column payloads remain charged to their source owner. Map/vector/Arrow
// headers follow the same accounting convention as BatchBytes.
func BorrowedRangeMetadataBytes(src *Batch, offset, length int) (int64, error) {
	if src == nil || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return 0, fmt.Errorf("invalid borrowed batch range")
	}
	// Validate typed storage without asking opaque values for an owned-clone
	// byte contract. A whole opaque binding can be borrowed without sizing it;
	// bounded exchanges independently require their actual byte contract.
	typed := *src
	typed.Opaque = nil
	typed.Structs = nil
	if _, err := EstimateBatchRangeBytes(&typed, offset, length); err != nil {
		return 0, err
	}
	var total int64
	add := func(validity Bitmap) error {
		if validity == nil || length == 0 {
			return nil
		}
		if len(validity) < WordsFor(offset+length) {
			return fmt.Errorf("borrowed range validity is shorter than rows")
		}
		if offset&63 != 0 {
			total += int64(WordsFor(length)) * 8
		}
		return nil
	}
	for _, v := range src.Int8s {
		if v == nil {
			return 0, fmt.Errorf("nil int8 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Int16s {
		if v == nil {
			return 0, fmt.Errorf("nil int16 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Int32s {
		if v == nil {
			return 0, fmt.Errorf("nil int32 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Int64s {
		if v == nil {
			return 0, fmt.Errorf("nil int64 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Uint8s {
		if v == nil {
			return 0, fmt.Errorf("nil uint8 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Uint16s {
		if v == nil {
			return 0, fmt.Errorf("nil uint16 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Uint32s {
		if v == nil {
			return 0, fmt.Errorf("nil uint32 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Uint64s {
		if v == nil {
			return 0, fmt.Errorf("nil uint64 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Float32s {
		if v == nil {
			return 0, fmt.Errorf("nil float32 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Decimals {
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Float64s {
		if v == nil {
			return 0, fmt.Errorf("nil float64 vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Strings {
		if v == nil || len(v.Offsets) < offset+length+1 {
			return 0, fmt.Errorf("short string offsets")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
		if v.Offsets[offset] != 0 {
			total += int64(length+1) * 4
		}
	}
	for _, v := range src.DictStrings {
		if v == nil {
			return 0, fmt.Errorf("nil DictStrings vector")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.StringLists {
		if v == nil || v.Values == nil || len(v.Offsets) < offset+length+1 {
			return 0, fmt.Errorf("short list offsets")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
		lo, hi := int(v.Offsets[offset]), int(v.Offsets[offset+length])
		if lo > hi || len(v.Values.Offsets) < hi+1 {
			return 0, fmt.Errorf("invalid list child offsets")
		}
		if lo != 0 {
			total += int64(length+1) * 4
		}
		if v.Values.Offsets[lo] != 0 {
			total += int64(hi-lo+1) * 4
		}
		if v.Values.Validity != nil && hi > lo {
			if len(v.Values.Validity) < WordsFor(hi) {
				return 0, fmt.Errorf("short list child validity")
			}
			if lo&63 != 0 {
				total += int64(WordsFor(hi-lo)) * 8
			}
		}
	}
	for _, v := range src.Int64Lists {
		lo, hi, err := int64ListRangeBounds(v, offset, length)
		if err != nil {
			return 0, err
		}
		if err := ownedRangeOffsets(v.Offsets, offset, length); err != nil {
			return 0, err
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
		if lo != 0 {
			total += int64(length+1) * 4
		}
		if v.Values.Validity != nil && hi > lo && lo%64 != 0 {
			total += int64(WordsFor(int(hi-lo))) * 8
		}
	}
	for _, v := range src.Opaque {
		if v == nil || v.Value == nil || v.Rows < offset+length {
			return 0, fmt.Errorf("opaque vector shorter than borrowed range")
		}
		if offset != 0 || length != src.Rows {
			if _, ok := v.Value.(arrow.Array); !ok {
				return 0, fmt.Errorf("non-Arrow opaque value cannot be sliced")
			}
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
	}
	for _, v := range src.Structs {
		if v == nil || v.Values == nil || v.Rows < offset+length {
			return 0, fmt.Errorf("struct vector shorter than borrowed range")
		}
		if err := add(v.Validity); err != nil {
			return 0, err
		}
		child, err := BorrowedRangeMetadataBytes(v.Values, offset, length)
		if err != nil {
			return 0, err
		}
		total += child
	}
	return total, nil
}

func borrowValidityRange(src Bitmap, offset, length int) Bitmap {
	if src == nil || length == 0 {
		return nil
	}
	if offset&63 == 0 {
		return src[offset>>6 : (offset>>6)+WordsFor(length)]
	}
	return cloneValidityRange(src, offset, length)
}

func borrowOffsets(src []uint32, offset, length int) []uint32 {
	out := src[offset : offset+length+1]
	base := out[0]
	if base == 0 {
		return out
	}
	rebased := make([]uint32, len(out))
	for i, value := range out {
		rebased[i] = value - base
	}
	return rebased
}
func borrowStringRange(src *StringViewVector, offset, length int) *StringViewVector {
	return &StringViewVector{Offsets: borrowOffsets(src.Offsets, offset, length), Data: src.Data[src.Offsets[offset]:src.Offsets[offset+length]], Validity: borrowValidityRange(src.Validity, offset, length), Encoding: src.Encoding}
}

// BatchPayloadTooLargeError identifies a single row that cannot be emitted
// under a native stream's hard payload limit.
type BatchPayloadTooLargeError struct {
	Row               int
	Bytes, LimitBytes int64
}

// Error describes the oversized row and its hard payload limit.
func (e *BatchPayloadTooLargeError) Error() string {
	return fmt.Sprintf("source row %d payload %d exceeds hard batch payload limit %d", e.Row, e.Bytes, e.LimitBytes)
}

// Unwrap identifies this admission failure as a memory budget error.
func (e *BatchPayloadTooLargeError) Unwrap() error { return ErrBudgetExceeded }

// SelectBatchRangeRows selects a maximal prefix under a soft payload target.
// One row may exceed the target, but never a positive hard limit. Both limits
// count logical payload (including shared dictionary/opaque backing), not new
// allocations; zero disables the corresponding limit.
func SelectBatchRangeRows(src *Batch, offset, maxRows int, targetBytes, hardBytes int64) (int, error) {
	if src == nil || offset < 0 || maxRows < 0 || offset > src.Rows || maxRows > src.Rows-offset || targetBytes < 0 || hardBytes < 0 {
		return 0, fmt.Errorf("invalid batch payload range or limits")
	}
	if maxRows == 0 {
		return 0, nil
	}
	limit := targetBytes
	if limit == 0 || (hardBytes > 0 && hardBytes < limit) {
		limit = hardBytes
	}
	if limit == 0 {
		return maxRows, nil
	}
	size, err := EstimateBatchRangeBytes(src, offset, maxRows)
	if err != nil {
		return 0, err
	}
	if size <= limit {
		return maxRows, nil
	}
	one, err := EstimateBatchRangeBytes(src, offset, 1)
	if err != nil {
		return 0, err
	}
	if hardBytes > 0 && one > hardBytes {
		return 0, &BatchPayloadTooLargeError{Row: offset, Bytes: one, LimitBytes: hardBytes}
	}
	if one > limit {
		return 1, nil
	}
	lo, hi := 1, maxRows-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		size, err := EstimateBatchRangeBytes(src, offset, mid)
		if err != nil {
			return 0, err
		}
		if size <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, nil
}
