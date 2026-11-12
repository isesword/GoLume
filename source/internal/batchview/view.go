// Package batchview binds bounded views of immutable source buffers.
// Packed Arrow Booleans are decoded into native uint32 values per output batch.
// It does not read or iterate partitions; native sources own those semantics.
package batchview

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/memory"
	"gocolumn/source"
)

// View owns range metadata, decoded Boolean buffers, and Arrow slice references
// for one output. Other source values and dictionaries remain borrowed.
type View struct {
	reservation memory.Reservation
	owned       []*memory.OpaqueVector
}

// Release drops owned slice references and their scratch reservation.
func (v *View) Release() {
	for _, x := range v.owned {
		if x.Owned {
			x.Value.(interface{ Release() }).Release()
			x.Owned = false
		}
	}
	v.owned = nil
	if v.reservation != nil {
		v.reservation.Release()
		v.reservation = nil
	}
}

// Bind fills the next row/byte-bounded range and returns its row count. Byte
// limits and reservations cover decoded Boolean values and new validity/offset buffers;
// caller-owned immutable data and Arrow buffers are not charged again.
func (v *View) Bind(ctx context.Context, dst, src *memory.Batch, offset int, opts source.ReadOptions) (int, error) {
	v.Release()
	dst.ClearColumns()
	if src == nil || offset < 0 || offset > src.Rows {
		return 0, fmt.Errorf("invalid source batch range")
	}
	rows := min(opts.BatchRows, src.Rows-offset)
	// Payload and allocation limits are independent. A legal single row may
	// exceed the soft target, but never the caller's hard payload limit.
	rows, err := selectDecodedRangeRows(src, offset, rows, opts.BatchPayloadTargetBytes, opts.BatchPayloadBytes)
	if err != nil {
		return 0, err
	}
	bytes, err := scratchBytes(src, offset, rows)
	if err != nil {
		return 0, err
	}
	if bytes > opts.BatchBytes {
		lo, hi := 0, rows
		for lo < hi {
			mid := lo + (hi-lo+1)/2
			size, err := scratchBytes(src, offset, mid)
			if err != nil {
				return 0, err
			}
			if size <= opts.BatchBytes {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		rows = lo
		if rows == 0 {
			return 0, fmt.Errorf("source row metadata exceeds batch byte limit %d: %w", opts.BatchBytes, memory.ErrBudgetExceeded)
		}
		bytes, err = scratchBytes(src, offset, rows)
		if err != nil {
			return 0, err
		}
	}
	if opts.MemoryManager != nil && bytes > 0 {
		v.reservation, err = opts.MemoryManager.Reserve(ctx, bytes)
		if err != nil {
			return 0, err
		}
	}
	if err := v.bind(ctx, dst, src, offset, rows); err != nil {
		v.Release()
		dst.ClearColumns()
		return 0, err
	}
	return rows, nil
}

func validityBytes(bits memory.Bitmap, offset, length int) int64 {
	if bits == nil || length == 0 || offset%64 == 0 {
		return 0
	}
	return int64(memory.WordsFor(length)) * 8
}

func validity(bits memory.Bitmap, offset, length int) memory.Bitmap {
	if bits == nil || length == 0 {
		return nil
	}
	if offset%64 == 0 {
		return bits[offset/64 : offset/64+memory.WordsFor(length)]
	}
	out := make(memory.Bitmap, memory.WordsFor(length))
	for i := 0; i < length; i++ {
		if memory.IsSet(bits, offset+i) {
			memory.Set(out, i)
		}
	}
	return out
}

func offsets(in []uint32, offset, length int) []uint32 {
	out := in[offset : offset+length+1]
	if out[0] == 0 {
		return out
	}
	base := out[0]
	result := make([]uint32, len(out))
	for i, n := range out {
		result[i] = n - base
	}
	return result
}

func stringBytes(v *memory.StringViewVector, offset, length int) (int64, error) {
	end := offset + length
	if v != nil && length == 0 && len(v.Offsets) == 0 {
		return 0, nil
	}
	if v == nil || len(v.Offsets) < end+1 {
		return 0, fmt.Errorf("string offsets shorter than source range")
	}
	if v.Offsets[offset] > v.Offsets[end] || int(v.Offsets[end]) > len(v.Data) {
		return 0, fmt.Errorf("invalid source string offsets")
	}
	if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
		return 0, fmt.Errorf("string validity shorter than source range")
	}
	n := validityBytes(v.Validity, offset, length)
	if v.Offsets[offset] != 0 {
		n += int64(length+1) * 4
	}
	return n, nil
}

func stringView(v *memory.StringViewVector, offset, length int) *memory.StringViewVector {
	if length == 0 && len(v.Offsets) == 0 {
		return &memory.StringViewVector{Encoding: v.Encoding}
	}
	return &memory.StringViewVector{Offsets: offsets(v.Offsets, offset, length), Data: v.Data[v.Offsets[offset]:v.Offsets[offset+length]], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
}

func scratchBytes(src *memory.Batch, offset, length int) (int64, error) {
	end := offset + length
	var n int64
	for name, v := range src.Int8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Int16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Int32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Int64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Uint8s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Uint16s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Uint32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Uint64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Float32s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Decimals {
		if v == nil || len(v.Values) < end || v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("decimal column %q shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Float64s {
		if v == nil || len(v.Values) < end {
			return 0, fmt.Errorf("column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Strings {
		size, err := stringBytes(v, offset, length)
		if err != nil {
			return 0, fmt.Errorf("column %q: %w", name, err)
		}
		n += size
	}
	for name, v := range src.DictStrings {
		if v == nil || len(v.Indices) < end {
			return 0, fmt.Errorf("dictionary column %q shorter than source range", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Int64Lists {
		if v == nil || v.Values == nil || len(v.Offsets) < end+1 {
			return 0, fmt.Errorf("list column %q shorter than source range", name)
		}
		lo, hi := v.Offsets[offset], v.Offsets[end]
		if lo > hi || uint64(hi) > uint64(v.Values.Len()) || v.Validity != nil && len(v.Validity) < memory.WordsFor(end) || v.Values.Validity != nil && len(v.Values.Validity) < memory.WordsFor(int(hi)) {
			return 0, fmt.Errorf("list column %q invalid offsets or validity", name)
		}
		for row := offset; row < end; row++ {
			if v.Offsets[row] > v.Offsets[row+1] {
				return 0, fmt.Errorf("list column %q decreasing offsets", name)
			}
		}
		n += validityBytes(v.Validity, offset, length) + validityBytes(v.Values.Validity, int(lo), int(hi-lo))
		if lo != 0 {
			n += int64(length+1) * 4
		}
	}
	for name, v := range src.StringLists {
		if v == nil || v.Values == nil || len(v.Offsets) < end+1 {
			return 0, fmt.Errorf("list column %q shorter than source range", name)
		}
		lo, hi := v.Offsets[offset], v.Offsets[end]
		if lo > hi {
			return 0, fmt.Errorf("invalid list column %q offsets", name)
		}
		size, err := stringBytes(v.Values, int(lo), int(hi-lo))
		if err != nil {
			return 0, fmt.Errorf("list column %q: %w", name, err)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += size + validityBytes(v.Validity, offset, length)
		if lo != 0 {
			n += int64(length+1) * 4
		}
	}
	for name, v := range src.Structs {
		if v == nil || v.Values == nil || v.Rows < end {
			return 0, fmt.Errorf("struct column %q shorter than source range", name)
		}
		size, err := scratchBytes(v.Values, offset, length)
		if err != nil {
			return 0, err
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += size + validityBytes(v.Validity, offset, length)
	}
	for name, v := range src.Opaque {
		if v == nil || v.Rows < end {
			return 0, fmt.Errorf("opaque column %q shorter than source range", name)
		}
		if a, ok := packedBoolean(v); ok {
			if a == nil || a.Len() < end {
				return 0, fmt.Errorf("Boolean column %q shorter than source range", name)
			}
			var err error
			n, err = addDecodedBooleanBytes(n, length)
			if err != nil {
				return 0, err
			}
		}
		if _, ok := v.Value.(arrow.Array); !ok && (offset != 0 || length != src.Rows) {
			return 0, fmt.Errorf("opaque column %q has no slicing contract", name)
		}
		if v.Validity != nil && len(v.Validity) < memory.WordsFor(end) {
			return 0, fmt.Errorf("column %q validity shorter than source range", name)
		}
		n += validityBytes(v.Validity, offset, length)
	}
	return n, nil
}

func (view *View) bind(ctx context.Context, dst, src *memory.Batch, offset, length int) error {
	end := offset + length
	for name, v := range src.Int8s {
		dst.Int8s[name] = &memory.Int8Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int16s {
		dst.Int16s[name] = &memory.Int16Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int32s {
		dst.Int32s[name] = &memory.Int32Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Int64s {
		dst.Int64s[name] = &memory.Int64Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint8s {
		dst.Uint8s[name] = &memory.Uint8Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint16s {
		dst.Uint16s[name] = &memory.Uint16Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint32s {
		dst.Uint32s[name] = &memory.Uint32Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Uint64s {
		dst.Uint64s[name] = &memory.Uint64Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Float32s {
		dst.Float32s[name] = &memory.Float32Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Decimals {
		dst.Decimals[name] = &memory.Decimal128Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Precision: v.Precision, Scale: v.Scale}
	}
	for name, v := range src.Float64s {
		dst.Float64s[name] = &memory.Float64Vector{Values: v.Values[offset:end], Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Strings {
		dst.Strings[name] = stringView(v, offset, length)
	}
	for name, v := range src.DictStrings {
		dst.DictStrings[name] = &memory.DictionaryStringVector{Indices: v.Indices[offset:end], Validity: validity(v.Validity, offset, length), Offsets: v.Offsets, Data: v.Data, Encoding: v.Encoding, DictionaryDomain: v.DictionaryDomain}
	}
	for name, v := range src.Int64Lists {
		lo, hi := v.Offsets[offset], v.Offsets[end]
		dst.Int64Lists[name] = &memory.Int64ListVector{Offsets: offsets(v.Offsets, offset, length), Values: &memory.Int64Vector{Values: v.Values.Values[lo:hi], Validity: validity(v.Values.Validity, int(lo), int(hi-lo)), Encoding: v.Values.Encoding}, Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.StringLists {
		lo, hi := v.Offsets[offset], v.Offsets[end]
		dst.StringLists[name] = &memory.StringListVector{Offsets: offsets(v.Offsets, offset, length), Values: stringView(v.Values, int(lo), int(hi-lo)), Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Structs {
		values := memory.NewBatch()
		if err := view.bind(ctx, values, v.Values, offset, length); err != nil {
			return err
		}
		dst.Structs[name] = &memory.StructVector{Rows: length, Fields: v.Fields, Values: values, Validity: validity(v.Validity, offset, length), Encoding: v.Encoding}
	}
	for name, v := range src.Opaque {
		if a, ok := packedBoolean(v); ok {
			values := make([]uint32, length)
			for row := range values {
				if row%4096 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if (v.Validity == nil || memory.IsSet(v.Validity, offset+row)) && a.Value(offset+row) {
					values[row] = 1
				}
			}
			dst.Uint32s[name] = &memory.Uint32Vector{Values: values, Validity: validity(v.Validity, offset, length), Encoding: memory.EncodingPlain}
			continue
		}
		out := &memory.OpaqueVector{Rows: length, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: validity(v.Validity, offset, length), Value: v.Value}
		if a, ok := v.Value.(arrow.Array); ok && (offset != 0 || length != src.Rows) {
			out.Value = array.NewSlice(a, int64(offset), int64(end))
			out.Owned = true
			view.owned = append(view.owned, out)
		}
		dst.Opaque[name] = out
	}
	dst.Reset(length)
	return nil
}
