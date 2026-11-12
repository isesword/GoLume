package physical

import (
	"fmt"
	"gocolumn/memory"
)

// copyByteJoinSlice preserves nil validity and uses exactly the admitted
// element capacity rather than append-to-nil allocator growth.
func copyByteJoinSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	out := make([]T, len(values))
	copy(out, values)
	return out
}

// cloneByteJoinBatch owns every payload buffer with exact slice capacities.
// Full Struct clones copy their child batches directly, without a temporary
// row-index array or incremental per-row growth.
func cloneByteJoinBatch(src *memory.Batch) (*memory.Batch, error) {
	if src == nil {
		return nil, fmt.Errorf("generic join payload has nil batch")
	}
	if len(src.Opaque) != 0 {
		return nil, fmt.Errorf("cannot retain opaque payload in Phase 2.6 join build")
	}
	dst := memory.NewBatch()
	dst.Reset(src.Rows)
	for k, v := range src.Int8s {
		dst.Int8s[k] = &memory.Int8Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}
	for k, v := range src.Int16s {
		dst.Int16s[k] = &memory.Int16Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}
	for k, v := range src.Uint8s {
		dst.Uint8s[k] = &memory.Uint8Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}
	for k, v := range src.Uint16s {
		dst.Uint16s[k] = &memory.Uint16Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = &memory.Float32Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}
	for k, v := range src.Int64s {
		x := &memory.Int64Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Int64s[k] = x
	}
	for k, v := range src.Int32s {
		x := &memory.Int32Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Int32s[k] = x
	}
	for k, v := range src.Uint32s {
		x := &memory.Uint32Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Uint32s[k] = x
	}
	for k, v := range src.Uint64s {
		x := &memory.Uint64Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Uint64s[k] = x
	}
	for k, v := range src.Float64s {
		x := &memory.Float64Vector{Values: copyByteJoinSlice(v.Values), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Float64s[k] = x
	}
	for k, v := range src.Strings {
		x := &memory.StringViewVector{Offsets: copyByteJoinSlice(v.Offsets), Data: copyByteJoinSlice(v.Data), Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
		dst.Strings[k] = x
	}
	for k, v := range src.DictStrings {
		x := &memory.DictionaryStringVector{Indices: copyByteJoinSlice(v.Indices), Validity: copyByteJoinSlice(v.Validity), Offsets: copyByteJoinSlice(v.Offsets), Data: copyByteJoinSlice(v.Data), DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
		dst.DictStrings[k] = x
	}
	for k, v := range src.StringLists {
		x := &memory.StringListVector{
			Offsets:  copyByteJoinSlice(v.Offsets),
			Validity: copyByteJoinSlice(v.Validity),
			Encoding: v.Encoding,
			Values:   &memory.StringViewVector{Offsets: copyByteJoinSlice(v.Values.Offsets), Data: copyByteJoinSlice(v.Values.Data), Validity: copyByteJoinSlice(v.Values.Validity), Encoding: v.Values.Encoding},
		}
		dst.StringLists[k] = x
	}
	for k, v := range src.Int64Lists {
		x := &memory.Int64ListVector{
			Offsets:  copyByteJoinSlice(v.Offsets),
			Validity: copyByteJoinSlice(v.Validity),
			Encoding: v.Encoding,
			Values:   &memory.Int64Vector{Values: copyByteJoinSlice(v.Values.Values), Validity: copyByteJoinSlice(v.Values.Validity), Encoding: v.Values.Encoding},
		}
		dst.Int64Lists[k] = x
	}
	for k, v := range src.Structs {
		values, err := cloneByteJoinBatch(v.Values)
		if err != nil {
			return nil, err
		}
		dst.Structs[k] = &memory.StructVector{Fields: v.Fields, Values: values, Rows: v.Rows, Validity: copyByteJoinSlice(v.Validity), Encoding: v.Encoding}
	}

	return dst, nil
}
