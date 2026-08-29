package memory

import "fmt"

// CloneBatch creates an owned copy of every vector in src. Specialized vectors
// are deep-copied. Opaque source-native values that expose Arrow-style
// Retain/Release semantics are retained and marked owned by the returned batch.
func CloneBatch(src *Batch) (*Batch, error) {
	if src == nil {
		return nil, fmt.Errorf("cannot clone nil batch")
	}
	dst := NewBatch()
	dst.Reset(src.Rows)
	for k, v := range src.Int64s {
		dst.Int64s[k] = &Int64Vector{Values: append([]int64(nil), v.Values...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Int32s {
		dst.Int32s[k] = &Int32Vector{Values: append([]int32(nil), v.Values...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Uint32s {
		dst.Uint32s[k] = &Uint32Vector{Values: append([]uint32(nil), v.Values...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Uint64s {
		dst.Uint64s[k] = &Uint64Vector{Values: append([]uint64(nil), v.Values...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Float64s {
		dst.Float64s[k] = &Float64Vector{Values: append([]float64(nil), v.Values...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Strings {
		dst.Strings[k] = &StringViewVector{Offsets: append([]uint32(nil), v.Offsets...), Data: append([]byte(nil), v.Data...), Validity: append(Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.DictStrings {
		dst.DictStrings[k] = &DictionaryStringVector{Indices: append([]uint32(nil), v.Indices...), Validity: append(Bitmap(nil), v.Validity...), Offsets: append([]uint32(nil), v.Offsets...), Data: append([]byte(nil), v.Data...), DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
	}
	for k, v := range src.StringLists {
		dst.StringLists[k] = &StringListVector{
			Offsets:  append([]uint32(nil), v.Offsets...),
			Validity: append(Bitmap(nil), v.Validity...),
			Encoding: v.Encoding,
			Values: &StringViewVector{
				Offsets:  append([]uint32(nil), v.Values.Offsets...),
				Data:     append([]byte(nil), v.Values.Data...),
				Validity: append(Bitmap(nil), v.Values.Validity...),
				Encoding: v.Values.Encoding,
			},
		}
	}
	for k, v := range src.Opaque {
		if v == nil {
			continue
		}
		rr, ok := v.Value.(interface {
			Retain()
			Release()
		})
		if !ok {
			dst.ReleaseOwned()
			return nil, fmt.Errorf("opaque column %q does not expose Retain/Release", k)
		}
		rr.Retain()
		dst.Opaque[k] = &OpaqueVector{Rows: v.Rows, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: append(Bitmap(nil), v.Validity...), Value: v.Value, Owned: true}
	}
	return dst, nil
}
