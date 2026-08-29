package arrowrecord

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type reader struct {
	owner    *recordOwner
	bindings []binding
}

// Close releases reader-local resources. Arrow buffers remain owned by the
// shared Source until Source.Close is called.
func (r *reader) Close() error { return nil }

// ReadPartition binds one retained Arrow RecordBatch into dst using typed fast
// paths where available and OpaqueVector bindings for all other Arrow types.
func (r *reader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.ID < 0 || p.ID >= len(r.owner.records) {
		return fmt.Errorf("arrowrecord: partition %d out of range", p.ID)
	}
	rec := r.owner.records[p.ID]
	rows := int(rec.NumRows())
	dst.ClearColumns()
	for _, b := range r.bindings {
		arr := rec.Column(b.index)
		validity := cachedValidity(b, p.ID)
		dst.Opaque[b.name] = &memory.OpaqueVector{Rows: rows, Type: b.field.Type, ArrowType: b.field.ArrowType, Encoding: b.field.Encoding, Validity: validity, Value: arr}
		bound, err := bindTyped(dst, b, arr, p.ID, validity)
		if err != nil {
			return err
		}
		if bound {
			delete(dst.Opaque, b.name)
		}
	}
	dst.Reset(rows)
	return nil
}

func bindTyped(dst *memory.Batch, b binding, arr arrow.Array, partition int, validity memory.Bitmap) (bool, error) {
	switch a := arr.(type) {
	case *array.Int32:
		dst.Int32s[b.name] = &memory.Int32Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Int64:
		dst.Int64s[b.name] = &memory.Int64Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Uint32:
		dst.Uint32s[b.name] = &memory.Uint32Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Uint64:
		dst.Uint64s[b.name] = &memory.Uint64Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Float64:
		dst.Float64s[b.name] = &memory.Float64Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Int8, *array.Int16, *array.Date32, *array.Time32:
		if partition < len(b.i32) && b.i32[partition] != nil {
			dst.Int32s[b.name] = &memory.Int32Vector{Values: b.i32[partition], Validity: validity, Encoding: b.field.Encoding}
		} else {
			return false, nil
		}
	case *array.Uint8, *array.Uint16:
		if partition < len(b.u32) && b.u32[partition] != nil {
			dst.Uint32s[b.name] = &memory.Uint32Vector{Values: b.u32[partition], Validity: validity, Encoding: b.field.Encoding}
		} else {
			return false, nil
		}
	case *array.Float32:
		if partition < len(b.f64) && b.f64[partition] != nil {
			dst.Float64s[b.name] = &memory.Float64Vector{Values: b.f64[partition], Validity: validity, Encoding: b.field.Encoding}
		} else {
			return false, nil
		}
	case *array.Date64, *array.Timestamp, *array.Time64, *array.Duration:
		if partition < len(b.i64) && b.i64[partition] != nil {
			dst.Int64s[b.name] = &memory.Int64Vector{Values: b.i64[partition], Validity: validity, Encoding: b.field.Encoding}
		} else {
			return false, nil
		}
	case *array.String:
		dst.Strings[b.name] = &memory.StringViewVector{Offsets: cachedOffsets(b, partition), Data: a.ValueBytes(), Validity: validity, Encoding: memory.EncodingView}
	case *array.Binary:
		dst.Strings[b.name] = &memory.StringViewVector{Offsets: cachedOffsets(b, partition), Data: a.ValueBytes(), Validity: validity, Encoding: memory.EncodingView}
	case *array.Dictionary:
		if b.dict == nil {
			return false, nil // non-string dictionary remains available through Opaque.
		}
		if partition < 0 || partition >= len(b.dict.indices) {
			return false, fmt.Errorf("arrowrecord: dictionary partition %d out of range", partition)
		}
		dst.DictStrings[b.name] = &memory.DictionaryStringVector{
			Indices:          b.dict.indices[partition],
			Validity:         validity,
			Offsets:          b.dict.offsets,
			Data:             b.dict.data,
			DictionaryDomain: b.dict.domain,
			Encoding:         memory.EncodingDictionary,
		}
	case *array.List:
		if b.field.Type != memory.TypeList || partition < 0 || partition >= len(b.stringLists) || b.stringLists[partition] == nil {
			return false, nil
		}
		dst.StringLists[b.name] = b.stringLists[partition]
	default:
		return false, nil
	}
	return true, nil
}

func cachedValidity(b binding, partition int) memory.Bitmap {
	if partition < 0 || partition >= len(b.validities) {
		return nil
	}
	return b.validities[partition]
}

func cachedOffsets(b binding, partition int) []uint32 {
	if partition < 0 || partition >= len(b.offsets) {
		return nil
	}
	return b.offsets[partition]
}

func validityBitmap(arr arrow.Array) memory.Bitmap {
	if arr.NullN() == 0 {
		return nil
	}
	out := make(memory.Bitmap, memory.WordsFor(arr.Len()))
	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) {
			memory.Set(out, i)
		}
	}
	return out
}

func offsets32(in []int32) []uint32 {
	if len(in) == 0 {
		return nil
	}
	base := in[0]
	out := make([]uint32, len(in))
	for i, v := range in {
		out[i] = uint32(v - base)
	}
	return out
}

func dictionaryIndices(d *array.Dictionary) ([]uint32, error) {
	idx := d.Indices()
	out := make([]uint32, idx.Len())
	switch a := idx.(type) {
	case *array.Int8:
		for i, v := range a.Values() {
			if v < 0 {
				return nil, fmt.Errorf("arrowrecord: negative dictionary index")
			}
			out[i] = uint32(v)
		}
	case *array.Int16:
		for i, v := range a.Values() {
			if v < 0 {
				return nil, fmt.Errorf("arrowrecord: negative dictionary index")
			}
			out[i] = uint32(v)
		}
	case *array.Int32:
		for i, v := range a.Values() {
			if v < 0 {
				return nil, fmt.Errorf("arrowrecord: negative dictionary index")
			}
			out[i] = uint32(v)
		}
	case *array.Int64:
		for i, v := range a.Values() {
			if v < 0 || uint64(v) > uint64(^uint32(0)) {
				return nil, fmt.Errorf("arrowrecord: dictionary index out of uint32 range")
			}
			out[i] = uint32(v)
		}
	case *array.Uint8:
		for i, v := range a.Values() {
			out[i] = uint32(v)
		}
	case *array.Uint16:
		for i, v := range a.Values() {
			out[i] = uint32(v)
		}
	case *array.Uint32:
		copy(out, a.Values())
	case *array.Uint64:
		for i, v := range a.Values() {
			if v > uint64(^uint32(0)) {
				return nil, fmt.Errorf("arrowrecord: dictionary index out of uint32 range")
			}
			out[i] = uint32(v)
		}
	default:
		return nil, fmt.Errorf("arrowrecord: unsupported dictionary index type %s", idx.DataType())
	}
	return out, nil
}
