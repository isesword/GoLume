package physical

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func compactSelected(ctx context.Context, dst, src *memory.Batch, rows []int, selection memory.Bitmap, kernels kernel.Set) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n := len(rows)
	for name, v := range src.Int64s {
		out := ensureI64(dst.Int64s[name], n)
		if got := kernels.CompactInt64(v.Values, selection, out.Values); got != n {
			return fmt.Errorf("compact int64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Int64s[name] = out
	}
	for name, v := range src.Int32s {
		out := ensureI32(dst.Int32s[name], n)
		if got := kernels.CompactInt32(v.Values, selection, out.Values); got != n {
			return fmt.Errorf("compact int32 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Int32s[name] = out
	}
	for name, v := range src.Uint32s {
		out := ensureU32(dst.Uint32s[name], n)
		if got := kernels.CompactUint32(v.Values, selection, out.Values); got != n {
			return fmt.Errorf("compact uint32 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Uint32s[name] = out
	}
	for name, v := range src.Uint64s {
		out := ensureU64(dst.Uint64s[name], n)
		if got := kernels.CompactUint64(v.Values, selection, out.Values); got != n {
			return fmt.Errorf("compact uint64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Uint64s[name] = out
	}
	for name, v := range src.Float64s {
		out := ensureF64(dst.Float64s[name], n)
		if got := kernels.CompactFloat64(v.Values, selection, out.Values); got != n {
			return fmt.Errorf("compact float64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Float64s[name] = out
	}
	for name, v := range src.DictStrings {
		out := dst.DictStrings[name]
		if out == nil {
			out = &memory.DictionaryStringVector{}
		}
		out.Indices = ensureU32Slice(out.Indices, n)
		if got := kernels.CompactUint32(v.Indices, selection, out.Indices); got != n {
			return fmt.Errorf("compact dictionary column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Offsets, out.Data, out.DictionaryDomain, out.Encoding = v.Offsets, v.Data, v.DictionaryDomain, v.Encoding
		dst.DictStrings[name] = out
	}
	for name, v := range src.Strings {
		out := dst.Strings[name]
		if out == nil {
			out = &memory.StringViewVector{}
		}
		out.Offsets = ensureU32Slice(out.Offsets, n+1)
		out.Offsets[0] = 0
		data := out.Data[:0]
		for i, row := range rows {
			data = append(data, v.ValueBytes(row)...)
			out.Offsets[i+1] = uint32(len(data))
		}
		out.Data = data
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = memory.EncodingView
		dst.Strings[name] = out
	}
	for name, v := range src.StringLists {
		out := resetStringList(dst.StringLists[name])
		for _, row := range rows {
			if err := memory.AppendStringListRow(out, v, row); err != nil {
				return fmt.Errorf("compact list<string> column %q: %w", name, err)
			}
		}
		dst.StringLists[name] = out
	}
	for name, v := range src.Opaque {
		tmpSrc := memory.NewBatch()
		tmpSrc.Opaque[name] = v
		tmpSrc.Reset(src.Rows)
		tmpDst := memory.NewBatch()
		if err := takeRows(ctx, tmpDst, tmpSrc, rows); err != nil {
			return err
		}
		dst.Opaque[name] = tmpDst.Opaque[name]
		delete(tmpDst.Opaque, name)
	}
	dst.Reset(n)
	return nil
}

func resetStringList(v *memory.StringListVector) *memory.StringListVector {
	if v == nil {
		return &memory.StringListVector{Offsets: []uint32{0}, Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
	}
	if cap(v.Offsets) == 0 {
		v.Offsets = []uint32{0}
	} else {
		v.Offsets = v.Offsets[:1]
		v.Offsets[0] = 0
	}
	v.Validity = nil
	if v.Values == nil {
		v.Values = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	} else {
		if cap(v.Values.Offsets) == 0 {
			v.Values.Offsets = []uint32{0}
		} else {
			v.Values.Offsets = v.Values.Offsets[:1]
			v.Values.Offsets[0] = 0
		}
		v.Values.Data = v.Values.Data[:0]
		v.Values.Validity = nil
		v.Values.Encoding = memory.EncodingView
	}
	v.Encoding = memory.EncodingView
	return v
}

func compactValidity(dst, src memory.Bitmap, rows []int) memory.Bitmap {
	if src == nil {
		return nil
	}
	dst = memory.EnsureBitmap(dst, len(rows))
	for i, row := range rows {
		if memory.IsSet(src, row) {
			memory.Set(dst, i)
		}
	}
	return dst
}

func ensureI64(v *memory.Int64Vector, n int) *memory.Int64Vector {
	if v == nil {
		v = &memory.Int64Vector{}
	}
	v.Values = ensureI64Slice(v.Values, n)
	return v
}
func ensureI32(v *memory.Int32Vector, n int) *memory.Int32Vector {
	if v == nil {
		v = &memory.Int32Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]int32, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}
func ensureU32(v *memory.Uint32Vector, n int) *memory.Uint32Vector {
	if v == nil {
		v = &memory.Uint32Vector{}
	}
	v.Values = ensureU32Slice(v.Values, n)
	return v
}
func ensureU64(v *memory.Uint64Vector, n int) *memory.Uint64Vector {
	if v == nil {
		v = &memory.Uint64Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]uint64, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}
func ensureF64(v *memory.Float64Vector, n int) *memory.Float64Vector {
	if v == nil {
		v = &memory.Float64Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]float64, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}
func ensureI64Slice(v []int64, n int) []int64 {
	if cap(v) < n {
		return make([]int64, n)
	}
	return v[:n]
}
func ensureU32Slice(v []uint32, n int) []uint32 {
	if cap(v) < n {
		return make([]uint32, n)
	}
	return v[:n]
}
