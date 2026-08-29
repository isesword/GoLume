package physical

import (
	"context"
	"fmt"
	gomemory "gocolumn/memory"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// takeRows gathers rows from src into dst. Specialized vectors use direct
// typed gathers; opaque Arrow vectors delegate to Arrow-Go's generic Take
// kernel so nested, extension, decimal, and other source-native arrays can be
// filtered, limited, sorted, or materialized without engine-specific kernels.
func takeRows(ctx context.Context, dst, src *gomemory.Batch, rows []int) error {
	if dst == nil || src == nil {
		return fmt.Errorf("physical take requires non-nil source and destination batches")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dst.ClearColumns()
	n := len(rows)
	for name, v := range src.Int64s {
		out := ensureI64(nil, n)
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Int64s[name] = out
	}
	for name, v := range src.Int32s {
		out := ensureI32(nil, n)
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Int32s[name] = out
	}
	for name, v := range src.Uint32s {
		out := ensureU32(nil, n)
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Uint32s[name] = out
	}
	for name, v := range src.Uint64s {
		out := ensureU64(nil, n)
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Uint64s[name] = out
	}
	for name, v := range src.Float64s {
		out := ensureF64(nil, n)
		for i, row := range rows {
			out.Values[i] = v.Values[row]
		}
		out.Validity = compactValidity(out.Validity, v.Validity, rows)
		out.Encoding = v.Encoding
		dst.Float64s[name] = out
	}
	for name, v := range src.DictStrings {
		out := &gomemory.DictionaryStringVector{
			Indices:          make([]uint32, n),
			Validity:         compactValidity(nil, v.Validity, rows),
			Offsets:          v.Offsets,
			Data:             v.Data,
			DictionaryDomain: v.DictionaryDomain,
			Encoding:         v.Encoding,
		}
		for i, row := range rows {
			out.Indices[i] = v.Indices[row]
		}
		dst.DictStrings[name] = out
	}
	for name, v := range src.Strings {
		out := &gomemory.StringViewVector{Offsets: make([]uint32, n+1), Validity: compactValidity(nil, v.Validity, rows), Encoding: gomemory.EncodingView}
		for i, row := range rows {
			out.Data = append(out.Data, v.ValueBytes(row)...)
			out.Offsets[i+1] = uint32(len(out.Data))
		}
		dst.Strings[name] = out
	}
	for name, v := range src.StringLists {
		out := &gomemory.StringListVector{Offsets: []uint32{0}, Values: &gomemory.StringViewVector{Offsets: []uint32{0}, Encoding: gomemory.EncodingView}, Encoding: gomemory.EncodingView}
		for _, row := range rows {
			if err := gomemory.AppendStringListRow(out, v, row); err != nil {
				return fmt.Errorf("take list<string> column %q: %w", name, err)
			}
		}
		dst.StringLists[name] = out
	}
	for name, v := range src.Opaque {
		arr, ok := v.Value.(arrow.Array)
		if !ok {
			return fmt.Errorf("opaque column %q is not an Arrow array", name)
		}
		taken, err := takeArrowRows(arr, rows)
		if err != nil {
			return fmt.Errorf("take opaque column %q: %w", name, err)
		}
		dst.Opaque[name] = &gomemory.OpaqueVector{Rows: n, Type: v.Type, ArrowType: v.ArrowType, Encoding: v.Encoding, Validity: validityFromArrow(taken), Value: taken, Owned: true}
	}
	dst.Reset(n)
	return nil
}

func takeArrowRows(arr arrow.Array, rows []int) (arrow.Array, error) {
	if arr == nil {
		return nil, fmt.Errorf("nil Arrow array")
	}
	if len(rows) == 0 {
		return array.NewSlice(arr, 0, 0), nil
	}
	slices := make([]arrow.Array, 0, len(rows))
	start := -1
	prev := -2
	flush := func(end int) {
		if start >= 0 {
			slices = append(slices, array.NewSlice(arr, int64(start), int64(end)))
		}
	}
	for _, row := range rows {
		if row < 0 || row >= arr.Len() {
			for _, s := range slices {
				s.Release()
			}
			return nil, fmt.Errorf("row %d out of range [0,%d)", row, arr.Len())
		}
		if start < 0 {
			start, prev = row, row
			continue
		}
		if row == prev+1 {
			prev = row
			continue
		}
		flush(prev + 1)
		start, prev = row, row
	}
	flush(prev + 1)
	if len(slices) == 1 {
		return slices[0], nil
	}
	out, err := array.Concatenate(slices, arrowmem.DefaultAllocator)
	for _, s := range slices {
		s.Release()
	}
	return out, err
}

func validityFromArrow(arr arrow.Array) gomemory.Bitmap {
	if arr == nil || arr.NullN() == 0 {
		return nil
	}
	out := gomemory.EnsureBitmap(nil, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			gomemory.Set(out, i)
		}
	}
	return out
}
