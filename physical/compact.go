package physical

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/kernel"
	"gocolumn/memory"
)

func compactSelected(ctx context.Context, dst, src *memory.Batch, rows []int, selection memory.Bitmap, kernels kernel.Set) error {
	return compactSelectedParents(ctx, dst, src, rows, selection, kernels, nil)
}

func compactSelectedParents(ctx context.Context, dst, src *memory.Batch, rows []int, selection memory.Bitmap, kernels kernel.Set, parents *compactParent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n := len(rows)
	for name, v := range src.Int8s {
		out := ensureI8(dst.Int8s[name], n)
		if got := kernels.CompactInt8(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact int8 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Int8s[name] = out
	}
	for name, v := range src.Int16s {
		out := ensureI16(dst.Int16s[name], n)
		if got := kernels.CompactInt16(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact int16 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Int16s[name] = out
	}
	for name, v := range src.Uint8s {
		out := ensureU8(dst.Uint8s[name], n)
		if got := kernels.CompactUint8(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact uint8 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Uint8s[name] = out
	}
	for name, v := range src.Uint16s {
		out := ensureU16(dst.Uint16s[name], n)
		if got := kernels.CompactUint16(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact uint16 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Uint16s[name] = out
	}
	for name, v := range src.Float32s {
		out := ensureF32(dst.Float32s[name], n)
		if got := kernels.CompactFloat32(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact float32 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Float32s[name] = out
	}
	for name, v := range src.Int64s {
		out := ensureI64(dst.Int64s[name], n)
		if got := kernels.CompactInt64(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact int64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Int64s[name] = out
	}
	for name, v := range src.Int32s {
		out := ensureI32(dst.Int32s[name], n)
		if got := kernels.CompactInt32(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact int32 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Int32s[name] = out
	}
	for name, v := range src.Uint32s {
		out := ensureU32(dst.Uint32s[name], n)
		if got := kernels.CompactUint32(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact uint32 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Uint32s[name] = out
	}
	for name, v := range src.Uint64s {
		out := ensureU64(dst.Uint64s[name], n)
		if got := kernels.CompactUint64(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact uint64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Uint64s[name] = out
	}
	for name, v := range src.Float64s {
		out := ensureF64(dst.Float64s[name], n)
		if got := kernels.CompactFloat64(v.Values[:src.Rows], selection, out.Values); got != n {
			return fmt.Errorf("compact float64 column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Values[i] = 0
				}
			}
		}
		out.Encoding = v.Encoding
		dst.Float64s[name] = out
	}
	for name, v := range src.DictStrings {
		out := dst.DictStrings[name]
		if out == nil {
			out = &memory.DictionaryStringVector{}
		}
		out.Indices = ensureU32Slice(out.Indices, n)
		if got := kernels.CompactUint32(v.Indices[:src.Rows], selection, out.Indices); got != n {
			return fmt.Errorf("compact dictionary column %q selected %d rows, want %d", name, got, n)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		if parents != nil {
			for i, row := range rows {
				if !rowValid(v.Validity, row) || !parents.valid(row) {
					out.Indices[i] = 0
				}
			}
		}
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
		size, err := compactStringSize(v, rows, parents)
		if err != nil {
			return err
		}
		out.Data = ensureBytes(out.Data, size)
		data := out.Data[:0]
		for i, row := range rows {
			if rowValid(v.Validity, row) && parents.valid(row) {
				data = append(data, v.ValueBytes(row)...)
			}
			out.Offsets[i+1] = uint32(len(data))
		}
		out.Data = data
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		out.Encoding = memory.EncodingView
		dst.Strings[name] = out
	}
	for name, v := range src.StringLists {
		out := dst.StringLists[name]
		if out == nil {
			out = &memory.StringListVector{Values: &memory.StringViewVector{}}
		}
		children, dataBytes, err := compactListSize(v, rows, parents)
		if err != nil {
			return err
		}
		out.Offsets = ensureU32Slice(out.Offsets, n+1)
		out.Values.Offsets = ensureU32Slice(out.Values.Offsets, children+1)
		out.Values.Data = ensureBytes(out.Values.Data, dataBytes)
		out.Offsets[0], out.Values.Offsets[0] = 0, 0
		if v.Values.Validity != nil {
			out.Values.Validity = memory.EnsureBitmap(out.Values.Validity, children)
		} else {
			out.Values.Validity = compactAllValid(out.Values.Validity, children)
		}
		child, position := 0, 0
		for i, row := range rows {
			if rowValid(v.Validity, row) && parents.valid(row) {
				for j := int(v.Offsets[row]); j < int(v.Offsets[row+1]); j++ {
					if rowValid(v.Values.Validity, j) {
						value := v.Values.ValueBytes(j)
						position += copy(out.Values.Data[position:], value)
					}
					out.Values.Offsets[child+1] = uint32(position)
					if v.Values.Validity != nil && memory.IsSet(v.Values.Validity, j) {
						memory.Set(out.Values.Validity, child)
					}
					child++
				}
			}
			out.Offsets[i+1] = uint32(child)
		}
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		out.Encoding, out.Values.Encoding = memory.EncodingView, memory.EncodingView
		dst.StringLists[name] = out
	}
	for name, v := range src.Structs {
		out := dst.Structs[name]
		if out == nil {
			out = &memory.StructVector{Values: memory.NewBatch()}
		}
		childSource := *v.Values
		childSource.Rows = src.Rows
		childParents := &compactParent{validity: v.Validity, parent: parents}
		if err := compactSelectedParents(ctx, out.Values, &childSource, rows, selection, kernels, childParents); err != nil {
			return err
		}
		out.Rows, out.Fields, out.Encoding = n, v.Fields, v.Encoding
		out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
		dst.Structs[name] = out
	}
	for name, v := range src.Opaque {
		if contiguousRows(rows) {
			arr, ok := v.Value.(arrow.Array)
			if !ok {
				return fmt.Errorf("opaque column %q is not an Arrow array", name)
			}
			start := 0
			if n > 0 {
				start = rows[0]
			}
			if start+n > arr.Len() {
				return fmt.Errorf("opaque column %q is shorter than selection", name)
			}
			out := dst.Opaque[name]
			if out == nil {
				out = &memory.OpaqueVector{}
			}
			// The new slice retains input payload without copying it. Its lifetime
			// ends before child Next, while source-owned payload is still charged.
			next := array.NewSlice(arr, int64(start), int64(start+n))
			if out.Owned {
				if old, ok := out.Value.(interface{ Release() }); ok {
					old.Release()
				}
			}
			out.Rows, out.Type, out.ArrowType, out.Encoding = n, v.Type, v.ArrowType, v.Encoding
			out.Validity = compactValidityParents(out.Validity, v.Validity, rows, parents)
			out.Value, out.Owned = next, true
			dst.Opaque[name] = out
			continue
		}
		tmpSrc := memory.NewBatch()
		tmpSrc.Opaque[name] = v
		tmpSrc.Rows = src.Rows
		tmpDst := memory.NewBatch()
		if err := takeRows(ctx, tmpDst, tmpSrc, rows); err != nil {
			tmpDst.ClearColumns()
			return err
		}
		if previous := dst.Opaque[name]; previous != nil && previous.Owned {
			if value, ok := previous.Value.(interface{ Release() }); ok {
				value.Release()
			}
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
		return compactAllValid(dst, len(rows))
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

func ensureI8(v *memory.Int8Vector, n int) *memory.Int8Vector {
	if v == nil {
		v = &memory.Int8Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]int8, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}

func ensureI16(v *memory.Int16Vector, n int) *memory.Int16Vector {
	if v == nil {
		v = &memory.Int16Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]int16, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}

func ensureU8(v *memory.Uint8Vector, n int) *memory.Uint8Vector {
	if v == nil {
		v = &memory.Uint8Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]uint8, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}

func ensureU16(v *memory.Uint16Vector, n int) *memory.Uint16Vector {
	if v == nil {
		v = &memory.Uint16Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]uint16, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}

func ensureF32(v *memory.Float32Vector, n int) *memory.Float32Vector {
	if v == nil {
		v = &memory.Float32Vector{}
	}
	if cap(v.Values) < n {
		v.Values = make([]float32, n)
	} else {
		v.Values = v.Values[:n]
	}
	return v
}

func compactAllValid(dst memory.Bitmap, n int) memory.Bitmap {
	if cap(dst) == 0 {
		return nil
	}
	dst = memory.EnsureBitmap(dst, n)
	for i := range dst {
		dst[i] = ^uint64(0)
	}
	if len(dst) > 0 {
		dst[len(dst)-1] &= memory.TailMask(n)
	}
	return dst
}

func contiguousRows(rows []int) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i] != rows[0]+i {
			return false
		}
	}
	return true
}

// compactParent tracks input-row validity through recursive struct ancestry.
// It is a stack-local sizing/copy context, not a retained payload allocation.
type compactParent struct {
	validity memory.Bitmap
	parent   *compactParent
}

func (p *compactParent) valid(row int) bool {
	for p != nil {
		if !rowValid(p.validity, row) {
			return false
		}
		p = p.parent
	}
	return true
}
func rowValid(v memory.Bitmap, row int) bool { return v == nil || memory.IsSet(v, row) }
func compactValidityParents(dst, src memory.Bitmap, rows []int, parents *compactParent) memory.Bitmap {
	if parents == nil {
		return compactValidity(dst, src, rows)
	}
	need := src != nil || cap(dst) > 0
	if !need {
		for _, row := range rows {
			if !parents.valid(row) {
				need = true
				break
			}
		}
	}
	if !need {
		return nil
	}
	dst = memory.EnsureBitmap(dst, len(rows))
	for i, row := range rows {
		if rowValid(src, row) && parents.valid(row) {
			memory.Set(dst, i)
		}
	}
	return dst
}
