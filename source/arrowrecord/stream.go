package arrowrecord

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/internal/batchview"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type batchStream struct {
	owner                       *recordOwner
	bindings                    []binding
	parts                       []source.Partition
	opts                        source.ReadOptions
	next, offset                int
	batch                       *memory.Batch
	view                        batchview.View
	closed, exhausted, released bool
	terminalErr                 error
}

func (s *batchStream) release() {
	s.view.Release()
	s.batch.ClearColumns()
	if !s.released {
		s.owner.release()
		s.released = true
	}
}

// Close releases the stream views and owned resources exactly once.
func (s *batchStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.release()
	return nil
}

func (s *batchStream) fail(err error) (bool, error) {
	s.release()
	s.terminalErr = err
	return false, err
}

// Next binds the next bounded view or returns the terminal stream result.
func (s *batchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("arrowrecord: nil destination")
	}
	dst.ClearColumns()
	s.view.Release()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, nil
	}
	if ctx == nil {
		return s.fail(fmt.Errorf("arrowrecord: nil context"))
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if s.next == len(s.parts) {
		s.exhausted = true
		s.release()
		return false, nil
	}
	if s.offset == 0 {
		if err := s.bindRecord(s.parts[s.next].ID); err != nil {
			return s.fail(err)
		}
	}
	rows, err := s.view.Bind(ctx, dst, s.batch, s.offset, s.opts)
	if err != nil {
		return s.fail(err)
	}
	s.offset += rows
	if s.offset == s.batch.Rows {
		s.next++
		s.offset = 0
	}
	return true, nil
}

// bindRecord constructs only vector headers over retained source buffers.
// Row slicing, bounded Boolean decoding, and validity/offset normalization
// happen in Next. Packed Boolean bindings stay internal to batchview; callers
// receive native uint32 vectors rather than opaque arrays.
func (s *batchStream) bindRecord(partition int) error {
	rec := s.owner.records[partition]
	rows := int(rec.NumRows())
	dst := s.batch
	dst.ClearColumns()
	for _, b := range s.bindings {
		arr := rec.Column(b.index)
		valid := cachedValidity(b, partition)
		bound, err := bindTyped(dst, b, arr, partition, valid)
		if err != nil {
			return err
		}
		if !bound {
			dst.Opaque[b.name] = &memory.OpaqueVector{Rows: rows, Type: b.field.Type, ArrowType: b.field.ArrowType, Encoding: b.field.Encoding, Validity: valid, Value: arr}
		}
	}
	dst.Reset(rows)
	return nil
}

var _ source.Source = (*Source)(nil)
var _ source.PreparedBatchStreamProvider = (*Source)(nil)

func bindTyped(dst *memory.Batch, b binding, arr arrow.Array, partition int, validity memory.Bitmap) (bool, error) {
	switch a := arr.(type) {
	case *array.Decimal128:
		if b.field.DecimalPrecision == 0 {
			return false, nil
		}
		// Metadata and valid coefficients were checked at construction. The
		// retained Arrow record is immutable; only vector headers are per-stream.
		dst.Decimals[b.name] = &memory.Decimal128Vector{Values: a.Values(), Validity: validity, Precision: b.field.DecimalPrecision, Scale: b.field.DecimalScale}
	case *array.Int8:
		dst.Int8s[b.name] = &memory.Int8Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Int16:
		dst.Int16s[b.name] = &memory.Int16Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
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
	case *array.Date32, *array.Time32:
		if partition < len(b.i32) && b.i32[partition] != nil {
			dst.Int32s[b.name] = &memory.Int32Vector{Values: b.i32[partition], Validity: validity, Encoding: b.field.Encoding}
		} else {
			return false, nil
		}
	case *array.Uint8:
		dst.Uint8s[b.name] = &memory.Uint8Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Uint16:
		dst.Uint16s[b.name] = &memory.Uint16Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
	case *array.Float32:
		dst.Float32s[b.name] = &memory.Float32Vector{Values: a.Values(), Validity: validity, Encoding: b.field.Encoding}
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
		if memory.IsNestedList(b.field) {
			if partition < 0 || partition >= len(b.arrays) || b.arrays[partition] == nil {
				return false, fmt.Errorf("missing native array node")
			}
			dst.Arrays[b.name] = b.arrays[partition]
			return true, nil
		}
		if memory.IsStructList(b.field) {
			if partition < 0 || partition >= len(b.structLists) || b.structLists[partition] == nil {
				return false, fmt.Errorf("missing native StructList")
			}
			dst.StructLists[b.name] = b.structLists[partition]
			return true, nil
		}
		if b.field.Type == memory.TypeList && len(b.field.Children) == 1 && b.field.Children[0].Type == memory.TypeInt64 {
			if partition < 0 || partition >= len(b.int64Lists) || b.int64Lists[partition] == nil {
				return false, nil
			}
			dst.Int64Lists[b.name] = b.int64Lists[partition]
			return true, nil
		}
		if b.field.Type != memory.TypeList || partition < 0 || partition >= len(b.stringLists) || b.stringLists[partition] == nil {
			return false, nil
		}
		dst.StringLists[b.name] = b.stringLists[partition]
	case *array.Struct:
		if b.field.Type != memory.TypeStruct || partition < 0 || partition >= len(b.structs) || b.structs[partition] == nil {
			return false, nil
		}
		dst.Structs[b.name] = b.structs[partition]
	default:
		return false, nil
	}
	return true, nil
}

func arrowStructVector(a *array.Struct, field memory.Field, parentValidity memory.Bitmap) (*memory.StructVector, error) {
	return arrowStructVectorOption(a, field, parentValidity, false)
}

func arrowStructVectorOption(a *array.Struct, field memory.Field, parentValidity memory.Bitmap, recursive bool) (*memory.StructVector, error) {
	if recursive {
		if err := field.ValidateRecursiveStruct(); err != nil {
			return nil, err
		}
		v, err := arrowRecursiveStructVector(a, field, parentValidity, nil)
		if err != nil {
			return nil, err
		}
		if err = v.ValidateRecursiveWithContext(field, a.Len(), nil); err != nil {
			return nil, err
		}
		return v, nil
	}
	if err := field.ValidateStruct(); err != nil {
		return nil, err
	}
	if len(field.Children) != a.NumField() {
		return nil, fmt.Errorf("struct child count %d does not match schema %d", a.NumField(), len(field.Children))
	}
	values := memory.NewBatch()
	values.Rows = a.Len()
	for i, child := range field.Children {
		arr := a.Field(i)
		if nilArrayValue(arr) {
			return nil, fmt.Errorf("nil native Struct child %q", child.Name)
		}
		validity := validityBitmap(arr)
		if child.Type == memory.TypeBool {
			bools, ok := arr.(*array.Boolean)
			if !ok {
				return nil, fmt.Errorf("struct child %q is %T, want boolean", child.Name, arr)
			}
			out := make([]uint32, bools.Len())
			for row := range out {
				if bools.Value(row) {
					out[row] = 1
				}
			}
			values.Uint32s[child.Name] = &memory.Uint32Vector{Values: out, Validity: validity, Encoding: child.Encoding}
			continue
		}
		b := binding{
			name: child.Name, field: child,
			validities: []memory.Bitmap{validity}, offsets: make([][]uint32, 1),
			i32: make([][]int32, 1), i64: make([][]int64, 1), u32: make([][]uint32, 1),
			int64Lists: make([]*memory.Int64ListVector, 1),
			f64:        make([][]float64, 1), stringLists: make([]*memory.StringListVector, 1),
		}
		if err := cachePhysicalValues(&b, 0, arr, parentValidity); err != nil {
			return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
		}
		switch x := arr.(type) {
		case *array.String:
			b.offsets[0] = offsets32(x.ValueOffsets())
		case *array.List:
			if child.Children[0].Type == memory.TypeInt64 {
				list, err := arrowInt64ListVector(x, validity)
				if err != nil {
					return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
				}
				b.int64Lists[0] = list
				break
			}
			list, err := arrowStringListVector(x, validity)
			if err != nil {
				return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
			}
			b.stringLists[0] = list
		}
		bound, err := bindTyped(values, b, arr, 0, validity)
		if err != nil {
			return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
		}
		if !bound {
			return nil, fmt.Errorf("struct child %q Arrow type %s is unsupported", child.Name, arr.DataType())
		}
	}
	v := &memory.StructVector{Rows: a.Len(), Fields: append([]memory.Field(nil), field.Children...), Values: values, Validity: parentValidity, Encoding: memory.EncodingView}
	if err := v.Validate(field, a.Len()); err != nil {
		return nil, err
	}
	return v, nil
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

func arrowRecursiveStructVector(a *array.Struct, field memory.Field, parentValidity, ancestor memory.Bitmap) (*memory.StructVector, error) {
	if len(field.Children) != a.NumField() {
		return nil, fmt.Errorf("struct child count %d does not match schema %d", a.NumField(), len(field.Children))
	}
	effective := parentValidity
	if ancestor != nil {
		effective = make(memory.Bitmap, memory.WordsFor(a.Len()))
		for row := 0; row < a.Len(); row++ {
			if memory.IsSet(ancestor, row) && (parentValidity == nil || memory.IsSet(parentValidity, row)) {
				memory.Set(effective, row)
			}
		}
	}
	values := memory.NewBatch()
	values.Rows = a.Len()
	for i, child := range field.Children {
		arr := a.Field(i)
		validity := validityBitmap(arr)
		if memory.IsNestedList(child) {
			v, err := arrowArrayNodeDomain(arr, child, 0, effective)
			if err != nil {
				return nil, err
			}
			values.Arrays[child.Name] = v
			continue
		}
		if memory.IsStructList(child) {
			x, ok := arr.(*array.List)
			if !ok || x == nil {
				return nil, fmt.Errorf("invalid StructList array")
			}
			v, err := arrowStructListVector(x, child, validity, effective)
			if err != nil {
				return nil, err
			}
			values.StructLists[child.Name] = v
			continue
		}
		if child.Type == memory.TypeStruct {
			x, ok := arr.(*array.Struct)
			if !ok || x == nil {
				return nil, fmt.Errorf("invalid recursive Struct child %q", child.Name)
			}
			v, err := arrowRecursiveStructVector(x, child, validity, effective)
			if err != nil {
				return nil, err
			}
			values.Structs[child.Name] = v
			continue
		}
		if d, ok := arr.(*array.Dictionary); ok && child.Type == memory.TypeString {
			table, ok := d.Dictionary().(*array.String)
			if !ok || table == nil {
				return nil, fmt.Errorf("recursive Struct dictionary requires String values")
			}
			raw := memory.StringViewVector{Offsets: offsets32(table.ValueOffsets()), Data: table.ValueBytes()}
			if err := raw.Validate(table.Len()); err != nil {
				return nil, err
			}
			// NULL dictionary values are valid Arrow, but this explicit native
			// subset does not yet merge table validity with index validity.
			if table.NullN() > 0 {
				return nil, fmt.Errorf("recursive Struct String dictionary with NULL table entries is unsupported")
			}
			if d.Indices().Len() != a.Len() {
				return nil, fmt.Errorf("recursive dictionary index length mismatch")
			}
			out := &memory.StringViewVector{Offsets: make([]uint32, a.Len()+1), Validity: validity, Encoding: memory.EncodingView}
			for row := 0; row < a.Len(); row++ {
				if (effective == nil || memory.IsSet(effective, row)) && (validity == nil || memory.IsSet(validity, row)) {
					code := d.GetValueIndex(row)
					if code < 0 || code >= table.Len() {
						return nil, fmt.Errorf("recursive dictionary code out of range")
					}
					data := raw.ValueBytes(code)
					if uint64(len(out.Data))+uint64(len(data)) > uint64(^uint32(0)) {
						return nil, fmt.Errorf("recursive dictionary expansion exceeds uint32")
					}
					out.Data = append(out.Data, data...)
				}
				out.Offsets[row+1] = uint32(len(out.Data))
			}
			values.Strings[child.Name] = out
			continue
		}
		if child.Type == memory.TypeBool {
			bools, ok := arr.(*array.Boolean)
			if !ok {
				return nil, fmt.Errorf("struct child %q is %T, want boolean", child.Name, arr)
			}
			out := make([]uint32, bools.Len())
			for row := range out {
				if bools.Value(row) {
					out[row] = 1
				}
			}
			values.Uint32s[child.Name] = &memory.Uint32Vector{Values: out, Validity: validity, Encoding: child.Encoding}
			continue
		}
		b := binding{
			name: child.Name, field: child,
			validities: []memory.Bitmap{validity}, offsets: make([][]uint32, 1),
			i32: make([][]int32, 1), i64: make([][]int64, 1), u32: make([][]uint32, 1),
			int64Lists: make([]*memory.Int64ListVector, 1),
			f64:        make([][]float64, 1), stringLists: make([]*memory.StringListVector, 1),
		}
		if err := cachePhysicalValues(&b, 0, arr, effective); err != nil {
			return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
		}
		switch x := arr.(type) {
		case *array.String:
			b.offsets[0] = offsets32(x.ValueOffsets())
		case *array.List:
			if child.Children[0].Type == memory.TypeInt64 {
				list, err := arrowInt64ListVector(x, validity)
				if err != nil {
					return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
				}
				b.int64Lists[0] = list
				break
			}
			list, err := arrowStringListVector(x, validity)
			if err != nil {
				return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
			}
			b.stringLists[0] = list
		}
		bound, err := bindTyped(values, b, arr, 0, validity)
		if err != nil {
			return nil, fmt.Errorf("struct child %q: %w", child.Name, err)
		}
		if !bound {
			return nil, fmt.Errorf("struct child %q Arrow type %s is unsupported", child.Name, arr.DataType())
		}
	}
	v := &memory.StructVector{Rows: a.Len(), Fields: append([]memory.Field(nil), field.Children...), Values: values, Validity: parentValidity, Encoding: memory.EncodingView}
	return v, nil
}
