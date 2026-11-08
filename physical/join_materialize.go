package physical

import (
	"fmt"
	"gocolumn/join"
	"gocolumn/memory"
	"math"
)

func addJoinEstimate(total *int64, value int64) error {
	if value < 0 || *total > math.MaxInt64-value {
		return fmt.Errorf("join materialization byte estimate overflows int64")
	}
	*total += value
	return nil
}

func joinValidityEstimate(rows int) int64 {
	return int64(memory.WordsFor(rows)) * 8
}

func estimateJoinListRows(s *JoinSource, left *memory.Batch, binding joinOutputBinding, pairs []join.Pair, child bool) (int64, error) {
	var children, data int64
	for _, pair := range pairs {
		src, row, ok := joinSourceRow(s, left, binding, pair)
		if !ok {
			continue
		}
		var vector *memory.StringListVector
		if child {
			parent, err := src.Struct(binding.in)
			if err != nil {
				return 0, err
			}
			if !parent.IsValid(row) {
				continue
			}
			vector = parent.Values.StringLists[binding.field.Name]
		} else {
			var err error
			vector, err = src.StringList(binding.in)
			if err != nil {
				return 0, err
			}
		}
		if vector == nil || !vector.IsValid(row) {
			continue
		}
		start, end, ok := vector.ValueRange(row)
		if !ok {
			return 0, fmt.Errorf("join list %q row %d has malformed offsets", binding.in, row)
		}
		children += int64(end - start)
		for element := start; element < end; element++ {
			if vector.Values.IsValid(element) {
				data += int64(len(vector.Values.ValueBytes(element)))
			}
		}
	}
	total := int64(len(pairs)+1)*4 + joinValidityEstimate(len(pairs))
	if err := addJoinEstimate(&total, (children+1)*4+joinValidityEstimate(int(children))+data); err != nil {
		return 0, err
	}
	return total, nil
}

func estimateJoinStructBytes(s *JoinSource, left *memory.Batch, binding joinOutputBinding, pairs []join.Pair) (int64, error) {
	rows := len(pairs)
	total := joinValidityEstimate(rows)
	for _, childField := range binding.field.Children {
		if err := addJoinEstimate(&total, joinValidityEstimate(rows)); err != nil {
			return 0, err
		}
		switch childField.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
			memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
			if err := addJoinEstimate(&total, int64(rows)*4); err != nil {
				return 0, err
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
			memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
			if err := addJoinEstimate(&total, int64(rows)*8); err != nil {
				return 0, err
			}
		case memory.TypeString:
			if err := addJoinEstimate(&total, int64(rows+1)*4); err != nil {
				return 0, err
			}
			for _, pair := range pairs {
				src, row, ok := joinSourceRow(s, left, binding, pair)
				if !ok {
					continue
				}
				parent, err := src.Struct(binding.in)
				if err != nil {
					return 0, err
				}
				vector := parent.Values.Strings[childField.Name]
				if parent.IsValid(row) && vector != nil && vector.IsValid(row) {
					if err := addJoinEstimate(&total, int64(len(vector.ValueBytes(row)))); err != nil {
						return 0, err
					}
				}
			}
		case memory.TypeList:
			childBinding := binding
			childBinding.field = childField
			bytes, err := estimateJoinListRows(s, left, childBinding, pairs, true)
			if err != nil {
				return 0, err
			}
			// estimateJoinListRows already includes its child validity; the
			// generic child validity added above is the list parent validity.
			bytes -= joinValidityEstimate(rows)
			if err := addJoinEstimate(&total, bytes); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("join struct child %q type %s has no byte estimator", childField.Name, childField.Type)
		}
	}
	return total, nil
}

// estimateJoinMaterializationBytes returns a conservative reservation made
// before materializeJoinBatch allocates. The factor of two covers Go's append
// growth for variable-width List/Struct buffers; fixed vectors are exact.
func estimateJoinMaterializationBytes(s *JoinSource, left *memory.Batch, pairs []join.Pair) (int64, error) {
	rows := len(pairs)
	var total int64
	for _, binding := range s.bindings {
		if binding.field.Nullable {
			if err := addJoinEstimate(&total, joinValidityEstimate(rows)); err != nil {
				return 0, err
			}
		}
		switch binding.field.Type {
		case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32,
			memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
			if err := addJoinEstimate(&total, int64(rows)*4); err != nil {
				return 0, err
			}
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration,
			memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64:
			if err := addJoinEstimate(&total, int64(rows)*8); err != nil {
				return 0, err
			}
		case memory.TypeString:
			if binding.field.Encoding == memory.EncodingDictionary {
				if err := addJoinEstimate(&total, int64(rows)*4+int64(len(binding.dict.Offsets))*4+int64(len(binding.dict.Data))); err != nil {
					return 0, err
				}
				continue
			}
			if err := addJoinEstimate(&total, int64(rows+1)*4); err != nil {
				return 0, err
			}
			for _, pair := range pairs {
				value, valid, err := joinStringValue(s, left, binding, pair)
				if err != nil {
					return 0, err
				}
				if valid {
					if err := addJoinEstimate(&total, int64(len(value))); err != nil {
						return 0, err
					}
				}
			}
		case memory.TypeBinary:
			if err := addJoinEstimate(&total, int64(rows+1)*4); err != nil {
				return 0, err
			}
			for _, pair := range pairs {
				value, valid, err := joinStringValue(s, left, binding, pair)
				if err != nil {
					return 0, err
				}
				if valid {
					if err := addJoinEstimate(&total, int64(len(value))); err != nil {
						return 0, err
					}
				}
			}
		case memory.TypeList:
			bytes, err := estimateJoinListRows(s, left, binding, pairs, false)
			if err != nil {
				return 0, err
			}
			if err := addJoinEstimate(&total, bytes); err != nil {
				return 0, err
			}
		case memory.TypeStruct:
			bytes, err := estimateJoinStructBytes(s, left, binding, pairs)
			if err != nil {
				return 0, err
			}
			if err := addJoinEstimate(&total, bytes); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("joined payload type %s for %q has no byte estimator", binding.field.Type, binding.out)
		}
	}
	if total > (math.MaxInt64-1024)/2 {
		return 0, fmt.Errorf("join materialization byte estimate overflows int64")
	}
	return total*2 + 1024, nil
}

func estimateBatchBytes(b *memory.Batch) int64 {
	return memory.BatchBytes(b)
}

func cloneBatch(src *memory.Batch) (*memory.Batch, error) {
	if len(src.Opaque) != 0 {
		return nil, fmt.Errorf("cannot retain opaque payload in Phase 2.6 join build")
	}
	dst := memory.NewBatch()
	dst.Reset(src.Rows)
	for k, v := range src.Int8s {
		dst.Int8s[k] = &memory.Int8Vector{Values: append([]int8(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Int16s {
		dst.Int16s[k] = &memory.Int16Vector{Values: append([]int16(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Uint8s {
		dst.Uint8s[k] = &memory.Uint8Vector{Values: append([]uint8(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Uint16s {
		dst.Uint16s[k] = &memory.Uint16Vector{Values: append([]uint16(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = &memory.Float32Vector{Values: append([]float32(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
	}
	for k, v := range src.Int64s {
		x := &memory.Int64Vector{Values: append([]int64(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Int64s[k] = x
	}
	for k, v := range src.Int32s {
		x := &memory.Int32Vector{Values: append([]int32(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Int32s[k] = x
	}
	for k, v := range src.Uint32s {
		x := &memory.Uint32Vector{Values: append([]uint32(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Uint32s[k] = x
	}
	for k, v := range src.Uint64s {
		x := &memory.Uint64Vector{Values: append([]uint64(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Uint64s[k] = x
	}
	for k, v := range src.Float64s {
		x := &memory.Float64Vector{Values: append([]float64(nil), v.Values...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Float64s[k] = x
	}
	for k, v := range src.Strings {
		x := &memory.StringViewVector{Offsets: append([]uint32(nil), v.Offsets...), Data: append([]byte(nil), v.Data...), Validity: append(memory.Bitmap(nil), v.Validity...), Encoding: v.Encoding}
		dst.Strings[k] = x
	}
	for k, v := range src.DictStrings {
		x := &memory.DictionaryStringVector{Indices: append([]uint32(nil), v.Indices...), Validity: append(memory.Bitmap(nil), v.Validity...), Offsets: v.Offsets, Data: v.Data, DictionaryDomain: v.DictionaryDomain, Encoding: v.Encoding}
		dst.DictStrings[k] = x
	}
	for k, v := range src.StringLists {
		x := &memory.StringListVector{
			Offsets:  append([]uint32(nil), v.Offsets...),
			Validity: append(memory.Bitmap(nil), v.Validity...),
			Encoding: v.Encoding,
			Values:   &memory.StringViewVector{Offsets: append([]uint32(nil), v.Values.Offsets...), Data: append([]byte(nil), v.Values.Data...), Validity: append(memory.Bitmap(nil), v.Values.Validity...), Encoding: v.Values.Encoding},
		}
		dst.StringLists[k] = x
	}
	for k, v := range src.Structs {
		rows := make([]int, v.Rows)
		for i := range rows {
			rows[i] = i
		}
		x, err := memory.TakeStructRows(v, rows)
		if err != nil {
			return nil, err
		}
		dst.Structs[k] = x
	}
	return dst, nil
}

func materializeJoinBatch(dst *memory.Batch, s *JoinSource, left *memory.Batch, pairs []join.Pair) error {
	n := len(pairs)
	for _, b := range s.bindings {
		switch b.field.Type {
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			out := ensureI64(dst.Int64s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinI64Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Int64s[b.out] = out
		case memory.TypeInt8:
			out := ensureI8(dst.Int8s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinI8Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Int8s[b.out] = out
		case memory.TypeInt16:
			out := ensureI16(dst.Int16s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinI16Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Int16s[b.out] = out
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			out := ensureI32(dst.Int32s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinI32Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Int32s[b.out] = out
		case memory.TypeUint8:
			out := ensureU8(dst.Uint8s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinU8Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Uint8s[b.out] = out
		case memory.TypeUint16:
			out := ensureU16(dst.Uint16s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinU16Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Uint16s[b.out] = out
		case memory.TypeUint32:
			out := ensureU32(dst.Uint32s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinU32Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Uint32s[b.out] = out
		case memory.TypeUint64:
			out := ensureU64(dst.Uint64s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinU64Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Uint64s[b.out] = out
		case memory.TypeFloat32:
			out := ensureF32(dst.Float32s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinF32Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Float32s[b.out] = out
		case memory.TypeFloat64:
			out := ensureF64(dst.Float64s[b.out], n)
			out.Encoding = b.field.Encoding
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinF64Value(s, left, b, p)
				if err != nil {
					return err
				}
				out.Values[i] = v
				if valid && out.Validity != nil {
					memory.Set(out.Validity, i)
				}
			}
			dst.Float64s[b.out] = out
		case memory.TypeString:
			if b.field.Encoding == memory.EncodingDictionary {
				out := dst.DictStrings[b.out]
				if out == nil {
					out = &memory.DictionaryStringVector{}
				}
				out.Indices = ensureU32Slice(out.Indices, n)
				out.Offsets = b.dict.Offsets
				out.Data = b.dict.Data
				out.DictionaryDomain = b.dict.Domain
				out.Encoding = memory.EncodingDictionary
				out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
				for i, p := range pairs {
					v, valid, err := joinDictValue(s, left, b, p)
					if err != nil {
						return err
					}
					out.Indices[i] = v
					if valid && out.Validity != nil {
						memory.Set(out.Validity, i)
					}
				}
				dst.DictStrings[b.out] = out
			} else {
				out := dst.Strings[b.out]
				if out == nil {
					out = &memory.StringViewVector{}
				}
				out.Offsets = ensureU32Slice(out.Offsets, n+1)
				out.Offsets[0] = 0
				out.Data = out.Data[:0]
				out.Encoding = memory.EncodingView
				out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
				for i, p := range pairs {
					v, valid, err := joinStringValue(s, left, b, p)
					if err != nil {
						return err
					}
					if valid {
						if uint64(len(out.Data))+uint64(len(v)) > math.MaxUint32 {
							return fmt.Errorf("join string output exceeds uint32 offset capacity")
						}
						out.Data = append(out.Data, v...)
						if out.Validity != nil {
							memory.Set(out.Validity, i)
						}
					}
					out.Offsets[i+1] = uint32(len(out.Data))
				}
				dst.Strings[b.out] = out
			}
		case memory.TypeBinary:
			out := dst.Strings[b.out]
			if out == nil {
				out = &memory.StringViewVector{}
			}
			out.Offsets = ensureU32Slice(out.Offsets, n+1)
			out.Offsets[0] = 0
			out.Data = out.Data[:0]
			out.Encoding = memory.EncodingView
			out.Validity = joinValidity(out.Validity, b.field.Nullable, n)
			for i, p := range pairs {
				v, valid, err := joinStringValue(s, left, b, p)
				if err != nil {
					return err
				}
				if valid {
					out.Data = append(out.Data, v...)
					if out.Validity != nil {
						memory.Set(out.Validity, i)
					}
				}
				out.Offsets[i+1] = uint32(len(out.Data))
			}
			dst.Strings[b.out] = out
		case memory.TypeList:
			out := resetStringList(dst.StringLists[b.out])
			for _, p := range pairs {
				src, row, ok := joinSourceRow(s, left, b, p)
				if !ok {
					if err := memory.AppendNullStringList(out); err != nil {
						return err
					}
					continue
				}
				v, err := src.StringList(b.in)
				if err != nil {
					return err
				}
				if err := memory.AppendStringListRow(out, v, row); err != nil {
					return err
				}
			}
			dst.StringLists[b.out] = out
		case memory.TypeStruct:
			var out *memory.StructVector
			for _, p := range pairs {
				src, row, ok := joinSourceRow(s, left, b, p)
				if !ok {
					if out == nil {
						out = &memory.StructVector{Fields: append([]memory.Field(nil), b.field.Children...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
					}
					template := &memory.StructVector{Fields: append([]memory.Field(nil), b.field.Children...), Values: memory.NewBatch()}
					for _, child := range b.field.Children {
						switch child.Type {
						case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
							template.Values.Int32s[child.Name] = &memory.Int32Vector{Values: []int32{0}, Validity: memory.EnsureBitmap(nil, 1)}
						case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
							template.Values.Int64s[child.Name] = &memory.Int64Vector{Values: []int64{0}, Validity: memory.EnsureBitmap(nil, 1)}
						case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
							template.Values.Uint32s[child.Name] = &memory.Uint32Vector{Values: []uint32{0}, Validity: memory.EnsureBitmap(nil, 1)}
						case memory.TypeUint64:
							template.Values.Uint64s[child.Name] = &memory.Uint64Vector{Values: []uint64{0}, Validity: memory.EnsureBitmap(nil, 1)}
						case memory.TypeFloat32, memory.TypeFloat64:
							template.Values.Float64s[child.Name] = &memory.Float64Vector{Values: []float64{0}, Validity: memory.EnsureBitmap(nil, 1)}
						case memory.TypeString:
							template.Values.Strings[child.Name] = &memory.StringViewVector{Offsets: []uint32{0, 0}, Validity: memory.EnsureBitmap(nil, 1), Encoding: memory.EncodingView}
						case memory.TypeList:
							template.Values.StringLists[child.Name] = &memory.StringListVector{Offsets: []uint32{0, 0}, Validity: memory.EnsureBitmap(nil, 1), Values: &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
						}
					}
					template.Rows = 1
					template.Values.Rows = 1
					if err := memory.AppendStructRow(out, template, 0, false); err != nil {
						return err
					}
					continue
				}
				v, err := src.Struct(b.in)
				if err != nil {
					return err
				}
				if out == nil {
					out = &memory.StructVector{Fields: append([]memory.Field(nil), v.Fields...), Values: memory.NewBatch(), Encoding: memory.EncodingView}
				}
				if err := memory.AppendStructRow(out, v, row, v.IsValid(row)); err != nil {
					return err
				}
			}
			dst.Structs[b.out] = out
		default:
			return fmt.Errorf("joined payload type %s for %q has no Phase 2.6 materializer", b.field.Type, b.out)
		}
	}
	dst.Reset(n)
	return nil
}

func joinValidity(current memory.Bitmap, nullable bool, rows int) memory.Bitmap {
	if !nullable {
		return nil
	}
	return memory.EnsureBitmap(current, rows)
}

func joinSourceRow(s *JoinSource, left *memory.Batch, b joinOutputBinding, p join.Pair) (*memory.Batch, int, bool) {
	if !b.right {
		if p.ProbeRow < 0 || left == nil {
			return nil, 0, false
		}
		return left, p.ProbeRow, true
	}
	if p.BuildRow < 0 {
		return nil, 0, false
	}
	loc := s.buildLoc[p.BuildRow]
	return s.rightBatches[loc.batch], loc.row, true
}
func joinI64Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (int64, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Int64(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinI32Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (int32, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Int32(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinI8Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (int8, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Int8(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinI16Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (int16, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Int16(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinU32Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (uint32, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Uint32(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinU8Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (uint8, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Uint8(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinU16Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (uint16, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Uint16(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinU64Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (uint64, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Uint64(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinF64Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (float64, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Float64(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinF32Value(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (float32, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.Float32(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Values[row], v.IsValid(row), nil
}
func joinDictValue(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) (uint32, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return 0, false, nil
	}
	v, e := src.DictionaryString(b.in)
	if e != nil {
		return 0, false, e
	}
	return v.Indices[row], v.IsValid(row), nil
}
func joinStringValue(s *JoinSource, l *memory.Batch, b joinOutputBinding, p join.Pair) ([]byte, bool, error) {
	src, row, ok := joinSourceRow(s, l, b, p)
	if !ok {
		return nil, false, nil
	}
	v, e := src.String(b.in)
	if e != nil {
		return nil, false, e
	}
	return v.ValueBytes(row), v.IsValid(row), nil
}
