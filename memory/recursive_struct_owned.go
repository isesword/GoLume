package memory

import (
	"context"
	"fmt"
	"math"
)

// OwnedRecursiveStruct retains a recursively copied native record and its
// conservative allocation reservation. Vector is invalid after Close.
type OwnedRecursiveStruct struct {
	Vector *StructVector
	Bytes  int64
	lease  Reservation
}

// Close releases the owned result and its reservation. Repeated calls are safe.
func (o *OwnedRecursiveStruct) Close() {
	if o == nil {
		return
	}
	o.Vector = nil
	if o.lease != nil {
		o.lease.Release()
		o.lease = nil
	}
}

// CloneRecursiveStructRangeOwned validates a bounded native tree before copying
// a canonical row range. It admits containers, deep schema snapshots and a
// conservative fourfold append-growth payload envelope before allocating output.
// Source memory, GC-delayed garbage and process RSS are outside this charge.
// Failure releases all new reservations and leaves the source untouched.
func CloneRecursiveStructRangeOwned(ctx context.Context, manager Manager, src *StructVector, field Field, offset, length int) (*OwnedRecursiveStruct, error) {
	if ctx == nil || manager == nil {
		return nil, fmt.Errorf("recursive owned copy requires context and manager")
	}
	if src == nil || offset < 0 || length < 0 || offset > src.Rows || length > src.Rows-offset {
		return nil, fmt.Errorf("recursive owned range out of bounds")
	}
	if err := src.ValidateRecursiveWithContext(field, src.Rows, ctx.Err); err != nil {
		return nil, err
	}
	bytes, err := recursiveOwnedEstimate(src, field, offset, length, ctx.Err)
	if err != nil {
		return nil, err
	}
	lease, err := manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			lease.Release()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := newRecursiveOwnedVector(field, ctx.Err)
	if err != nil {
		return nil, err
	}
	for row := offset; row < offset+length; row++ {
		if err := appendRecursiveOwnedRow(out, src, field, row, true, ctx.Err); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return &OwnedRecursiveStruct{Vector: out, Bytes: bytes, lease: lease}, nil
}

func recursiveOwnedEstimate(v *StructVector, f Field, offset, length int, check func() error) (int64, error) {
	if err := recursiveCheck(check); err != nil {
		return 0, err
	}
	// Every node owns its Batch/maps, vector, Fields and parent bitmap. Schema
	// descendants can occur in up to eight snapshots, so charge each edge eight
	// times rather than relying on leaf-count envelopes for a narrow deep chain.
	total := int64(4096)
	add := func(n, m int64) error { var e error; total, e = ownedRangeAdd(total, n, m); return e }
	if err := add(int64(WordsFor(length)), 32); err != nil {
		return 0, err
	}
	for _, c := range f.Children {
		if err := recursiveCheck(check); err != nil {
			return 0, err
		}
		if err := add(512+int64(len(c.Name))+int64(len(c.ArrowType))+int64(len(c.DictionaryDomain)), MaxStructDepth); err != nil {
			return 0, err
		}
		if c.Type == TypeStruct {
			n, e := recursiveOwnedEstimate(v.Values.Structs[c.Name], c, offset, length, check)
			if e != nil {
				return 0, e
			}
			if e = add(n, 1); e != nil {
				return 0, e
			}
			continue
		}
		if err := add(4096, 1); err != nil {
			return 0, err
		}
		// All terminal buffers are conservatively sized even for hidden rows;
		// dictionary values are expanded only for visible valid codes below.
		var payload int64
		var err error
		if c.Type == TypeString {
			payload, err = ownedRangeAdd(int64(WordsFor(length))*8, int64(length)+1, 4)
			if err != nil {
				return 0, err
			}
			for row := offset; row < offset+length; row++ {
				if err := recursiveCheck(check); err != nil {
					return 0, err
				}
				if !v.IsValid(row) || !batchFieldValid(v.Values, c, row) {
					continue
				}
				var data []byte
				if s := v.Values.Strings[c.Name]; s != nil {
					data = s.ValueBytes(row)
				} else {
					d := v.Values.DictStrings[c.Name]
					if uint64(d.Indices[row]) >= uint64(d.DictionaryLen()) {
						continue
					}
					data = d.DictionaryValue(d.Indices[row])
				}
				payload, err = ownedRangeAdd(payload, int64(len(data)), 1)
				if err != nil {
					return 0, err
				}
			}
		} else {
			single := *v
			var one [1]Field
			one[0] = c
			single.Fields = one[:]
			payload, _, err = nativeStructAppendEstimate(&single, offset, length, true)
			if err != nil {
				return 0, err
			}
		}
		if err := add(payload, 4); err != nil {
			return 0, err
		}
	}
	return total, nil
}

func newRecursiveOwnedVector(f Field, check func() error) (*StructVector, error) {
	if err := recursiveCheck(check); err != nil {
		return nil, err
	}
	out := &StructVector{Fields: make([]Field, len(f.Children)), Values: NewBatch(), Encoding: EncodingView}
	for i, c := range f.Children {
		if err := recursiveCheck(check); err != nil {
			return nil, err
		}
		snapshot, err := cloneRecursiveFieldContext(c, check)
		if err != nil {
			return nil, err
		}
		out.Fields[i] = snapshot
		switch c.Type {
		case TypeStruct:
			v, e := newRecursiveOwnedVector(c, check)
			if e != nil {
				return nil, e
			}
			out.Values.Structs[c.Name] = v
		case TypeString:
			out.Values.Strings[c.Name] = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
		case TypeList:
			if c.Children[0].Type == TypeInt64 {
				out.Values.Int64Lists[c.Name] = &Int64ListVector{Offsets: []uint32{0}, Values: &Int64Vector{Encoding: EncodingPlain}, Encoding: EncodingView}
			} else {
				out.Values.StringLists[c.Name] = &StringListVector{Offsets: []uint32{0}, Values: &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}, Encoding: EncodingView}
			}
		default:
			initRecursiveFixed(out.Values, c)
		}
	}
	return out, nil
}

func initRecursiveFixed(b *Batch, f Field) {
	switch f.Type {
	case TypeInt8:
		b.Int8s[f.Name] = &Int8Vector{Encoding: f.Encoding}
	case TypeInt16:
		b.Int16s[f.Name] = &Int16Vector{Encoding: f.Encoding}
	case TypeInt32, TypeDate32, TypeTime32:
		b.Int32s[f.Name] = &Int32Vector{Encoding: f.Encoding}
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		b.Int64s[f.Name] = &Int64Vector{Encoding: f.Encoding}
	case TypeUint8:
		b.Uint8s[f.Name] = &Uint8Vector{Encoding: f.Encoding}
	case TypeUint16:
		b.Uint16s[f.Name] = &Uint16Vector{Encoding: f.Encoding}
	case TypeUint32, TypeBool:
		b.Uint32s[f.Name] = &Uint32Vector{Encoding: f.Encoding}
	case TypeUint64:
		b.Uint64s[f.Name] = &Uint64Vector{Encoding: f.Encoding}
	case TypeFloat32:
		b.Float32s[f.Name] = &Float32Vector{Encoding: f.Encoding}
	case TypeFloat64:
		b.Float64s[f.Name] = &Float64Vector{Encoding: f.Encoding}
	}
}

func appendRecursiveString(dst *StringViewVector, data []byte, valid bool, check func() error) error {
	row := dst.Len()
	if valid {
		if uint64(len(dst.Data))+uint64(len(data)) > math.MaxUint32 {
			return fmt.Errorf("recursive String byte offsets overflow")
		}
		for len(data) > 0 {
			if err := recursiveCheck(check); err != nil {
				return err
			}
			n := len(data)
			if n > 32768 {
				n = 32768
			}
			dst.Data = append(dst.Data, data[:n]...)
			data = data[n:]
		}
	}
	dst.Validity = appendValidity(dst.Validity, valid, row)
	dst.Offsets = append(dst.Offsets, uint32(len(dst.Data)))
	return nil
}

func appendRecursiveOwnedRow(dst, src *StructVector, f Field, row int, ancestor bool, check func() error) error {
	if err := recursiveCheck(check); err != nil {
		return err
	}
	present := ancestor && src.IsValid(row)
	for _, c := range f.Children {
		if err := recursiveCheck(check); err != nil {
			return err
		}
		if c.Type == TypeStruct {
			if err := appendRecursiveOwnedRow(dst.Values.Structs[c.Name], src.Values.Structs[c.Name], c, row, present, check); err != nil {
				return err
			}
			continue
		}
		valid := present && batchFieldValid(src.Values, c, row)
		switch c.Type {
		case TypeString:
			var data []byte
			if valid {
				if s := src.Values.Strings[c.Name]; s != nil {
					data = s.ValueBytes(row)
				} else {
					d := src.Values.DictStrings[c.Name]
					data = d.DictionaryValue(d.Indices[row])
				}
			}
			if err := appendRecursiveString(dst.Values.Strings[c.Name], data, valid, check); err != nil {
				return err
			}
		case TypeList:
			if c.Children[0].Type == TypeInt64 {
				in, out := src.Values.Int64Lists[c.Name], dst.Values.Int64Lists[c.Name]
				if valid {
					start, end, _ := in.ValueRange(row)
					if uint64(out.Values.Len())+uint64(end-start) > math.MaxUint32 {
						return fmt.Errorf("recursive List element offsets overflow")
					}
					for e := start; e < end; e++ {
						if err := recursiveCheck(check); err != nil {
							return err
						}
						ok := in.Values.IsValid(e)
						value := int64(0)
						if ok {
							value = in.Values.Values[e]
						}
						out.Values.Validity = appendValidity(out.Values.Validity, ok, out.Values.Len())
						out.Values.Values = append(out.Values.Values, value)
					}
				}
				out.Validity = appendValidity(out.Validity, valid, dst.Rows)
				out.Offsets = append(out.Offsets, uint32(out.Values.Len()))
			} else {
				in, out := src.Values.StringLists[c.Name], dst.Values.StringLists[c.Name]
				if valid {
					start, end, _ := in.ValueRange(row)
					if uint64(out.Values.Len())+uint64(end-start) > math.MaxUint32 {
						return fmt.Errorf("recursive List element offsets overflow")
					}
					for e := start; e < end; e++ {
						if err := recursiveCheck(check); err != nil {
							return err
						}
						ok := in.Values.IsValid(e)
						var data []byte
						if ok {
							data = in.Values.ValueBytes(e)
						}
						if err := appendRecursiveString(out.Values, data, ok, check); err != nil {
							return err
						}
					}
				}
				out.Validity = appendValidity(out.Validity, valid, dst.Rows)
				out.Offsets = append(out.Offsets, uint32(out.Values.Len()))
			}
		default:
			appendRecursiveFixed(dst.Values, src.Values, c, row, valid)

		}
	}
	dst.Validity = appendValidity(dst.Validity, present, dst.Rows)
	dst.Rows++
	dst.Values.Rows = dst.Rows
	return nil
}

func appendRecursiveFixed(dst, src *Batch, f Field, row int, valid bool) {
	switch f.Type {
	case TypeInt8:
		out, in := dst.Int8s[f.Name], src.Int8s[f.Name]
		var value int8
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeInt16:
		out, in := dst.Int16s[f.Name], src.Int16s[f.Name]
		var value int16
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeInt32, TypeDate32, TypeTime32:
		out, in := dst.Int32s[f.Name], src.Int32s[f.Name]
		var value int32
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		out, in := dst.Int64s[f.Name], src.Int64s[f.Name]
		var value int64
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeUint8:
		out, in := dst.Uint8s[f.Name], src.Uint8s[f.Name]
		var value uint8
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeUint16:
		out, in := dst.Uint16s[f.Name], src.Uint16s[f.Name]
		var value uint16
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeUint32, TypeBool:
		out, in := dst.Uint32s[f.Name], src.Uint32s[f.Name]
		var value uint32
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeUint64:
		out, in := dst.Uint64s[f.Name], src.Uint64s[f.Name]
		var value uint64
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeFloat32:
		out, in := dst.Float32s[f.Name], src.Float32s[f.Name]
		var value float32
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	case TypeFloat64:
		out, in := dst.Float64s[f.Name], src.Float64s[f.Name]
		var value float64
		if valid {
			value = in.Values[row]
		}
		out.Validity = appendValidity(out.Validity, valid, len(out.Values))
		out.Values = append(out.Values, value)
	}
}

func cloneRecursiveFieldContext(f Field, check func() error) (Field, error) {
	if err := recursiveCheck(check); err != nil {
		return Field{}, err
	}
	out := f
	if len(f.Children) > 0 {
		out.Children = make([]Field, len(f.Children))
		for i, c := range f.Children {
			v, err := cloneRecursiveFieldContext(c, check)
			if err != nil {
				return Field{}, err
			}
			out.Children[i] = v
		}
	}
	return out, nil
}
