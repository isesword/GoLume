package engine

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
)

// Generic group outputs always use native vectors and resolved plain strings.
func allocateGenericGroupColumn(dst *memory.Batch, field memory.Field, rows, stringCapacity int) error {
	var validity memory.Bitmap
	if field.Nullable {
		validity = memory.EnsureBitmap(nil, rows)
	}
	switch field.Type {
	case memory.TypeInt8:
		dst.Int8s[field.Name] = &memory.Int8Vector{Values: make([]int8, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeInt16:
		dst.Int16s[field.Name] = &memory.Int16Vector{Values: make([]int16, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		dst.Int32s[field.Name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		dst.Int64s[field.Name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeUint8:
		dst.Uint8s[field.Name] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeUint16:
		dst.Uint16s[field.Name] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeUint32, memory.TypeBool:
		dst.Uint32s[field.Name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeUint64:
		dst.Uint64s[field.Name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeFloat32:
		dst.Float32s[field.Name] = &memory.Float32Vector{Values: make([]float32, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeFloat64:
		dst.Float64s[field.Name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: validity, Encoding: memory.EncodingPlain}
	case memory.TypeString, memory.TypeBinary:
		dst.Strings[field.Name] = &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, 0, stringCapacity), Validity: validity, Encoding: memory.EncodingView}
	default:
		return fmt.Errorf("unsupported generic key output %s", field.Type)
	}
	return nil
}

func setGenericGroupColumn(dst *memory.Batch, field memory.Field, row int, valid bool, bits uint64, data []byte) error {
	if !valid && !field.Nullable {
		return fmt.Errorf("%w: NULL generic key for non-nullable field %q", memory.ErrSpillCorrupt, field.Name)
	}
	var validity memory.Bitmap
	switch field.Type {
	case memory.TypeInt8:
		v := dst.Int8s[field.Name]
		v.Values[row] = int8(bits)
		validity = v.Validity
	case memory.TypeInt16:
		v := dst.Int16s[field.Name]
		v.Values[row] = int16(bits)
		validity = v.Validity
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v := dst.Int32s[field.Name]
		v.Values[row] = int32(bits)
		validity = v.Validity
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v := dst.Int64s[field.Name]
		v.Values[row] = int64(bits)
		validity = v.Validity
	case memory.TypeUint8:
		v := dst.Uint8s[field.Name]
		v.Values[row] = uint8(bits)
		validity = v.Validity
	case memory.TypeUint16:
		v := dst.Uint16s[field.Name]
		v.Values[row] = uint16(bits)
		validity = v.Validity
	case memory.TypeUint32, memory.TypeBool:
		v := dst.Uint32s[field.Name]
		v.Values[row] = uint32(bits)
		validity = v.Validity
	case memory.TypeUint64:
		v := dst.Uint64s[field.Name]
		v.Values[row] = uint64(bits)
		validity = v.Validity
	case memory.TypeFloat32:
		v := dst.Float32s[field.Name]
		v.Values[row] = math.Float32frombits(uint32(bits))
		validity = v.Validity
	case memory.TypeFloat64:
		v := dst.Float64s[field.Name]
		v.Values[row] = math.Float64frombits(bits)
		validity = v.Validity
	case memory.TypeString, memory.TypeBinary:
		v := dst.Strings[field.Name]
		validity = v.Validity
		if valid {
			if len(data) > cap(v.Data)-len(v.Data) || uint64(len(v.Data))+uint64(len(data)) > math.MaxUint32 {
				return fmt.Errorf("generic output key payload exceeds reserved capacity")
			}
			v.Data = append(v.Data, data...)
		}
		v.Offsets[row+1] = uint32(len(v.Data))
	default:
		return fmt.Errorf("unsupported generic key output %s", field.Type)
	}
	if valid && validity != nil {
		memory.Set(validity, row)
	}
	return nil
}
func (s *spillableAggregateStream) setGenericOutputKeys(dst *memory.Batch, row int, key []byte) error {
	return visitCanonicalGroupKey(key, s.query.cq.GenericFields, func(i int, _ memory.DataType, valid bool, bits uint64, data []byte) error {
		field, ok := s.schema.Field(s.query.cq.GenericFields[i].Name)
		if !ok {
			return fmt.Errorf("missing generic output field")
		}
		return setGenericGroupColumn(dst, field, row, valid, bits, data)
	})
}

// A combined lease covers every string buffer. Replacement reserves all new
// capacities before allocating or releasing the old buffers, including overlap.
func (s *sortedAggregateStream) ensureGenericOutputStrings(ctx context.Context, dst *memory.Batch, key []byte) error {
	var total int64
	grow := false
	err := visitCanonicalGroupKey(key, s.query.cq.GenericFields, func(i int, typ memory.DataType, valid bool, _ uint64, data []byte) error {
		if typ != memory.TypeString && typ != memory.TypeBinary {
			return nil
		}
		v := dst.Strings[s.query.cq.GenericFields[i].Name]
		n := len(data)
		if !valid {
			n = 0
		}
		capacity, err := genericStringCapacity(len(v.Data), cap(v.Data), n)
		if err != nil {
			return err
		}
		if capacity != cap(v.Data) {
			grow = true
		}
		if int64(capacity) > math.MaxInt64-total {
			return fmt.Errorf("generic string capacity overflow")
		}
		total += int64(capacity)
		return nil
	})
	if err != nil || !grow {
		return err
	}
	lease, err := s.outputBudget.ReserveGrowth(ctx, total)
	if err != nil {
		return fmt.Errorf("reserve generic output strings: %w", err)
	}
	err = visitCanonicalGroupKey(key, s.query.cq.GenericFields, func(i int, typ memory.DataType, valid bool, _ uint64, data []byte) error {
		if typ != memory.TypeString && typ != memory.TypeBinary {
			return nil
		}
		v := dst.Strings[s.query.cq.GenericFields[i].Name]
		n := len(data)
		if !valid {
			n = 0
		}
		capacity, _ := genericStringCapacity(len(v.Data), cap(v.Data), n)
		replacement := make([]byte, len(v.Data), capacity)
		copy(replacement, v.Data)
		v.Data = replacement
		return nil
	})
	if err != nil {
		lease.Release()
		return err
	}
	if s.outputStringLease != nil {
		s.outputStringLease.Release()
	}
	s.outputStringLease = lease
	return nil
}
func genericStringCapacity(length, capacity, extra int) (int, error) {
	if extra > math.MaxInt-length || uint64(length)+uint64(extra) > math.MaxUint32 {
		return 0, fmt.Errorf("generic output string offsets overflow")
	}
	need := length + extra
	if need <= capacity {
		return capacity, nil
	}
	next := need
	if capacity <= math.MaxInt/2 && capacity*2 > next {
		next = capacity * 2
	}
	if next < 64 {
		next = 64
	}
	return next, nil
}

func genericGroupPayloadSize(key []byte, fields []memory.Field, index int) (int64, error) {
	var size int64
	err := visitCanonicalGroupKey(key, fields, func(i int, typ memory.DataType, valid bool, _ uint64, data []byte) error {
		if valid && (index < 0 || index == i) && (typ == memory.TypeString || typ == memory.TypeBinary) {
			if int64(len(data)) > math.MaxInt64-size {
				return fmt.Errorf("generic group payload overflow")
			}
			size += int64(len(data))
		}
		return nil
	})
	return size, err
}
