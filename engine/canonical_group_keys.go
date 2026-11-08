package engine

import (
	"encoding/binary"
	"fmt"
	"math"

	"gocolumn/memory"
)

const canonicalGroupHeaderBytes = 6
const canonicalGroupHashOffset uint64 = 14695981039346656037
const canonicalGroupHashPrime uint64 = 1099511628211

// canonicalGroupBinding borrows an immutable batch and its immutable group-key
// descriptors. It owns no buffers. Opaque Arrow values must be materialized by
// the source into their native vectors before this key binding is constructed.
// Logical metadata (such as timestamp units) belongs to the fixed descriptors,
// not the key bytes; keys are compared only within the same descriptor schema.
type canonicalGroupBinding struct {
	fields     []memory.Field
	batch      *memory.Batch
	components []canonicalGroupComponent
}

// canonicalGroupComponent resolves each column once per batch, never per row.
type canonicalGroupComponent struct {
	width       int
	validity    memory.Bitmap
	int8        *memory.Int8Vector
	int16       *memory.Int16Vector
	int32       *memory.Int32Vector
	int64       *memory.Int64Vector
	uint8       *memory.Uint8Vector
	uint16      *memory.Uint16Vector
	uint32      *memory.Uint32Vector
	uint64      *memory.Uint64Vector
	float32     *memory.Float32Vector
	float64     *memory.Float64Vector
	strings     *memory.StringViewVector
	dictStrings *memory.DictionaryStringVector
}

func canonicalGroupWidth(typ memory.DataType) (int, bool) {
	switch typ {
	case memory.TypeInt8, memory.TypeUint8, memory.TypeBool:
		return 1, true
	case memory.TypeInt16, memory.TypeUint16:
		return 2, true
	case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32:
		return 4, true
	case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return 8, true
	case memory.TypeString, memory.TypeBinary:
		return 0, true
	default:
		return 0, false
	}
}

func bindCanonicalGroupComponents(fields []memory.Field, batch *memory.Batch, components []canonicalGroupComponent) (canonicalGroupBinding, error) {
	binding := canonicalGroupBinding{fields: fields, batch: batch, components: components}
	if batch == nil || batch.Rows < 0 {
		return canonicalGroupBinding{}, fmt.Errorf("canonical group keys require a nonnegative batch")
	}
	for index, field := range fields {
		if _, ok := canonicalGroupWidth(field.Type); !ok {
			return canonicalGroupBinding{}, fmt.Errorf("canonical group key %q has unsupported type %s", field.Name, field.Type)
		}
		rows, validity, err := aggregateColumnInfo(batch, field.Name, field.Type, true)
		if err != nil {
			return canonicalGroupBinding{}, err
		}
		if rows != batch.Rows || (validity != nil && len(validity) < memory.WordsFor(rows)) {
			return canonicalGroupBinding{}, fmt.Errorf("canonical group key %q has invalid row count or validity", field.Name)
		}
		if field.Type == memory.TypeString || field.Type == memory.TypeBinary {
			if v := batch.Strings[field.Name]; v != nil {
				if err := validateCanonicalOffsets(v.Offsets, v.Data); err != nil {
					return canonicalGroupBinding{}, fmt.Errorf("canonical group key %q: %w", field.Name, err)
				}
			} else if v := batch.DictStrings[field.Name]; v != nil {
				if err := validateCanonicalOffsets(v.Offsets, v.Data); err != nil {
					return canonicalGroupBinding{}, fmt.Errorf("canonical dictionary key %q: %w", field.Name, err)
				}
				for row, code := range v.Indices {
					if v.IsValid(row) && uint64(code)+1 >= uint64(len(v.Offsets)) {
						return canonicalGroupBinding{}, fmt.Errorf("canonical dictionary key %q has invalid code at row %d", field.Name, row)
					}
				}
			}
		}
		if field.Type == memory.TypeBool {
			v := batch.Uint32s[field.Name]
			for row, value := range v.Values {
				if v.IsValid(row) && value > 1 {
					return canonicalGroupBinding{}, fmt.Errorf("canonical boolean key %q is not boolean at row %d", field.Name, row)
				}
			}
		}
		c := &components[index]
		c.width, _ = canonicalGroupWidth(field.Type)
		c.validity = validity
		switch field.Type {
		case memory.TypeInt8:
			c.int8 = batch.Int8s[field.Name]
		case memory.TypeInt16:
			c.int16 = batch.Int16s[field.Name]
		case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
			c.int32 = batch.Int32s[field.Name]
		case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
			c.int64 = batch.Int64s[field.Name]
		case memory.TypeUint8:
			c.uint8 = batch.Uint8s[field.Name]
		case memory.TypeUint16:
			c.uint16 = batch.Uint16s[field.Name]
		case memory.TypeUint32, memory.TypeBool:
			c.uint32 = batch.Uint32s[field.Name]
		case memory.TypeUint64:
			c.uint64 = batch.Uint64s[field.Name]
		case memory.TypeFloat32:
			c.float32 = batch.Float32s[field.Name]
		case memory.TypeFloat64:
			c.float64 = batch.Float64s[field.Name]
		case memory.TypeString, memory.TypeBinary:
			c.strings = batch.Strings[field.Name]
			if c.strings == nil {
				c.dictStrings = batch.DictStrings[field.Name]
			}
		}
	}
	return binding, nil
}

func validateCanonicalOffsets(offsets []uint32, data []byte) error {
	if len(offsets) == 0 {
		return fmt.Errorf("missing string offsets")
	}
	previous := uint32(0)
	for index, offset := range offsets {
		if uint64(offset) > uint64(len(data)) || (index > 0 && offset < previous) {
			return fmt.Errorf("invalid string offset %d", index)
		}
		previous = offset
	}
	return nil
}

func canonicalFloatBits(typ memory.DataType, bits uint64) uint64 {
	if typ == memory.TypeFloat32 {
		b := uint32(bits)
		if b&0x7fffffff == 0 {
			return 0
		}
		if b&0x7f800000 == 0x7f800000 && b&0x007fffff != 0 {
			return 0x7fc00000
		}
		return uint64(b)
	}
	if typ == memory.TypeFloat64 {
		if bits&0x7fffffffffffffff == 0 {
			return 0
		}
		if bits&0x7ff0000000000000 == 0x7ff0000000000000 && bits&0x000fffffffffffff != 0 {
			return 0x7ff8000000000000
		}
	}
	return bits
}

func (c *canonicalGroupComponent) value(field memory.Field, row int) (bits uint64, data []byte, valid bool, err error) {
	switch field.Type {
	case memory.TypeInt8:
		v := c.int8
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(uint8(v.Values[row])), nil, true, nil
	case memory.TypeInt16:
		v := c.int16
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(uint16(v.Values[row])), nil, true, nil
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v := c.int32
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(uint32(v.Values[row])), nil, true, nil
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v := c.int64
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(v.Values[row]), nil, true, nil
	case memory.TypeUint8:
		v := c.uint8
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(v.Values[row]), nil, true, nil
	case memory.TypeUint16:
		v := c.uint16
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(v.Values[row]), nil, true, nil
	case memory.TypeUint32, memory.TypeBool:
		v := c.uint32
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return uint64(v.Values[row]), nil, true, nil
	case memory.TypeUint64:
		v := c.uint64
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return v.Values[row], nil, true, nil
	case memory.TypeFloat32:
		v := c.float32
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return canonicalFloatBits(field.Type, uint64(math.Float32bits(v.Values[row]))), nil, true, nil
	case memory.TypeFloat64:
		v := c.float64
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		return canonicalFloatBits(field.Type, math.Float64bits(v.Values[row])), nil, true, nil
	case memory.TypeString, memory.TypeBinary:
		if v := c.strings; v != nil {
			if !v.IsValid(row) {
				return 0, nil, false, nil
			}
			lo, hi := v.Offsets[row], v.Offsets[row+1]
			if lo > hi || uint64(hi) > uint64(len(v.Data)) {
				return 0, nil, false, fmt.Errorf("canonical string key %q has invalid offsets", field.Name)
			}
			return 0, v.Data[lo:hi], true, nil
		}
		v := c.dictStrings
		if !v.IsValid(row) {
			return 0, nil, false, nil
		}
		code := uint64(v.Indices[row])
		if code+1 >= uint64(len(v.Offsets)) {
			return 0, nil, false, fmt.Errorf("canonical dictionary key %q has invalid code", field.Name)
		}
		lo, hi := v.Offsets[code], v.Offsets[code+1]
		if lo > hi || uint64(hi) > uint64(len(v.Data)) {
			return 0, nil, false, fmt.Errorf("canonical dictionary key %q has invalid offsets", field.Name)
		}
		return 0, v.Data[lo:hi], true, nil
	}
	return 0, nil, false, fmt.Errorf("unsupported canonical group type %s", field.Type)
}

func (b canonicalGroupBinding) checkRow(row int) error {
	if b.batch == nil || row < 0 || row >= b.batch.Rows {
		return fmt.Errorf("canonical group row %d is out of range", row)
	}
	return nil
}

// KeySize reports the exact encoded row length, including type/validity/length
// framing. The descriptors and batch must remain unchanged after binding.
func (b canonicalGroupBinding) KeySize(row int) (int, error) {
	if err := b.checkRow(row); err != nil {
		return 0, err
	}
	total := 0
	for index, field := range b.fields {
		c := &b.components[index]
		width := c.width
		if c.validity != nil && !memory.IsSet(c.validity, row) {
			width = 0
		} else if width == 0 {
			_, data, _, err := c.value(field, row)
			if err != nil {
				return 0, err
			}
			width = len(data)
		}
		if uint64(width) > math.MaxUint32 || total > math.MaxInt-canonicalGroupHeaderBytes || width > math.MaxInt-total-canonicalGroupHeaderBytes {
			return 0, fmt.Errorf("canonical group key size overflow")
		}
		total += canonicalGroupHeaderBytes + width
	}
	return total, nil
}

// AppendKey appends a canonical row without growing dst's backing array. The
// caller reserves and allocates capacity first. Capacity errors leave dst and
// its backing data unchanged. All row payloads were validated during binding.
func (b canonicalGroupBinding) AppendKey(dst []byte, row int) ([]byte, error) {
	size, err := b.KeySize(row)
	if err != nil {
		return dst, err
	}
	return b.appendSizedKey(dst, row, size)
}

// appendSizedKey uses a size previously obtained from KeySize for the same row
// and immutable binding. It avoids repeating the sizing pass in trusted callers.
func (b canonicalGroupBinding) appendSizedKey(dst []byte, row, size int) ([]byte, error) {
	if err := b.checkRow(row); err != nil {
		return dst, err
	}
	if size < 0 {
		return dst, fmt.Errorf("canonical group key size is negative")
	}
	if size > cap(dst)-len(dst) {
		return dst, fmt.Errorf("canonical group key destination needs %d spare bytes", size)
	}
	base := len(dst)
	out := dst[:base+size]
	pos := base
	for index, field := range b.fields {
		bits, data, valid, err := b.components[index].value(field, row)
		if err != nil {
			return dst, err
		}
		width := b.components[index].width
		if !valid {
			width = 0
		} else if width == 0 {
			width = len(data)
		}
		if len(out)-pos < canonicalGroupHeaderBytes || width > len(out)-pos-canonicalGroupHeaderBytes {
			return dst, fmt.Errorf("canonical group key size mismatch")
		}
		out[pos] = byte(field.Type)
		out[pos+1] = 0
		if valid {
			out[pos+1] = 1
		}
		binary.LittleEndian.PutUint32(out[pos+2:pos+6], uint32(width))
		pos += 6
		if valid {
			if field.Type == memory.TypeString || field.Type == memory.TypeBinary {
				copy(out[pos:pos+width], data)
			} else {
				for i := 0; i < width; i++ {
					out[pos+i] = byte(bits >> (8 * i))
				}
			}
		}
		pos += width
	}
	if pos != len(out) {
		return dst, fmt.Errorf("canonical group key size mismatch")
	}
	return out, nil
}

func canonicalGroupHashByte(hash uint64, value byte) uint64 {
	return (hash ^ uint64(value)) * canonicalGroupHashPrime
}

func canonicalGroupKeyHash(encoded []byte) uint64 {
	hash := canonicalGroupHashOffset
	for _, value := range encoded {
		hash = canonicalGroupHashByte(hash, value)
	}
	return hash
}

// HashRow hashes precisely the canonical encoded bytes without constructing a
// key or allocating scratch. It matches canonicalGroupKeyHash(AppendKey(...)).
func (b canonicalGroupBinding) HashRow(row int) (uint64, error) {
	if err := b.checkRow(row); err != nil {
		return 0, err
	}
	hash := canonicalGroupHashOffset
	for index, field := range b.fields {
		bits, data, valid, err := b.components[index].value(field, row)
		if err != nil {
			return 0, err
		}
		width := b.components[index].width
		if !valid {
			width = 0
		} else if width == 0 {
			width = len(data)
		}
		if uint64(width) > math.MaxUint32 {
			return 0, fmt.Errorf("canonical group component size overflow")
		}
		hash = canonicalGroupHashByte(hash, byte(field.Type))
		flag := byte(0)
		if valid {
			flag = 1
		}
		hash = canonicalGroupHashByte(hash, flag)
		for i := 0; i < 4; i++ {
			hash = canonicalGroupHashByte(hash, byte(uint32(width)>>(8*i)))
		}
		if valid {
			if field.Type == memory.TypeString || field.Type == memory.TypeBinary {
				for _, value := range data {
					hash = canonicalGroupHashByte(hash, value)
				}
			} else {
				for i := 0; i < width; i++ {
					hash = canonicalGroupHashByte(hash, byte(bits>>(8*i)))
				}
			}
		}
	}
	return hash, nil
}

// visitCanonicalGroupKey validates and visits a key without allocating or
// taking ownership. String/binary payloads borrow encoded until the callback
// returns; consumers copy them only after their own budget reservations. Native
// bits use the low physical width (signed narrow values are not sign-extended).
// A callback can precede a later framing error, so callbacks must not commit
// externally visible state until this function returns successfully.
func visitCanonicalGroupKey(encoded []byte, fields []memory.Field, visit func(index int, typ memory.DataType, valid bool, bits uint64, data []byte) error) error {
	if visit == nil {
		return fmt.Errorf("canonical group visitor is nil")
	}
	pos := 0
	for index, field := range fields {
		width, supported := canonicalGroupWidth(field.Type)
		if !supported {
			return fmt.Errorf("unsupported canonical group descriptor type %s", field.Type)
		}
		if len(encoded)-pos < canonicalGroupHeaderBytes {
			return fmt.Errorf("truncated canonical group header")
		}
		typ, flag := memory.DataType(encoded[pos]), encoded[pos+1]
		length := uint64(binary.LittleEndian.Uint32(encoded[pos+2 : pos+6]))
		pos += 6
		if typ != field.Type || flag > 1 {
			return fmt.Errorf("canonical group type or validity mismatch at component %d", index)
		}
		if length > uint64(len(encoded)-pos) {
			return fmt.Errorf("truncated canonical group payload")
		}
		if flag == 0 && length != 0 {
			return fmt.Errorf("NULL canonical group component has payload")
		}
		if flag == 1 && width != 0 && length != uint64(width) {
			return fmt.Errorf("canonical group native width mismatch")
		}
		payload := encoded[pos : pos+int(length)]
		pos += int(length)
		var bits uint64
		var data []byte
		if flag == 1 {
			if width == 0 {
				data = payload
			} else {
				for i, value := range payload {
					bits |= uint64(value) << (8 * i)
				}
			}
			if typ == memory.TypeBool && bits > 1 {
				return fmt.Errorf("canonical group boolean payload is not boolean")
			}
			if (typ == memory.TypeFloat32 || typ == memory.TypeFloat64) && canonicalFloatBits(typ, bits) != bits {
				return fmt.Errorf("noncanonical floating group payload")
			}
		}
		if err := visit(index, typ, flag == 1, bits, data); err != nil {
			return err
		}
	}
	if pos != len(encoded) {
		return fmt.Errorf("canonical group key has trailing bytes")
	}
	return nil
}
