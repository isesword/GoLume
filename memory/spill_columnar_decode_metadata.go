package memory

import (
	"fmt"
	"math"
	"strconv"
	"unsafe"
)

// The decoder's metadata charge is a conservative engine-accounting contract,
// not a measurement or an absolute bound on Go heap/RSS. In particular it does
// not depend on the private runtime map header layout. We charge each nonempty
// map 128 bytes and each entry 512 bytes. For comparison, a Go 1.27 string/pointer
// map group on a 64-bit target has eight 24-byte slots and an 8-byte control
// word (200 bytes before rounding). 512 per entry allows two rounded groups
// even for a singleton, and ample table/directory/load-factor slack at larger
// cardinalities. Hinted construction avoids intentional incremental growth.
//
// Each Batch is charged twice its public header size plus 256 bytes for owner,
// reservation, temporary reader and allocation overhead. Vector descriptors
// and owned Field arrays are charged separately, rounded to 16 bytes. The
// original payload-length charge is retained in full for exact-capacity value,
// validity, offset, data and dictionary-domain copies. This is deliberately not
// an assertion about arbitrary future Go map/allocator implementations.
const (
	columnarDecodeMapBytes   = int64(128)
	columnarDecodeEntryBytes = int64(512)
	columnarDecodeBaseBytes  = 2*int64(unsafe.Sizeof(Batch{})) + 256
)

type columnarDecodeMap uint8

const (
	decodeInt8Map columnarDecodeMap = iota
	decodeInt16Map
	decodeInt32Map
	decodeInt64Map
	decodeUint8Map
	decodeUint16Map
	decodeUint32Map
	decodeUint64Map
	decodeFloat32Map
	decodeFloat64Map
	decodeDecimalMap
	decodeStringMap
	decodeDictionaryMap
	decodeListMap
	decodeStructMap
	decodeOpaqueMap // Classified explicitly, but not supported by this codec.
	decodeMapCount
)

// ColumnarRunDecodeMetadataBytes returns the schema-dependent metadata charge
// used by DecodeColumnarRun, in addition to len(payload). Successful estimation
// does not allocate. It rejects unsupported schemas before any decoder-owned
// object or validation scratch is allocated. The supported nesting is bounded:
// Struct children are scalars or List<String>; recursive Structs are rejected.
// Field names, strings and Children in the input schema remain borrowed.
func ColumnarRunDecodeMetadataBytes(schema Schema) (int64, error) {
	return columnarDecodeBatchBytes(schema.Fields, false)
}

func columnarDecodeAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, fmt.Errorf("columnar decode metadata size overflow")
	}
	return a + b, nil
}

func columnarDecodeMul(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, fmt.Errorf("columnar decode metadata size overflow")
	}
	return a * b, nil
}

func columnarDecodeRounded(bytes int64) (int64, error) {
	n, err := columnarDecodeAdd(bytes, 15)
	if err != nil {
		return 0, err
	}
	return n &^ 15, nil
}

// This is also the sparse constructor's classification. Logical numeric aliases
// share one physical map; binary/string share the view or dictionary map.
func columnarDecodeFieldMap(field Field) (columnarDecodeMap, uintptr) {
	switch field.Type {
	case TypeInt8:
		return decodeInt8Map, unsafe.Sizeof(Int8Vector{})
	case TypeInt16:
		return decodeInt16Map, unsafe.Sizeof(Int16Vector{})
	case TypeInt32, TypeDate32, TypeTime32:
		return decodeInt32Map, unsafe.Sizeof(Int32Vector{})
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		return decodeInt64Map, unsafe.Sizeof(Int64Vector{})
	case TypeUint8:
		return decodeUint8Map, unsafe.Sizeof(Uint8Vector{})
	case TypeUint16:
		return decodeUint16Map, unsafe.Sizeof(Uint16Vector{})
	case TypeUint32, TypeBool:
		return decodeUint32Map, unsafe.Sizeof(Uint32Vector{})
	case TypeUint64:
		return decodeUint64Map, unsafe.Sizeof(Uint64Vector{})
	case TypeFloat32:
		return decodeFloat32Map, unsafe.Sizeof(Float32Vector{})
	case TypeFloat64:
		return decodeFloat64Map, unsafe.Sizeof(Float64Vector{})
	case TypeDecimal:
		return decodeDecimalMap, unsafe.Sizeof(Decimal128Vector{})
	case TypeString, TypeBinary:
		if field.Encoding == EncodingDictionary {
			return decodeDictionaryMap, unsafe.Sizeof(DictionaryStringVector{})
		}
		return decodeStringMap, unsafe.Sizeof(StringViewVector{})
	case TypeList:
		return decodeListMap, unsafe.Sizeof(StringListVector{})
	case TypeStruct:
		return decodeStructMap, unsafe.Sizeof(StructVector{})
	case TypeArrow:
		return decodeOpaqueMap, unsafe.Sizeof(OpaqueVector{})
	default:
		return decodeMapCount, 0
	}
}

func columnarDecodeBatchBytes(fields []Field, structChildren bool) (int64, error) {
	if uint64(len(fields)) > math.MaxUint32 {
		return 0, fmt.Errorf("columnar schema field count exceeds uint32")
	}
	var counts [decodeMapCount]int64
	total := columnarDecodeBaseBytes
	for i, field := range fields {
		// An allocation-free uniqueness check avoids a seen map before Reserve.
		// Names only need to be scanned within this bounded nesting level.
		for j := 0; j < i; j++ {
			if fields[j].Name == field.Name {
				return 0, fmt.Errorf("columnar schema has duplicate field %q", field.Name)
			}
		}
		kind, descriptor := columnarDecodeFieldMap(field)
		if kind >= decodeOpaqueMap {
			return 0, fmt.Errorf("unsupported columnar field %q type %s", field.Name, field.Type)
		}
		if structChildren {
			if field.Name == "" || field.Type == TypeStruct || field.Type == TypeBinary || field.Type == TypeDecimal || field.Type == TypeString && field.Encoding == EncodingDictionary {
				return 0, fmt.Errorf("unsupported struct child %q type %s", field.Name, field.Type)
			}
		}
		var extra int64
		var err error
		switch field.Type {
		case TypeStruct:
			if len(field.Children) == 0 {
				return 0, fmt.Errorf("struct field %q requires at least one child", field.Name)
			}
			extra, err = columnarDecodeBatchBytes(field.Children, true)
			if err == nil {
				var bytes int64
				bytes, err = columnarDecodeMul(int64(len(field.Children)), int64(unsafe.Sizeof(Field{})))
				if err == nil {
					bytes, err = columnarDecodeRounded(bytes)
				}
				if err == nil {
					extra, err = columnarDecodeAdd(extra, bytes)
				}
			}
		case TypeList:
			if err = field.ValidateListString(); err == nil {
				if len(field.Children[0].Children) != 0 {
					return 0, fmt.Errorf("list field %q has nested string child", field.Name)
				}
				extra, err = columnarDecodeRounded(int64(unsafe.Sizeof(StringViewVector{})))
			}
		default:
			if len(field.Children) != 0 {
				return 0, fmt.Errorf("scalar field %q has nested children", field.Name)
			}
			if field.Type == TypeDecimal {
				err = validateColumnarDecodeDecimal(field)
			}
		}
		if err != nil {
			return 0, err
		}
		bytes, err := columnarDecodeRounded(int64(descriptor))
		if err != nil {
			return 0, err
		}
		bytes, err = columnarDecodeAdd(bytes, extra)
		if err == nil {
			total, err = columnarDecodeAdd(total, bytes)
		}
		if err != nil {
			return 0, err
		}
		counts[kind]++
	}
	for _, count := range counts {
		if count == 0 {
			continue
		}
		bytes, err := columnarDecodeMul(count, columnarDecodeEntryBytes)
		if err == nil {
			bytes, err = columnarDecodeAdd(bytes, columnarDecodeMapBytes)
		}
		if err == nil {
			total, err = columnarDecodeAdd(total, bytes)
		}
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// ValidateDecimal128 formats an Arrow spelling with fmt even on success. The
// decoder uses a bounded stack buffer instead, preserving the same contract.
func validateColumnarDecodeDecimal(field Field) error {
	if field.DecimalPrecision < 1 || field.DecimalPrecision > 38 || field.DecimalScale < 0 || field.DecimalScale > field.DecimalPrecision || field.Encoding != EncodingPlain || len(field.Children) != 0 || field.DictionaryDomain != "" {
		return fmt.Errorf("invalid native decimal128 field %q", field.Name)
	}
	if field.ArrowType != "" {
		var buf [32]byte
		spelling := append(buf[:0], "decimal("...)
		spelling = strconv.AppendInt(spelling, int64(field.DecimalPrecision), 10)
		spelling = append(spelling, ',', ' ')
		spelling = strconv.AppendInt(spelling, int64(field.DecimalScale), 10)
		spelling = append(spelling, ')')
		if field.ArrowType != string(spelling) {
			return fmt.Errorf("decimal field %q Arrow type conflicts with precision/scale", field.Name)
		}
	}
	return nil
}

// Called only after ColumnarRunDecodeMetadataBytes has validated the schema and
// the single owner reservation has been admitted. Unused maps remain nil.
func newColumnarDecodeBatch(fields []Field, rows int) *Batch {
	var counts [decodeMapCount]int
	for _, field := range fields {
		kind, _ := columnarDecodeFieldMap(field)
		counts[kind]++
	}
	batch := &Batch{Rows: rows}
	if n := counts[decodeInt8Map]; n != 0 {
		batch.Int8s = make(map[string]*Int8Vector, n)
	}
	if n := counts[decodeInt16Map]; n != 0 {
		batch.Int16s = make(map[string]*Int16Vector, n)
	}
	if n := counts[decodeInt32Map]; n != 0 {
		batch.Int32s = make(map[string]*Int32Vector, n)
	}
	if n := counts[decodeInt64Map]; n != 0 {
		batch.Int64s = make(map[string]*Int64Vector, n)
	}
	if n := counts[decodeUint8Map]; n != 0 {
		batch.Uint8s = make(map[string]*Uint8Vector, n)
	}
	if n := counts[decodeUint16Map]; n != 0 {
		batch.Uint16s = make(map[string]*Uint16Vector, n)
	}
	if n := counts[decodeUint32Map]; n != 0 {
		batch.Uint32s = make(map[string]*Uint32Vector, n)
	}
	if n := counts[decodeUint64Map]; n != 0 {
		batch.Uint64s = make(map[string]*Uint64Vector, n)
	}
	if n := counts[decodeFloat32Map]; n != 0 {
		batch.Float32s = make(map[string]*Float32Vector, n)
	}
	if n := counts[decodeFloat64Map]; n != 0 {
		batch.Float64s = make(map[string]*Float64Vector, n)
	}
	if n := counts[decodeDecimalMap]; n != 0 {
		batch.Decimals = make(map[string]*Decimal128Vector, n)
	}
	if n := counts[decodeStringMap]; n != 0 {
		batch.Strings = make(map[string]*StringViewVector, n)
	}
	if n := counts[decodeDictionaryMap]; n != 0 {
		batch.DictStrings = make(map[string]*DictionaryStringVector, n)
	}
	if n := counts[decodeListMap]; n != 0 {
		batch.StringLists = make(map[string]*StringListVector, n)
	}
	if n := counts[decodeStructMap]; n != 0 {
		batch.Structs = make(map[string]*StructVector, n)
	}
	return batch
}

// All successful data validation is allocation-free. The schema has already
// been validated, so Struct.Validate's allocating duplicate-name map and
// Decimal.ValidateField's formatted Arrow type are unnecessary here.
func validateColumnarDecodedField(batch *Batch, field Field, rows int) error {
	if field.Type == TypeStruct {
		v := batch.Structs[field.Name]
		if v == nil || v.Rows != rows || v.Values == nil || v.Values.Rows != rows || len(v.Fields) != len(field.Children) {
			return fmt.Errorf("struct field %q shape mismatch", field.Name)
		}
		if _, err := validitySize(v.Validity, rows); err != nil {
			return err
		}
		if !field.Nullable && HasNulls(v.Validity, rows) {
			return fmt.Errorf("non-nullable struct field %q contains NULL", field.Name)
		}
		for _, child := range field.Children {
			shape := child
			shape.Nullable = true
			if err := validateColumnarDecodedField(v.Values, shape, rows); err != nil {
				return err
			}
			if !child.Nullable {
				for row := 0; row < rows; row++ {
					if v.IsValid(row) && !batchFieldValid(v.Values, child, row) {
						return fmt.Errorf("struct field %q non-nullable child %q is NULL", field.Name, child.Name)
					}
				}
			}
		}
		return nil
	}
	if field.Type == TypeDecimal {
		v := batch.Decimals[field.Name]
		if v == nil || v.Precision != field.DecimalPrecision || v.Scale != field.DecimalScale {
			return fmt.Errorf("decimal field %q precision/scale mismatch", field.Name)
		}
		if err := v.Validate(rows); err != nil {
			return err
		}
		if !field.Nullable && HasNulls(v.Validity, rows) {
			return fmt.Errorf("non-nullable decimal field %q contains NULL", field.Name)
		}
		return nil
	}
	return validateRunField(batch, field, rows)
}

func clearColumnarDecodedVectors[T any](vectors map[string]*T) {
	for _, vector := range vectors {
		if vector != nil {
			var zero T
			*vector = zero
		}
	}
	clear(vectors)
}

// Destroy only the graph allocated by the decoder. In particular Fields owns
// its outer array, but its names and Children are borrowed from the schema.
// Clear maps as well as the Batch header so pre-Close map aliases lose entries.
func destroyColumnarDecodedBatch(batch *Batch) {
	if batch == nil {
		return
	}
	for _, vector := range batch.Structs {
		if vector != nil {
			destroyColumnarDecodedBatch(vector.Values)
			clear(vector.Fields) // Shallow: never clear borrowed Children arrays.
			*vector = StructVector{}
		}
	}
	for _, vector := range batch.StringLists {
		if vector != nil {
			if vector.Values != nil {
				*vector.Values = StringViewVector{}
			}
			*vector = StringListVector{}
		}
	}
	clearColumnarDecodedVectors(batch.Int8s)
	clearColumnarDecodedVectors(batch.Int16s)
	clearColumnarDecodedVectors(batch.Int32s)
	clearColumnarDecodedVectors(batch.Int64s)
	clearColumnarDecodedVectors(batch.Uint8s)
	clearColumnarDecodedVectors(batch.Uint16s)
	clearColumnarDecodedVectors(batch.Uint32s)
	clearColumnarDecodedVectors(batch.Uint64s)
	clearColumnarDecodedVectors(batch.Float32s)
	clearColumnarDecodedVectors(batch.Float64s)
	clearColumnarDecodedVectors(batch.Decimals)
	clearColumnarDecodedVectors(batch.Strings)
	clearColumnarDecodedVectors(batch.DictStrings)
	clear(batch.StringLists)
	clear(batch.Structs)
	// Opaque is never constructed by this codec.
	clear(batch.Opaque)
	*batch = Batch{}
}
