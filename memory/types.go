package memory

import "fmt"

// DataType identifies a logical scalar type supported by the columnar engine.
type DataType uint8

const (
	// TypeInvalid marks an unspecified or unsupported logical type.
	TypeInvalid DataType = iota
	// TypeInt32 identifies signed 32-bit integers.
	TypeInt32
	// TypeInt64 identifies signed 64-bit integers.
	TypeInt64
	// TypeUint32 identifies unsigned 32-bit integers.
	TypeUint32
	// TypeUint64 identifies unsigned 64-bit integers.
	TypeUint64
	// TypeFloat64 identifies IEEE-754 64-bit floating-point values.
	TypeFloat64
	// TypeBinary identifies arbitrary byte sequences.
	TypeBinary
	// TypeString identifies UTF-8 string values.
	TypeString
	// TypeBool identifies boolean values.
	TypeBool
	// TypeInt8 identifies signed 8-bit integers.
	TypeInt8
	// TypeInt16 identifies signed 16-bit integers.
	TypeInt16
	// TypeUint8 identifies unsigned 8-bit integers.
	TypeUint8
	// TypeUint16 identifies unsigned 16-bit integers.
	TypeUint16
	// TypeFloat32 identifies IEEE-754 32-bit floating-point values.
	TypeFloat32
	// TypeDate32 identifies Arrow date32 values.
	TypeDate32
	// TypeDate64 identifies Arrow date64 values.
	TypeDate64
	// TypeTimestamp identifies Arrow timestamp values.
	TypeTimestamp
	// TypeTime32 identifies Arrow time32 values.
	TypeTime32
	// TypeTime64 identifies Arrow time64 values.
	TypeTime64
	// TypeDuration identifies Arrow duration values.
	TypeDuration
	// TypeDecimal identifies Arrow decimal values whose exact precision/scale is
	// carried in Field.ArrowType.
	TypeDecimal
	// TypeArrow identifies an Arrow logical type that is preserved by an Arrow
	// RecordBatch source but does not yet have a specialized execution vector.
	TypeArrow
)

// String returns the stable textual name of the data type.
func (t DataType) String() string {
	switch t {
	case TypeInt32:
		return "int32"
	case TypeInt64:
		return "int64"
	case TypeUint32:
		return "uint32"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeBinary:
		return "binary"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	case TypeInt8:
		return "int8"
	case TypeInt16:
		return "int16"
	case TypeUint8:
		return "uint8"
	case TypeUint16:
		return "uint16"
	case TypeFloat32:
		return "float32"
	case TypeDate32:
		return "date32"
	case TypeDate64:
		return "date64"
	case TypeTimestamp:
		return "timestamp"
	case TypeTime32:
		return "time32"
	case TypeTime64:
		return "time64"
	case TypeDuration:
		return "duration"
	case TypeDecimal:
		return "decimal"
	case TypeArrow:
		return "arrow"
	default:
		return "invalid"
	}
}

// Encoding identifies the physical representation used by a logical column.
type Encoding uint8

const (
	// EncodingPlain stores values directly in a type-specific buffer.
	EncodingPlain Encoding = iota
	// EncodingDictionary stores integer codes that reference a separate dictionary.
	EncodingDictionary
	// EncodingConstant represents one value repeated for every logical row.
	EncodingConstant
	// EncodingView stores offsets or lightweight views into backing byte buffers.
	EncodingView
)

// String returns the stable textual name of the physical encoding.
func (e Encoding) String() string {
	switch e {
	case EncodingPlain:
		return "plain"
	case EncodingDictionary:
		return "dictionary"
	case EncodingConstant:
		return "constant"
	case EncodingView:
		return "view"
	default:
		return "unknown"
	}
}

// Field describes one logical column and the physical encoding contract exposed by its source.
type Field struct {
	Name     string
	Type     DataType
	Nullable bool
	Encoding Encoding
	// DictionaryDomain is non-empty only when dictionary codes are stable
	// across every batch/partition exposed by the source.
	DictionaryDomain string
	// ArrowType preserves the exact Arrow logical type spelling for RecordBatch
	// sources, including nested, extension, decimal, timezone, and other type
	// metadata that is intentionally not collapsed into DataType.
	ArrowType string
}

// Schema is an immutable logical field list with name lookup support.
type Schema struct {
	Fields []Field
	index  map[string]int
}

// NewSchema constructs a schema and indexes fields by name.
func NewSchema(fields ...Field) Schema {
	s := Schema{Fields: append([]Field(nil), fields...), index: make(map[string]int, len(fields))}
	for i, f := range fields {
		s.index[f.Name] = i
	}
	return s
}

// Field returns the field named name and whether it exists.
func (s Schema) Field(name string) (Field, bool) {
	if s.index != nil {
		i, ok := s.index[name]
		if !ok {
			return Field{}, false
		}
		return s.Fields[i], true
	}
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Require validates that name exists and has typ.
func (s Schema) Require(name string, typ DataType) error {
	f, ok := s.Field(name)
	if !ok {
		return fmt.Errorf("column %q not found", name)
	}
	if f.Type != typ {
		return fmt.Errorf("column %q: expected %s, got %s", name, typ, f.Type)
	}
	return nil
}
