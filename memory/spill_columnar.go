package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"math"
	"sync"
)

const (
	// Version 2 stores the narrow numeric vectors at their native physical
	// widths. Version 1 was an internal spill representation which widened
	// those columns, so it must not be decoded as this layout.
	columnarRunVersion = uint16(2)
	columnarRunHeader  = 22
)

var columnarRunMagic = [8]byte{'G', 'L', 'C', 'R', 'U', 'N', '0', '1'}

// ColumnarRunCodec is the stable codec name stored in SpillObject metadata.
const ColumnarRunCodec = "arrow-shaped-columnar-v1"

// EncodedColumnarRun owns a fixed-size columnar payload and its workspace
// reservation until the object has been committed to a spill store.
type EncodedColumnarRun struct {
	Payload     []byte
	reservation Reservation
	once        sync.Once
}

// Close releases encode workspace accounting and the payload reference.
func (r *EncodedColumnarRun) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		r.Payload = nil
		if r.reservation != nil {
			r.reservation.Release()
			r.reservation = nil
		}
	})
	return nil
}

// DecodedColumnarRun owns a reconstructed specialized Batch and its workspace
// reservation. Close releases opaque Arrow references if future codecs add any.
type DecodedColumnarRun struct {
	Batch       *Batch
	reservation Reservation
	once        sync.Once
}

// Close releases decoded Batch ownership and its reservation.
func (r *DecodedColumnarRun) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.Batch != nil {
			r.Batch.ReleaseOwned()
			r.Batch = nil
		}
		if r.reservation != nil {
			r.reservation.Release()
			r.reservation = nil
		}
	})
	return nil
}

// EncodeColumnarRun encodes the current specialized vectors into a versioned,
// little-endian, Arrow-shaped buffer layout. It reserves the exact payload
// length before allocating. Opaque Arrow values require a future IPC codec and
// are rejected explicitly rather than serialized through interface{}.
func EncodeColumnarRun(ctx context.Context, schema Schema, batch *Batch, workspace Manager) (*EncodedColumnarRun, error) {
	if ctx == nil {
		return nil, fmt.Errorf("columnar encode context is nil")
	}
	if batch == nil {
		return nil, fmt.Errorf("columnar encode batch is nil")
	}
	if workspace == nil {
		return nil, fmt.Errorf("columnar encode requires a workspace manager")
	}
	size, err := columnarRunSize(schema, batch)
	if err != nil {
		return nil, err
	}
	reservation, err := workspace.Reserve(ctx, int64(size))
	if err != nil {
		return nil, fmt.Errorf("reserve columnar encode buffer: %w", err)
	}
	payload := make([]byte, size)
	writer := runWriter{buf: payload}
	writer.bytes(columnarRunMagic[:])
	writer.u16(columnarRunVersion)
	writer.u64(uint64(batch.Rows))
	writer.u32(uint32(len(schema.Fields)))
	for _, field := range schema.Fields {
		if err := encodeRunField(&writer, batch, field, batch.Rows); err != nil {
			reservation.Release()
			return nil, err
		}
	}
	if writer.err != nil || writer.pos != len(payload) {
		reservation.Release()
		if writer.err != nil {
			return nil, writer.err
		}
		return nil, fmt.Errorf("columnar encoder wrote %d bytes, expected %d", writer.pos, len(payload))
	}
	return &EncodedColumnarRun{Payload: payload, reservation: reservation}, nil
}

// DecodeColumnarRun reconstructs specialized vectors from a validated restored
// payload. It reserves payload length before allocating vector buffers.
func DecodeColumnarRun(ctx context.Context, schema Schema, restored *RestoredSpill, workspace Manager) (*DecodedColumnarRun, error) {
	if ctx == nil {
		return nil, fmt.Errorf("columnar decode context is nil")
	}
	if restored == nil || restored.Object.Codec != ColumnarRunCodec {
		return nil, fmt.Errorf("columnar decode requires codec %q", ColumnarRunCodec)
	}
	if workspace == nil {
		return nil, fmt.Errorf("columnar decode requires a workspace manager")
	}
	payload := restored.Object.Payload
	reservation, err := workspace.Reserve(ctx, int64(len(payload)))
	if err != nil {
		return nil, fmt.Errorf("reserve columnar decode buffers: %w", err)
	}
	reader := runReader{buf: payload}
	if string(reader.bytes(8)) != string(columnarRunMagic[:]) || reader.u16() != columnarRunVersion {
		reservation.Release()
		return nil, fmt.Errorf("%w: columnar run magic or version mismatch", ErrSpillCorrupt)
	}
	rows64 := reader.u64()
	if rows64 >= uint64(math.MaxInt) || reader.u32() != uint32(len(schema.Fields)) {
		reservation.Release()
		return nil, fmt.Errorf("%w: columnar run row or field count mismatch", ErrSpillCorrupt)
	}
	rows := int(rows64)
	batch := NewBatch()
	batch.Rows = rows
	for _, field := range schema.Fields {
		if err := decodeRunField(&reader, batch, field, rows); err != nil {
			batch.ReleaseOwned()
			reservation.Release()
			return nil, err
		}
	}
	if reader.err != nil {
		batch.ReleaseOwned()
		reservation.Release()
		return nil, reader.err
	}
	for _, field := range schema.Fields {
		if err := validateRunField(batch, field, rows); err != nil {
			batch.ReleaseOwned()
			reservation.Release()
			return nil, fmt.Errorf("%w: decoded field %q: %v", ErrSpillCorrupt, field.Name, err)
		}
	}
	if reader.pos != len(payload) {
		batch.ReleaseOwned()
		reservation.Release()
		return nil, fmt.Errorf("%w: columnar run has trailing bytes", ErrSpillCorrupt)
	}
	return &DecodedColumnarRun{Batch: batch, reservation: reservation}, nil
}

func columnarRunSize(schema Schema, batch *Batch) (int, error) {
	if batch.Rows < 0 {
		return 0, fmt.Errorf("columnar batch rows cannot be negative")
	}
	if uint64(len(schema.Fields)) > math.MaxUint32 {
		return 0, fmt.Errorf("columnar schema field count exceeds uint32")
	}
	size := int64(columnarRunHeader)
	for _, field := range schema.Fields {
		if err := validateRunField(batch, field, batch.Rows); err != nil {
			return 0, fmt.Errorf("columnar field %q: %w", field.Name, err)
		}
		fieldSize, err := runFieldSize(batch, field, batch.Rows)
		if err != nil {
			return 0, err
		}
		if fieldSize > math.MaxInt64-size {
			return 0, fmt.Errorf("columnar run size overflow")
		}
		size += fieldSize
	}
	if size > int64(math.MaxInt) {
		return 0, fmt.Errorf("columnar run exceeds addressable memory")
	}
	return int(size), nil
}

func validitySize(validity Bitmap, rows int) (int64, error) {
	if validity == nil {
		return 4, nil
	}
	words := WordsFor(rows)
	if len(validity) < words {
		return 0, fmt.Errorf("validity words=%d want>=%d", len(validity), words)
	}
	return 4 + int64(words)*8, nil
}

func runFieldSize(batch *Batch, field Field, rows int) (int64, error) {
	if batch == nil {
		return 0, fmt.Errorf("field %q has nil batch", field.Name)
	}
	if field.Type == TypeStruct {
		vector := batch.Structs[field.Name]
		if vector == nil || vector.Values == nil {
			return 0, fmt.Errorf("struct field %q is missing", field.Name)
		}
		if err := vector.Validate(field, rows); err != nil {
			return 0, err
		}
		size, err := validitySize(vector.Validity, rows)
		if err != nil {
			return 0, err
		}
		for _, child := range field.Children {
			childSize, err := runFieldSize(vector.Values, child, rows)
			if err != nil {
				return 0, err
			}
			size, err = addRunSizes(size, childSize)
			if err != nil {
				return 0, err
			}
		}
		return size, nil
	}
	if field.Type == TypeList {
		vector := batch.StringLists[field.Name]
		if vector == nil {
			return 0, fmt.Errorf("list field %q is missing", field.Name)
		}
		if err := vector.Validate(rows); err != nil {
			return 0, err
		}
		parent, err := validitySize(vector.Validity, rows)
		if err != nil {
			return 0, err
		}
		childRows := vector.Values.Len()
		child, err := validitySize(vector.Values.Validity, childRows)
		if err != nil {
			return 0, err
		}
		data, err := normalizedOffsetDataSize(vector.Values.Offsets, vector.Values.Data)
		if err != nil {
			return 0, err
		}
		return addRunSizes(parent, 4, int64(len(vector.Offsets))*4, child, 4, int64(len(vector.Values.Offsets))*4, 8, data)
	}
	if field.Type == TypeString || field.Type == TypeBinary {
		if field.Encoding == EncodingDictionary {
			vector := batch.DictStrings[field.Name]
			if vector == nil {
				return 0, fmt.Errorf("dictionary field %q is missing", field.Name)
			}
			if err := vector.Validate(rows); err != nil {
				return 0, err
			}
			validity, err := validitySize(vector.Validity, rows)
			if err != nil {
				return 0, err
			}
			data, err := normalizedOffsetDataSize(vector.Offsets, vector.Data)
			if err != nil {
				return 0, err
			}
			return addRunSizes(validity, int64(rows)*4, 4, int64(len(vector.Offsets))*4, 8, data, 4, int64(len(vector.DictionaryDomain)))
		}
		vector := batch.Strings[field.Name]
		if vector == nil {
			return 0, fmt.Errorf("string field %q is missing", field.Name)
		}
		if err := vector.Validate(rows); err != nil {
			return 0, err
		}
		validity, err := validitySize(vector.Validity, rows)
		if err != nil {
			return 0, err
		}
		data, err := normalizedOffsetDataSize(vector.Offsets, vector.Data)
		if err != nil {
			return 0, err
		}
		return addRunSizes(validity, 4, int64(len(vector.Offsets))*4, 8, data)
	}
	validity, width, err := fixedRunField(batch, field, rows)
	if err != nil {
		return 0, err
	}
	validityBytes, err := validitySize(validity, rows)
	if err != nil {
		return 0, err
	}
	if int64(rows) > math.MaxInt64/int64(width) {
		return 0, fmt.Errorf("fixed-width field %q size overflow", field.Name)
	}
	metadataBytes := int64(0)
	if field.Type == TypeDecimal {
		metadataBytes = 8
	}
	return addRunSizes(validityBytes, metadataBytes, int64(rows)*int64(width))
}

func addRunSizes(parts ...int64) (int64, error) {
	var total int64
	for _, part := range parts {
		if part < 0 || part > math.MaxInt64-total {
			return 0, fmt.Errorf("columnar run field size overflow")
		}
		total += part
	}
	return total, nil
}

func normalizedOffsetDataSize(offsets []uint32, data []byte) (int64, error) {
	if len(offsets) == 0 {
		return 0, fmt.Errorf("offset vector is empty")
	}
	base, end := offsets[0], offsets[len(offsets)-1]
	if base > end || int(end) > len(data) {
		return 0, fmt.Errorf("offset vector exceeds data")
	}
	return int64(end - base), nil
}

func validateRunField(batch *Batch, field Field, rows int) error {
	if batch == nil {
		return fmt.Errorf("field %q has nil batch", field.Name)
	}
	switch field.Type {
	case TypeStruct:
		vector := batch.Structs[field.Name]
		if vector == nil {
			return fmt.Errorf("struct field %q is missing", field.Name)
		}
		if err := vector.Validate(field, rows); err != nil {
			return err
		}
		// Struct.Validate applies child nullability only to rows whose parent is
		// valid. Validate every child's physical shape separately here.
		for _, child := range field.Children {
			shapeOnly := child
			shapeOnly.Nullable = true
			if err := validateRunField(vector.Values, shapeOnly, rows); err != nil {
				return fmt.Errorf("struct field %q child %q: %w", field.Name, child.Name, err)
			}
		}
		return nil
	case TypeList:
		if err := field.ValidateListString(); err != nil {
			return err
		}
		vector := batch.StringLists[field.Name]
		if vector == nil {
			return fmt.Errorf("list field %q is missing", field.Name)
		}
		if err := vector.Validate(rows); err != nil {
			return err
		}
		if !field.Nullable && HasNulls(vector.Validity, rows) {
			return fmt.Errorf("non-nullable list field %q contains NULL", field.Name)
		}
		if !field.Children[0].Nullable {
			for row := 0; row < rows; row++ {
				start, end, ok := vector.ValueRange(row)
				if !ok {
					continue
				}
				for child := start; child < end; child++ {
					if !vector.Values.IsValid(child) {
						return fmt.Errorf("list field %q non-nullable element is NULL at row %d element %d", field.Name, row, child-start)
					}
				}
			}
		}
		return nil
	case TypeString, TypeBinary:
		if field.Encoding == EncodingDictionary {
			vector := batch.DictStrings[field.Name]
			if vector == nil {
				return fmt.Errorf("dictionary field %q is missing", field.Name)
			}
			if err := vector.Validate(rows); err != nil {
				return err
			}
			if _, err := validitySize(vector.Validity, rows); err != nil {
				return err
			}
			if _, err := normalizedOffsetDataSize(vector.Offsets, vector.Data); err != nil {
				return err
			}
			for i := 1; i < len(vector.Offsets); i++ {
				if vector.Offsets[i] < vector.Offsets[i-1] {
					return fmt.Errorf("dictionary field %q offset %d decreases", field.Name, i)
				}
			}
			cardinality := uint32(vector.DictionaryLen())
			for row, index := range vector.Indices {
				if vector.IsValid(row) && index >= cardinality {
					return fmt.Errorf("dictionary field %q row %d index %d exceeds cardinality %d", field.Name, row, index, cardinality)
				}
			}
			if !field.Nullable && HasNulls(vector.Validity, rows) {
				return fmt.Errorf("non-nullable field %q contains NULL", field.Name)
			}
			return nil
		}
		vector := batch.Strings[field.Name]
		if vector == nil {
			return fmt.Errorf("string field %q is missing", field.Name)
		}
		if err := vector.Validate(rows); err != nil {
			return err
		}
		if _, err := validitySize(vector.Validity, rows); err != nil {
			return err
		}
		if !field.Nullable && HasNulls(vector.Validity, rows) {
			return fmt.Errorf("non-nullable field %q contains NULL", field.Name)
		}
		return nil
	default:
		validity, _, err := fixedRunField(batch, field, rows)
		if err != nil {
			return err
		}
		if _, err := validitySize(validity, rows); err != nil {
			return err
		}
		if !field.Nullable && HasNulls(validity, rows) {
			return fmt.Errorf("non-nullable field %q contains NULL", field.Name)
		}
		return nil
	}
}

func fixedRunField(batch *Batch, field Field, rows int) (Bitmap, int, error) {
	switch field.Type {
	case TypeDecimal:
		v := batch.Decimals[field.Name]
		if err := v.ValidateField(field, rows); err != nil {
			return nil, 0, err
		}
		return v.Validity, 16, nil
	case TypeInt8:
		v := batch.Int8s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("int8 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 1, nil
	case TypeInt16:
		v := batch.Int16s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("int16 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 2, nil
	case TypeInt32, TypeDate32, TypeTime32:
		v := batch.Int32s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("int32 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 4, nil
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		v := batch.Int64s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("int64 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 8, nil
	case TypeUint8:
		v := batch.Uint8s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("uint8 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 1, nil
	case TypeUint16:
		v := batch.Uint16s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("uint16 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 2, nil
	case TypeUint32, TypeBool:
		v := batch.Uint32s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("uint32 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 4, nil
	case TypeUint64:
		v := batch.Uint64s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("uint64 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 8, nil
	case TypeFloat32:
		v := batch.Float32s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("float32 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 4, nil
	case TypeFloat64:
		v := batch.Float64s[field.Name]
		if v == nil || len(v.Values) != rows {
			return nil, 0, fmt.Errorf("float64 physical field %q rows mismatch", field.Name)
		}
		return v.Validity, 8, nil
	default:
		return nil, 0, fmt.Errorf("field %q type %s requires an unsupported opaque spill codec", field.Name, field.Type)
	}
}

func encodeRunField(writer *runWriter, batch *Batch, field Field, rows int) error {
	if field.Type == TypeStruct {
		vector := batch.Structs[field.Name]
		encodeValidity(writer, vector.Validity, rows)
		for _, child := range field.Children {
			if err := encodeRunField(writer, vector.Values, child, rows); err != nil {
				return err
			}
		}
		return writer.err
	}
	if field.Type == TypeList {
		vector := batch.StringLists[field.Name]
		encodeValidity(writer, vector.Validity, rows)
		writer.u32(uint32(len(vector.Offsets)))
		encodeOffsets(writer, vector.Offsets, 0)
		encodeValidity(writer, vector.Values.Validity, vector.Values.Len())
		writer.u32(uint32(len(vector.Values.Offsets)))
		encodeNormalizedString(writer, vector.Values.Offsets, vector.Values.Data)
		return writer.err
	}
	if field.Type == TypeString || field.Type == TypeBinary {
		if field.Encoding == EncodingDictionary {
			vector := batch.DictStrings[field.Name]
			encodeValidity(writer, vector.Validity, rows)
			for _, value := range vector.Indices {
				writer.u32(value)
			}
			writer.u32(uint32(len(vector.Offsets)))
			encodeNormalizedString(writer, vector.Offsets, vector.Data)
			writer.string(vector.DictionaryDomain)
			return writer.err
		}
		vector := batch.Strings[field.Name]
		encodeValidity(writer, vector.Validity, rows)
		writer.u32(uint32(len(vector.Offsets)))
		encodeNormalizedString(writer, vector.Offsets, vector.Data)
		return writer.err
	}
	validity, _, err := fixedRunField(batch, field, rows)
	if err != nil {
		return err
	}
	encodeValidity(writer, validity, rows)
	switch field.Type {
	case TypeDecimal:
		writer.u32(uint32(field.DecimalPrecision))
		writer.u32(uint32(field.DecimalScale))
		for _, value := range batch.Decimals[field.Name].Values {
			writer.u64(value.LowBits())
			writer.u64(uint64(value.HighBits()))
		}
	case TypeInt8:
		for _, value := range batch.Int8s[field.Name].Values {
			writer.u8(uint8(value))
		}
	case TypeInt16:
		for _, value := range batch.Int16s[field.Name].Values {
			writer.u16(uint16(value))
		}
	case TypeInt32, TypeDate32, TypeTime32:
		for _, value := range batch.Int32s[field.Name].Values {
			writer.u32(uint32(value))
		}
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		for _, value := range batch.Int64s[field.Name].Values {
			writer.u64(uint64(value))
		}
	case TypeUint8:
		for _, value := range batch.Uint8s[field.Name].Values {
			writer.u8(value)
		}
	case TypeUint16:
		for _, value := range batch.Uint16s[field.Name].Values {
			writer.u16(value)
		}
	case TypeUint32, TypeBool:
		for _, value := range batch.Uint32s[field.Name].Values {
			writer.u32(value)
		}
	case TypeUint64:
		for _, value := range batch.Uint64s[field.Name].Values {
			writer.u64(value)
		}
	case TypeFloat32:
		for _, value := range batch.Float32s[field.Name].Values {
			writer.u32(math.Float32bits(value))
		}
	case TypeFloat64:
		for _, value := range batch.Float64s[field.Name].Values {
			writer.u64(math.Float64bits(value))
		}
	}
	return writer.err
}

func encodeValidity(writer *runWriter, validity Bitmap, rows int) {
	if validity == nil {
		writer.u32(0)
		return
	}
	words := WordsFor(rows)
	writer.u32(uint32(words))
	for _, word := range validity[:words] {
		writer.u64(word)
	}
}

func encodeOffsets(writer *runWriter, offsets []uint32, base uint32) {
	for _, offset := range offsets {
		writer.u32(offset - base)
	}
}

func encodeNormalizedString(writer *runWriter, offsets []uint32, data []byte) {
	base, end := offsets[0], offsets[len(offsets)-1]
	encodeOffsets(writer, offsets, base)
	writer.u64(uint64(end - base))
	writer.bytes(data[base:end])
}

func decodeRunField(reader *runReader, batch *Batch, field Field, rows int) error {
	if field.Type == TypeStruct {
		validity := decodeValidity(reader, rows)
		values := NewBatch()
		values.Rows = rows
		for _, child := range field.Children {
			if err := decodeRunField(reader, values, child, rows); err != nil {
				return err
			}
		}
		batch.Structs[field.Name] = &StructVector{Rows: rows, Fields: append([]Field(nil), field.Children...), Values: values, Validity: validity, Encoding: field.Encoding}
		return reader.err
	}
	if field.Type == TypeList {
		validity := decodeValidity(reader, rows)
		offsets := reader.offsets(reader.u32(), rows+1)
		childRows := 0
		if len(offsets) != 0 {
			last := offsets[len(offsets)-1]
			if uint64(last) > uint64(math.MaxInt) {
				reader.fail("list child row count exceeds addressable memory")
			} else {
				childRows = int(last)
			}
		}
		childValidity := decodeValidity(reader, childRows)
		childOffsets := reader.offsets(reader.u32(), childRows+1)
		data := reader.data()
		batch.StringLists[field.Name] = &StringListVector{Offsets: offsets, Validity: validity, Values: &StringViewVector{Offsets: childOffsets, Data: data, Validity: childValidity, Encoding: EncodingView}, Encoding: field.Encoding}
		return reader.err
	}
	if field.Type == TypeString || field.Type == TypeBinary {
		validity := decodeValidity(reader, rows)
		if field.Encoding == EncodingDictionary {
			if !reader.requireArray(rows, 4, "dictionary indices exceed remaining payload") {
				return reader.err
			}
			indices := make([]uint32, rows)
			for i := range indices {
				indices[i] = reader.u32()
			}
			offsets := reader.offsets(reader.u32(), -1)
			data := reader.data()
			domain := reader.string()
			batch.DictStrings[field.Name] = &DictionaryStringVector{Indices: indices, Validity: validity, Offsets: offsets, Data: data, DictionaryDomain: domain, Encoding: field.Encoding}
			return reader.err
		}
		offsets := reader.offsets(reader.u32(), rows+1)
		data := reader.data()
		batch.Strings[field.Name] = &StringViewVector{Offsets: offsets, Data: data, Validity: validity, Encoding: field.Encoding}
		return reader.err
	}
	validity := decodeValidity(reader, rows)
	switch field.Type {
	case TypeDecimal:
		if err := field.ValidateDecimal128(); err != nil {
			return fmt.Errorf("%w: %v", ErrSpillCorrupt, err)
		}
		if int32(reader.u32()) != field.DecimalPrecision || int32(reader.u32()) != field.DecimalScale {
			return fmt.Errorf("%w: decimal precision/scale mismatch", ErrSpillCorrupt)
		}
		if !reader.requireArray(rows, 16, "decimal values exceed remaining payload") {
			return reader.err
		}
		values := make([]Decimal128, rows)
		for i := range values {
			lo := reader.u64()
			hi := reader.u64()
			values[i] = decimal128.New(int64(hi), lo)
		}
		batch.Decimals[field.Name] = &Decimal128Vector{Values: values, Validity: validity, Precision: field.DecimalPrecision, Scale: field.DecimalScale}
	case TypeInt8:
		if !reader.requireArray(rows, 1, "int8 values exceed remaining payload") {
			return reader.err
		}
		values := make([]int8, rows)
		for i := range values {
			values[i] = int8(reader.u8())
		}
		batch.Int8s[field.Name] = &Int8Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeInt16:
		if !reader.requireArray(rows, 2, "int16 values exceed remaining payload") {
			return reader.err
		}
		values := make([]int16, rows)
		for i := range values {
			values[i] = int16(reader.u16())
		}
		batch.Int16s[field.Name] = &Int16Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeInt32, TypeDate32, TypeTime32:
		if !reader.requireArray(rows, 4, "int32 values exceed remaining payload") {
			return reader.err
		}
		values := make([]int32, rows)
		for i := range values {
			values[i] = int32(reader.u32())
		}
		batch.Int32s[field.Name] = &Int32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeInt64, TypeDate64, TypeTimestamp, TypeTime64, TypeDuration:
		if !reader.requireArray(rows, 8, "int64 values exceed remaining payload") {
			return reader.err
		}
		values := make([]int64, rows)
		for i := range values {
			values[i] = int64(reader.u64())
		}
		batch.Int64s[field.Name] = &Int64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeUint8:
		if !reader.requireArray(rows, 1, "uint8 values exceed remaining payload") {
			return reader.err
		}
		values := make([]uint8, rows)
		for i := range values {
			values[i] = reader.u8()
		}
		batch.Uint8s[field.Name] = &Uint8Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeUint16:
		if !reader.requireArray(rows, 2, "uint16 values exceed remaining payload") {
			return reader.err
		}
		values := make([]uint16, rows)
		for i := range values {
			values[i] = reader.u16()
		}
		batch.Uint16s[field.Name] = &Uint16Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeUint32, TypeBool:
		if !reader.requireArray(rows, 4, "uint32 values exceed remaining payload") {
			return reader.err
		}
		values := make([]uint32, rows)
		for i := range values {
			values[i] = reader.u32()
		}
		batch.Uint32s[field.Name] = &Uint32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeUint64:
		if !reader.requireArray(rows, 8, "uint64 values exceed remaining payload") {
			return reader.err
		}
		values := make([]uint64, rows)
		for i := range values {
			values[i] = reader.u64()
		}
		batch.Uint64s[field.Name] = &Uint64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeFloat32:
		if !reader.requireArray(rows, 4, "float32 values exceed remaining payload") {
			return reader.err
		}
		values := make([]float32, rows)
		for i := range values {
			values[i] = math.Float32frombits(reader.u32())
		}
		batch.Float32s[field.Name] = &Float32Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	case TypeFloat64:
		if !reader.requireArray(rows, 8, "float64 values exceed remaining payload") {
			return reader.err
		}
		values := make([]float64, rows)
		for i := range values {
			values[i] = math.Float64frombits(reader.u64())
		}
		batch.Float64s[field.Name] = &Float64Vector{Values: values, Validity: validity, Encoding: field.Encoding}
	default:
		return fmt.Errorf("%w: unsupported columnar field %q type %s", ErrSpillCorrupt, field.Name, field.Type)
	}
	return reader.err
}

func decodeValidity(reader *runReader, rows int) Bitmap {
	words := int(reader.u32())
	if words == 0 {
		return nil
	}
	if words != WordsFor(rows) {
		reader.fail("validity word count mismatch")
		return nil
	}
	if !reader.requireArray(words, 8, "validity bitmap exceeds remaining payload") {
		return nil
	}
	validity := make(Bitmap, words)
	for i := range validity {
		validity[i] = reader.u64()
	}
	return validity
}

type runWriter struct {
	buf []byte
	pos int
	err error
}

func (w *runWriter) bytes(value []byte) {
	if w.err != nil || len(value) > len(w.buf)-w.pos {
		w.err = fmt.Errorf("columnar encoder buffer overflow")
		return
	}
	copy(w.buf[w.pos:], value)
	w.pos += len(value)
}

func (w *runWriter) u8(value uint8) {
	if w.err != nil || len(w.buf)-w.pos < 1 {
		w.err = fmt.Errorf("columnar encoder buffer overflow")
		return
	}
	w.buf[w.pos] = value
	w.pos++
}
func (w *runWriter) u16(value uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	w.bytes(buf[:])
}
func (w *runWriter) u32(value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	w.bytes(buf[:])
}
func (w *runWriter) u64(value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	w.bytes(buf[:])
}
func (w *runWriter) string(value string) {
	w.u32(uint32(len(value)))
	w.bytes([]byte(value))
}

type runReader struct {
	buf []byte
	pos int
	err error
}

func (r *runReader) fail(detail string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrSpillCorrupt, detail)
	}
}
func (r *runReader) bytes(length int) []byte {
	if r.err != nil || length < 0 || length > len(r.buf)-r.pos {
		r.fail("columnar run truncated")
		return nil
	}
	out := r.buf[r.pos : r.pos+length]
	r.pos += length
	return out
}

func (r *runReader) u8() uint8 {
	if r.err != nil || len(r.buf)-r.pos < 1 {
		r.fail("uint8 exceeds remaining payload")
		return 0
	}
	v := r.buf[r.pos]
	r.pos++
	return v
}
func (r *runReader) u16() uint16 {
	value := r.bytes(2)
	if len(value) != 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(value)
}
func (r *runReader) u32() uint32 {
	value := r.bytes(4)
	if len(value) != 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(value)
}
func (r *runReader) u64() uint64 {
	value := r.bytes(8)
	if len(value) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(value)
}
func (r *runReader) offsets(count uint32, want int) []uint32 {
	if uint64(count) > uint64(math.MaxInt) || want >= 0 && int(count) != want {
		r.fail("columnar offset count mismatch")
		return nil
	}
	if !r.requireArray(int(count), 4, "columnar offsets exceed remaining payload") {
		return nil
	}
	offsets := make([]uint32, int(count))
	for i := range offsets {
		offsets[i] = r.u32()
		if i > 0 && offsets[i] < offsets[i-1] {
			r.fail("columnar offsets decrease")
		}
	}
	return offsets
}
func (r *runReader) requireArray(count, width int, detail string) bool {
	if r.err != nil {
		return false
	}
	if count < 0 || width <= 0 || count > (len(r.buf)-r.pos)/width {
		r.fail(detail)
		return false
	}
	return true
}
func (r *runReader) data() []byte {
	length := r.u64()
	if length > uint64(math.MaxInt) {
		r.fail("columnar data exceeds addressable memory")
		return nil
	}
	return append([]byte(nil), r.bytes(int(length))...)
}
func (r *runReader) string() string {
	length := r.u32()
	if uint64(length) > uint64(math.MaxInt) {
		r.fail("columnar string exceeds addressable memory")
		return ""
	}
	return string(r.bytes(int(length)))
}
