package parquet

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"os"
	"time"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type columnBinding struct {
	Name     string
	Index    int
	Type     memory.DataType
	Nullable bool
	// TimestampScale converts the physical int64 timestamp unit to UTC
	// nanoseconds. It is one for ordinary int64 and nanosecond timestamps.
	TimestampScale   int64
	DecimalPrecision int32
	DecimalScale     int32
}

// Source exposes projected flat Parquet columns as row-group partitions. It
// supports signed and unsigned integers, floats, booleans, UTF-8 strings, dates, and UTC
// timestamps. Pushdown clones are immutable and safe to prepare independently
// from the same base source.
type Source struct {
	path        string
	meta        *metadata.FileMetaData
	baseSchema  memory.Schema
	schema      memory.Schema
	allBindings []columnBinding
	bindings    []columnBinding
	allParts    []source.Partition
	parts       []source.Partition
	fileBytes   int64
	stats       source.Stats
	report      source.PushdownReport
}

// PlanIdentity is stable across immutable row-group/projection pushdown clones.
func (s *Source) PlanIdentity() string { return fmt.Sprintf("parquet:%p", s.meta) }

// OpenFile discovers a flat primitive Parquet schema and exposes supported
// integer, float, boolean, UTF-8 string, date, and UTC timestamp columns.
// Repeated/nested columns and unsupported physical or logical types are
// rejected rather than silently decoded incorrectly. DECIMAL precision 1..38
// and nonnegative scale are decoded exactly to decimal128; TIME is unsupported.
func OpenFile(path string) (*Source, error) {
	base, err := openMetadata(path)
	if err != nil {
		return nil, err
	}
	cols := base.meta.Schema.NumColumns()
	bindings := make([]columnBinding, 0, cols)
	fields := make([]memory.Field, 0, cols)
	for i := 0; i < cols; i++ {
		d := base.meta.Schema.Column(i)
		if d.MaxRepetitionLevel() != 0 || d.MaxDefinitionLevel() > 1 {
			return nil, fmt.Errorf("parquet column %q is nested/repeated; phase-2.3 lazy scan supports flat primitive columns", d.Path())
		}
		name := d.Path()
		b, err := bindingAt(base.meta, i, name)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, b)
		fields = append(fields, fieldForBinding(b))
	}
	base.allBindings = bindings
	base.bindings = append([]columnBinding(nil), bindings...)
	base.baseSchema = memory.NewSchema(fields...)
	base.schema = base.baseSchema
	return base, nil
}

func openMetadata(path string) (*Source, error) {
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, err
	}
	meta := r.MetaData()
	parts := make([]source.Partition, r.NumRowGroups())
	var rows int64
	for i := 0; i < r.NumRowGroups(); i++ {
		n := r.RowGroup(i).MetaData().NumRows()
		parts[i] = source.Partition{ID: i, Rows: n}
		rows += n
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &Source{
		path:      path,
		meta:      meta,
		allParts:  append([]source.Partition(nil), parts...),
		parts:     append([]source.Partition(nil), parts...),
		fileBytes: st.Size(),
		stats:     source.Stats{Rows: rows, Partitions: len(parts), Bytes: st.Size()},
	}, nil
}

func bindingAt(meta *metadata.FileMetaData, index int, logicalName string) (columnBinding, error) {
	if index < 0 || index >= meta.Schema.NumColumns() {
		return columnBinding{}, fmt.Errorf("parquet column index %d out of range", index)
	}
	d := meta.Schema.Column(index)
	binding := columnBinding{Name: logicalName, Index: index, Nullable: d.MaxDefinitionLevel() > 0, TimestampScale: 1}
	// Physical storage is not a logical type: DECIMAL stores unscaled integers,
	// TIME has units/UTC semantics, and UNKNOWN denotes nulls. Only annotations
	// whose semantics this native reader preserves may reach the decoder. Arrow
	// also normalizes legacy converted types into these logical types.
	switch d.LogicalType().(type) {
	case schema.NoLogicalType, schema.IntLogicalType, schema.StringLogicalType, schema.DateLogicalType, schema.TimestampLogicalType:
	case schema.DecimalLogicalType:
		decimal := d.LogicalType().(schema.DecimalLogicalType)

		if _, err := memory.Decimal128Field(logicalName, binding.Nullable, decimal.Precision(), decimal.Scale()); err != nil {
			return columnBinding{}, fmt.Errorf("parquet column %q logical type %s is not supported: %w", d.Path(), d.LogicalType(), err)
		}
		switch d.PhysicalType() {
		case arrowparquet.Types.Int32:
			if decimal.Precision() > 9 {
				return columnBinding{}, fmt.Errorf("parquet decimal column %q INT32 precision exceeds 9", d.Path())
			}
		case arrowparquet.Types.Int64:
			if decimal.Precision() > 18 {
				return columnBinding{}, fmt.Errorf("parquet decimal column %q INT64 precision exceeds 18", d.Path())
			}
		case arrowparquet.Types.ByteArray:
		case arrowparquet.Types.FixedLenByteArray:
			// Exact Parquet precision capacities for widths below decimal128.
			capacities := [...]int32{0, 2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35}
			length := d.TypeLength()
			if length < 1 || length < len(capacities) && decimal.Precision() > capacities[length] {
				return columnBinding{}, fmt.Errorf("parquet decimal column %q precision %d is incompatible with fixed width %d", d.Path(), decimal.Precision(), length)
			}
		default:
			return columnBinding{}, fmt.Errorf("parquet decimal column %q physical type %s is not supported", d.Path(), d.PhysicalType())
		}
		if err := validateDecimalPageEncodings(meta, index); err != nil {
			return columnBinding{}, err
		}
		binding.Type = memory.TypeDecimal
		binding.DecimalPrecision, binding.DecimalScale = decimal.Precision(), decimal.Scale()
		return binding, nil
	default:
		return columnBinding{}, fmt.Errorf("parquet column %q logical type %s is not supported by native scan", d.Path(), d.LogicalType())
	}
	switch d.PhysicalType() {
	case arrowparquet.Types.Boolean:
		binding.Type = memory.TypeBool
	case arrowparquet.Types.Float:
		binding.Type = memory.TypeFloat32
	case arrowparquet.Types.Double:
		binding.Type = memory.TypeFloat64
	case arrowparquet.Types.Int32:
		if _, ok := d.LogicalType().(schema.DateLogicalType); ok {
			binding.Type = memory.TypeDate32
		} else if integer, ok := d.LogicalType().(schema.IntLogicalType); ok {
			switch integer.BitWidth() {
			case 8:
				if integer.IsSigned() {
					binding.Type = memory.TypeInt8
				} else {
					binding.Type = memory.TypeUint8
				}
			case 16:
				if integer.IsSigned() {
					binding.Type = memory.TypeInt16
				} else {
					binding.Type = memory.TypeUint16
				}
			case 32:
				if integer.IsSigned() {
					binding.Type = memory.TypeInt32
				} else {
					binding.Type = memory.TypeUint32
				}
			default:
				return columnBinding{}, fmt.Errorf("parquet column %q integer width %d is not supported for INT32", d.Path(), integer.BitWidth())
			}
		} else {
			binding.Type = memory.TypeInt32
		}
	case arrowparquet.Types.Int64:
		if timestamp, ok := d.LogicalType().(schema.TimestampLogicalType); ok {
			if !timestamp.IsAdjustedToUTC() {
				return columnBinding{}, fmt.Errorf("parquet timestamp column %q is not UTC-adjusted", d.Path())
			}
			scale, err := parquetTimestampScale(timestamp.TimeUnit())
			if err != nil {
				return columnBinding{}, fmt.Errorf("parquet timestamp column %q: %w", d.Path(), err)
			}
			binding.Type = memory.TypeTimestamp
			binding.TimestampScale = scale
		} else if integer, ok := d.LogicalType().(schema.IntLogicalType); ok && !integer.IsSigned() {
			binding.Type = memory.TypeUint64
		} else {
			binding.Type = memory.TypeInt64
		}
	case arrowparquet.Types.ByteArray:
		if _, ok := d.LogicalType().(schema.StringLogicalType); !ok {
			return columnBinding{}, fmt.Errorf("parquet byte array column %q is not UTF-8 STRING", d.Path())
		}
		binding.Type = memory.TypeString
	default:
		return columnBinding{}, fmt.Errorf("parquet column %q physical type %s is not supported by lazy scan", d.Path(), d.PhysicalType())
	}
	return binding, nil
}

func fieldForBinding(b columnBinding) memory.Field {
	if b.Type == memory.TypeDecimal {
		return memory.Field{Name: b.Name, Type: b.Type, Nullable: b.Nullable, Encoding: memory.EncodingPlain, DecimalPrecision: b.DecimalPrecision, DecimalScale: b.DecimalScale}
	}
	encoding := memory.EncodingPlain
	if b.Type == memory.TypeString {
		encoding = memory.EncodingView
	}
	return memory.Field{Name: b.Name, Type: b.Type, Nullable: b.Nullable, Encoding: encoding}
}

func parquetTimestampScale(unit schema.TimeUnitType) (int64, error) {
	switch unit {
	case schema.TimeUnitMillis:
		return int64(1e6), nil
	case schema.TimeUnitMicros:
		return int64(1e3), nil
	case schema.TimeUnitNanos:
		return 1, nil
	default:
		return 0, fmt.Errorf("timestamp unit %d is not supported", unit)
	}
}

// Description returns a concise source label used by explain output.
func (s *Source) Description() string { return "parquet(" + s.path + ")" }

// Schema returns the logical schema exposed by the current projection.
func (s *Source) Schema() memory.Schema { return s.schema }

// Stats returns row/partition counts after any row-group pruning.
func (s *Source) Stats(context.Context) (source.Stats, error) { return s.stats, nil }

// Partitions returns independently readable retained row groups.
func (s *Source) Partitions(context.Context) ([]source.Partition, error) {
	out := make([]source.Partition, len(s.parts))
	copy(out, s.parts)
	return out, nil
}

// ApplyPushdown returns an immutable source clone that reads only requested
// columns and prunes row groups that cannot make Predicate SQL TRUE according
// to Parquet min/max/null-count statistics. The row-level predicate must still
// be evaluated by the engine for every retained row group.
func (s *Source) ApplyPushdown(_ context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	if s == nil {
		return nil, source.PushdownReport{}, fmt.Errorf("nil parquet source")
	}
	// Metadata can prove a predicate never TRUE while an earlier evaluated
	// subexpression still errors. Keep all row groups for a fallible predicate;
	// the residual evaluator is responsible for its ordered mask semantics.
	pruningPredicate := req.Predicate
	var predicateColumns []string
	if req.Predicate != nil {
		bound, err := expr.Bind(s.baseSchema, req.Predicate)
		if err != nil {
			return nil, source.PushdownReport{}, fmt.Errorf("pushdown predicate: %w", err)
		}
		if bound.Field.Type != memory.TypeBool {
			return nil, source.PushdownReport{}, fmt.Errorf("pushdown predicate must be bool, got %s", bound.Field.Type)
		}
		predicateColumns = bound.Columns
		if bound.MayError {
			pruningPredicate = nil
		}
	}
	projection := append([]string(nil), req.Projection...)
	if len(projection) == 0 {
		projection = schemaNames(s.baseSchema)
	}
	// Predicate columns are required for the residual row-level filter even if a
	// caller constructed a malformed projection request by hand.
	need := make(map[string]struct{}, len(projection))
	for _, name := range projection {
		need[name] = struct{}{}
	}
	for _, name := range predicateColumns {
		need[name] = struct{}{}
	}
	projection = orderedNames(s.baseSchema, need)
	if len(projection) != len(need) {
		return nil, source.PushdownReport{}, fmt.Errorf("pushdown references a column not present in parquet source")
	}

	byName := make(map[string]columnBinding, len(s.allBindings))
	for _, b := range s.allBindings {
		byName[b.Name] = b
	}
	bindings := make([]columnBinding, 0, len(projection))
	fields := make([]memory.Field, 0, len(projection))
	for _, name := range projection {
		b, ok := byName[name]
		if !ok {
			return nil, source.PushdownReport{}, fmt.Errorf("projection column %q not found", name)
		}
		bindings = append(bindings, b)
		f, _ := s.baseSchema.Field(name)
		fields = append(fields, f)
	}

	parts := make([]source.Partition, 0, len(s.allParts))
	var selectedRows int64
	var selectedBytes int64
	var totalChunkBytes int64
	for _, p := range s.allParts {
		rg := s.meta.RowGroup(p.ID)
		for _, b := range s.allBindings {
			chunk, err := rg.ColumnChunk(b.Index)
			if err != nil {
				return nil, source.PushdownReport{}, err
			}
			totalChunkBytes += chunk.TotalCompressedSize()
		}

		keep, err := rowGroupMayMatch(s.meta, p.ID, pruningPredicate, byName)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		if !keep {
			continue
		}
		parts = append(parts, p)
		selectedRows += p.Rows
		for _, b := range bindings {
			chunk, err := rg.ColumnChunk(b.Index)
			if err != nil {
				return nil, source.PushdownReport{}, err
			}
			selectedBytes += chunk.TotalCompressedSize()
		}
	}
	var totalRows int64
	for _, p := range s.allParts {
		totalRows += p.Rows
	}
	report := source.PushdownReport{
		Projection:         append([]string(nil), projection...),
		TotalPartitions:    len(s.allParts),
		SelectedPartitions: len(parts),
		PrunedPartitions:   len(s.allParts) - len(parts),
		TotalRows:          totalRows,
		SelectedRows:       selectedRows,
		PrunedRows:         totalRows - selectedRows,
		TotalBytes:         totalChunkBytes,
		SelectedBytes:      selectedBytes,
		PrunedBytes:        totalChunkBytes - selectedBytes,
	}
	if req.Predicate != nil {
		report.Predicate = req.Predicate.String()
	}
	out := *s
	out.bindings = bindings
	out.schema = memory.NewSchema(fields...)
	out.parts = parts
	out.stats = source.Stats{Rows: selectedRows, Partitions: len(parts), Bytes: selectedBytes}
	out.report = report
	return &out, report, nil
}

func schemaNames(schema memory.Schema) []string {
	out := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		out[i] = f.Name
	}
	return out
}

func orderedNames(schema memory.Schema, names map[string]struct{}) []string {
	out := make([]string, 0, len(names))
	for _, f := range schema.Fields {
		if _, ok := names[f.Name]; ok {
			out = append(out, f.Name)
		}
	}
	return out
}

// typedDecoder holds only pre-reserved scratch for native batch conversion.
// Its methods only slice and fill buffers installed by allocateFixed; they
// cannot create an eager reader or grow vectors outside stream reservations.
type typedDecoder struct {
	kernels   kernel.Set
	dense64   []int64
	dense32   []int32
	denseF32  []float32
	denseF64  []float64
	denseBool []bool
	defs      []int16
	// Inline decimal lookahead is bounded decoder metadata, not row-scaled scratch.
	decimalI32   [1]int32
	decimalI64   [1]int64
	decimalBytes [1]arrowparquet.ByteArray
	decimalFixed [1]arrowparquet.FixedLenByteArray
	decimalDef   [1]int16
}

// scatterNullable fills already reserved vectors and writes each validity
// word once. A local mask avoids one bitmap read/modify/write per non-NULL
// value, and overwrites both NULL bits and the final word's unused tail.
func scatterNullable[T any](out, dense []T, defs []int16, validity memory.Bitmap, count int, column string) error {
	if count < 0 || count > len(dense) {
		return fmt.Errorf("column %q invalid dense count %d", column, count)
	}
	physical := dense[:count]
	next := 0
	var zero T
	for base := 0; base < len(out); base += 64 {
		var mask uint64
		end := min(base+64, len(out))
		outputWord, definitionWord := out[base:end], defs[base:end]
		for row, bit := 0, uint64(1); row < len(outputWord); row, bit = row+1, bit<<1 {
			if definitionWord[row] == 0 {
				outputWord[row] = zero
				continue
			}
			if next >= len(physical) {
				return fmt.Errorf("column %q dense overflow", column)
			}
			outputWord[row] = physical[next]
			next++
			mask |= bit
		}
		validity[base>>6] = mask
	}
	if next != count {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", column, next, count)
	}
	return nil
}

func (r *typedDecoder) readInt64(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Int64s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	c, ok := chunk.(*file.Int64ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not an int64 chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		total, values, err := c.ReadBatch(int64(rows), v.Values, nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		return normalizeTimestampColumn(b, v.Values, nil, r.kernels)
	}
	r.dense64 = r.dense64[:rows]
	r.defs = r.defs[:rows]
	v.Validity = v.Validity[:memory.WordsFor(rows)]
	total, values, err := c.ReadBatch(int64(rows), r.dense64[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	if err := scatterNullable(v.Values, r.dense64, r.defs, v.Validity, values, b.Name); err != nil {
		return err
	}
	return normalizeTimestampColumn(b, v.Values, v.Validity, r.kernels)
}

func (r *typedDecoder) readInt32(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	if b.Type != memory.TypeInt32 && b.Type != memory.TypeDate32 {
		return r.readNarrowInt32(chunk, b, rows, dst)
	}
	v := dst.Int32s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	c, ok := chunk.(*file.Int32ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not an int32 chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		total, values, err := c.ReadBatch(int64(rows), v.Values, nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		return nil
	}
	r.dense32 = r.dense32[:rows]
	r.defs = r.defs[:rows]
	v.Validity = v.Validity[:memory.WordsFor(rows)]
	total, values, err := c.ReadBatch(int64(rows), r.dense32[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	if err := scatterNullable(v.Values, r.dense32, r.defs, v.Validity, values, b.Name); err != nil {
		return err
	}
	return nil
}

// scatterUnsigned preserves the physical integer bit pattern, including the
// sign bit, without a float64 or signed widening intermediate. Scratch and
// output capacity are both reserved by the batch stream.
func scatterUnsigned[U uint32 | uint64, S int32 | int64](out []U, dense []S, defs []int16, validity memory.Bitmap, count int, column string) error {
	if count < 0 || count > len(dense) {
		return fmt.Errorf("column %q invalid dense count %d", column, count)
	}
	if validity == nil {
		if count != len(out) {
			return fmt.Errorf("column %q dense mismatch got=%d want=%d", column, count, len(out))
		}
		for i, value := range dense[:count] {
			out[i] = U(value)
		}
		return nil
	}
	clear(validity)
	next := 0
	for i := range out {
		if defs[i] == 0 {
			out[i] = 0
			continue
		}
		if next >= count {
			return fmt.Errorf("column %q dense overflow", column)
		}
		out[i] = U(dense[next])
		next++
		memory.Set(validity, i)
	}
	if next != count {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", column, next, count)
	}
	return nil
}

func (r *typedDecoder) readUint32(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	c, ok := chunk.(*file.Int32ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not an int32 chunk reader", b.Name)
	}
	v := dst.Uint32s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	var defs []int16
	if b.Nullable {
		defs = r.defs[:rows]
		v.Validity = v.Validity[:memory.WordsFor(rows)]
	} else {
		v.Validity = nil
	}
	dense := r.dense32[:rows]
	total, values, err := c.ReadBatch(int64(rows), dense, defs, nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	return scatterUnsigned(v.Values, dense, defs, v.Validity, values, b.Name)
}

func (r *typedDecoder) readUint64(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	c, ok := chunk.(*file.Int64ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not an int64 chunk reader", b.Name)
	}
	v := dst.Uint64s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	var defs []int16
	if b.Nullable {
		defs = r.defs[:rows]
		v.Validity = v.Validity[:memory.WordsFor(rows)]
	} else {
		v.Validity = nil
	}
	dense := r.dense64[:rows]
	total, values, err := c.ReadBatch(int64(rows), dense, defs, nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	return scatterUnsigned(v.Values, dense, defs, v.Validity, values, b.Name)
}

func (r *typedDecoder) readNarrowInt32(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	c, ok := chunk.(*file.Int32ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not an int32 chunk reader", b.Name)
	}
	r.dense32 = r.dense32[:rows]
	r.defs = r.defs[:rows]
	var validity memory.Bitmap
	switch b.Type {
	case memory.TypeInt8:
		if v := dst.Int8s[b.Name]; v != nil {
			validity = v.Validity
		}
	case memory.TypeInt16:
		if v := dst.Int16s[b.Name]; v != nil {
			validity = v.Validity
		}
	case memory.TypeUint8:
		if v := dst.Uint8s[b.Name]; v != nil {
			validity = v.Validity
		}
	case memory.TypeUint16:
		if v := dst.Uint16s[b.Name]; v != nil {
			validity = v.Validity
		}
	}
	if b.Nullable {
		validity = validity[:memory.WordsFor(rows)]
		clear(validity)
	} else {
		validity = nil
	}
	var total int64
	var values int
	var err error
	if b.Nullable {
		total, values, err = c.ReadBatch(int64(rows), r.dense32[:rows], r.defs[:rows], nil)
	} else {
		total, values, err = c.ReadBatch(int64(rows), r.dense32[:rows], nil, nil)
	}
	if err != nil {
		return err
	}
	if int(total) != rows || values != rows && !b.Nullable {
		return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
	}
	put := func(i int, dense int32) {
		if validity != nil {
			memory.Set(validity, i)
		}
		switch b.Type {
		case memory.TypeInt8:
			dst.Int8s[b.Name].Values[i] = int8(dense)
		case memory.TypeInt16:
			dst.Int16s[b.Name].Values[i] = int16(dense)
		case memory.TypeUint8:
			dst.Uint8s[b.Name].Values[i] = uint8(dense)
		case memory.TypeUint16:
			dst.Uint16s[b.Name].Values[i] = uint16(dense)
		}
	}
	switch b.Type {
	case memory.TypeInt8:
		v := dst.Int8s[b.Name]
		v.Values = v.Values[:rows]
		clear(v.Values)
		v.Validity = validity
		v.Encoding = memory.EncodingPlain
	case memory.TypeInt16:
		v := dst.Int16s[b.Name]
		v.Values = v.Values[:rows]
		clear(v.Values)
		v.Validity = validity
		v.Encoding = memory.EncodingPlain
	case memory.TypeUint8:
		v := dst.Uint8s[b.Name]
		v.Values = v.Values[:rows]
		clear(v.Values)
		v.Validity = validity
		v.Encoding = memory.EncodingPlain
	case memory.TypeUint16:
		v := dst.Uint16s[b.Name]
		v.Values = v.Values[:rows]
		clear(v.Values)
		v.Validity = validity
		v.Encoding = memory.EncodingPlain
	}
	dense := 0
	for i := 0; i < rows; i++ {
		if b.Nullable && r.defs[i] == 0 {
			continue
		}
		if dense >= values {
			return fmt.Errorf("column %q dense overflow", b.Name)
		}
		raw := r.dense32[dense]
		if (b.Type == memory.TypeInt8 && (raw < math.MinInt8 || raw > math.MaxInt8)) || (b.Type == memory.TypeInt16 && (raw < math.MinInt16 || raw > math.MaxInt16)) || (b.Type == memory.TypeUint8 && (raw < 0 || raw > math.MaxUint8)) || (b.Type == memory.TypeUint16 && (raw < 0 || raw > math.MaxUint16)) {
			return fmt.Errorf("column %q value %d exceeds logical width %s", b.Name, raw, b.Type)
		}
		put(i, raw)
		dense++
	}
	if dense != values {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", b.Name, dense, values)
	}
	return nil
}

func (r *typedDecoder) readFloat32(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Float32s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	c, ok := chunk.(*file.Float32ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not a float32 chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		total, values, err := c.ReadBatch(int64(rows), v.Values, nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		return nil
	}
	r.denseF32 = r.denseF32[:rows]
	r.defs = r.defs[:rows]
	v.Validity = v.Validity[:memory.WordsFor(rows)]
	total, values, err := c.ReadBatch(int64(rows), r.denseF32[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	if err := scatterNullable(v.Values, r.denseF32, r.defs, v.Validity, values, b.Name); err != nil {
		return err
	}
	return nil
}

func (r *typedDecoder) readFloat64(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Float64s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	c, ok := chunk.(*file.Float64ColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not a float64 chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		total, values, err := c.ReadBatch(int64(rows), v.Values, nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		return nil
	}
	r.denseF64 = r.denseF64[:rows]
	r.defs = r.defs[:rows]
	v.Validity = v.Validity[:memory.WordsFor(rows)]
	total, values, err := c.ReadBatch(int64(rows), r.denseF64[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	if err := scatterNullable(v.Values, r.denseF64, r.defs, v.Validity, values, b.Name); err != nil {
		return err
	}
	return nil
}

// readBool decodes a Parquet boolean column into the engine bool physical
// representation: a uint32 vector of 0/1 values under a TypeBool schema field.
func (r *typedDecoder) readBool(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Uint32s[b.Name]
	v.Values = v.Values[:rows]
	v.Encoding = memory.EncodingPlain
	c, ok := chunk.(*file.BooleanColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not a boolean chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		r.denseBool = r.denseBool[:rows]
		total, values, err := c.ReadBatch(int64(rows), r.denseBool[:rows], nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		for i, value := range r.denseBool[:rows] {
			if value {
				v.Values[i] = 1
			} else {
				v.Values[i] = 0
			}
		}
		return nil
	}
	r.denseBool = r.denseBool[:rows]
	r.defs = r.defs[:rows]
	v.Validity = v.Validity[:memory.WordsFor(rows)]
	total, values, err := c.ReadBatch(int64(rows), r.denseBool[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	if values < 0 || values > len(r.denseBool) {
		return fmt.Errorf("column %q invalid dense count %d", b.Name, values)
	}
	physical := r.denseBool[:values]
	dense := 0
	for base := 0; base < rows; base += 64 {
		var mask uint64
		end := min(base+64, rows)
		outputWord, definitionWord := v.Values[base:end], r.defs[base:end]
		for i, bit := 0, uint64(1); i < len(outputWord); i, bit = i+1, bit<<1 {
			if definitionWord[i] == 0 {
				outputWord[i] = 0
				continue
			}
			if dense >= len(physical) {
				return fmt.Errorf("column %q dense overflow", b.Name)
			}
			if physical[dense] {
				outputWord[i] = 1
			} else {
				outputWord[i] = 0
			}
			dense++
			mask |= bit
		}
		v.Validity[base>>6] = mask
	}
	if dense != values {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", b.Name, dense, values)
	}

	return nil
}

func normalizeTimestampColumn(b columnBinding, values []int64, validity memory.Bitmap, kernels kernel.Set) error {
	if b.Type != memory.TypeTimestamp || b.TimestampScale == 1 {
		return nil
	}
	if row := kernels.ScaleInt64(values, validity, b.TimestampScale); row >= 0 {
		return fmt.Errorf("column %q timestamp row %d overflows UTC nanosecond normalization", b.Name, row)
	}
	return nil
}

// truthSet is a conservative set of SQL truth values that may occur in one row group.
type truthSet uint8

const (
	truthTrue truthSet = 1 << iota
	truthFalse
	truthNull
	truthAny = truthTrue | truthFalse | truthNull
)

// pruningRowGroup retains access to raw footer presence bits. Arrow's typed
// statistics currently replace an absent null count or one absent bound with
// zero, which must not be used as evidence to discard rows.
type pruningRowGroup struct {
	*metadata.RowGroupMetaData
	file  *metadata.FileMetaData
	index int
}

func rowGroupMayMatch(meta *metadata.FileMetaData, rowGroup int, predicate expr.Expr, bindings map[string]columnBinding) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	rg := &pruningRowGroup{RowGroupMetaData: meta.RowGroup(rowGroup), file: meta, index: rowGroup}
	truth, err := predicateTruth(rg, predicate, bindings)
	if err != nil {
		return false, err
	}
	return truth&truthTrue != 0, nil
}

func predicateTruth(rg *pruningRowGroup, predicate expr.Expr, bindings map[string]columnBinding) (truthSet, error) {
	switch n := predicate.(type) {
	case expr.InExpr:
		return inPredicateTruth(rg, n, bindings)
	case expr.Binary:
		switch n.Op {
		case expr.OpAnd, expr.OpOr:
			left, err := predicateTruth(rg, n.Left, bindings)
			if err != nil {
				return truthAny, err
			}
			right, err := predicateTruth(rg, n.Right, bindings)
			if err != nil {
				return truthAny, err
			}
			return combineTruth(n.Op, left, right), nil
		case expr.OpGT, expr.OpGE, expr.OpLT, expr.OpLE, expr.OpEQ:
			return comparisonTruth(rg, n, bindings)
		case expr.OpContains, expr.OpStartsWith, expr.OpEndsWith:
			return stringPredicateTruth(rg, n, bindings)
		default:
			return truthAny, nil
		}
	case expr.Unary:
		switch n.Op {
		case expr.OpNot:
			v, err := predicateTruth(rg, n.Value, bindings)
			if err != nil {
				return truthAny, err
			}
			return notTruth(v), nil
		case expr.OpIsNull, expr.OpIsNotNull:
			col, ok := n.Value.(expr.Column)
			if !ok {
				return truthAny, nil
			}
			return nullTruth(rg, col.Name, n.Op, bindings)
		default:
			return truthAny, nil
		}
	default:
		return truthAny, nil
	}
}

func comparisonTruth(rg *pruningRowGroup, b expr.Binary, bindings map[string]columnBinding) (truthSet, error) {
	if extract, ok := b.Left.(expr.Unary); ok {
		return extractComparisonTruth(rg, b, extract, bindings)
	}
	col, literal, op, ok := normalizeColumnLiteralComparison(b)
	if !ok {
		return truthAny, nil
	}
	binding, ok := bindings[col.Name]
	if !ok {
		return truthAny, nil
	}
	stats, err := columnStatistics(rg, binding)
	if err != nil {
		return truthAny, err
	}
	return columnComparisonTruth(rg, binding, stats, literal, op)
}

// inPredicateTruth is the SQL three-valued OR of literal equalities. In
// particular, a NULL candidate makes a non-match NULL, not FALSE, so NOT IN
// cannot keep that row. Only total literal leaves are considered; ApplyPushdown
// separately disables all pruning when binding finds a demanded-error risk.
func inPredicateTruth(rg *pruningRowGroup, in expr.InExpr, bindings map[string]columnBinding) (truthSet, error) {
	column, ok := in.Value.(expr.Column)
	if !ok || len(in.Values) == 0 {
		return truthAny, nil // Bind rejects an empty list before pruning.
	}
	for _, candidate := range in.Values {
		switch candidate.(type) {
		case expr.Int64Literal, expr.Float64Literal, expr.StringLiteral, expr.BoolLiteral,
			expr.NullLiteral, expr.Date32Literal, expr.TimestampLiteral, expr.TimestampUnitLiteral,
			expr.DurationLiteral, expr.Decimal128Literal, expr.Decimal128NullLiteral:
		default:
			return truthAny, nil
		}
	}
	binding, ok := bindings[column.Name]
	if !ok {
		return truthAny, nil
	}
	// Decode and validate the column statistics once, not once per candidate.
	// Nil means unavailable/untrusted, not an empty value domain. NULL literals
	// still provide useful truth information even when statistics are absent.
	stats, err := columnStatistics(rg, binding)
	if err != nil {
		return truthAny, err
	}
	out := truthFalse
	for _, candidate := range in.Values {
		value := truthNull
		switch candidate.(type) {
		case expr.NullLiteral, expr.Decimal128NullLiteral:
		default:
			value, err = columnComparisonTruth(rg, binding, stats, candidate, expr.OpEQ)
			if err != nil {
				return truthAny, err
			}
		}
		out = combineTruth(expr.OpOr, out, value)
	}
	return out, nil
}

func columnComparisonTruth(rg *pruningRowGroup, binding columnBinding, stats metadata.TypedStatistics, literal expr.Expr, op expr.Op) (truthSet, error) {
	if stats == nil {
		return truthAny, nil
	}
	// Parquet has no portable way to tell whether arbitrary BYTE_ARRAY min/max
	// statistics were prefix-truncated. They are therefore never used to infer
	// string value ranges; null-count-only pruning remains safe.
	// Unsigned min/max are physical signed Go values and cannot enter the
	// int64 range evaluator (UINT64 can exceed MaxInt64). Until a matching
	// unsigned metadata evaluator is available, retain value ranges and use
	// only null counts; the residual evaluator compares unsigned values exactly.
	if binding.Type == memory.TypeString || binding.Type == memory.TypeUint32 || binding.Type == memory.TypeUint64 || binding.Type == memory.TypeDecimal {
		if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
			return truthNull, nil
		}
		return truthAny, nil
	}
	if binding.Type == memory.TypeFloat64 {
		return floatComparisonTruth(rg, stats, literal, op)
	}
	literalValue, ok, err := comparisonLiteral(binding, literal)
	if err != nil || !ok {
		if err != nil {
			return truthAny, err
		}
		return truthAny, nil
	}
	min, max, hasMinMax, err := numericStatistics(binding, stats)
	if err != nil {
		return truthAny, err
	}
	return rangeTruth(rg, stats, hasMinMax, min, max, literalValue, op), nil
}

func normalizeColumnLiteralComparison(b expr.Binary) (expr.Column, expr.Expr, expr.Op, bool) {
	if c, ok := b.Left.(expr.Column); ok {
		return c, b.Right, b.Op, true
	}
	c, ok := b.Right.(expr.Column)
	if !ok {
		return expr.Column{}, nil, expr.OpInvalid, false
	}
	op := b.Op
	switch op {
	case expr.OpGT:
		op = expr.OpLT
	case expr.OpGE:
		op = expr.OpLE
	case expr.OpLT:
		op = expr.OpGT
	case expr.OpLE:
		op = expr.OpGE
	case expr.OpEQ:
	default:
		return expr.Column{}, nil, expr.OpInvalid, false
	}
	return c, b.Left, op, true
}

func comparisonLiteral(binding columnBinding, literal expr.Expr) (int64, bool, error) {
	switch v := literal.(type) {
	case expr.Int64Literal:
		return v.Value, true, nil
	case expr.Date32Literal:
		if binding.Type != memory.TypeDate32 {
			return 0, false, nil
		}
		return int64(v.Days), true, nil
	case expr.TimestampLiteral:
		if binding.Type != memory.TypeTimestamp {
			return 0, false, nil
		}
		return v.Value, true, nil
	case expr.TimestampUnitLiteral:
		if binding.Type != memory.TypeTimestamp {
			return 0, false, nil
		}
		nanos, err := v.Nanoseconds()
		return nanos, err == nil, err
	default:
		return 0, false, nil
	}
}

func columnStatistics(rg *pruningRowGroup, binding columnBinding) (metadata.TypedStatistics, error) {
	if rg == nil || rg.RowGroupMetaData == nil || rg.file == nil || rg.Schema == nil ||
		rg.index < 0 || rg.index >= len(rg.file.RowGroups) ||
		binding.Index < 0 || binding.Index >= rg.Schema.NumColumns() {
		return nil, nil
	}
	rawGroup := rg.file.RowGroups[rg.index]
	if rawGroup == nil || binding.Index >= len(rawGroup.Columns) || rawGroup.Columns[binding.Index] == nil {
		return nil, nil
	}
	rawColumn := rawGroup.Columns[binding.Index].MetaData
	if rawColumn == nil || rawColumn.Statistics == nil {
		return nil, nil
	}
	raw := rawColumn.Statistics
	// Do not let Arrow's default zero turn an unknown count into "no NULLs".
	// For these flat columns every row contributes one value or one null.
	if !raw.IsSetNullCount() || raw.GetNullCount() < 0 || raw.GetNullCount() > rg.NumRows() {
		return nil, nil
	}
	descriptor := rg.Schema.Column(binding.Index)
	min, max := raw.Min, raw.Max
	if descriptor.ColumnOrder() == arrowparquet.ColumnOrders.TypeDefinedOrder {
		min, max = raw.MinValue, raw.MaxValue
	}
	// Arrow tests the two presence bits with OR, then silently ignores decode
	// failures. Check the actual pair and physical width before it decodes.
	if (min == nil) != (max == nil) {
		return nil, nil
	}
	if min != nil {
		width := 0
		switch descriptor.PhysicalType() {
		case arrowparquet.Types.Boolean:
			width = 1
		case arrowparquet.Types.Int32, arrowparquet.Types.Float:
			width = 4
		case arrowparquet.Types.Int64, arrowparquet.Types.Double:
			width = 8
		case arrowparquet.Types.FixedLenByteArray:
			width = descriptor.TypeLength()
		}
		if width > 0 && (len(min) != width || len(max) != width) {
			return nil, nil
		}
	}
	chunk, err := rg.ColumnChunk(binding.Index)
	if err != nil {
		return nil, err
	}
	// Retain Arrow's writer-version and sort-order trust checks as well.
	return chunk.Statistics()
}

func numericStatistics(binding columnBinding, stats metadata.TypedStatistics) (int64, int64, bool, error) {
	if !stats.HasMinMax() {
		return 0, 0, false, nil
	}
	switch binding.Type {
	case memory.TypeInt32, memory.TypeDate32:
		v, ok := stats.(*metadata.Int32Statistics)
		if !ok {
			return 0, 0, false, nil
		}
		return int64(v.Min()), int64(v.Max()), true, nil
	case memory.TypeInt64:
		v, ok := stats.(*metadata.Int64Statistics)
		if !ok {
			return 0, 0, false, nil
		}
		return v.Min(), v.Max(), true, nil
	case memory.TypeTimestamp:
		v, ok := stats.(*metadata.Int64Statistics)
		if !ok {
			return 0, 0, false, nil
		}
		min, err := normalizeTimestampStatistic(binding, v.Min())
		if err != nil {
			return 0, 0, false, err
		}
		max, err := normalizeTimestampStatistic(binding, v.Max())
		if err != nil {
			return 0, 0, false, err
		}
		return min, max, true, nil
	case memory.TypeBool:
		v, ok := stats.(*metadata.BooleanStatistics)
		if !ok {
			return 0, 0, false, nil
		}
		return boolStatistic(v.Min()), boolStatistic(v.Max()), true, nil
	default:
		return 0, 0, false, nil
	}
}

// boolStatistic maps a boolean statistic bound to the engine 0/1 physical
// representation.
func boolStatistic(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

// floatComparisonTruth prunes a float64 column comparison against a numeric
// literal using float64 min/max statistics. NaN bounds disable range reasoning;
// NaN literals are FALSE on valid rows. Parquet may omit NaNs from statistics,
// so ordinary comparisons must also retain FALSE as a possible result.
func floatComparisonTruth(rg *pruningRowGroup, stats metadata.TypedStatistics, literal expr.Expr, op expr.Op) (truthSet, error) {
	value, ok := floatComparisonLiteral(literal)
	if !ok {
		return truthAny, nil
	}
	var min, max float64
	hasMinMax := false
	if stats.HasMinMax() {
		if v, ok := stats.(*metadata.Float64Statistics); ok {
			min, max = v.Min(), v.Max()
			hasMinMax = !math.IsNaN(min) && !math.IsNaN(max)
		}
	}
	return floatRangeTruth(rg, stats, hasMinMax, min, max, value, op), nil
}

// floatComparisonLiteral accepts float64 literals and exactly representable
// int64 bounds. Runtime integer/float comparisons are exact, so rounding a
// nonrepresentable integer here could incorrectly exclude a matching group.
func floatComparisonLiteral(literal expr.Expr) (float64, bool) {
	switch v := literal.(type) {
	case expr.Float64Literal:
		return v.Value, true
	case expr.Int64Literal:
		value := float64(v.Value)
		if value >= 0x1p63 || value < -0x1p63 || int64(value) != v.Value {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
}

// floatRangeTruth mirrors rangeTruth for float64 bounds and literals.
func floatRangeTruth(rg *pruningRowGroup, stats metadata.TypedStatistics, hasMinMax bool, min, max, literal float64, op expr.Op) truthSet {
	out := nullOnlyTruth(stats)
	if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
		return out
	}
	if math.IsNaN(literal) {
		return out | truthFalse
	}
	if !hasMinMax || math.IsNaN(min) || math.IsNaN(max) || min > max {
		return out | truthTrue | truthFalse
	}
	// NaN rows are valid but can be absent from min/max. They make every
	// supported comparison FALSE, including one whose numeric range is TRUE.
	// Losing that possibility makes NOT and larger Boolean trees unsound.
	out |= truthFalse
	var mayTrue bool
	switch op {
	case expr.OpGT:
		mayTrue = max > literal
	case expr.OpGE:
		mayTrue = max >= literal
	case expr.OpLT:
		mayTrue = min < literal
	case expr.OpLE:
		mayTrue = min <= literal
	case expr.OpEQ:
		mayTrue = min <= literal && literal <= max
	}
	if mayTrue {
		out |= truthTrue
	}
	return out
}

func normalizeTimestampStatistic(binding columnBinding, value int64) (int64, error) {
	if binding.TimestampScale == 1 {
		return value, nil
	}
	if value > math.MaxInt64/binding.TimestampScale || value < math.MinInt64/binding.TimestampScale {
		return 0, fmt.Errorf("column %q timestamp statistics overflow UTC nanosecond normalization", binding.Name)
	}
	return value * binding.TimestampScale, nil
}

func rangeTruth(rg *pruningRowGroup, stats metadata.TypedStatistics, hasMinMax bool, min, max, literal int64, op expr.Op) truthSet {
	out := nullOnlyTruth(stats)
	if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
		return out
	}
	if !hasMinMax || min > max {
		return out | truthTrue | truthFalse
	}
	var mayTrue, mayFalse bool
	switch op {
	case expr.OpGT:
		mayTrue, mayFalse = max > literal, min <= literal
	case expr.OpGE:
		mayTrue, mayFalse = max >= literal, min < literal
	case expr.OpLT:
		mayTrue, mayFalse = min < literal, max >= literal
	case expr.OpLE:
		mayTrue, mayFalse = min <= literal, max > literal
	case expr.OpEQ:
		mayTrue = min <= literal && literal <= max
		mayFalse = min != literal || max != literal
	}
	if mayTrue {
		out |= truthTrue
	}
	if mayFalse {
		out |= truthFalse
	}
	return out
}

// nullOnlyTruth returns every truth value possible solely from null-count
// metadata. Unknown null counts intentionally retain NULL as possible.
func nullOnlyTruth(stats metadata.TypedStatistics) truthSet {
	if !stats.HasNullCount() {
		return truthNull
	}
	var out truthSet
	if stats.NullCount() > 0 {
		out |= truthNull
	}
	return out
}

func stringPredicateTruth(rg *pruningRowGroup, b expr.Binary, bindings map[string]columnBinding) (truthSet, error) {
	column, ok := b.Left.(expr.Column)
	if !ok {
		return truthAny, nil
	}
	if _, ok := b.Right.(expr.StringLiteral); !ok {
		return truthAny, nil
	}
	binding, ok := bindings[column.Name]
	if !ok || binding.Type != memory.TypeString {
		return truthAny, nil
	}
	stats, err := columnStatistics(rg, binding)
	if err != nil || stats == nil {
		return truthAny, err
	}
	if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
		return truthNull, nil
	}
	return truthAny, nil
}

func extractComparisonTruth(rg *pruningRowGroup, b expr.Binary, extract expr.Unary, bindings map[string]columnBinding) (truthSet, error) {
	if extract.Op != expr.OpYear && extract.Op != expr.OpMonth && extract.Op != expr.OpDay &&
		extract.Op != expr.OpHour && extract.Op != expr.OpMinute && extract.Op != expr.OpSecond {
		return truthAny, nil
	}
	column, ok := extract.Value.(expr.Column)
	if !ok {
		return truthAny, nil
	}
	literal, ok := b.Right.(expr.Int64Literal)
	if !ok {
		return truthAny, nil
	}
	binding, ok := bindings[column.Name]
	if !ok {
		return truthAny, nil
	}
	if (extract.Op == expr.OpHour || extract.Op == expr.OpMinute || extract.Op == expr.OpSecond) && binding.Type != memory.TypeTimestamp {
		return truthAny, nil
	}
	if (extract.Op == expr.OpYear || extract.Op == expr.OpMonth || extract.Op == expr.OpDay) &&
		binding.Type != memory.TypeDate32 && binding.Type != memory.TypeDate64 && binding.Type != memory.TypeTimestamp {
		return truthAny, nil
	}
	stats, err := columnStatistics(rg, binding)
	if err != nil || stats == nil {
		return truthAny, err
	}
	min, max, hasMinMax, err := numericStatistics(binding, stats)
	if err != nil {
		return truthAny, err
	}
	if !hasMinMax {
		return rangeTruth(rg, stats, false, 0, 0, 0, b.Op), nil
	}
	minTime := utcTemporalTime(binding.Type, min)
	maxTime := utcTemporalTime(binding.Type, max)
	if extract.Op == expr.OpYear {
		return rangeTruth(rg, stats, true, int64(minTime.Year()), int64(maxTime.Year()), literal.Value, b.Op), nil
	}
	if sameTemporalBucket(minTime, maxTime, extract.Op) {
		part := temporalPart(minTime, extract.Op)
		return rangeTruth(rg, stats, true, part, part, literal.Value, b.Op), nil
	}
	lo, hi := temporalPartDomain(extract.Op)
	return rangeTruth(rg, stats, true, lo, hi, literal.Value, b.Op), nil
}

func utcTemporalTime(typ memory.DataType, value int64) time.Time {
	switch typ {
	case memory.TypeDate32:
		return time.Unix(value*24*60*60, 0).UTC()
	case memory.TypeDate64:
		return time.Unix(value/1000, (value%1000)*int64(time.Millisecond)).UTC()
	default: // TypeTimestamp values are normalized to UTC nanoseconds.
		return time.Unix(value/int64(time.Second), value%int64(time.Second)).UTC()
	}
}

func sameTemporalBucket(min, max time.Time, extract expr.Op) bool {
	switch extract {
	case expr.OpMonth:
		return min.Year() == max.Year() && min.Month() == max.Month()
	case expr.OpDay:
		return min.Year() == max.Year() && min.YearDay() == max.YearDay()
	case expr.OpHour:
		return min.Year() == max.Year() && min.YearDay() == max.YearDay() && min.Hour() == max.Hour()
	case expr.OpMinute:
		return min.Year() == max.Year() && min.YearDay() == max.YearDay() && min.Hour() == max.Hour() && min.Minute() == max.Minute()
	case expr.OpSecond:
		return min.Year() == max.Year() && min.YearDay() == max.YearDay() && min.Hour() == max.Hour() && min.Minute() == max.Minute() && min.Second() == max.Second()
	default:
		return false
	}
}

func temporalPart(value time.Time, extract expr.Op) int64 {
	switch extract {
	case expr.OpMonth:
		return int64(value.Month())
	case expr.OpDay:
		return int64(value.Day())
	case expr.OpHour:
		return int64(value.Hour())
	case expr.OpMinute:
		return int64(value.Minute())
	case expr.OpSecond:
		return int64(value.Second())
	default:
		return int64(value.Year())
	}
}

func temporalPartDomain(extract expr.Op) (int64, int64) {
	switch extract {
	case expr.OpMonth:
		return 1, 12
	case expr.OpDay:
		return 1, 31
	case expr.OpHour:
		return 0, 23
	case expr.OpMinute, expr.OpSecond:
		return 0, 59
	default:
		return math.MinInt64, math.MaxInt64
	}
}

func nullTruth(rg *pruningRowGroup, name string, op expr.Op, bindings map[string]columnBinding) (truthSet, error) {
	binding, ok := bindings[name]
	if !ok {
		return truthAny, nil
	}
	stats, err := columnStatistics(rg, binding)
	if err != nil || stats == nil || !stats.HasNullCount() {
		return truthTrue | truthFalse, err
	}
	nulls := stats.NullCount()
	nonNull := rg.NumRows() - nulls
	var out truthSet
	if op == expr.OpIsNull {
		if nulls > 0 {
			out |= truthTrue
		}
		if nonNull > 0 {
			out |= truthFalse
		}
	} else {
		if nonNull > 0 {
			out |= truthTrue
		}
		if nulls > 0 {
			out |= truthFalse
		}
	}
	return out, nil
}

func combineTruth(op expr.Op, left, right truthSet) truthSet {
	values := []truthSet{truthTrue, truthFalse, truthNull}
	var out truthSet
	for _, l := range values {
		if left&l == 0 {
			continue
		}
		for _, r := range values {
			if right&r == 0 {
				continue
			}
			out |= truthTable(op, l, r)
		}
	}
	return out
}

func truthTable(op expr.Op, l, r truthSet) truthSet {
	if op == expr.OpAnd {
		if l == truthFalse || r == truthFalse {
			return truthFalse
		}
		if l == truthTrue && r == truthTrue {
			return truthTrue
		}
		return truthNull
	}
	if l == truthTrue || r == truthTrue {
		return truthTrue
	}
	if l == truthFalse && r == truthFalse {
		return truthFalse
	}
	return truthNull
}

func notTruth(in truthSet) truthSet {
	var out truthSet
	if in&truthTrue != 0 {
		out |= truthFalse
	}
	if in&truthFalse != 0 {
		out |= truthTrue
	}
	if in&truthNull != 0 {
		out |= truthNull
	}
	return out
}
