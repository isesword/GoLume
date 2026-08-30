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
	"unicode/utf8"

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
	TimestampScale int64
}

// Source exposes projected flat Parquet columns as row-group partitions. It
// supports integers, UTF-8 strings, dates, and UTC timestamps. Pushdown clones
// are immutable and safe to prepare independently from the same base source.
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
// integer, UTF-8 string, date, and UTC timestamp columns. Repeated/nested
// columns and unsupported physical or logical types are rejected rather than
// silently decoded incorrectly.
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
	switch d.PhysicalType() {
	case arrowparquet.Types.Int32:
		if _, ok := d.LogicalType().(schema.DateLogicalType); ok {
			binding.Type = memory.TypeDate32
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

// NewReader opens an independent Arrow-Go reader configured to decode only the
// projected column chunks.
func (s *Source) NewReader(_ context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r, err := file.OpenParquetFile(s.path, false)
	if err != nil {
		return nil, err
	}
	return &reader{r: r, bindings: append([]columnBinding(nil), s.bindings...), kernels: opts.Kernels}, nil
}

// ApplyPushdown returns an immutable source clone that reads only requested
// columns and prunes row groups that cannot make Predicate SQL TRUE according
// to Parquet min/max/null-count statistics. The row-level predicate must still
// be evaluated by the engine for every retained row group.
func (s *Source) ApplyPushdown(_ context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	if s == nil {
		return nil, source.PushdownReport{}, fmt.Errorf("nil parquet source")
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
	for _, name := range expr.Columns(req.Predicate) {
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

		keep, err := rowGroupMayMatch(s.meta, p.ID, req.Predicate, byName)
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

type reader struct {
	r          *file.Reader
	bindings   []columnBinding
	kernels    kernel.Set
	dense64    []int64
	dense32    []int32
	denseBytes []arrowparquet.ByteArray
	defs       []int16
}

// Close releases the underlying Parquet file reader.
func (r *reader) Close() error { return r.r.Close() }

func resize[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

// ReadPartition decodes one projected row group into reusable typed vectors in dst.
func (r *reader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.ID < 0 || p.ID >= r.r.NumRowGroups() {
		return fmt.Errorf("row group %d out of range", p.ID)
	}
	rg := r.r.RowGroup(p.ID)
	rows := int(rg.MetaData().NumRows())
	for _, b := range r.bindings {
		chunk, err := rg.Column(b.Index)
		if err != nil {
			return err
		}
		switch b.Type {
		case memory.TypeInt64, memory.TypeTimestamp:
			if err := r.readInt64(chunk, b, rows, dst); err != nil {
				return err
			}
		case memory.TypeInt32, memory.TypeDate32:
			if err := r.readInt32(chunk, b, rows, dst); err != nil {
				return err
			}
		case memory.TypeString:
			if err := r.readString(chunk, b, rows, dst); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported projected type %s for %q", b.Type, b.Name)
		}
	}
	dst.Reset(rows)
	return nil
}

func (r *reader) readInt64(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Int64s[b.Name]
	if v == nil {
		v = &memory.Int64Vector{}
		dst.Int64s[b.Name] = v
	}
	v.Values = resize(v.Values, rows)
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
	r.dense64 = resize(r.dense64, rows)
	r.defs = resize(r.defs, rows)
	v.Validity = memory.EnsureBitmap(v.Validity, rows)
	total, values, err := c.ReadBatch(int64(rows), r.dense64[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	dense := 0
	for i := 0; i < rows; i++ {
		if r.defs[i] == 0 {
			v.Values[i] = 0
			continue
		}
		if dense >= values {
			return fmt.Errorf("column %q dense overflow", b.Name)
		}
		v.Values[i] = r.dense64[dense]
		dense++
		memory.Set(v.Validity, i)
	}
	if dense != values {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", b.Name, dense, values)
	}
	return normalizeTimestampColumn(b, v.Values, v.Validity, r.kernels)
}

func (r *reader) readInt32(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Int32s[b.Name]
	if v == nil {
		v = &memory.Int32Vector{}
		dst.Int32s[b.Name] = v
	}
	v.Values = resize(v.Values, rows)
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
	r.dense32 = resize(r.dense32, rows)
	r.defs = resize(r.defs, rows)
	v.Validity = memory.EnsureBitmap(v.Validity, rows)
	total, values, err := c.ReadBatch(int64(rows), r.dense32[:rows], r.defs[:rows], nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	dense := 0
	for i := 0; i < rows; i++ {
		if r.defs[i] == 0 {
			v.Values[i] = 0
			continue
		}
		if dense >= values {
			return fmt.Errorf("column %q dense overflow", b.Name)
		}
		v.Values[i] = r.dense32[dense]
		dense++
		memory.Set(v.Validity, i)
	}
	if dense != values {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", b.Name, dense, values)
	}
	return nil
}

func (r *reader) readString(chunk file.ColumnChunkReader, b columnBinding, rows int, dst *memory.Batch) error {
	v := dst.Strings[b.Name]
	if v == nil {
		v = &memory.StringViewVector{}
		dst.Strings[b.Name] = v
	}
	v.Offsets = resize(v.Offsets, rows+1)
	v.Offsets[0] = 0
	v.Data = v.Data[:0]
	v.Encoding = memory.EncodingView
	c, ok := chunk.(*file.ByteArrayColumnChunkReader)
	if !ok {
		return fmt.Errorf("column %q is not a byte-array chunk reader", b.Name)
	}
	if !b.Nullable {
		v.Validity = nil
		r.denseBytes = resize(r.denseBytes, rows)
		total, values, err := c.ReadBatch(int64(rows), r.denseBytes, nil, nil)
		if err != nil {
			return err
		}
		if int(total) != rows || values != rows {
			return fmt.Errorf("column %q row count mismatch total=%d values=%d want=%d", b.Name, total, values, rows)
		}
		for row, value := range r.denseBytes {
			if err := appendUTF8Value(v, b.Name, row, value); err != nil {
				return err
			}
		}
		return nil
	}
	r.denseBytes = resize(r.denseBytes, rows)
	r.defs = resize(r.defs, rows)
	v.Validity = memory.EnsureBitmap(v.Validity, rows)
	total, values, err := c.ReadBatch(int64(rows), r.denseBytes, r.defs, nil)
	if err != nil {
		return err
	}
	if int(total) != rows {
		return fmt.Errorf("column %q row count mismatch total=%d want=%d", b.Name, total, rows)
	}
	dense := 0
	for row := 0; row < rows; row++ {
		if r.defs[row] == 0 {
			v.Offsets[row+1] = uint32(len(v.Data))
			continue
		}
		if dense >= values {
			return fmt.Errorf("column %q dense overflow", b.Name)
		}
		if err := appendUTF8Value(v, b.Name, row, r.denseBytes[dense]); err != nil {
			return err
		}
		dense++
		memory.Set(v.Validity, row)
	}
	if dense != values {
		return fmt.Errorf("column %q dense mismatch got=%d want=%d", b.Name, dense, values)
	}
	return nil
}

func appendUTF8Value(v *memory.StringViewVector, column string, row int, value arrowparquet.ByteArray) error {
	if !utf8.Valid(value) {
		return fmt.Errorf("column %q row %d contains invalid UTF-8", column, row)
	}
	if len(v.Data) > math.MaxUint32-len(value) {
		return fmt.Errorf("column %q string data exceeds uint32 offset capacity", column)
	}
	v.Data = append(v.Data, value...)
	v.Offsets[row+1] = uint32(len(v.Data))
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

func rowGroupMayMatch(meta *metadata.FileMetaData, rowGroup int, predicate expr.Expr, bindings map[string]columnBinding) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	rg := meta.RowGroup(rowGroup)
	truth, err := predicateTruth(rg, predicate, bindings)
	if err != nil {
		return false, err
	}
	return truth&truthTrue != 0, nil
}

func predicateTruth(rg *metadata.RowGroupMetaData, predicate expr.Expr, bindings map[string]columnBinding) (truthSet, error) {
	switch n := predicate.(type) {
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

func comparisonTruth(rg *metadata.RowGroupMetaData, b expr.Binary, bindings map[string]columnBinding) (truthSet, error) {
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
	if err != nil || stats == nil {
		return truthAny, err
	}
	// Parquet has no portable way to tell whether arbitrary BYTE_ARRAY min/max
	// statistics were prefix-truncated. They are therefore never used to infer
	// string value ranges; null-count-only pruning remains safe.
	if binding.Type == memory.TypeString {
		if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
			return truthNull, nil
		}
		return truthAny, nil
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

func columnStatistics(rg *metadata.RowGroupMetaData, binding columnBinding) (metadata.TypedStatistics, error) {
	chunk, err := rg.ColumnChunk(binding.Index)
	if err != nil {
		return nil, err
	}
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
	default:
		return 0, 0, false, nil
	}
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

func rangeTruth(rg *metadata.RowGroupMetaData, stats metadata.TypedStatistics, hasMinMax bool, min, max, literal int64, op expr.Op) truthSet {
	out := nullOnlyTruth(stats)
	if stats.HasNullCount() && stats.NullCount() >= rg.NumRows() {
		return out
	}
	if !hasMinMax {
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

func stringPredicateTruth(rg *metadata.RowGroupMetaData, b expr.Binary, bindings map[string]columnBinding) (truthSet, error) {
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

func extractComparisonTruth(rg *metadata.RowGroupMetaData, b expr.Binary, extract expr.Unary, bindings map[string]columnBinding) (truthSet, error) {
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

func nullTruth(rg *metadata.RowGroupMetaData, name string, op expr.Op, bindings map[string]columnBinding) (truthSet, error) {
	binding, ok := bindings[name]
	if !ok {
		return truthAny, nil
	}
	chunk, err := rg.ColumnChunk(binding.Index)
	if err != nil {
		return truthAny, err
	}
	stats, err := chunk.Statistics()
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
