package parquet

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"os"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
)

// Columns maps the three Phase 2.2 logical query roles to Parquet physical
// column indexes. Open is retained for source compatibility; new lazy code
// should prefer OpenFile, which discovers primitive columns by name.
type Columns struct {
	PredicateName  string
	PredicateIndex int
	GroupName      string
	GroupIndex     int
	ValueName      string
	ValueIndex     int
}

type columnBinding struct {
	Name     string
	Index    int
	Type     memory.DataType
	Nullable bool
}

// Source exposes projected primitive Parquet columns as row-group partitions.
// Phase 2.3 supports flat int32 and int64 columns. Pushdown clones are immutable
// and safe to prepare independently from the same base source.
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

// Open validates the legacy three-column mapping and constructs a Parquet
// source. New lazy code should prefer OpenFile so projection can be discovered
// directly from file schema metadata.
func Open(path string, cols Columns) (*Source, error) {
	if cols.PredicateIndex == 0 && cols.GroupIndex == 0 && cols.ValueIndex == 0 {
		cols.PredicateIndex, cols.GroupIndex, cols.ValueIndex = 0, 1, 2
	}
	if cols.PredicateName == "" {
		cols.PredicateName = "pred"
	}
	if cols.GroupName == "" {
		cols.GroupName = "key"
	}
	if cols.ValueName == "" {
		cols.ValueName = "value"
	}
	base, err := openMetadata(path)
	if err != nil {
		return nil, err
	}
	indexes := []int{cols.PredicateIndex, cols.GroupIndex, cols.ValueIndex}
	names := []string{cols.PredicateName, cols.GroupName, cols.ValueName}
	bindings := make([]columnBinding, len(indexes))
	fields := make([]memory.Field, len(indexes))
	for i, idx := range indexes {
		b, err := bindingAt(base.meta, idx, names[i])
		if err != nil {
			return nil, err
		}
		bindings[i] = b
		fields[i] = memory.Field{Name: b.Name, Type: b.Type, Nullable: b.Nullable, Encoding: memory.EncodingPlain}
	}
	base.allBindings = bindings
	base.bindings = append([]columnBinding(nil), bindings...)
	base.baseSchema = memory.NewSchema(fields...)
	base.schema = base.baseSchema
	return base, nil
}

// OpenFile discovers a flat primitive Parquet schema and exposes all supported
// int32/int64 columns. Repeated/nested columns and other physical types are
// rejected in Phase 2.3 rather than silently decoded incorrectly.
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
		fields = append(fields, memory.Field{Name: b.Name, Type: b.Type, Nullable: b.Nullable, Encoding: memory.EncodingPlain})
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
	var typ memory.DataType
	switch d.PhysicalType() {
	case arrowparquet.Types.Int32:
		typ = memory.TypeInt32
	case arrowparquet.Types.Int64:
		typ = memory.TypeInt64
	default:
		return columnBinding{}, fmt.Errorf("parquet column %q physical type %s is not supported by phase-2.3 lazy scan", d.Path(), d.PhysicalType())
	}
	return columnBinding{Name: logicalName, Index: index, Type: typ, Nullable: d.MaxDefinitionLevel() > 0}, nil
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
func (s *Source) NewReader(context.Context) (source.Reader, error) {
	r, err := file.OpenParquetFile(s.path, false)
	if err != nil {
		return nil, err
	}
	return &reader{r: r, bindings: append([]columnBinding(nil), s.bindings...)}, nil
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
	r        *file.Reader
	bindings []columnBinding
	dense64  []int64
	dense32  []int32
	defs     []int16
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
		case memory.TypeInt64:
			if err := r.readInt64(chunk, b, rows, dst); err != nil {
				return err
			}
		case memory.TypeInt32:
			if err := r.readInt32(chunk, b, rows, dst); err != nil {
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
		return nil
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
	return nil
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
	col, lit, op, ok := normalizeComparison(b)
	if !ok {
		return truthAny, nil
	}
	binding, ok := bindings[col.Name]
	if !ok || binding.Type != memory.TypeInt64 {
		return truthAny, nil
	}
	chunk, err := rg.ColumnChunk(binding.Index)
	if err != nil {
		return truthAny, err
	}
	stats, err := chunk.Statistics()
	if err != nil || stats == nil {
		return truthAny, err
	}
	i64, ok := stats.(*metadata.Int64Statistics)
	if !ok {
		return truthAny, nil
	}
	var out truthSet
	nullsKnown := i64.HasNullCount()
	nulls := int64(0)
	if nullsKnown {
		nulls = i64.NullCount()
		if nulls > 0 {
			out |= truthNull
		}
	}
	nonNullPossible := !nullsKnown || nulls < rg.NumRows()
	if !nonNullPossible {
		return out, nil
	}
	if !i64.HasMinMax() {
		return out | truthTrue | truthFalse, nil
	}
	min, max := i64.Min(), i64.Max()
	v := lit.Value
	var mayTrue, mayFalse bool
	switch op {
	case expr.OpGT:
		mayTrue, mayFalse = max > v, min <= v
	case expr.OpGE:
		mayTrue, mayFalse = max >= v, min < v
	case expr.OpLT:
		mayTrue, mayFalse = min < v, max >= v
	case expr.OpLE:
		mayTrue, mayFalse = min <= v, max > v
	case expr.OpEQ:
		mayTrue = min <= v && v <= max
		mayFalse = min != v || max != v
	}
	if mayTrue {
		out |= truthTrue
	}
	if mayFalse {
		out |= truthFalse
	}
	return out, nil
}

func normalizeComparison(b expr.Binary) (expr.Column, expr.Int64Literal, expr.Op, bool) {
	if c, ok := b.Left.(expr.Column); ok {
		if lit, ok := b.Right.(expr.Int64Literal); ok {
			return c, lit, b.Op, true
		}
	}
	lit, litOK := b.Left.(expr.Int64Literal)
	c, colOK := b.Right.(expr.Column)
	if !litOK || !colOK {
		return expr.Column{}, expr.Int64Literal{}, expr.OpInvalid, false
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
		return expr.Column{}, expr.Int64Literal{}, expr.OpInvalid, false
	}
	return c, lit, op, true
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
