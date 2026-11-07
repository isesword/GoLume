package arrowrecord

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type recordOwner struct {
	records []arrow.RecordBatch
	mu      sync.Mutex
	closed  bool
	streams int
	statsMu sync.Mutex
	stats   map[int]map[string]int64Stats
}

func (o *recordOwner) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	if o.streams == 0 {
		o.releaseRecords()
	}
}

func (o *recordOwner) acquire() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return fmt.Errorf("arrowrecord: source closed")
	}
	o.streams++
	return nil
}

func (o *recordOwner) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.streams--
	if o.closed && o.streams == 0 {
		o.releaseRecords()
	}
}

func (o *recordOwner) releaseRecords() {
	for _, rec := range o.records {
		if rec != nil {
			rec.Release()
		}
	}
	o.records = nil
}

type dictionaryInfo struct {
	domain  string
	offsets []uint32
	data    []byte
	indices [][]uint32
}

type binding struct {
	name        string
	index       int
	field       memory.Field
	dict        *dictionaryInfo
	validities  []memory.Bitmap
	offsets     [][]uint32
	i32         [][]int32
	i64         [][]int64
	u32         [][]uint32
	f64         [][]float64
	stringLists []*memory.StringListVector
	structs     []*memory.StructVector
}

// Source exposes retained Arrow RecordBatches as independent source
// partitions. Pushdown clones share retained Arrow buffers with the base
// source; Close should therefore be called only on the source returned by
// NewSource after all prepared queries using its clones have been closed.
type Source struct {
	owner       *recordOwner
	baseSchema  memory.Schema
	schema      memory.Schema
	allBindings []binding
	bindings    []binding
	allParts    []source.Partition
	parts       []source.Partition
	report      source.PushdownReport
}

// NewSource retains records and constructs an Arrow RecordBatch source. All
// records must have equal schemas. The source owns one retain on every record
// until Close is called.
func NewSource(records ...arrow.RecordBatch) (*Source, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("arrowrecord: at least one RecordBatch is required")
	}
	if records[0] == nil {
		return nil, fmt.Errorf("arrowrecord: record 0 is nil")
	}
	schema := records[0].Schema()
	if schema == nil {
		return nil, fmt.Errorf("arrowrecord: nil schema")
	}
	owner := &recordOwner{records: make([]arrow.RecordBatch, len(records)), stats: make(map[int]map[string]int64Stats)}
	for i, rec := range records {
		if rec == nil {
			owner.close()
			return nil, fmt.Errorf("arrowrecord: record %d is nil", i)
		}
		if !schema.Equal(rec.Schema()) {
			owner.close()
			return nil, fmt.Errorf("arrowrecord: record %d schema differs from record 0", i)
		}
		rec.Retain()
		owner.records[i] = rec
	}
	bindings := make([]binding, len(schema.Fields()))
	fields := make([]memory.Field, len(bindings))
	for i, f := range schema.Fields() {
		mf := memoryField(f)
		bindings[i] = binding{name: f.Name, index: i, field: mf}
		fields[i] = mf
	}
	for i := range bindings {
		bindings[i].validities = make([]memory.Bitmap, len(owner.records))
		bindings[i].offsets = make([][]uint32, len(owner.records))
		bindings[i].i32 = make([][]int32, len(owner.records))
		bindings[i].i64 = make([][]int64, len(owner.records))
		bindings[i].u32 = make([][]uint32, len(owner.records))
		bindings[i].f64 = make([][]float64, len(owner.records))
		bindings[i].stringLists = make([]*memory.StringListVector, len(owner.records))
		bindings[i].structs = make([]*memory.StructVector, len(owner.records))
		for ri, rec := range owner.records {
			arr := rec.Column(bindings[i].index)
			bindings[i].validities[ri] = validityBitmap(arr)
			if err := cachePhysicalValues(&bindings[i], ri, arr); err != nil {
				owner.close()
				return nil, fmt.Errorf("arrowrecord: column %q: %w", bindings[i].name, err)
			}
			switch a := arr.(type) {
			case *array.String:
				bindings[i].offsets[ri] = offsets32(a.ValueOffsets())
			case *array.Binary:
				bindings[i].offsets[ri] = offsets32(a.ValueOffsets())
			case *array.List:
				if bindings[i].field.Type == memory.TypeList {
					v, err := arrowStringListVector(a, bindings[i].validities[ri])
					if err != nil {
						owner.close()
						return nil, fmt.Errorf("arrowrecord: column %q: %w", bindings[i].name, err)
					}
					if !bindings[i].field.Nullable && memory.HasNulls(v.Validity, v.Len()) {
						owner.close()
						return nil, fmt.Errorf("arrowrecord: non-nullable list<string> column %q contains NULL", bindings[i].name)
					}
					if !bindings[i].field.Children[0].Nullable && memory.HasNulls(v.Values.Validity, v.Values.Len()) {
						owner.close()
						return nil, fmt.Errorf("arrowrecord: list<string> column %q has NULL in non-nullable child", bindings[i].name)
					}
					bindings[i].stringLists[ri] = v
				}
			case *array.Struct:
				if bindings[i].field.Type == memory.TypeStruct {
					v, err := arrowStructVector(a, bindings[i].field, bindings[i].validities[ri])
					if err != nil {
						owner.close()
						return nil, fmt.Errorf("arrowrecord: column %q: %w", bindings[i].name, err)
					}
					bindings[i].structs[ri] = v
				}
			}
		}
		if bindings[i].field.Encoding != memory.EncodingDictionary || bindings[i].field.Type != memory.TypeString {
			continue
		}
		info, err := normalizeDictionary(owner.records, bindings[i].index, bindings[i].name)
		if err != nil {
			owner.close()
			return nil, err
		}
		bindings[i].dict = info
		bindings[i].field.DictionaryDomain = info.domain
		fields[i].DictionaryDomain = info.domain
	}
	parts := make([]source.Partition, len(records))
	for i, rec := range records {
		parts[i] = source.Partition{ID: i, Rows: rec.NumRows()}
	}
	logicalSchema := memory.NewSchema(fields...)
	return &Source{
		owner:       owner,
		baseSchema:  logicalSchema,
		schema:      logicalSchema,
		allBindings: bindings,
		bindings:    append([]binding(nil), bindings...),
		allParts:    append([]source.Partition(nil), parts...),
		parts:       append([]source.Partition(nil), parts...),
	}, nil
}

// Close releases the RecordBatch retains owned by the source. It is idempotent.
func (s *Source) Close() error {
	if s != nil && s.owner != nil {
		s.owner.close()
	}
	return nil
}

// Description returns a concise source label used by explain output.
func (s *Source) Description() string { return "arrow-record-batches" }

// Schema returns the current projected logical schema.
func (s *Source) Schema() memory.Schema { return s.schema }

// Replayability reports that retained RecordBatches are a stable snapshot.
func (*Source) Replayability() source.Replayability { return source.ReplayStable }

// PlanIdentity is stable across immutable projection/pushdown clones.
func (s *Source) PlanIdentity() string { return fmt.Sprintf("arrowrecord:%p", s.owner) }

// Stats reports rows and retained RecordBatch partitions. Byte size is unknown
// because Arrow buffers may be shared with caller-owned arrays.
func (s *Source) Stats(context.Context) (source.Stats, error) {
	var rows int64
	for _, p := range s.parts {
		rows += p.Rows
	}
	return source.Stats{Rows: rows, Partitions: len(s.parts)}, nil
}

// Partitions returns retained RecordBatch partitions selected by pushdown.
func (s *Source) Partitions(context.Context) ([]source.Partition, error) {
	out := make([]source.Partition, len(s.parts))
	copy(out, s.parts)
	return out, nil
}

// NewBatchStream creates an independent bounded stream over the source's
// projected Arrow bindings. Immutable Arrow values and dictionaries are shared.
func (s *Source) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if s == nil {
		return nil, fmt.Errorf("arrowrecord: nil source")
	}
	return s.NewBatchStreamWithPartitions(ctx, opts, s.parts)
}

// NewBatchStreamWithPartitions borrows exactly parts, including an empty list.
// The stream keeps the shared source buffers alive until Close, EOF, or error.
func (s *Source) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if s == nil || s.owner == nil {
		return nil, fmt.Errorf("arrowrecord: nil source")
	}
	if ctx == nil {
		return nil, fmt.Errorf("arrowrecord: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := s.owner.acquire(); err != nil {
		return nil, err
	}
	for _, part := range parts {
		if part.ID < 0 || part.ID >= len(s.owner.records) {
			s.owner.release()
			return nil, fmt.Errorf("arrowrecord: partition %d out of range", part.ID)
		}
	}
	return &batchStream{owner: s.owner, bindings: s.bindings, parts: parts, opts: opts.Normalize(), batch: memory.NewBatch()}, nil
}

// Dictionary returns the normalized source-wide dictionary for column.
func (s *Source) Dictionary(_ context.Context, column string) (source.Dictionary, error) {
	for _, b := range s.allBindings {
		if b.name != column {
			continue
		}
		if b.dict == nil {
			return source.Dictionary{}, fmt.Errorf("arrowrecord: column %q has no normalized string dictionary", column)
		}
		return source.Dictionary{Domain: b.dict.domain, Offsets: b.dict.offsets, Data: b.dict.data}, nil
	}
	return source.Dictionary{}, fmt.Errorf("arrowrecord: column %q not found", column)
}

// ApplyPushdown returns an immutable projection clone. For int64 predicates it
// synthesizes and caches conservative per-RecordBatch min/max/null statistics
// and prunes batches that cannot produce SQL TRUE. Residual row-level predicate
// execution remains mandatory for every retained batch.
func (s *Source) ApplyPushdown(_ context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	if s == nil {
		return nil, source.PushdownReport{}, fmt.Errorf("arrowrecord: nil source")
	}
	// A no-TRUE result is not enough to prune when evaluation can raise a
	// demanded row error. The residual executor owns mask/order semantics.
	pruningPredicate := req.Predicate
	var predicateColumns []string
	if req.Predicate != nil {
		bound, err := expr.Bind(s.baseSchema, req.Predicate)
		if err != nil {
			return nil, source.PushdownReport{}, fmt.Errorf("arrowrecord: pushdown predicate: %w", err)
		}
		if bound.Field.Type != memory.TypeBool {
			return nil, source.PushdownReport{}, fmt.Errorf("arrowrecord: pushdown predicate must be bool, got %s", bound.Field.Type)
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
	need := make(map[string]struct{}, len(projection))
	for _, name := range projection {
		need[name] = struct{}{}
	}
	for _, name := range predicateColumns {
		need[name] = struct{}{}
	}
	projection = orderedNames(s.baseSchema, need)
	if len(projection) != len(need) {
		return nil, source.PushdownReport{}, fmt.Errorf("arrowrecord: pushdown references unknown column")
	}
	byName := make(map[string]binding, len(s.allBindings))
	for _, b := range s.allBindings {
		byName[b.name] = b
	}
	bindings := make([]binding, 0, len(projection))
	fields := make([]memory.Field, 0, len(projection))
	for _, name := range projection {
		b := byName[name]
		bindings = append(bindings, b)
		fields = append(fields, b.field)
	}
	var totalRows, selectedRows int64
	parts := make([]source.Partition, 0, len(s.allParts))
	for _, part := range s.allParts {
		totalRows += part.Rows
		keep, err := s.recordMayMatch(part.ID, pruningPredicate)
		if err != nil {
			return nil, source.PushdownReport{}, err
		}
		if !keep {
			continue
		}
		parts = append(parts, part)
		selectedRows += part.Rows
	}
	report := source.PushdownReport{
		Projection:         append([]string(nil), projection...),
		TotalPartitions:    len(s.allParts),
		SelectedPartitions: len(parts),
		PrunedPartitions:   len(s.allParts) - len(parts),
		TotalRows:          totalRows,
		SelectedRows:       selectedRows,
		PrunedRows:         totalRows - selectedRows,
	}
	if req.Predicate != nil {
		report.Predicate = req.Predicate.String()
	}
	out := *s
	out.bindings = bindings
	out.schema = memory.NewSchema(fields...)
	out.parts = parts
	out.report = report
	return &out, report, nil
}

func cachePhysicalValues(b *binding, partition int, arr arrow.Array) error {
	switch a := arr.(type) {
	case *array.Int8, *array.Int16, *array.Uint8, *array.Uint16, *array.Float32:
		// Native-width arrays are bound directly by bindTyped; no widened cache
		// is retained here.
	case *array.Date32:
		out := make([]int32, len(a.Values()))
		for i, v := range a.Values() {
			out[i] = int32(v)
		}
		b.i32[partition] = out
	case *array.Time32:
		out := make([]int32, len(a.Values()))
		for i, v := range a.Values() {
			out[i] = int32(v)
		}
		b.i32[partition] = out
	case *array.Date64:
		out := make([]int64, len(a.Values()))
		for i, v := range a.Values() {
			out[i] = int64(v)
		}
		b.i64[partition] = out
	case *array.Timestamp:
		typ, ok := a.DataType().(*arrow.TimestampType)
		if !ok {
			return fmt.Errorf("timestamp array has invalid data type %T", a.DataType())
		}
		if typ.TimeZone != "" && typ.TimeZone != "UTC" && typ.TimeZone != "Etc/UTC" {
			return fmt.Errorf("timestamp timezone %q is not supported; use UTC or no timezone", typ.TimeZone)
		}
		scale, err := arrowTimeUnitScale(typ.Unit, "timestamp")
		if err != nil {
			return err
		}
		out := make([]int64, len(a.Values()))
		for i, v := range a.Values() {
			ns, err := scaleTimestampToNanos(int64(v), scale)
			if err != nil {
				return fmt.Errorf("timestamp row %d: %w", i, err)
			}
			out[i] = ns
		}
		b.i64[partition] = out
	case *array.Time64:
		out := make([]int64, len(a.Values()))
		for i, v := range a.Values() {
			out[i] = int64(v)
		}
		b.i64[partition] = out
	case *array.Duration:
		typ, ok := a.DataType().(*arrow.DurationType)
		if !ok {
			return fmt.Errorf("duration array has invalid data type %T", a.DataType())
		}
		scale, err := arrowTimeUnitScale(typ.Unit, "duration")
		if err != nil {
			return err
		}
		out := make([]int64, len(a.Values()))
		for i, v := range a.Values() {
			ns, err := scaleTimestampToNanos(int64(v), scale)
			if err != nil {
				return fmt.Errorf("duration row %d: %w", i, err)
			}
			out[i] = ns
		}
		b.i64[partition] = out
	}
	return nil
}

func arrowTimeUnitScale(unit arrow.TimeUnit, kind string) (int64, error) {
	switch unit {
	case arrow.Second:
		return int64(1e9), nil
	case arrow.Millisecond:
		return int64(1e6), nil
	case arrow.Microsecond:
		return int64(1e3), nil
	case arrow.Nanosecond:
		return 1, nil
	default:
		return 0, fmt.Errorf("%s unit %s is not supported", kind, unit)
	}
}

func scaleTimestampToNanos(value, scale int64) (int64, error) {
	if scale <= 0 {
		return 0, fmt.Errorf("timestamp scale %d is invalid", scale)
	}
	if value > math.MaxInt64/scale || value < math.MinInt64/scale {
		return 0, fmt.Errorf("nanosecond normalization overflows")
	}
	return value * scale, nil
}

func memoryField(f arrow.Field) memory.Field {
	out := memory.Field{Name: f.Name, Nullable: f.Nullable, Encoding: memory.EncodingPlain, ArrowType: f.Type.String()}
	dt := f.Type
	if dict, ok := dt.(*arrow.DictionaryType); ok {
		out.Encoding = memory.EncodingDictionary
		dt = dict.ValueType
		// The Arrow source preserves every dictionary array, but only UTF-8
		// dictionaries currently have a specialized stable-code execution path.
		if dt.ID() != arrow.STRING {
			out.Type = memory.TypeArrow
			return out
		}
	}
	switch dt.ID() {
	case arrow.BOOL:
		out.Type = memory.TypeBool
	case arrow.INT8:
		out.Type = memory.TypeInt8
	case arrow.INT16:
		out.Type = memory.TypeInt16
	case arrow.INT32:
		out.Type = memory.TypeInt32
	case arrow.INT64:
		out.Type = memory.TypeInt64
	case arrow.UINT8:
		out.Type = memory.TypeUint8
	case arrow.UINT16:
		out.Type = memory.TypeUint16
	case arrow.UINT32:
		out.Type = memory.TypeUint32
	case arrow.UINT64:
		out.Type = memory.TypeUint64
	case arrow.FLOAT32:
		out.Type = memory.TypeFloat32
	case arrow.FLOAT64:
		out.Type = memory.TypeFloat64
	case arrow.STRING:
		out.Type = memory.TypeString
		if out.Encoding == memory.EncodingPlain {
			out.Encoding = memory.EncodingView
		}
	case arrow.BINARY:
		out.Type = memory.TypeBinary
		if out.Encoding == memory.EncodingPlain {
			out.Encoding = memory.EncodingView
		}
	case arrow.LARGE_STRING, arrow.STRING_VIEW, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		out.Type = memory.TypeArrow
	case arrow.DATE32:
		out.Type = memory.TypeDate32
	case arrow.DATE64:
		out.Type = memory.TypeDate64
	case arrow.TIMESTAMP:
		out.Type = memory.TypeTimestamp
	case arrow.TIME32:
		out.Type = memory.TypeTime32
	case arrow.TIME64:
		out.Type = memory.TypeTime64
	case arrow.DURATION:
		out.Type = memory.TypeDuration
	case arrow.LIST:
		list, ok := dt.(*arrow.ListType)
		if !ok || list.Elem().ID() != arrow.STRING {
			out.Type = memory.TypeArrow
			break
		}
		out.Type = memory.TypeList
		out.Encoding = memory.EncodingView
		elem := list.ElemField()
		out.Children = []memory.Field{{Name: "item", Type: memory.TypeString, Nullable: elem.Nullable, Encoding: memory.EncodingView, ArrowType: elem.Type.String()}}
	case arrow.STRUCT:
		structType, ok := dt.(*arrow.StructType)
		if !ok {
			out.Type = memory.TypeArrow
			break
		}
		out.Type = memory.TypeStruct
		out.Encoding = memory.EncodingView
		children := structType.Fields()
		out.Children = make([]memory.Field, len(children))
		for i, child := range children {
			out.Children[i] = memoryField(child)
		}
	case arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		out.Type = memory.TypeDecimal
	default:
		out.Type = memory.TypeArrow
	}
	return out
}

func arrowStringListVector(a *array.List, parentValidity memory.Bitmap) (*memory.StringListVector, error) {
	values, ok := a.ListValues().(*array.String)
	if !ok {
		return nil, fmt.Errorf("list child array is %T, want *array.String", a.ListValues())
	}
	rows := a.Len()
	base, terminal := int64(0), int64(0)
	if rows > 0 {
		base, _ = a.ValueOffsets(0)
		_, terminal = a.ValueOffsets(rows - 1)
	}
	if base < 0 || terminal < base || terminal > int64(values.Len()) {
		return nil, fmt.Errorf("list child range [%d,%d) exceeds %d values", base, terminal, values.Len())
	}
	listOffsets := make([]uint32, rows+1)
	for row := 0; row < rows; row++ {
		start, end := a.ValueOffsets(row)
		if start < base || end < start || end > terminal {
			return nil, fmt.Errorf("list row %d offsets [%d,%d) outside [%d,%d)", row, start, end, base, terminal)
		}
		listOffsets[row] = uint32(start - base)
		listOffsets[row+1] = uint32(end - base)
	}
	childRows := int(terminal - base)
	rawOffsets := values.ValueOffsets()
	if int(terminal) >= len(rawOffsets) {
		return nil, fmt.Errorf("list child terminal %d exceeds string offsets", terminal)
	}
	bufferBase := rawOffsets[0]
	byteBase, byteEnd := rawOffsets[base], rawOffsets[terminal]
	data := values.ValueBytes()
	lo, hi := int(byteBase-bufferBase), int(byteEnd-bufferBase)
	if lo < 0 || hi < lo || hi > len(data) {
		return nil, fmt.Errorf("list child byte range [%d,%d) exceeds %d bytes", lo, hi, len(data))
	}
	childOffsets := make([]uint32, childRows+1)
	for i := 0; i <= childRows; i++ {
		childOffsets[i] = uint32(rawOffsets[int(base)+i] - byteBase)
	}
	var childValidity memory.Bitmap
	for i := 0; i < childRows; i++ {
		if !values.IsValid(int(base) + i) {
			if childValidity == nil {
				childValidity = memory.EnsureBitmap(nil, childRows)
				for j := 0; j < i; j++ {
					memory.Set(childValidity, j)
				}
			}
			continue
		}
		if childValidity != nil {
			memory.Set(childValidity, i)
		}
	}
	v := &memory.StringListVector{
		Offsets: listOffsets,
		Values: &memory.StringViewVector{
			Offsets:  childOffsets,
			Data:     data[lo:hi],
			Validity: childValidity,
			Encoding: memory.EncodingView,
		},
		Validity: parentValidity,
		Encoding: memory.EncodingView,
	}
	if err := v.Validate(rows); err != nil {
		return nil, err
	}
	return v, nil
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

func normalizeDictionary(records []arrow.RecordBatch, column int, name string) (*dictionaryInfo, error) {
	info := &dictionaryInfo{indices: make([][]uint32, len(records))}
	codes := make(map[string]uint32)
	values := make([][]byte, 0)
	for ri, rec := range records {
		d, ok := rec.Column(column).(*array.Dictionary)
		if !ok {
			return nil, fmt.Errorf("arrowrecord: dictionary field %q is not an array.Dictionary", name)
		}
		dict := d.Dictionary()
		translation := make([]uint32, dict.Len())
		for i := 0; i < dict.Len(); i++ {
			value, err := dictionaryValueBytes(dict, i)
			if err != nil {
				return nil, fmt.Errorf("arrowrecord: dictionary %q: %w", name, err)
			}
			key := string(value) // dictionary-cardinality allocation, never per row
			code, exists := codes[key]
			if !exists {
				code = uint32(len(values))
				codes[key] = code
				values = append(values, append([]byte(nil), value...))
			}
			translation[i] = code
		}
		local, err := dictionaryIndices(d)
		if err != nil {
			return nil, fmt.Errorf("arrowrecord: dictionary %q indices: %w", name, err)
		}
		normalized := make([]uint32, len(local))
		for row, code := range local {
			if int(code) >= len(translation) {
				return nil, fmt.Errorf("arrowrecord: dictionary code %d out of range for %q", code, name)
			}
			normalized[row] = translation[code]
		}
		info.indices[ri] = normalized
	}
	info.offsets = make([]uint32, len(values)+1)
	for i, value := range values {
		info.data = append(info.data, value...)
		info.offsets[i+1] = uint32(len(info.data))
	}
	h := sha256.New()
	h.Write([]byte(name))
	var nbuf [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(nbuf[:], uint32(len(value)))
		h.Write(nbuf[:])
		h.Write(value)
	}
	info.domain = fmt.Sprintf("arrow:%s:%x", name, h.Sum(nil)[:12])
	return info, nil
}

func dictionaryValueBytes(dict arrow.Array, i int) ([]byte, error) {
	switch a := dict.(type) {
	case *array.String:
		return []byte(a.Value(i)), nil
	case *array.LargeString:
		return []byte(a.Value(i)), nil
	case *array.Binary:
		return a.Value(i), nil
	case *array.LargeBinary:
		return a.Value(i), nil
	default:
		return nil, fmt.Errorf("value type %s is preserved as opaque dictionary, not normalized string", dict.DataType())
	}
}

type int64Stats struct {
	min, max  int64
	nulls     int
	rows      int
	hasValues bool
}

type truthSet uint8

const (
	truthTrue truthSet = 1 << iota
	truthFalse
	truthNull
	truthAny = truthTrue | truthFalse | truthNull
)

func (s *Source) recordMayMatch(partition int, predicate expr.Expr) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	truth, err := s.recordTruth(partition, predicate)
	if err != nil {
		return false, err
	}
	return truth&truthTrue != 0, nil
}

func (s *Source) recordTruth(partition int, predicate expr.Expr) (truthSet, error) {
	switch n := predicate.(type) {
	case expr.Binary:
		switch n.Op {
		case expr.OpAnd, expr.OpOr:
			left, err := s.recordTruth(partition, n.Left)
			if err != nil {
				return truthAny, err
			}
			right, err := s.recordTruth(partition, n.Right)
			if err != nil {
				return truthAny, err
			}
			return combineTruth(n.Op, left, right), nil
		case expr.OpGT, expr.OpGE, expr.OpLT, expr.OpLE, expr.OpEQ:
			col, ok := n.Left.(expr.Column)
			if !ok {
				return truthAny, nil
			}
			lit, ok := n.Right.(expr.Int64Literal)
			if !ok {
				return truthAny, nil
			}
			st, ok, err := s.partitionInt64Stats(partition, col.Name)
			if err != nil {
				return truthAny, err
			}
			if !ok {
				return truthAny, nil
			}
			return compareStats(st, n.Op, lit.Value), nil
		default:
			return truthAny, nil
		}
	case expr.Unary:
		switch n.Op {
		case expr.OpNot:
			v, err := s.recordTruth(partition, n.Value)
			if err != nil {
				return truthAny, err
			}
			return notTruth(v), nil
		case expr.OpIsNull, expr.OpIsNotNull:
			col, ok := n.Value.(expr.Column)
			if !ok {
				return truthAny, nil
			}
			st, ok, err := s.partitionInt64Stats(partition, col.Name)
			if err != nil {
				return truthAny, err
			}
			if !ok {
				return truthAny, nil
			}
			var out truthSet
			if st.nulls > 0 {
				out |= truthTrue
			}
			if st.nulls < st.rows {
				out |= truthFalse
			}
			if n.Op == expr.OpIsNotNull {
				out = notTruth(out)
			}
			return out, nil
		default:
			return truthAny, nil
		}
	default:
		return truthAny, nil
	}
}

func (s *Source) partitionInt64Stats(partition int, name string) (int64Stats, bool, error) {
	if partition < 0 || partition >= len(s.owner.records) {
		return int64Stats{}, false, fmt.Errorf("arrowrecord: partition %d out of range", partition)
	}
	bindingIndex := -1
	for _, b := range s.allBindings {
		if b.name == name {
			if b.field.Type != memory.TypeInt64 || b.field.Encoding == memory.EncodingDictionary {
				return int64Stats{}, false, nil
			}
			bindingIndex = b.index
			break
		}
	}
	if bindingIndex < 0 {
		return int64Stats{}, false, nil
	}
	s.owner.statsMu.Lock()
	if byCol := s.owner.stats[partition]; byCol != nil {
		if st, ok := byCol[name]; ok {
			s.owner.statsMu.Unlock()
			return st, true, nil
		}
	}
	s.owner.statsMu.Unlock()
	a, ok := s.owner.records[partition].Column(bindingIndex).(*array.Int64)
	if !ok {
		return int64Stats{}, false, nil
	}
	st := int64Stats{rows: a.Len(), nulls: a.NullN()}
	for i, v := range a.Values() {
		if a.IsNull(i) {
			continue
		}
		if !st.hasValues {
			st.min, st.max, st.hasValues = v, v, true
			continue
		}
		if v < st.min {
			st.min = v
		}
		if v > st.max {
			st.max = v
		}
	}
	s.owner.statsMu.Lock()
	byCol := s.owner.stats[partition]
	if byCol == nil {
		byCol = make(map[string]int64Stats)
		s.owner.stats[partition] = byCol
	}
	byCol[name] = st
	s.owner.statsMu.Unlock()
	return st, true, nil
}

func compareStats(st int64Stats, op expr.Op, lit int64) truthSet {
	var out truthSet
	if st.nulls > 0 {
		out |= truthNull
	}
	if !st.hasValues {
		return out
	}
	var valid truthSet
	switch op {
	case expr.OpGT:
		if st.max <= lit {
			valid = truthFalse
		} else if st.min > lit {
			valid = truthTrue
		} else {
			valid = truthTrue | truthFalse
		}
	case expr.OpGE:
		if st.max < lit {
			valid = truthFalse
		} else if st.min >= lit {
			valid = truthTrue
		} else {
			valid = truthTrue | truthFalse
		}
	case expr.OpLT:
		if st.min >= lit {
			valid = truthFalse
		} else if st.max < lit {
			valid = truthTrue
		} else {
			valid = truthTrue | truthFalse
		}
	case expr.OpLE:
		if st.min > lit {
			valid = truthFalse
		} else if st.max <= lit {
			valid = truthTrue
		} else {
			valid = truthTrue | truthFalse
		}
	case expr.OpEQ:
		if lit < st.min || lit > st.max {
			valid = truthFalse
		} else if st.min == st.max && st.min == lit {
			valid = truthTrue
		} else {
			valid = truthTrue | truthFalse
		}
	default:
		valid = truthTrue | truthFalse
	}
	return out | valid
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

func combineTruth(op expr.Op, a, b truthSet) truthSet {
	var out truthSet
	values := []truthSet{truthTrue, truthFalse, truthNull}
	for _, av := range values {
		if a&av == 0 {
			continue
		}
		for _, bv := range values {
			if b&bv == 0 {
				continue
			}
			out |= truthPair(op, av, bv)
		}
	}
	return out
}

func truthPair(op expr.Op, a, b truthSet) truthSet {
	if op == expr.OpAnd {
		if a == truthFalse || b == truthFalse {
			return truthFalse
		}
		if a == truthTrue && b == truthTrue {
			return truthTrue
		}
		return truthNull
	}
	if a == truthTrue || b == truthTrue {
		return truthTrue
	}
	if a == truthFalse && b == truthFalse {
		return truthFalse
	}
	return truthNull
}
