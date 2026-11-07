package inmemory

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/internal/batchview"
)

// Source is a zero-copy test/embedding source. Batches are treated as immutable
// and each native stream exposes bounded shallow vector views into them.
type Source struct {
	identity   *byte
	schema     memory.Schema
	batches    []*memory.Batch
	parts      []source.Partition
	stats      source.Stats
	dicts      map[string]source.Dictionary
	projection []string
	ordering   []source.OrderingColumn
}

// New constructs a zero-copy immutable source over batches.
func New(schema memory.Schema, batches ...*memory.Batch) (*Source, error) {
	s := &Source{identity: new(byte), schema: schema, batches: append([]*memory.Batch(nil), batches...), dicts: make(map[string]source.Dictionary)}
	for i, b := range s.batches {
		if b == nil {
			return nil, fmt.Errorf("batch %d is nil", i)
		}
		for _, field := range schema.Fields {
			if field.Type == memory.TypeStruct {
				v, ok := b.Structs[field.Name]
				if !ok {
					return nil, fmt.Errorf("batch %d struct column %q is missing", i, field.Name)
				}
				if err := v.Validate(field, b.Rows); err != nil {
					return nil, fmt.Errorf("batch %d column %q: %w", i, field.Name, err)
				}
				continue
			}
			if field.Type != memory.TypeList {
				continue
			}
			if err := field.ValidateListString(); err != nil {
				return nil, err
			}
			v, ok := b.StringLists[field.Name]
			if !ok {
				return nil, fmt.Errorf("batch %d list<string> column %q is missing", i, field.Name)
			}
			if err := v.Validate(b.Rows); err != nil {
				return nil, fmt.Errorf("batch %d column %q: %w", i, field.Name, err)
			}
			if !field.Nullable && memory.HasNulls(v.Validity, b.Rows) {
				return nil, fmt.Errorf("batch %d non-nullable list<string> column %q contains NULL", i, field.Name)
			}
			if !field.Children[0].Nullable && memory.HasNulls(v.Values.Validity, v.Values.Len()) {
				return nil, fmt.Errorf("batch %d list<string> column %q has NULL in non-nullable child", i, field.Name)
			}
		}
		s.parts = append(s.parts, source.Partition{ID: i, Rows: int64(b.Rows)})
		s.stats.Rows += int64(b.Rows)
		s.stats.Bytes += memory.BatchBytes(b)
	}
	s.stats.Partitions = len(s.parts)
	return s, nil
}

// WithDictionary attaches a stable dictionary contract for column and returns s.
func (s *Source) WithDictionary(column string, d source.Dictionary) *Source {
	s.dicts[column] = d
	return s
}

// WithOrdering declares a global ordering contract for immutable batches.
// Callers are responsible for ensuring the concatenated batch sequence obeys
// it; aggregation and merge operators may rely on the declaration directly.
func (s *Source) WithOrdering(columns ...source.OrderingColumn) *Source {
	s.ordering = append([]source.OrderingColumn(nil), columns...)
	return s
}

// Ordering returns the declared global ordering prefix.
func (s *Source) Ordering() []source.OrderingColumn {
	return append([]source.OrderingColumn(nil), s.ordering...)
}

// Schema returns the source schema.
func (s *Source) Schema() memory.Schema { return s.schema }

// Replayability reports that immutable in-memory batches are a stable snapshot.
func (*Source) Replayability() source.Replayability { return source.ReplayStable }

// PlanIdentity is stable across immutable projection/pushdown clones.
func (s *Source) PlanIdentity() string { return fmt.Sprintf("inmemory:%p", s.identity) }

// Stats returns precomputed source statistics.
func (s *Source) Stats(context.Context) (source.Stats, error) { return s.stats, nil }

// Partitions returns a copy of the immutable batch partition list.
func (s *Source) Partitions(context.Context) ([]source.Partition, error) {
	return append([]source.Partition(nil), s.parts...), nil
}

// NewBatchStream creates an independent stream over immutable batches.
func (s *Source) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if s == nil {
		return nil, fmt.Errorf("inmemory: nil source")
	}
	return s.NewBatchStreamWithPartitions(ctx, opts, s.parts)
}

// NewBatchStreamWithPartitions borrows the exact immutable partition sequence.
// An empty sequence produces no batches; it never falls back to all partitions.
func (s *Source) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if s == nil {
		return nil, fmt.Errorf("inmemory: nil source")
	}
	if ctx == nil {
		return nil, fmt.Errorf("inmemory: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	for _, part := range parts {
		if part.ID < 0 || part.ID >= len(s.batches) {
			return nil, fmt.Errorf("inmemory: partition %d out of range", part.ID)
		}
	}
	return &batchStream{s: s, parts: parts, opts: opts.Normalize(), projected: memory.NewBatch()}, nil
}

// ApplyPushdown returns a shallow projected source. In-memory sources have no
// persistent row-group statistics, so predicates remain residual and do not
// prune partitions.
func (s *Source) ApplyPushdown(_ context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	projection := append([]string(nil), req.Projection...)
	if len(projection) == 0 {
		for _, f := range s.schema.Fields {
			projection = append(projection, f.Name)
		}
	}
	fields := make([]memory.Field, 0, len(projection))
	for _, name := range projection {
		f, ok := s.schema.Field(name)
		if !ok {
			return nil, source.PushdownReport{}, fmt.Errorf("projection column %q not found", name)
		}
		fields = append(fields, f)
	}
	out := *s
	out.schema = memory.NewSchema(fields...)
	out.projection = projection
	visible := make(map[string]struct{}, len(projection))
	for _, name := range projection {
		visible[name] = struct{}{}
	}
	out.ordering = nil
	for _, key := range s.ordering {
		if _, ok := visible[key.Name]; !ok {
			break
		}
		out.ordering = append(out.ordering, key)
	}
	report := source.PushdownReport{
		Projection: append([]string(nil), projection...), TotalPartitions: len(s.parts), SelectedPartitions: len(s.parts),
		TotalRows: s.stats.Rows, SelectedRows: s.stats.Rows, TotalBytes: s.stats.Bytes, SelectedBytes: s.stats.Bytes,
	}
	if req.Predicate != nil {
		report.Predicate = req.Predicate.String()
	}
	return &out, report, nil
}

// Dictionary returns stable dictionary metadata previously attached to column.
func (s *Source) Dictionary(_ context.Context, column string) (source.Dictionary, error) {
	d, ok := s.dicts[column]
	if !ok {
		return source.Dictionary{}, fmt.Errorf("dictionary %q not available", column)
	}
	return d, nil
}

type batchStream struct {
	s                 *Source
	parts             []source.Partition
	opts              source.ReadOptions
	next, offset      int
	projected         *memory.Batch
	view              batchview.View
	closed, exhausted bool
	terminalErr       error
}

// Close releases the stream views and owned resources exactly once.
func (s *batchStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.view.Release()
	s.clearProjection()
	return nil
}

func (s *batchStream) fail(err error) (bool, error) {
	s.view.Release()
	s.clearProjection()
	s.terminalErr = err
	return false, err
}

// Next binds the next bounded view or returns the terminal stream result.
func (s *batchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if dst == nil {
		return false, fmt.Errorf("inmemory: nil destination")
	}
	dst.ClearColumns()
	s.view.Release()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.terminalErr != nil {
		return false, s.terminalErr
	}
	if s.exhausted {
		return false, nil
	}
	if ctx == nil {
		return s.fail(fmt.Errorf("inmemory: nil context"))
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if s.next == len(s.parts) {
		s.exhausted = true
		s.clearProjection()
		return false, nil
	}
	batch := s.s.batches[s.parts[s.next].ID]
	if s.offset == 0 {
		if err := s.bindProjection(batch); err != nil {
			return s.fail(err)
		}
	}
	rows, err := s.view.Bind(ctx, dst, s.projected, s.offset, s.opts)
	if err != nil {
		return s.fail(err)
	}
	s.offset += rows
	if s.offset == batch.Rows {
		s.next++
		s.offset = 0
	}
	return true, nil
}

// bindProjection selects schema-visible vectors without allocating value buffers.
func (s *batchStream) bindProjection(src *memory.Batch) error {
	dst := s.projected
	s.clearProjection()
	dst.Rows = src.Rows
	for _, field := range s.s.schema.Fields {
		name := field.Name
		if v, ok := src.Int8s[name]; ok {
			dst.Int8s[name] = v
			continue
		}
		if v, ok := src.Int16s[name]; ok {
			dst.Int16s[name] = v
			continue
		}
		if v, ok := src.Int32s[name]; ok {
			dst.Int32s[name] = v
			continue
		}
		if v, ok := src.Int64s[name]; ok {
			dst.Int64s[name] = v
			continue
		}
		if v, ok := src.Uint8s[name]; ok {
			dst.Uint8s[name] = v
			continue
		}
		if v, ok := src.Uint16s[name]; ok {
			dst.Uint16s[name] = v
			continue
		}
		if v, ok := src.Uint32s[name]; ok {
			dst.Uint32s[name] = v
			continue
		}
		if v, ok := src.Uint64s[name]; ok {
			dst.Uint64s[name] = v
			continue
		}
		if v, ok := src.Float32s[name]; ok {
			dst.Float32s[name] = v
			continue
		}
		if v, ok := src.Float64s[name]; ok {
			dst.Float64s[name] = v
			continue
		}
		if v, ok := src.Strings[name]; ok {
			dst.Strings[name] = v
			continue
		}
		if v, ok := src.DictStrings[name]; ok {
			dst.DictStrings[name] = v
			continue
		}
		if v, ok := src.StringLists[name]; ok {
			dst.StringLists[name] = v
			continue
		}
		if v, ok := src.Structs[name]; ok {
			dst.Structs[name] = v
			continue
		}
		if v, ok := src.Opaque[name]; ok {
			copy := *v
			copy.Owned = false
			dst.Opaque[name] = &copy
			continue
		}
		return fmt.Errorf("inmemory: column %q not present in batch", name)
	}
	return nil
}

var _ source.Source = (*Source)(nil)
var _ source.PreparedBatchStreamProvider = (*Source)(nil)

// clearProjection drops borrowed headers without releasing source-owned nested
// opaque values. Only batchview owns range-local Arrow slice references.
func (s *batchStream) clearProjection() {
	clear(s.projected.Int8s)
	clear(s.projected.Int16s)
	clear(s.projected.Int32s)
	clear(s.projected.Int64s)
	clear(s.projected.Uint8s)
	clear(s.projected.Uint16s)
	clear(s.projected.Uint32s)
	clear(s.projected.Uint64s)
	clear(s.projected.Float32s)
	clear(s.projected.Float64s)
	clear(s.projected.Strings)
	clear(s.projected.DictStrings)
	clear(s.projected.StringLists)
	clear(s.projected.Structs)
	clear(s.projected.Opaque)
	s.projected.Rows = 0
}
