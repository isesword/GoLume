package inmemory

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// Source is a zero-copy test/embedding source. Batches are treated as immutable
// and Reader exposes shallow vector views into them.
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

// NewReader creates an independent reader over the immutable batches.
func (s *Source) NewReader(_ context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &reader{s: s}, nil
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

type reader struct{ s *Source }

// Close releases reader resources; the in-memory reader owns no external resources.
func (*reader) Close() error { return nil }

// ReadPartition binds dst to immutable zero-copy vector views for p.
func (r *reader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.ID < 0 || p.ID >= len(r.s.batches) {
		return fmt.Errorf("partition %d out of range", p.ID)
	}
	src := r.s.batches[p.ID]
	dst.ClearColumns()
	dst.Rows = src.Rows
	for k, v := range src.Int8s {
		dst.Int8s[k] = v
	}
	for k, v := range src.Int16s {
		dst.Int16s[k] = v
	}
	for k, v := range src.Uint8s {
		dst.Uint8s[k] = v
	}
	for k, v := range src.Uint16s {
		dst.Uint16s[k] = v
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = v
	}
	if len(r.s.projection) == 0 {
		for k, v := range src.Int64s {
			dst.Int64s[k] = v
		}
		for k, v := range src.Int32s {
			dst.Int32s[k] = v
		}
		for k, v := range src.Uint32s {
			dst.Uint32s[k] = v
		}
		for k, v := range src.Uint64s {
			dst.Uint64s[k] = v
		}
		for k, v := range src.Float64s {
			dst.Float64s[k] = v
		}
		for k, v := range src.Strings {
			dst.Strings[k] = v
		}
		for k, v := range src.DictStrings {
			dst.DictStrings[k] = v
		}
		for k, v := range src.StringLists {
			dst.StringLists[k] = v
		}
		for k, v := range src.Structs {
			dst.Structs[k] = v
		}
		for k, v := range src.Opaque {
			dst.Opaque[k] = v
		}
		return nil
	}
	for _, k := range r.s.projection {
		if v, ok := src.Int8s[k]; ok {
			dst.Int8s[k] = v
			continue
		}
		if v, ok := src.Int16s[k]; ok {
			dst.Int16s[k] = v
			continue
		}
		if v, ok := src.Uint8s[k]; ok {
			dst.Uint8s[k] = v
			continue
		}
		if v, ok := src.Uint16s[k]; ok {
			dst.Uint16s[k] = v
			continue
		}
		if v, ok := src.Float32s[k]; ok {
			dst.Float32s[k] = v
			continue
		}
		if v, ok := src.Int64s[k]; ok {
			dst.Int64s[k] = v
			continue
		}
		if v, ok := src.Int32s[k]; ok {
			dst.Int32s[k] = v
			continue
		}
		if v, ok := src.Uint32s[k]; ok {
			dst.Uint32s[k] = v
			continue
		}
		if v, ok := src.Uint64s[k]; ok {
			dst.Uint64s[k] = v
			continue
		}
		if v, ok := src.Float64s[k]; ok {
			dst.Float64s[k] = v
			continue
		}
		if v, ok := src.Strings[k]; ok {
			dst.Strings[k] = v
			continue
		}
		if v, ok := src.DictStrings[k]; ok {
			dst.DictStrings[k] = v
			continue
		}
		if v, ok := src.StringLists[k]; ok {
			dst.StringLists[k] = v
			continue
		}
		if v, ok := src.Structs[k]; ok {
			dst.Structs[k] = v
			continue
		}
		if v, ok := src.Opaque[k]; ok {
			dst.Opaque[k] = v
			continue
		}
		return fmt.Errorf("projected column %q not present in batch", k)
	}
	return nil
}
