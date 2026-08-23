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
	schema     memory.Schema
	batches    []*memory.Batch
	parts      []source.Partition
	stats      source.Stats
	dicts      map[string]source.Dictionary
	projection []string
}

// New constructs a zero-copy immutable source over batches.
func New(schema memory.Schema, batches ...*memory.Batch) (*Source, error) {
	s := &Source{schema: schema, batches: append([]*memory.Batch(nil), batches...), dicts: make(map[string]source.Dictionary)}
	for i, b := range s.batches {
		if b == nil {
			return nil, fmt.Errorf("batch %d is nil", i)
		}
		s.parts = append(s.parts, source.Partition{ID: i, Rows: int64(b.Rows)})
		s.stats.Rows += int64(b.Rows)
	}
	s.stats.Partitions = len(s.parts)
	return s, nil
}

// WithDictionary attaches a stable dictionary contract for column and returns s.
func (s *Source) WithDictionary(column string, d source.Dictionary) *Source {
	s.dicts[column] = d
	return s
}

// Schema returns the source schema.
func (s *Source) Schema() memory.Schema { return s.schema }

// Stats returns precomputed source statistics.
func (s *Source) Stats(context.Context) (source.Stats, error) { return s.stats, nil }

// Partitions returns a copy of the immutable batch partition list.
func (s *Source) Partitions(context.Context) ([]source.Partition, error) {
	return append([]source.Partition(nil), s.parts...), nil
}

// NewReader creates an independent reader over the immutable batches.
func (s *Source) NewReader(context.Context) (source.Reader, error) { return &reader{s: s}, nil }

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
	report := source.PushdownReport{Projection: append([]string(nil), projection...), TotalPartitions: len(s.parts), SelectedPartitions: len(s.parts), TotalRows: s.stats.Rows, SelectedRows: s.stats.Rows}
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
		for k, v := range src.Opaque {
			dst.Opaque[k] = v
		}
		return nil
	}
	for _, k := range r.s.projection {
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
		if v, ok := src.Opaque[k]; ok {
			dst.Opaque[k] = v
			continue
		}
		return fmt.Errorf("projected column %q not present in batch", k)
	}
	return nil
}
