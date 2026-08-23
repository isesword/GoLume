package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

// aggregateResultSource exposes a reusable PreparedQuery as a single-partition
// columnar source so downstream lazy operators can sort, limit, project, or
// filter grouped aggregate output without changing the terminal aggregate fast
// path used by ordinary Collect/Prepare.
type aggregateResultSource struct {
	query   *engine.PreparedQuery
	schema  memory.Schema
	groupBy []string
	aggs    []expr.Aggregate
	dicts   map[string]source.Dictionary
}

func newAggregateResultSource(ctx context.Context, query *engine.PreparedQuery, input source.Source, schema memory.Schema, groupBy []string, aggs []expr.Aggregate) (*aggregateResultSource, error) {
	s := &aggregateResultSource{query: query, schema: schema, groupBy: append([]string(nil), groupBy...), aggs: append([]expr.Aggregate(nil), aggs...), dicts: make(map[string]source.Dictionary)}
	if dp, ok := input.(source.DictionaryProvider); ok {
		for _, name := range groupBy {
			f, _ := schema.Field(name)
			if f.Type == memory.TypeString && f.Encoding == memory.EncodingDictionary {
				d, err := dp.Dictionary(ctx, name)
				if err != nil {
					return nil, err
				}
				s.dicts[name] = d
			}
		}
	}
	return s, nil
}

// Schema returns the materialized aggregate output schema.
func (s *aggregateResultSource) Schema() memory.Schema { return s.schema }

// Stats returns an upper-bound row estimate derived from the prepared aggregate plan.
func (s *aggregateResultSource) Stats(context.Context) (source.Stats, error) {
	info := s.query.Plan()
	rows := int64(info.EstimatedCardinality)
	if rows <= 0 {
		rows = info.Rows
	}
	return source.Stats{Rows: rows, Partitions: 1, RowsUpperBound: true}, nil
}

// Partitions exposes the aggregate result as one downstream columnar partition.
func (s *aggregateResultSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, _ := s.Stats(ctx)
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}

// NewReader creates a reader that executes the reusable prepared aggregate once.
func (s *aggregateResultSource) NewReader(context.Context) (source.Reader, error) {
	return &aggregateResultReader{owner: s}, nil
}

// Dictionary returns stable dictionary metadata preserved from grouped dictionary keys.
func (s *aggregateResultSource) Dictionary(_ context.Context, column string) (source.Dictionary, error) {
	d, ok := s.dicts[column]
	if !ok {
		return source.Dictionary{}, fmt.Errorf("aggregate output column %q has no dictionary", column)
	}
	return d, nil
}

type aggregateResultReader struct {
	owner *aggregateResultSource
	done  bool
}

// Close releases reader resources; the aggregate result reader owns no additional resources.
func (*aggregateResultReader) Close() error { return nil }

// ReadPartition executes and materializes the single aggregate output partition.
func (r *aggregateResultReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if p.ID != 0 {
		return fmt.Errorf("aggregate output partition %d is invalid", p.ID)
	}
	if r.done {
		dst.ClearColumns()
		dst.Reset(0)
		return nil
	}
	r.done = true
	res, err := r.owner.query.Execute(ctx, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		return err
	}
	return materializeAggregateRows(dst, r.owner, res.Rows)
}

func materializeAggregateRows(dst *memory.Batch, s *aggregateResultSource, rows []engine.AggregateRow) error {
	dst.ClearColumns()
	n := len(rows)
	for gi, name := range s.groupBy {
		field, _ := s.schema.Field(name)
		switch field.Type {
		case memory.TypeInt32:
			v := &memory.Int32Vector{Values: make([]int32, n), Encoding: field.Encoding}
			if field.Nullable {
				v.Validity = memory.EnsureBitmap(nil, n)
			}
			for i, row := range rows {
				k := row.Keys[gi]
				v.Values[i] = int32(k.Int64)
				if k.Valid && v.Validity != nil {
					memory.Set(v.Validity, i)
				}
			}
			dst.Int32s[name] = v
		case memory.TypeInt64:
			v := &memory.Int64Vector{Values: make([]int64, n), Encoding: field.Encoding}
			if field.Nullable {
				v.Validity = memory.EnsureBitmap(nil, n)
			}
			for i, row := range rows {
				k := row.Keys[gi]
				v.Values[i] = k.Int64
				if k.Valid && v.Validity != nil {
					memory.Set(v.Validity, i)
				}
			}
			dst.Int64s[name] = v
		case memory.TypeUint32:
			v := &memory.Uint32Vector{Values: make([]uint32, n), Encoding: field.Encoding}
			if field.Nullable {
				v.Validity = memory.EnsureBitmap(nil, n)
			}
			for i, row := range rows {
				k := row.Keys[gi]
				v.Values[i] = uint32(k.Uint64)
				if k.Valid && v.Validity != nil {
					memory.Set(v.Validity, i)
				}
			}
			dst.Uint32s[name] = v
		case memory.TypeString:
			if field.Encoding == memory.EncodingDictionary {
				d := s.dicts[name]
				v := &memory.DictionaryStringVector{Indices: make([]uint32, n), Offsets: d.Offsets, Data: d.Data, DictionaryDomain: d.Domain, Encoding: memory.EncodingDictionary}
				if field.Nullable {
					v.Validity = memory.EnsureBitmap(nil, n)
				}
				for i, row := range rows {
					k := row.Keys[gi]
					v.Indices[i] = uint32(k.Uint64)
					if k.Valid && v.Validity != nil {
						memory.Set(v.Validity, i)
					}
				}
				dst.DictStrings[name] = v
			} else {
				v := &memory.StringViewVector{Offsets: make([]uint32, n+1), Encoding: memory.EncodingView}
				if field.Nullable {
					v.Validity = memory.EnsureBitmap(nil, n)
				}
				for i, row := range rows {
					k := row.Keys[gi]
					if k.Valid {
						v.Data = append(v.Data, k.String...)
						if v.Validity != nil {
							memory.Set(v.Validity, i)
						}
					}
					v.Offsets[i+1] = uint32(len(v.Data))
				}
				dst.Strings[name] = v
			}
		default:
			return fmt.Errorf("aggregate output group type %s for %q is not materializable", field.Type, name)
		}
	}
	for ai, agg := range s.aggs {
		name := agg.Alias
		if name == "" {
			name = fmt.Sprintf("agg_%d", ai)
		}
		field, _ := s.schema.Field(name)
		switch field.Type {
		case memory.TypeUint64:
			v := &memory.Uint64Vector{Values: make([]uint64, n), Encoding: memory.EncodingPlain}
			for i, row := range rows {
				if ai >= len(row.Aggregates) {
					return fmt.Errorf("aggregate row %d missing value %d", i, ai)
				}
				v.Values[i] = uint64(row.Aggregates[ai].Int64)
			}
			dst.Uint64s[name] = v
		case memory.TypeInt64:
			v := &memory.Int64Vector{Values: make([]int64, n), Encoding: memory.EncodingPlain}
			if field.Nullable {
				v.Validity = memory.EnsureBitmap(nil, n)
			}
			for i, row := range rows {
				a := row.Aggregates[ai]
				v.Values[i] = a.Int64
				if a.Valid && v.Validity != nil {
					memory.Set(v.Validity, i)
				}
			}
			dst.Int64s[name] = v
		case memory.TypeFloat64:
			v := &memory.Float64Vector{Values: make([]float64, n), Encoding: memory.EncodingPlain}
			if field.Nullable {
				v.Validity = memory.EnsureBitmap(nil, n)
			}
			for i, row := range rows {
				a := row.Aggregates[ai]
				v.Values[i] = a.Float64
				if a.Valid && v.Validity != nil {
					memory.Set(v.Validity, i)
				}
			}
			dst.Float64s[name] = v
		default:
			return fmt.Errorf("aggregate output type %s for %q is not materializable", field.Type, name)
		}
	}
	dst.Reset(n)
	return nil
}
