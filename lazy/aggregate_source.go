package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source"
)

// aggregateResultSource exposes a reusable compiled aggregate as a native
// physical stream so downstream lazy operators can sort, limit, project, or
// filter grouped output without crossing an engine.Result boundary.
type aggregateResultSource struct {
	query     *engine.PreparedQuery
	schema    memory.Schema
	groupBy   []string
	aggs      []expr.Aggregate
	dicts     map[string]source.Dictionary
	spillable bool
}

func newAggregateResultSource(ctx context.Context, query *engine.PreparedQuery, input source.Source, schema memory.Schema, groupBy []string, aggs []expr.Aggregate, spillable bool) (*aggregateResultSource, error) {
	s := &aggregateResultSource{query: query, schema: schema, groupBy: append([]string(nil), groupBy...), aggs: append([]expr.Aggregate(nil), aggs...), dicts: make(map[string]source.Dictionary), spillable: spillable}
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

// Schema returns the stable grouped-output schema.
func (s *aggregateResultSource) Schema() memory.Schema { return s.schema }

// Replayability reports that each prepared aggregate execution is rebuilt from
// the immutable child snapshot and can safely perform a bounded second pass.
func (s *aggregateResultSource) Replayability() source.Replayability { return source.ReplayStable }

// StreamPlan reports whether this aggregate uses resident, sorted, or spillable state.
func (s *aggregateResultSource) StreamPlan() source.StreamPlanNode {
	class := source.StreamBlocking
	mode := "resident-stream"
	if s.spillable {
		class = source.StreamSpillable
		mode = "radix-spill"
	} else if s.query.Plan().Strategy.Kind == planner.StrategySorted {
		class = source.StreamBounded
		mode = "sorted-adjacent"
	}
	return source.StreamPlanNode{
		Operator: "Aggregate", Class: class,
		Detail: fmt.Sprintf("groups=%d aggregates=%d strategy=%s", len(s.groupBy), len(s.aggs), mode),
	}
}

// Stats returns the estimated grouped-output cardinality.
func (s *aggregateResultSource) Stats(context.Context) (source.Stats, error) {
	info := s.query.Plan()
	rows := int64(info.EstimatedCardinality)
	if rows <= 0 {
		rows = info.Rows
	}
	return source.Stats{Rows: rows, Partitions: 1, RowsUpperBound: true}, nil
}

// Partitions returns the single logical aggregate output partition.
func (s *aggregateResultSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, _ := s.Stats(ctx)
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}

// NewBatchStream selects only a physical strategy. Every strategy consumes the
// same native child graph and emits chunked columnar batches.
func (s *aggregateResultSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if s.spillable {
		return s.query.NewSpillableAggregateStream(ctx, s.schema, 0)
	}
	if s.query.IsSortedAggregate() {
		return s.query.NewSortedAggregateStream(ctx, s.schema, 0)
	}
	return s.query.NewResidentAggregateStream(ctx, s.schema, 0)
}

// NewBatchStreamWithPartitions opens the aggregate stream for the requested output partitions.
func (s *aggregateResultSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, _ []source.Partition) (source.BatchStream, error) {
	return s.NewBatchStream(ctx, opts)
}

// Dictionary returns stable dictionary metadata for a grouped string column.
func (s *aggregateResultSource) Dictionary(_ context.Context, column string) (source.Dictionary, error) {
	d, ok := s.dicts[column]
	if !ok {
		return source.Dictionary{}, fmt.Errorf("aggregate output column %q has no dictionary", column)
	}
	return d, nil
}

var _ source.Source = (*aggregateResultSource)(nil)
var _ source.BatchStreamProvider = (*aggregateResultSource)(nil)
var _ source.PreparedBatchStreamProvider = (*aggregateResultSource)(nil)
