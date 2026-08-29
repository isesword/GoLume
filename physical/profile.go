package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"time"
)

// ProfileSource is a diagnostic-only source wrapper used by ExplainAnalyze. It
// records inclusive reader time and row counts without changing the wrapped
// source's data or partition semantics.
type ProfileSource struct {
	input source.Source
	trace *observer.OperatorTrace
	id    string
}

// NewProfileSource wraps input with one registered operator node. A nil trace
// returns input unchanged so profiling never enters normal execution hot paths.
func NewProfileSource(input source.Source, trace *observer.OperatorTrace, node observer.OperatorNode) source.Source {
	if input == nil || trace == nil {
		return input
	}
	trace.Register(node)
	return &ProfileSource{input: input, trace: trace, id: node.ID}
}

// OperatorID returns the trace node ID represented by this wrapper.
func (s *ProfileSource) OperatorID() string { return s.id }

// Schema forwards the wrapped source schema.
func (s *ProfileSource) Schema() memory.Schema { return s.input.Schema() }

// Stats forwards source statistics.
func (s *ProfileSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Partitions forwards source partitions.
func (s *ProfileSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewReader returns a reader that records one inclusive timing per partition.
func (s *ProfileSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r, err := s.input.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &profileReader{owner: s, child: r}, nil
}

// Dictionary forwards stable dictionary metadata when supported by input.
func (s *ProfileSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("profile input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

type profileReader struct {
	owner *ProfileSource
	child source.Reader
}

// Close releases the wrapped reader.
func (r *profileReader) Close() error { return r.child.Close() }

// ReadPartition records inclusive duration and actual emitted row count.
func (r *profileReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	start := time.Now()
	err := r.child.ReadPartition(ctx, p, dst)
	elapsed := time.Since(start)
	out := int64(0)
	if err == nil && dst != nil {
		out = int64(dst.Rows)
	}
	bytes := int64(0)
	if err == nil && dst != nil {
		bytes = estimateBatchBytes(dst)
	}
	r.owner.trace.RecordBytes(r.owner.id, observer.PhaseFromContext(ctx), p.Rows, out, bytes, elapsed)
	return err
}

var _ source.Source = (*ProfileSource)(nil)
var _ source.DictionaryProvider = (*ProfileSource)(nil)
