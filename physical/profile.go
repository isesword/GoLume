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

// Replayability forwards the child snapshot contract.
func (s *ProfileSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// Ordering forwards a proven child ordering through the diagnostic wrapper.
func (s *ProfileSource) Ordering() []source.OrderingColumn {
	provider, ok := s.input.(source.OrderingProvider)
	if !ok {
		return nil
	}
	return provider.Ordering()
}

// StreamPlan omits the diagnostic wrapper and forwards the executable child.
func (s *ProfileSource) StreamPlan() source.StreamPlanNode { return source.StreamPlanOf(s.input) }

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

// NewBatchStream preserves native streaming operators while recording one
// timing sample per produced batch. Without this forwarding method diagnostic
// wrapping would accidentally force spillable operators through legacy Reader
// compatibility paths.
func (s *ProfileSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		return nil, err
	}
	return &profileBatchStream{owner: s, child: child}, nil
}

// NewBatchStreamWithPartitions is the prepared-plan form of NewBatchStream.
func (s *ProfileSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	child, err := source.OpenBatchStreamWithPartitions(ctx, s.input, opts, parts)
	if err != nil {
		return nil, err
	}
	return &profileBatchStream{owner: s, child: child}, nil
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

type profileBatchStream struct {
	owner       *ProfileSource
	child       source.BatchStream
	diagnostics bool
}

func (s *profileBatchStream) captureDiagnostics(ctx context.Context) {
	if s == nil || s.diagnostics {
		return
	}
	s.diagnostics = true
	if diagnostics, ok := s.child.(source.BatchStreamDiagnostics); ok {
		s.owner.trace.RecordAttributes(s.owner.id, observer.PhaseFromContext(ctx), diagnostics.StreamDiagnostics())
	}
}

// Next forwards one native stream batch and records cold-path diagnostics.
func (s *profileBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	start := time.Now()
	ok, err := s.child.Next(ctx, dst)
	elapsed := time.Since(start)
	rows, bytes := int64(0), int64(0)
	if ok && err == nil && dst != nil {
		rows = int64(dst.Rows)
		bytes = estimateBatchBytes(dst)
	}
	s.owner.trace.RecordBytes(s.owner.id, observer.PhaseFromContext(ctx), 0, rows, bytes, elapsed)
	if !ok || err != nil {
		s.captureDiagnostics(ctx)
	}
	return ok, err
}

// Close releases the child stream and captures its final diagnostics.
func (s *profileBatchStream) Close() error {
	err := s.child.Close()
	s.captureDiagnostics(context.Background())
	return err
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
var _ source.StreamPlanProvider = (*ProfileSource)(nil)
var _ source.ReplayabilityProvider = (*ProfileSource)(nil)
var _ source.OrderingProvider = (*ProfileSource)(nil)
var _ source.BatchStreamProvider = (*ProfileSource)(nil)
var _ source.PreparedBatchStreamProvider = (*ProfileSource)(nil)
