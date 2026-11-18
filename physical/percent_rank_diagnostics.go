package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"strconv"
	"sync"
	"unsafe"
)

// These observers are execution-private, single-open slots. No per-batch trace
// map is retained: only final monotone counters survive phase teardown.
type percentRankDiagnostics struct {
	mu          sync.Mutex
	reservation memory.Reservation
	closed      bool
	values      [3]percentRankStageValues
	sources     [3]percentRankDiagnosticSource
	streams     [3]percentRankDiagnosticStream
}
type percentRankStageValues struct {
	units, bytes, partitions int64
	peekPages, replayPages   int64
	cumeObserved             bool
	replayObserved           bool
	observed                 bool
}
type percentRankDiagnosticSource struct {
	owner  *percentRankDiagnostics
	stage  int
	input  source.Source
	opened bool
}
type percentRankDiagnosticStream struct {
	mu     sync.Mutex
	owner  *percentRankDiagnostics
	stage  int
	child  source.BatchStream
	closed bool
}

func newPercentRankDiagnostics(ctx context.Context, manager memory.Manager) (*percentRankDiagnostics, error) {
	if ctx == nil {
		return nil, fmt.Errorf("percent rank diagnostics requires context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, fmt.Errorf("percent rank diagnostics requires memory manager")
	}
	lease, err := manager.Reserve(ctx, int64(unsafe.Sizeof(percentRankDiagnostics{})))
	if err != nil {
		return nil, err
	}
	d := &percentRankDiagnostics{reservation: lease}
	for i := range d.sources {
		d.sources[i].owner, d.sources[i].stage = d, i
		d.streams[i].owner, d.streams[i].stage = d, i
	}
	return d, nil
}
func percentRankDiagnosticStage(stage string) int {
	switch stage {
	case "front_sort":
		return 0
	case "partition_spool":
		return 1
	case "restore_sort":
		return 2
	}
	panic("invalid private percent rank diagnostic stage")
}

// Wrap attaches one execution-private observer without allocating another slot.
func (d *percentRankDiagnostics) Wrap(stage string, input source.Source) source.Source {
	i := percentRankDiagnosticStage(stage)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.sources[i].input != nil || d.sources[i].opened || input == nil {
		panic("invalid private percent rank diagnostic wrapping")
	}
	d.sources[i].input = input
	return &d.sources[i]
}
func (d *percentRankDiagnostics) capture(stage int, values map[string]string) {
	unitKey, byteKey := "spill_runs", "spill_bytes"
	if stage == 1 {
		unitKey, byteKey = "partition_spool_pages", "partition_spool_bytes"
	}
	units, e1 := strconv.ParseInt(values[unitKey], 10, 64)
	bytes, e2 := strconv.ParseInt(values[byteKey], 10, 64)
	if e1 != nil || e2 != nil || units < 0 || bytes < 0 {
		return
	}
	partitions := int64(0)
	if stage == 1 {
		var err error
		partitions, err = strconv.ParseInt(values["partition_spool_partitions"], 10, 64)
		if err != nil || partitions < 0 {
			return
		}
	}
	peekPages, replayPages := int64(0), int64(0)
	cumeObserved, replayObserved := false, false
	if stage == 1 {
		if replay, ok := values["partition_replay_pages"]; ok {
			var err error
			replayPages, err = strconv.ParseInt(replay, 10, 64)
			replayObserved = err == nil && replayPages >= 0
		}
		if peek, ok := values["peer_lookahead_pages"]; ok {
			var e1, e2 error
			peekPages, e1 = strconv.ParseInt(peek, 10, 64)
			replayPages, e2 = strconv.ParseInt(values["partition_replay_pages"], 10, 64)
			cumeObserved = e1 == nil && e2 == nil && peekPages >= 0 && replayPages >= 0
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	v := &d.values[stage]
	v.units = max(v.units, units)
	v.bytes = max(v.bytes, bytes)
	v.partitions = max(v.partitions, partitions)
	v.observed = true
	if replayObserved {
		v.replayObserved = true
		v.replayPages = max(v.replayPages, replayPages)
	}
	if cumeObserved {
		v.cumeObserved = true
		v.peekPages = max(v.peekPages, peekPages)
		v.replayPages = max(v.replayPages, replayPages)
	}
}

// Snapshot copies completed phase counters, including after Close.
func (d *percentRankDiagnostics) Snapshot() map[string]string {
	d.mu.Lock()
	values := d.values
	d.mu.Unlock()
	out := make(map[string]string, 10)
	for i, prefix := range [...]string{"front_sort", "partition_spool", "restore_sort"} {
		unit := "spill_runs"
		b := "spill_bytes"
		if i == 1 {
			unit = "pages"
			b = "bytes"
		}
		out[prefix+"_"+unit] = strconv.FormatInt(values[i].units, 10)
		out[prefix+"_"+b] = strconv.FormatInt(values[i].bytes, 10)
		out[prefix+"_observed"] = strconv.FormatBool(values[i].observed)
	}
	out["partition_spool_partitions"] = strconv.FormatInt(values[1].partitions, 10)
	if values[1].cumeObserved {
		out["peer_lookahead_pages"] = strconv.FormatInt(values[1].peekPages, 10)
	}
	if values[1].replayObserved {
		out["partition_replay_pages"] = strconv.FormatInt(values[1].replayPages, 10)
	}
	return out
}

// Close releases active graph references and accounting, retaining cold integers.
func (d *percentRankDiagnostics) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	var err error
	for i := range d.streams {
		err = errors.Join(err, d.streams[i].Close())
	}
	d.mu.Lock()
	// Fixed terminal integers remain cold stream statistics. All dynamic graph
	// references and the active execution lease are relinquished, not merely hidden.
	for i := range d.sources {
		d.sources[i].input = nil
	}
	lease := d.reservation
	d.reservation = nil
	d.mu.Unlock()
	if lease != nil {
		lease.Release()
	}
	return err
}
func (s *percentRankDiagnosticSource) inputSource() source.Source {
	if s.owner == nil {
		return nil
	}
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.input
}

// Schema forwards the input schema while the execution is live.
func (s *percentRankDiagnosticSource) Schema() memory.Schema {
	if in := s.inputSource(); in != nil {
		return in.Schema()
	}
	return memory.Schema{}
}

// Stats forwards source statistics without consuming the input.
func (s *percentRankDiagnosticSource) Stats(ctx context.Context) (source.Stats, error) {
	if in := s.inputSource(); in != nil {
		return in.Stats(ctx)
	}
	return source.Stats{}, source.ErrBatchStreamClosed
}

// Partitions forwards the input partition description.
func (s *percentRankDiagnosticSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	if in := s.inputSource(); in != nil {
		return in.Partitions(ctx)
	}
	return nil, source.ErrBatchStreamClosed
}

// Ordering preserves proven input ordering.
func (s *percentRankDiagnosticSource) Ordering() []source.OrderingColumn {
	if p, ok := s.inputSource().(source.OrderingProvider); ok {
		return p.Ordering()
	}
	return nil
}

// Replayability is conservative because each private observer opens once.
func (s *percentRankDiagnosticSource) Replayability() source.Replayability {
	return source.ReplayUnknown
}

// StreamPlan preserves the observed execution plan.
func (s *percentRankDiagnosticSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanOf(s.inputSource())
}

// Dictionary forwards stable dictionary metadata when supplied by the input.
func (s *percentRankDiagnosticSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	if p, ok := s.inputSource().(source.DictionaryProvider); ok {
		return p.Dictionary(ctx, column)
	}
	return source.Dictionary{}, fmt.Errorf("percent rank diagnostic input has no dictionary for %q", column)
}

// NewBatchStream opens the observed source at most once.
func (s *percentRankDiagnosticSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	d := s.owner
	if d == nil {
		return nil, source.ErrBatchStreamClosed
	}
	d.mu.Lock()
	if d.closed || s.opened || s.input == nil {
		d.mu.Unlock()
		return nil, fmt.Errorf("percent rank diagnostic stage can only open once")
	}
	s.opened = true
	input := s.input
	d.mu.Unlock()
	child, err := source.OpenBatchStream(ctx, input, opts)
	if err != nil {
		return nil, err
	}
	stream := &d.streams[s.stage]
	stream.mu.Lock()
	defer stream.mu.Unlock()
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed || stream.closed {
		return nil, errors.Join(source.ErrBatchStreamClosed, child.Close())
	}
	stream.child = child
	return stream, nil
}
func (s *percentRankDiagnosticStream) sample() {
	if p, ok := s.child.(windowDiagnosticProvider); ok && s.owner != nil {
		s.owner.captureFixed(s.stage, p.windowDiagnostics())
	}
}

// Next forwards one borrowed batch and records terminal phase diagnostics.
func (s *percentRankDiagnosticStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.child == nil {
		return false, source.ErrBatchStreamClosed
	}
	ok, err := s.child.Next(ctx, dst)
	if !ok || err != nil {
		s.sample()
	}
	return ok, err
}

// Close samples before child teardown, then drops the child reference.
func (s *percentRankDiagnosticStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.sample()
	var err error
	if s.child != nil {
		err = s.child.Close()
	}
	s.child = nil
	s.owner = nil
	return err
}

// StreamDiagnostics exposes the live child cold-path snapshot.
func (s *percentRankDiagnosticStream) StreamDiagnostics() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.child.(source.BatchStreamDiagnostics); ok {
		values := p.StreamDiagnostics()
		if s.owner != nil {
			s.owner.capture(s.stage, values)
		}
		return values
	}
	return nil
}
