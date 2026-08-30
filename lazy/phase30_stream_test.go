package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPhase30CollectUsesNativeStreamAndAcceptsMultipleOutputs(t *testing.T) {
	src := &phase30NativeStreamSource{
		schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}),
		values: [][]int64{{1, 2}, {3}, {4, 5, 6}},
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(src).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if got := frameInt64Column(t, df, "x"); !equalI64(got, []int64{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("stream output=%v", got)
	}
	if src.readerCalls.Load() != 0 || src.streamCalls.Load() != 1 || src.closeCalls.Load() != 1 {
		t.Fatalf("reader=%d stream=%d close=%d", src.readerCalls.Load(), src.streamCalls.Load(), src.closeCalls.Load())
	}
}

func TestPhase30ExplainLabelsPipelineBlockingAndMemorySink(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64},
		memory.Field{Name: "y", Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	batch.Int64s["y"] = &memory.Int64Vector{Values: []int64{30, 10, 20}}
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	pipeline := session.FromSource(src).
		Filter(Col("x").Gt(Lit(1))).
		SelectExprs(Named(Col("x").Add(Lit(1)), "z")).
		Limit(1)
	explain, err := pipeline.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"EXECUTION policy=auto memory_limit=unbounded",
		"STREAM MEMORY_SINK Collect",
		"STREAM PIPELINE Limit n=1",
		"STREAM PIPELINE Project outputs=1",
		"STREAM PIPELINE Filter",
		"STREAM PIPELINE Scan",
	} {
		if !strings.Contains(explain, want) {
			t.Fatalf("physical explain missing %q:\n%s", want, explain)
		}
	}
	sortedExplain, err := session.FromSource(src).Sort(Asc("x")).ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sortedExplain, "STREAM BLOCKING Sort") {
		t.Fatalf("sort explain missing blocking label:\n%s", sortedExplain)
	}
}

func TestPhase30BoundedPolicyRequiresLimitAndRejectsBlocking(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	batch.Reset(3)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	unlimited := NewSession(engine.DefaultOptions())
	defer unlimited.Close()
	_, err = unlimited.FromSource(src).
		WithExecution(ExecutionOptions{Policy: ExecutionBounded}).
		Collect(context.Background())
	if !errors.Is(err, ErrBoundedMemoryLimitRequired) {
		t.Fatalf("unlimited bounded error=%v", err)
	}

	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	limited := NewSession(opts)
	defer limited.Close()
	df, err := limited.FromSource(src).
		WithExecution(ExecutionOptions{Policy: ExecutionBounded}).
		Filter(Col("x").Gt(Lit(1))).
		Limit(1).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	df.Close()

	_, err = limited.FromSource(src).
		WithExecution(ExecutionOptions{Policy: ExecutionBounded}).
		Sort(Asc("x")).
		Prepare(context.Background())
	var unsupported *UnsupportedBoundedPlanError
	if !errors.As(err, &unsupported) || unsupported.Operator != "Sort" {
		t.Fatalf("blocking error=%T %v", err, err)
	}
}

func TestPhase30ExecutionOverrideIsImmutableAndJoinPoliciesMustAgree(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	src, _ := inmemory.New(schema, batch)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	base := session.FromSource(src)
	bounded := base.WithExecution(ExecutionOptions{Policy: ExecutionBounded})
	if base.hasExecutionPolicy || !bounded.hasExecutionPolicy || bounded.executionPolicy != ExecutionBounded {
		t.Fatalf("base override=%v bounded override=%v policy=%v", base.hasExecutionPolicy, bounded.hasExecutionPolicy, bounded.executionPolicy)
	}
	joined := bounded.Join(base.WithExecution(ExecutionOptions{Policy: ExecutionMemoryOnly}), JoinOptions{
		Type: InnerJoin, LeftOn: []string{"x"}, RightOn: []string{"x"},
	})
	if joined.err == nil || !strings.Contains(joined.err.Error(), "conflicting execution policies") {
		t.Fatalf("join policy error=%v", joined.err)
	}
}

func TestPhase30LimitStopsReadingUpstreamPartitions(t *testing.T) {
	src := newPhase30CountingPartitionSource(4)
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	df, err := session.FromSource(src).
		Filter(Col("x").Gt(Lit(0))).
		Select("x").
		Limit(1).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if got := frameInt64Column(t, df, "x"); !equalI64(got, []int64{1}) {
		t.Fatalf("limit output=%v", got)
	}
	if src.reader.readCalls != 1 || src.reader.closeCalls != 1 {
		t.Fatalf("upstream read calls=%d close calls=%d", src.reader.readCalls, src.reader.closeCalls)
	}
}

func TestPhase30CollectJoinsStreamAndCloseErrorsWithoutBudgetLeak(t *testing.T) {
	nextErr := errors.New("injected stream failure")
	closeErr := errors.New("injected stream close failure")
	src := &phase30NativeStreamSource{
		schema:   memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}),
		values:   [][]int64{{1, 2}, {3, 4}},
		nextErr:  nextErr,
		errAt:    1,
		closeErr: closeErr,
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	session := NewSession(opts)
	defer session.Close()
	_, err := session.FromSource(src).Collect(context.Background())
	if !errors.Is(err, nextErr) || !errors.Is(err, closeErr) {
		t.Fatalf("collect error=%v", err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("failed collect leaked %d reserved bytes", used)
	}
	if src.closeCalls.Load() != 1 {
		t.Fatalf("stream close calls=%d", src.closeCalls.Load())
	}
}

type phase30NativeStreamSource struct {
	schema      memory.Schema
	values      [][]int64
	readerCalls atomic.Int64
	streamCalls atomic.Int64
	closeCalls  atomic.Int64
	nextErr     error
	errAt       int
	closeErr    error
}

func (s *phase30NativeStreamSource) Schema() memory.Schema { return s.schema }
func (s *phase30NativeStreamSource) Stats(context.Context) (source.Stats, error) {
	rows := int64(0)
	for _, values := range s.values {
		rows += int64(len(values))
	}
	return source.Stats{Rows: rows, Partitions: 1}, nil
}
func (s *phase30NativeStreamSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0}}, nil
}
func (s *phase30NativeStreamSource) NewReader(context.Context, source.ReadOptions) (source.Reader, error) {
	s.readerCalls.Add(1)
	return nil, fmt.Errorf("partition reader must not be opened")
}
func (s *phase30NativeStreamSource) NewBatchStream(_ context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	s.streamCalls.Add(1)
	return &phase30NativeStream{owner: s}, nil
}

type phase30NativeStream struct {
	owner  *phase30NativeStreamSource
	next   int
	closed bool
}

func (s *phase30NativeStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.owner.nextErr != nil && s.next == s.owner.errAt {
		dst.ClearColumns()
		return false, s.owner.nextErr
	}
	if s.next == len(s.owner.values) {
		dst.ClearColumns()
		return false, nil
	}
	values := s.owner.values[s.next]
	s.next++
	dst.ClearColumns()
	dst.Int64s["x"] = &memory.Int64Vector{Values: values}
	dst.Reset(len(values))
	return true, nil
}

func (s *phase30NativeStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closeCalls.Add(1)
	}
	return s.owner.closeErr
}

type phase30CountingPartitionSource struct {
	parts  []source.Partition
	reader *phase30CountingPartitionReader
}

func newPhase30CountingPartitionSource(partitions int) *phase30CountingPartitionSource {
	parts := make([]source.Partition, partitions)
	for i := range parts {
		parts[i] = source.Partition{ID: i, Rows: 2}
	}
	return &phase30CountingPartitionSource{parts: parts, reader: &phase30CountingPartitionReader{}}
}

func (*phase30CountingPartitionSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
}
func (s *phase30CountingPartitionSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.parts) * 2), Partitions: len(s.parts)}, nil
}
func (s *phase30CountingPartitionSource) Partitions(context.Context) ([]source.Partition, error) {
	return append([]source.Partition(nil), s.parts...), nil
}
func (s *phase30CountingPartitionSource) NewReader(_ context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return s.reader, nil
}

type phase30CountingPartitionReader struct {
	readCalls  int
	closeCalls int
}

func (r *phase30CountingPartitionReader) ReadPartition(_ context.Context, part source.Partition, dst *memory.Batch) error {
	r.readCalls++
	base := int64(part.ID*2 + 1)
	dst.ClearColumns()
	dst.Int64s["x"] = &memory.Int64Vector{Values: []int64{base, base + 1}}
	dst.Reset(2)
	return nil
}
func (r *phase30CountingPartitionReader) Close() error {
	r.closeCalls++
	return nil
}

var _ source.BatchStreamProvider = (*phase30NativeStreamSource)(nil)
var _ source.BatchStream = (*phase30NativeStream)(nil)
var _ source.Source = (*phase30CountingPartitionSource)(nil)
var _ source.Reader = (*phase30CountingPartitionReader)(nil)
