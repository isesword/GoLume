package source_test

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestPartitionBatchStreamPreservesEmptyPartitionAndDrains(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	empty := memory.NewBatch()
	empty.Int64s["x"] = &memory.Int64Vector{}
	rows := memory.NewBatch()
	rows.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	rows.Reset(3)
	src, err := inmemory.New(schema, empty, rows)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.DrainBatchStream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Batches != 2 || stats.Rows != 3 || stats.Bytes <= 0 {
		t.Fatalf("drain stats=%+v", stats)
	}
}

func TestPartitionBatchStreamCloseIsIdempotentAndStopsNext(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
		t.Fatalf("next after close ok=%v err=%v", ok, err)
	}
}

func TestPartitionBatchStreamCancellationClearsDestination(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := memory.NewBatch()
	out.Int64s["stale"] = &memory.Int64Vector{Values: []int64{9}}
	out.Reset(1)
	if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled next ok=%v err=%v", ok, err)
	}
	if out.Rows != 0 || len(out.Int64s) != 0 {
		t.Fatalf("destination retained stale data: rows=%d columns=%d", out.Rows, len(out.Int64s))
	}
}

func TestStreamPlanFormattingAndBlockingDetection(t *testing.T) {
	root := source.StreamPlanNode{
		Operator: "Collect",
		Class:    source.StreamMemorySink,
		Children: []source.StreamPlanNode{{
			Operator: "Sort",
			Class:    source.StreamBlocking,
			Children: []source.StreamPlanNode{{Operator: "Scan", Class: source.StreamPipeline}},
		}},
	}
	want := "STREAM MEMORY_SINK Collect\n  STREAM BLOCKING Sort\n    STREAM PIPELINE Scan"
	if got := source.FormatStreamPlan(root); got != want {
		t.Fatalf("stream plan:\n%s\nwant:\n%s", got, want)
	}
	node, path, ok := source.FirstBlocking(root)
	if !ok || node.Operator != "Sort" || len(path) != 2 || path[0] != "Collect" || path[1] != "Sort" {
		t.Fatalf("blocking node=%+v path=%v ok=%v", node, path, ok)
	}
}

func TestStreamClassExplainLabels(t *testing.T) {
	for class, want := range map[source.StreamClass]string{
		source.StreamPipeline:   "PIPELINE",
		source.StreamBounded:    "BOUNDED",
		source.StreamBlocking:   "BLOCKING",
		source.StreamSpillable:  "SPILLABLE",
		source.StreamMemorySink: "MEMORY_SINK",
	} {
		if got := class.String(); got != want {
			t.Fatalf("class %d label=%q want=%q", class, got, want)
		}
	}
}

func TestPartitionParallelCapabilityIsConservativeForNativeSources(t *testing.T) {
	legacy := newFaultSource(2)
	if plan := source.StreamPlanOf(legacy); !plan.PartitionParallel || !source.CanParallelizePartitions(plan) {
		t.Fatalf("legacy partition source plan=%+v", plan)
	}
	native := &nilNativeStreamSource{Source: legacy}
	if plan := source.StreamPlanOf(native); plan.PartitionParallel || source.CanParallelizePartitions(plan) {
		t.Fatalf("native stream without prepared partitions plan=%+v", plan)
	}
}

func TestPartitionBatchStreamReadAndCloseErrorsAreSticky(t *testing.T) {
	readErr := errors.New("injected read failure")
	closeErr := errors.New("injected close failure")
	src := newFaultSource(2)
	src.reader.readErrAt = 0
	src.reader.readErr = readErr
	src.reader.closeErr = closeErr
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	out.Int64s["stale"] = &memory.Int64Vector{Values: []int64{9}}
	out.Reset(1)
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
		t.Fatalf("first next ok=%v err=%v", ok, err)
	}
	if out.Rows != 0 || len(out.Int64s) != 0 {
		t.Fatalf("failed read retained destination rows=%d columns=%d", out.Rows, len(out.Int64s))
	}
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
		t.Fatalf("sticky next ok=%v err=%v", ok, err)
	}
	if err := stream.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close error=%v", err)
	}
	if src.reader.readCalls != 1 || src.reader.closeCalls != 1 {
		t.Fatalf("read calls=%d close calls=%d", src.reader.readCalls, src.reader.closeCalls)
	}
}

func TestPartitionBatchStreamNaturalEOFCloseError(t *testing.T) {
	closeErr := errors.New("injected close failure")
	src := newFaultSource(1)
	src.reader.closeErr = closeErr
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("data next ok=%v err=%v", ok, err)
	}
	if ok, err := stream.Next(context.Background(), out); ok || !errors.Is(err, closeErr) {
		t.Fatalf("eof next ok=%v err=%v", ok, err)
	}
	if err := stream.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close error=%v", err)
	}
	if src.reader.closeCalls != 1 {
		t.Fatalf("close calls=%d", src.reader.closeCalls)
	}
}

func TestPartitionBatchStreamDownstreamEarlyStopClosesWithoutReadingAhead(t *testing.T) {
	src := newFaultSource(3)
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("first next ok=%v err=%v", ok, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if src.reader.readCalls != 1 || src.reader.closeCalls != 1 {
		t.Fatalf("read calls=%d close calls=%d", src.reader.readCalls, src.reader.closeCalls)
	}
}

func TestOpenBatchStreamRejectsNilNativeStream(t *testing.T) {
	base := newFaultSource(1)
	src := &nilNativeStreamSource{Source: base}
	stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err == nil || stream != nil {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
	if base.reader.readCalls != 0 || base.reader.closeCalls != 0 {
		t.Fatalf("adapter reader unexpectedly used: read=%d close=%d", base.reader.readCalls, base.reader.closeCalls)
	}
}

type faultSource struct {
	parts  []source.Partition
	reader *faultReader
}

func newFaultSource(partitions int) *faultSource {
	parts := make([]source.Partition, partitions)
	for i := range parts {
		parts[i] = source.Partition{ID: i, Rows: 1}
	}
	return &faultSource{parts: parts, reader: &faultReader{readErrAt: -1}}
}

func (*faultSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
}
func (s *faultSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.parts)), Partitions: len(s.parts)}, nil
}
func (s *faultSource) Partitions(context.Context) ([]source.Partition, error) {
	return append([]source.Partition(nil), s.parts...), nil
}
func (s *faultSource) NewReader(context.Context, source.ReadOptions) (source.Reader, error) {
	return s.reader, nil
}

type faultReader struct {
	readCalls  int
	closeCalls int
	readErrAt  int
	readErr    error
	closeErr   error
}

func (r *faultReader) ReadPartition(_ context.Context, part source.Partition, dst *memory.Batch) error {
	call := r.readCalls
	r.readCalls++
	if call == r.readErrAt {
		if r.readErr == nil {
			return fmt.Errorf("injected read failure at call %d", call)
		}
		return r.readErr
	}
	dst.ClearColumns()
	dst.Int64s["x"] = &memory.Int64Vector{Values: []int64{int64(part.ID)}}
	dst.Reset(1)
	return nil
}

func (r *faultReader) Close() error {
	r.closeCalls++
	return r.closeErr
}

type nilNativeStreamSource struct{ source.Source }

func (*nilNativeStreamSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return nil, nil
}

var _ source.Source = (*faultSource)(nil)
var _ source.Reader = (*faultReader)(nil)
var _ source.BatchStreamProvider = (*nilNativeStreamSource)(nil)
