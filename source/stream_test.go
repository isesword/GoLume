package source_test

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestInMemoryBatchStreamPreservesEmptyPartitionAndDrains(t *testing.T) {
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

func TestInMemoryBatchStreamCloseIsIdempotentAndStopsNext(t *testing.T) {
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

func TestInMemoryBatchStreamCancellationClearsDestination(t *testing.T) {
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

func TestPartitionParallelCapabilityRequiresPreparedProvider(t *testing.T) {
	native := &nilNativeStreamSource{}
	if plan := source.StreamPlanOf(native); plan.PartitionParallel || source.CanParallelizePartitions(plan) {
		t.Fatalf("unprepared source plan=%+v", plan)
	}
	prepared := &preparedNativeStreamSource{}
	if plan := source.StreamPlanOf(prepared); !plan.PartitionParallel || !source.CanParallelizePartitions(plan) {
		t.Fatalf("prepared source plan=%+v", plan)
	}
}

func TestOpenBatchStreamRejectsNilNativeStream(t *testing.T) {
	stream, err := source.OpenBatchStream(context.Background(), &nilNativeStreamSource{}, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err == nil || stream != nil {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
}

func TestOpenPreparedStreamNeverIgnoresPartitions(t *testing.T) {
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}
	if stream, err := source.OpenBatchStreamWithPartitions(context.Background(), &nilNativeStreamSource{}, opts, nil); err == nil || stream != nil {
		t.Fatalf("unprepared provider stream=%v err=%v", stream, err)
	}
	src := &preparedNativeStreamSource{}
	for _, parts := range [][]source.Partition{nil, {}, {{ID: 3}, {ID: 1}, {ID: 3}}} {
		stream, err := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, parts)
		if err != nil {
			t.Fatal(err)
		}
		stream.Close()
		if len(src.parts) != len(parts) {
			t.Fatalf("parts=%v want=%v", src.parts, parts)
		}
		for i := range parts {
			if src.parts[i] != parts[i] {
				t.Fatalf("parts=%v want=%v", src.parts, parts)
			}
		}
	}
}

func TestDrainBatchStreamClosesOnError(t *testing.T) {
	nextErr := errors.New("next failed")
	closeErr := errors.New("close failed")
	stream := &failingStream{nextErr: nextErr, closeErr: closeErr}
	if _, err := source.DrainBatchStream(context.Background(), stream); !errors.Is(err, nextErr) || !errors.Is(err, closeErr) {
		t.Fatalf("drain err=%v", err)
	}
	if stream.closes != 1 {
		t.Fatalf("closes=%d", stream.closes)
	}
}

type nilNativeStreamSource struct{}

func (*nilNativeStreamSource) Schema() memory.Schema { return memory.Schema{} }
func (*nilNativeStreamSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{}, nil
}
func (*nilNativeStreamSource) Partitions(context.Context) ([]source.Partition, error) {
	return nil, nil
}
func (*nilNativeStreamSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return nil, nil
}

type preparedNativeStreamSource struct {
	nilNativeStreamSource
	parts []source.Partition
}

func (s *preparedNativeStreamSource) NewBatchStreamWithPartitions(_ context.Context, _ source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	s.parts = parts
	return emptyReadOptionsStream{}, nil
}

type failingStream struct {
	nextErr, closeErr error
	closes            int
}

func (s *failingStream) Next(context.Context, *memory.Batch) (bool, error) { return false, s.nextErr }
func (s *failingStream) Close() error                                      { s.closes++; return s.closeErr }
