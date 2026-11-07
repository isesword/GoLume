package physical

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestNativePipelineConsumesZeroToManyInputsPerOutput(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	makeBatch := func(values ...int64) *memory.Batch {
		batch := memory.NewBatch()
		batch.Int64s["x"] = &memory.Int64Vector{Values: values}
		batch.Reset(len(values))
		return batch
	}
	base, err := inmemory.New(schema,
		makeBatch(0, 0),
		makeBatch(0, 1, 0),
		makeBatch(1, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(base, expr.EQ(expr.Col("x"), expr.I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("x"), expr.I64(1)), "y"),
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	limited, err := NewLimitSource(projected, 1)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(
		context.Background(), limited,
		source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("first output ok=%v err=%v", ok, err)
	}
	y, err := out.Int64("y")
	if err != nil || out.Rows != 1 || len(y.Values) != 1 || y.Values[0] != 2 {
		t.Fatalf("output rows=%d y=%v err=%v", out.Rows, y, err)
	}
	if ok, err := stream.Next(context.Background(), out); ok || err != nil {
		t.Fatalf("limit eof ok=%v err=%v", ok, err)
	}
}

func TestNativeFilterCanConsumeAllInputWithoutOutput(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	batch := memory.NewBatch()
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	batch.Reset(3)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(base, expr.GT(expr.Col("x"), expr.I64(10)))
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := filtered.Partitions(context.Background())
	stream, err := source.OpenBatchStreamWithPartitions(context.Background(), filtered, testReadOptions(), parts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); ok || err != nil {
		t.Fatalf("filtered eof ok=%v err=%v", ok, err)
	}
	if out.Rows != 0 || len(out.Int64s) != 0 {
		t.Fatalf("eof retained output rows=%d columns=%d", out.Rows, len(out.Int64s))
	}
}

func TestUnaryStreamCloseOrderAndIdempotence(t *testing.T) {
	closeErr := errors.New("child close")
	events := make([]string, 0, 2)
	state := unaryStreamState{
		child: &recordingBatchStream{close: func() error {
			events = append(events, "child")
			return closeErr
		}},
		closeOwn: func() error {
			events = append(events, "operator")
			return nil
		},
	}
	if err := state.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("close error=%v", err)
	}
	if err := state.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("second close error=%v", err)
	}
	if len(events) != 2 || events[0] != "operator" || events[1] != "child" {
		t.Fatalf("close events=%v", events)
	}
}

type recordingBatchStream struct{ close func() error }

func (*recordingBatchStream) Next(context.Context, *memory.Batch) (bool, error) { return false, nil }
func (s *recordingBatchStream) Close() error                                    { return s.close() }

var _ source.BatchStream = (*recordingBatchStream)(nil)

func TestGlobalOperatorsDoNotAdvertisePartitionSubsets(t *testing.T) {
	for _, operator := range []any{&SortSource{}, &TopKSource{}, &WindowSource{}, &LimitSource{}} {
		if _, ok := operator.(source.PreparedBatchStreamProvider); ok {
			t.Fatalf("global operator %T advertises partition subsets", operator)
		}
	}
}
