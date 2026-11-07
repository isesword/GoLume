package engine

import (
	"context"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
	"time"
)

func outputLifecycleStream(t *testing.T, kind string, groups, outputRows int) (*Engine, *PreparedQuery, *memory.QueryBudget, source.BatchStream) {
	t.Helper()
	opts := streamTestOptions()
	src := &generatedStreamSource{rows: 64, groups: groups, sorted: kind == "sorted"}
	query := streamTestQuery()
	if kind != "resident" {
		query.Hints.ForceStrategy = ""
	}
	if kind == "spill" {
		opts.ExecutionPolicy = ExecutionBounded
		opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
	}
	eng := New(opts)
	prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := eng.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), budget)
	var stream source.BatchStream
	switch kind {
	case "resident":
		stream, err = prepared.NewResidentAggregateStream(ctx, prepared.resultSchema(), outputRows)
	case "sorted":
		stream, err = prepared.NewSortedAggregateStream(ctx, prepared.resultSchema(), outputRows)
	default:
		stream, err = prepared.NewSpillableAggregateStream(ctx, prepared.resultSchema(), outputRows)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close(); prepared.Close(); budget.Close(); eng.Close() })
	return eng, prepared, budget, stream
}

func TestAggregateOutputInvalidatesPreviousDestination(t *testing.T) {
	for _, kind := range []string{"resident", "sorted", "spill"} {
		t.Run(kind, func(t *testing.T) {
			_, _, budget, stream := outputLifecycleStream(t, kind, 2, 1)
			first, second := memory.NewBatch(), memory.NewBatch()
			for _, dst := range []*memory.Batch{first, second} {
				if ok, err := stream.Next(context.Background(), dst); err != nil || !ok {
					t.Fatalf("next=%v error=%v", ok, err)
				}
			}
			if first.Rows != 0 || len(first.Int32s) != 0 || len(first.Int64s) != 0 {
				t.Fatal("previous destination retained uncharged output")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if second.Rows != 0 || len(second.Int32s) != 0 || len(second.Int64s) != 0 {
				t.Fatal("Close retained bound output")
			}
			if budget.Used() != 0 {
				t.Fatalf("closed budget=%d", budget.Used())
			}
		})
	}
}

func TestPreparedCloseWaitsForFinalAggregateOutputBorrow(t *testing.T) {
	for _, kind := range []string{"resident", "sorted", "spill"} {
		for _, finish := range []string{"eof", "close"} {
			t.Run(kind+"/"+finish, func(t *testing.T) {
				_, prepared, budget, stream := outputLifecycleStream(t, kind, 1, 2)
				dst := memory.NewBatch()
				if ok, err := stream.Next(context.Background(), dst); err != nil || !ok || dst.Rows != 1 {
					t.Fatalf("next=%v rows=%d error=%v", ok, dst.Rows, err)
				}
				done := make(chan error, 1)
				go func() { done <- prepared.Close() }()
				select {
				case err := <-done:
					t.Fatalf("prepared Close returned with final output borrowed: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
				if finish == "eof" {
					if ok, err := stream.Next(context.Background(), memory.NewBatch()); err != nil || ok {
						t.Fatalf("eof=%v error=%v", ok, err)
					}
				} else if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("prepared Close did not finish after output release")
				}
				if dst.Rows != 0 || budget.Used() != 0 {
					t.Fatalf("released rows=%d budget=%d", dst.Rows, budget.Used())
				}
			})
		}
	}
}

func TestAggregateDiagnosticsSurviveCleanup(t *testing.T) {
	for _, kind := range []string{"resident", "sorted", "spill"} {
		t.Run(kind, func(t *testing.T) {
			_, _, _, stream := outputLifecycleStream(t, kind, 1, 2)
			diagnostic := stream.(source.BatchStreamDiagnostics)
			for {
				ok, err := stream.Next(context.Background(), memory.NewBatch())
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
			}
			want := map[string]string{"resident": "bitmap-direct", "sorted": "sorted", "spill": "spill-hash"}[kind]
			if got := diagnostic.StreamDiagnostics(); got["strategy"] != want || got["workers"] != "1" {
				t.Fatalf("diagnostics=%v", got)
			}
		})
	}
	opts := streamTestOptions()
	opts.Workers = 3
	eng := New(opts)
	defer eng.Close()
	prepared, err := eng.Prepare(context.Background(), &generatedStreamSource{rows: 64, groups: 17}, partitionedQuery(), PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	budget, err := eng.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := prepared.NewResidentAggregateStream(memory.WithQueryBudget(context.Background(), budget), prepared.resultSchema(), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for {
		ok, err := stream.Next(context.Background(), memory.NewBatch())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	got := stream.(source.BatchStreamDiagnostics).StreamDiagnostics()
	if got["strategy"] != "partitioned-hash" || got["workers"] != "3" || got["state_partitions"] == "0" {
		t.Fatalf("partitioned diagnostics=%v", got)
	}
}
