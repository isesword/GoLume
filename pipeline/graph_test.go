package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelPipelineStableFanInAndByteBackpressure(t *testing.T) {
	tracked := newTrackedIntSource(t, 8, 2)
	filtered, err := physical.NewFilterSource(tracked, expr.GE(expr.Col("x"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := physical.NewProjectSource(filtered, []expr.NamedExpr{
		expr.Named(expr.Add(expr.Col("x"), expr.I64(1)), "y"),
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := projected.Partitions(context.Background())
	manager := memory.NewBudgetManager(1 << 20)
	collector := &observer.Collector{}
	opts := pipeline.Options{
		Workers: 4, TargetMorselBytes: 16, MaxMorselBytes: 16, MaxInFlightBytes: 64,
		MinMorselRows: 1, MaxMorselRows: 2, Ordering: pipeline.OrderingStable,
	}
	stream, err := pipeline.OpenBatchStream(context.Background(), projected, scalarReadOptions(), parts, manager, opts, collector)
	if err != nil {
		t.Fatal(err)
	}
	statsProvider, ok := stream.(pipeline.StreamStatsProvider)
	if !ok {
		t.Fatalf("stream %T has no pipeline stats", stream)
	}
	out := memory.NewBatch()
	values := make([]int64, 0, 16)
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("first next ok=%v err=%v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)
	values = append(values, out.Int64s["y"].Values...)
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		values = append(values, out.Int64s["y"].Values...)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for i, got := range values {
		if want := int64(i + 1); got != want {
			t.Fatalf("stable value[%d]=%d want=%d all=%v", i, got, want, values)
		}
	}
	if len(values) != 16 {
		t.Fatalf("rows=%d values=%v", len(values), values)
	}
	stats := statsProvider.PipelineStats()
	if stats.Exchange.PeakInFlightBytes > opts.MaxInFlightBytes || stats.Exchange.PeakInFlightBytes == 0 {
		t.Fatalf("peak in-flight=%d limit=%d", stats.Exchange.PeakInFlightBytes, opts.MaxInFlightBytes)
	}
	if stats.Exchange.ProducerBlocked < 20*time.Millisecond {
		t.Fatalf("producer blocked only %s", stats.Exchange.ProducerBlocked)
	}
	if manager.Used() != 0 || tracked.opened.Load() != tracked.closed.Load() {
		t.Fatalf("used=%d opened=%d closed=%d", manager.Used(), tracked.opened.Load(), tracked.closed.Load())
	}
	events := collector.Events()
	if len(events) != 1 || events[0].Stage != "pipeline" || events[0].Rows != 16 || events[0].Attributes["morsels"] == "" {
		t.Fatalf("observer events=%+v", events)
	}
}

func TestParallelPipelineWorkerCountsAreDeterministic(t *testing.T) {
	collect := func(workers int) []int64 {
		tracked := newTrackedIntSource(t, 9, 3)
		projected, err := physical.NewProjectSource(tracked, []expr.NamedExpr{
			expr.Named(expr.Add(expr.Col("x"), expr.I64(0)), "x"),
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		parts, _ := projected.Partitions(context.Background())
		opts := pipeline.Options{Workers: workers, TargetMorselBytes: 24, MaxMorselBytes: 48, MaxInFlightBytes: 256, MinMorselRows: 1, MaxMorselRows: 3, Ordering: pipeline.OrderingStable}
		stream, err := pipeline.OpenBatchStream(context.Background(), projected, scalarReadOptions(), parts, memory.NewBudgetManager(1<<20), opts, observer.Nop{})
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		out := memory.NewBatch()
		var values []int64
		for {
			ok, err := stream.Next(context.Background(), out)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				return values
			}
			values = append(values, out.Int64s["x"].Values...)
		}
	}
	one, many := collect(1), collect(4)
	if len(one) != len(many) {
		t.Fatalf("worker rows one=%d many=%d", len(one), len(many))
	}
	for i := range one {
		if one[i] != many[i] {
			t.Fatalf("row %d one=%d many=%d", i, one[i], many[i])
		}
	}
}

func TestEffectiveWorkersKeepsTinyPartitionsOnDirectPath(t *testing.T) {
	tracked := newTrackedIntSource(t, 256, 16)
	parts, _ := tracked.Partitions(context.Background())
	opts := pipeline.Options{
		Workers: 4, TargetMorselBytes: 4 << 10, MaxMorselBytes: 8 << 10, MaxInFlightBytes: 64 << 10,
		MinMorselRows: 1, MaxMorselRows: 256,
	}
	if got := pipeline.EffectiveWorkers(tracked, parts, opts); got != 1 {
		t.Fatalf("tiny partition workers=%d want=1", got)
	}

	wide := make([]source.Partition, len(parts))
	for i := range wide {
		wide[i] = source.Partition{ID: i, Rows: 4096}
	}
	if got := pipeline.EffectiveWorkers(tracked, wide, opts); got != 4 {
		t.Fatalf("wide partition workers=%d want=4", got)
	}
}

func TestParallelPipelineEarlyCloseAndFailureReleaseWorkers(t *testing.T) {
	for _, failPartition := range []int{-1, 3} {
		t.Run(fmt.Sprintf("fail=%d", failPartition), func(t *testing.T) {
			tracked := newTrackedIntSource(t, 8, 2)
			tracked.failPartition = failPartition
			parts, _ := tracked.Partitions(context.Background())
			manager := memory.NewBudgetManager(1 << 20)
			opts := pipeline.Options{Workers: 4, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 64, MinMorselRows: 1, MaxMorselRows: 2}
			stream, err := pipeline.OpenBatchStream(context.Background(), tracked, scalarReadOptions(), parts, manager, opts, observer.Nop{})
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			_, nextErr := stream.Next(context.Background(), out)
			if failPartition >= 0 {
				for nextErr == nil {
					ok, err := stream.Next(context.Background(), out)
					nextErr = err
					if !ok {
						break
					}
				}
				if nextErr == nil || !errors.Is(nextErr, errInjectedRead) {
					t.Fatalf("failure error=%v", nextErr)
				}
			}
			if err := stream.Close(); failPartition < 0 && err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 || tracked.opened.Load() != tracked.closed.Load() {
				t.Fatalf("used=%d opened=%d closed=%d", manager.Used(), tracked.opened.Load(), tracked.closed.Load())
			}
		})
	}
}

func TestParallelPipelineRejectsOversizedStringRow(t *testing.T) {
	batches := make([]*memory.Batch, 2)
	for i := range batches {
		batch := memory.NewBatch()
		batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1024}, Data: []byte(fmt.Sprintf("%01024d", i)), Encoding: memory.EncodingView}
		batch.Reset(1)
		batches[i] = batch
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := base.Partitions(context.Background())
	manager := memory.NewBudgetManager(1 << 20)
	opts := pipeline.Options{Workers: 2, TargetMorselBytes: 128, MaxMorselBytes: 512, MaxInFlightBytes: 1024, MinMorselRows: 1, MaxMorselRows: 8}
	stream, err := pipeline.OpenBatchStream(context.Background(), base, scalarReadOptions(), parts, manager, opts, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	_, err = stream.Next(context.Background(), out)
	var tooLarge *pipeline.MorselTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("oversized error=%T %v", err, err)
	}
	_ = stream.Close()
	if manager.Used() != 0 {
		t.Fatalf("reservation leak=%d", manager.Used())
	}
}

func scalarReadOptions() source.ReadOptions {
	return source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}
}

var errInjectedRead = errors.New("injected partition read failure")

type trackedSource struct {
	source.Source
	opened        atomic.Int64
	closed        atomic.Int64
	failPartition int
}

func newTrackedIntSource(t *testing.T, partitions, rows int) *trackedSource {
	t.Helper()
	batches := make([]*memory.Batch, partitions)
	value := int64(0)
	for part := range batches {
		batch := memory.NewBatch()
		values := make([]int64, rows)
		for row := range values {
			values[row] = value
			value++
		}
		batch.Int64s["x"] = &memory.Int64Vector{Values: values}
		batch.Reset(rows)
		batches[part] = batch
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return &trackedSource{Source: base, failPartition: -1}
}

func (s *trackedSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	reader, err := s.Source.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &trackedReader{owner: s, child: reader}, nil
}

type trackedReader struct {
	owner *trackedSource
	child source.Reader
	once  sync.Once
}

func (r *trackedReader) ReadPartition(ctx context.Context, part source.Partition, dst *memory.Batch) error {
	if part.ID == r.owner.failPartition {
		return errInjectedRead
	}
	return r.child.ReadPartition(ctx, part, dst)
}

func (r *trackedReader) Close() error {
	var err error
	r.once.Do(func() {
		r.owner.closed.Add(1)
		err = r.child.Close()
	})
	return err
}

var _ source.Source = (*trackedSource)(nil)
var _ source.Reader = (*trackedReader)(nil)
