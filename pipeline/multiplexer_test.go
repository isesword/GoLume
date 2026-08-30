package pipeline_test

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/pipeline"
	"gocolumn/source"
	"sync"
	"testing"
	"time"
)

func TestBoundedMultiplexerSingleScanStableResultsAndBackpressure(t *testing.T) {
	tracked := newTrackedIntSource(t, 12, 2)
	parts, err := tracked.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1 << 20)
	collector := &observer.Collector{}
	opts := pipeline.Options{
		Workers: 1, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 64,
		MinMorselRows: 1, MaxMorselRows: 2, Ordering: pipeline.OrderingStable,
	}
	streams, err := pipeline.OpenFanOut(context.Background(), tracked, scalarReadOptions(), parts, manager, opts, 2, collector)
	if err != nil {
		t.Fatal(err)
	}

	results := make([][]int64, len(streams))
	errs := make(chan error, len(streams))
	var wg sync.WaitGroup
	for branch, stream := range streams {
		wg.Add(1)
		go func(branch int, stream source.BatchStream) {
			defer wg.Done()
			defer stream.Close()
			out := memory.NewBatch()
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					return
				}
				results[branch] = append(results[branch], out.Int64s["x"].Values...)
				if branch == 1 {
					time.Sleep(time.Millisecond)
				}
			}
		}(branch, stream)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for branch, values := range results {
		if len(values) != 24 {
			t.Fatalf("branch %d rows=%d values=%v", branch, len(values), values)
		}
		for i, value := range values {
			if value != int64(i) {
				t.Fatalf("branch %d value[%d]=%d", branch, i, value)
			}
		}
	}
	if tracked.opened.Load() != 1 || tracked.closed.Load() != 1 {
		t.Fatalf("opened=%d closed=%d; bounded fan-out must scan once", tracked.opened.Load(), tracked.closed.Load())
	}
	if manager.Used() != 0 {
		t.Fatalf("reservation leak=%d", manager.Used())
	}
	events := collector.Events()
	if len(events) != 1 || events[0].Stage != "multiplexer" || events[0].Rows != 24 {
		t.Fatalf("observer events=%+v", events)
	}
	if events[0].Attributes["branches"] != "2" || events[0].Attributes["producer_blocked_ns"] == "0" {
		t.Fatalf("multiplexer attributes=%v", events[0].Attributes)
	}
}

func TestBoundedMultiplexerEarlyBranchCloseDoesNotStopSibling(t *testing.T) {
	tracked := newTrackedIntSource(t, 8, 2)
	parts, _ := tracked.Partitions(context.Background())
	manager := memory.NewBudgetManager(1 << 20)
	opts := pipeline.Options{
		Workers: 1, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 64,
		MinMorselRows: 1, MaxMorselRows: 2,
	}
	streams, err := pipeline.OpenFanOut(context.Background(), tracked, scalarReadOptions(), parts, manager, opts, 2, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	if err := streams[0].Close(); err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	var values []int64
	for {
		ok, err := streams[1].Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		values = append(values, out.Int64s["x"].Values...)
	}
	if err := streams[1].Close(); err != nil {
		t.Fatal(err)
	}
	if len(values) != 16 || values[15] != 15 {
		t.Fatalf("sibling result=%v", values)
	}
	if tracked.opened.Load() != 1 || tracked.closed.Load() != 1 || manager.Used() != 0 {
		t.Fatalf("opened=%d closed=%d used=%d", tracked.opened.Load(), tracked.closed.Load(), manager.Used())
	}
}

func TestBoundedMultiplexerFaultReachesEveryBranchAndReleases(t *testing.T) {
	tracked := newTrackedIntSource(t, 8, 2)
	tracked.failPartition = 3
	parts, _ := tracked.Partitions(context.Background())
	manager := memory.NewBudgetManager(1 << 20)
	opts := pipeline.Options{
		Workers: 1, TargetMorselBytes: 16, MaxMorselBytes: 32, MaxInFlightBytes: 64,
		MinMorselRows: 1, MaxMorselRows: 2,
	}
	streams, err := pipeline.OpenFanOut(context.Background(), tracked, scalarReadOptions(), parts, manager, opts, 2, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, len(streams))
	var wg sync.WaitGroup
	for _, stream := range streams {
		wg.Add(1)
		go func(stream source.BatchStream) {
			defer wg.Done()
			out := memory.NewBatch()
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil || !ok {
					errCh <- err
					_ = stream.Close()
					return
				}
			}
		}(stream)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if !errors.Is(err, errInjectedRead) {
			t.Fatalf("branch error=%v", err)
		}
	}
	if tracked.opened.Load() != 1 || tracked.closed.Load() != 1 || manager.Used() != 0 {
		t.Fatalf("opened=%d closed=%d used=%d", tracked.opened.Load(), tracked.closed.Load(), manager.Used())
	}
}

func TestFanOutRescansStableSourceAndRejectsInsufficientBranchBudget(t *testing.T) {
	stable := newTrackedIntSource(t, 2, 1)
	// The tracking wrapper is intentionally replay-unknown. Its stable in-memory
	// child becomes visible only when passed directly.
	parts, _ := stable.Partitions(context.Background())
	opts := pipeline.Options{
		Workers: 1, TargetMorselBytes: 32, MaxMorselBytes: 32, MaxInFlightBytes: 32,
		MinMorselRows: 1, MaxMorselRows: 4,
	}
	_, err := pipeline.OpenFanOut(context.Background(), stable, scalarReadOptions(), parts, memory.NewBudgetManager(1<<20), opts, 2, observer.Nop{})
	if err == nil {
		t.Fatal("expected per-branch byte budget error")
	}

	base := stable.Source
	stableParts, _ := base.Partitions(context.Background())
	streams, err := pipeline.OpenFanOut(context.Background(), base, scalarReadOptions(), stableParts, memory.NewBudgetManager(1<<20), opts, 2, observer.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range streams {
		out := memory.NewBatch()
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatalf("stable rescan next ok=%v err=%v", ok, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
