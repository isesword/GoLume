package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"testing"
)

// Public formatting is never an execution-time fallback.
type windowDiagReviewStream struct {
	value  windowDiagnosticValues
	closes int
}

func (s *windowDiagReviewStream) Next(context.Context, *memory.Batch) (bool, error) {
	return false, nil
}
func (s *windowDiagReviewStream) Close() error { s.closes++; return nil }
func (s *windowDiagReviewStream) StreamDiagnostics() map[string]string {
	panic("automatic public formatter")
}
func (s *windowDiagReviewStream) windowDiagnostics() windowDiagnosticValues { return s.value }

type windowDiagReviewProvider struct{ value windowDiagnosticValues }

func (p *windowDiagReviewProvider) PercentRankDiagnostics() map[string]string {
	panic("automatic source formatter")
}
func (p *windowDiagReviewProvider) windowDiagnostics() windowDiagnosticValues { return p.value }

var windowDiagReviewSink windowDiagnosticValues

func TestWindowDiagnosticsActualObserverEOFAndCloseNoAlloc(t *testing.T) {
	ctx, q := percentRankReviewQuery(t, 8<<20, true)
	for i := 0; i < 1024; i++ {
		q.Operator(fmt.Sprintf("window-diagnostic-%d", i), memory.MemoryWorkspace)
	}
	observer, err := newPercentRankDiagnostics(ctx, q.Operator("window-observer", memory.MemoryRetainedState))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	mock := &windowDiagReviewStream{value: windowDiagnosticValues{sortRuns: 13, sortBytes: 2049, hasSort: true}}
	stage := &observer.streams[0]
	external := &externalWindowStream{query: q, percentRankDiagnostics: observer, rows: 71}
	b := memory.NewBatch()
	// Reuse the real observer/external lifecycle, not a formatter or FILTER seam.
	// The synthetic child isolates automatic diagnostic allocation from sorting.
	allocs := testing.AllocsPerRun(1000, func() {
		mock.closes = 0
		observer.closed = false
		stage.child, stage.owner, stage.closed = mock, observer, false
		external.child, external.closed = stage, false
		if ok, e := stage.Next(ctx, b); ok || e != nil {
			panic("synthetic EOF")
		}
		windowDiagReviewSink = external.windowDiagnostics()
		if e := external.Close(); e != nil {
			panic(e)
		}
		windowDiagReviewSink = external.windowDiagnostics()
	})
	if allocs != 0 {
		t.Fatalf("automatic EOF/Close allocations: %g", allocs)
	}
	if stage.child != nil || stage.owner != nil || mock.closes != 1 {
		t.Fatal("observer retained/closed child incorrectly")
	}
	v := windowDiagReviewSink
	if v.values[0] != 71 || v.values[3] != 13 || v.values[4] != 2049 || v.observed != 1 || v.observedPresent != 7 {
		t.Fatal("incorrect cold counters", v)
	}
	if v.values[7] != 0 || v.values[8] != 0 || v.observed&2 != 0 {
		t.Fatal("fabricated partition spool", v)
	}
	// Explicit reporting remains a cold allocating path with unchanged values.
	report := external.StreamDiagnostics()
	for i, key := range windowDiagnosticKeys {
		if v.present&(1<<i) != 0 && report[key] != fmt.Sprint(v.values[i]) {
			t.Fatalf("%s typed/public mismatch: %s vs %d", key, report[key], v.values[i])
		}
	}
	if q.Used() != 0 {
		t.Fatal("observer reservation retained", q.Used())
	}
}

func TestWindowDiagnosticsTypedProviderAndColdSpool(t *testing.T) {
	for _, kind := range []string{"pr", "cd", "ntile"} {
		t.Run(kind, func(t *testing.T) {
			stream := &percentRankPartitionStream{coldPages: 17, coldBytes: 9001, coldPartitions: 4, coldPeekPages: 19, coldReplayPages: 17, hasCume: kind == "cd", hasReplay: kind != "pr"}
			provider := &windowDiagReviewProvider{value: stream.windowDiagnostics()}
			ordered := &orderedWindowStream{percentRankProvider: provider}
			d := &percentRankDiagnostics{}
			if n := testing.AllocsPerRun(1000, func() {
				windowDiagReviewSink = ordered.windowDiagnostics()
				d.captureFixed(1, windowDiagReviewSink)
				windowDiagReviewSink = d.fixedSnapshot()
			}); n != 0 {
				t.Fatal("cold spool allocations", n)
			}
			v := windowDiagReviewSink
			if v.observed != 2 || v.values[7] != 17 || v.values[8] != 9001 || v.values[9] != 4 {
				t.Fatal("spool count", v)
			}
			if (v.present&(1<<10) != 0) != (kind == "cd") || (v.present&(1<<11) != 0) != (kind != "pr") {
				t.Fatal("peer/replay presence", v)
			}
			report := stream.StreamDiagnostics()
			for i, key := range windowDiagnosticKeys {
				if provider.value.present&(1<<i) != 0 && report[key] != fmt.Sprint(provider.value.values[i]) {
					t.Fatal("typed/public mismatch", key, report)
				}
			}
			d2 := &percentRankDiagnostics{}
			d2.captureFixed(1, windowDiagnosticValues{})
			if d2.fixedSnapshot().observed != 0 {
				t.Fatal("absent provider marked observed")
			}
		})
	}
}

func TestWindowDiagnosticsLiveSortAndSpoolNoAlloc(t *testing.T) {
	sort := &externalSortStream{spillRuns: 7, spillBytes: 123}
	metrics := &percentRankCounters{}
	metrics.pages.Store(3)
	metrics.bytes.Store(100)
	metrics.partitions.Store(1)
	metrics.peekPages.Store(2)
	metrics.replayPages.Store(3)
	p := &percentRankSource{metrics: metrics, cumeHidden: "cd"}
	d := &percentRankDiagnostics{}
	if n := testing.AllocsPerRun(1000, func() {
		d.captureFixed(0, sort.windowDiagnostics())
		d.captureFixed(1, p.windowDiagnostics())
		windowDiagReviewSink = d.fixedSnapshot()
	}); n != 0 {
		t.Fatal("live diagnostics allocations", n)
	}
	v := windowDiagReviewSink
	if v.observed != 3 || v.values[3] != 7 || v.values[4] != 123 || v.values[7] != 3 || v.values[8] != 100 || v.values[10] != 2 || v.values[11] != 3 {
		t.Fatal(v)
	}
}
