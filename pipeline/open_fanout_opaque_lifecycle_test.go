package pipeline

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

type mixedFanOutManager struct {
	*memory.BudgetManager
	ordinary  atomic.Int64
	immediate atomic.Int64
}

func (m *mixedFanOutManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.ordinary.Add(1)
	return m.BudgetManager.Reserve(ctx, n)
}

func (m *mixedFanOutManager) TryReserveImmediate(ctx context.Context, n int64) (memory.Reservation, bool, error) {
	m.immediate.Add(1)
	return m.BudgetManager.TryReserveImmediate(ctx, n)
}

type mixedFanOutSource struct {
	alloc  *arrowmem.CheckedAllocator
	dtype  *arrow.TimestampType
	finish <-chan struct{}
	opens  atomic.Int64
}

func (s *mixedFanOutSource) Schema() memory.Schema {
	return memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "ts", Type: memory.TypeTimestamp, ArrowType: s.dtype.String(), Nullable: true},
	)
}

func (*mixedFanOutSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 2, Partitions: 1}, nil
}

func (*mixedFanOutSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: 2}}, nil
}

func (s *mixedFanOutSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens.Add(1)
	return &mixedFanOutInput{source: s}, nil
}

type mixedFanOutInput struct {
	source *mixedFanOutSource
	sent   bool
}

func (s *mixedFanOutInput) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.sent {
		// Drop the producer's Arrow reference while both destination ranges
		// remain independently owned. This wait controls EOF vs cancellation.
		dst.ClearColumns()
		select {
		case <-s.source.finish:
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	s.sent = true
	builder := array.NewTimestampBuilder(s.source.alloc, s.source.dtype)
	builder.AppendValues([]arrow.Timestamp{11, 29, 0, 47}, []bool{true, true, false, true})
	base := builder.NewArray()
	builder.Release()
	part := array.NewSlice(base, 1, 3)
	base.Release()
	dst.ClearColumns()
	dst.Rows = 2
	dst.Int64s["id"] = &memory.Int64Vector{Values: []int64{71, 83}}
	dst.Opaque["ts"] = &memory.OpaqueVector{
		Rows: 2, Type: memory.TypeTimestamp, ArrowType: s.source.dtype.String(),
		Value: part, Owned: true,
	}
	return true, nil
}

func (*mixedFanOutInput) Close() error { return nil }

type mixedFanOutObserver chan observer.Event

func (o mixedFanOutObserver) Observe(e observer.Event) { o <- e }

func TestOpenFanOutMixedTimestampOpaqueLifecycle(t *testing.T) {
	for _, budget := range []string{"custom_manager", "active_query"} {
		for _, finish := range []string{"eof", "cancel"} {
			t.Run(budget+"/"+finish, func(t *testing.T) {
				testOpenFanOutMixedTimestampOpaque(t, budget == "active_query", finish == "cancel")
			})
		}
	}
}

func testOpenFanOutMixedTimestampOpaque(t *testing.T, queryBudget, cancelProducer bool) {
	t.Helper()
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	const budget = 512 << 10
	manager := &mixedFanOutManager{BudgetManager: memory.NewBudgetManager(budget)}
	adapter, err := memory.NewNativeSharingManager(manager)
	if err != nil {
		t.Fatal(err)
	}
	engine := memory.NewEngineBudget(budget)
	parent := context.Background()
	var query *memory.QueryBudget
	if queryBudget {
		query, err = engine.BeginQuery(memory.BudgetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer query.Close()
		parent = memory.WithQueryBudget(parent, query)
	}
	producer, cancel := context.WithCancel(parent)
	defer cancel()
	// Consumer calls use a separate deadline so the cancellation case must
	// surface the producer's terminal error, not Next's own context error.
	pull, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	finish := make(chan struct{})
	dtype := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	input := &mixedFanOutSource{alloc: alloc, dtype: dtype, finish: finish}
	events := make(mixedFanOutObserver, 1)
	opts := Options{
		Workers: 1, TargetMorselBytes: 32768, MaxMorselBytes: 65536,
		MaxInFlightBytes: 262144, MinMorselRows: 1, MaxMorselRows: 64,
	}
	streams, err := OpenFanOut(producer, input, source.ReadOptions{Kernels: kernel.New(kernel.ModeAuto)}, nil, adapter, opts, 2, events)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		for _, stream := range streams {
			if err := stream.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	}()

	fast, slow := memory.NewBatch(), memory.NewBatch()
	for i, dst := range []*memory.Batch{fast, slow} {
		if ok, err := streams[i].Next(pull, dst); !ok || err != nil {
			t.Fatalf("branch %d first Next: ok=%v err=%v", i, ok, err)
		}
		checkMixedFanOutBatch(t, dst, dtype)
	}
	if fast.Int64s["id"] == slow.Int64s["id"] ||
		&fast.Int64s["id"].Values[0] == &slow.Int64s["id"].Values[0] {
		t.Fatal("guard must independently copy the native Int64 bindings and payload")
	}
	fastTS := fast.Opaque["ts"].Value.(*array.Timestamp)
	slowTS := slow.Opaque["ts"].Value.(*array.Timestamp)
	if fast.Opaque["ts"] == slow.Opaque["ts"] || fastTS == slowTS ||
		fastTS.Data().Buffers()[1] != slowTS.Data().Buffers()[1] {
		t.Fatal("guard must own independent Arrow slices retaining the same immutable data buffer")
	}
	estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(slow, 0, 2)
	if err != nil || !estimate.OpaquePresent || estimate.NativeBytes <= 0 ||
		estimate.LegacyOpaqueBytes <= 0 || estimate.Bytes != estimate.NativeBytes+estimate.LegacyOpaqueBytes {
		t.Fatalf("invalid mixed admission estimate: %+v, %v", estimate, err)
	}
	assertCharge := func(want int64) {
		t.Helper()
		queryUsed := int64(0)
		if query != nil {
			queryUsed = query.Used()
		}
		wantManager, wantQuery := want, int64(0)
		if queryBudget {
			wantManager, wantQuery = 0, want
		}
		if got := manager.Used(); got != wantManager || queryUsed != wantQuery || engine.Used() != wantQuery {
			t.Fatalf("budget routing: manager=%d, query=%d, engine=%d; want manager=%d, query/engine=%d",
				got, queryUsed, engine.Used(), wantManager, wantQuery)
		}
	}
	assertCharge(2 * estimate.Bytes)
	if err := streams[0].Close(); err != nil {
		t.Fatal(err)
	}
	if fast.Rows != 0 || len(fast.Int64s) != 0 || len(fast.Opaque) != 0 {
		t.Fatal("fast Close did not restore the caller destination")
	}
	assertCharge(estimate.Bytes)

	if cancelProducer {
		cancel()
	} else {
		close(finish)
	}
	var event observer.Event
	select {
	case event = <-events:
	case <-pull.Done():
		t.Fatalf("multiplexer did not finish: %v", pull.Err())
	}
	// The producer and the fast branch no longer own the array, but the slow
	// branch can still read its Arrow slice and pins its independent charge.
	assertCharge(estimate.Bytes)
	checkMixedFanOutBatch(t, slow, dtype)
	if alloc.CurrentAlloc() <= 0 {
		t.Fatal("slow branch did not retain Arrow storage")
	}
	if input.opens.Load() != 1 || event.Stage != "multiplexer" || event.Rows != 2 {
		t.Fatalf("not a one-scan, two-row multiplexer: opens=%d event=%+v", input.opens.Load(), event)
	}
	for key, want := range map[string]string{
		"shared_native_ranges": "0", "guard_clone_ranges": "1",
		"owned_admission_model":       "native_capacity_plus_legacy_opaque_logical",
		"legacy_opaque_items":         "2",
		"legacy_opaque_logical_bytes": strconv.FormatInt(2*estimate.LegacyOpaqueBytes, 10),
		"peak_in_flight_bytes":        strconv.FormatInt(2*estimate.Bytes, 10),
	} {
		if got := event.Attributes[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	wantReserves := int64(2)
	if queryBudget {
		wantReserves = 0
	}
	if got, immediate := manager.ordinary.Load(), manager.immediate.Load(); got != wantReserves || immediate != 0 {
		t.Errorf("adapter calls: ordinary=%d immediate=%d; want ordinary=%d immediate=0", got, immediate, wantReserves)
	}

	ok, err := streams[1].Next(pull, slow) // final ACK
	if ok || (cancelProducer && !errors.Is(err, context.Canceled)) || (!cancelProducer && err != nil) {
		t.Fatalf("slow terminal Next: ok=%v err=%v, producer canceled=%v", ok, err, cancelProducer)
	}
	if slow.Rows != 0 || len(slow.Int64s) != 0 || len(slow.Opaque) != 0 {
		t.Fatal("slow terminal Next did not restore the caller destination")
	}
	if err := streams[1].Close(); err != nil {
		t.Fatal(err)
	}
	assertCharge(0) // before query.Close can mask a leak
	for _, stream := range streams {
		ex := stream.(*fanOutBranch).exchange
		if ex.Stats().CurrentInFlightBytes != 0 {
			t.Error("branch in-flight charge leaked")
		}
		if ex.bytes.TryAcquire(ex.maxBytes) {
			ex.bytes.Release(ex.maxBytes)
		} else {
			t.Error("branch global byte credits leaked")
		}
		if ex.groupBytes[0].TryAcquire(ex.groupMax) {
			ex.groupBytes[0].Release(ex.groupMax)
		} else {
			t.Error("branch group byte credits leaked")
		}
	}
}

func checkMixedFanOutBatch(t *testing.T, dst *memory.Batch, dtype *arrow.TimestampType) {
	t.Helper()
	ids, opaque := dst.Int64s["id"], dst.Opaque["ts"]
	if dst.Rows != 2 || ids == nil || len(ids.Values) != 2 || ids.Values[0] != 71 || ids.Values[1] != 83 ||
		opaque == nil || opaque.Rows != 2 || !opaque.Owned || opaque.Type != memory.TypeTimestamp ||
		opaque.ArrowType != dtype.String() {
		t.Fatalf("wrong mixed fan-out result: rows=%d ids=%+v opaque=%+v", dst.Rows, ids, opaque)
	}
	ts, ok := opaque.Value.(*array.Timestamp)
	if !ok || ts.DataType() != dtype || ts.Len() != 2 || !ts.IsValid(0) || ts.Value(0) != 29 || !ts.IsNull(1) {
		t.Fatalf("wrong Timestamp slice: %v", opaque.Value)
	}
}
