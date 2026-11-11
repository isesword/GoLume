package pipeline

import (
	"context"
	"errors"
	"gocolumn/memory"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func ownedAdmissionFixture(rows int) *memory.Batch {
	c := memory.NewBatch()
	c.Rows = rows
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = uint32(i)
	}
	c.StringLists["l"] = &memory.StringListVector{Offsets: offsets, Values: &memory.StringViewVector{Offsets: offsets, Data: make([]byte, rows)}}
	b := memory.NewBatch()
	b.Rows = rows
	b.Structs["s"] = &memory.StructVector{Rows: rows, Fields: []memory.Field{{Name: "l", Type: memory.TypeList, Children: []memory.Field{{Name: "item", Type: memory.TypeString}}}}, Values: c}
	return b
}

func TestOwnedExchangeAdmissionBeforeClone(t *testing.T) {
	for _, rows := range []int{1, 257} {
		b := ownedAdmissionFixture(rows)
		logical, err := memory.EstimateBatchRangeBytes(b, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		owned, err := nativeOwnedRangeBytes(b, 0, rows)
		if err != nil {
			t.Fatal(err)
		}
		if owned <= logical {
			t.Fatalf("owned=%d logical=%d", owned, logical)
		}
		for _, mode := range []string{"exact", "manager short", "exchange short", "group short", "closed", "canceled", "queue canceled"} {
			t.Run(mode, func(t *testing.T) {
				capacity, limit, groups := owned, owned, 1
				if mode == "manager short" {
					limit--
				}
				if mode == "exchange short" {
					capacity--
				}
				if mode == "group short" {
					capacity = owned*2 - 1
					groups = 2
				}
				e, err := NewExchange(memory.NewBudgetManager(limit), capacity, 1, groups)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "closed" {
					e.closeSignal()
				}
				if mode == "canceled" {
					cancel()
				}
				if mode == "queue canceled" {
					e.items <- exchangeItem{end: true}
					var timeout context.CancelFunc
					ctx, timeout = context.WithTimeout(ctx, 5*time.Millisecond)
					defer timeout()
				}
				// Deliberately supply payload-only bytes: enqueue must independently admit.
				err = e.enqueueRange(ctx, Sequence{}, b, MorselRange{Rows: rows, Bytes: logical})
				if mode == "exact" {
					if err != nil {
						t.Fatal(err)
					}
					if e.manager.Used() != owned || e.Stats().CurrentInFlightBytes != owned {
						t.Fatal("charge mismatch")
					}
					item := <-e.items
					e.release(item)
				} else {
					if err == nil {
						t.Fatal("expected denial")
					}
					if mode == "manager short" && !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatal(err)
					}
					if mode == "queue canceled" {
						<-e.items
					}
				}
				if e.manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 || len(e.items) != 0 {
					t.Fatal("leaked")
				}
				if !e.bytes.TryAcquire(capacity) || !e.groupBytes[0].TryAcquire(e.groupMax) {
					t.Fatal("semaphore leaked")
				}
				if b.Structs["s"].Values.StringLists["l"].Values.Data[0] != 0 {
					t.Fatal("source changed")
				}
			})
		}
	}
}

func TestOwnedSizerSplitsWithMetadataAndKeepsPublicPayload(t *testing.T) {
	b := ownedAdmissionFixture(257)
	one, err := nativeOwnedRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Workers: 1, TargetMorselBytes: one, MaxMorselBytes: one, MaxInFlightBytes: one, MinMorselRows: 1, MaxMorselRows: 257}
	owned, err := newOwnedByteSizer(memory.Schema{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	ranges, err := owned.Split(b)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, r := range ranges {
		n, err := nativeOwnedRangeBytes(b, r.Offset, r.Rows)
		if err != nil {
			t.Fatal(err)
		}
		if r.Bytes != n || n > one || r.Offset != total {
			t.Fatalf("bad range %+v", r)
		}
		total += r.Rows
	}
	if total != 257 || len(ranges) < 2 {
		t.Fatal("did not split")
	}
	logical, _ := NewByteSizer(memory.Schema{}, opts)
	public, err := logical.Split(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range public {
		n, _ := memory.EstimateBatchRangeBytes(b, r.Offset, r.Rows)
		if n != r.Bytes {
			t.Fatal("public semantics changed")
		}
	}
	opts.TargetMorselBytes--
	opts.MaxMorselBytes--
	tooSmall, _ := newOwnedByteSizer(memory.Schema{}, opts)
	_, err = tooSmall.Split(b)
	var large *MorselTooLargeError
	if !errors.As(err, &large) || large.Bytes != one {
		t.Fatalf("one-byte-short error=%v", err)
	}
}

// Releasing a delivered item must not leave clone-owned map backing in the
// caller's reusable Batch after returning its reservation.
func TestOwnedBranchRestoresDestinationMaps(t *testing.T) {
	src := ownedAdmissionFixture(1)
	n, err := nativeOwnedRangeBytes(src, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := NewExchange(memory.NewBudgetManager(n), n, 1, 1)
	if err := e.enqueueRange(context.Background(), Sequence{}, src, MorselRange{Rows: 1, Bytes: 1}); err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	original := dst.Int64s
	branch := &fanOutBranch{exchange: e}
	branch.bindCurrent(dst, <-e.items)
	if len(dst.Structs) != 1 {
		t.Fatal("missing clone")
	}
	branch.releaseCurrent()
	if len(dst.Structs) != 0 || dst.Rows != 0 || e.manager.Used() != 0 {
		t.Fatal("release failed")
	}
	original["sentinel"] = &memory.Int64Vector{}
	if dst.Int64s["sentinel"] == nil {
		t.Fatal("did not restore caller map")
	}
	branch.releaseCurrent() // idempotent
	// The same and a different destination remain writable after release.
	for _, target := range []*memory.Batch{dst, memory.NewBatch(), dst} {
		if err := e.enqueueRange(context.Background(), Sequence{}, src, MorselRange{Rows: 1, Bytes: 1}); err != nil {
			t.Fatal(err)
		}
		branch.bindCurrent(target, <-e.items)
		branch.releaseCurrent()
		target.Int64s["reuse"] = &memory.Int64Vector{Values: []int64{1}}
		target.ClearColumns()
	}

}

func TestOwnedExchangeQueryBudgetOverridesManager(t *testing.T) {
	b := ownedAdmissionFixture(1)
	n, err := nativeOwnedRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int64{n - 1, n} {
		engine := memory.NewEngineBudget(limit)
		q, err := engine.BeginQuery(memory.BudgetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), q)
		fallback := memory.NewBudgetManager(1)
		e, _ := NewExchange(fallback, n, 1, 1)
		err = e.enqueueRange(ctx, Sequence{}, b, MorselRange{Rows: 1, Bytes: 1})
		if limit < n {
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if q.Used() != n {
				t.Fatal("query not charged")
			}
			e.release(<-e.items)
		}
		if q.Used() != 0 || fallback.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 {
			t.Fatal("budget leaked")
		}
		q.Close()
	}
}

func TestNativeOwnedExchangeOpaqueCompatibilityAndCleanup(t *testing.T) {
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	dtype := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	builder := array.NewTimestampBuilder(alloc, dtype)
	builder.AppendValues([]arrow.Timestamp{7, 9}, nil)
	a := builder.NewArray()
	builder.Release()
	defer a.Release()
	src := memory.NewBatch()
	src.Rows = 2
	src.Opaque["t"] = &memory.OpaqueVector{Rows: 2, Value: a}
	n, err := nativeOwnedRangeBytes(src, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"consume", "closed", "denied", "queue canceled"} {
		t.Run(mode, func(t *testing.T) {
			limit := n
			if mode == "denied" {
				limit--
			}
			manager := memory.NewBudgetManager(limit)
			e, _ := NewExchange(manager, n, 1, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "closed" {
				e.closeSignal()
			}
			if mode == "queue canceled" {
				e.items <- exchangeItem{end: true}
				var deadline context.CancelFunc
				ctx, deadline = context.WithTimeout(ctx, 5*time.Millisecond)
				defer deadline()
			}
			err := e.enqueueRange(ctx, Sequence{}, src, MorselRange{Rows: 2, Bytes: 1})
			if mode == "consume" {
				if err != nil {
					t.Fatal(err)
				}
				dst := memory.NewBatch()
				branch := &fanOutBranch{exchange: e}
				branch.bindCurrent(dst, <-e.items)
				got := dst.Opaque["t"].Value.(*array.Timestamp)
				gotType := got.DataType().(*arrow.TimestampType)
				if gotType != dtype || gotType.Unit != dtype.Unit || gotType.TimeZone != dtype.TimeZone {
					t.Fatal("legacy timestamp type identity changed")
				}
				if got.Value(0) != 7 || got.Value(1) != 9 {
					t.Fatal("timestamp payload changed")
				}
				if _, err := gotType.GetZone(); err != nil {
					t.Fatal(err)
				}
				dtype.ClearCachedLocation()
				branch.releaseCurrent()
				dst.Int64s["reuse"] = &memory.Int64Vector{} // original writable maps restored
			} else {
				if err == nil {
					t.Fatal("expected denied publication")
				}
				if mode == "queue canceled" {
					<-e.items
				}
			}
			if manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 {
				t.Fatal("reservation leaked")
			}
			if !e.bytes.TryAcquire(n) || !e.groupBytes[0].TryAcquire(n) {
				t.Fatal("semaphore leaked")
			}
		})
	}
	if a.DataType() != dtype || a.(*array.Timestamp).Value(0) != 7 {
		t.Fatal("source changed")
	}
}

func TestNativeOwnedZeroOpaqueScopeCounters(t *testing.T) {
	a := array.NewNull(3)
	defer a.Release()
	b := memory.NewBatch()
	b.Rows = 3
	b.Opaque["n"] = &memory.OpaqueVector{Rows: 3, Value: a}
	estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(b, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !estimate.OpaquePresent || estimate.NativeBytes != 0 || estimate.LegacyOpaqueBytes != 0 || estimate.Bytes != 0 {
		t.Fatalf("wrong legacy-zero scope: %+v", estimate)
	}
	e, _ := NewExchange(memory.NewBudgetManager(1), 1, 1, 1)
	if err := e.enqueueRange(context.Background(), Sequence{}, b, MorselRange{Rows: 3, Bytes: 1}); err != nil {
		t.Fatal(err)
	}
	stats := e.Stats()
	if stats.CurrentInFlightBytes != 1 || stats.LegacyOpaqueItems != 1 || stats.LegacyOpaqueBytes != 0 {
		t.Fatalf("scope counters: %+v", stats)
	}
	e.release(<-e.items)
	if e.manager.Used() != 0 || e.Stats().CurrentInFlightBytes != 0 {
		t.Fatal("legacy zero item leaked")
	}
}
