package pipeline

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/observer"
	"gocolumn/source"
	"testing"
)

func TestSharedFinalFourBranchesReverseACK(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	m, cancel := reviewSharedMux(t, manager, manager)
	defer cancel(context.Canceled)
	for i := 2; i < 4; i++ {
		e, err := NewExchange(manager, 131072, 2, 1)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cc := context.WithCancel(m.ctx)
		b := &fanOutBranch{owner: m, id: i, exchange: e, ctx: ctx, cancel: cc}
		b.active.Store(true)
		m.branches = append(m.branches, b)
	}
	m.active.Store(4)
	if err := m.branches[2].Close(); err != nil {
		t.Fatal(err)
	}
	src := reviewSharedSource(4)
	cost, err := memory.EstimateNativeSharedBatchRange(src, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	handled, active, err := m.trySharedRange(src, MorselRange{Rows: 2}, Sequence{})
	if err != nil || !handled || active != 3 {
		t.Fatalf("three active: %v %d %v", handled, active, err)
	}
	if manager.Used() != cost.OwnerBytes+3*cost.BindingBytes {
		t.Fatal("three-binding accounting")
	}
	remaining := 3
	for _, i := range []int{3, 1, 0} {
		item := <-m.branches[i].exchange.items
		m.branches[i].exchange.release(item)
		remaining--
		want := int64(0)
		if remaining > 0 {
			want = cost.OwnerBytes + int64(remaining)*cost.BindingBytes
		}
		if manager.Used() != want {
			t.Fatalf("ACK branch %d used=%d want=%d", i, manager.Used(), want)
		}
	}
	reviewSharedClean(t, m, manager)
}

func TestSharedFinalZeroAndDifferentDestinations(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	m, cancel := reviewSharedMux(t, manager, manager)
	defer cancel(context.Canceled)
	src := reviewSharedSource(4)
	var first memory.Batch
	second := memory.NewBatch()
	original := second.Int64s
	for turn, dst := range []*memory.Batch{&first, second} {
		handled, _, err := m.trySharedRange(src, MorselRange{Offset: turn * 2, Rows: 2}, Sequence{Index: uint64(turn)})
		if err != nil || !handled {
			t.Fatalf("prepare %v %v", handled, err)
		}
		if ok, err := m.branches[0].Next(context.Background(), dst); err != nil || !ok {
			t.Fatal(err)
		}
		if dst.Int64s["v"].Values[0] != int64(turn*2+1) {
			t.Fatal("wrong destination value")
		}
		// All exposed categories must still be writable, including on a zero dst.
		dst.Strings["added"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0}}
		m.branches[1].exchange.release(<-m.branches[1].exchange.items)
	}
	// Advancing with a different dst must already have restored the first one.
	if first.Rows != 0 || first.Int64s != nil || first.Strings != nil {
		t.Fatal("old zero destination retained clone maps")
	}
	if err := m.branches[0].Close(); err != nil {
		t.Fatal(err)
	}
	if second.Rows != 0 || len(second.Strings) != 0 || len(second.Int64s) != 0 {
		t.Fatal("second destination not empty")
	}
	original["identity"] = &memory.Int64Vector{}
	if second.Int64s["identity"] == nil {
		t.Fatal("caller map identity was lost")
	}
	delete(original, "identity")
	reviewSharedClean(t, m, manager)
}

func TestSharedFinalNullNestedAndEmptySchema(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			m, cancel := reviewSharedMux(t, manager, manager)
			defer cancel(context.Canceled)
			src := memory.NewBatch()
			src.Rows = 2
			if !empty {
				child := reviewSharedSource(2)
				src.Structs["s"] = &memory.StructVector{Rows: 2, Values: child, Fields: []memory.Field{{Name: "v", Type: memory.TypeInt64}}, Validity: memory.Bitmap{1}}
			}
			handled, _, err := m.trySharedRange(src, MorselRange{Rows: 2}, Sequence{})
			if err != nil || !handled {
				t.Fatalf("prepare %v %v", handled, err)
			}
			for _, b := range m.branches {
				dst := memory.NewBatch()
				if ok, err := b.Next(context.Background(), dst); err != nil || !ok {
					t.Fatal(err)
				}
				if dst.Rows != 2 {
					t.Fatal("row count")
				}
				if !empty {
					v := dst.Structs["s"]
					if v.IsValid(1) || v.Values.Int64s["v"].IsValid(1) || v.Values.Int64s["v"].Values[0] != 1 {
						t.Fatal("NULL inheritance changed")
					}
				}
				dst.ClearColumns()
				b.releaseCurrent()
			}
			reviewSharedClean(t, m, manager)
		})
	}
}

type finalReviewSource struct{ batch *memory.Batch }

func (s *finalReviewSource) Schema() memory.Schema { return memory.Schema{} }
func (s *finalReviewSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.batch.Rows), Partitions: 1}, nil
}
func (s *finalReviewSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{Rows: int64(s.batch.Rows)}}, nil
}
func (s *finalReviewSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &finalReviewStream{batch: s.batch}, nil
}

type finalReviewStream struct {
	batch *memory.Batch
	sent  bool
}

func (s *finalReviewStream) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if s.sent {
		return false, nil
	}
	s.sent = true
	dst.Rows = s.batch.Rows
	// The fixture borrows source-owned, immutable descriptors. Closing the scan
	// does not own or release the external Arrow array fixture.
	for k, v := range s.batch.Int64s {
		dst.Int64s[k] = v
	}
	for k, v := range s.batch.Structs {
		dst.Structs[k] = v
	}
	for k, v := range s.batch.Opaque {
		dst.Opaque[k] = v
	}
	return true, nil
}
func (s *finalReviewStream) Close() error { return nil }

func TestSharedFinalOpenFanOutOpaqueScope(t *testing.T) {
	for _, kind := range []string{"mixed", "nested_undeclared", "zero_payload"} {
		t.Run(kind, func(t *testing.T) {
			a := array.NewNull(2)
			defer a.Release()
			src := memory.NewBatch()
			src.Rows = 2
			opaque := &memory.OpaqueVector{Rows: 2, Value: a}
			switch kind {
			case "mixed":
				src.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2}}
				src.Opaque["o"] = opaque
			case "nested_undeclared":
				child := reviewSharedSource(2)
				child.Opaque["hidden"] = opaque
				src.Structs["s"] = &memory.StructVector{Rows: 2, Values: child, Fields: []memory.Field{{Name: "v", Type: memory.TypeInt64}}}
			case "zero_payload":
				src.Opaque["o"] = opaque
			}
			manager := memory.NewBudgetManager(512 << 10)
			collector := &observer.Collector{}
			opts := Options{Workers: 1, TargetMorselBytes: 32768, MaxMorselBytes: 65536, MaxInFlightBytes: 262144, MinMorselRows: 1, MaxMorselRows: 64}
			streams, err := OpenFanOut(context.Background(), &finalReviewSource{src}, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}, nil, manager, opts, 2, collector)
			if err != nil {
				t.Fatal(err)
			}
			errs := make(chan error, 2)
			for _, stream := range streams {
				go func(stream source.BatchStream) {
					run := func() error {
						dst := memory.NewBatch()
						rows := 0
						for {
							ok, e := stream.Next(context.Background(), dst)
							if e != nil {
								return e
							}
							if !ok {
								break
							}
							rows += dst.Rows
							if kind != "nested_undeclared" && dst.Opaque["o"].Value == nil {
								return fmt.Errorf("opaque dropped")
							}
						}
						if rows != 2 {
							return fmt.Errorf("rows=%d", rows)
						}
						return nil
					}
					err := run()
					closeErr := stream.Close()
					if err == nil {
						err = closeErr
					}
					errs <- err
				}(stream)
			}
			for range streams {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			// Close synchronizes with producer completion before reading diagnostics.
			for _, stream := range streams {
				_ = stream.Close()
			}
			if manager.Used() != 0 {
				t.Fatalf("leak %d", manager.Used())
			}
			events := collector.Events()
			if len(events) != 1 {
				t.Fatalf("events %v", events)
			}
			attrs := events[0].Attributes
			if attrs["shared_native_ranges"] != "0" || attrs["guard_clone_ranges"] != "1" || attrs["legacy_opaque_items"] != "2" {
				t.Fatalf("scope counters %v", attrs)
			}
			if kind == "zero_payload" && attrs["legacy_opaque_logical_bytes"] != "0" {
				t.Fatalf("zero opaque cost %v", attrs)
			}
		})
	}
}
