package physical

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

// A truly one-shot source, unlike a stable source with its replay marker hidden.
// It reuses its payload buffers and supplies no useful cardinality estimate.
type topKOneShotSource struct {
	schema                      memory.Schema
	batch                       *memory.Batch
	count, opens, nexts, closes int
	fill                        func(int, *memory.Batch)
	nextErr, closeErr           error
	after                       func(int)
}

func (s *topKOneShotSource) Schema() memory.Schema { return s.schema }
func (s *topKOneShotSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: -1}, errors.New("one-shot statistics unavailable")
}
func (s *topKOneShotSource) Partitions(context.Context) ([]source.Partition, error) {
	return nil, errors.New("one-shot partition list unavailable")
}
func (s *topKOneShotSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens > 1 {
		return nil, errors.New("one-shot source reopened")
	}
	return &topKOneShotStream{s}, nil
}

type topKOneShotStream struct{ owner *topKOneShotSource }

func (s *topKOneShotStream) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	p := s.owner
	dst.ClearColumns()
	p.nexts++
	if p.nexts > p.count {
		return false, p.nextErr
	}
	p.fill(p.nexts-1, p.batch)
	bindAll(dst, p.batch)
	borrowFilterOwners(dst)
	dst.Rows = p.batch.Rows
	if p.after != nil {
		p.after(p.nexts)
	}
	return true, nil
}
func (s *topKOneShotStream) Close() error {
	s.owner.closes++
	s.owner.batch.ClearColumns()
	return s.owner.closeErr
}

type topKPeakManager struct {
	*memory.BudgetManager
	peak int64
}

func (m *topKPeakManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, err := m.BudgetManager.Reserve(ctx, n)
	if used := m.Used(); used > m.peak {
		m.peak = used
	}
	return r, err
}

func topKMutableSource(count, payloadBytes int) *topKOneShotSource {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, 1)}
	b.Strings["payload"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(payloadBytes)}, Data: make([]byte, payloadBytes)}
	return &topKOneShotSource{
		schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString}),
		batch:  b, count: count,
		fill: func(i int, b *memory.Batch) {
			b.Int64s["key"].Values[0] = int64(count - i)
			for j := range b.Strings["payload"].Data {
				b.Strings["payload"].Data[j] = byte(i % 251)
			}
		},
	}
}

func TestTopKSnapshotUnknownStatsAndMutableOneShot(t *testing.T) {
	var peaks []int64
	for _, rows := range []int{100, 100000} {
		input := topKMutableSource(rows, 73)
		// The narrower key source must never be opened without a replay promise.
		keys := topKMutableSource(1, 0)
		manager := &topKPeakManager{BudgetManager: memory.NewBudgetManager(32 << 10)}
		top, err := NewTopKSourceWithKeyInput(input, keys, 7, []SortKey{{Column: "key"}}, manager)
		if err != nil {
			t.Fatal(err)
		}
		if plan := source.StreamPlanOf(top); plan.Class != source.StreamBounded || !strings.Contains(plan.Detail, "strategy=one-pass-snapshots") {
			t.Fatal(plan)
		}
		stream, err := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		for i := 0; i < 7; i++ {
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("row %d: (%v,%v)", i, ok, err)
			}
			if got := out.Int64s["key"].Values[0]; got != int64(i+1) {
				t.Fatalf("key=%d", got)
			}
			want := byte((rows - 1 - i) % 251)
			for _, got := range out.Strings["payload"].Data {
				if got != want {
					t.Fatalf("aliased payload=%d want=%d", got, want)
				}
			}
		}
		if ok, err := stream.Next(context.Background(), out); ok || err != nil {
			t.Fatal(ok, err)
		}
		if ok, err := stream.Next(context.Background(), out); ok || err != nil {
			t.Fatal(ok, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 0 || input.opens != 1 || input.closes != 1 || keys.opens != 0 {
			t.Fatalf("used=%d opens=%d closes=%d key opens=%d", manager.Used(), input.opens, input.closes, keys.opens)
		}
		peaks = append(peaks, manager.peak)
	}
	if peaks[0] != peaks[1] {
		t.Fatalf("retained peak depends on N: %v", peaks)
	}
}

func TestTopKSnapshotTerminalFailuresReleaseEverything(t *testing.T) {
	errRead, errClose := errors.New("source read failed"), errors.New("source close failed")
	for _, failure := range []string{"read", "close", "both", "cancel-during-input", "budget", "nil-context", "nil-destination", "ordinal-overflow"} {
		t.Run(failure, func(t *testing.T) {
			input := topKMutableSource(30, 400)
			manager := memory.NewBudgetManager(1 << 20)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var want error
			switch failure {
			case "read":
				input.nextErr, want = errRead, errRead
			case "close":
				input.closeErr, want = errClose, errClose
			case "both":
				input.nextErr, input.closeErr, want = errRead, errClose, errRead
			case "cancel-during-input":
				input.after = func(n int) {
					if n == 3 {
						cancel()
					}
				}
				want = context.Canceled
			case "budget":
				manager = memory.NewBudgetManager(3 << 10)
				want = memory.ErrBudgetExceeded
			}
			top, _ := NewTopKSourceWithManager(input, 10, []SortKey{{Column: "key"}}, manager)
			stream, err := top.NewBatchStream(ctx, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			callCtx, dst := ctx, out
			switch failure {
			case "nil-context":
				callCtx = nil
			case "nil-destination":
				dst = nil
			case "ordinal-overflow":
				stream.(*topKSnapshotStream).rows = math.MaxInt64
			}
			ok, err := stream.Next(callCtx, dst)
			if ok || err == nil || (want != nil && !errors.Is(err, want)) {
				t.Fatalf("Next=(%v,%v), want %v", ok, err, want)
			}
			if failure == "both" && !errors.Is(err, errClose) {
				t.Fatal("close error lost", err)
			}
			if out.Rows != 0 || manager.Used() != 0 || input.closes != 1 {
				t.Fatalf("failed stream rows=%d used=%d closes=%d", out.Rows, manager.Used(), input.closes)
			}
			if _, again := stream.Next(context.Background(), out); again != err {
				t.Fatalf("terminal error changed: %v -> %v", err, again)
			}
			_ = stream.Close()
			_ = stream.Close()
			if input.closes != 1 {
				t.Fatal("source closed twice")
			}
			if _, after := stream.Next(context.Background(), out); !errors.Is(after, source.ErrBatchStreamClosed) {
				t.Fatal(after)
			}
		})
	}
}

func TestTopKSnapshotOutputLifetimeAndBudgetOwnership(t *testing.T) {
	for _, ending := range []string{"close", "cancel", "foreign-query", "nil-context", "nil-destination", "eof"} {
		t.Run(ending, func(t *testing.T) {
			root := memory.NewEngineBudget(1 << 20)
			query, _ := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 64 << 10})
			defer query.Close()
			input := topKMutableSource(100, 50)
			unlimited := memory.NewBudgetManager(0)
			top, _ := NewTopKSourceWithManager(input, 3, []SortKey{{Column: "key"}}, unlimited)
			stream, err := top.NewBatchStream(memory.WithQueryBudget(context.Background(), query), testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			// Calling Next without the query in its context must not bypass it.
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatal(ok, err)
			}
			if query.Used() <= 0 || query.Snapshot().Operators["TopKSnapshot"] <= 0 || unlimited.Used() != 0 {
				t.Fatalf("wrong manager: %+v", query.Snapshot())
			}
			var finishErr error
			switch ending {
			case "close":
				finishErr = stream.Close()
			case "cancel":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, finishErr = stream.Next(ctx, memory.NewBatch())
				if !errors.Is(finishErr, context.Canceled) {
					t.Fatal(finishErr)
				}
			case "foreign-query":
				other, _ := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 64 << 10})
				defer other.Close()
				_, finishErr = stream.Next(memory.WithQueryBudget(context.Background(), other), memory.NewBatch())
				if finishErr == nil || other.Used() != 0 {
					t.Fatal("query ownership changed")
				}
			case "nil-context":
				_, finishErr = stream.Next(nil, memory.NewBatch())
			case "nil-destination":
				_, finishErr = stream.Next(context.Background(), nil)
			case "eof":
				for {
					ok, err := stream.Next(context.Background(), memory.NewBatch())
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
				}
			}
			if (ending == "close" || ending == "eof") && finishErr != nil {
				t.Fatal(finishErr)
			}
			if out.Rows != 0 || query.Used() != 0 || root.Used() != 0 || input.closes != 1 {
				t.Fatalf("rows=%d retained=%d root=%d closes=%d", out.Rows, query.Used(), root.Used(), input.closes)
			}
			_ = stream.Close()
		})
	}
}

func TestTopKSnapshotZeroDoesNotOpenInput(t *testing.T) {
	input := topKMutableSource(1, 1)
	top, _ := NewTopKSourceWithManager(input, 0, []SortKey{{Column: "key"}}, memory.NewBudgetManager(1))
	stream, err := top.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if ok, err := stream.Next(context.Background(), memory.NewBatch()); ok || err != nil {
		t.Fatal(ok, err)
	}
	if input.opens != 0 || input.nexts != 0 || input.closes != 0 {
		t.Fatal("K=0 consumed input")
	}
}

func TestTopKSnapshotLargeKOnlyAllocatesObservedCandidates(t *testing.T) {
	for _, count := range []int{0, 3} {
		input := topKMutableSource(count, 1)
		manager := &topKPeakManager{BudgetManager: memory.NewBudgetManager(16 << 10)}
		top, _ := NewTopKSourceWithManager(input, math.MaxInt, []SortKey{{Column: "key"}}, manager)
		stream, err := top.NewBatchStream(context.Background(), testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		stats, err := source.DrainBatchStream(context.Background(), stream)
		if err != nil || stats.Rows != int64(count) || manager.Used() != 0 {
			t.Fatalf("count=%d stats=%+v err=%v used=%d", count, stats, err, manager.Used())
		}
	}
}

func TestTopKSnapshotWideReplacementAdmission(t *testing.T) {
	for _, winning := range []bool{false, true} {
		input := topKMutableSource(2, 1)
		input.fill = func(i int, b *memory.Batch) {
			b.Int64s["key"].Values[0] = 2
			if i == 1 {
				b.Int64s["key"].Values[0] = 3
				if winning {
					b.Int64s["key"].Values[0] = 1
				}
				b.Strings["payload"].Data = make([]byte, 1<<20)
				b.Strings["payload"].Offsets[1] = 1 << 20
			}
		}
		manager := memory.NewBudgetManager(16 << 10)
		top, _ := NewTopKSourceWithManager(input, 1, []SortKey{{Column: "key"}}, manager)
		stream, err := top.NewBatchStream(context.Background(), testReadOptions())
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		ok, err := stream.Next(context.Background(), out)
		if winning {
			if ok || !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 || input.closes != 1 {
				t.Fatalf("replacement failure=(%v,%v) used=%d closes=%d", ok, err, manager.Used(), input.closes)
			}
		} else if !ok || err != nil || out.Int64s["key"].Values[0] != 2 || len(out.Strings["payload"].Data) != 1 {
			t.Fatalf("losing payload was retained: (%v,%v)", ok, err)
		}
		_ = stream.Close()
		if manager.Used() != 0 {
			t.Fatal("replacement leaked budget")
		}
	}
}

func TestTopKSnapshotDictionaryDomainsAndNestedNulls(t *testing.T) {
	childField := memory.Field{Name: "text", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}
	input := topKMutableSource(12, 2)
	input.schema = memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString}, memory.Field{Name: "dict", Type: memory.TypeString, Encoding: memory.EncodingDictionary}, memory.StructField("nested", true, childField))
	input.batch.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 2}, Data: make([]byte, 2), Encoding: memory.EncodingDictionary}
	child := memory.NewBatch()
	child.Rows = 1
	child.DictStrings["text"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 2}, Data: make([]byte, 2), Encoding: memory.EncodingDictionary}
	input.batch.Structs["nested"] = &memory.StructVector{Rows: 1, Fields: []memory.Field{childField}, Values: child, Validity: memory.Bitmap{1}}
	baseFill := input.fill
	input.fill = func(i int, b *memory.Batch) {
		baseFill(i, b)
		for _, d := range []*memory.DictionaryStringVector{b.DictStrings["dict"], child.DictStrings["text"]} {
			d.Data[0], d.Data[1] = byte('a'+i), byte('A'+i)
			d.DictionaryDomain = fmt.Sprintf("batch-%d", i)
		}
		b.Structs["nested"].Validity[0] = uint64(i % 2)
	}
	manager := memory.NewBudgetManager(64 << 10)
	top, _ := NewTopKSourceWithManager(input, 5, []SortKey{{Column: "key"}}, manager)
	stream, err := top.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	for j := 0; j < 5; j++ {
		if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
			t.Fatal(ok, err)
		}
		i := 11 - j
		d := out.DictStrings["dict"]
		if d.DictionaryDomain != fmt.Sprintf("batch-%d", i) || string(d.Data) != string([]byte{byte('a' + i), byte('A' + i)}) {
			t.Fatal("dictionary was aliased or remapped")
		}
		nested := out.Structs["nested"]
		if nested.IsValid(0) != (i%2 == 1) || nested.Values.DictStrings["text"].IsValid(0) != (i%2 == 1) {
			t.Fatal("nested NULL changed")
		}
		if i%2 == 1 && !reflect.DeepEqual(nested.Values.DictStrings["text"].Data, d.Data) {
			t.Fatal("nested dictionary changed")
		}
	}
	_ = stream.Close()
	if manager.Used() != 0 {
		t.Fatal("dictionary snapshots leaked budget")
	}
}

func TestTopKSnapshotOpaqueBackingAdmissionAndOwnership(t *testing.T) {
	for _, budget := range []int64{16 << 10, 2 << 20} {
		for _, ending := range []string{"eof", "close", "cancel", "read-error"} {
			t.Run(fmt.Sprintf("budget=%d/%s", budget, ending), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				input := topKMutableSource(4, 1)
				input.schema = memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "payload", Type: memory.TypeString}, memory.Field{Name: "opaque", Type: memory.TypeArrow, ArrowType: arrow.BinaryTypes.LargeString.String()})
				fill := input.fill
				input.fill = func(i int, b *memory.Batch) {
					fill(i, b)
					if v := b.Opaque["opaque"]; v != nil {
						v.Value.(arrow.Array).Release()
					}
					builder := array.NewLargeStringBuilder(alloc)
					builder.Append(fmt.Sprintf("winner-%d", i))
					builder.Append(strings.Repeat("z", 256<<10))
					full := builder.NewArray()
					builder.Release()
					// A single logical row still retains the large original backing.
					view := array.NewSlice(full, 0, 1)
					full.Release()
					b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, ArrowType: arrow.BinaryTypes.LargeString.String(), Value: view, Owned: true}
				}
				if ending == "read-error" {
					input.nextErr = errors.New("injected read failure")
				}
				manager := memory.NewBudgetManager(budget)
				top, _ := NewTopKSourceWithManager(input, 1, []SortKey{{Column: "key"}}, manager)
				stream, err := top.NewBatchStream(context.Background(), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				out := memory.NewBatch()
				ok, err := stream.Next(context.Background(), out)
				if budget == 16<<10 {
					if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
						t.Fatalf("large backing accepted: %v %v", ok, err)
					}
				} else if ending == "read-error" {
					if ok || !errors.Is(err, input.nextErr) {
						t.Fatal(ok, err)
					}
				} else {
					if !ok || err != nil {
						t.Fatal(ok, err)
					}
					if out.Opaque["opaque"].Owned {
						t.Fatal("output steals snapshot ownership")
					}
					if out.Opaque["opaque"].Value.(*array.LargeString).Value(0) != "winner-3" {
						t.Fatal("opaque winner corrupt")
					}
					if manager.Used() < 256<<10 {
						t.Fatal("opaque backing was not charged")
					}
					out.ClearColumns() // Must not free the stream's retained Arrow reference.
					if alloc.CurrentAlloc() == 0 {
						t.Fatal("clearing borrowed output released owner")
					}
					switch ending {
					case "close":
						_ = stream.Close()
					case "cancel":
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						_, err = stream.Next(ctx, memory.NewBatch())
						if !errors.Is(err, context.Canceled) {
							t.Fatal(err)
						}
					default:
						if ok, err := stream.Next(context.Background(), memory.NewBatch()); ok || err != nil {
							t.Fatal(ok, err)
						}
					}
				}
				_ = stream.Close()
				if manager.Used() != 0 || input.closes != 1 {
					t.Fatalf("used=%d closes=%d", manager.Used(), input.closes)
				}
			})
		}
	}
}
