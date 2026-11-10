package physical

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

// Every stream reuses its vectors, including dictionary byte/code/offset buffers.
// Replay stability promises identical logical rows, not immutable vector storage.
type residentDictionaryInput struct {
	schema        memory.Schema
	batches       []*memory.Batch
	stats         source.Stats
	statsErr      error
	after         func(int)
	opens, closes int
}

func (s *residentDictionaryInput) Schema() memory.Schema { return s.schema }
func (s *residentDictionaryInput) Stats(context.Context) (source.Stats, error) {
	return s.stats, s.statsErr
}
func (s *residentDictionaryInput) Partitions(context.Context) ([]source.Partition, error) {
	return nil, errors.New("use BatchStream")
}
func (s *residentDictionaryInput) Replayability() source.Replayability { return source.ReplayStable }
func (s *residentDictionaryInput) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	b := memory.NewBatch()
	b.DictStrings["key"] = &memory.DictionaryStringVector{}
	b.Int64s["id"] = &memory.Int64Vector{}
	return &residentDictionaryInputStream{owner: s, batch: b}, nil
}

type residentDictionaryInputStream struct {
	owner  *residentDictionaryInput
	batch  *memory.Batch
	next   int
	closed bool
}

func (s *residentDictionaryInputStream) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	dst.ClearColumns()
	if s.next == len(s.owner.batches) {
		return false, nil
	}
	src := s.owner.batches[s.next]
	d, v := s.batch.DictStrings["key"], src.DictStrings["key"]
	d.Indices = append(d.Indices[:0], v.Indices...)
	d.Offsets = append(d.Offsets[:0], v.Offsets...)
	d.Data = append(d.Data[:0], v.Data...)
	d.Validity = append(d.Validity[:0], v.Validity...)
	d.Encoding, d.DictionaryDomain = v.Encoding, v.DictionaryDomain
	s.batch.Int64s["id"].Values = append(s.batch.Int64s["id"].Values[:0], src.Int64s["id"].Values...)
	s.batch.Rows = src.Rows
	bindAll(dst, s.batch)
	borrowFilterOwners(dst)
	dst.Rows = src.Rows
	s.next++
	if s.owner.after != nil {
		s.owner.after(s.next)
	}
	return true, nil
}
func (s *residentDictionaryInputStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes++
		// A consumer may not retain dictionary storage after child close either.
		for i := range s.batch.DictStrings["key"].Data {
			s.batch.DictStrings["key"].Data[i] = 0x42
		}
		s.batch.ClearColumns()
	}
	return nil
}
func newResidentDictionaryInput(typ memory.DataType) (*residentDictionaryInput, []binaryDictionarySortRow) {
	batches, rows := binaryDictionarySortFixture(4)
	for _, b := range batches {
		d := b.DictStrings["key"]
		d.Data = append([]byte("prefix"), d.Data...)
		for i := range d.Offsets {
			d.Offsets[i] += 6
		}
		d.Data = append(d.Data, []byte("unused-tail")...)
	}
	return &residentDictionaryInput{schema: memory.NewSchema(memory.Field{Name: "key", Type: typ, Encoding: memory.EncodingDictionary, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}), batches: batches, stats: source.Stats{Rows: int64(len(rows))}}, rows
}
func openResidentDictionaryOrder(t *testing.T, ctx context.Context, input source.Source, top bool, k int, key SortKey, manager memory.Manager) source.BatchStream {
	t.Helper()
	var owner source.Source
	var err error
	if top {
		owner, err = NewTopKSourceWithManager(input, k, []SortKey{key}, manager)
	} else {
		owner, err = NewSortSource(input, []SortKey{key}, manager)
	}
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(ctx, owner, testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}
func TestResidentDictionaryMixedValuesMutableReplay(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, top := range []bool{false, true} {
			for _, stats := range []string{"exact", "understated", "unknown", "unavailable"} {
				for _, desc := range []bool{false, true} {
					for _, nullsLast := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/top=%v/stats=%s/desc=%v/nullsLast=%v", typ, top, stats, desc, nullsLast), func(t *testing.T) {
							input, rows := newResidentDictionaryInput(typ)
							switch stats {
							case "understated":
								input.stats = source.Stats{Rows: 1, Bytes: 1}
							case "unknown":
								input.stats = source.Stats{Rows: -1, Bytes: -1}
							case "unavailable":
								input.statsErr = errors.New("stats unavailable")
							}
							key := SortKey{Column: "key", Descending: desc, NullsLast: nullsLast}
							sort.SliceStable(rows, func(i, j int) bool { return compareBinaryDictionarySortRows(rows[i], rows[j], key) < 0 })
							k := len(rows) - 3
							if top {
								rows = rows[:k]
							}
							manager := memory.NewBudgetManager(4 << 20)
							stream := openResidentDictionaryOrder(t, context.Background(), input, top, k, key, manager)
							out := memory.NewBatch()
							defer out.ReleaseOwned()
							seen := 0
							for {
								ok, err := stream.Next(context.Background(), out)
								if err != nil {
									t.Fatal(err)
								}
								if !ok {
									break
								}
								if manager.Used() <= 0 {
									t.Fatal("live output has no reservation")
								}
								d := out.DictStrings["key"]
								if d == nil {
									t.Fatal("dictionary encoding lost")
								}
								for i := 0; i < out.Rows; i++ {
									if seen >= len(rows) {
										t.Fatal("extra output row")
									}
									want := rows[seen]
									if out.Int64s["id"].Values[i] != want.id || d.IsValid(i) != want.valid {
										t.Fatalf("row %d: id=%d valid=%v, want id=%d valid=%v", seen, out.Int64s["id"].Values[i], d.IsValid(i), want.id, want.valid)
									}
									if want.valid && !bytes.Equal(d.DictionaryValue(d.Indices[i]), want.value) {
										t.Fatalf("row %d: got %x want %x", seen, d.DictionaryValue(d.Indices[i]), want.value)
									}
									seen++
								}
							}
							if seen != len(rows) {
								t.Fatalf("rows=%d want=%d", seen, len(rows))
							}
							if err := stream.Close(); err != nil {
								t.Fatal(err)
							}
							if err := stream.Close(); err != nil {
								t.Fatal(err)
							}
							if manager.Used() != 0 {
								t.Fatalf("leaked reservation: %d", manager.Used())
							}
							if input.opens != input.closes {
								t.Fatalf("child opens=%d closes=%d", input.opens, input.closes)
							}
						})
					}
				}
			}
		}
	}
}

func TestResidentDictionaryDynamicAdmissionAndCancellation(t *testing.T) {
	for _, top := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("top=%v/cancelled=%v", top, cancelled), func(t *testing.T) {
				input, _ := newResidentDictionaryInput(memory.TypeBinary)
				input.stats = source.Stats{Rows: 1, Bytes: 1}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				limit := int64(64 << 10)
				if cancelled {
					limit = 4 << 20
					input.after = func(n int) {
						if n == 2 {
							cancel()
						}
					}
				} else {
					// The initial row estimate fits, but one referenced dictionary value does
					// not. Growth must be admitted dynamically, before retaining the bytes.
					d := input.batches[1].DictStrings["key"]
					d.Data = bytes.Repeat([]byte{0xfe}, 128<<10)
					d.Offsets = []uint32{0, uint32(len(d.Data))}
					for i := range d.Indices {
						if d.IsValid(i) {
							d.Indices[i] = 0
						}
					}
				}
				manager := &topKPeakManager{BudgetManager: memory.NewBudgetManager(limit)}
				stream := openResidentDictionaryOrder(t, ctx, input, top, 36, SortKey{Column: "key"}, manager)
				out := memory.NewBatch()
				defer out.ReleaseOwned()
				ok, err := stream.Next(ctx, out)
				expected := memory.ErrBudgetExceeded
				if cancelled {
					expected = context.Canceled
				}
				if ok || !errors.Is(err, expected) {
					t.Fatalf("Next=(%v,%v), want %v", ok, err, expected)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 || manager.peak > limit {
					t.Fatalf("used=%d peak=%d limit=%d", manager.Used(), manager.peak, limit)
				}
				if input.opens != input.closes {
					t.Fatalf("child opens=%d closes=%d", input.opens, input.closes)
				}
			})
		}
	}
}

func TestResidentDictionaryOutputBorrowEndsOnClose(t *testing.T) {
	for _, top := range []bool{false, true} {
		t.Run(fmt.Sprintf("top=%v", top), func(t *testing.T) {
			input, rows := newResidentDictionaryInput(memory.TypeBinary)
			manager := memory.NewBudgetManager(4 << 20)
			stream := openResidentDictionaryOrder(t, context.Background(), input, top, len(rows), SortKey{Column: "key"}, manager)
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatal(ok, err)
			}
			aliases := captureSortOutputOwnershipAliases(out)
			if manager.Used() == 0 {
				t.Fatal("output borrow is uncharged")
			}
			// ReleaseOwned on borrowed output must not release the stream's ownership.
			out.ReleaseOwned()
			if manager.Used() == 0 {
				t.Fatal("borrower released the stream's reservation")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			aliases.checkCleared(t)
			if manager.Used() != 0 {
				t.Fatal("output owner leaked", manager.Used())
			}
		})
	}
}
