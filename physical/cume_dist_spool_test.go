package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strconv"
	"testing"
)

func cumeSpoolOwner(src source.Source, m memory.Manager, both bool) *percentRankSource {
	o := rankSpoolOwner(src, m)
	o.cumeHidden = "cume"
	if !both {
		o.hidden = ""
	}
	fields := append([]memory.Field(nil), src.Schema().Fields...)
	if both {
		fields = append(fields, memory.Field{Name: "ratio", Type: memory.TypeFloat64})
	}
	fields = append(fields, memory.Field{Name: "cume", Type: memory.TypeFloat64})
	o.schema = memory.NewSchema(fields...)
	return o
}

func cumeFloatSource(t *testing.T, mode string) *percentRankReviewSource {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "k", Type: memory.TypeFloat64, Nullable: true})
	b := memory.NewBatch()
	b.Rows = 70
	b.Int64s["g"] = &memory.Int64Vector{Values: make([]int64, 70)}
	v := &memory.Float64Vector{Values: make([]float64, 70), Validity: make(memory.Bitmap, memory.WordsFor(70))}
	b.Float64s["k"] = v
	for i := 0; i < 70; i++ {
		memory.Set(v.Validity, i)
		switch mode {
		case "unique":
			v.Values[i] = float64(i)
		case "special":
			if i < 3 {
				v.Validity[i>>6] &^= uint64(1) << uint(i&63)
			} else if i < 66 {
				v.Values[i] = math.Copysign(0, float64(1-2*(i%2)))
			} else {
				v.Values[i] = math.NaN()
			}
		}
	}
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	return &percentRankReviewSource{Source: base}
}
func TestCumeDistSpoolPeerEndsAndTwoDecodes(t *testing.T) {
	for _, mode := range []string{"all-peer", "unique", "special"} {
		for _, both := range []bool{false, true} {
			for _, disk := range []bool{false, true} {
				t.Run(fmt.Sprint(mode, both, disk), func(t *testing.T) {
					ctx, q := percentRankReviewQuery(t, 8<<20, disk)
					src := cumeFloatSource(t, mode)
					st, err := newPercentRankPartitionStream(ctx, cumeSpoolOwner(src, q, both), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
					if err != nil {
						t.Fatal(err)
					}
					s := st.(*percentRankPartitionStream)
					out := memory.NewBatch()
					count := 0
					for {
						ok, err := s.Next(ctx, out)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						for row := 0; row < out.Rows; row++ {
							end, start := 70, 0
							switch mode {
							case "unique":
								end = count + 1
								start = count
							case "special":
								if count < 3 {
									end = 3
								} else if count < 66 {
									end = 66
									start = 3
								} else {
									start = 66
								}
							}
							if got := out.Float64s["cume"].Values[row]; got != float64(end)/70 {
								t.Fatal(count, got, end)
							}
							if both && out.Float64s["ratio"].Values[row] != float64(start)/69 {
								t.Fatal("PR", count)
							}
							count++
						}
					}
					if count != 70 {
						t.Fatal(count)
					}
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
					d := s.StreamDiagnostics()
					peek, _ := strconv.Atoi(d["peer_lookahead_pages"])
					replay, _ := strconv.Atoi(d["partition_replay_pages"])
					if peek != 18 || replay != 18 {
						t.Fatal("must decode each captured page exactly once per cursor", d)
					}
					if q.Used() != 0 || src.opens != 1 || src.closes != 1 || s.peekDecoded != nil || s.peerLease != nil {
						t.Fatal("leak", q.Used(), src.opens, src.closes)
					}
				})
			}
		}
	}
}
func TestCumeDistSpoolBytePeers(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, dict := range []bool{false, true} {
			for _, disk := range []bool{false, true} {
				t.Run(fmt.Sprint(kind, dict, disk), func(t *testing.T) {
					ctx, q := percentRankReviewQuery(t, 8<<20, disk)
					src := cumeSafeByteInput(t, kind, dict)
					st, err := newPercentRankPartitionStream(ctx, cumeSpoolOwner(src, q, true), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
					if err != nil {
						t.Fatal(err)
					}
					want := []float64{2.0 / 6, 2.0 / 6, 4.0 / 6, 4.0 / 6, 5.0 / 6, 1, 3.0 / 5, 3.0 / 5, 3.0 / 5, 4.0 / 5, 1}
					n := 0
					b := memory.NewBatch()
					for {
						ok, err := st.Next(ctx, b)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						for _, v := range b.Float64s["cume"].Values {
							if n >= len(want) || v != want[n] {
								t.Fatal(n, v)
							}
							n++
						}
					}
					if n != len(want) {
						t.Fatal(n)
					}
					if err = st.Close(); err != nil {
						t.Fatal(err)
					}
					if q.Used() != 0 || src.opens != 1 || src.closes != 1 {
						t.Fatal("leak", q.Used())
					}
				})
			}
		}
	}
}
func cumeAdmissionRun(t *testing.T, fail int) (int, error) {
	t.Helper()
	m := &rankSpoolFailManager{Manager: memory.NewBudgetManager(8 << 20), fail: fail}
	src := cumeSafeByteInput(t, memory.TypeBinary, true)
	st, err := newPercentRankPartitionStream(context.Background(), cumeSpoolOwner(src, m, true), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2, MemoryManager: m})
	if err == nil {
		b := memory.NewBatch()
		for {
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				err = e
				break
			}
			if !ok {
				break
			}
		}
		err = errors.Join(err, st.Close())
	}
	if m.Used() != 0 {
		t.Fatal("leak", fail, m.Used())
	}
	if src.opens > 1 || src.closes != src.opens {
		t.Fatal("source lifecycle", fail, src.opens, src.closes)
	}
	return m.calls, err
}
func TestCumeDistSpoolEveryAdmission(t *testing.T) {
	calls, err := cumeAdmissionRun(t, 0)
	if err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= calls; n++ {
		_, err = cumeAdmissionRun(t, n)
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal(n, err)
		}
	}
	t.Log("actual admissions", calls)
}

type cumeReadFault struct {
	*rankSpoolFaultStore
	reads, failRead int
	cancel          context.CancelFunc
}

// Restore injects independent failures for peek and output reads.
func (s *cumeReadFault) Restore(ctx context.Context, h memory.SpillHandle, f [32]byte, m memory.Manager) (*memory.RestoredSpill, error) {
	s.reads++
	if s.reads == s.failRead {
		return nil, rankSpoolSentinel
	}
	r, e := s.rankSpoolFaultStore.Restore(ctx, h, f, m)
	if s.cancel != nil && s.reads == 1 {
		s.cancel()
	}
	return r, e
}
func TestCumeDistSpoolRestoreDeleteAndCancel(t *testing.T) {
	for _, mode := range []string{"peek", "output", "delete", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			src := cumeFloatSource(t, "all-peer")
			st, err := newPercentRankPartitionStream(ctx, cumeSpoolOwner(src, q, true), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
			if err != nil {
				t.Fatal(err)
			}
			s := st.(*percentRankPartitionStream)
			_ = s.store.Close()
			base, err := q.SpillManager()
			if err != nil {
				t.Fatal(err)
			}
			fault := &cumeReadFault{rankSpoolFaultStore: &rankSpoolFaultStore{SpillManager: base}}
			want := error(rankSpoolSentinel)
			switch mode {
			case "peek":
				fault.failRead = 1
			case "output":
				fault.failRead = 19
			case "delete":
				fault.mode = "delete"
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				fault.cancel = cancel
				want = context.Canceled
			}
			s.store = spillmeta.NewStore(fault, s.metadata)
			_, err = s.Next(ctx, memory.NewBatch())
			if !errors.Is(err, want) {
				t.Fatal(mode, err)
			}
			_ = s.Close()
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || src.closes != 1 || s.peekDecoded != nil || s.owner != nil {
				t.Fatal("cleanup", q.Used(), src.closes)
			}
		})
	}
}

func TestCumeDistSpoolRejectsEmptyPageAndInvalidIndex(t *testing.T) {
	for _, mode := range []string{"zero-metadata", "zero-decoded", "negative-index", "past-end-index"} {
		t.Run(mode, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, false)
			src := cumeFloatSource(t, "all-peer")
			st, err := newPercentRankPartitionStream(ctx, cumeSpoolOwner(src, q, true), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
			if err != nil {
				t.Fatal(err)
			}
			s := st.(*percentRankPartitionStream)
			ok, err := s.capturePartition(ctx)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			if mode == "negative-index" || mode == "past-end-index" {
				index := -1
				if mode == "past-end-index" {
					index = len(s.pages.Values)
				}
				_, err = s.decodePage(ctx, index)
			} else {
				// A structurally valid zero-row codec payload plus corrupt page metadata
				// must not make lookahead attempt snapshotCell(row=0) on an empty vector.
				b := memory.NewBatch()
				b.Int64s["g"] = &memory.Int64Vector{}
				b.Float64s["k"] = &memory.Float64Vector{}
				encoded, e := memory.EncodeColumnarRun(ctx, s.owner.original, b, s.manager)
				if e != nil {
					t.Fatal(e)
				}
				ref := &s.pages.Values[0]
				if ref.encoded != nil {
					_ = ref.encoded.Close()
				}
				ref.encoded = encoded
				if mode == "zero-metadata" {
					ref.rows = 0
				}
				_, err = s.Next(ctx, memory.NewBatch())
			}
			if !errors.Is(err, memory.ErrSpillCorrupt) {
				t.Fatal(mode, err)
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			if q.Used() != 0 || src.opens != 1 || src.closes != 1 || s.peekDecoded != nil || s.peerLease != nil {
				t.Fatal("leak", q.Used(), src.opens, src.closes)
			}
		})
	}
}

func TestCumeDistSpoolFlatProfileThreePaths(t *testing.T) {
	for _, both := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprint(mode, "/both=", both), func(t *testing.T) {
				schema, expected := rankSpoolFlatProfile(t)
				oracle, e := memory.ArrowRecord(am.DefaultAllocator, schema, expected)
				if e != nil {
					t.Fatal(e)
				}
				defer oracle.Release()
				ids := []int{3, 0, 4, 1, 2}
				if mode == WindowExecutionOrdered {
					ids = []int{0, 1, 2, 3, 4}
				}
				var batches []*memory.Batch
				for _, id := range ids {
					b, e := memory.CloneBatchRange(expected, id, 1)
					if e != nil {
						t.Fatal(e)
					}
					batches = append(batches, b)
				}
				input, e := inmemory.New(schema, batches...)
				if e != nil {
					t.Fatal(e)
				}
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				expressions := []WindowExpression{{Function: WindowCumeDist, Alias: "cd"}}
				fields := []memory.Field{{Name: "cd", Type: memory.TypeFloat64}}
				if both {
					expressions = append(expressions, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
					fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
				}
				win, e := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, expressions, fields, q, WindowSourceOptions{Mode: mode})
				if e != nil {
					t.Fatal(e)
				}
				st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				count := 0
				for {
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					rec, e := memory.ArrowRecord(am.DefaultAllocator, win.Schema(), out)
					if e != nil {
						t.Fatal(e)
					}
					for row, id := range out.Int64s["id"].Values {
						if int(id) != ids[count] {
							t.Fatal("row order", id, ids[count])
						}
						count++
						if both && (out.Float64s["pr"].Values[row] != float64(id)/4 || !out.Float64s["pr"].IsValid(row)) {
							t.Fatal("rank", id)
						}
						if out.Float64s["cd"].Values[row] != float64(id+1)/5 || !out.Float64s["cd"].IsValid(row) {
							t.Fatal("cume", id)
						}
						for _, f := range fields {
							got, _ := win.Schema().Field(f.Name)
							if got.Type != memory.TypeFloat64 || got.Nullable {
								t.Fatal("distribution metadata", got)
							}
						}
						for col, f := range schema.Fields {
							if !arrow.TypeEqual(rec.Schema().Field(col).Type, oracle.Schema().Field(col).Type) {
								t.Fatal("type", f.Name)
							}
							gotField, wantField := rec.Schema().Field(col), oracle.Schema().Field(col)
							if gotField.Name != wantField.Name || gotField.Nullable != wantField.Nullable || !gotField.Metadata.Equal(wantField.Metadata) {
								t.Fatal("metadata", f.Name)
							}
							if rec.Column(col).IsNull(row) != oracle.Column(col).IsNull(int(id)) {
								t.Fatal("NULL", f.Name, id)
							}
							actual := array.NewSlice(rec.Column(col), int64(row), int64(row+1))
							want := array.NewSlice(oracle.Column(col), id, id+1)
							equal := array.Equal(actual, want)
							actual.Release()
							want.Release()
							if !equal {
								t.Fatal("raw value", f.Name, id)
							}
						}
					}
					rec.Release()
				}
				if count != 5 {
					t.Fatal(count)
				}
				if e = st.Close(); e != nil {
					t.Fatal(e)
				}
				if q.Used() != 0 {
					t.Fatal("leak", q.Used())
				}
			})
		}
	}
}

// cumeSafePoisonSource preserves the backend's four-row batches while poisoning
// only an independently admitted published range. The older wrapper poisons a
// shared dictionary table and therefore corrupts still-unpublished backend rows
// when BatchRows splits one backend batch; it is intentionally not modified.
type cumeSafePoisonSource struct {
	source.Source
	opens, closes int
}

func cumeSafeByteInput(t *testing.T, kind memory.DataType, dict bool) *cumeSafePoisonSource {
	old := rankSpoolByteInput(t, kind, dict)
	return &cumeSafePoisonSource{Source: old.Source}
}

// NewBatchStream opens the original backend once and accounts the private view.
func (s *cumeSafePoisonSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if s.opens != 0 {
		return nil, fmt.Errorf("source reopened")
	}
	manager := opts.MemoryManager
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		manager = q.Operator("SourceScan", memory.MemoryWorkspace)
	}
	if manager == nil {
		return nil, fmt.Errorf("source clone requires accounting")
	}
	base, err := manager.Reserve(ctx, 4096)
	if err != nil {
		return nil, err
	}
	child, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		base.Release()
		return nil, err
	}
	s.opens++
	return &cumeSafePoisonStream{owner: s, child: child, manager: manager, base: base, scratch: memory.NewBatch()}, nil
}

type cumeSafePoisonStream struct {
	owner          *cumeSafePoisonSource
	child          source.BatchStream
	manager        memory.Manager
	scratch, owned *memory.Batch
	lease, base    memory.Reservation
	closed         bool
}

func (s *cumeSafePoisonStream) retire() {
	if s.owned != nil {
		for _, v := range s.owned.Strings {
			for i := range v.Data {
				v.Data[i] = 255
			}
		}
		for _, v := range s.owned.DictStrings {
			for i := range v.Data {
				v.Data[i] = 255
			}
		}
		s.owned.ReleaseOwned()
		s.owned = nil
	}
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
}

// Next publishes an owned clone and invalidates only the previous published clone.
func (s *cumeSafePoisonStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	dst.ClearColumns()
	s.retire()
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	ok, err = s.child.Next(ctx, s.scratch)
	if err != nil || !ok {
		return ok, err
	}
	estimate, err := memory.EstimateNativeOwnedBatchRangeBytes(s.scratch, 0, s.scratch.Rows)
	if err != nil {
		return false, err
	}
	s.lease, err = s.manager.Reserve(ctx, estimate.Bytes)
	if err != nil {
		return false, err
	}
	s.owned, err = memory.CloneBatchRange(s.scratch, 0, s.scratch.Rows)
	if err != nil {
		return false, err
	}
	bindAll(dst, s.owned)
	dst.Reset(s.owned.Rows)
	return true, nil
}

// Close invalidates the last published borrow and releases every private owner.
func (s *cumeSafePoisonStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.retire()
	err := s.child.Close()
	s.child = nil
	s.scratch = nil
	s.manager = nil
	s.owner.closes++
	s.owner = nil
	s.base.Release()
	s.base = nil
	return err
}
func TestCumeDistSafePoisonSourceContract(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, dict := range []bool{false, true} {
			t.Run(fmt.Sprint(kind, dict), func(t *testing.T) {
				ctx, q := percentRankReviewQuery(t, 8<<20, false)
				src := cumeSafeByteInput(t, kind, dict)
				st, err := source.OpenBatchStream(ctx, src, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				b := memory.NewBatch()
				want := []string{"a", "a", "b", "b", "c", "d", "a", "a", "a", "b", "c"}
				n := 0
				var previous []byte
				for {
					ok, err := st.Next(ctx, b)
					if err != nil {
						t.Fatal(err)
					}
					for _, v := range previous {
						if v != 255 {
							t.Fatal("old borrow not poisoned")
						}
					}
					if !ok {
						break
					}
					if b.Rows < 1 || b.Rows > 2 {
						t.Fatal("pagination", b.Rows)
					}
					for row := 0; row < b.Rows; row++ {
						var value []byte
						if dict {
							v := b.DictStrings["k"]
							value = v.DictionaryValue(v.Indices[row])
						} else {
							value = b.Strings["k"].ValueBytes(row)
						}
						if n >= len(want) || string(value) != want[n] {
							t.Fatal("unpublished source corrupted", n, value)
						}
						group := "first"
						if n >= 6 {
							group = "second"
						}
						if string(b.Strings["g"].ValueBytes(row)) != group {
							t.Fatal("partition", n)
						}
						n++
					}
					if dict {
						previous = b.DictStrings["k"].Data
					} else {
						previous = b.Strings["k"].Data
					}
				}
				if n != 11 {
					t.Fatal(n)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 || src.opens != 1 || src.closes != 1 {
					t.Fatal("cleanup", q.Used(), src.opens, src.closes)
				}
			})
		}
	}
}
