package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

type rankResidentReuseSource struct {
	source.Source
	opens, closes int
	huge          bool
	sameDomain    bool
	failAt        int
	cancel        context.CancelFunc
}

// Stats intentionally understates both cardinality and live payload size.
func (s *rankResidentReuseSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 1, Bytes: 1}, nil
}

// NewBatchStream exposes one non-replayable, mutable borrowed batch.
func (s *rankResidentReuseSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("reopened source")
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 1)}
	b.Float64s["key"] = &memory.Float64Vector{Values: make([]float64, 1), Validity: memory.Bitmap{1}}
	for _, name := range []string{"text", "bin"} {
		b.DictStrings[name] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte{'x'}, Validity: memory.Bitmap{1}, Encoding: memory.EncodingDictionary, DictionaryDomain: "initial"}
	}
	return &rankResidentReuseStream{owner: s, batch: b}, nil
}

type rankResidentReuseStream struct {
	owner  *rankResidentReuseSource
	batch  *memory.Batch
	next   int
	closed bool
}

func (s *rankResidentReuseStream) poison() {
	s.batch.Int64s["id"].Values[0] = -99
	s.batch.Float64s["key"].Values[0] = -99
	for _, v := range s.batch.DictStrings {
		for i := range v.Data {
			v.Data[i] = 255
		}
		v.Indices[0] = 999
		v.Offsets[1] = 999
		v.Validity[0] = 0
	}
}

// Next overwrites the same descriptors and buffers after invalidating old data.
func (s *rankResidentReuseStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.poison()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.owner.failAt == s.next+1 {
		return false, rankSpoolSentinel
	}
	if s.next == 4 {
		return false, nil
	}
	row := s.next
	s.next++
	s.batch.Int64s["id"].Values[0] = int64(row)
	s.batch.Float64s["key"].Values[0] = []float64{2, 0, 1, 2}[row]
	s.batch.Float64s["key"].Validity[0] = 1
	if row == 1 {
		s.batch.Float64s["key"].Validity[0] = 0
	}
	for _, name := range []string{"text", "bin"} {
		v := s.batch.DictStrings[name]
		v.Indices[0] = 0
		v.Offsets[1] = 1
		v.Data = v.Data[:1]
		v.Data[0] = byte('a' + row)
		v.Validity[0] = 1
		v.DictionaryDomain = fmt.Sprint(name, row)
		if s.owner.sameDomain {
			v.DictionaryDomain = name
		}
		if name == "bin" {
			v.Data[0] = byte(128 + row)
		}
		if row == 2 {
			v.Validity[0] = 0
			v.Indices[0] = 999
		}
		if s.owner.huge {
			v.Data = make([]byte, 9<<20)
			v.Offsets[1] = uint32(len(v.Data))
		}
	}
	bindAll(dst, s.batch)
	dst.Reset(s.batch.Rows)
	if s.owner.cancel != nil {
		s.owner.cancel()
	}
	return true, nil
}

// Close invalidates the final borrow exactly once.
func (s *rankResidentReuseStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes++
		s.poison()
	}
	return nil
}

func rankResidentReuseInput(t *testing.T) *rankResidentReuseSource {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "text", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true}, memory.Field{Name: "bin", Type: memory.TypeBinary, Encoding: memory.EncodingDictionary, Nullable: true})
	base, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	return &rankResidentReuseSource{Source: base}
}
func rankResidentOpen(t *testing.T, ctx context.Context, q *memory.QueryBudget, src source.Source) source.BatchStream {
	t.Helper()
	w, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "key"}}, []WindowExpression{{Function: WindowPercentRank, Alias: "key"}}, []memory.Field{{Name: "key", Type: memory.TypeFloat64}}, q, WindowSourceOptions{Mode: WindowExecutionResident})
	if err != nil {
		t.Fatal(err)
	}
	st, err := source.OpenBatchStream(ctx, w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	return st
}
func TestPercentRankResidentReusedDictionaryAndAlias(t *testing.T) {
	for _, sameDomain := range []bool{false, true} {
		t.Run(fmt.Sprint("same-domain=", sameDomain), func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, false)
			src := rankResidentReuseInput(t)
			src.sameDomain = sameDomain
			st := rankResidentOpen(t, ctx, q, src)
			out := memory.NewBatch()
			count := 0
			for {
				ok, err := st.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for row, id := range out.Int64s["id"].Values {
					if id != int64(count) {
						t.Fatal("original order", id, count)
					}
					want := []float64{2.0 / 3, 0, 1.0 / 3, 2.0 / 3}[count]
					if out.Float64s["key"].Values[row] != want || !out.Float64s["key"].IsValid(row) {
						t.Fatal("alias rank", id, out.Float64s["key"])
					}
					for _, name := range []string{"text", "bin"} {
						f, _ := src.Schema().Field(name)
						valid, err := batchValueForRankResident(out, f, row)
						if err != nil {
							t.Fatal(err)
						}
						if valid != (id != 2) {
							t.Fatal("payload validity", name, id)
						}
						if valid {
							var got []byte
							if v := out.DictStrings[name]; v != nil {
								got = v.DictionaryValue(v.Indices[row])
							} else {
								got = out.Strings[name].ValueBytes(row)
							}
							wantByte := byte('a' + id)
							if name == "bin" {
								wantByte = byte(128 + id)
							}
							if len(got) != 1 || got[0] != wantByte {
								t.Fatal("borrowed dictionary corruption", name, id, got)
							}
						}
					}
					count++
				}
			}
			if count != 4 {
				t.Fatal(count)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || src.opens != 1 || src.closes != 1 {
				t.Fatal(q.Used(), src.opens, src.closes)
			}
		})
	}
}

func batchValueForRankResident(b *memory.Batch, f memory.Field, row int) (bool, error) {
	if v := b.DictStrings[f.Name]; v != nil {
		return v.IsValid(row), nil
	}
	if v := b.Strings[f.Name]; v != nil {
		return v.IsValid(row), nil
	}
	return false, fmt.Errorf("missing %s", f.Name)
}

func TestPercentRankResidentActualBudgetAndFailureCleanup(t *testing.T) {
	for _, mode := range []string{"budget", "source", "cancel", "early-close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, false)
			src := rankResidentReuseInput(t)
			want := error(nil)
			switch mode {
			case "budget":
				src.huge = true
				want = memory.ErrBudgetExceeded
			case "source":
				src.failAt = 3
				want = rankSpoolSentinel
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				src.cancel = cancel
				want = context.Canceled
			}
			st := rankResidentOpen(t, ctx, q, src)
			if mode != "early-close" {
				_, err := st.Next(ctx, memory.NewBatch())
				if !errors.Is(err, want) {
					t.Fatalf("got %v want %v", err, want)
				}
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || src.closes != 1 {
				t.Fatal("leak", q.Used(), src.closes)
			}
		})
	}
}

func rankResidentAdmissionRun(t *testing.T, sameDomain bool, fail int) (int, error) {
	t.Helper()
	ctx := context.Background() // no QueryBudget override of injected manager
	manager := &rankSpoolFailManager{Manager: memory.NewBudgetManager(8 << 20), fail: fail}
	src := rankResidentReuseInput(t)
	src.sameDomain = sameDomain
	owner, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "key"}},
		[]WindowExpression{{Function: WindowPercentRank, Alias: "key"}},
		[]memory.Field{{Name: "key", Type: memory.TypeFloat64}}, manager,
		WindowSourceOptions{Mode: WindowExecutionResident})
	if err != nil {
		t.Fatal("factory must not perform execution admission", err)
	}
	stream, err := source.OpenBatchStream(ctx, owner,
		source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
	if err == nil {
		out := memory.NewBatch()
		count := 0
		for {
			ok, e := stream.Next(ctx, out)
			if e != nil {
				err = e
				break
			}
			if !ok {
				break
			}
			count += out.Rows
		}
		err = errors.Join(err, stream.Close(), stream.Close())
		if fail == 0 && count != 4 {
			t.Fatalf("fixture yielded %d rows, want 4", count)
		}
		concrete := stream.(*percentRankResidentStream)
		if concrete.owner != nil || concrete.child != nil || concrete.scratch != nil ||
			concrete.workspace != nil || concrete.output != nil || concrete.base != nil || concrete.results != nil {
			t.Fatal("closed reader retains charged graph")
		}
	}
	if manager.Used() != 0 {
		t.Fatalf("fail=%d retained %d bytes", fail, manager.Used())
	}
	if fail == 1 {
		if src.opens != 0 || src.closes != 0 {
			t.Fatal("base failure opened source", src.opens, src.closes)
		}
	} else if src.opens != 1 || src.closes != 1 {
		t.Fatal("single-open/close violated", fail, src.opens, src.closes)
	}
	return manager.calls, err
}

func TestPercentRankResidentEveryActualAdmission(t *testing.T) {
	for _, sameDomain := range []bool{false, true} {
		t.Run(fmt.Sprint("same-domain=", sameDomain), func(t *testing.T) {
			calls, err := rankResidentAdmissionRun(t, sameDomain, 0)
			if err != nil {
				t.Fatal(err)
			}
			if calls < 2 {
				t.Fatal("missing actual input/output admissions", calls)
			}
			for fail := 1; fail <= calls; fail++ {
				n, err := rankResidentAdmissionRun(t, sameDomain, fail)
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("admission %d/%d got %v", fail, calls, err)
				}
				if n < fail {
					t.Fatalf("fault %d not reached, calls=%d", fail, n)
				}
			}
			t.Logf("swept %d actual parent admissions, not logical workspace token requests", calls)
		})
	}
}
