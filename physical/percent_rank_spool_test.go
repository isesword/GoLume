package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

type rankSpoolPoisonSource struct {
	source.Source
	opens, closes int
}
type rankSpoolPoisonStream struct {
	source.BatchStream
	owner  *rankSpoolPoisonSource
	data   [][]byte
	closed bool
}

func (s *rankSpoolPoisonSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens > 1 {
		return nil, fmt.Errorf("single open source")
	}
	ch, e := source.OpenBatchStream(ctx, s.Source, o)
	if e != nil {
		return nil, e
	}
	return &rankSpoolPoisonStream{BatchStream: ch, owner: s}, nil
}
func (s *rankSpoolPoisonStream) poison() {
	for _, b := range s.data {
		for i := range b {
			b[i] = 255
		}
	}
	s.data = nil
}
func (s *rankSpoolPoisonStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.poison()
	ok, e := s.BatchStream.Next(ctx, b)
	if ok {
		for _, v := range b.Strings {
			s.data = append(s.data, v.Data)
		}
		for _, v := range b.DictStrings {
			s.data = append(s.data, v.Data)
		}
	}
	return ok, e
}
func (s *rankSpoolPoisonStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.poison()
	s.owner.closes++
	return s.BatchStream.Close()
}
func rankSpoolStrings(values []string) *memory.StringViewVector {
	v := &memory.StringViewVector{Offsets: []uint32{0}}
	for _, x := range values {
		v.Data = append(v.Data, x...)
		v.Offsets = append(v.Offsets, uint32(len(v.Data)))
	}
	return v
}
func rankSpoolByteInput(t *testing.T, kind memory.DataType, dictionary bool) *rankSpoolPoisonSource {
	t.Helper()
	encoding := memory.EncodingPlain
	if dictionary {
		encoding = memory.EncodingDictionary
	}
	schema := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeString}, memory.Field{Name: "k", Type: kind, Encoding: encoding})
	keys := []string{"a", "a", "b", "b", "c", "d", "a", "a", "a", "b", "c"}
	var batches []*memory.Batch
	for start := 0; start < len(keys); start += 4 {
		end := min(start+4, len(keys))
		b := memory.NewBatch()
		b.Rows = end - start
		groups := make([]string, b.Rows)
		for i := range groups {
			groups[i] = "first"
			if start+i >= 6 {
				groups[i] = "second"
			}
		}
		b.Strings["g"] = rankSpoolStrings(groups)
		if dictionary {
			codes := make([]uint32, b.Rows)
			for i, k := range keys[start:end] {
				codes[i] = uint32(k[0] - 'a')
			}
			b.DictStrings["k"] = &memory.DictionaryStringVector{Indices: codes, Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte("abcd"), DictionaryDomain: fmt.Sprint(start), Encoding: memory.EncodingDictionary}
		} else {
			b.Strings["k"] = rankSpoolStrings(keys[start:end])
		}
		batches = append(batches, b)
	}
	src, e := inmemory.New(schema, batches...)
	if e != nil {
		t.Fatal(e)
	}
	return &rankSpoolPoisonSource{Source: src}
}
func rankSpoolOwner(src source.Source, m memory.Manager) *percentRankSource {
	fields := append([]memory.Field(nil), src.Schema().Fields...)
	fields = append(fields, memory.Field{Name: "ratio", Type: memory.TypeFloat64})
	return &percentRankSource{input: src, original: src.Schema(), schema: memory.NewSchema(fields...), partition: []string{"g"}, order: []SortKey{{Column: "k"}}, hidden: "ratio", manager: m, metrics: new(percentRankCounters)}
}
func TestPercentRankSpoolByteKeysAcrossPages(t *testing.T) {
	for _, kind := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		for _, dict := range []bool{false, true} {
			for _, disk := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/dict%v/disk%v", kind, dict, disk), func(t *testing.T) {
					ctx, q := percentRankReviewQuery(t, 8<<20, disk)
					src := rankSpoolByteInput(t, kind, dict)
					st, e := newPercentRankPartitionStream(ctx, rankSpoolOwner(src, q), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
					if e != nil {
						t.Fatal(e)
					}
					b := memory.NewBatch()
					want := []float64{0, 0, 0.4, 0.4, 0.8, 1, 0, 0, 0, 0.75, 1}
					i := 0
					for {
						ok, e := st.Next(ctx, b)
						if e != nil {
							t.Fatal(e)
						}
						if !ok {
							break
						}
						for _, v := range b.Float64s["ratio"].Values {
							if i >= len(want) || v != want[i] {
								t.Fatal(i, v, want)
							}
							i++
						}
					}
					if i != len(want) {
						t.Fatal(i)
					}
					if e = st.Close(); e != nil {
						t.Fatal(e)
					}
					if src.opens != 1 || src.closes != 1 || q.Used() != 0 {
						t.Fatal(src.opens, src.closes, q.Used())
					}
					if disk && st.(*percentRankPartitionStream).StreamDiagnostics()["partition_spool_pages"] == "0" {
						t.Fatal("missing actual spool")
					}
				})
			}
		}
	}
}

type rankSpoolFailManager struct {
	memory.Manager
	calls, fail int
}

func (m *rankSpoolFailManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func rankSpoolFaultRun(t *testing.T, fail int) (int, error) {
	t.Helper()
	m := &rankSpoolFailManager{Manager: memory.NewBudgetManager(8 << 20), fail: fail}
	src := rankSpoolByteInput(t, memory.TypeString, false)
	st, e := newPercentRankPartitionStream(context.Background(), rankSpoolOwner(src, m), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
	if e == nil {
		b := memory.NewBatch()
		for {
			ok, err := st.Next(context.Background(), b)
			if err != nil {
				e = err
				break
			}
			if !ok {
				break
			}
		}
		e = errors.Join(e, st.Close())
	}
	if m.Used() != 0 {
		t.Fatalf("fail%d leaked%d", fail, m.Used())
	}
	return m.calls, e
}
func TestPercentRankSpoolEveryMemoryAdmission(t *testing.T) {
	calls, e := rankSpoolFaultRun(t, 0)
	if e != nil {
		t.Fatal(e)
	}
	for fail := 1; fail <= calls; fail++ {
		_, e = rankSpoolFaultRun(t, fail)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(fail, e)
		}
	}
	t.Logf("swept %d actual allocation admissions", calls)
}
func TestPercentRankSpoolCancellationCloseAndFinite(t *testing.T) {
	for _, nilctx := range []bool{false, true} {
		m := memory.NewBudgetManager(8 << 20)
		src := rankSpoolByteInput(t, memory.TypeString, false)
		st, e := newPercentRankPartitionStream(context.Background(), rankSpoolOwner(src, m), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		if ok, e := st.Next(context.Background(), b); e != nil || !ok {
			t.Fatal(ok, e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if nilctx {
			ctx = nil
		}
		if _, e = st.Next(ctx, b); e == nil {
			t.Fatal("missing cancellation/invalid context")
		}
		_ = st.Close()
		concrete := st.(*percentRankPartitionStream)
		if m.Used() != 0 || src.closes != 1 || concrete.owner != nil || concrete.partitionKeys != nil || concrete.manager != nil {
			t.Fatal("retained owner after Close", m.Used())
		}
	}
	src := rankSpoolByteInput(t, memory.TypeString, false)
	if _, e := newPercentRankPartitionStream(context.Background(), rankSpoolOwner(src, memory.NewBudgetManager(0)), source.ReadOptions{Kernels: offsetTestKernels()}); e == nil {
		t.Fatal("unlimited streaming accepted")
	}
}

var rankSpoolSentinel = errors.New("spool injected IO")

type rankSpoolFaultStore struct {
	memory.SpillManager
	mode   string
	failed bool
}

func (s *rankSpoolFaultStore) Spill(ctx context.Context, o memory.SpillObject) (memory.SpillHandle, error) {
	if s.mode == "write" && !s.failed {
		s.failed = true
		return nil, rankSpoolSentinel
	}
	return s.SpillManager.Spill(ctx, o)
}
func (s *rankSpoolFaultStore) SpillWithMetadata(ctx context.Context, o memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	if s.mode == "write" && !s.failed {
		s.failed = true
		return nil, rankSpoolSentinel
	}
	if meta, ok := s.SpillManager.(memory.MetadataSpillManager); ok {
		return meta.SpillWithMetadata(ctx, o, m)
	}
	return s.SpillManager.Spill(ctx, o)
}
func (s *rankSpoolFaultStore) Restore(ctx context.Context, h memory.SpillHandle, f [32]byte, m memory.Manager) (*memory.RestoredSpill, error) {
	if s.mode == "read" && !s.failed {
		s.failed = true
		return nil, rankSpoolSentinel
	}
	r, e := s.SpillManager.Restore(ctx, h, f, m)
	if e == nil && s.mode == "corrupt" && !s.failed {
		s.failed = true
		r.Object.Payload[0] ^= 255
	}
	return r, e
}
func (s *rankSpoolFaultStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.mode == "delete" && !s.failed {
		s.failed = true
		return rankSpoolSentinel
	}
	return s.SpillManager.Delete(ctx, h)
}
func TestPercentRankSpoolIOFailuresAndCorruption(t *testing.T) {
	for _, mode := range []string{"write", "read", "corrupt", "delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			src := rankSpoolByteInput(t, memory.TypeString, false)
			stream, e := newPercentRankPartitionStream(ctx, rankSpoolOwner(src, q), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 4})
			if e != nil {
				t.Fatal(e)
			}
			s := stream.(*percentRankPartitionStream)
			_ = s.store.Close()
			base, e := q.SpillManager()
			if e != nil {
				t.Fatal(e)
			}
			fault := &rankSpoolFaultStore{SpillManager: base, mode: mode}
			s.store = spillmeta.NewStore(fault, s.metadata)
			b := memory.NewBatch()
			_, e = s.Next(ctx, b)
			if mode == "corrupt" {
				if !errors.Is(e, memory.ErrSpillCorrupt) {
					t.Fatal(e)
				}
			} else if !errors.Is(e, rankSpoolSentinel) {
				t.Fatal(e)
			}
			_ = s.Close()
			if e = q.Close(); e != nil {
				t.Fatal(e)
			}
			if q.Used() != 0 || src.closes != 1 {
				t.Fatal(q.Used(), src.closes)
			}
		})
	}
}

func rankSpoolFlatProfile(t *testing.T) (memory.Schema, *memory.Batch) {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 5
	fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}}
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4}}
	valid := memory.Bitmap{27} // row2 is NULL for every payload field
	add := func(name string, typ memory.DataType) {
		fields = append(fields, memory.Field{Name: name, Type: typ, Nullable: true})
	}
	add("i8", memory.TypeInt8)
	b.Int8s["i8"] = &memory.Int8Vector{Values: []int8{-2, -1, 0, 1, 2}, Validity: valid}
	add("i16", memory.TypeInt16)
	b.Int16s["i16"] = &memory.Int16Vector{Values: []int16{1000, 1001, 1002, 1003, 1004}, Validity: valid}
	add("i32", memory.TypeInt32)
	b.Int32s["i32"] = &memory.Int32Vector{Values: []int32{100000, 100001, 100002, 100003, 100004}, Validity: valid}
	add("i64", memory.TypeInt64)
	b.Int64s["i64"] = &memory.Int64Vector{Values: []int64{1 << 40, 1<<40 + 1, 1<<40 + 2, 1<<40 + 3, 1<<40 + 4}, Validity: valid}
	add("u8", memory.TypeUint8)
	b.Uint8s["u8"] = &memory.Uint8Vector{Values: []uint8{0, 1, 2, 3, 4}, Validity: valid}
	add("u16", memory.TypeUint16)
	b.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{256, 257, 258, 259, 260}, Validity: valid}
	add("u32", memory.TypeUint32)
	b.Uint32s["u32"] = &memory.Uint32Vector{Values: []uint32{70000, 70001, 70002, 70003, 70004}, Validity: valid}
	add("u64", memory.TypeUint64)
	b.Uint64s["u64"] = &memory.Uint64Vector{Values: []uint64{1 << 50, 1<<50 + 1, 1<<50 + 2, 1<<50 + 3, 1<<50 + 4}, Validity: valid}
	add("f32", memory.TypeFloat32)
	b.Float32s["f32"] = &memory.Float32Vector{Values: []float32{0.25, 1.25, 2.25, 3.25, 4.25}, Validity: valid}
	add("f64", memory.TypeFloat64)
	b.Float64s["f64"] = &memory.Float64Vector{Values: []float64{0.5, 1.5, 2.5, 3.5, 4.5}, Validity: valid}
	add("bool", memory.TypeBool)
	b.Uint32s["bool"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0, 1, 0}, Validity: valid}
	for name, typ := range map[string]memory.DataType{"date": memory.TypeDate32, "time32": memory.TypeTime32} {
		add(name, typ)
		b.Int32s[name] = &memory.Int32Vector{Values: []int32{0, 1, 2, 3, 4}, Validity: valid}
	}
	for name, typ := range map[string]memory.DataType{"date64": memory.TypeDate64, "timestamp": memory.TypeTimestamp, "time64": memory.TypeTime64, "duration": memory.TypeDuration} {
		add(name, typ)
		b.Int64s[name] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4}, Validity: valid}
	}
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBinary} {
		name := typ.String()
		add(name, typ)
		b.Strings[name] = rankSpoolStrings([]string{"a", "bb", "ccc", "dddd", "eeeee"})
		b.Strings[name].Validity = valid
		name += "_dict"
		fields = append(fields, memory.Field{Name: name, Type: typ, Encoding: memory.EncodingDictionary, Nullable: true})
		b.DictStrings[name] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2, 3, 4}, Offsets: []uint32{0, 1, 3, 6, 10, 15}, Data: []byte("abbcccddddeeeee"), Validity: valid, Encoding: memory.EncodingDictionary, DictionaryDomain: name}
	}
	return memory.NewSchema(fields...), b
}
func TestPercentRankSpoolFlatProfileThreePaths(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
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
			win, e := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowPercentRank, Alias: "pr"}}, []memory.Field{{Name: "pr", Type: memory.TypeFloat64}}, q, WindowSourceOptions{Mode: mode})
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
					if out.Float64s["pr"].Values[row] != float64(id)/4 {
						t.Fatal("rank", id)
					}
					for col, f := range schema.Fields {
						if !arrow.TypeEqual(rec.Schema().Field(col).Type, oracle.Schema().Field(col).Type) {
							t.Fatal("type", f.Name)
						}
						actual, want := rec.Column(col).GetOneForMarshal(row), oracle.Column(col).GetOneForMarshal(int(id))
						if !reflect.DeepEqual(actual, want) {
							t.Fatalf("%s row%d got%v want%v", f.Name, id, actual, want)
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

func TestPercentRankSpoolTimestampTypeSemanticEquality(t *testing.T) {
	cold := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	warm := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	if _, err := warm.GetZone(); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(cold, warm) {
		t.Fatal("fixture must exercise distinct private cache state")
	}
	if !arrow.TypeEqual(cold, warm) {
		t.Fatal("same timestamp semantics differ")
	}
	for _, other := range []*arrow.TimestampType{{Unit: arrow.Millisecond, TimeZone: "UTC"}, {Unit: arrow.Nanosecond, TimeZone: "+01:00"}} {
		if arrow.TypeEqual(cold, other) {
			t.Fatal("different timestamp semantics compare equal")
		}
	}
}
