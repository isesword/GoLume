package engine_test

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const item25Rows = 1031

var item25Names = []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64"}
var item25Types = []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64}
var item25F32 = []uint32{0, 0x80000000, 1, 0x807fffff, 0x3fa00000, 0x7f800000, 0xff800000, 0x7fc12345, 0xffc54321, 0x7f812345}
var item25F64 = []uint64{0, 0x8000000000000000, 1, 0x800fffffffffffff, 0x3ff4000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8123456789abc, 0xfff8abcdef012345, 0x7ff0123456789abc}
var item25Words = []string{"", "alpha", "汉🙂", "x\x00y", "repeat"}

func item25Valid(id, c int) bool { return (id+3*c)%17 != 0 && id%65 != (63+c)%65 }
func item25Value(id, c int) any {
	switch c {
	case 0:
		return int8(uint8(id * 37))
	case 1:
		return int16(uint16(id * 911))
	case 2:
		return int32(uint32(id) * 2147483647)
	case 3:
		return int64(uint64(id) * 0x8000000000000001)
	case 4:
		return uint8(id * 71)
	case 5:
		return uint16(id * 991)
	case 6:
		return uint32(id) * 0x80000001
	case 7:
		return uint64(id) * 0x8000000000000001
	case 8:
		return math.Float32frombits(item25F32[id%len(item25F32)])
	default:
		return math.Float64frombits(item25F64[id%len(item25F64)])
	}
}
func item25Fixture(t *testing.T) *inmemory.Source {
	t.Helper()
	fields := []memory.Field{{Name: "key", Type: memory.TypeInt64}, {Name: "id", Type: memory.TypeInt64}}
	for i, n := range item25Names {
		fields = append(fields, memory.Field{Name: n, Type: item25Types[i], Nullable: true})
	}
	fields = append(fields, memory.Field{Name: "dict", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "item25-stable-dictionary"})
	offsets := []uint32{0}
	var data []byte
	for _, s := range item25Words {
		data = append(data, s...)
		offsets = append(offsets, uint32(len(data)))
	}
	var batches []*memory.Batch
	for start := 0; start < item25Rows; start += 65 {
		n := min(65, item25Rows-start)
		b := memory.NewBatch()
		keys, ids := make([]int64, n), make([]int64, n)
		valid := make([]memory.Bitmap, 11)
		for c := range valid {
			valid[c] = memory.EnsureBitmap(nil, n)
		}
		i8 := make([]int8, n)
		i16 := make([]int16, n)
		i32 := make([]int32, n)
		i64 := make([]int64, n)
		u8 := make([]uint8, n)
		u16 := make([]uint16, n)
		u32 := make([]uint32, n)
		u64 := make([]uint64, n)
		f32 := make([]float32, n)
		f64 := make([]float64, n)
		codes := make([]uint32, n)
		for j := 0; j < n; j++ {
			id := start + j
			keys[j], ids[j] = int64(id*7%11), int64(id)
			for c := range valid {
				if item25Valid(id, c) {
					memory.Set(valid[c], j)
				}
			}
			i8[j] = item25Value(id, 0).(int8)
			i16[j] = item25Value(id, 1).(int16)
			i32[j] = item25Value(id, 2).(int32)
			i64[j] = item25Value(id, 3).(int64)
			u8[j] = item25Value(id, 4).(uint8)
			u16[j] = item25Value(id, 5).(uint16)
			u32[j] = item25Value(id, 6).(uint32)
			u64[j] = item25Value(id, 7).(uint64)
			f32[j] = item25Value(id, 8).(float32)
			f64[j] = item25Value(id, 9).(float64)
			codes[j] = uint32(id % len(item25Words))
		}
		b.Int64s["key"] = &memory.Int64Vector{Values: keys}
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Int8s["i8"] = &memory.Int8Vector{Values: i8, Validity: valid[0]}
		b.Int16s["i16"] = &memory.Int16Vector{Values: i16, Validity: valid[1]}
		b.Int32s["i32"] = &memory.Int32Vector{Values: i32, Validity: valid[2]}
		b.Int64s["i64"] = &memory.Int64Vector{Values: i64, Validity: valid[3]}
		b.Uint8s["u8"] = &memory.Uint8Vector{Values: u8, Validity: valid[4]}
		b.Uint16s["u16"] = &memory.Uint16Vector{Values: u16, Validity: valid[5]}
		b.Uint32s["u32"] = &memory.Uint32Vector{Values: u32, Validity: valid[6]}
		b.Uint64s["u64"] = &memory.Uint64Vector{Values: u64, Validity: valid[7]}
		b.Float32s["f32"] = &memory.Float32Vector{Values: f32, Validity: valid[8]}
		b.Float64s["f64"] = &memory.Float64Vector{Values: f64, Validity: valid[9]}
		b.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: codes, Offsets: offsets, Data: data, Validity: valid[10], Encoding: memory.EncodingDictionary, DictionaryDomain: "item25-stable-dictionary"}
		b.Reset(n)
		if b.Strings["dict"] != nil || b.DictStrings["dict"] == nil || len(b.DictStrings["dict"].Indices) != n {
			t.Fatal("not actual dictionary representation")
		}
		batches = append(batches, b)
	}
	s, e := inmemory.New(memory.NewSchema(fields...), batches...)
	if e != nil {
		t.Fatal(e)
	}
	s.WithDictionary("dict", source.Dictionary{Domain: "item25-stable-dictionary", Offsets: offsets, Data: data})
	return s
}

// Conservative pin reservation for the actual borrowed source batch. Immutable
// fixture backing already exists outside the query and is not an RSS claim.
type item25PinnedSource struct {
	input          *inmemory.Source
	opened, closed int
	peakPin        int64
}

func (s *item25PinnedSource) Schema() memory.Schema { return s.input.Schema() }
func (s *item25PinnedSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}
func (s *item25PinnedSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}
func (s *item25PinnedSource) Dictionary(ctx context.Context, name string) (source.Dictionary, error) {
	return s.input.Dictionary(ctx, name)
}
func (s *item25PinnedSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	in, e := s.input.NewBatchStream(ctx, o)
	if e != nil {
		return nil, e
	}
	if o.MemoryManager == nil {
		in.Close()
		return nil, fmt.Errorf("source missing query manager")
	}
	s.opened++
	return &item25PinnedStream{s: s, in: in, m: o.MemoryManager}, nil
}
func (s *item25PinnedSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	in, e := s.input.NewBatchStreamWithPartitions(ctx, o, parts)
	if e != nil {
		return nil, e
	}
	if o.MemoryManager == nil {
		in.Close()
		return nil, fmt.Errorf("source missing query manager")
	}
	s.opened++
	return &item25PinnedStream{s: s, in: in, m: o.MemoryManager}, nil
}

type item25PinnedStream struct {
	s      *item25PinnedSource
	in     source.BatchStream
	m      memory.Manager
	lease  memory.Reservation
	closed bool
}

func (s *item25PinnedStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
	ok, e := s.in.Next(ctx, b)
	if e != nil || !ok {
		return ok, e
	}
	bytes := memory.BatchBytes(b)
	s.lease, e = s.m.Reserve(ctx, bytes)
	if e != nil {
		b.ClearColumns()
		return false, e
	}
	s.s.peakPin = max(s.s.peakPin, bytes)
	return true, nil
}
func (s *item25PinnedStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	e := s.in.Close()
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
	s.s.closed++
	return e
}
func item25Permutation() []int {
	out := make([]int, 0, item25Rows)
	for key := 0; key < 11; key++ {
		for id := 0; id < item25Rows; id++ {
			if id*7%11 == key {
				out = append(out, id)
			}
		}
	}
	return out
}
func item25Check(t *testing.T, v lazy.BatchView, want []int, offset int) {
	t.Helper()
	schema := v.Schema()
	if len(schema.Fields) != 13 {
		t.Fatalf("columns=%d", len(schema.Fields))
	}
	names := append([]string{"key", "id"}, item25Names...)
	names = append(names, "dict")
	for c, name := range names {
		col, e := v.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		values, e := col.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		if len(values) != int(v.Len()) {
			t.Fatal("column length")
		}
		f, ok := schema.Field(name)
		if !ok {
			t.Fatal(name)
		}
		expectedType := memory.TypeInt64
		if c >= 2 && c < 12 {
			expectedType = item25Types[c-2]
		}
		if c == 12 {
			expectedType = memory.TypeString
		}
		if f.Type != expectedType || col.Type() != expectedType || f.Nullable != (c >= 2) {
			t.Fatalf("field %s=%+v", name, f)
		}
		if c == 12 && (f.Encoding != memory.EncodingDictionary || f.DictionaryDomain != "") {
			t.Fatalf("sort must retain dict encoding but clear remapped domain: %+v", f)
		}
		var nulls int64
		for row, got := range values {
			if offset+row >= len(want) {
				t.Fatal("excess rows")
			}
			id := want[offset+row]
			valid := c < 2 || item25Valid(id, c-2)
			if !valid {
				nulls++
				if got != nil {
					t.Fatalf("NULL id=%d col=%s got=%v", id, name, got)
				}
				continue
			}
			var expected any
			switch {
			case c == 0:
				expected = int64(id * 7 % 11)
			case c == 1:
				expected = int64(id)
			case c == 12:
				expected = item25Words[id%len(item25Words)]
			default:
				expected = item25Value(id, c-2)
			}
			if c == 10 {
				g, ok := got.(float32)
				if !ok || math.Float32bits(g) != math.Float32bits(expected.(float32)) {
					t.Fatalf("F32 id%d got%T:%v wantbits%08x", id, got, got, math.Float32bits(expected.(float32)))
				}
			} else if c == 11 {
				g, ok := got.(float64)
				if !ok || math.Float64bits(g) != math.Float64bits(expected.(float64)) {
					t.Fatalf("F64 id%d got%T:%v wantbits%016x", id, got, got, math.Float64bits(expected.(float64)))
				}
			} else if !reflect.DeepEqual(got, expected) {
				t.Fatalf("id%d col%s got%T:%v want%T:%v", id, name, got, got, expected, expected)
			}
		}
		if col.NullCount() != nulls {
			t.Fatalf("nullcount %s=%d want%d", name, col.NullCount(), nulls)
		}
	}
}
func item25Files(t *testing.T, dir string) (int, int64) {
	t.Helper()
	n := 0
	var size int64
	e := filepath.WalkDir(dir, func(_ string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			i, e := d.Info()
			if e != nil {
				return e
			}
			n++
			size += i.Size()
		}
		return nil
	})
	if e != nil && !os.IsNotExist(e) {
		t.Fatal(e)
	}
	return n, size
}
func TestItem25SortPayloadIntegrity(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, cancelRun := range []bool{false, true} {
			t.Run(fmt.Sprintf("external%v/cancel%v", external, cancelRun), func(t *testing.T) {
				dir := t.TempDir()
				o := engine.DefaultOptions()
				o.Workers = 1
				o.PreparedCacheBytes = 0
				o.MemoryLimitBytes = 1 << 20
				o.ExecutionPolicy = engine.ExecutionMemoryOnly
				if external {
					o.ExecutionPolicy = engine.ExecutionBounded
					o.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 128 << 20}
				}
				o.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 16 << 10, MaxMorselBytes: 128 << 10, MaxInFlightBytes: 512 << 10, MinMorselRows: 1, MaxMorselRows: 63}
				session := lazy.NewSession(o)
				defer session.Close()
				src := &item25PinnedSource{input: item25Fixture(t)}
				frame := session.FromSource(src).Sort(lazy.Asc("key"))
				plan, e := frame.ExplainPhysical(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				t.Log(plan)
				wantStrategy := "resident-stable"
				if external {
					wantStrategy = "external-stable"
				}
				if !strings.Contains(plan, wantStrategy) {
					t.Fatalf("strategy missing %s", wantStrategy)
				}
				q, e := session.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				defer q.Close()
				ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), q))
				defer cancel()
				p, e := frame.Prepare(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer p.Close()
				r, e := p.ExecuteBatches(ctx, lazy.BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 16 << 10})
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				want := item25Permutation()
				count, pages := 0, 0
				live := false
				var previous lazy.BatchView
				for r.Next(ctx) {
					if previous.Valid() {
						t.Fatal("previous borrowed view not expired")
					}
					v := r.Batch()
					item25Check(t, v, want, count)
					count += int(v.Len())
					pages++
					previous = v
					if external {
						store, err := q.SpillManager()
						if err != nil {
							t.Fatal(err)
						}
						disk, ok := store.(*memory.QuerySpillStore)
						if !ok {
							t.Fatalf("store=%T", store)
						}
						n, bytes := item25Files(t, disk.Directory())
						if n > 0 && bytes > 0 && disk.Used() > 0 {
							live = true
						}
					}
					if cancelRun {
						cancel()
						break
					}
				}
				if cancelRun {
					if count == 0 {
						t.Fatal("cancel before output")
					}
					if r.Next(ctx) {
						t.Fatal("published after cancel")
					}
					if !errors.Is(r.Err(), context.Canceled) {
						t.Fatalf("cancel=%v", r.Err())
					}
				} else {
					if e = r.Err(); e != nil {
						t.Fatal(e)
					}
					if count != item25Rows || pages < 1 || (external && pages < 2) {
						t.Fatalf("rows=%d pages=%d", count, pages)
					}
				}
				if e = r.Close(); e != nil && !errors.Is(e, context.Canceled) {
					t.Fatal(e)
				}
				if previous.Valid() {
					t.Fatal("closed view not expired")
				}
				if e = p.Close(); e != nil {
					t.Fatal(e)
				}
				snap := q.Snapshot()
				n, bytes := item25Files(t, dir)
				if snap.UsedBytes != 0 || n != 0 || bytes != 0 || src.opened != src.closed || src.peakPin <= 0 {
					t.Fatalf("cleanup snap=%+v files=%d bytes=%d source=%d/%d pin=%d", snap, n, bytes, src.opened, src.closed, src.peakPin)
				}
				if external {
					if snap.SpilledBytes <= 0 || !live {
						t.Fatalf("actual spill missing written=%d live=%v", snap.SpilledBytes, live)
					}
				} else if snap.SpilledBytes != 0 {
					t.Fatal("resident spilled")
				}
				t.Logf("rows=%d pages=%d peak=%d sourcePinMax=%d written=%d live=%v used=%d", count, pages, snap.PeakBytes, src.peakPin, snap.SpilledBytes, live, snap.UsedBytes)
				if e = q.Close(); e != nil {
					t.Fatal(e)
				}
				if session.Engine().Memory().Used() != 0 {
					t.Fatal("engine leak")
				}
			})
		}
	}
}
