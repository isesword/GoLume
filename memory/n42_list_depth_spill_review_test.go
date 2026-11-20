package memory

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"
)

// n42ReviewWire is an independent fixed v4 wire fixture, not produced by EncodeColumnarRun.
func n42ReviewWire(text bool) []byte {
	b := new(bytes.Buffer)
	b.Write([]byte{'G', 'L', 'C', 'R', 'U', 'N', '0', '1'})
	put := func(v any) {
		if e := binary.Write(b, binary.LittleEndian, v); e != nil {
			panic(e)
		}
	}
	put(uint16(4))
	put(uint64(3))
	put(uint32(1))
	for axis := 0; axis < 3; axis++ {
		put(uint32(0))
		put(uint32(3))
		put(uint32(4))
		for _, x := range []uint32{0, 1, 2, 3} {
			put(x)
		}
	}
	put(uint32(0))
	if text {
		put(uint32(4))
		for _, x := range []uint32{0, 1, 4, 4} {
			put(x)
		}
		put(uint64(4))
		b.Write([]byte("a界"))
	} else {
		for _, x := range []uint64{7, 8, 9} {
			put(x)
		}
	}
	return b.Bytes()
}
func TestN42ReviewEncodeFixedThreeAxis(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, v := n38ReviewTree(3, kind)
			b := NewBatch()
			b.Rows = 3
			b.Arrays["v"] = v
			m := NewBudgetManager(1 << 20)
			enc, e := EncodeColumnarRun(context.Background(), NewSchema(f), b, m)
			if e != nil {
				t.Fatal("canonical List3 encode rejected", kind, e)
			}
			if !bytes.Equal(enc.Payload, n42ReviewWire(kind == TypeString)) {
				t.Fatal("v4 fixed encoding differs", kind)
			}
			enc.Close()
			if m.Used() != 0 {
				t.Fatal("encode leak")
			}
		})
	}
}
func TestN42ReviewDecodeIndependentFixedThreeAxis(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, want := n38ReviewTree(3, kind)
			wire := n42ReviewWire(kind == TypeString)
			m := NewBudgetManager(1 << 20)
			dec, e := DecodeColumnarRun(context.Background(), NewSchema(f), &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: wire}}, m)
			if e != nil {
				t.Fatal("fixed v4 List3 decode rejected", kind, e)
			}
			for i := 0; i < 3; i++ {
				if !reflect.DeepEqual(n38ReviewValue(dec.Batch.Arrays["v"], i), n38ReviewValue(want, i)) {
					t.Fatal("decoded oracle", kind, i)
				}
			}
			dec.Close()
			if m.Used() != 0 {
				t.Fatal("decode leak")
			}
		})
	}
}

type n42ReviewManager struct {
	*BudgetManager
	calls  int
	cancel context.CancelFunc
}

func (m *n42ReviewManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	m.calls++
	r, e := m.BudgetManager.Reserve(ctx, n)
	if e == nil && m.cancel != nil {
		m.cancel()
	}
	return r, e
}
func TestN42ReviewDecodeBudgetsAndIndependentOwners(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, v := n38ReviewTree(3, kind)
			sc := NewSchema(f)
			wire := n42ReviewWire(kind == TypeString)
			meta, e := ColumnarRunDecodeMetadataBytes(sc)
			if e != nil {
				t.Fatal(e)
			}
			charge := int64(len(wire)) + meta
			for _, limit := range []int64{charge, charge - 1} {
				m := NewBudgetManager(limit)
				d, e := DecodeColumnarRun(context.Background(), sc, nestedSpillRestore(wire), m)
				if limit == charge {
					if e != nil {
						t.Fatal(e)
					}
					d.Close()
				} else {
					if !errors.Is(e, ErrBudgetExceeded) || d != nil {
						t.Fatal("exact-1", e)
					}
				}
				if m.Used() != 0 {
					t.Fatal("budget leak")
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			m := &n42ReviewManager{BudgetManager: NewBudgetManager(charge), cancel: cancel}
			d, e := DecodeColumnarRun(ctx, sc, nestedSpillRestore(wire), m)
			cancel()
			if !errors.Is(e, context.Canceled) || d != nil || m.calls == 0 || m.Used() != 0 {
				t.Fatal("post-reserve cancellation", e, m.calls, m.Used())
			}
			b := NewBatch()
			b.Rows = 3
			b.Arrays["v"] = v
			em := NewBudgetManager(1 << 20)
			enc, e := EncodeColumnarRun(context.Background(), sc, b, em)
			if e != nil {
				t.Fatal(e)
			}
			dm := NewBudgetManager(1 << 20)
			d, e = DecodeColumnarRun(context.Background(), sc, nestedSpillRestore(enc.Payload), dm)
			if e != nil {
				enc.Close()
				t.Fatal(e)
			}
			for i := range enc.Payload {
				enc.Payload[i] = 0
			}
			enc.Close()
			if em.Used() != 0 || dm.Used() == 0 {
				t.Fatal("owners not independent")
			}
			for i := 0; i < 3; i++ {
				if !reflect.DeepEqual(n38ReviewValue(d.Batch.Arrays["v"], i), n38ReviewValue(v, i)) {
					t.Fatal("decode aliases restored payload")
				}
			}
			d.Close()
			if dm.Used() != 0 {
				t.Fatal("decoded leak")
			}
		})
	}
}
func TestN42ReviewMalformedPreallocationAndWire(t *testing.T) {
	f, _ := n38ReviewTree(3, TypeInt64)
	for _, edit := range []func(*Field){func(f *Field) { f.Children = nil }, func(f *Field) { f.Children[0].Children[0].Children[0].Type = TypeTime64 }, func(f *Field) { f.Children[0].Encoding = EncodingDictionary }, func(f *Field) { f.Children[0].DictionaryDomain = "bad" }} {
		bad := cloneField(f)
		edit(&bad)
		m := &n42ReviewManager{BudgetManager: NewBudgetManager(1 << 20)}
		d, e := DecodeColumnarRun(context.Background(), NewSchema(bad), nestedSpillRestore(n42ReviewWire(false)), m)
		if e == nil || d != nil || m.calls != 0 || m.Used() != 0 {
			t.Fatal("bad schema not preallocation", e, m.calls)
		}
	}
	for axis := 0; axis < 3; axis++ {
		for _, delta := range []int{4, 12, 24} {
			wire := n42ReviewWire(false)
			binary.LittleEndian.PutUint32(wire[22+axis*28+delta:], 0xffffffff)
			m := NewBudgetManager(1 << 20)
			d, e := DecodeColumnarRun(context.Background(), NewSchema(f), nestedSpillRestore(wire), m)
			if !errors.Is(e, ErrSpillCorrupt) || d != nil || m.Used() != 0 {
				t.Fatal("corruption accepted/leak", axis, delta, e)
			}
		}
	}
	for _, cut := range []int{0, 9, 21, 22, 50, 100, 133} {
		wire := n42ReviewWire(false)[:cut]
		m := NewBudgetManager(1 << 20)
		d, e := DecodeColumnarRun(context.Background(), NewSchema(f), nestedSpillRestore(wire), m)
		if !errors.Is(e, ErrSpillCorrupt) || d != nil || m.Used() != 0 {
			t.Fatal("truncation", cut, e)
		}
	}
	// Required root NULL: insert one zero bitmap word into otherwise valid v4.
	f.Nullable = false
	wire := n42ReviewWire(false)
	masked := append([]byte(nil), wire[:22]...)
	masked = append(masked, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	masked = append(masked, wire[26:]...)
	m := NewBudgetManager(1 << 20)
	d, e := DecodeColumnarRun(context.Background(), NewSchema(f), nestedSpillRestore(masked), m)
	if !errors.Is(e, ErrSpillCorrupt) || d != nil || m.Used() != 0 {
		t.Fatal("required root corruption", e)
	}
}

type n42ReviewSyncFile struct {
	spillWriteFile
	err   error
	calls *int
}

func (f *n42ReviewSyncFile) Sync() error { *f.calls++; return f.err }
func TestN42ReviewV4ActualSyncFailureRollback(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(map[bool]string{false: "file-sync", true: "directory-sync"}[directory], func(t *testing.T) {
			f, v := n38ReviewTree(3, TypeString)
			sc := NewSchema(f)
			b := NewBatch()
			b.Rows = 3
			b.Arrays["v"] = v
			em := NewBudgetManager(1 << 20)
			enc, e := EncodeColumnarRun(context.Background(), sc, b, em)
			if e != nil {
				t.Fatal(e)
			}
			defer enc.Close()
			store, e := NewQuerySpillStore(SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			original := store.ops
			injected := errors.New("n42 injected sync")
			calls := 0
			if directory {
				store.ops.syncDir = func(string) error { calls++; return injected }
			} else {
				create := store.ops.createTemp
				store.ops.createTemp = func(dir, pattern string) (spillWriteFile, string, error) {
					f, p, e := create(dir, pattern)
					if e != nil {
						return nil, "", e
					}
					return &n42ReviewSyncFile{f, injected, &calls}, p, nil
				}
			}
			obj := SpillObject{Kind: SpillColumnarRun, Codec: ColumnarRunCodec, Payload: enc.Payload}
			if _, e = store.Spill(context.Background(), obj); !errors.Is(e, injected) || calls != 1 {
				t.Fatal("sync injection not reached", e, calls)
			}
			entries, e := os.ReadDir(store.Directory())
			if e != nil || len(entries) != 0 || store.Used() != 0 {
				t.Fatal("sync rollback", len(entries), store.Used(), e)
			}
			// Retry with normal fsync and restore the actual v4 payload under distinct owners.
			store.ops = original
			h, e := store.Spill(context.Background(), obj)
			if e != nil {
				t.Fatal(e)
			}
			enc.Close()
			if em.Used() != 0 {
				t.Fatal("encode retained")
			}
			rm := NewBudgetManager(1 << 20)
			restored, e := store.Restore(context.Background(), h, [32]byte{}, rm)
			if e != nil {
				t.Fatal(e)
			}
			dm := NewBudgetManager(1 << 20)
			decoded, e := DecodeColumnarRun(context.Background(), sc, restored, dm)
			if e != nil {
				restored.Close()
				t.Fatal(e)
			}
			restored.Close()
			if rm.Used() != 0 || dm.Used() == 0 || string(decoded.Batch.Arrays["v"].Child.Child.Child.Strings.Data) != "a界" {
				t.Fatal("restored/decoded owners")
			}
			decoded.Close()
			if dm.Used() != 0 {
				t.Fatal("decode leak")
			}
			if e = store.Delete(context.Background(), h); e != nil || store.Used() != 0 {
				t.Fatal("delete", e)
			}
		})
	}
}
