package memory

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func nestedSpillRestore(p []byte) *RestoredSpill {
	return &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: p}}
}
func nestedSpillCheckRaw(t *testing.T, b *Batch, depth int) {
	t.Helper()
	cur := b
	if depth > 0 {
		v := b.Structs["root"]
		for d := 0; d < depth; d++ {
			if v.Rows != 7 {
				t.Fatal("outer rows")
			}
			if d == 0 && !reflect.DeepEqual(v.Validity, Bitmap{95}) {
				t.Fatal("raw ancestor mask")
			}
			cur = v.Values
			if d+1 < depth {
				v = cur.Structs["inner"]
			}
		}
	}
	for _, tc := range []struct {
		v *ArrayNode
		k DataType
	}{{cur.Arrays["nested"], TypeInt64}, {b.Arrays["other"], TypeString}} {
		_, want := nestedListTestFixture(tc.k)
		if !reflect.DeepEqual(tc.v.Offsets, want.Offsets) || !reflect.DeepEqual(tc.v.Validity, want.Validity) || !reflect.DeepEqual(tc.v.Child.Offsets, want.Child.Offsets) || !reflect.DeepEqual(tc.v.Child.Validity, want.Child.Validity) {
			t.Fatal("raw list axes")
		}
		if tc.k == TypeInt64 {
			if !reflect.DeepEqual(tc.v.Child.Child.Int64.Values, want.Child.Child.Int64.Values) || !reflect.DeepEqual(tc.v.Child.Child.Int64.Validity, want.Child.Child.Int64.Validity) {
				t.Fatal("raw int")
			}
		} else {
			a, z := tc.v.Child.Child.Strings, want.Child.Child.Strings
			if !reflect.DeepEqual(a.Offsets, z.Offsets) || !bytes.Equal(a.Data, z.Data) || !reflect.DeepEqual(a.Validity, z.Validity) {
				t.Fatal("raw String")
			}
		}
	}
}
func TestNestedListSpillRoundtripAndBudgets(t *testing.T) {
	for _, depth := range []int{0, 1, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := nestedBatchFixture(t, depth)
			m := NewBudgetManager(0)
			enc, e := EncodeColumnarRun(context.Background(), s, b, m)
			if e != nil {
				t.Fatal(e)
			}
			if binary.LittleEndian.Uint16(enc.Payload[8:10]) != 4 {
				t.Fatal("newshape version")
			}
			p := append([]byte(nil), enc.Payload...)
			fee := m.Used()
			enc.Close()
			if m.Used() != 0 {
				t.Fatal("encode close")
			}
			for _, limit := range []int64{fee, fee - 1} {
				mm := NewBudgetManager(limit)
				v, e := EncodeColumnarRun(context.Background(), s, b, mm)
				if limit == fee {
					if e != nil {
						t.Fatal(e)
					}
					v.Close()
				} else if e == nil || v != nil {
					t.Fatal("encode exact-1")
				}
				if mm.Used() != 0 {
					t.Fatal("encode leak")
				}
			}
			meta, e := ColumnarRunDecodeMetadataBytes(s)
			if e != nil {
				t.Fatal(e)
			}
			charge := int64(len(p)) + meta
			for _, limit := range []int64{charge, charge - 1} {
				mm := NewBudgetManager(limit)
				v, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p), mm)
				if limit == charge {
					if e != nil {
						t.Fatal(e)
					}
					nestedSpillCheckRaw(t, v.Batch, depth)
					borrow := v.Batch.Arrays
					v.Close()
					if len(borrow) != 0 {
						t.Fatal("Close did not clear bindings")
					}
				} else if !errors.Is(e, ErrBudgetExceeded) || v != nil {
					t.Fatal("decode exact-1", e)
				}
				if mm.Used() != 0 {
					t.Fatal("decode leak")
				}
			}
			bound, e := EstimateColumnarRunRangeBytes(s, b, 0, 7)
			if e != nil || bound < int64(len(p)) {
				t.Fatal("full raw bound", bound, len(p), e)
			}
			out, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p), m)
			if e != nil {
				t.Fatal(e)
			}
			for i := range p {
				p[i] = 0
			}
			b.Arrays["other"].Child.Child.Strings.Data[0] = '!'
			nestedSpillCheckRaw(t, out.Batch, depth)
			out.Close()
			if m.Used() != 0 {
				t.Fatal("poison ownership")
			}
		})
	}
}
func TestNestedListSpillIndependentGoldenAndCorruption(t *testing.T) {
	f, v := nestedListTestFixture(TypeInt64)
	s, e := NewSchemaWithNestedListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 7
	b.Arrays[f.Name] = v
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	p := append([]byte(nil), enc.Payload...)
	enc.Close()
	// Independent v4 bytes: no production writer/size helper is used.
	var golden bytes.Buffer
	golden.WriteString("GLCRUN01")
	put := func(x any) {
		if e := binary.Write(&golden, binary.LittleEndian, x); e != nil {
			t.Fatal(e)
		}
	}
	put(uint16(4))
	put(uint64(7))
	put(uint32(1))
	put(uint32(1))
	put(uint64(126))
	put(uint32(7))
	put(uint32(8))
	for _, x := range []uint32{0, 1, 1, 2, 3, 5, 6, 7} {
		put(x)
	}
	put(uint32(1))
	put(uint64(125))
	put(uint32(8))
	put(uint32(8))
	for _, x := range []uint32{0, 1, 2, 2, 5, 6, 7, 8} {
		put(x)
	}
	put(uint32(1))
	put(uint64(247))
	for _, x := range []int64{91, 92, -9223372036854775808, 93, 9223372036854775807, 0, 94, -7} {
		put(x)
	}
	if !bytes.Equal(p, golden.Bytes()) {
		t.Fatal("v4 golden wire mismatch")
	}
	// Positions derive from the fixed independently written fixture, not runtime parsing.
	cases := map[string]func([]byte) []byte{
		"version2": func(p []byte) []byte { binary.LittleEndian.PutUint16(p[8:], 2); return p }, "version3": func(p []byte) []byte { binary.LittleEndian.PutUint16(p[8:], 3); return p },
		"outer_child_count": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[34:], 8); return p }, "outer_offsets_count": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[38:], 9); return p }, "outer_base": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[42:], 1); return p },
		"inner_child_count": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[86:], 9); return p }, "inner_offsets_count": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[90:], 9); return p }, "inner_decrease": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[98:], 99); return p }, "leaf_bitmap_count": func(p []byte) []byte { binary.LittleEndian.PutUint32(p[126:], 99); return p },
		"huge_outer_children_short_payload": func(p []byte) []byte {
			binary.LittleEndian.PutUint32(p[34:], ^uint32(0))
			binary.LittleEndian.PutUint32(p[70:], ^uint32(0))
			return p
		}, "huge_terminal_short_payload": func(p []byte) []byte {
			binary.LittleEndian.PutUint32(p[86:], ^uint32(0))
			binary.LittleEndian.PutUint32(p[122:], ^uint32(0))
			return p
		}, "trailing": func(p []byte) []byte { return append(p, 1) },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			data := mut(append([]byte(nil), p...))
			mm := NewBudgetManager(0)
			out, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(data), mm)
			if e == nil || out != nil || mm.Used() != 0 {
				t.Fatal("corruption admitted/leaked", e)
			}
		})
	}
	for end := 0; end < len(p); end++ {
		mm := NewBudgetManager(0)
		out, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p[:end]), mm)
		if e == nil || out != nil || mm.Used() != 0 {
			t.Fatal("truncation", end, e)
		}
	}
}
func TestNestedListSpillSelectedAndRequired(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, v := nestedListTestFixture(kind)
			s, e := NewSchemaWithNestedListsChecked(f)
			if e != nil {
				t.Fatal(e)
			}
			b := NewBatch()
			b.Rows = 7
			b.Arrays[f.Name] = v
			for _, rows := range [][]int{{6, 4, 2, 4, 0}, {}, {1, 3}} {
				bound, e := EstimateColumnarRunSelectionBytes(s, b, rows)
				if e != nil {
					t.Fatal(e)
				}
				m := NewBudgetManager(0)
				owned, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, rows)
				if e != nil {
					t.Fatal(e)
				}
				selected := NewBatch()
				selected.Rows = len(rows)
				selected.Arrays[f.Name] = owned.Vector
				enc, e := EncodeColumnarRun(context.Background(), s, selected, m)
				if e != nil {
					t.Fatal(e)
				}
				if bound < int64(len(enc.Payload)) {
					t.Fatal("selected estimate underflow", bound, len(enc.Payload))
				}
				out, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(enc.Payload), m)
				if e != nil {
					t.Fatal(e)
				}
				nestedListTestCheck(t, &OwnedNestedList{Vector: out.Batch.Arrays[f.Name], Field: f}, kind, rows, false)
				out.Close()
				enc.Close()
				owned.Close()
				if m.Used() != 0 {
					t.Fatal("selected cleanup")
				}
			}
			s.Fields[0].Children[0].Children[0].Nullable = false
			if _, e := EstimateColumnarRunSelectionBytes(s, b, []int{6}); e != nil {
				t.Fatal(e)
			}
			if _, e := EstimateColumnarRunSelectionBytes(s, b, []int{4}); e == nil {
				t.Fatal("selected required accepted")
			}
			v.Child.Offsets[1] = 99
			if _, e := EstimateColumnarRunSelectionBytes(s, b, nil); e == nil {
				t.Fatal("hidden structure under empty selection")
			}
		})
	}
}
func TestNestedListSpillLongCopyCancel(t *testing.T) {
	f, v := nestedListTestFixture(TypeString)
	text := "xx" + strings.Repeat("界", 44000)
	v.Child.Child.Strings.Data = []byte(text)
	v.Child.Child.Strings.Offsets = []uint32{0, 0, 0, 0, 0, 0, 0, 0, uint32(len(text))}
	s, e := NewSchemaWithNestedListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 7
	b.Arrays[f.Name] = v
	m := NewBudgetManager(0)
	ctx := &structListSpillCancelContext{Context: context.Background(), manager: m, target: "runWriter).bytes"}
	enc, e := EncodeColumnarRun(ctx, s, b, m)
	if !errors.Is(e, context.Canceled) || enc != nil || ctx.charged <= 0 || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("encode cancel", e)
	}
	enc, e = EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	payload := append([]byte(nil), enc.Payload...)
	enc.Close()
	ctx = &structListSpillCancelContext{Context: context.Background(), manager: m, target: "runReader).data"}
	out, e := DecodeColumnarRun(ctx, s, nestedSpillRestore(payload), m)
	if !errors.Is(e, context.Canceled) || out != nil || ctx.charged <= 0 || m.Used() != 0 {
		t.Fatal("decode data cancel", e, ctx.checks, ctx.charged)
	}
	out, e = DecodeColumnarRun(context.Background(), s, nestedSpillRestore(payload), m)
	if e != nil {
		t.Fatal(e)
	}
	if string(out.Batch.Arrays[f.Name].Child.Child.Strings.Data) != text {
		t.Fatal("retry bytes")
	}
	out.Close()
}

func TestNestedListSpillIndependentNodeMetadata(t *testing.T) {
	round := func(n uintptr) int64 { return int64((n + 15) &^ 15) }
	for _, kind := range []DataType{TypeInt64, TypeString} {
		f, _ := nestedListTestFixture(kind)
		got, e := arrayDecodeMetadata(f)
		if e != nil {
			t.Fatal(e)
		}
		leaf := unsafe.Sizeof(Int64Vector{})
		if kind == TypeString {
			leaf = unsafe.Sizeof(StringViewVector{})
		}
		want := 2*round(unsafe.Sizeof(ArrayNode{})) + round(leaf)
		if got != want {
			t.Fatalf("%s extra two independently allocated nodes + leaf descriptors got%d want%d (node sizeof%d)", kind, got, want, unsafe.Sizeof(ArrayNode{}))
		}
	}
}

func TestNestedListSpillLegacyBytesUnchanged(t *testing.T) {
	for _, version := range []uint16{2, 3} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			b := NewBatch()
			b.Rows = 1
			f := Field{Name: "x", Type: TypeInt64}
			var s Schema
			if version == 2 {
				b.Int64s["x"] = &Int64Vector{Values: []int64{17}}
				s = NewSchema(f)
			} else {
				var e error
				f, e = ListStructField("x", false, false, Field{Name: "n", Type: TypeInt64})
				if e != nil {
					t.Fatal(e)
				}
				child := NewBatch()
				child.Rows = 1
				child.Int64s["n"] = &Int64Vector{Values: []int64{17}}
				b.StructLists["x"] = &StructListVector{Offsets: []uint32{0, 1}, Values: &StructVector{Rows: 1, Fields: f.Children[0].Children, Values: child}}
				s, e = NewSchemaWithStructListsChecked(f)
				if e != nil {
					t.Fatal(e)
				}
			}
			var golden bytes.Buffer
			golden.WriteString("GLCRUN01")
			put := func(v any) {
				if e := binary.Write(&golden, binary.LittleEndian, v); e != nil {
					t.Fatal(e)
				}
			}
			put(version)
			put(uint64(1))
			put(uint32(1))
			if version == 3 {
				put(uint32(0))
				put(uint32(2))
				put(uint32(0))
				put(uint32(1))
				put(uint32(0))
			}
			put(uint32(0))
			put(int64(17))
			m := NewBudgetManager(0)
			out, e := EncodeColumnarRun(context.Background(), s, b, m)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(out.Payload, golden.Bytes()) {
				t.Fatal("old wire changed")
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("old cleanup")
			}
		})
	}
}
func TestNestedListSpillEveryReserveAndMetadataCancel(t *testing.T) {
	s, b := nestedBatchFixture(t, 8)
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	p := append([]byte(nil), enc.Payload...)
	enc.Close()
	for _, decode := range []bool{false, true} {
		t.Run(fmt.Sprint(decode), func(t *testing.T) {
			probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			if decode {
				v, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p), probe)
				if e != nil {
					t.Fatal(e)
				}
				v.Close()
			} else {
				v, e := EncodeColumnarRun(context.Background(), s, b, probe)
				if e != nil {
					t.Fatal(e)
				}
				v.Close()
			}
			for fail := 1; fail <= probe.calls; fail++ {
				fm := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
				if decode {
					v, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p), fm)
					if e == nil || v != nil {
						t.Fatal("decode admission", fail)
					}
				} else {
					v, e := EncodeColumnarRun(context.Background(), s, b, fm)
					if e == nil || v != nil {
						t.Fatal("encode admission", fail)
					}
				}
				if fm.Used() != 0 {
					t.Fatal("fault leak", fail)
				}
			}
		})
	}
	// Deep metadata traversal is canceled before the first allocation reservation.
	fm := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	ctx := &listStructTestCancelContext{Context: context.Background(), target: ".checkArrayContainerField", stop: 5}
	v, e := EncodeColumnarRun(ctx, s, b, fm)
	if !errors.Is(e, context.Canceled) || v != nil || fm.calls != 0 {
		t.Fatalf("metadata preReserve %v calls%d checks%d", e, fm.calls, ctx.checks)
	}
}

func TestNestedListSpillStringHiddenCorruption(t *testing.T) {
	f, v := nestedListTestFixture(TypeString)
	s, e := NewSchemaWithNestedListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 7
	b.Arrays[f.Name] = v
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	p := append([]byte(nil), enc.Payload...)
	enc.Close()
	// Two List nodes each 12 bitmap +8 counts +32 offsets; terminal begins126.
	if binary.LittleEndian.Uint32(p[138:]) != 9 {
		t.Fatal("String count wire")
	}
	cases := map[string]func([]byte){"outer_bitmap": func(p []byte) { binary.LittleEndian.PutUint32(p[22:], 2) }, "inner_bitmap": func(p []byte) { binary.LittleEndian.PutUint32(p[74:], 2) }, "leaf_bitmap": func(p []byte) { binary.LittleEndian.PutUint32(p[126:], 2) }, "string_count": func(p []byte) { binary.LittleEndian.PutUint32(p[138:], ^uint32(0)) }, "hidden_base": func(p []byte) { binary.LittleEndian.PutUint32(p[142:], 1) }, "hidden_decrease": func(p []byte) { binary.LittleEndian.PutUint32(p[146:], 999) }, "huge_bytes": func(p []byte) { binary.LittleEndian.PutUint64(p[178:], ^uint64(0)) }}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			data := append([]byte(nil), p...)
			mut(data)
			mm := NewBudgetManager(0)
			o, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(data), mm)
			if e == nil || o != nil || mm.Used() != 0 {
				t.Fatal("hidden corruption accepted", e)
			}
		})
	}
	for cut := 0; cut < len(p); cut++ {
		mm := NewBudgetManager(0)
		o, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(p[:cut]), mm)
		if e == nil || o != nil || mm.Used() != 0 {
			t.Fatal("String truncation", cut, e)
		}
	}
}

func TestNestedListSpillLargeEmptyInnerAxis(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, _ := nestedListTestFixture(kind)
			s, e := NewSchemaWithNestedListsChecked(f)
			if e != nil {
				t.Fatal(e)
			}
			leaf := &ArrayNode{Kind: kind}
			if kind == TypeInt64 {
				leaf.Int64 = &Int64Vector{}
			} else {
				leaf.Strings = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
			}
			v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 4097}, Child: &ArrayNode{Kind: TypeList, Offsets: make([]uint32, 4098), Child: leaf}}
			b := NewBatch()
			b.Rows = 1
			b.Arrays[f.Name] = v
			m := NewBudgetManager(0)
			enc, e := EncodeColumnarRun(context.Background(), s, b, m)
			if e != nil {
				t.Fatal(e)
			}
			if len(enc.Payload) < 4098*4 {
				t.Fatal("empty inner offsets omitted")
			}
			out, e := DecodeColumnarRun(context.Background(), s, nestedSpillRestore(enc.Payload), m)
			if e != nil {
				t.Fatal(e)
			}
			a := out.Batch.Arrays[f.Name]
			if a.Len() != 1 || a.Child.Len() != 4097 || a.Child.Child.Len() != 0 {
				t.Fatal("empty axis counts")
			}
			for i := 0; i < 4097; i++ {
				if !a.Child.IsValid(i) || a.Child.Offsets[i] != 0 {
					t.Fatal("empty confused with NULL")
				}
			}
			out.Close()
			enc.Close()
			if m.Used() != 0 {
				t.Fatal("large empty cleanup")
			}
		})
	}
}
