package memory

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func structListSpillRestore(payload []byte) *RestoredSpill {
	return &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}
}
func structListSpillRawCheck(t *testing.T, b *Batch, depth int) {
	t.Helper()
	if b.Rows != 7 {
		t.Fatal("parent rows", b.Rows)
	}
	current := b
	if depth > 0 {
		v := b.Structs["root"]
		for i := 0; i < depth; i++ {
			if v.Rows != 7 || v.Values.Rows != 7 {
				t.Fatal("ancestor row axis")
			}
			if i == 0 && v.IsValid(5) {
				t.Fatal("ancestor NULL")
			}
			current = v.Values
			if i+1 < depth {
				v = current.Structs["inner"]
			}
		}
	}
	for column, v := range []*StructListVector{current.StructLists["items"], b.StructLists["other"]} {
		wantMask := uint64(94)
		if column == 0 && depth > 0 {
			wantMask = 126
		}
		if v == nil || !reflect.DeepEqual(v.Validity, Bitmap{wantMask}) {
			t.Fatal("raw parent validity", column)
		}
		if v == nil || !reflect.DeepEqual(v.Offsets, []uint32{0, 1, 1, 2, 3, 6, 7, 8}) || v.Values.Rows != 8 || v.Values.Values.Rows != 8 {
			t.Fatal("raw offsets/element axis")
		}
		// Inspect every physical scalar slot with independent literal input; no other codec/copy is oracle.
		_, raw, _, _ := listStructTestRaw(t)
		for _, f := range raw.Fields {
			got, want := listStructTestColumn(v.Values.Values, f), listStructTestColumn(raw.Values, f)
			if !reflect.DeepEqual(got.Elem().FieldByName("Validity").Interface(), want.Elem().FieldByName("Validity").Interface()) {
				t.Fatal("child mask", f.Name)
			}
			if f.Type == TypeString {
				if !reflect.DeepEqual(v.Values.Values.Strings[f.Name].Offsets, raw.Values.Strings[f.Name].Offsets) || string(v.Values.Values.Strings[f.Name].Data) != string(raw.Values.Strings[f.Name].Data) {
					t.Fatal("raw string", f.Name)
				}
			} else if !reflect.DeepEqual(got.Elem().FieldByName("Values").Interface(), want.Elem().FieldByName("Values").Interface()) {
				t.Fatal("physical values", f.Name)
			}
		}
		if !reflect.DeepEqual(v.Values.Validity, Bitmap{253}) {
			t.Fatal("item bitmap")
		}
	}
}
func TestStructListSpillRoundtripRawAndBudgets(t *testing.T) {
	for _, depth := range []int{0, 1, 7} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := structListBatchTestFixture(t, depth)
			m := NewBudgetManager(0)
			enc, e := EncodeColumnarRun(context.Background(), s, b, m)
			if e != nil {
				t.Fatal(e)
			}
			if binary.LittleEndian.Uint16(enc.Payload[8:10]) != 3 {
				t.Fatal("new shape requires v3")
			}
			payload := append([]byte(nil), enc.Payload...)
			encodeCharge := m.Used()
			enc.Close()
			if m.Used() != 0 {
				t.Fatal("encode close")
			}
			for _, limit := range []int64{encodeCharge, encodeCharge - 1} {
				mm := NewBudgetManager(limit)
				v, e := EncodeColumnarRun(context.Background(), s, b, mm)
				if limit == encodeCharge {
					if e != nil {
						t.Fatal(e)
					}
					v.Close()
				} else if v != nil || !errors.Is(e, ErrBudgetExceeded) {
					t.Fatal("encode exact-1", v, e)
				}
				if mm.Used() != 0 {
					t.Fatal("encode leaked")
				}
			}
			meta, e := ColumnarRunDecodeMetadataBytes(s)
			if e != nil {
				t.Fatal(e)
			}
			charge := int64(len(payload)) + meta
			for _, limit := range []int64{charge, charge - 1} {
				mm := NewBudgetManager(limit)
				v, e := DecodeColumnarRun(context.Background(), s, structListSpillRestore(payload), mm)
				if limit == charge {
					if e != nil {
						t.Fatal(e)
					}
					if mm.Used() != charge {
						t.Fatal("metadata charge")
					}
					structListSpillRawCheck(t, v.Batch, depth)
					borrowedMap := v.Batch.StructLists
					v.Close()
					if len(borrowedMap) != 0 {
						t.Fatal("Close retained top-level StructLists bindings")
					}
					v.Close()
				} else if v != nil || !errors.Is(e, ErrBudgetExceeded) {
					t.Fatal("decode exact-1", v, e)
				}
				if mm.Used() != 0 {
					t.Fatal("decode leaked")
				}
			}
			if n, e := EstimateColumnarRunRangeBytes(s, b, 0, 7); e != nil || n < int64(len(payload)) {
				t.Fatal("range bound", n, len(payload), e)
			}
		})
	}
}
func TestStructListSpillVersionsAndCorruption(t *testing.T) {
	s, b := structListBatchTestFixture(t, 0)
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	payload := append([]byte(nil), enc.Payload...)
	enc.Close()
	// Independent parsing of v3 first field: 22-byte header, u32 bitmap word count, bitmap, u32 offset count.
	pos := 22
	maskBytes := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	pos += 4 + 8*maskBytes
	if int(binary.LittleEndian.Uint32(payload[pos:pos+4])) != 8 {
		t.Fatal("fixture offset count framing", pos)
	}
	offsets := pos + 4
	for _, kind := range []string{"version2", "version4", "count", "base", "decrease", "terminal", "huge_terminal", "item_bitmap_count", "truncate", "trailing"} {
		t.Run(kind, func(t *testing.T) {
			p := append([]byte(nil), payload...)
			switch kind {
			case "version2":
				binary.LittleEndian.PutUint16(p[8:10], 2)
			case "version4":
				binary.LittleEndian.PutUint16(p[8:10], 4)
			case "count":
				binary.LittleEndian.PutUint32(p[pos:pos+4], 7)
			case "base":
				binary.LittleEndian.PutUint32(p[offsets:offsets+4], 1)
			case "decrease":
				binary.LittleEndian.PutUint32(p[offsets+4:offsets+8], 2)
			case "terminal":
				binary.LittleEndian.PutUint32(p[offsets+28:offsets+32], 9)
			case "huge_terminal":
				binary.LittleEndian.PutUint32(p[offsets+28:offsets+32], ^uint32(0))
				p = p[:offsets+32]
			case "item_bitmap_count":
				binary.LittleEndian.PutUint32(p[offsets+32:offsets+36], 2)
			case "truncate":
				p = p[:len(p)-1]
			case "trailing":
				p = append(p, 0)
			}
			mm := NewBudgetManager(0)
			v, e := DecodeColumnarRun(context.Background(), s, structListSpillRestore(p), mm)
			if v != nil || !errors.Is(e, ErrSpillCorrupt) || mm.Used() != 0 {
				t.Fatal(kind, v, e, mm.Used())
			}
		})
	}
	// Existing scalar v2 framing stays independently specified, not a new-codec self comparison.
	old := NewBatch()
	old.Rows = 1
	old.Int64s["x"] = &Int64Vector{Values: []int64{7}}
	os := NewSchema(Field{Name: "x", Type: TypeInt64})
	v, e := EncodeColumnarRun(context.Background(), os, old, m)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	want := append([]byte("GLCRUN01"), 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0)
	if !reflect.DeepEqual(v.Payload, want) {
		t.Fatalf("old v2 bytes changed: %x", v.Payload)
	}
}

func TestStructListSpillSelectionAncestorDomain(t *testing.T) {
	for _, depth := range []int{0, 7} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := structListBatchTestFixture(t, depth)
			for _, rows := range [][]int{nil, {6, 4, 6, 0, 1}, {5}, {2, 3}} {
				n, e := EstimateColumnarRunSelectionBytes(s, b, rows)
				if e != nil || n < 22 {
					t.Fatal("selection", rows, n, e)
				}
			}
			// Make scalar children required without changing physical NULL storage.
			// Row6 has element7 valid; row3 has present element2 with NULL child.
			var field *Field
			var list *StructListVector
			if depth == 0 {
				field = &s.Fields[0]
				list = b.StructLists["items"]
			} else {
				field = &s.Fields[0]
				v := b.Structs["root"]
				for i := 0; i < depth; i++ {
					field = &field.Children[0]
					if i+1 == depth {
						list = v.Values.StructLists["items"]
					} else {
						v = v.Values.Structs["inner"]
					}
				}
			}
			field.Children[0].Children[0].Nullable = false
			list.Values.Fields[0].Nullable = false
			if _, e := EstimateColumnarRunSelectionBytes(s, b, []int{6, 0, 1}); e != nil {
				t.Fatal("unselected required NULL", e)
			}
			if _, e := EstimateColumnarRunSelectionBytes(s, b, []int{3}); e == nil {
				t.Fatal("selected required NULL admitted")
			}
			// Empty selection never waives structural integrity.
			list.Offsets[1] = 2
			if _, e := EstimateColumnarRunSelectionBytes(s, b, nil); e == nil {
				t.Fatal("hidden malformed offsets with empty selection")
			}
		})
	}
}
func TestStructListSpillDepthAndBadStorageBeforeReserve(t *testing.T) {
	for _, kind := range []string{"depth9", "extra", "hidden_offset"} {
		t.Run(kind, func(t *testing.T) {
			s, b := structListBatchTestFixture(t, 7)
			switch kind {
			case "depth9":
				f := StructField("outer", true, s.Fields[0])
				child := NewBatch()
				child.Rows = 7
				child.Structs["root"] = b.Structs["root"]
				b = NewBatch()
				b.Rows = 7
				b.Structs["outer"] = &StructVector{Rows: 7, Fields: f.Children, Values: child}
				s = Schema{Fields: []Field{f}}
			case "extra":
				b.StructLists["other"].Values.Values.Int64s["extra"] = &Int64Vector{Values: make([]int64, 8)}
			case "hidden_offset":
				b.StructLists["other"].Values.Values.Strings["string"].Offsets[1] = 999
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			v, e := EncodeColumnarRun(context.Background(), s, b, m)
			if v != nil || e == nil || m.calls != 0 || m.Used() != 0 {
				t.Fatal(kind, v, e, m.calls, m.Used())
			}
		})
	}
}

type structListSpillCancelContext struct {
	context.Context
	target   string
	checks   int
	canceled bool
	manager  Manager
	charged  int64
}

func (c *structListSpillCancelContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	matched := false
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, c.target) {
			matched = true
		}
		if matched && (c.target != "runWriter).bytes" || strings.HasSuffix(f.Function, ".encodeNormalizedString")) {
			c.checks++
			if c.checks == 2 {
				c.canceled = true
				c.charged = c.manager.Used()
				return context.Canceled
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestStructListSpillChunkCancellationAndRetry(t *testing.T) {
	f, v, _, text := listStructTestLong(t)
	s, e := NewSchemaWithStructListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 1
	b.StructLists[f.Name] = v
	m := NewBudgetManager(0)
	ctx := &structListSpillCancelContext{Context: context.Background(), target: "runWriter).bytes", manager: m}
	enc, e := EncodeColumnarRun(ctx, s, b, m)
	if enc != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charged <= 0 || m.Used() != 0 {
		t.Fatal("write copy cancel", enc, e, ctx.checks, ctx.charged, m.Used())
	}
	enc, e = EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	payload := append([]byte(nil), enc.Payload...)
	enc.Close()
	ctx = &structListSpillCancelContext{Context: context.Background(), target: "runReader).data", manager: m}
	dec, e := DecodeColumnarRun(ctx, s, structListSpillRestore(payload), m)
	if dec != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charged <= 0 || m.Used() != 0 {
		t.Fatal("read copy cancel", dec, e, ctx.checks, ctx.charged, m.Used())
	}
	dec, e = DecodeColumnarRun(context.Background(), s, structListSpillRestore(payload), m)
	if e != nil {
		t.Fatal(e)
	}
	if string(dec.Batch.StructLists[f.Name].Values.Values.Strings["s"].Data) != text {
		t.Fatal("retry value")
	}
	dec.Close()
	if m.Used() != 0 {
		t.Fatal("retry leak")
	}
}

func TestStructListSpillSelectedOwnedPayloadBound(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	s, e := NewSchemaWithStructListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	// Top-level estimator sees the same effective parent domain as the explicit owner.
	borrowed := *v
	borrowed.Validity = Bitmap{94}
	b := NewBatch()
	b.Rows = 7
	b.StructLists[f.Name] = &borrowed
	for _, rows := range [][]int{nil, {6, 4, 6, 0, 1}, {2, 3, 5}, {0}} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			m := NewBudgetManager(0)
			owner, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
			if e != nil {
				t.Fatal(e)
			}
			defer owner.Close()
			listStructTestCheck(t, owner, rows)
			selected := NewBatch()
			selected.Rows = len(rows)
			selected.StructLists[f.Name] = owner.Vector
			enc, e := EncodeColumnarRun(context.Background(), s, selected, m)
			if e != nil {
				t.Fatal(e)
			}
			defer enc.Close()
			estimate, e := EstimateColumnarRunSelectionBytes(s, b, rows)
			if e != nil || estimate < int64(len(enc.Payload)) {
				t.Fatal("selected bound", estimate, len(enc.Payload), e)
			}
			dec, e := DecodeColumnarRun(context.Background(), s, structListSpillRestore(enc.Payload), m)
			if e != nil {
				t.Fatal(e)
			}
			listStructTestCheck(t, &OwnedStructList{Vector: dec.Batch.StructLists[f.Name], Field: f}, rows)
			dec.Close()
			enc.Close()
			owner.Close()
			if m.Used() != 0 {
				t.Fatal("selected owners leak")
			}
		})
	}
	for _, r := range [][2]int{{0, 0}, {1, 0}, {2, 3}, {7, 0}} {
		t.Run(fmt.Sprint(r), func(t *testing.T) {
			m := NewBudgetManager(0)
			owner, e := CloneStructListRangeOwned(context.Background(), m, v, f, d, r[0], r[1])
			if e != nil {
				t.Fatal(e)
			}
			defer owner.Close()
			rows := make([]int, r[1])
			for i := range rows {
				rows[i] = r[0] + i
			}
			listStructTestCheck(t, owner, rows)
			selected := NewBatch()
			selected.Rows = r[1]
			selected.StructLists[f.Name] = owner.Vector
			enc, e := EncodeColumnarRun(context.Background(), s, selected, m)
			if e != nil {
				t.Fatal(e)
			}
			n, e := EstimateColumnarRunRangeBytes(s, b, r[0], r[1])
			if e != nil || n < int64(len(enc.Payload)) {
				t.Fatal("range bound", n, len(enc.Payload), e)
			}
			enc.Close()
			owner.Close()
			if m.Used() != 0 {
				t.Fatal("range leak")
			}
		})
	}
}

func TestStructListSpillMetadataCancelAndSourceImmutability(t *testing.T) {
	s, b := structListBatchTestFixture(t, 7)
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	ctx := &recursiveSpillSelectionContext{Context: context.Background(), stop: 5, target: ".columnarDecodeBatchBytesContext"}
	out, e := DecodeColumnarRun(ctx, s, structListSpillRestore(nil), m)
	if out != nil || !errors.Is(e, context.Canceled) || m.calls != 0 || m.Used() != 0 || ctx.checks != 5 {
		t.Fatal("metadata preadmission cancel", out, e, ctx.checks, m.calls, m.Used())
	}
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	payload := append([]byte(nil), enc.Payload...)
	enc.Close()
	// Decoder owns its reconstructed scalar and String buffers, not the caller payload.
	dec, e := DecodeColumnarRun(context.Background(), s, structListSpillRestore(payload), m)
	if e != nil {
		t.Fatal(e)
	}
	for i := range payload {
		payload[i] = 0
	}
	b.StructLists["other"].Values.Values.Int64s["int64"].Values[3] = 999
	structListSpillRawCheck(t, dec.Batch, 7)
	dec.Close()
	if m.Used() != 0 {
		t.Fatal("independent decode ownership")
	}
}

func TestStructListSpillNullParentCorruptStringStorage(t *testing.T) {
	f, v, _, _ := listStructTestLong(t)
	f.Nullable = true
	v.Validity = Bitmap{0}
	s, e := NewSchemaWithStructListsChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 1
	b.StructLists[f.Name] = v
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	payload := append([]byte(nil), enc.Payload...)
	enc.Close()
	pos := 22
	skipBitmap := func() { n := int(binary.LittleEndian.Uint32(payload[pos : pos+4])); pos += 4 + 8*n }
	skipBitmap()
	n := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	if n != 2 {
		t.Fatal("fixture parent offsets")
	}
	pos += 4 + 4*n
	skipBitmap()
	stringBitmap := pos
	skipBitmap()
	stringCount := pos
	if binary.LittleEndian.Uint32(payload[stringCount:stringCount+4]) != 2 {
		t.Fatal("fixture String offsets")
	}
	for _, kind := range []string{"bitmap", "offset_count", "offset_decrease"} {
		t.Run(kind, func(t *testing.T) {
			p := append([]byte(nil), payload...)
			switch kind {
			case "bitmap":
				binary.LittleEndian.PutUint32(p[stringBitmap:stringBitmap+4], 2)
			case "offset_count":
				binary.LittleEndian.PutUint32(p[stringCount:stringCount+4], 3)
			case "offset_decrease":
				terminal := binary.LittleEndian.Uint32(p[stringCount+8 : stringCount+12])
				binary.LittleEndian.PutUint32(p[stringCount+4:stringCount+8], terminal+1)
			}
			mm := NewBudgetManager(0)
			out, e := DecodeColumnarRun(context.Background(), s, structListSpillRestore(p), mm)
			if out != nil || !errors.Is(e, ErrSpillCorrupt) || mm.Used() != 0 {
				if out != nil {
					out.Close()
				}
				t.Fatal("hidden structural corruption", out, e, mm.Used())
			}
		})
	}
}
