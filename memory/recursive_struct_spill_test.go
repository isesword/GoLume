package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func recursiveSpillFixture(t *testing.T, depth int) (Schema, *Batch) {
	t.Helper()
	f, v := recursiveTestAllLeaves(depth)
	s, e := NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 3
	b.Structs[f.Name] = v
	return s, b
}
func recursiveSpillCheck(t *testing.T, b *Batch, rows []int) {
	t.Helper()
	v := b.Structs["node"]
	if v == nil || v.Rows != len(rows) || b.Rows != len(rows) {
		t.Fatal("spill root shape")
	}
	leaf := recursiveTestLeaf(v)
	if len(leaf.Fields) != 20 {
		t.Fatal("spill child metadata")
	}
	for i, row := range rows {
		if v.IsValid(i) != (row != 1) {
			t.Fatal("parent mask", row)
		}
		if row != 0 {
			continue
		}
		if leaf.Values.Int8s["int8"].Values[i] != 7 || leaf.Values.Float64s["float64"].Values[i] != 7 || leaf.Values.Uint32s["bool"].Values[i] != 1 || string(leaf.Values.Strings["string"].ValueBytes(i)) != "a" {
			t.Fatal("spill scalar oracle")
		}
		for _, field := range leaf.Fields {
			if field.Type == TypeString || field.Type == TypeList {
				continue
			}
			physical := recursiveTestPhysical(leaf.Values, field.Name)
			x := physical.FieldByName("Values").Index(i)
			want := float64(7)
			if field.Type == TypeBool {
				want = 1
			}
			var got float64
			switch x.Kind() {
			case reflect.Float32, reflect.Float64:
				got = x.Float()
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				got = float64(x.Uint())
			default:
				got = float64(x.Int())
			}
			if got != want {
				t.Fatal("spill fixed type oracle", field.Type, got, want)
			}
		}
		ints := leaf.Values.Int64Lists["int_list"]
		lo, hi := int(ints.Offsets[i]), int(ints.Offsets[i+1])
		if hi-lo != 2 || ints.Values.Values[lo] != 9223372036854775807 || !ints.Values.IsValid(lo) || ints.Values.IsValid(lo+1) {
			t.Fatal("spill Int64List oracle")
		}
		texts := leaf.Values.StringLists["list"]
		lo, hi = int(texts.Offsets[i]), int(texts.Offsets[i+1])
		if hi-lo != 2 || string(texts.Values.ValueBytes(lo)) != "a" || !texts.Values.IsValid(lo) || texts.Values.IsValid(lo+1) {
			t.Fatal("spill StringList oracle")
		}
	}
}
func TestRecursiveStructSpillRoundtripAndAdmission(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := recursiveSpillFixture(t, depth)
			ctx := context.Background()
			m := NewBudgetManager(0)
			enc, e := EncodeColumnarRun(ctx, s, b, m)
			if e != nil {
				t.Fatal(e)
			}
			encodeCharge := m.Used()
			if encodeCharge <= 0 {
				t.Fatal("unaccounted encode")
			}
			payload := append([]byte(nil), enc.Payload...)
			enc.Close()
			if m.Used() != 0 {
				t.Fatal("encode close")
			}
			for _, limit := range []int64{encodeCharge, encodeCharge - 1} {
				mgr := NewBudgetManager(limit)
				result, err := EncodeColumnarRun(ctx, s, b, mgr)
				if limit == encodeCharge {
					if err != nil {
						t.Fatal("exact encode budget", err)
					}
					result.Close()
				} else if result != nil || !errors.Is(err, ErrBudgetExceeded) {
					t.Fatal("encode -1", result, err)
				}
				if mgr.Used() != 0 {
					t.Fatal("encode cleanup")
				}
			}
			restored := &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}
			decoded, e := DecodeColumnarRun(ctx, s, restored, m)
			if e != nil {
				t.Fatal(e)
			}
			decodeCharge := m.Used()
			recursiveSpillCheck(t, decoded.Batch, []int{0, 1, 2})
			if e = decoded.Batch.Structs["node"].ValidateRecursiveWithContext(s.Fields[0], 3, nil); e != nil {
				t.Fatal(e)
			}
			decoded.Close()
			if m.Used() != 0 {
				t.Fatal("decode cleanup")
			}
			for _, limit := range []int64{decodeCharge, decodeCharge - 1} {
				mgr := NewBudgetManager(limit)
				result, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: payload}}, mgr)
				if limit == decodeCharge {
					if err != nil {
						t.Fatal("exact decode budget", err)
					}
					result.Close()
				} else if result != nil || !errors.Is(err, ErrBudgetExceeded) {
					t.Fatal("decode -1", result, err)
				}
				if mgr.Used() != 0 {
					t.Fatal("decode cleanup")
				}
			}
		})
	}
}
func TestRecursiveStructSpillMalformedHiddenAndTruncation(t *testing.T) {
	s, b := recursiveSpillFixture(t, 8)
	leaf := recursiveTestLeaf(b.Structs["node"])
	leaf.Values.Strings["string"].Offsets = []uint32{0, 7, 1, 13}
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if enc != nil || e == nil || m.Used() != 0 {
		t.Fatal("malformed hidden storage accepted", enc, e)
	}
	s, b = recursiveSpillFixture(t, 2)
	enc, e = EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	defer enc.Close()
	for _, n := range []int{0, 1, 8, len(enc.Payload) / 2, len(enc.Payload) - 1} {
		mgr := NewBudgetManager(0)
		out, e := DecodeColumnarRun(context.Background(), s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: enc.Payload[:n]}}, mgr)
		if out != nil || !errors.Is(e, ErrSpillCorrupt) || mgr.Used() != 0 {
			t.Fatal("truncation cleanup", n, out, e, mgr.Used())
		}
	}
}

func TestRecursiveStructSpillDepthAndRequiredAncestors(t *testing.T) {
	f, v := recursiveTestAllLeaves(9)
	s := Schema{Fields: []Field{f}}
	b := NewBatch()
	b.Rows = 3
	b.Structs[f.Name] = v
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if enc != nil || e == nil || m.Used() != 0 {
		t.Fatal("depth9 encode accepted", enc, e)
	}
	if _, e = EstimateColumnarRunSelectionBytes(s, b, []int{0}); e == nil {
		t.Fatal("depth9 selection estimate accepted")
	}
	f, v = recursiveTestAllLeaves(8)
	field := &f
	for field.Children[0].Type == TypeStruct {
		field = &field.Children[0]
		field.Nullable = false
	}
	s, e = NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b = NewBatch()
	b.Rows = 3
	b.Structs[f.Name] = v
	enc, e = EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal("required descendant hidden by root NULL", e)
	}
	defer enc.Close()
	out, e := DecodeColumnarRun(context.Background(), s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: enc.Payload}}, m)
	if e != nil {
		t.Fatal(e)
	}
	recursiveSpillCheck(t, out.Batch, []int{0, 1, 2})
	out.Close()
}

func recursiveSpillSelectedFixture(depth int, rows []int) (Field, *StructVector) {
	f, v := recursiveTestVector(depth)
	node := v
	for {
		node.Rows = len(rows)
		node.Values.Rows = len(rows)
		node.Validity = make(Bitmap, WordsFor(len(rows)))
		for i, row := range rows {
			if row != 2 {
				Set(node.Validity, i)
			}
		}
		if len(node.Values.Structs) == 0 {
			break
		}
		node = node.Values.Structs["inner"]
	}
	nums := &Int64Vector{Validity: make(Bitmap, WordsFor(len(rows)))}
	text := &StringViewVector{Offsets: []uint32{0}, Validity: make(Bitmap, WordsFor(len(rows))), Encoding: EncodingView}
	for i, row := range rows {
		n := int64(0)
		if row != 2 {
			n = int64(11 * (row + 1))
			Set(nums.Validity, i)
			Set(text.Validity, i)
			text.Data = append(text.Data, byte('a'+row))
		}
		nums.Values = append(nums.Values, n)
		text.Offsets = append(text.Offsets, uint32(len(text.Data)))
	}
	node.Values.Int64s["n"] = nums
	node.Values.Strings["text"] = text
	return f, v
}
func TestRecursiveStructSpillSelectionAncestorDomain(t *testing.T) {
	for _, depth := range []int{2, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f, v := recursiveSpillSelectedFixture(depth, []int{0, 1, 2})
			s, e := NewSchemaChecked(f)
			if e != nil {
				t.Fatal(e)
			}
			b := NewBatch()
			b.Rows = 3
			b.Structs["node"] = v
			// Visible required-NULL row2 must not poison a selection of only0/1.
			v.Validity = Bitmap{7}
			for node := v.Values.Structs["inner"]; node != nil; node = node.Values.Structs["inner"] {
				node.Validity = Bitmap{7}
			}
			if _, e = EstimateColumnarRunSelectionBytes(s, b, []int{0, 1, 0}); e != nil {
				t.Fatal("unselected required NULL", e)
			}
			if _, e = EstimateColumnarRunSelectionBytes(s, b, []int{2}); e == nil {
				t.Fatal("selected visible required NULL accepted")
			}
			// NULL outer ancestor hides the same required descendant row.
			v.Validity = Bitmap{3}
			for _, rows := range [][]int{nil, {1, 0, 1, 2}, {2}, {0}} {
				estimate, e := EstimateColumnarRunSelectionBytes(s, b, rows)
				if e != nil {
					t.Fatal("selection domain", rows, e)
				}
				_, selected := recursiveSpillSelectedFixture(depth, rows)
				out := NewBatch()
				out.Rows = len(rows)
				out.Structs["node"] = selected
				m := NewBudgetManager(0)
				enc, e := EncodeColumnarRun(context.Background(), s, out, m)
				if e != nil {
					t.Fatal(e)
				}
				if int64(len(enc.Payload)) > estimate {
					t.Fatal("selection underestimates payload", len(enc.Payload), estimate)
				}
				enc.Close()
				if m.Used() != 0 {
					t.Fatal("selection encoding cleanup")
				}
			}
			leaf := recursiveTestLeaf(v)
			leaf.Values.Strings["text"].Offsets = []uint32{0, 2, 1, 2}
			if _, e = EstimateColumnarRunSelectionBytes(s, b, []int{0}); e == nil {
				t.Fatal("unselected malformed storage hidden")
			}
		})
	}
}

type recursiveSpillCopyContext struct {
	context.Context
	decode bool
	checks int
}

func (c *recursiveSpillCopyContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	prev := ""
	for {
		f, more := frames.Next()
		match := strings.Contains(prev, "runWriter).bytes") && strings.HasSuffix(f.Function, ".encodeNormalizedString")
		if c.decode {
			match = prev == "" && strings.Contains(f.Function, "runReader).data")
		}
		if match {
			c.checks++
			if c.checks == 2 {
				return context.Canceled
			}
			break
		}
		prev = f.Function
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestRecursiveStructSpillChunkCancellation(t *testing.T) {
	f, v := recursiveTestVector(8)
	leaf := recursiveTestLeaf(v)
	payload := strings.Repeat("界", 44000)
	leaf.Values.Strings["text"] = &StringViewVector{Offsets: []uint32{0, uint32(len(payload)), uint32(len(payload) + 6), uint32(len(payload) + 7)}, Data: []byte(payload + "HIDDENc"), Validity: Bitmap{5}, Encoding: EncodingView}
	s, e := NewSchemaChecked(f)
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 3
	b.Structs["node"] = v
	m := NewBudgetManager(0)
	ctx := &recursiveSpillCopyContext{Context: context.Background()}
	enc, e := EncodeColumnarRun(ctx, s, b, m)
	if enc != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("encode chunk cancellation", enc, e, ctx.checks, m.Used())
	}
	enc, e = EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	raw := append([]byte(nil), enc.Payload...)
	enc.Close()
	ctx = &recursiveSpillCopyContext{Context: context.Background(), decode: true}
	out, e := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: raw}}, m)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("decode chunk cancellation", out, e, ctx.checks, m.Used())
	}
	out, e = DecodeColumnarRun(context.Background(), s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: raw}}, m)
	if e != nil {
		t.Fatal(e)
	}
	if string(recursiveTestLeaf(out.Batch.Structs["node"]).Values.Strings["text"].ValueBytes(0)) != payload {
		t.Fatal("retry payload")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("retry cleanup")
	}
}

func TestRecursiveStructSpillRangeEstimatorAndMetadata(t *testing.T) {
	for _, depth := range []int{2, 8} {
		f, v := recursiveSpillSelectedFixture(depth, []int{0, 1, 2})
		s, e := NewSchemaChecked(f)
		if e != nil {
			t.Fatal(e)
		}
		b := NewBatch()
		b.Rows = 3
		b.Structs["node"] = v
		metadata, e := ColumnarRunDecodeMetadataBytes(s)
		if e != nil || metadata <= 0 {
			t.Fatal("recursive metadata", metadata, e)
		}
		for _, span := range [][2]int{{0, 3}, {1, 2}, {3, 0}} {
			estimate, e := EstimateColumnarRunRangeBytes(s, b, span[0], span[1])
			if e != nil {
				t.Fatal(e)
			}
			rows := make([]int, span[1])
			for i := range rows {
				rows[i] = span[0] + i
			}
			_, out := recursiveSpillSelectedFixture(depth, rows)
			batch := NewBatch()
			batch.Rows = len(rows)
			batch.Structs["node"] = out
			enc, e := EncodeColumnarRun(context.Background(), s, batch, NewBudgetManager(0))
			if e != nil {
				t.Fatal(e)
			}
			if int64(len(enc.Payload)) > estimate {
				t.Fatal("range underestimate")
			}
			enc.Close()
		}
	}
	f := recursiveTestField(9)
	if _, e := ColumnarRunDecodeMetadataBytes(Schema{Fields: []Field{f}}); e == nil {
		t.Fatal("deep metadata admitted")
	}
}

func TestRecursiveStructSpillSafeTakeActualCopy(t *testing.T) {
	for _, depth := range []int{2, 8} {
		for _, rows := range [][]int{nil, {2, 0, 1, 0}, {0}, {1}} {
			t.Run(fmt.Sprintf("%d/%v", depth, rows), func(t *testing.T) {
				s, b := recursiveSpillFixture(t, depth)
				m := NewBudgetManager(0)
				out, e := TakeRecursiveStructRowsOwned(context.Background(), m, b.Structs["node"], s.Fields[0], rows)
				if e != nil {
					t.Fatal(e)
				}
				selected := NewBatch()
				selected.Rows = len(rows)
				selected.Structs["node"] = out.Vector
				recursiveSpillCheck(t, selected, rows)
				if e = out.Vector.ValidateRecursiveWithContext(s.Fields[0], len(rows), nil); e != nil {
					t.Fatal(e)
				}
				estimate, e := EstimateColumnarRunSelectionBytes(s, b, rows)
				if e != nil {
					t.Fatal(e)
				}
				enc, e := EncodeColumnarRun(context.Background(), s, selected, NewBudgetManager(0))
				if e != nil {
					t.Fatal(e)
				}
				if int64(len(enc.Payload)) > estimate {
					t.Fatal("actual gathered payload underestimated")
				}
				enc.Close()
				charge := out.Bytes
				out.Close()
				if m.Used() != 0 {
					t.Fatal("take Close")
				}
				tight := NewBudgetManager(charge)
				again, e := TakeRecursiveStructRowsOwned(context.Background(), tight, b.Structs["node"], s.Fields[0], rows)
				if e != nil {
					t.Fatal(e)
				}
				again.Close()
				small := NewBudgetManager(charge - 1)
				bad, e := TakeRecursiveStructRowsOwned(context.Background(), small, b.Structs["node"], s.Fields[0], rows)
				if bad != nil || !errors.Is(e, ErrBudgetExceeded) || small.Used() != 0 {
					t.Fatal("take admission", bad, e)
				}
			})
		}
	}
}
func TestRecursiveStructSpillSafeTakeSelectedDomainAndCancel(t *testing.T) {
	f, v := recursiveSpillSelectedFixture(8, []int{0, 1, 2})
	for node := v; ; node = node.Values.Structs["inner"] {
		node.Validity = Bitmap{7}
		if len(node.Values.Structs) == 0 {
			break
		}
	}
	m := NewBudgetManager(0)
	out, e := TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{1, 0, 1})
	if e != nil {
		t.Fatal("unselected required NULL poisoned take", e)
	}
	leaf := recursiveTestLeaf(out.Vector)
	if !reflect.DeepEqual(leaf.Values.Int64s["n"].Values, []int64{22, 11, 22}) {
		t.Fatal("reordered duplicate gather")
	}
	out.Close()
	if out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{2}); out != nil || e == nil || m.Used() != 0 {
		t.Fatal("selected invalid required NULL")
	}
	v.Validity = Bitmap{3}
	out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{2})
	if e != nil {
		t.Fatal("NULL ancestor domain", e)
	}
	out.Close()
	leaf = recursiveTestLeaf(v)
	payload := strings.Repeat("界", 44000)
	leaf.Values.Strings["text"] = &StringViewVector{Offsets: []uint32{0, uint32(len(payload)), uint32(len(payload) + 1), uint32(len(payload) + 1)}, Data: []byte(payload + "b"), Validity: Bitmap{3}, Encoding: EncodingView}
	ctx := &recursiveTestCopyContext{Context: context.Background(), manager: m}
	out, e = TakeRecursiveStructRowsOwned(ctx, m, v, f, []int{0, 0})
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("take actual copy cancellation", out, e, ctx.checks, m.Used())
	}
	out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{0, 0})
	if e != nil {
		t.Fatal(e)
	}
	if string(recursiveTestLeaf(out.Vector).Values.Strings["text"].ValueBytes(1)) != payload {
		t.Fatal("take retry exact bytes")
	}
	out.Close()
	if out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{-1}); out != nil || e == nil {
		t.Fatal("negative take row accepted")
	}
	if out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{3}); out != nil || e == nil {
		t.Fatal("OOB take row accepted")
	}
	if m.Used() != 0 {
		t.Fatal("take cleanup")
	}
}

func TestRecursiveStructSpillMetadataMismatchAndEmptyStorageValidation(t *testing.T) {
	s, b := recursiveSpillFixture(t, 8)
	m := NewBudgetManager(0)
	enc, e := EncodeColumnarRun(context.Background(), s, b, m)
	if e != nil {
		t.Fatal(e)
	}
	raw := append([]byte(nil), enc.Payload...)
	enc.Close()
	wrong, e := NewSchemaChecked(s.Fields...)
	if e != nil {
		t.Fatal(e)
	}
	f := &wrong.Fields[0]
	for f.Children[0].Type == TypeStruct {
		f = &f.Children[0]
	}
	// Version2 codec is positional: names are supplied by the external schema.
	f.Children[0].Type = TypeInt64 // encoded Int8 width cannot satisfy this layout
	out, e := DecodeColumnarRun(context.Background(), wrong, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: raw}}, m)
	if out != nil || !errors.Is(e, ErrSpillCorrupt) || m.Used() != 0 {
		t.Fatal("nested metadata mismatch", out, e, m.Used())
	}
	leaf := recursiveTestLeaf(b.Structs["node"])
	leaf.Values.StringLists["list"].Offsets = []uint32{0, 3, 2, 3}
	if _, e = EstimateColumnarRunSelectionBytes(s, b, nil); e == nil {
		t.Fatal("empty selection hid malformed descendant storage")
	}
	owned, e := TakeRecursiveStructRowsOwned(context.Background(), m, b.Structs["node"], s.Fields[0], nil)
	if owned != nil || e == nil || m.Used() != 0 {
		t.Fatal("empty take hid malformed storage", owned, e)
	}
}

// Only direct selection-domain checkpoints count: storage validators and entry
// checks cannot accidentally satisfy the long-element-scan cancellation case.
type recursiveSpillSelectionContext struct {
	context.Context
	checks, stop int
	target       string
	canceled     bool
}

func (c *recursiveSpillSelectionContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [8]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".recursiveCheck") {
			if !more {
				break
			}
			continue
		}
		target := c.target
		if target == "" {
			target = ".selectionRunFieldSize"
		}
		if strings.HasSuffix(f.Function, target) {
			c.checks++
			if c.checks >= c.stop {
				c.canceled = true
				return context.Canceled
			}
		}
		break
	}
	return nil
}
func TestRecursiveStructSpillSafeTakePreflightCancellation(t *testing.T) {
	for _, stop := range []int{1, 100} {
		t.Run(fmt.Sprintf("selection_checkpoint_%d", stop), func(t *testing.T) {
			f, v := recursiveTestAllLeaves(8)
			leaf := recursiveTestLeaf(v)
			const count = 4096
			offsets := make([]uint32, count+1)
			for i := range offsets {
				offsets[i] = uint32(i)
			}
			leaf.Values.StringLists["list"] = &StringListVector{
				Offsets: []uint32{0, count, count, count}, Validity: Bitmap{7},
				Values: &StringViewVector{Offsets: offsets, Data: []byte(strings.Repeat("x", count)), Encoding: EncodingView},
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			ctx := &recursiveSpillSelectionContext{Context: context.Background(), stop: stop}
			out, e := TakeRecursiveStructRowsOwned(ctx, m, v, f, []int{0})
			if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != stop || m.calls != 0 || m.Used() != 0 {
				t.Fatalf("pre-admission cancellation out=%v err=%v checks=%d reserves=%d used=%d", out, e, ctx.checks, m.calls, m.Used())
			}
			// Same source remains intact and retry really copies all long-list elements.
			out, e = TakeRecursiveStructRowsOwned(context.Background(), m, v, f, []int{0})
			if e != nil {
				t.Fatal(e)
			}
			got := recursiveTestLeaf(out.Vector).Values.StringLists["list"]
			if got.Offsets[1] != count || string(got.Values.Data) != strings.Repeat("x", count) {
				t.Fatal("retry list payload")
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("retry leaked")
			}
		})
	}
}

func TestRecursiveStructSpillDecodeMetadataCancellation(t *testing.T) {
	s, b := recursiveSpillFixture(t, 8)
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	enc, err := EncodeColumnarRun(context.Background(), s, b, m)
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte(nil), enc.Payload...)
	enc.Close()
	m.calls = 0
	ctx := &recursiveSpillSelectionContext{Context: context.Background(), stop: 5, target: ".columnarDecodeBatchBytesContext"}
	out, err := DecodeColumnarRun(ctx, s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: raw}}, m)
	if out != nil || !errors.Is(err, context.Canceled) || ctx.checks != 5 || m.calls != 0 || m.Used() != 0 {
		t.Fatalf("metadata cancel out=%v err=%v checkpoints=%d reserves=%d used=%d", out, err, ctx.checks, m.calls, m.Used())
	}
	out, err = DecodeColumnarRun(context.Background(), s, &RestoredSpill{Object: SpillObject{Codec: ColumnarRunCodec, Payload: raw}}, m)
	if err != nil {
		t.Fatal(err)
	}
	recursiveSpillCheck(t, out.Batch, []int{0, 1, 2})
	out.Close()
	if m.Used() != 0 {
		t.Fatal("decode retry leaked")
	}
}
