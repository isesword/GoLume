package memory

import (
	"github.com/apache/arrow-go/v18/arrow"
	"reflect"
	"testing"
	"unsafe"
)

type sharedUntouchedOpaque struct{ arrow.Array }

func (*sharedUntouchedOpaque) Data() arrow.ArrayData { panic("shared fallback touched Arrow.Data") }

func sharedNativeFixture() *Batch {
	b := NewBatch()
	b.Rows = 3
	// Populate every scalar kind without introducing reflection in production.
	root := reflect.ValueOf(b).Elem()
	for _, name := range []string{"Int8s", "Int16s", "Int32s", "Int64s", "Uint8s", "Uint16s", "Uint32s", "Uint64s", "Float32s", "Float64s", "Decimals"} {
		m := root.FieldByName(name)
		v := reflect.New(m.Type().Elem().Elem())
		values := v.Elem().FieldByName("Values")
		values.Set(reflect.MakeSlice(values.Type(), 3, 8))
		v.Elem().FieldByName("Validity").Set(reflect.ValueOf(Bitmap{5}))
		m.SetMapIndex(reflect.ValueOf("v"), v)
	}
	b.Decimals["v"].Precision = 10
	b.Strings["s"] = &StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc"), Validity: Bitmap{5}}
	b.DictStrings["d"] = &DictionaryStringVector{Indices: []uint32{0, 0, 0}, Offsets: []uint32{0, 1}, Data: []byte("x"), DictionaryDomain: "domain", Validity: Bitmap{5}}
	b.StringLists["l"] = &StringListVector{Offsets: []uint32{0, 1, 2, 3}, Values: b.Strings["s"], Validity: Bitmap{5}}
	return b
}

func assertIndependentNativeHeaders(t *testing.T, a, b *Batch) {
	t.Helper()
	if a == b || a.Rows != b.Rows {
		t.Fatal("Batch header was not copied")
	}
	av, bv := reflect.ValueOf(a).Elem(), reflect.ValueOf(b).Elem()
	for i := 0; i < av.NumField(); i++ {
		am, bm := av.Field(i), bv.Field(i)
		if am.Kind() != reflect.Map {
			continue
		}
		if am.Pointer() == bm.Pointer() {
			t.Fatalf("shared map %s", av.Type().Field(i).Name)
		}
		for _, key := range am.MapKeys() {
			x, y := am.MapIndex(key), bm.MapIndex(key)
			if x.Pointer() == y.Pointer() {
				t.Fatalf("shared vector header %s", key)
			}
			if !reflect.DeepEqual(x.Interface(), y.Interface()) {
				t.Fatalf("header contents differ %s", key)
			}
		}
	}
	for k, l := range a.StringLists {
		if l.Values == b.StringLists[k].Values {
			t.Fatal("List child header shared")
		}
	}
	for k, v := range a.Structs {
		assertIndependentNativeHeaders(t, v.Values, b.Structs[k].Values)
	}
}

func TestSharedNativeWholeBindings(t *testing.T) {
	for _, rows := range []int{0, 3} {
		for _, nulls := range []bool{false, true} {
			src := sharedNativeFixture()
			child := NewBatch()
			child.Rows = 3
			child.Int8s["x"] = &Int8Vector{Values: []int8{1, 2, 3}}
			v := &StructVector{Rows: 3, Values: child, Fields: []Field{{Name: "x", Type: TypeInt8}, {Name: "x", Type: TypeInt8}}}
			if nulls {
				v.Validity = Bitmap{0}
			}
			src.Structs["s"] = v
			owned, e := CloneBatchRange(src, 0, rows)
			if e != nil {
				t.Fatal(e)
			}
			first, e := BorrowNativeWholeBatch(owned)
			if e != nil {
				t.Fatal(e)
			}
			second, e := BorrowNativeWholeBatch(owned)
			if e != nil {
				t.Fatal(e)
			}
			assertIndependentNativeHeaders(t, owned, first)
			assertIndependentNativeHeaders(t, first, second)
			if got := len(second.Structs["s"].Values.Int8s["x"].Values); got != 2*rows {
				t.Fatalf("duplicate field payload truncated %d", got)
			}
			if rows > 0 && &first.Int8s["v"].Values[0] != &owned.Int8s["v"].Values[0] {
				t.Fatal("payload copied")
			}
			// Legal root binding replacement and early clearing cannot erase siblings.
			first.Int8s["replacement"] = &Int8Vector{Values: []int8{7}}
			first.Structs["s"].Values.ClearColumns()
			first.ClearColumns()
			if len(second.Int8s) != len(owned.Int8s) || second.Structs["s"].Values.Int8s["x"] == nil {
				t.Fatal("clear reached sibling")
			}
			second.ClearColumns()
			owned.ReleaseOwned()
		}
	}
}

func TestSharedNativeCostsAndFallback(t *testing.T) {
	b := sharedNativeFixture()
	for offset := 0; offset <= 3; offset++ {
		for rows := 0; rows <= 3-offset; rows++ {
			s, e := EstimateNativeSharedBatchRange(b, offset, rows)
			if e != nil {
				t.Fatal(e)
			}
			c, e := EstimateNativeOwnedBatchRangeBytes(b, offset, rows)
			if e != nil {
				t.Fatal(e)
			}
			state := nativeOwnedEstimateState{}
			m, e := ownedRangeMetadata(b, offset, rows, &state, false, false, true, true)
			if e != nil {
				t.Fatal(e)
			}
			if !s.Supported || s.CloneBytes != c.Bytes || s.OwnerBytes+s.BindingBytes != c.Bytes || s.BindingBytes != m-256 || s.OwnerBytes != c.Bytes-m+256 {
				t.Fatalf("bad decomposition %+v C=%d M=%d", s, c.Bytes, m)
			}
			if n := testing.AllocsPerRun(20, func() {
				_, err := EstimateNativeSharedBatchRange(b, offset, rows)
				if err != nil {
					panic(err)
				}
			}); n != 0 {
				t.Fatalf("cost helper allocated %v", n)
			}
		}
	}
	for _, nested := range []bool{false, true} {
		src := NewBatch()
		src.Rows = 1
		dst := src
		if nested {
			dst = NewBatch()
			dst.Rows = 1
			src.Structs["hidden"] = &StructVector{Rows: 1, Values: dst}
		}
		dst.Opaque["any"] = &OpaqueVector{Rows: 1, Value: &sharedUntouchedOpaque{}}
		got, e := EstimateNativeSharedBatchRange(src, 0, 1)
		if e != nil || got.Supported {
			t.Fatalf("fallback %+v %v", got, e)
		}
		if got, e := BorrowNativeWholeBatch(src); e == nil || got != nil {
			t.Fatal("Opaque bindings accepted")
		}
	}
}

func TestSharedNativeBaseAllowances(t *testing.T) {
	// Map's Go 1.27 runtime header is 48 bytes on this 64-bit target; use 64
	// for rounding. Empty maps have no payload groups; per-entry charges cover
	// nonempty groups separately. This check intentionally pins the model.
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("Go 1.27 64-bit allocation model")
	}
	if unsafe.Sizeof(Batch{}) > 160 {
		t.Fatalf("Batch grew to %d", unsafe.Sizeof(Batch{}))
	}
	base := int64(160 + 16*64)
	if base+NativeSharedOwnerAllowance+NativeSharedBindingOverheadAllowance > 4096 {
		t.Fatal("base cannot carry shared bookkeeping")
	}
}
