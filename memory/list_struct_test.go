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

// This fixture uses literal schema/storage, not the checked factory or copier.
// Parent and element coordinates deliberately differ.
func listStructTestRaw(t *testing.T) (Field, *StructVector, []uint32, Bitmap) {
	t.Helper()
	kinds := []DataType{TypeInt8, TypeInt16, TypeInt32, TypeInt64, TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeFloat32, TypeFloat64, TypeBool, TypeDate32, TypeTimestamp, TypeString}
	children := make([]Field, len(kinds))
	for i, k := range kinds {
		children[i] = Field{Name: k.String(), Type: k, Nullable: true}
		if k == TypeString {
			children[i].Encoding = EncodingView
		}
	}
	b := NewBatch()
	b.Rows = 8
	valid := func() Bitmap { return Bitmap{216} } // valid children at element3,4,6,7
	b.Int8s["int8"] = &Int8Vector{Values: []int8{81, 82, 83, -7, 8, 85, 86, -9}, Validity: valid()}
	b.Int16s["int16"] = &Int16Vector{Values: []int16{81, 82, 83, -700, 800, 85, 86, -900}, Validity: valid()}
	b.Int32s["int32"] = &Int32Vector{Values: []int32{81, 82, 83, -70000, 80000, 85, 86, -90000}, Validity: valid()}
	b.Int64s["int64"] = &Int64Vector{Values: []int64{81, 82, 83, -7, 9223372036854775807, 85, 86, -9223372036854775808}, Validity: valid()}
	b.Uint8s["uint8"] = &Uint8Vector{Values: []uint8{81, 82, 83, 7, 255, 85, 86, 9}, Validity: valid()}
	b.Uint16s["uint16"] = &Uint16Vector{Values: []uint16{81, 82, 83, 700, 65535, 85, 86, 900}, Validity: valid()}
	b.Uint32s["uint32"] = &Uint32Vector{Values: []uint32{81, 82, 83, 70000, 4294967295, 85, 86, 90000}, Validity: valid()}
	b.Uint64s["uint64"] = &Uint64Vector{Values: []uint64{81, 82, 83, 7, 18446744073709551615, 85, 86, 9}, Validity: valid()}
	b.Float32s["float32"] = &Float32Vector{Values: []float32{81, 82, 83, -7.25, 8.5, 85, 86, 9.75}, Validity: valid()}
	b.Float64s["float64"] = &Float64Vector{Values: []float64{81, 82, 83, -7.25, 8.5, 85, 86, 9.75}, Validity: valid()}
	b.Uint32s["bool"] = &Uint32Vector{Values: []uint32{0, 0, 0, 1, 0, 0, 1, 1}, Validity: valid()}
	b.Int32s["date32"] = &Int32Vector{Values: []int32{81, 82, 83, -1, 0, 85, 86, 20000}, Validity: valid()}
	b.Int64s["timestamp"] = &Int64Vector{Values: []int64{81, 82, 83, -1, 0, 85, 86, 9223372036854775807}, Validity: valid()}
	texts := []string{"list-hidden", "item-hidden", "null-child", "界", "", "child-hidden", "ancestor-hidden", "tail"}
	offsets := []uint32{0}
	var data []byte
	for _, s := range texts {
		data = append(data, []byte(s)...)
		offsets = append(offsets, uint32(len(data)))
	}
	b.Strings["string"] = &StringViewVector{Offsets: offsets, Data: data, Validity: valid(), Encoding: EncodingView}
	item := Field{Name: "item", Type: TypeStruct, Nullable: true, Encoding: EncodingView, Children: children}
	field := Field{Name: "items", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{item}}
	values := &StructVector{Rows: 8, Fields: children, Values: b, Validity: Bitmap{253}, Encoding: EncodingView}
	return field, values, []uint32{0, 1, 1, 2, 3, 6, 7, 8}, Bitmap{126}
}

func listStructTestColumn(b *Batch, f Field) reflect.Value {
	var vector any
	switch f.Type {
	case TypeInt8:
		vector = b.Int8s[f.Name]
	case TypeInt16:
		vector = b.Int16s[f.Name]
	case TypeInt32, TypeDate32:
		vector = b.Int32s[f.Name]
	case TypeInt64, TypeTimestamp:
		vector = b.Int64s[f.Name]
	case TypeUint8:
		vector = b.Uint8s[f.Name]
	case TypeUint16:
		vector = b.Uint16s[f.Name]
	case TypeUint32, TypeBool:
		vector = b.Uint32s[f.Name]
	case TypeUint64:
		vector = b.Uint64s[f.Name]
	case TypeFloat32:
		vector = b.Float32s[f.Name]
	case TypeFloat64:
		vector = b.Float64s[f.Name]
	case TypeString:
		vector = b.Strings[f.Name]
	}
	return reflect.ValueOf(vector)
}

// Values are independent literal expectations for the only visible source
// elements 3,4,7; no copy/decoder is used as the oracle.
func listStructTestCheckElements(t *testing.T, v *StructVector, sourceElements []int) {
	t.Helper()
	if v.Rows != len(sourceElements) || v.Values.Rows != len(sourceElements) {
		t.Fatal("element row shape")
	}
	for row, e := range sourceElements {
		if v.IsValid(row) != (e != 1) {
			t.Fatal("NULL element vs all-null record", row, e)
		}
		valid := e == 3 || e == 4 || e == 7
		which := 0
		if e == 4 {
			which = 1
		}
		if e == 7 {
			which = 2
		}
		for _, f := range v.Fields {
			ptr := listStructTestColumn(v.Values, f)
			if !ptr.IsValid() || ptr.IsNil() {
				t.Fatal("missing copied leaf", f.Name)
			}
			if ptr.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(row)})[0].Bool() != valid {
				t.Fatal("child validity", row, e, f.Name)
			}
			if !valid {
				continue
			}
			if f.Type == TypeString {
				want := []string{"界", "", "tail"}[which]
				if string(v.Values.Strings[f.Name].ValueBytes(row)) != want {
					t.Fatal("String oracle", row, e)
				}
				continue
			}
			x := ptr.Elem().FieldByName("Values").Index(row)
			switch f.Type {
			case TypeFloat32, TypeFloat64:
				if x.Float() != []float64{-7.25, 8.5, 9.75}[which] {
					t.Fatal("float oracle", f.Name, row)
				}
			case TypeUint8, TypeUint16, TypeUint32, TypeUint64, TypeBool:
				want := map[DataType][]uint64{TypeUint8: {7, 255, 9}, TypeUint16: {700, 65535, 900}, TypeUint32: {70000, 4294967295, 90000}, TypeUint64: {7, 18446744073709551615, 9}, TypeBool: {1, 0, 1}}[f.Type][which]
				if x.Uint() != want {
					t.Fatal("unsigned oracle", f.Name, row)
				}
			default:
				want := map[DataType][]int64{TypeInt8: {-7, 8, -9}, TypeInt16: {-700, 800, -900}, TypeInt32: {-70000, 80000, -90000}, TypeInt64: {-7, 9223372036854775807, -9223372036854775808}, TypeDate32: {-1, 0, 20000}, TypeTimestamp: {-1, 0, 9223372036854775807}}[f.Type][which]
				if x.Int() != want {
					t.Fatal("signed/temporal oracle", f.Name, row)
				}
			}
		}
	}
}

func listStructTestFixture(t *testing.T) (Field, *StructListVector, StructListParentDomain) {
	t.Helper()
	f, values, offsets, validity := listStructTestRaw(t)
	d, e := NewStructListParentDomain(7, Bitmap{95})
	if e != nil {
		t.Fatal(e)
	}
	return f, &StructListVector{Offsets: offsets, Validity: validity, Values: values, Encoding: EncodingView}, d
}
func listStructTestCheck(t *testing.T, out *OwnedStructList, rows []int) {
	t.Helper()
	v := out.Vector
	if len(v.Offsets) != len(rows)+1 || v.Offsets[0] != 0 {
		t.Fatal("parent offsets shape")
	}
	spans := [][]int{nil, nil, {1}, {2}, {3, 4, 5}, nil, {7}}
	var elements []int
	for i, row := range rows {
		valid := row != 0 && row != 5
		if v.IsValid(i) != valid {
			t.Fatal("parent/domain validity", i, row)
		}
		elements = append(elements, spans[row]...)
		if v.Offsets[i+1] != uint32(len(elements)) {
			t.Fatal("canonical offset", i, row)
		}
	}
	listStructTestCheckElements(t, v.Values, elements)
	if len(v.Values.Fields) != 14 || len(out.Field.Children[0].Children) != 14 {
		t.Fatal("all14 leaves copied")
	}
	if !out.Field.Nullable || !out.Field.Children[0].Nullable {
		t.Fatal("output metadata")
	}
}
func TestStructListOwnedAllFourteenKinds(t *testing.T) {
	selections := [][]int{nil, {0, 1, 2, 3, 4, 5, 6}, {6, 4, 2, 4, 0, 5, 1, 3}, {4}, {3}, {2}}
	for i, rows := range selections {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			f, v, d := listStructTestFixture(t)
			m := NewBudgetManager(0)
			out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
			if e != nil {
				t.Fatal(e)
			}
			listStructTestCheck(t, out, rows)
			charge := out.Bytes
			if charge <= 0 || m.Used() != charge {
				t.Fatal("owner charge", charge, m.Used())
			}
			// Poison every source buffer and metadata after copy; literal oracle survives.
			for _, field := range v.Values.Fields {
				ptr := listStructTestColumn(v.Values.Values, field)
				if field.Type == TypeString {
					for j := range v.Values.Values.Strings[field.Name].Data {
						v.Values.Values.Strings[field.Name].Data[j] = '!'
					}
				} else {
					a := ptr.Elem().FieldByName("Values")
					for j := 0; j < a.Len(); j++ {
						a.Index(j).SetZero()
					}
				}
			}
			v.Offsets[1] = 999
			v.Values.Fields[0].Name = "poison"
			f.Children[0].Children[1].Name = "poison2"
			listStructTestCheck(t, out, rows)
			out.Close()
			out.Close()
			if m.Used() != 0 || out.Vector != nil {
				t.Fatal("Close lifecycle")
			}
			f, v, d = listStructTestFixture(t)
			exact := NewBudgetManager(charge)
			out, e = TakeStructListRowsOwned(context.Background(), exact, v, f, d, rows)
			if e != nil {
				t.Fatal("exact", e)
			}
			out.Close()
			if exact.Used() != 0 {
				t.Fatal("exact release")
			}
			short := NewBudgetManager(charge - 1)
			out, e = TakeStructListRowsOwned(context.Background(), short, v, f, d, rows)
			if out != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
				t.Fatal("one byte short", out, e, short.Used())
			}
		})
	}
}
func TestStructListOwnedRangeAndZero(t *testing.T) {
	for _, span := range [][2]int{{0, 7}, {2, 4}, {4, 1}, {7, 0}, {0, 0}} {
		t.Run(fmt.Sprint(span), func(t *testing.T) {
			f, v, d := listStructTestFixture(t)
			m := NewBudgetManager(0)
			out, e := CloneStructListRangeOwned(context.Background(), m, v, f, d, span[0], span[1])
			if e != nil {
				t.Fatal(e)
			}
			rows := make([]int, span[1])
			for i := range rows {
				rows[i] = span[0] + i
			}
			listStructTestCheck(t, out, rows)
			out.Close()
			if m.Used() != 0 {
				t.Fatal("range leak")
			}
		})
	}
	f, v, _ := listStructTestFixture(t)
	v.Offsets = []uint32{0}
	v.Validity = nil
	v.Values.Rows = 0
	v.Values.Validity = nil
	v.Values.Values.Rows = 0
	for _, field := range v.Values.Fields {
		ptr := listStructTestColumn(v.Values.Values, field)
		if field.Type == TypeString {
			s := v.Values.Values.Strings[field.Name]
			s.Offsets = []uint32{0}
			s.Data = nil
			s.Validity = nil
		} else {
			a := ptr.Elem().FieldByName("Values")
			a.Set(a.Slice(0, 0))
			ptr.Elem().FieldByName("Validity").Set(reflect.Zero(reflect.TypeOf(Bitmap{})))
		}
	}
	d, e := NewStructListParentDomain(0)
	if e != nil {
		t.Fatal(e)
	}
	m := NewBudgetManager(0)
	out, e := CloneStructListRangeOwned(context.Background(), m, v, f, d, 0, 0)
	if e != nil {
		t.Fatal(e)
	}
	listStructTestCheck(t, out, nil)
	out.Close()
	if m.Used() != 0 {
		t.Fatal("zero input leak")
	}
}

func TestStructListCheckedSchemaAndDepth(t *testing.T) {
	raw, _, _, _ := listStructTestRaw(t)
	f, e := ListStructField("items", true, true, raw.Children[0].Children...)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.ValidateListStruct(); e != nil {
		t.Fatal(e)
	}
	if e = f.ValidateList(); e == nil {
		t.Fatal("legacy primitive List validator broadened")
	}
	if _, e = NewSchemaChecked(f); e == nil {
		t.Fatal("legacy checked schema broadened")
	}
	if !reflect.DeepEqual(f, raw) {
		t.Fatal("factory metadata", f, raw)
	}
	raw.Children[0].Children[0].Name = "mutated"
	if f.Children[0].Children[0].Name != "int8" {
		t.Fatal("factory aliases children")
	}
	for _, outer := range []int{0, 1, 7, 8} {
		t.Run(fmt.Sprintf("outer%d", outer), func(t *testing.T) {
			field := f
			for i := 0; i < outer; i++ {
				field = StructField("root", true, field)
			}
			s, e := NewSchemaWithStructListsChecked(field)
			if outer == 8 {
				if e == nil {
					t.Fatal("ninth Struct accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			cursor := &field
			for i := 0; i < outer; i++ {
				cursor = &cursor.Children[0]
			}
			cursor.Children[0].Children[0].Name = "poison"
			got := s.Fields[0]
			for i := 0; i < outer; i++ {
				got = got.Children[0]
			}
			if got.Children[0].Children[0].Name != "int8" {
				t.Fatal("checked deep snapshot")
			}
			f, _, _, _ = listStructTestRaw(t)
		})
	}
	for _, typ := range []DataType{TypeDate64, TypeTime32, TypeTime64, TypeDuration, TypeDecimal, TypeBinary, TypeArrow, TypeList, TypeStruct} {
		if _, e = ListStructField("x", true, true, Field{Name: "bad", Type: typ}); e == nil {
			t.Fatal("unsupported element leaf", typ)
		}
	}
	if _, e = ListStructField("", true, true, Field{Name: "n", Type: TypeInt64}); e == nil {
		t.Fatal("empty List name accepted")
	}
	if _, e = ListStructField("x", true, true, Field{Name: "", Type: TypeInt64}); e == nil {
		t.Fatal("empty child name accepted")
	}
	if _, e = ListStructField("x", true, true); e == nil {
		t.Fatal("empty element accepted")
	}
	if _, e = ListStructField("x", true, true, Field{Name: "n", Type: TypeInt64}, Field{Name: "n", Type: TypeInt64}); e == nil {
		t.Fatal("duplicate child accepted")
	}
	children := make([]Field, 1)
	cycle := Field{Name: "cycle", Type: TypeStruct, Children: children}
	children[0] = cycle
	if _, e = NewSchemaWithStructListsChecked(cycle); e == nil {
		t.Fatal("schema cycle accepted")
	}
	for _, count := range []int{0, 1, 7, 8} {
		bits := make([]Bitmap, count)
		_, e = NewStructListParentDomain(7, bits...)
		if (count == 8) != (e != nil) {
			t.Fatal("domain depth", count, e)
		}
	}
}

func listStructRequiredFixture(t *testing.T) (Field, *StructListVector, StructListParentDomain) {
	t.Helper()
	f, e := ListStructField("items", true, false, Field{Name: "n", Type: TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 3
	b.Int64s["n"] = &Int64Vector{Values: []int64{91, 5, 92}, Validity: Bitmap{2}}
	v := &StructListVector{Offsets: []uint32{0, 1, 2, 3}, Validity: Bitmap{6}, Encoding: EncodingView, Values: &StructVector{Rows: 3, Fields: f.Children[0].Children, Values: b, Validity: Bitmap{2}, Encoding: EncodingView}}
	d, e := NewStructListParentDomain(3, Bitmap{3})
	if e != nil {
		t.Fatal(e)
	}
	return f, v, d
}
func TestStructListRequiredParentElementDomains(t *testing.T) {
	f, v, d := listStructRequiredFixture(t)
	if e := v.ValidateWithContext(f, 3, d, nil); e != nil {
		t.Fatal("hidden required item/child", e)
	}
	m := NewBudgetManager(0)
	out, e := CloneStructListRangeOwned(context.Background(), m, v, f, d, 0, 3)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(out.Vector.Offsets, []uint32{0, 0, 1, 1}) || !out.Vector.IsValid(1) || out.Vector.IsValid(0) || out.Vector.IsValid(2) || out.Vector.Values.Values.Int64s["n"].Values[0] != 5 {
		t.Fatal("parent-to-element domain mapping")
	}
	out.Close()
	v.Validity = Bitmap{7}
	if e = v.ValidateWithContext(f, 3, d, nil); e == nil {
		t.Fatal("visible required NULL item accepted")
	}
	// Unselected invalid required item must not poison selected row1.
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{1, 1})
	if e != nil {
		t.Fatal("selected domain", e)
	}
	out.Close()
	v.Values.Validity = Bitmap{3}
	if e = v.ValidateWithContext(f, 3, d, nil); e == nil {
		t.Fatal("visible required child NULL accepted")
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{0})
	if out != nil || e == nil || m.Used() != 0 {
		t.Fatal("selected required child accepted", out, e, m.Used())
	}
	f.Nullable = false
	d, e = NewStructListParentDomain(3, Bitmap{2})
	if e != nil {
		t.Fatal(e)
	}
	v.Validity = Bitmap{6}
	if e = v.ValidateWithContext(f, 3, d, nil); e != nil {
		t.Fatal("ancestor hides required NULL list", e)
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{0, 1, 2})
	if e != nil {
		t.Fatal(e)
	}
	if !out.Field.Nullable || out.Field.Children[0].Nullable || out.Field.Children[0].Children[0].Nullable {
		t.Fatal("only parent nullable widened")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("domain cleanup")
	}
}

func TestStructListMalformedStorageHiddenAndUnselected(t *testing.T) {
	cases := map[string]func(*Field, *StructListVector){
		"nonzero_base":              func(_ *Field, v *StructListVector) { v.Offsets[0] = 1 },
		"decreasing":                func(_ *Field, v *StructListVector) { v.Offsets[1] = 2 },
		"short_offsets":             func(_ *Field, v *StructListVector) { v.Offsets = v.Offsets[:7] },
		"terminal":                  func(_ *Field, v *StructListVector) { v.Offsets[7] = 7 },
		"uint32_terminal":           func(_ *Field, v *StructListVector) { v.Offsets[7] = ^uint32(0) },
		"element_rows":              func(_ *Field, v *StructListVector) { v.Values.Rows = 9 },
		"child_rows":                func(_ *Field, v *StructListVector) { v.Values.Values.Rows = 9 },
		"short_child":               func(_ *Field, v *StructListVector) { v.Values.Values.Int64s["int64"].Values = nil },
		"hidden_bad_string_offsets": func(_ *Field, v *StructListVector) { v.Values.Values.Strings["string"].Offsets[1] = 1000 },
		"field_type": func(_ *Field, v *StructListVector) {
			v.Values.Fields = append([]Field(nil), v.Values.Fields...)
			v.Values.Fields[0].Type = TypeFloat64
		},
		"element_shape": func(f *Field, _ *StructListVector) { f.Children[0].Name = "wrong" },
		"extra_scalar":  func(_ *Field, v *StructListVector) { v.Values.Values.Int8s["undeclared"] = &Int8Vector{} },
		"runtime_cycle": func(_ *Field, v *StructListVector) { v.Values.Values.Structs["cycle"] = v.Values },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f, v, d := listStructTestFixture(t)
			mutate(&f, v)
			if e := v.ValidateWithContext(f, 7, d, nil); e == nil {
				t.Fatal("malformed full validation accepted")
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			for _, rows := range [][]int{nil, {6}} {
				out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
				if out != nil || e == nil || m.calls != 0 || m.Used() != 0 {
					t.Fatal("empty/unselected concealed bad storage", out, e, m.calls, m.Used())
				}
			}
		})
	}
	f, e := ListStructField("items", true, true, Field{Name: "n", Type: TypeInt64, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	b := NewBatch()
	b.Rows = 65
	b.Int64s["n"] = &Int64Vector{Values: make([]int64, 65)}
	v := &StructListVector{Offsets: []uint32{0, 65}, Values: &StructVector{Rows: 65, Values: b, Fields: f.Children[0].Children, Validity: Bitmap{^uint64(0)}}}
	d, e := NewStructListParentDomain(1)
	if e != nil {
		t.Fatal(e)
	}
	if e = v.ValidateWithContext(f, 1, d, nil); e == nil {
		t.Fatal("short element validity accepted")
	}
	if _, e = NewStructListParentDomain(65, Bitmap{1}); e == nil {
		t.Fatal("short ancestor validity accepted")
	}
}

func TestStructListBoundsAndAdmissionRollback(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	for _, rows := range [][]int{{-1}, {7}} {
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
		out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
		if out != nil || e == nil || m.calls != 0 {
			t.Fatal("invalid selection allocated", out, e, m.calls)
		}
	}
	for _, span := range [][2]int{{-1, 1}, {0, -1}, {7, 1}, {1, int(^uint(0) >> 1)}} {
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
		out, e := CloneStructListRangeOwned(context.Background(), m, v, f, d, span[0], span[1])
		if out != nil || e == nil || m.calls != 0 {
			t.Fatal("invalid range allocated", out, e, m.calls)
		}
	}
	probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	out, e := TakeStructListRowsOwned(context.Background(), probe, v, f, d, []int{4, 6, 4})
	if e != nil {
		t.Fatal(e)
	}
	calls := probe.calls
	charge := out.Bytes
	out.Close()
	if calls == 0 {
		t.Fatal("unbudgeted owned output")
	}
	for fail := 1; fail <= calls; fail++ {
		m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
		out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4, 6, 4})
		if out != nil || !errors.Is(e, ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("reserve failure", fail, out, e, m.Used())
		}
		m.failAt = 0
		out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4, 6, 4})
		if e != nil {
			t.Fatal("retry", e)
		}
		listStructTestCheck(t, out, []int{4, 6, 4})
		out.Close()
	}
	// The old owner remains charged and readable while another owner is refused.
	m := NewBudgetManager(charge)
	old, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4, 6, 4})
	if e != nil {
		t.Fatal(e)
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4, 6, 4})
	if out != nil || !errors.Is(e, ErrBudgetExceeded) || m.Used() != charge {
		t.Fatal("live old owner accounting", out, e, m.Used())
	}
	listStructTestCheck(t, old, []int{4, 6, 4})
	old.Close()
	if m.Used() != 0 {
		t.Fatal("old owner cleanup")
	}
}

type listStructTestCancelContext struct {
	context.Context
	manager         Manager
	target          string
	checks, stop    int
	chargedAtCancel int64
	canceled        bool
}

func (c *listStructTestCancelContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, c.target) {
			c.checks++
			if c.checks == c.stop {
				c.canceled = true
				if c.manager != nil {
					c.chargedAtCancel = c.manager.Used()
				}
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
func listStructTestLong(t *testing.T) (Field, *StructListVector, StructListParentDomain, string) {
	t.Helper()
	f, e := ListStructField("items", false, false, Field{Name: "s", Type: TypeString})
	if e != nil {
		t.Fatal(e)
	}
	payload := "xx" + strings.Repeat("界", 44000) + "z"
	b := NewBatch()
	b.Rows = 1
	b.Strings["s"] = &StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: []byte(payload), Encoding: EncodingView}
	v := &StructListVector{Offsets: []uint32{0, 1}, Values: &StructVector{Rows: 1, Fields: f.Children[0].Children, Values: b, Encoding: EncodingView}, Encoding: EncodingView}
	d, e := NewStructListParentDomain(1)
	if e != nil {
		t.Fatal(e)
	}
	return f, v, d, payload
}
func TestStructListOwnedCancelAfterAdmission(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), cancelAt: 1, cancel: cancel}
	out, e := TakeStructListRowsOwned(ctx, m, v, f, d, []int{4, 6})
	if out != nil || !errors.Is(e, context.Canceled) || m.calls != 1 || m.Used() != 0 {
		t.Fatal("cancel after real Reserve", out, e, m.calls, m.Used())
	}
	m.cancelAt = 0
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4, 6})
	if e != nil {
		t.Fatal(e)
	}
	listStructTestCheck(t, out, []int{4, 6})
	out.Close()
	if m.Used() != 0 {
		t.Fatal("cancel retry cleanup")
	}
}

func TestStructListParentDomainIntersectionAndRowMismatch(t *testing.T) {
	f, v, _ := listStructRequiredFixture(t)
	// Seven ancestors include required nil bitmaps. All must use parent row1,
	// not the element row that will become output index0.
	d, e := NewStructListParentDomain(3, nil, Bitmap{3}, nil, Bitmap{6}, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = v.ValidateWithContext(f, 3, d, nil); e != nil {
		t.Fatal(e)
	}
	m := NewBudgetManager(0)
	out, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{2, 1, 0, 1})
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(out.Vector.Offsets, []uint32{0, 0, 1, 1, 2}) || !reflect.DeepEqual(out.Vector.Values.Values.Int64s["n"].Values, []int64{5, 5}) {
		t.Fatal("ancestor domain coordinate/intersection")
	}
	out.Close()
	wrong, e := NewStructListParentDomain(4, Bitmap{15})
	if e != nil {
		t.Fatal(e)
	}
	if e = v.ValidateWithContext(f, 3, wrong, nil); e == nil {
		t.Fatal("wrong parent domain length accepted")
	}
	short := &StructListVector{Offsets: make([]uint32, 66), Validity: Bitmap{1}, Values: &StructVector{Rows: 0, Fields: f.Children[0].Children, Values: NewBatch()}}
	short.Values.Values.Int64s["n"] = &Int64Vector{}
	if e = short.ValidateWithContext(f, 65, StructListParentDomain{}, nil); e == nil {
		t.Fatal("short parent bitmap accepted")
	}
}

func TestStructListOwnedSharedChildOccurrences(t *testing.T) {
	var previous int64
	for _, width := range []int{1, 2, 32} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			children := make([]Field, width)
			b := NewBatch()
			b.Rows = 1
			shared := &Int64Vector{Values: []int64{42}}
			for i := range children {
				children[i] = Field{Name: fmt.Sprintf("n%d", i), Type: TypeInt64}
				b.Int64s[children[i].Name] = shared
			}
			f, e := ListStructField("items", false, false, children...)
			if e != nil {
				t.Fatal(e)
			}
			v := &StructListVector{Offsets: []uint32{0, 1}, Values: &StructVector{Rows: 1, Values: b, Fields: children}}
			m := NewBudgetManager(0)
			out, e := CloneStructListRangeOwned(context.Background(), m, v, f, StructListParentDomain{}, 0, 1)
			if e != nil {
				t.Fatal(e)
			}
			if out.Bytes <= previous || m.Used() != out.Bytes {
				t.Fatal("occurrence metadata/values uncharged", out.Bytes, previous)
			}
			previous = out.Bytes
			shared.Values[0] = -1
			for i := range children {
				v := out.Vector.Values.Values.Int64s[children[i].Name]
				if v.Values[0] != 42 {
					t.Fatal("shared source aliases owner")
				}
				if i > 0 && &v.Values[0] == &out.Vector.Values.Values.Int64s["n0"].Values[0] {
					t.Fatal("copied occurrences alias")
				}
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("shared cleanup")
			}
		})
	}
}

func TestStructListOwnedActualChunkCancelAndRetry(t *testing.T) {
	f, v, d, payload := listStructTestLong(t)
	m := NewBudgetManager(0)
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".appendRecursiveString", stop: 2}
	out, e := TakeStructListRowsOwned(ctx, m, v, f, d, []int{0, 0})
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.chargedAtCancel <= 0 || m.Used() != 0 {
		t.Fatal("second actual 32KiB copy checkpoint", out, e, ctx.checks, m.Used())
	}
	if string(v.Values.Values.Strings["s"].Data) != payload {
		t.Fatal("failed copy mutated input")
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{0, 0})
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(out.Vector.Offsets, []uint32{0, 1, 2}) {
		t.Fatal("duplicate parent offsets")
	}
	for row := 0; row < 2; row++ {
		if string(out.Vector.Values.Values.Strings["s"].ValueBytes(row)) != payload {
			t.Fatal("retry duplicate bytes")
		}
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("copy retry cleanup")
	}
}
func TestStructListSelectedPreflightCancellation(t *testing.T) {
	f, v, d := listStructRequiredFixture(t)
	rows := make([]int, 1024)
	for i := range rows {
		rows[i] = 1
	}
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	ctx := &listStructTestCancelContext{Context: context.Background(), target: ".validateParent", stop: 100}
	out, e := TakeStructListRowsOwned(ctx, m, v, f, d, rows)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 100 || m.calls != 0 || m.Used() != 0 {
		t.Fatal("selected value preflight cancel", out, e, ctx.checks, m.calls, m.Used())
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
	if e != nil {
		t.Fatal(e)
	}
	if out.Vector.Values.Rows != 1024 || out.Vector.Values.Values.Int64s["n"].Values[1023] != 5 {
		t.Fatal("preflight retry")
	}
	out.Close()
	if m.Used() != 0 {
		t.Fatal("preflight retry cleanup")
	}
}
func TestStructListCheckedCountArithmetic(t *testing.T) {
	// Direct arithmetic guards only: synthetic spans are not presented as valid
	// physical storage and no huge buffer is allocated.
	if structListCountOK(-1) || structListCountOK(int(^uint(0)>>1)) {
		t.Fatal("host bounds accepted")
	}
	if uint64(^uint(0)>>1) > uint64(^uint32(0)) && structListCountOK(int(uint64(^uint32(0)))) {
		t.Fatal("offset count overflow accepted")
	}
	v := &StructListVector{Offsets: []uint32{0, ^uint32(0)}}
	if _, e := structListSelectedElements(v, StructListParentDomain{}, 1, func(int) int { return 0 }, nil); e == nil {
		t.Fatal("selected element total overflow accepted")
	}
	b := NewBatch()
	b.Rows = 1
	b.Strings["s"] = &StringViewVector{Offsets: []uint32{0, ^uint32(0)}}
	v.Offsets = []uint32{0, 1}
	v.Values = &StructVector{Rows: 1, Values: b}
	if _, e := structListStringBytes(v, StructListParentDomain{}, Field{Name: "s", Type: TypeString}, 2, func(int) int { return 0 }, nil); e == nil {
		t.Fatal("repeated selected String byte overflow accepted")
	}
}

func TestStructListOwnedElementBitmapBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 63, 64, 65, 129, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f, e := ListStructField("items", false, true, Field{Name: "n", Type: TypeInt64, Nullable: true}, Field{Name: "s", Type: TypeString, Nullable: true})
			if e != nil {
				t.Fatal(e)
			}
			b := NewBatch()
			b.Rows = count
			ints := &Int64Vector{Values: make([]int64, count), Validity: make(Bitmap, WordsFor(count))}
			ss := &StringViewVector{Offsets: []uint32{0}, Validity: make(Bitmap, WordsFor(count)), Encoding: EncodingView}
			elementValid := make(Bitmap, WordsFor(count))
			for i := 0; i < count; i++ {
				ints.Values[i] = int64(i*11 + 5)
				ss.Data = append(ss.Data, []byte(fmt.Sprintf("e%d", i))...)
				ss.Offsets = append(ss.Offsets, uint32(len(ss.Data)))
				if i%3 != 0 {
					Set(elementValid, i)
				}
				if i%5 != 0 {
					Set(ints.Validity, i)
					Set(ss.Validity, i)
				}
			}
			b.Int64s["n"], b.Strings["s"] = ints, ss
			v := &StructListVector{Offsets: []uint32{0, uint32(count)}, Values: &StructVector{Rows: count, Values: b, Fields: f.Children[0].Children, Validity: elementValid}}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			if count == 4097 {
				ctx := &listStructTestCancelContext{Context: context.Background(), target: ".validateParent", stop: 100}
				out, e := TakeStructListRowsOwned(ctx, m, v, f, StructListParentDomain{}, []int{0})
				if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 100 || m.calls != 0 || m.Used() != 0 {
					t.Fatal("long single-parent element scan cancellation", out, e, ctx.checks, m.calls, m.Used())
				}
			}
			out, e := TakeStructListRowsOwned(context.Background(), m, v, f, StructListParentDomain{}, []int{0, 0})
			if e != nil {
				t.Fatal(e)
			}
			if out.Field.Nullable || out.Vector.Values.Rows != count*2 || !reflect.DeepEqual(out.Vector.Offsets, []uint32{0, uint32(count), uint32(count * 2)}) {
				t.Fatal("element cardinality/metadata")
			}
			for row := 0; row < count*2; row++ {
				i := row % count
				pv := i%3 != 0
				cv := pv && i%5 != 0
				got := out.Vector.Values
				if got.IsValid(row) != pv || got.Values.Int64s["n"].IsValid(row) != cv || got.Values.Strings["s"].IsValid(row) != cv {
					t.Fatal("bitmap boundary", count, row)
				}
				wantN := int64(0)
				wantS := ""
				if cv {
					wantN = int64(i*11 + 5)
					wantS = fmt.Sprintf("e%d", i)
				}
				if got.Values.Int64s["n"].Values[row] != wantN || string(got.Values.Strings["s"].ValueBytes(row)) != wantS {
					t.Fatal("hidden value normalization", count, row)
				}
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("bitmap cleanup")
			}
		})
	}
}

func TestStructListSafeAccessorsEncodingAndMetadataCancel(t *testing.T) {
	var nilVector *StructListVector
	for _, v := range []*StructListVector{nilVector, {}, {Offsets: []uint32{0, 0}}} {
		for _, row := range []int{-1, 1, int(^uint(0) >> 1)} {
			if v.IsValid(row) {
				t.Fatal("invalid IsValid true")
			}
			a, b, valid := v.ValueRange(row)
			if a != 0 || b != 0 || valid {
				t.Fatal("invalid range", a, b, valid)
			}
		}
	}
	for _, enc := range []Encoding{EncodingDictionary, EncodingConstant, Encoding(255)} {
		for _, at := range []string{"list", "item", "leaf"} {
			f, _, _, _ := listStructTestRaw(t)
			switch at {
			case "list":
				f.Encoding = enc
			case "item":
				f.Children[0].Encoding = enc
			case "leaf":
				f.Children[0].Children[0].Encoding = enc
			}
			if e := f.ValidateListStruct(); e == nil {
				t.Fatal("unsupported metadata encoding", enc, at)
			}
		}
		f, v, d := listStructTestFixture(t)
		v.Encoding = enc
		if e := v.ValidateWithContext(f, 7, d, nil); e == nil {
			t.Fatal("unsupported parent storage encoding", enc)
		}
		f, v, d = listStructTestFixture(t)
		v.Values.Encoding = enc
		if e := v.ValidateWithContext(f, 7, d, nil); e == nil {
			t.Fatal("unsupported item storage encoding", enc)
		}
	}
	f, v, d := listStructTestFixture(t)
	m := NewBudgetManager(0)
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".cloneRecursiveFieldContext", stop: 3}
	out, e := TakeStructListRowsOwned(ctx, m, v, f, d, []int{4})
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 3 || ctx.chargedAtCancel <= 0 || m.Used() != 0 {
		t.Fatal("paid metadata snapshot cancel", out, e, ctx.checks, ctx.chargedAtCancel, m.Used())
	}
	out, e = TakeStructListRowsOwned(context.Background(), m, v, f, d, []int{4})
	if e != nil {
		t.Fatal(e)
	}
	listStructTestCheck(t, out, []int{4})
	out.Close()
	if !reflect.DeepEqual(out.Field, Field{}) || out.Vector != nil || m.Used() != 0 {
		t.Fatal("Close retains metadata/owner")
	}
}
