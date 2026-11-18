package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

type structGetKind struct {
	typ    memory.DataType
	vector string
	sample any
	list   bool
}

var structGetKinds = []structGetKind{
	{memory.TypeInt8, "Int8", int8(-7), false}, {memory.TypeInt16, "Int16", int16(-301), false}, {memory.TypeInt32, "Int32", int32(-70001), false}, {memory.TypeInt64, "Int64", int64(-9007199254740993), false},
	{memory.TypeUint8, "Uint8", uint8(201), false}, {memory.TypeUint16, "Uint16", uint16(60001), false}, {memory.TypeUint32, "Uint32", uint32(4000000001), false}, {memory.TypeUint64, "Uint64", uint64(18446744073709551615), false},
	{memory.TypeFloat32, "Float32", float32(1.25), false}, {memory.TypeFloat64, "Float64", float64(-1.125), false}, {memory.TypeBool, "Uint32", uint32(1), false}, {memory.TypeString, "String", nil, false},
	{memory.TypeDate32, "Int32", int32(-719), false}, {memory.TypeDate64, "Int64", int64(-86400000), false}, {memory.TypeTimestamp, "Int64", int64(-123456789), false}, {memory.TypeTime32, "Int32", int32(4321), false}, {memory.TypeTime64, "Int64", int64(123456789), false}, {memory.TypeDuration, "Int64", int64(-456789), false},
	{memory.TypeList, "StringList", nil, true}, {memory.TypeList, "Int64List", nil, true},
}

func structGetFixture(k structGetKind, n int, pnull, cnull bool) (memory.Schema, *memory.Batch) {
	child := memory.Field{Name: "a.b:数", Type: k.typ, Nullable: cnull}
	if k.list {
		if k.vector == "StringList" {
			child = memory.ListStringField(child.Name, cnull, true)
		} else {
			child = memory.ListInt64Field(child.Name, cnull, true)
		}
	}
	parent := memory.StructField("r", pnull, child)
	b := memory.NewBatch()
	b.Rows = n
	children := memory.NewBatch()
	children.Rows = n
	pv, cv := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
	for i := 0; i < n; i++ {
		if !pnull || i%5 != 0 {
			memory.Set(pv, i)
		}
		if !cnull || i%3 != 0 {
			memory.Set(cv, i)
		}
	}
	if !pnull {
		pv = nil
	}
	if !cnull {
		cv = nil
	}
	switch k.vector {
	case "String":
		offsets := make([]uint32, n+1)
		var data []byte
		for i := 0; i < n; i++ {
			data = append(data, []byte(fmt.Sprintf("值-%d", i))...)
			offsets[i+1] = uint32(len(data))
		}
		children.Strings[child.Name] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: cv}
	case "StringList", "Int64List":
		offsets := make([]uint32, n+1)
		for i := 0; i < n; i++ {
			offsets[i+1] = offsets[i] + uint32(i%3)
		}
		count := int(offsets[n])
		ev := make(memory.Bitmap, memory.WordsFor(count))
		for j := 0; j < count; j++ {
			if j%2 == 0 {
				memory.Set(ev, j)
			}
		}
		if k.vector == "Int64List" {
			values := make([]int64, count)
			for j := range values {
				values[j] = int64(j + 17)
			}
			children.Int64Lists[child.Name] = &memory.Int64ListVector{Offsets: offsets, Validity: cv, Values: &memory.Int64Vector{Values: values, Validity: ev}}
		} else {
			eo := make([]uint32, count+1)
			var data []byte
			for j := 0; j < count; j++ {
				data = append(data, []byte(fmt.Sprintf("e%d", j))...)
				eo[j+1] = uint32(len(data))
			}
			children.StringLists[child.Name] = &memory.StringListVector{Offsets: offsets, Validity: cv, Values: &memory.StringViewVector{Offsets: eo, Data: data, Validity: ev}}
		}
	default:
		m := reflect.ValueOf(children).Elem().FieldByName(k.vector + "s")
		v := reflect.New(m.Type().Elem().Elem())
		values := reflect.MakeSlice(v.Elem().FieldByName("Values").Type(), n, n)
		for i := 0; i < n; i++ {
			values.Index(i).Set(reflect.ValueOf(k.sample))
		}
		v.Elem().FieldByName("Values").Set(values)
		v.Elem().FieldByName("Validity").Set(reflect.ValueOf(cv))
		m.SetMapIndex(reflect.ValueOf(child.Name), v)
	}
	b.Structs["r"] = &memory.StructVector{Rows: n, Fields: parent.Children, Values: children, Validity: pv, Encoding: memory.EncodingView}
	return memory.NewSchema(parent), b
}
func structGetAssert(t *testing.T, k structGetKind, got ScalarValue, n int, pnull, cnull bool) {
	t.Helper()
	if got.Type != k.typ {
		t.Fatalf("type=%s want%s", got.Type, k.typ)
	}
	vector := reflect.ValueOf(got).FieldByName(k.vector)
	if vector.IsNil() {
		t.Fatal("missing", k.vector)
	}
	childStart := 0
	for row := 0; row < n; row++ {
		valid := (!pnull || row%5 != 0) && (!cnull || row%3 != 0)
		gv := vector.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(row)})[0].Bool()
		if gv != valid {
			t.Fatalf("row%d validity%v want%v", row, gv, valid)
		}
		switch k.vector {
		case "String":
			want := ""
			if valid {
				want = fmt.Sprintf("值-%d", row)
			}
			if string(got.String.ValueBytes(row)) != want {
				t.Fatalf("row%d string", row)
			}
		case "Int64List", "StringList":
			var lo, hi uint32
			if k.vector == "Int64List" {
				lo, hi = got.Int64List.Offsets[row], got.Int64List.Offsets[row+1]
			} else {
				lo, hi = got.StringList.Offsets[row], got.StringList.Offsets[row+1]
			}
			count := 0
			if valid {
				count = row % 3
			}
			if int(hi-lo) != count {
				t.Fatalf("row%d list count", row)
			}
			for j := 0; j < count; j++ {
				original := childStart + j
				ev := original%2 == 0
				index := int(lo) + j
				if k.vector == "Int64List" {
					v := got.Int64List.Values
					if v.IsValid(index) != ev {
						t.Fatal("element validity")
					}
					want := int64(0)
					if ev {
						want = int64(original + 17)
					}
					if v.Values[index] != want {
						t.Fatal("element coefficient")
					}
				} else {
					v := got.StringList.Values
					if v.IsValid(index) != ev {
						t.Fatal("element validity")
					}
					want := ""
					if ev {
						want = fmt.Sprintf("e%d", original)
					}
					if string(v.ValueBytes(index)) != want {
						t.Fatal("element text")
					}
				}
			}
			childStart += row % 3
		default:
			value := vector.Elem().FieldByName("Values").Index(row).Interface()
			want := reflect.Zero(reflect.TypeOf(k.sample)).Interface()
			if valid {
				want = k.sample
			}
			if !reflect.DeepEqual(value, want) {
				t.Fatalf("row%d got%v want%v", row, value, want)
			}
		}
	}
}
func TestStructGetAllDirectChildren(t *testing.T) {
	for _, k := range structGetKinds {
		for _, pn := range []bool{false, true} {
			for _, cn := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s-%s/p%v/c%v", k.typ, k.vector, pn, cn), func(t *testing.T) {
					schema, _ := structGetFixture(k, 0, pn, cn)
					p, err := Compile(schema, StructGet(Col("r"), "a.b:数"))
					if err != nil {
						t.Fatal(err)
					}
					field := p.ResultField()
					child := schema.Fields[0].Children[0]
					if field.Type != k.typ || field.Nullable != (pn || cn) || !reflect.DeepEqual(field.Children, child.Children) {
						t.Fatalf("metadata %+v", field)
					}
					if p.SupportsSelection() {
						t.Fatal("struct selected fast path enabled")
					}
					if len(field.Children) > 0 {
						field.Children[0].Name = "mutated-result"
						if p.ResultField().Children[0].Name != "item" {
							t.Fatal("result metadata alias")
						}
					}

					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
						manager := memory.NewBudgetManager(0)
						w, err := NewWorkspace(context.Background(), p, manager)
						if err != nil {
							t.Fatal(err)
						}
						for _, n := range []int{0, 1, 63, 64, 65, 130, 1, 0} {
							_, b := structGetFixture(k, n, pn, cn)
							before, _ := json.Marshal(b)
							got, err := w.Evaluate(context.Background(), b, kernel.New(mode))
							if err != nil {
								t.Fatal(err)
							}
							structGetAssert(t, k, got[0], n, pn, cn)
							after, _ := json.Marshal(b)
							if string(before) != string(after) {
								t.Fatal("mutated input")
							}
							// Mutate borrowed source payload after the call. Owned output must survive.
							v := b.Structs["r"].Values
							switch k.vector {
							case "String":
								clear(v.Strings[child.Name].Data)
							case "Int64List":
								clear(v.Int64Lists[child.Name].Values.Values)
							case "StringList":
								clear(v.StringLists[child.Name].Values.Data)
							default:
								m := reflect.ValueOf(v).Elem().FieldByName(k.vector + "s")
								values := m.MapIndex(reflect.ValueOf(child.Name)).Elem().FieldByName("Values")
								values.Clear()
							}
							structGetAssert(t, k, got[0], n, pn, cn)
						}
						w.Close()
						if manager.Used() != 0 {
							t.Fatal("close leak")
						}
					}
				})
			}
		}
	}
}

func TestStructGetBindingAndStructuralErrors(t *testing.T) {
	k := structGetKinds[3]
	schema, _ := structGetFixture(k, 3, true, true)
	for _, name := range []string{"", "missing", "A.B:数", "a"} {
		if _, err := Compile(schema, StructGet(Col("r"), name)); err == nil {
			t.Fatal("accepted field", name)
		}
	}
	if _, err := Compile(memory.NewSchema(memory.Field{Name: "r", Type: memory.TypeInt64}), StructGet(Col("r"), "x")); err == nil {
		t.Fatal("accepted scalar parent")
	}
	dup := schema.Fields[0]
	dup.Children = append(append([]memory.Field(nil), dup.Children...), dup.Children[0])
	if _, err := Compile(memory.NewSchema(dup), StructGet(Col("r"), "a.b:数")); err == nil {
		t.Fatal("accepted duplicates")
	}
	nested := memory.StructField("r", true, memory.StructField("a.b:数", true, memory.Field{Name: "x", Type: memory.TypeInt64}))
	// N3 intentionally admits bounded recursive children; depth9 still rejects.
	if _, err := Compile(memory.NewSchema(nested), StructGet(Col("r"), "a.b:数")); err != nil {
		t.Fatal("N3 rejected recursive Struct child", err)
	}
	tooDeep := memory.StructField("n", false, memory.Field{Name: "x", Type: memory.TypeInt64})
	for d := 1; d < 9; d++ {
		tooDeep = memory.StructField("n", false, tooDeep)
	}
	if _, err := Compile(memory.NewSchema(tooDeep), StructGet(Col("n"), "n")); err == nil {
		t.Fatal("depth9 accepted")
	}
	unsupported := memory.StructField("r", false, memory.StructField("n", false, memory.Field{Name: "x", Type: memory.TypeDecimal, DecimalPrecision: 20, DecimalScale: 2}))
	if _, err := Compile(memory.NewSchema(unsupported), StructGet(Col("r"), "n")); err == nil {
		t.Fatal("recursive Decimal accepted")
	}

	p, err := Compile(schema, StructGet(Col("r"), "a.b:数"))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"parent_rows", "short_parent", "missing_child", "short_child", "required_null", "schema"} {
		t.Run(bad, func(t *testing.T) {
			sc, b := structGetFixture(k, 65, true, true)
			v := b.Structs["r"]
			switch bad {
			case "parent_rows":
				v.Rows--
			case "short_parent":
				v.Validity = memory.Bitmap{0}
			case "missing_child":
				delete(v.Values.Int64s, "a.b:数")
			case "short_child":
				v.Values.Int64s["a.b:数"].Values = nil
			case "required_null":
				sc.Fields[0].Children[0].Nullable = false
				v.Fields = sc.Fields[0].Children
			case "schema":
				v.Fields = append([]memory.Field(nil), v.Fields...)
				v.Fields[0].Name = "wrong"
			}
			testP := p
			if bad == "required_null" {
				testP, err = Compile(sc, StructGet(Col("r"), "a.b:数"))
				if err != nil {
					t.Fatal(err)
				}
			}
			m := memory.NewBudgetManager(0)
			w, _ := NewWorkspace(context.Background(), testP, m)
			_, err := w.Evaluate(context.Background(), b, testKernelSet())
			w.Close()
			if err == nil {
				t.Fatal("malformed parent accepted")
			}
			if m.Used() != 0 {
				t.Fatal("error leak")
			}
		})
	}
	// Whole-parent validation must reject malformed unselected sibling storage.
	sibling := memory.Field{Name: "other", Type: memory.TypeString, Nullable: true}
	schema, b := structGetFixture(k, 3, true, true)
	schema.Fields[0].Children = append(schema.Fields[0].Children, sibling)
	b.Structs["r"].Fields = schema.Fields[0].Children
	b.Structs["r"].Values.Strings["other"] = &memory.StringViewVector{Offsets: []uint32{0, 9, 0, 0}}
	p, err = Compile(schema, StructGet(Col("r"), "a.b:数"))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("bad sibling ignored")
	}
}
func TestStructGetHiddenDictionaryAndRequiredElements(t *testing.T) {
	field := memory.StructField("r", true, memory.Field{Name: "text", Type: memory.TypeString})
	child := memory.NewBatch()
	child.Rows = 2
	child.DictStrings["text"] = &memory.DictionaryStringVector{Indices: []uint32{999, 0}, Offsets: []uint32{0, 2}, Data: []byte("ok"), Encoding: memory.EncodingDictionary, DictionaryDomain: "source-domain"}
	b := memory.NewBatch()
	b.Rows = 2
	b.Structs["r"] = &memory.StructVector{Rows: 2, Fields: field.Children, Values: child, Validity: memory.Bitmap{2}}
	p, err := Compile(memory.NewSchema(field), StructGet(Col("r"), "text"))
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	got, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].String.IsValid(0) || string(got[0].String.ValueBytes(0)) != "" || string(got[0].String.ValueBytes(1)) != "ok" || got[0].DictionaryString != nil {
		t.Fatal("hidden dictionary/read ownership")
	}
	w.Close()
	for _, isString := range []bool{false, true} {
		f := memory.ListInt64Field("xs", false, false)
		if isString {
			f = memory.ListStringField("xs", false, false)
		}
		parent := memory.StructField("r", true, f)
		values := memory.NewBatch()
		values.Rows = 2
		if isString {
			values.StringLists["xs"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2}, Values: &memory.StringViewVector{Offsets: []uint32{0, 6, 8}, Data: []byte("hiddenok"), Validity: memory.Bitmap{2}}}
		} else {
			values.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{0, 1, 2}, Values: &memory.Int64Vector{Values: []int64{999, 22}, Validity: memory.Bitmap{2}}}
		}
		b := memory.NewBatch()
		b.Rows = 2
		b.Structs["r"] = &memory.StructVector{Rows: 2, Fields: parent.Children, Values: values, Validity: memory.Bitmap{2}}
		p, err := Compile(memory.NewSchema(parent), StructGet(Col("r"), "xs"))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if isString {
			v := got[0].StringList
			if v.IsValid(0) || v.Offsets[1] != 0 || string(v.Values.ValueBytes(0)) != "ok" {
				t.Fatal("hidden list string copied")
			}
		} else {
			v := got[0].Int64List
			if v.IsValid(0) || v.Offsets[1] != 0 || v.Values.Values[0] != 22 {
				t.Fatal("hidden list integer copied")
			}
		}
		w.Close()
	}
}
func TestStructGetCSEGuardsAndInputReuse(t *testing.T) {
	k := structGetKinds[3]
	schema, b := structGetFixture(k, 3, false, false)
	child2 := memory.Field{Name: "other", Type: memory.TypeInt64}
	schema.Fields[0].Children = append(schema.Fields[0].Children, child2)
	b.Structs["r"].Fields = schema.Fields[0].Children
	b.Structs["r"].Values.Int64s["other"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	p, err := CompileExpressions(schema, []NamedExpr{Named(StructGet(Col("r"), "a.b:数"), "a"), Named(StructGet(Col("r"), "a.b:数"), "b"), Named(StructGet(Col("r"), "other"), "c")})
	if err != nil {
		t.Fatal(err)
	}
	if got := Columns(StructGet(Col("r"), "other")); !reflect.DeepEqual(got, []string{"r"}) {
		t.Fatal("dependencies", got)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	for i := 0; i < 3; i++ {
		b.Structs["r"].Values.Int64s["other"].Values[0] = int64(100 + i)
		got, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Int64.Values[1] != k.sample.(int64) || got[1].Int64.Values[1] != k.sample.(int64) || got[2].Int64.Values[0] != int64(100+i) {
			t.Fatal("CSE/input address cache")
		}
	}
	// Both bad JSON and missing required field are real value errors when demanded.
	js := memory.NewSchema(memory.Field{Name: "json", Type: memory.TypeString}, memory.Field{Name: "keep", Type: memory.TypeBool})
	jb := memory.NewBatch()
	jb.Rows = 3
	jb.Strings["json"] = testStringVector([]string{"bad", `{"x":42}`, `{}`}, nil)
	jb.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0}}
	get := StructGet(JSONDecode(Col("json"), memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})), "x")
	for _, e := range []Expr{If(Col("keep"), get, I64(7)), Case([]WhenThen{{When: Col("keep"), Then: get}}, I64(7))} {
		p, err := Compile(js, e)
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		got, err := w.Evaluate(context.Background(), jb, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[0].Int64.Values, []int64{7, 42, 7}) {
			t.Fatal("guard values")
		}
		w.Close()
	}
	p, err = Compile(js, get)
	if err != nil {
		t.Fatal(err)
	}
	unguarded, _ := NewWorkspace(context.Background(), p, nil)
	defer unguarded.Close()
	if _, err = unguarded.Evaluate(context.Background(), jb, testKernelSet()); err == nil || !strings.Contains(err.Error(), "JSON_DECODE") {
		t.Fatal("error control", err)
	}
}

type structGetAuditManager struct {
	base        *memory.BudgetManager
	calls, fail int
	peak        int64
}

func (m *structGetAuditManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.fail > 0 && m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	r, err := m.base.Reserve(ctx, n)
	if m.base.Used() > m.peak {
		m.peak = m.base.Used()
	}
	return r, err
}
func (m *structGetAuditManager) Used() int64  { return m.base.Used() }
func (m *structGetAuditManager) Limit() int64 { return m.base.Limit() }
func TestStructGetBudgetFailurePeakAndCancel(t *testing.T) {
	for _, k := range []structGetKind{structGetKinds[3], structGetKinds[11], structGetKinds[18], structGetKinds[19]} {
		t.Run(k.vector, func(t *testing.T) {
			schema, b := structGetFixture(k, 130, true, true)
			p, err := Compile(schema, StructGet(Col("r"), "a.b:数"))
			if err != nil {
				t.Fatal(err)
			}
			m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
			w, _ := NewWorkspace(context.Background(), p, m)
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			calls := m.calls
			if w.MemoryBytes() != m.Used() {
				t.Fatalf("double/missing accounting workspace%d manager%d", w.MemoryBytes(), m.Used())
			}
			first := m.Used()
			m.peak = first
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			if first <= 0 || m.peak <= first {
				t.Fatalf("old+new result not accounted: retained%d peak%d", first, m.peak)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("close leak")
			}
			for fail := 1; fail <= calls; fail++ {
				m := &structGetAuditManager{base: memory.NewBudgetManager(0), fail: fail}
				w, _ := NewWorkspace(context.Background(), p, m)
				_, err := w.Evaluate(context.Background(), b, testKernelSet())
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					w.Close()
					t.Fatalf("reservation%d expected denial got%v", fail, err)
				}
				m.fail = 0
				if got, err := w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
					w.Close()
					t.Fatalf("retry %d: %v", fail, err)
				} else {
					structGetAssert(t, k, got[0], 130, true, true)
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("failure leak", fail)
				}
			}
			m = &structGetAuditManager{base: memory.NewBudgetManager(0)}
			w, _ = NewWorkspace(context.Background(), p, m)
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("cancel leak")
			}
			w, _ = NewWorkspace(context.Background(), p, nil)
			if _, err = w.EvaluateSelected(context.Background(), b, []int{1, 2}, testKernelSet()); err == nil {
				t.Fatal("selected struct support accidentally enabled")
			}
			w.Close()
		})
	}
}

func TestStructGetGuardedAllChildConsumers(t *testing.T) {
	for _, k := range structGetKinds {
		t.Run(fmt.Sprintf("%s-%s", k.typ, k.vector), func(t *testing.T) {
			schema, b := structGetFixture(k, 65, true, true)
			schema = memory.NewSchema(append(schema.Fields, memory.Field{Name: "keep", Type: memory.TypeBool})...)
			keep := make([]uint32, 65)
			for i := range keep {
				keep[i] = uint32(i % 2)
			}
			b.Uint32s["keep"] = &memory.Uint32Vector{Values: keep}
			value := Expr(StructGet(Col("r"), "a.b:数"))
			resultType := k.typ
			if k.typ == memory.TypeList {
				value = IsNull(value)
				resultType = memory.TypeBool
			} else if k.typ == memory.TypeString {
				value = Length(value)
				resultType = memory.TypeInt64
			}
			for _, guard := range []Expr{If(Col("keep"), value, Null(resultType)), Case([]WhenThen{{When: Col("keep"), Then: value}}, Null(resultType))} {
				p, err := Compile(schema, guard)
				if err != nil {
					t.Fatal(err)
				}
				w, _ := NewWorkspace(context.Background(), p, nil)
				got, err := w.Evaluate(context.Background(), b, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				field := k.vector
				if k.typ == memory.TypeList {
					field = "Uint32"
				} else if k.typ == memory.TypeString {
					field = "Int64"
				}
				v := reflect.ValueOf(got[0]).FieldByName(field)
				for i := 0; i < 65; i++ {
					valid := i%2 == 1 && i%5 != 0 && i%3 != 0
					if k.typ == memory.TypeList {
						valid = i%2 == 1
					}
					if v.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(i)})[0].Bool() != valid {
						t.Fatal("guard validity", i)
					}
					if !valid {
						continue
					}
					actual := v.Elem().FieldByName("Values").Index(i).Interface()
					want := k.sample
					if k.typ == memory.TypeList {
						want = uint32(0)
						if i%5 == 0 || i%3 == 0 {
							want = uint32(1)
						}
					} else if k.typ == memory.TypeString {
						want = int64(utf8.RuneCountInString(fmt.Sprintf("值-%d", i)))
					}
					if !reflect.DeepEqual(actual, want) {
						t.Fatalf("row%d got%v want%v", i, actual, want)
					}
				}
				w.Close()
			}
		})
	}
}
func TestStructGetSameRootCSE(t *testing.T) {
	schema, _ := structGetFixture(structGetKinds[3], 0, false, false)
	e := StructGet(Col("r"), "a.b:数")
	one, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	two, err := CompileExpressions(schema, []NamedExpr{Named(e, "a"), Named(e, "b")})
	if err != nil {
		t.Fatal(err)
	}
	if two.InstructionCount() != one.InstructionCount() {
		t.Fatalf("same extraction duplicated: one%d two%d", one.InstructionCount(), two.InstructionCount())
	}
}

func TestStructGetGuardHidesValueNotStructure(t *testing.T) {
	parent := memory.StructField("r", false, memory.Field{Name: "text", Type: memory.TypeString})
	children := memory.NewBatch()
	children.Rows = 2
	children.DictStrings["text"] = &memory.DictionaryStringVector{Indices: []uint32{999, 0}, Offsets: []uint32{0, 2}, Data: []byte("ok"), Encoding: memory.EncodingDictionary}
	b := memory.NewBatch()
	b.Rows = 2
	b.Structs["r"] = &memory.StructVector{Rows: 2, Fields: parent.Children, Values: children}
	b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1}}
	schema := memory.NewSchema(parent, memory.Field{Name: "keep", Type: memory.TypeBool})
	e := If(Col("keep"), Length(StructGet(Col("r"), "text")), I64(0))
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := NewWorkspace(context.Background(), p, nil)
	got, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[0].Int64.Values, []int64{0, 2}) {
		t.Fatal("inactive code accessed")
	}
	w.Close()
	p, err = Compile(schema, StructGet(Col("r"), "text"))
	if err != nil {
		t.Fatal(err)
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "dictionary code") {
		t.Fatal("required demanded error", err)
	}
	w.Close()
	// A malformed offset affects structure even when its value is inactive.
	children.DictStrings["text"].Offsets = []uint32{0, 99}
	p, _ = Compile(schema, e)
	w, _ = NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
		t.Fatal("guard hid malformed dictionary storage")
	}
}

type structGetCheckpointContext struct {
	context.Context
	cancel       context.CancelFunc
	checks, stop int
}

func (c *structGetCheckpointContext) Err() error {
	c.checks++
	if c.stop > 0 && c.checks >= c.stop {
		c.cancel()
	}
	return c.Context.Err()
}
func TestStructGetCancellationCheckpoints(t *testing.T) {
	for _, k := range []structGetKind{structGetKinds[11], structGetKinds[18], structGetKinds[19]} {
		t.Run(k.vector, func(t *testing.T) {
			schema, b := structGetFixture(k, 130, true, true)
			p, err := Compile(schema, StructGet(Col("r"), "a.b:数"))
			if err != nil {
				t.Fatal(err)
			}
			base, cancel := context.WithCancel(context.Background())
			probe := &structGetCheckpointContext{Context: base, cancel: cancel}
			w, _ := NewWorkspace(context.Background(), p, nil)
			if _, err = w.Evaluate(probe, b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			checks := probe.checks
			w.Close()
			cancel()
			if checks < 5 {
				t.Fatalf("too few cancellation checkpoints: %d", checks)
			}
			for _, stop := range []int{1, checks / 2, checks - 1} {
				base, cancel := context.WithCancel(context.Background())
				ctx := &structGetCheckpointContext{Context: base, cancel: cancel, stop: stop}
				m := memory.NewBudgetManager(0)
				w, _ := NewWorkspace(context.Background(), p, m)
				_, err := w.Evaluate(ctx, b, testKernelSet())
				cancel()
				w.Close()
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("checkpoint %d/%d err%v", stop, checks, err)
				}
				if m.Used() != 0 {
					t.Fatal("checkpoint cancellation leak")
				}
			}
		})
	}
}

func TestStructGetReplacementFailure(t *testing.T) {
	for _, k := range []structGetKind{structGetKinds[11], structGetKinds[18], structGetKinds[19]} {
		t.Run(k.vector, func(t *testing.T) {
			schema, b := structGetFixture(k, 65, true, true)
			p, err := Compile(schema, StructGet(Col("r"), "a.b:数"))
			if err != nil {
				t.Fatal(err)
			}
			m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
			w, _ := NewWorkspace(context.Background(), p, m)
			defer w.Close()
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			before := m.Used()
			m.fail = m.calls + 1
			if _, err = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal("replacement denial", err)
			}
			if m.Used() != before {
				t.Fatalf("failed replacement changed retained charge %d -> %d", before, m.Used())
			}
			m.fail = 0
			got, err := w.Evaluate(context.Background(), b, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			structGetAssert(t, k, got[0], 65, true, true)
			w.Close()
			if m.Used() != 0 {
				t.Fatal("replacement leak")
			}
		})
	}
}

func TestStructGetMetadataExportAgreement(t *testing.T) {
	for _, k := range structGetKinds {
		t.Run(fmt.Sprintf("%s-%s", k.typ, k.vector), func(t *testing.T) {
			schema, _ := structGetFixture(k, 0, true, false)
			parent := schema.Fields[0]
			child := parent.Children[0]
			as, err := memory.ArrowSchema(memory.NewSchema(child))
			if err != nil {
				t.Fatal(err)
			}
			child.ArrowType = as.Field(0).Type.String()
			parent.Children = []memory.Field{child}
			schema = memory.NewSchema(parent)
			e := StructGet(Col("r"), child.Name)
			p, err := Compile(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			info, err := InferScalarType(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			out := p.ResultField()
			if out.Type != info.Type || out.Nullable != info.Nullable || out.ArrowType != child.ArrowType || !reflect.DeepEqual(out.Children, child.Children) {
				t.Fatal("metadata disagreement", out, info)
			}
			wantEncoding := memory.EncodingPlain
			if k.typ == memory.TypeString || k.typ == memory.TypeList {
				wantEncoding = memory.EncodingView
			}
			if out.Encoding != wantEncoding || out.DictionaryDomain != "" {
				t.Fatal("owned encoding", out)
			}
			out.Name = "output"
			exported, err := memory.ArrowSchema(memory.NewSchema(out))
			if err != nil {
				t.Fatal(err)
			}
			if exported.Field(0).Type.String() != as.Field(0).Type.String() || !exported.Field(0).Nullable {
				t.Fatal("Arrow output contract")
			}
		})
	}
}

// TestStructGetLongPayloadCopyCancellation arms cancellation only after the
// second reservation (the sized output, after validation and estimation).
// Post-reservation checks are: row, then each 32KiB string chunk; lists add
// an element check. Thus these stops interrupt partially copied payloads,
// rather than validation, estimation, entry, or the final commit check.
func TestStructGetLongPayloadCopyCancellation(t *testing.T) {
	for _, kind := range []string{"String", "StringList", "Int64List"} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retained=%v", kind, warm), func(t *testing.T) {
				child := memory.Field{Name: "x", Type: memory.TypeString}
				values := memory.NewBatch()
				values.Rows = 1
				payload := []byte(strings.Repeat("abcdefgh", 16384)) // 128KiB, one row/element.
				stop := 3                                            // row, first chunk, second chunk (first 32KiB already copied).
				switch kind {
				case "String":
					values.Strings["x"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: payload}
				case "StringList":
					child = memory.ListStringField("x", false, false)
					stop = 4
					values.StringLists["x"] = &memory.StringListVector{Offsets: []uint32{0, 1}, Values: &memory.StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: payload}}
				case "Int64List":
					child = memory.ListInt64Field("x", false, false)
					stop = 1026
					xs := make([]int64, 16384)
					for i := range xs {
						xs[i] = int64(i + 17)
					}
					values.Int64Lists["x"] = &memory.Int64ListVector{Offsets: []uint32{0, uint32(len(xs))}, Values: &memory.Int64Vector{Values: xs}}
				}
				parent := memory.StructField("r", false, child)
				input := scalarRegister{typ: memory.TypeStruct, strct: &memory.StructVector{Rows: 1, Fields: parent.Children, Values: values}}
				plan := &structGetPlan{parent: parent, child: child, index: 0}
				m := memory.NewBudgetManager(0)
				out := scalarRegister{typ: child.Type}
				out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
				snapshot := func() string {
					v := struct {
						Data     []byte
						Offsets  []uint32
						Validity memory.Bitmap
						Strings  *memory.StringListVector
						Ints     *memory.Int64ListVector
					}{out.data, out.offsets, out.validity, out.strList, out.intList}
					b, err := json.Marshal(v)
					if err != nil {
						t.Fatal(err)
					}
					return string(b)
				}
				if warm {
					if err := evalStructGet(&out, &input, plan, 1); err != nil {
						t.Fatal(err)
					}
				}
				before, charge := snapshot(), m.Used()
				// Make candidate replacement distinguishable from the already owned output.
				if kind == "Int64List" {
					values.Int64Lists["x"].Values.Values[0] = -999
				} else {
					payload[0] = 'Z'
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reservations, copyChecks := 0, 0
				out.acquire = func(n int64) (memory.Reservation, error) {
					lease, err := m.Reserve(ctx, n)
					if err == nil {
						reservations++
					}
					return lease, err
				}
				out.checkContext = func() error {
					if reservations == 2 {
						copyChecks++
						if copyChecks == stop {
							cancel()
						}
					}
					return ctx.Err()
				}
				err := evalStructGet(&out, &input, plan, 1)
				if !errors.Is(err, context.Canceled) || reservations != 2 || copyChecks != stop {
					t.Fatalf("copy checkpoint: err=%v reservations=%d checks=%d want=%d", err, reservations, copyChecks, stop)
				}
				if snapshot() != before {
					t.Fatal("failed copy changed previously committed output")
				}
				if m.Used() != charge {
					t.Fatalf("failed copy leaked candidate/scratch: used=%d retained=%d", m.Used(), charge)
				}
				if out.structGetReservation != nil {
					out.structGetReservation.Release()
				}
				if m.Used() != 0 {
					t.Fatalf("owner release leaked %d", m.Used())
				}
				t.Logf("after output admission: canceled copy checkpoint %d; retained=%v; released=0", copyChecks, warm)
			})
		}
	}
}

func TestStructGetCoalesceGuard(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeString} {
		t.Run(typ.String(), func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "first", Type: typ, Nullable: true}, memory.Field{Name: "json", Type: memory.TypeString})
			get := StructGet(JSONDecode(Col("json"), memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true})), "x")
			var fallback Expr = I64(99)
			if typ == memory.TypeString {
				fallback = Str("fallback")
			}
			p, err := Compile(schema, Coalesce(Col("first"), get, fallback))
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			// Alternate both newly allocated batches and repeated evaluation at the same
			// batch address. NULL domains move between calls; stale region masks fail.
			for batchIndex := 0; batchIndex < 2; batchIndex++ {
				b := memory.NewBatch()
				b.Rows = 4
				for reuse := 0; reuse < 3; reuse++ {
					active := (batchIndex + reuse) % 2
					valid := []bool{active != 0, active == 0, true, false}
					bitmap := make(memory.Bitmap, 1)
					for i, v := range valid {
						if v {
							memory.Set(bitmap, i)
						}
					}
					docs := []string{"bad", "bad", "bad", `{}`}
					demanded := active
					docs[demanded] = `{"x":42}`
					if typ == memory.TypeString {
						docs[demanded] = `{"x":"decoded"}`
						b.Strings["first"] = testStringVector([]string{"first0", "first1", "first2", "hidden"}, valid)
					} else {
						b.Int64s["first"] = &memory.Int64Vector{Values: []int64{7, 8, 9, -123}, Validity: bitmap}
					}
					b.Strings["json"] = testStringVector(docs, nil)
					got, err := w.Evaluate(context.Background(), b, testKernelSet())
					if err != nil {
						t.Fatal(err)
					}
					for row := 0; row < 4; row++ {
						if typ == memory.TypeString {
							want := fmt.Sprintf("first%d", row)
							if row == demanded {
								want = "decoded"
							}
							if row == 3 {
								want = "fallback"
							}
							if !got[0].String.IsValid(row) || string(got[0].String.ValueBytes(row)) != want {
								t.Fatalf("batch%d reuse%d row%d string oracle", batchIndex, reuse, row)
							}
						} else {
							want := int64(7 + row)
							if row == demanded {
								want = 42
							}
							if row == 3 {
								want = 99
							}
							if !got[0].Int64.IsValid(row) || got[0].Int64.Values[row] != want {
								t.Fatalf("batch%d reuse%d row%d numeric oracle", batchIndex, reuse, row)
							}
						}
					}
					// Identical bad input becomes a real JSON error when first is NULL.
					docs[demanded] = "bad"
					b.Strings["json"] = testStringVector(docs, nil)
					control, controlErr := NewWorkspace(context.Background(), p, m)
					if controlErr != nil {
						t.Fatal(controlErr)
					}
					_, err = control.Evaluate(context.Background(), b, testKernelSet())
					control.Close()
					if err == nil || !strings.Contains(err.Error(), "JSON_DECODE") {
						t.Fatalf("active invalid JSON error control: %v", err)
					}
				}
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatalf("Coalesce lifecycle leaked %d", m.Used())
			}
		})
	}
}
