package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// The preexisting StructGet fixture/oracle contains literal native values and
// row formulas, independent of both constructor and extractor implementations.
func constructFixture(n int, nullable bool) (memory.Schema, *memory.Batch, []NamedExpr) {
	b := memory.NewBatch()
	b.Rows = n
	fields := []memory.Field{}
	args := []NamedExpr{}
	for i, k := range structGetKinds {
		schema, source := structGetFixture(k, n, false, nullable)
		field := schema.Fields[0].Children[0]
		name := fmt.Sprintf("input%d", i)
		field.Name = name
		fields = append(fields, field)
		src := reflect.ValueOf(source.Structs["r"].Values).Elem().FieldByName(k.vector + "s")
		dst := reflect.ValueOf(b).Elem().FieldByName(k.vector + "s")
		dst.SetMapIndex(reflect.ValueOf(name), src.MapIndex(reflect.ValueOf("a.b:数")))
		args = append(args, Named(Col(name), fmt.Sprintf("child.%d:数", i)))
	}
	return memory.NewSchema(fields...), b, args
}
func constructChildValue(v *memory.StructVector, k structGetKind, name string) ScalarValue {
	result := ScalarValue{Type: k.typ}
	ptr := reflect.ValueOf(v.Values).Elem().FieldByName(k.vector + "s").MapIndex(reflect.ValueOf(name))
	if ptr.IsValid() {
		reflect.ValueOf(&result).Elem().FieldByName(k.vector).Set(ptr)
	}
	return result
}
func constructCheck(t *testing.T, n int, nullable bool, out []ScalarValue) {
	t.Helper()
	s := out[0].Struct
	if s == nil || s.Len() != n || len(s.Fields) != 20 {
		t.Fatal("constructed shape")
	}
	for row := 0; row < n; row++ {
		if !s.IsValid(row) {
			t.Fatal("parent must be valid even all children NULL", row)
		}
	}
	for i, k := range structGetKinds {
		structGetAssert(t, k, constructChildValue(s, k, fmt.Sprintf("child.%d:数", i)), n, false, nullable)
		if len(out) > 1 {
			structGetAssert(t, k, out[i+1], n, false, nullable)
		}
	}
}
func TestStructConstructAllTypesOwnedAndExtracted(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, identity := range []bool{false, true} {
			for _, km := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("null%v/identity%v/kernel%v", nullable, identity, km), func(t *testing.T) {
					schema, _, args := constructFixture(0, nullable)
					e := StructFrom(args...)
					es := []NamedExpr{Named(e, "record")}
					for i := range structGetKinds {
						es = append(es, Named(StructGet(e, fmt.Sprintf("child.%d:数", i)), fmt.Sprint(i)))
					}
					bound, err := Bind(schema, e)
					if err != nil {
						t.Fatal(err)
					}
					if bound.Field.Nullable || bound.Field.Type != memory.TypeStruct || len(bound.Field.Children) != 20 {
						t.Fatal("parent metadata")
					}
					for i, f := range bound.Field.Children {
						if f.Type != schema.Fields[i].Type || f.Nullable != nullable {
							t.Fatal("child metadata", i)
						}
					}
					p, err := CompileExpressions(schema, es)
					if err != nil {
						t.Fatal(err)
					}
					if p.SupportsSelection() {
						t.Fatal("unexpected Struct selected support")
					}
					if identity {
						p.instructions = append([]instruction(nil), p.instructions...)
						for i := range p.instructions {
							p.instructions[i].storageOwner = i + 1
						}
					}
					m := memory.NewBudgetManager(0)
					w, _ := NewWorkspace(context.Background(), p, m)
					for _, n := range []int{0, 1, 63, 64, 65, 130, 1, 0} {
						_, b, _ := constructFixture(n, nullable)
						before, _ := json.Marshal(b)
						out, err := w.Evaluate(context.Background(), b, kernel.New(km))
						if err != nil {
							t.Fatal(err)
						}
						constructCheck(t, n, nullable, out)
						after, _ := json.Marshal(b)
						if string(before) != string(after) {
							t.Fatal("input mutated")
						}
						// Poison source numeric/string/list payload after extraction; both exported
						// Struct and scalar roots must remain independently owned and correct.
						for _, v := range b.Int64s {
							for i := range v.Values {
								v.Values[i] = -999
							}
						}
						for _, v := range b.Strings {
							for i := range v.Data {
								v.Data[i] = 'X'
							}
						}
						for _, v := range b.Int64Lists {
							for i := range v.Values.Values {
								v.Values.Values[i] = -999
							}
						}
						for _, v := range b.StringLists {
							for i := range v.Values.Data {
								v.Values.Data[i] = 'X'
							}
						}
						constructCheck(t, n, nullable, out)
						if m.Used() != w.MemoryBytes() {
							t.Fatalf("owner budget drift rows=%d manager=%d workspace=%d", n, m.Used(), w.MemoryBytes())
						}
					}
					w.Close()
					if m.Used() != 0 {
						t.Fatal("owner close leak")
					}
				})
			}
		}
	}
}
func TestStructConstructBindingAndCSE(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true})
	a := StructFrom(Named(Col("a"), "x"), Named(Col("b"), "y"))
	p, err := CompileExpressions(schema, []NamedExpr{Named(a, "one"), Named(a, "same"), Named(StructFrom(Named(Col("b"), "x"), Named(Col("a"), "y")), "values"), Named(StructFrom(Named(Col("a"), "different"), Named(Col("b"), "y")), "names"), Named(StructFrom(Named(Col("b"), "y"), Named(Col("a"), "x")), "order")})
	if err != nil {
		t.Fatal(err)
	}
	if p.outputs[0].root != p.outputs[1].root {
		t.Fatal("identical struct CSE")
	}
	for i := 2; i < 5; i++ {
		if p.outputs[0].root == p.outputs[i].root {
			t.Fatal("name/order/child missing in CSE", i)
		}
	}
	if !reflect.DeepEqual(Columns(a), []string{"a", "b"}) {
		t.Fatal("all dependencies")
	}
	for _, e := range []Expr{StructFrom(), StructFrom(Named(I64(1), "")), StructFrom(Named(I64(1), "dup"), Named(I64(2), "dup")), StructFrom(Named(nil, "nil")), StructFrom(Named(NullDecimal128(1, 0), "decimal"))} {
		if _, err := Compile(schema, e); err == nil {
			t.Fatalf("invalid constructor accepted %v", e)
		}
	}
	// Names compare exactly; punctuation and non-ASCII remain literal names.
	if _, err := Compile(schema, StructFrom(Named(I64(1), "a.b"), Named(I64(2), "A.b"), Named(I64(3), "数"))); err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(schema, StructFrom(Named(Col("a"), ""))); err != nil {
		t.Fatal("bare column name contract", err)
	}
}

func TestStructConstructIndependentNullsAndDictionary(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "y", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "source-domain"})
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{777, 12, 777}, Validity: memory.Bitmap{2}}
	b.DictStrings["y"] = &memory.DictionaryStringVector{Indices: []uint32{0, 999, 999}, Offsets: []uint32{0, 2}, Data: []byte("ok"), Validity: memory.Bitmap{1}, Encoding: memory.EncodingDictionary}
	p, err := Compile(schema, StructFrom(Named(Col("x"), "x"), Named(Col("y"), "y")))
	if err != nil {
		t.Fatal(err)
	}
	f := p.ResultField()
	if f.Nullable || f.Children[1].DictionaryDomain != "" || f.Children[1].Encoding != memory.EncodingView {
		t.Fatal("owned dictionary metadata", f)
	}
	m := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, m)
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	s := out[0].Struct
	for i := 0; i < 3; i++ {
		if !s.IsValid(i) || s.Values.Int64s["x"].IsValid(i) != (i == 1) || s.Values.Strings["y"].IsValid(i) != (i == 0) {
			t.Fatal("independent child NULL/all-null-parent", i)
		}
	}
	if string(s.Values.Strings["y"].ValueBytes(0)) != "ok" || s.Values.Int64s["x"].Values[1] != 12 {
		t.Fatal("dictionary/number")
	}
	b.DictStrings["y"].Data[0] = 'X'
	if string(s.Values.Strings["y"].ValueBytes(0)) != "ok" {
		t.Fatal("borrowed dictionary output")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("dictionary close leak")
	}
}
func TestStructConstructJSONListAndEagerGuardDomains(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	e := StructFrom(Named(Split(Col("s"), Str(",")), "parts"), Named(Str(`{"x":42}`), "json"))
	parts := StructGet(e, "parts")
	decode := JSONDecode(StructGet(e, "json"), memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}))
	p, err := CompileExpressions(schema, []NamedExpr{Named(e, "record"), Named(ListLength(parts), "length"), Named(ListGet(parts, I64(1)), "second"), Named(StructGet(decode, "x"), "decoded")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["s"] = testStringVector([]string{"a,b", "x", "ignored"}, []bool{true, true, false})
	w, _ := NewWorkspace(context.Background(), p, nil)
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out[1].Int64.Values, []int64{2, 1, 0}) || string(out[2].String.ValueBytes(0)) != "b" || out[2].String.IsValid(1) || out[2].String.IsValid(2) || !reflect.DeepEqual(out[3].Int64.Values, []int64{42, 42, 42}) {
		t.Fatal("Construct/Get/List/JSON oracle")
	}
	for i := 0; i < 3; i++ {
		if !out[0].Struct.IsValid(i) {
			t.Fatal("always valid construct parent")
		}
	}
	w.Close()
	// JSON-decoded primitive/list children are valid constructor inputs; the
	// decoded Struct is now also accepted within the checked N7 depth profile.
	decoded := JSONDecode(Str(`{"n":9,"list":[10,null,30]}`), memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64}, memory.ListInt64Field("list", false, true)))
	rewrap := StructFrom(Named(StructGet(decoded, "n"), "n"), Named(StructGet(decoded, "list"), "list"))
	p, err = CompileExpressions(memory.NewSchema(), []NamedExpr{Named(StructGet(rewrap, "n"), "n"), Named(ListGet(StructGet(rewrap, "list"), I64(2)), "last")})
	if err != nil {
		t.Fatal(err)
	}
	b = memory.NewBatch()
	b.Rows = 1
	w, _ = NewWorkspace(context.Background(), p, nil)
	out, err = w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil || out[0].Int64.Values[0] != 9 || out[1].Int64.Values[0] != 30 {
		t.Fatal("decoded child rewrap", err)
	}
	w.Close()
	schema = memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}, memory.Field{Name: "keep", Type: memory.TypeBool}, memory.Field{Name: "first", Type: memory.TypeInt64, Nullable: true})
	eager := StructGet(StructFrom(Named(I64(8), "good"), Named(CastTo(Col("s"), memory.TypeInt64), "bad")), "good")
	b = memory.NewBatch()
	b.Rows = 3
	b.Strings["s"] = testStringVector([]string{"bad", "1", "bad"}, nil)
	b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0}}
	b.Int64s["first"] = &memory.Int64Vector{Values: []int64{7, 0, 7}, Validity: memory.Bitmap{5}}
	for name, e := range map[string]Expr{"if": If(Col("keep"), eager, I64(7)), "case": Case([]WhenThen{{When: Col("keep"), Then: eager}}, I64(7)), "coalesce": Coalesce(Col("first"), eager)} {
		p, err = Compile(schema, e)
		if err != nil {
			t.Fatal(err)
		}
		w, _ = NewWorkspace(context.Background(), p, nil)
		out, err = w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || !reflect.DeepEqual(out[0].Int64.Values, []int64{7, 8, 7}) {
			t.Fatal(name, err)
		}
		w.Close()
	}
	p, err = Compile(schema, In(I64(8), If(Col("keep"), I64(0), I64(8)), eager))
	if err != nil {
		t.Fatal(err)
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	out, err = w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil || !reflect.DeepEqual(out[0].Uint32.Values, []uint32{1, 1, 1}) {
		t.Fatal("IN constructor guard", err)
	}
	w.Close()
	p, err = Compile(schema, eager)
	if err != nil {
		t.Fatal(err)
	}
	w, _ = NewWorkspace(context.Background(), p, nil)
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil || !strings.Contains(err.Error(), "cast") {
		t.Fatal("unselected child is still eager", err)
	}
	w.Close()
}
func TestStructConstructAdmissionsReuseAndCancel(t *testing.T) {
	schema, b, args := constructFixture(65, true)
	p, err := Compile(schema, StructFrom(args...))
	if err != nil {
		t.Fatal(err)
	}
	probe := &selectedFaultManager{Manager: memory.NewBudgetManager(0)}
	w, _ := NewWorkspace(context.Background(), p, probe)
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	calls := probe.calls
	w.Close()
	for fail := 1; fail <= calls; fail++ {
		m := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
		w, _ := NewWorkspace(context.Background(), p, m)
		if _, err = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("Reserve%d: %v", fail, err)
		}
		m.fail = 0
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal(err)
		}
		constructCheck(t, 65, true, out)
		w.Close()
		if m.Used() != 0 {
			t.Fatal("admission close leak")
		}
	}
	m := memory.NewBudgetManager(0)
	w, _ = NewWorkspace(context.Background(), p, m)
	if _, err = w.EvaluateSelected(context.Background(), b, []int{1}, testKernelSet()); err == nil {
		t.Fatal("StructFrom selection must reject")
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("selected reject leak")
	}
	w, _ = NewWorkspace(context.Background(), p, m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
}

func TestStructConstructSnapshotAndSameAddress(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	fields := []NamedExpr{Named(Col("x"), "original")}
	e := StructFrom(fields...)
	fields[0] = Named(I64(99), "mutated")
	p, err := Compile(schema, e)
	if err != nil {
		t.Fatal(err)
	}
	if p.ResultField().Children[0].Name != "original" {
		t.Fatal("constructor retained mutable argument slice")
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{0}}
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	for _, value := range []int64{1, 999, -7, 0} {
		b.Int64s["x"].Values[0] = value
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil || !out[0].Struct.IsValid(0) || out[0].Struct.Values.Int64s["original"].Values[0] != value {
			t.Fatal("same address stale constructor", err)
		}
	}
}

func TestStructConstructListViewAndMalformedStorage(t *testing.T) {
	for _, text := range []bool{false, true} {
		field := memory.ListInt64Field("xs", true, true)
		if text {
			field = memory.ListStringField("xs", true, true)
		}
		b := memory.NewBatch()
		b.Rows = 1
		if text {
			b.StringLists["xs"] = &memory.StringListVector{Offsets: []uint32{1, 2, 999}, Values: &memory.StringViewVector{Offsets: []uint32{0, 6, 8, 999}, Data: []byte("hiddenok")}}
		} else {
			b.Int64Lists["xs"] = &memory.Int64ListVector{Offsets: []uint32{1, 2, 999}, Values: &memory.Int64Vector{Values: []int64{-999, 42}}}
		}
		p, err := Compile(memory.NewSchema(field), StructFrom(Named(Col("xs"), "xs")))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := NewWorkspace(context.Background(), p, nil)
		out, err := w.Evaluate(context.Background(), b, testKernelSet())
		if err != nil {
			t.Fatal("logical prefix/nonzero offset", err)
		}
		s := out[0].Struct
		if !s.IsValid(0) {
			t.Fatal("parent invalid")
		}
		if text {
			v := s.Values.StringLists["xs"]
			if !reflect.DeepEqual(v.Offsets, []uint32{0, 1}) || string(v.Values.ValueBytes(0)) != "ok" {
				t.Fatal("canonical liststring prefix")
			}
		} else {
			v := s.Values.Int64Lists["xs"]
			if !reflect.DeepEqual(v.Offsets, []uint32{0, 1}) || v.Values.Values[0] != 42 {
				t.Fatal("canonical listint prefix")
			}
		}
		w.Close()
		// Even a NULL child must not conceal malformed referenced structure.
		if text {
			b.StringLists["xs"].Validity = memory.Bitmap{0}
			b.StringLists["xs"].Offsets[1] = 999
		} else {
			b.Int64Lists["xs"].Validity = memory.Bitmap{0}
			b.Int64Lists["xs"].Offsets[1] = 999
		}
		w, _ = NewWorkspace(context.Background(), p, nil)
		if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err == nil {
			t.Fatal("NULL child masked malformed list offsets")
		}
		w.Close()
	}
}

func TestStructConstructManyComputedChildrenAndMetadataCopy(t *testing.T) {
	es := []NamedExpr{}
	for i := 0; i < 8; i++ {
		es = append(es, Named(Add(I64(int64(i)), I64(10)), fmt.Sprintf("n%d", i)))
	}
	es = append(es, Named(Bool(true), "yes"), Named(Bool(false), "no"), Named(Null(memory.TypeString), "null"))
	record := StructFrom(es...)
	p, err := CompileExpressions(memory.NewSchema(), []NamedExpr{Named(record, "record"), Named(Add(StructGet(record, "n7"), I64(100)), "later"), Named(StructGet(record, "yes"), "bool")})
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 65
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	out, err := w.Evaluate(context.Background(), b, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 65; row++ {
		s := out[0].Struct
		if !s.IsValid(row) {
			t.Fatal("constant parent")
		}
		for i := 0; i < 8; i++ {
			if s.Values.Int64s[fmt.Sprintf("n%d", i)].Values[row] != int64(10+i) {
				t.Fatal("multi-input/liveness", i)
			}
		}
		if s.Values.Uint32s["yes"].Values[row] != 1 || s.Values.Uint32s["no"].Values[row] != 0 || s.Values.Strings["null"].IsValid(row) || out[1].Int64.Values[row] != 117 || out[2].Uint32.Values[row] != 1 {
			t.Fatal("computed/bool/NULL children")
		}
	}
	field := memory.ListInt64Field("list", false, true)
	field.Children[0].ArrowType = "int64"
	p2, err := Compile(memory.NewSchema(field), StructFrom(Named(Col("list"), "nested-metadata")))
	if err != nil {
		t.Fatal(err)
	}
	got := p2.ResultField()
	if got.Children[0].Children[0].ArrowType != "int64" {
		t.Fatal("item metadata lost")
	}
	got.Children[0].Children[0].ArrowType = "changed"
	got.Children[0].Name = "changed"
	fresh := p2.ResultField()
	if fresh.Children[0].Name != "nested-metadata" || fresh.Children[0].Children[0].ArrowType != "int64" {
		t.Fatal("result metadata alias")
	}
}

func TestStructConstructCopyCancellationAtomicOwner(t *testing.T) {
	for _, list := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			for _, late := range []bool{false, true} {
				t.Run(fmt.Sprintf("list%v/warm%v/late%v", list, warm, late), func(t *testing.T) {
					payload := []byte(strings.Repeat("a", 128<<10))
					field := memory.Field{Name: "x", Type: memory.TypeString}
					input := scalarRegister{typ: memory.TypeString, strView: &memory.StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: payload}}
					stop := 5 // after output Reserve: parent,field,row,copy1,copy2.
					if list {
						field = memory.ListStringField("x", false, false)
						input = scalarRegister{typ: memory.TypeList, strList: &memory.StringListVector{Offsets: []uint32{0, 1}, Values: input.strView}}
						stop = 6
					} // extra element checkpoint.
					if late {
						stop += 3
					} // final check after all four copy chunks, before atomic swap.
					m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
					out := scalarRegister{typ: memory.TypeStruct}
					out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
					regs := []*scalarRegister{&input}
					fields := []memory.Field{field}
					if warm {
						if err := evalStructFrom(&out, regs, []int{0}, fields, 1, nil); err != nil {
							t.Fatal(err)
						}
					}
					before, _ := json.Marshal(out.strct)
					charge := m.Used()
					payload[0] = 'Z'
					reservations, checks := 0, 0
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					out.acquire = func(n int64) (memory.Reservation, error) {
						r, err := m.Reserve(ctx, n)
						if err == nil {
							reservations++
						}
						return r, err
					}
					out.checkContext = func() error {
						if reservations == 2 {
							checks++
							if checks == stop {
								cancel()
							}
						}
						return ctx.Err()
					}
					if err := evalStructFrom(&out, regs, []int{0}, fields, 1, nil); !errors.Is(err, context.Canceled) || checks != stop || reservations != 2 {
						t.Fatal("copy-phase cancellation", err, checks, reservations)
					}
					after, _ := json.Marshal(out.strct)
					if string(before) != string(after) {
						t.Fatal("failed construction replaced old payload")
					}
					if m.Used() != charge {
						t.Fatal("scratch/candidate lease leaked")
					}
					if warm && m.peak < 2*charge {
						t.Fatal("replacement peak omitted old+new", m.peak, charge)
					}
					if out.structFromReservation != nil {
						out.structFromReservation.Release()
					}
					if m.Used() != 0 {
						t.Fatal("final owner release leak")
					}
				})
			}
		}
	}
}

type constructCopyCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *constructCopyCancelContext) Err() error {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".copyStructBytes") {
			c.checks++
			if c.checks == 2 {
				c.cancel()
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestStructConstructWorkspaceRunningCancellation(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeString})
	p, err := Compile(schema, StructFrom(Named(Col("x"), "x")))
	if err != nil {
		t.Fatal(err)
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["x"] = testStringVector([]string{strings.Repeat("a", 128<<10)}, nil)
	m := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, m)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &constructCopyCancelContext{Context: base, cancel: cancel}
	if _, err = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(err, context.Canceled) || ctx.checks != 2 {
		t.Fatal("running workspace copy cancellation", err, ctx.checks)
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("copy cancellation manager leak")
	}
}
