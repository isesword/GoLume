package expr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"strings"
	"testing"
)

func listStructExprFixture(t *testing.T, typ memory.DataType, n int) (memory.Schema, *memory.Batch) {
	t.Helper()
	_, b := listGetFixture(typ, false, n)
	old := b.Int64Lists["xs"]
	delete(b.Int64Lists, "xs")
	field, e := memory.ListStructField("xs", true, true, memory.Field{Name: "code.中", Type: memory.TypeInt64}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	children := memory.NewBatch()
	children.Rows = len(old.Values.Values)
	children.Int64s["code.中"] = &memory.Int64Vector{Values: append([]int64(nil), old.Values.Values...)}
	texts := make([]string, children.Rows)
	valid := make([]bool, children.Rows)
	for i, v := range old.Values.Values {
		texts[i] = fmt.Sprintf("v%d", v)
		valid[i] = i%4 != 0
	}
	children.Strings["s"] = testStringVector(texts, valid)
	b.StructLists["xs"] = &memory.StructListVector{Offsets: old.Offsets, Validity: old.Validity, Values: &memory.StructVector{Rows: children.Rows, Fields: field.Children[0].Children, Values: children, Validity: old.Values.Validity, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
	schema, e := memory.NewSchemaWithStructListsChecked(field, memory.Field{Name: "i", Type: typ, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	return schema, b
}
func TestListStructBExpr(t *testing.T) {
	for _, typ := range listGetIndexTypes {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			t.Run(fmt.Sprintf("%s/m%d", typ, mode), func(t *testing.T) {
				schema, b := listStructExprFixture(t, typ, 65)
				item := ListGet(Col("xs"), Col("i"))
				p, e := CompileExpressions(schema, []NamedExpr{ListLength(Col("xs")).Alias("len"), item.Alias("item"), StructGet(item, "code.中").Alias("code"), StructGet(item, "s").Alias("s"), StructGet(item, "code.中").Alias("again")})
				if e != nil {
					t.Fatal(e)
				}
				f := p.OutputField(1)
				if f.Type != memory.TypeStruct || !f.Nullable || f.Children[0].Nullable || p.SupportsSelection() {
					t.Fatal("metadata/selected", f)
				}
				f.Children[0].Name = "mutated"
				if p.OutputField(1).Children[0].Name != "code.中" {
					t.Fatal("metadata alias")
				}
				m := memory.NewBudgetManager(0)
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				defer w.Close()
				got, e := w.Evaluate(context.Background(), b, kernel.New(mode))
				if e != nil {
					t.Fatal(e)
				}
				for row := 0; row < 65; row++ {
					lv := row%7 != 0
					if got[0].Int64.IsValid(row) != lv {
						t.Fatal("length validity", row)
					}
					length := int64(3)
					if row%5 == 0 {
						length = 0
					}
					if lv && got[0].Int64.Values[row] != length {
						t.Fatal("length", row)
					}
					idx := row % 6
					valid := lv && row%11 != 0 && row%5 != 0 && (idx == 0 || idx == 2)
					if got[1].Struct.IsValid(row) != valid || got[2].Int64.IsValid(row) != valid || got[4].Int64.IsValid(row) != valid {
						t.Fatal("item/code validity", row)
					}
					elementIndex := idx
					for prior := 0; prior < row; prior++ {
						if prior%5 != 0 {
							elementIndex += 3
						}
					}
					sv := valid && elementIndex%4 != 0
					if got[3].String.IsValid(row) != sv {
						t.Fatal("string validity", row)
					}
					if sv && string(got[3].String.ValueBytes(row)) != fmt.Sprintf("v%d", row*10+idx) {
						t.Fatal("string oracle", row)
					}
					if valid {
						want := int64(row*10 + idx)
						if got[1].Struct.Values.Int64s["code.中"].Values[row] != want || got[2].Int64.Values[row] != want || got[4].Int64.Values[row] != want {
							t.Fatal("code oracle", row)
						}
					}
				}
				// Owned item remains stable while the immutable-input contract is deliberately
				// broken after Evaluate; read only before any workspace reuse.
				before := got[1].Struct.Values.Int64s["code.中"].Values[2]
				for i := range b.StructLists["xs"].Values.Values.Int64s["code.中"].Values {
					b.StructLists["xs"].Values.Values.Int64s["code.中"].Values[i] = -999
				}
				if before != 22 || got[1].Struct.Values.Int64s["code.中"].Values[2] != 22 {
					t.Fatal("owned item aliases")
				}
				for i := range b.StructLists["xs"].Values.Values.Strings["s"].Data {
					b.StructLists["xs"].Values.Values.Strings["s"].Data[i] = '!'
				}
				if string(got[1].Struct.Values.Strings["s"].ValueBytes(2)) != "v22" {
					t.Fatal("owned String alias")
				}
				w.Close()
				if m.Used() != 0 {
					t.Fatal("owner leak")
				}
			})
		}
	}
}
func TestListStructBBoundariesAndReplacement(t *testing.T) {
	schema, b := listStructExprFixture(t, memory.TypeInt64, 65)
	item := ListGet(Col("xs"), Col("i"))
	p, e := Compile(schema, item)
	if e != nil {
		t.Fatal(e)
	}
	for _, bad := range []Expr{JSONDecode(Str("{}"), schema)} {
		if _, e := Compile(schema, bad); e == nil {
			t.Fatal("unexpanded constructor accepted")
		}
	}
	m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	old := m.Used()
	m.fail = m.calls + 1
	if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("replacement denial", e)
	}
	if m.Used() != old {
		t.Fatal("old owner charge lost")
	}
	m.fail = 0
	if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = w.Evaluate(ctx, b, testKernelSet()); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
	selected, e := NewWorkspace(context.Background(), p, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer selected.Close()
	if _, e = selected.EvaluateSelected(context.Background(), b, []int{1, 2}, testKernelSet()); e == nil {
		t.Fatal("selected unexpectedly enabled")
	}
}

func TestListStructBReplacementEveryAdmission(t *testing.T) {
	schema, b := listStructExprFixture(t, memory.TypeInt64, 65)
	p, e := Compile(schema, ListGet(Col("xs"), Col("i")))
	if e != nil {
		t.Fatal(e)
	}
	probe := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	pw, e := NewWorkspace(context.Background(), p, probe)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pw.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	start := probe.calls
	if _, e = pw.Evaluate(context.Background(), b, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	calls := probe.calls - start
	pw.Close()
	if calls < 2 {
		t.Fatal("missing owned admissions")
	}
	for fail := 1; fail <= calls; fail++ {
		m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e != nil {
			t.Fatal(e)
		}
		before := m.Used()
		m.fail = m.calls + fail
		if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("reserve", fail, e)
		}
		if m.Used() != before {
			t.Fatal("failed replacement lost old charge", fail, m.Used(), before)
		}
		m.fail = 0
		got, e := w.Evaluate(context.Background(), b, testKernelSet())
		if e != nil || got[0].Struct.Values.Int64s["code.中"].Values[2] != 22 {
			t.Fatal("retry", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}
func TestListStructBOperatorCancel(t *testing.T) {
	schema, b := listStructExprFixture(t, memory.TypeInt64, 65)
	f := schema.Fields[0]
	input := scalarRegister{typ: memory.TypeList, structList: b.StructLists["xs"], validity: b.StructLists["xs"].Validity}
	index := scalarRegister{typ: memory.TypeInt64, i64: b.Int64s["i"].Values, validity: b.Int64s["i"].Validity}
	m := memory.NewBudgetManager(0)
	out := scalarRegister{typ: memory.TypeStruct}
	out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
	if e := evalStructListGet(&out, &input, &index, f, 65, nil); e != nil {
		t.Fatal(e)
	}
	checks := 0
	out.checkContext = func() error { checks++; return nil }
	if e := evalStructListGet(&out, &input, &index, f, 65, nil); e != nil {
		t.Fatal(e)
	}
	before, _ := json.Marshal(out.strct)
	charge := m.Used()
	for _, stop := range []int{1, checks / 2, checks - 1} {
		seen := 0
		out.checkContext = func() error {
			seen++
			if seen >= stop {
				return context.Canceled
			}
			return nil
		}
		e := evalStructListGet(&out, &input, &index, f, 65, nil)
		if !errors.Is(e, context.Canceled) {
			t.Fatal("operator checkpoint", stop, e)
		}
		after, _ := json.Marshal(out.strct)
		if string(after) != string(before) || m.Used() != charge {
			t.Fatal("partial owner replacement", stop, m.Used(), charge)
		}
	}
	out.structGetReservation.Release()
	if m.Used() != 0 {
		t.Fatal("operator leak")
	}
}
func TestListStructBGuard(t *testing.T) {
	schema, b := listStructExprFixture(t, memory.TypeInt64, 65)
	value := StructGet(ListGet(Col("xs"), Col("i")), "code.中")
	p, e := Compile(schema, If(GT(Col("i"), I64(0)), value, I64(-17)))
	if e != nil {
		t.Fatal(e)
	}
	w, e := NewWorkspace(context.Background(), p, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	got, e := w.Evaluate(context.Background(), b, testKernelSet())
	if e != nil {
		t.Fatal(e)
	}
	for row := 0; row < 65; row++ {
		take := row%11 != 0 && row%6 >= 1 && row%6 <= 3
		valid := true
		want := int64(-17)
		if take {
			valid = row%7 != 0 && row%5 != 0 && row%6 == 2
			want = int64(row*10 + row%6)
		}
		if got[0].Int64.IsValid(row) != valid || valid && got[0].Int64.Values[row] != want {
			t.Fatal("guard", row)
		}
	}
}

func TestListStructBGuardErrorDomains(t *testing.T) {
	for _, broken := range []string{"required_item", "required_child"} {
		for _, form := range []string{"if", "case", "coalesce"} {
			t.Run(broken+"/"+form, func(t *testing.T) {
				f, e := memory.ListStructField("xs", false, false, memory.Field{Name: "n", Type: memory.TypeInt64})
				if e != nil {
					t.Fatal(e)
				}
				schema, e := memory.NewSchemaWithStructListsChecked(f, memory.Field{Name: "keep", Type: memory.TypeBool}, memory.Field{Name: "first", Type: memory.TypeInt64, Nullable: true})
				if e != nil {
					t.Fatal(e)
				}
				child := memory.NewBatch()
				child.Rows = 2
				child.Int64s["n"] = &memory.Int64Vector{Values: []int64{7, 8}}
				b := memory.NewBatch()
				b.Rows = 2
				b.StructLists["xs"] = &memory.StructListVector{Offsets: []uint32{0, 1, 2}, Values: &memory.StructVector{Rows: 2, Fields: f.Children[0].Children, Values: child, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
				b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{0, 0}}
				b.Int64s["first"] = &memory.Int64Vector{Values: []int64{99, 99}}
				get := StructGet(ListGet(Col("xs"), I64(0)), "n")
				var guard Expr = If(Col("keep"), get, I64(99))
				if form == "case" {
					guard = Case([]WhenThen{{When: Col("keep"), Then: get}}, I64(99))
				}
				if form == "coalesce" {
					guard = Coalesce(Col("first"), get, I64(99))
				}
				p, e := Compile(schema, guard)
				if e != nil {
					t.Fatal(e)
				}
				w, e := NewWorkspace(context.Background(), p, nil)
				if e != nil {
					t.Fatal(e)
				}
				defer w.Close()
				for reuse := 0; reuse < 4; reuse++ {
					active := reuse % 2
					mask := memory.Bitmap{uint64(1) << uint(active)}
					b.Uint32s["keep"].Values[0] = 0
					b.Uint32s["keep"].Values[1] = 0
					b.Uint32s["keep"].Values[active] = 1
					b.Int64s["first"].Validity = memory.Bitmap{3 ^ mask[0]}
					if broken == "required_item" {
						b.StructLists["xs"].Values.Validity = mask
					} else {
						child.Int64s["n"].Validity = mask
					}
					got, e := w.Evaluate(context.Background(), b, testKernelSet())
					if e != nil {
						t.Fatal("inactive required NULL not suppressed", reuse, e)
					}
					for row := 0; row < 2; row++ {
						want := int64(99)
						if row == active {
							want = int64(7 + row)
						}
						if !got[0].Int64.IsValid(row) || got[0].Int64.Values[row] != want {
							t.Fatal("same-address guard oracle", reuse, row)
						}
					}
				}
				unguarded, e := Compile(schema, get)
				if e != nil {
					t.Fatal(e)
				}
				uw, e := NewWorkspace(context.Background(), unguarded, nil)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = uw.Evaluate(context.Background(), b, testKernelSet()); e == nil {
					uw.Close()
					t.Fatal("active required NULL accepted")
				}
				uw.Close()
				// Entirely unrequested branches are not evaluated. Once any row requests
				// this opcode, structural checks include the inactive rows too.
				for _, bad := range []string{"offset", "bitmap"} {
					b.StructLists["xs"].Values.Validity = nil
					child.Rows = 2
					b.StructLists["xs"].Values.Rows = 2
					child.Int64s["n"].Values = []int64{7, 8}
					child.Int64s["n"].Validity = nil
					b.StructLists["xs"].Offsets = []uint32{0, 1, 0}
					if bad == "bitmap" {
						child.Rows = 65
						b.StructLists["xs"].Values.Rows = 65
						child.Int64s["n"].Values = make([]int64, 65)
						child.Int64s["n"].Values[0] = 7
						child.Int64s["n"].Validity = memory.Bitmap{1}
						b.StructLists["xs"].Offsets = []uint32{0, 1, 65}
					}
					b.Uint32s["keep"].Values = []uint32{0, 0}
					b.Int64s["first"].Validity = nil
					dead, e := NewWorkspace(context.Background(), p, nil)
					if e != nil {
						t.Fatal(e)
					}
					got, e := dead.Evaluate(context.Background(), b, testKernelSet())
					if e != nil {
						dead.Close()
						t.Fatal("fully inactive must short circuit", e)
					}
					if got[0].Int64.Values[0] != 99 || got[0].Int64.Values[1] != 99 {
						t.Fatal("dead branch control")
					}
					dead.Close()
					b.Uint32s["keep"].Values = []uint32{1, 0}
					b.Int64s["first"].Validity = memory.Bitmap{2}
					mixed, e := NewWorkspace(context.Background(), p, nil)
					if e != nil {
						t.Fatal(e)
					}
					if _, e = mixed.Evaluate(context.Background(), b, testKernelSet()); e == nil {
						mixed.Close()
						t.Fatal("executed opcode ignored inactive malformed storage", bad)
					}
					mixed.Close()
				}

			})
		}
	}
}

func TestListStructBLongStringCopyCancel(t *testing.T) {
	f, e := memory.ListStructField("xs", false, false, memory.Field{Name: "s", Type: memory.TypeString})
	if e != nil {
		t.Fatal(e)
	}
	payload := []byte(strings.Repeat("x", 2<<20))
	child := memory.NewBatch()
	child.Rows = 1
	child.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(payload))}, Data: payload}
	v := &memory.StructListVector{Offsets: []uint32{0, 1}, Values: &memory.StructVector{Rows: 1, Fields: f.Children[0].Children, Values: child, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
	input := scalarRegister{typ: memory.TypeList, structList: v}
	index := scalarRegister{typ: memory.TypeInt64, i64: []int64{0}}
	m := memory.NewBudgetManager(0)
	out := scalarRegister{typ: memory.TypeStruct}
	out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
	if e = evalStructListGet(&out, &input, &index, f, 1, nil); e != nil {
		t.Fatal(e)
	}
	charge := m.Used()
	old := out.strct
	payload[0] = 'Z'
	reservations, checks := 0, 0
	out.acquire = func(n int64) (memory.Reservation, error) {
		r, e := m.Reserve(context.Background(), n)
		if e == nil {
			reservations++
		}
		return r, e
	}
	out.checkContext = func() error {
		if reservations == 3 {
			checks++
			if checks == 32 {
				return context.Canceled
			}
		}
		return nil
	}
	e = evalStructListGet(&out, &input, &index, f, 1, nil)
	if !errors.Is(e, context.Canceled) || reservations != 3 || checks != 32 {
		t.Fatalf("post-admission copy cancel err%v reserve%d checks%d", e, reservations, checks)
	}
	if out.strct != old || out.strct.Values.Strings["s"].Data[0] != 'x' || m.Used() != charge {
		t.Fatal("failed long copy replaced owner")
	}
	out.structGetReservation.Release()
	if m.Used() != 0 {
		t.Fatal("long copy leak")
	}
	t.Log("canceled after third reservation, at copy checkpoint32; retained old owner until explicit release")
}

func TestListStructBStructGetReplacement(t *testing.T) {
	base, b := listStructExprFixture(t, memory.TypeInt64, 65)
	inner := memory.NewBatch()
	inner.Rows = 65
	inner.StructLists["xs"] = b.StructLists["xs"]
	field := memory.StructField("outer", false, base.Fields[0])
	schema, e := memory.NewSchemaWithStructListsChecked(field)
	if e != nil {
		t.Fatal(e)
	}
	root := memory.NewBatch()
	root.Rows = 65
	root.Structs["outer"] = &memory.StructVector{Rows: 65, Fields: field.Children, Values: inner, Encoding: memory.EncodingView}
	p, e := Compile(schema, StructGet(Col("outer"), "xs"))
	if e != nil {
		t.Fatal(e)
	}
	probe := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	w, e := NewWorkspace(context.Background(), p, probe)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), root, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	start := probe.calls
	if _, e = w.Evaluate(context.Background(), root, testKernelSet()); e != nil {
		t.Fatal(e)
	}
	calls := probe.calls - start
	w.Close()
	if calls < 2 {
		t.Fatal("missing list owner admissions")
	}
	for fail := 1; fail <= calls; fail++ {
		m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Evaluate(context.Background(), root, testKernelSet()); e != nil {
			t.Fatal(e)
		}
		charge := m.Used()
		m.fail = m.calls + fail
		if _, e = w.Evaluate(context.Background(), root, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(fail, e)
		}
		if m.Used() != charge {
			t.Fatal("old list owner changed")
		}
		m.fail = 0
		got, e := w.Evaluate(context.Background(), root, testKernelSet())
		if e != nil || !got[0].StructList.IsValid(2) {
			t.Fatal("list retry", e)
		}
		w.Close()
		if m.Used() != 0 {
			t.Fatal("list owner leak")
		}
	}
}
