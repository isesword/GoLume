package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"strings"
	"testing"
)

func nestedListExprRaw(row int, text bool) any {
	switch row % 9 {
	case 0:
		return nil
	case 1:
		return []any{}
	case 2:
		return []any{nil}
	case 3:
		return []any{[]any{}}
	case 4:
		return []any{[]any{nil}}
	}
	value := func(i int) any {
		if text {
			return []string{"", "中🙂", "a\x00b"}[(row+i)%3]
		}
		return int64(row*10 + i)
	}
	return []any{[]any{value(0), nil, value(2)}, []any{}, nil, []any{value(3)}}
}
func nestedListExprFixture(t *testing.T, text bool, typ memory.DataType, n int) (memory.Schema, *memory.Batch) {
	t.Helper()
	_, b := listGetFixture(typ, false, n)
	delete(b.Int64Lists, "xs")
	inner := memory.ListInt64Field("item", true, true)
	if text {
		inner = memory.ListStringField("item", true, true)
	}
	f, e := memory.NestedListField("xs", true, inner)
	if e != nil {
		t.Fatal(e)
	}
	root := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0}, Validity: make(memory.Bitmap, memory.WordsFor(n)), Encoding: memory.EncodingView}
	middle := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0}, Encoding: memory.EncodingView}
	root.Child = middle
	var middleValid, leafValid []bool
	var nums []int64
	var texts []string
	for row := 0; row < n; row++ {
		raw := nestedListExprRaw(row, text)
		if raw != nil {
			memory.Set(root.Validity, row)
			for _, v := range raw.([]any) {
				middleValid = append(middleValid, v != nil)
				if v != nil {
					for _, x := range v.([]any) {
						leafValid = append(leafValid, x != nil)
						if text {
							a := ""
							if x != nil {
								a = x.(string)
							}
							texts = append(texts, a)
						} else {
							var a int64
							if x != nil {
								a = x.(int64)
							}
							nums = append(nums, a)
						}
					}
				}
				middle.Offsets = append(middle.Offsets, uint32(len(leafValid)))
			}
		}
		root.Offsets = append(root.Offsets, uint32(len(middleValid)))
	}
	middle.Validity = make(memory.Bitmap, memory.WordsFor(len(middleValid)))
	for i, v := range middleValid {
		if v {
			memory.Set(middle.Validity, i)
		}
	}
	if text {
		middle.Child = &memory.ArrayNode{Kind: memory.TypeString, Strings: testStringVector(texts, leafValid), Encoding: memory.EncodingView}
	} else {
		bits := make(memory.Bitmap, memory.WordsFor(len(nums)))
		for i, v := range leafValid {
			if v {
				memory.Set(bits, i)
			}
		}
		middle.Child = &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Values: nums, Validity: bits}, Encoding: memory.EncodingPlain}
	}
	b.Arrays["xs"] = root
	sc, e := memory.NewSchemaWithNestedListsChecked(f, memory.Field{Name: "i", Type: typ, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	return sc, b
}
func nestedListInnerValue(v ScalarValue, row int, text bool) any {
	if text {
		lo, hi, ok := v.StringList.ValueRange(row)
		if !ok {
			return nil
		}
		out := make([]any, hi-lo)
		for i := range out {
			if v.StringList.Values.IsValid(int(lo) + i) {
				out[i] = string(v.StringList.Values.ValueBytes(int(lo) + i))
			}
		}
		return out
	}
	lo, hi, ok := v.Int64List.ValueRange(row)
	if !ok {
		return nil
	}
	out := make([]any, hi-lo)
	for i := range out {
		if v.Int64List.Values.IsValid(int(lo) + i) {
			out[i] = v.Int64List.Values.Values[int(lo)+i]
		}
	}
	return out
}
func TestNestedListBExpr(t *testing.T) {
	for _, text := range []bool{false, true} {
		for _, typ := range listGetIndexTypes {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("text%v/%s/m%d", text, typ, mode), func(t *testing.T) {
					sc, b := nestedListExprFixture(t, text, typ, 130)
					inner := ListGet(Col("xs"), Col("i"))
					p, e := CompileExpressions(sc, []NamedExpr{ListLength(Col("xs")).Alias("outerlen"), inner.Alias("inner"), ListLength(inner).Alias("innerlen"), ListGet(inner, I64(0)).Alias("scalar"), inner.Alias("again")})
					if e != nil {
						t.Fatal(e)
					}
					if p.SupportsSelection() {
						t.Fatal("selected expanded")
					}
					f := p.OutputField(1)
					if f.Type != memory.TypeList || !f.Nullable || len(f.Children) != 1 || !f.Children[0].Nullable {
						t.Fatal("inner metadata", f)
					}
					f.Children[0].Name = "changed"
					if p.OutputField(1).Children[0].Name != "item" {
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
					for row := 0; row < 130; row++ {
						raw := nestedListExprRaw(row, text)
						if got[0].Int64.IsValid(row) != (raw != nil) {
							t.Fatal("outer validity", row)
						}
						if raw != nil && got[0].Int64.Values[row] != int64(len(raw.([]any))) {
							t.Fatal("outer length", row)
						}
						var want any
						idx := row % 6
						if raw != nil && row%11 != 0 && idx < 4 && idx < len(raw.([]any)) {
							want = raw.([]any)[idx]
						}
						if !reflect.DeepEqual(nestedListInnerValue(got[1], row, text), want) || !reflect.DeepEqual(nestedListInnerValue(got[4], row, text), want) {
							t.Fatal("inner oracle", row)
						}
						if got[2].Int64.IsValid(row) != (want != nil) {
							t.Fatal("inner validity", row)
						}
						if want != nil && got[2].Int64.Values[row] != int64(len(want.([]any))) {
							t.Fatal("inner length", row)
						}
						var scalar any
						if want != nil && len(want.([]any)) > 0 {
							scalar = want.([]any)[0]
						}
						if text {
							if got[3].String.IsValid(row) != (scalar != nil) || (scalar != nil && string(got[3].String.ValueBytes(row)) != scalar.(string)) {
								t.Fatal("text scalar", row)
							}
						} else if got[3].Int64.IsValid(row) != (scalar != nil) || (scalar != nil && got[3].Int64.Values[row] != scalar.(int64)) {
							t.Fatal("int scalar", row)
						}
					}
					// Result must own payload before the next Evaluate invalidates its view.
					originalInner := nestedListInnerValue(got[1], 6, text)
					if text {
						leaf := b.Arrays["xs"].Child.Child.Strings
						saved := append([]byte(nil), leaf.Data...)
						for i := range leaf.Data {
							leaf.Data[i] = 'q'
						}
						if !reflect.DeepEqual(nestedListInnerValue(got[1], 6, text), originalInner) {
							t.Fatal("input String alias")
						}
						copy(leaf.Data, saved)
					} else {
						leaf := b.Arrays["xs"].Child.Child.Int64
						saved := append([]int64(nil), leaf.Values...)
						for i := range leaf.Values {
							leaf.Values[i] = -999
						}
						if !reflect.DeepEqual(nestedListInnerValue(got[1], 6, text), originalInner) {
							t.Fatal("input int alias")
						}
						copy(leaf.Values, saved)
					}
					// Reuse exactly the same Batch address with a different requested element.
					if typ == memory.TypeInt64 {
						for i := range b.Int64s["i"].Values {
							b.Int64s["i"].Values[i] = 1
						}
						got, e = w.Evaluate(context.Background(), b, kernel.New(mode))
						if e != nil {
							t.Fatal(e)
						}
						if !reflect.DeepEqual(nestedListInnerValue(got[1], 6, text), []any{}) {
							t.Fatal("same address stale cache")
						}
					}
					w.Close()
					if m.Used() != 0 {
						t.Fatal("workspace leak")
					}
				})
			}
		}
	}
}
func TestNestedListBReplacement(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			sc, b := nestedListExprFixture(t, text, memory.TypeInt64, 65)
			p, e := Compile(sc, ListGet(Col("xs"), Col("i")))
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
			if calls < 1 {
				t.Fatal("no owned admission")
			}
			for fail := 1; fail <= calls; fail++ {
				m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
				w, e := NewWorkspace(context.Background(), p, m)
				if e != nil {
					t.Fatal(e)
				}
				_, e = w.Evaluate(context.Background(), b, testKernelSet())
				if e != nil {
					t.Fatal(e)
				}
				before := m.Used()
				m.fail = m.calls + fail
				if _, e = w.Evaluate(context.Background(), b, testKernelSet()); !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("second owner rejection", fail, e)
				}
				// Public output views expire on the next Evaluate, including a failed call.
				// Direct operator payload atomicity is independently tested below.
				if m.Used() != before {
					t.Fatalf("old owner charge lost fail=%d charge=%d before=%d", fail, m.Used(), before)
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
				if m.Used() != 0 {
					t.Fatal("public cancel failed to close")
				}
				w.Close()
			}
		})
	}
}

func TestNestedListBOperatorAtomicity(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprint(text), func(t *testing.T) {
			sc, b := nestedListExprFixture(t, text, memory.TypeInt64, 65)
			input := scalarRegister{typ: memory.TypeList, array: b.Arrays["xs"], validity: b.Arrays["xs"].Validity}
			index := scalarRegister{typ: memory.TypeInt64, i64: b.Int64s["i"].Values, validity: b.Int64s["i"].Validity}
			m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
			out := scalarRegister{typ: memory.TypeList}
			out.acquire = func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }
			if e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil); e != nil {
				t.Fatal(e)
			}
			checks := 0
			out.checkContext = func() error { checks++; return nil }
			start := m.calls
			if e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil); e != nil {
				t.Fatal(e)
			}
			calls := m.calls - start
			successfulChecks := checks
			original := nestedListInnerValue(scalarValue(&out, 65), 6, text)
			charge := m.Used()
			for fail := 1; fail <= calls; fail++ {
				m.fail = m.calls + fail
				e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil)
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("direct budget", fail, e)
				}
				if m.Used() != charge || !reflect.DeepEqual(nestedListInnerValue(scalarValue(&out, 65), 6, text), original) {
					t.Fatal("direct lost owner", fail)
				}
				m.fail = 0
			}
			for _, stop := range []int{1, successfulChecks / 2, successfulChecks - 1} {
				seen := 0
				out.checkContext = func() error {
					seen++
					if seen >= stop {
						return context.Canceled
					}
					return nil
				}
				e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil)
				if !errors.Is(e, context.Canceled) {
					t.Fatal("direct cancel", stop, e)
				}
				if m.Used() != charge || !reflect.DeepEqual(nestedListInnerValue(scalarValue(&out, 65), 6, text), original) {
					t.Fatal("direct cancel replaced owner", stop)
				}
			}
			out.structGetReservation.Release()
			if m.Used() != 0 {
				t.Fatal("operator leak")
			}
		})
	}
}

func TestNestedListBBoundaries(t *testing.T) {
	sc, b := nestedListExprFixture(t, false, memory.TypeInt64, 65)
	for _, bad := range []Expr{JSONDecode(Str("{}"), sc)} {
		if _, e := Compile(sc, bad); e == nil {
			t.Fatal("unexpanded constructor")
		}
	}
	p, e := Compile(sc, ListGet(Col("xs"), Col("i")))
	if e != nil {
		t.Fatal(e)
	}
	w, e := NewWorkspace(context.Background(), p, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Close()
	if _, e = w.EvaluateSelected(context.Background(), b, []int{1, 6}, testKernelSet()); e == nil {
		t.Fatal("selected expanded")
	}
}
func TestNestedListBGuardDomains(t *testing.T) {
	for _, broken := range []string{"inner_null", "leaf_null", "bad_offsets"} {
		for _, form := range []string{"if", "case", "coalesce"} {
			t.Run(broken+"/"+form, func(t *testing.T) {
				f, e := memory.NestedListField("xs", false, memory.ListInt64Field("item", false, false))
				if e != nil {
					t.Fatal(e)
				}
				sc, e := memory.NewSchemaWithNestedListsChecked(f, memory.Field{Name: "keep", Type: memory.TypeBool}, memory.Field{Name: "first", Type: memory.TypeInt64, Nullable: true})
				if e != nil {
					t.Fatal(e)
				}
				b := memory.NewBatch()
				b.Rows = 2
				leaf := &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Values: []int64{7, 8}}}
				mid := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 2}, Child: leaf}
				root := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 2}, Child: mid}
				b.Arrays["xs"] = root
				b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{1, 0}}
				b.Int64s["first"] = &memory.Int64Vector{Values: []int64{99, 99}, Validity: memory.Bitmap{2}}
				value := ListGet(ListGet(Col("xs"), I64(0)), I64(0))
				var guard Expr = If(Col("keep"), value, I64(99))
				if form == "case" {
					guard = Case([]WhenThen{{When: Col("keep"), Then: value}}, I64(99))
				}
				if form == "coalesce" {
					guard = Coalesce(Col("first"), value, I64(99))
				}
				p, e := Compile(sc, guard)
				if e != nil {
					t.Fatal(e)
				}
				w, e := NewWorkspace(context.Background(), p, nil)
				if e != nil {
					t.Fatal(e)
				}
				defer w.Close()
				if broken == "inner_null" {
					mid.Validity = memory.Bitmap{1}
				}
				if broken == "leaf_null" {
					leaf.Int64.Validity = memory.Bitmap{1}
				}
				if broken == "bad_offsets" {
					mid.Offsets[2] = 3
				}
				if broken == "bad_offsets" {
					if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e == nil {
						t.Fatal("hidden bad storage accepted")
					}
					w.Close()
					w, e = NewWorkspace(context.Background(), p, nil)
					if e != nil {
						t.Fatal(e)
					}
					defer w.Close()
					b.Uint32s["keep"].Values[0] = 0
					b.Int64s["first"].Validity = nil
					got, e := w.Evaluate(context.Background(), b, testKernelSet())
					if e != nil {
						t.Fatal("fully dead branch should skip", e)
					}
					for r := 0; r < 2; r++ {
						if !got[0].Int64.IsValid(r) || got[0].Int64.Values[r] != 99 {
							t.Fatal("dead oracle")
						}
					}
					return
				}
				for attempt := 0; attempt < 2; attempt++ {
					got, e := w.Evaluate(context.Background(), b, testKernelSet())
					if e != nil {
						t.Fatal("inactive value domain", e)
					}
					for r, want := range []int64{7, 99} {
						if !got[0].Int64.IsValid(r) || got[0].Int64.Values[r] != want {
							t.Fatal("guard values", r)
						}
					}
				}
				b.Uint32s["keep"].Values[1] = 1
				b.Int64s["first"].Validity = memory.Bitmap{0}
				if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e == nil {
					t.Fatal("active required null accepted")
				}
			})
		}
	}
}

func TestNestedListBLongCopyCancellation(t *testing.T) {
	sc, b := nestedListExprFixture(t, true, memory.TypeInt64, 65)
	root := b.Arrays["xs"]
	oldLeaf := root.Child.Child.Strings
	texts := make([]string, oldLeaf.Len())
	valid := make([]bool, len(texts))
	for i := range texts {
		valid[i] = oldLeaf.IsValid(i)
		if valid[i] {
			texts[i] = string(oldLeaf.ValueBytes(i))
		}
	}
	leafIndex := int(root.Child.Offsets[root.Offsets[6]])
	texts[leafIndex] = strings.Repeat("z", 2<<20)
	valid[leafIndex] = true
	root.Child.Child.Strings = testStringVector(texts, valid)
	input := scalarRegister{typ: memory.TypeList, array: root, validity: root.Validity}
	index := scalarRegister{typ: memory.TypeInt64, i64: b.Int64s["i"].Values, validity: b.Int64s["i"].Validity}
	m := memory.NewBudgetManager(0)
	out := scalarRegister{typ: memory.TypeList}
	admitted := false
	post := 0
	out.acquire = func(n int64) (memory.Reservation, error) {
		r, e := m.Reserve(context.Background(), n)
		if e == nil && n > 1<<20 {
			admitted = true
		}
		return r, e
	}
	out.checkContext = func() error {
		if admitted {
			post++
		}
		return nil
	}
	if e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil); e != nil {
		t.Fatal(e)
	}
	if !admitted || post < 64 {
		t.Fatal("large owned copy checkpoints absent", post)
	}
	old := nestedListInnerValue(scalarValue(&out, 65), 6, true)
	charge := m.Used()
	admitted = false
	stop := post / 2
	post = 0
	out.checkContext = func() error {
		if admitted {
			post++
			if post >= stop {
				return context.Canceled
			}
		}
		return nil
	}
	if e := evalNestedListGet(&out, &input, &index, sc.Fields[0], 65, nil); !errors.Is(e, context.Canceled) {
		t.Fatal("post-admission copy cancel", e)
	}
	if !admitted || m.Used() != charge || !reflect.DeepEqual(nestedListInnerValue(scalarValue(&out, 65), 6, true), old) {
		t.Fatal("cancel copy mutated owner")
	}
	out.structGetReservation.Release()
	if m.Used() != 0 {
		t.Fatal("copy owner leak")
	}
}

// Post-review negative control: index payload validation was already added by
// the runtime author; no pre-fix panic/red result is claimed for this test.
func TestNestedListBShortIndexStorage(t *testing.T) {
	for _, typ := range listGetIndexTypes {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			sc, b := nestedListExprFixture(t, false, typ, 65)
			switch typ {
			case memory.TypeInt8:
				b.Int8s["i"].Values = b.Int8s["i"].Values[:1]
			case memory.TypeInt16:
				b.Int16s["i"].Values = b.Int16s["i"].Values[:1]
			case memory.TypeInt32:
				b.Int32s["i"].Values = b.Int32s["i"].Values[:1]
			case memory.TypeInt64:
				b.Int64s["i"].Values = b.Int64s["i"].Values[:1]
			case memory.TypeUint8:
				b.Uint8s["i"].Values = b.Uint8s["i"].Values[:1]
			case memory.TypeUint16:
				b.Uint16s["i"].Values = b.Uint16s["i"].Values[:1]
			case memory.TypeUint32:
				b.Uint32s["i"].Values = b.Uint32s["i"].Values[:1]
			case memory.TypeUint64:
				b.Uint64s["i"].Values = b.Uint64s["i"].Values[:1]
			}
			p, e := Compile(sc, ListGet(Col("xs"), Col("i")))
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(0)
			w, e := NewWorkspace(context.Background(), p, m)
			if e != nil {
				t.Fatal(e)
			}
			defer w.Close()
			if _, e = w.Evaluate(context.Background(), b, testKernelSet()); e == nil {
				t.Fatal("short index payload accepted")
			}
			if m.Used() != 0 {
				t.Fatal("invalid index workspace not closed")
			}
		})
	}
}
