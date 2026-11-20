package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Literal three-axis fixture: R0=7, R1=7, R2=8. NULL outer0
// and inner1 deliberately retain raw hidden spans on input.
func nestedListTestFixture(kind DataType) (Field, *ArrayNode) {
	leaf := Field{Name: "item", Type: kind, Nullable: true}
	if kind == TypeString {
		leaf.Encoding = EncodingView
	}
	f := Field{Name: "nested", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{{Name: "item", Type: TypeList, Nullable: true, Encoding: EncodingView, Children: []Field{leaf}}}}
	terminal := &ArrayNode{Kind: kind, Encoding: EncodingView}
	if kind == TypeInt64 {
		terminal.Int64 = &Int64Vector{Values: []int64{91, 92, -9223372036854775808, 93, 9223372036854775807, 0, 94, -7}, Validity: Bitmap{247}}
	} else {
		texts := []string{"outer-hidden", "inner-hidden", "界", "leaf-hidden", "", "a\x00b", "ancestor-hidden", "尾🙂"}
		off := []uint32{0}
		var data []byte
		for _, s := range texts {
			data = append(data, s...)
			off = append(off, uint32(len(data)))
		}
		terminal.Strings = &StringViewVector{Offsets: off, Data: data, Validity: Bitmap{247}, Encoding: EncodingView}
	}
	inner := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1, 2, 2, 5, 6, 7, 8}, Validity: Bitmap{125}, Child: terminal, Encoding: EncodingView}
	return f, &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1, 1, 2, 3, 5, 6, 7}, Validity: Bitmap{126}, Child: inner, Encoding: EncodingView}
}
func nestedListTestExpected(kind DataType) []any {
	var a, b, c, d any = int64(-9223372036854775808), int64(9223372036854775807), int64(0), int64(-7)
	if kind == TypeString {
		a, b, c, d = "界", "", "a\x00b", "尾🙂"
	}
	return []any{nil, []any{}, []any{nil}, []any{[]any{}}, []any{[]any{a, nil, b}, []any{c}}, []any{[]any{func() any {
		if kind == TypeString {
			return "ancestor-hidden"
		}
		return int64(94)
	}()}}, []any{[]any{d}}}
}
func nestedListTestRead(n *ArrayNode, row int) any {
	switch n.Kind {
	case TypeList:
		if !n.IsValid(row) {
			return nil
		}
		lo, hi := n.Offsets[row], n.Offsets[row+1]
		out := make([]any, hi-lo)
		for i := lo; i < hi; i++ {
			out[i-lo] = nestedListTestRead(n.Child, int(i))
		}
		return out
	case TypeInt64:
		if !n.Int64.IsValid(row) {
			return nil
		}
		return n.Int64.Values[row]
	case TypeString:
		if !n.Strings.IsValid(row) {
			return nil
		}
		return string(n.Strings.Data[n.Strings.Offsets[row]:n.Strings.Offsets[row+1]])
	}
	panic("unexpected test kind")
}
func nestedListTestCheck(t *testing.T, o *OwnedNestedList, kind DataType, rows []int, ancestor bool) {
	t.Helper()
	want := nestedListTestExpected(kind)
	if ancestor {
		want[5] = nil
	}
	if o.Vector.Len() != len(rows) {
		t.Fatalf("R0 count %d want%d", o.Vector.Len(), len(rows))
	}
	r1, r2 := 0, 0
	for i, r := range rows {
		if got := nestedListTestRead(o.Vector, i); !reflect.DeepEqual(got, want[r]) {
			t.Fatalf("row%d source%d got%#v want%#v", i, r, got, want[r])
		}
		if want[r] != nil {
			for _, in := range want[r].([]any) {
				r1++
				if in != nil {
					r2 += len(in.([]any))
				}
			}
		}
	}
	if len(o.Vector.Offsets) != len(rows)+1 || o.Vector.Offsets[0] != 0 || int(o.Vector.Offsets[len(rows)]) != r1 {
		t.Fatal("R0 offset cardinality")
	}
	if len(o.Vector.Child.Offsets) != r1+1 || o.Vector.Child.Offsets[0] != 0 || int(o.Vector.Child.Offsets[r1]) != r2 {
		t.Fatal("R1 offset cardinality")
	}
	if o.Vector.Child.Child.Len() != r2 {
		t.Fatal("R2 cardinality")
	}
	if err := o.Vector.ValidateWithContext(o.Field, len(rows), NestedListParentDomain{}, nil); err != nil {
		t.Fatal(err)
	}
}
func TestNestedListOwnedRangeTake(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		for _, ancestor := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ancestor%v", kind, ancestor), func(t *testing.T) {
				f, src := nestedListTestFixture(kind)
				domain := NestedListParentDomain{}
				if ancestor {
					var e error
					domain, e = NewNestedListParentDomain(7, Bitmap{95})
					if e != nil {
						t.Fatal(e)
					}
				}
				for _, rows := range [][]int{{0, 1, 2, 3, 4, 5, 6}, {6, 4, 2, 4, 0, 3}, {}, {5}} {
					m := NewBudgetManager(0)
					o, e := TakeNestedListRowsOwned(context.Background(), m, src, f, domain, rows)
					if e != nil {
						t.Fatal(e)
					}
					nestedListTestCheck(t, o, kind, rows, ancestor)
					charge := o.Bytes
					if charge <= 0 || m.Used() != charge {
						t.Fatalf("charge %d/%d", charge, m.Used())
					}
					o.Close()
					o.Close()
					if m.Used() != 0 || o.Vector != nil || !reflect.DeepEqual(o.Field, Field{}) {
						t.Fatal("Close ownership")
					}
					exact := NewBudgetManager(charge)
					o, e = TakeNestedListRowsOwned(context.Background(), exact, src, f, domain, rows)
					if e != nil {
						t.Fatal(e)
					}
					o.Close()
					short := NewBudgetManager(charge - 1)
					o, e = TakeNestedListRowsOwned(context.Background(), short, src, f, domain, rows)
					if e == nil || o != nil || short.Used() != 0 {
						t.Fatal("exact minus one")
					}
				}
				for _, r := range [][2]int{{0, 7}, {2, 4}, {7, 0}, {0, 0}} {
					m := NewBudgetManager(0)
					o, e := CloneNestedListRangeOwned(context.Background(), m, src, f, domain, r[0], r[1])
					if e != nil {
						t.Fatal(e)
					}
					rows := make([]int, r[1])
					for i := range rows {
						rows[i] = r[0] + i
					}
					nestedListTestCheck(t, o, kind, rows, ancestor)
					o.Close()
					if m.Used() != 0 {
						t.Fatal("range leak")
					}
				}
			})
		}
	}
}
func TestNestedListRequiredSelectedDomain(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		for axis := 0; axis < 3; axis++ {
			t.Run(fmt.Sprintf("%s/axis%d", kind, axis), func(t *testing.T) {
				f, v := nestedListTestFixture(kind)
				target := &f
				for i := 0; i < axis; i++ {
					target = &target.Children[0]
				}
				target.Nullable = false
				bad := []int{0, 2, 4}[axis]
				m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
				o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, []int{6})
				if e != nil {
					t.Fatal(e)
				}
				o.Close()
				m.calls = 0
				o, e = TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, []int{bad})
				if e == nil || o != nil || m.calls != 0 || m.Used() != 0 {
					t.Fatalf("required visible row %d %v calls%d", bad, e, m.calls)
				}
				domain, e := NewNestedListParentDomain(7, Bitmap{uint64(127) &^ (uint64(1) << bad)})
				if e != nil {
					t.Fatal(e)
				}
				o, e = TakeNestedListRowsOwned(context.Background(), m, v, f, domain, []int{bad})
				if e != nil {
					t.Fatal(e)
				}
				if o.Vector.IsValid(0) {
					t.Fatal("ancestor hidden")
				}
				o.Close()
			})
		}
	}
}
func TestNestedListHiddenStructureReject(t *testing.T) {
	cases := map[string]func(*ArrayNode){
		"root_base": func(v *ArrayNode) { v.Offsets[0] = 1 }, "inner_base": func(v *ArrayNode) { v.Child.Offsets[0] = 1 }, "root_short": func(v *ArrayNode) { v.Offsets = v.Offsets[:7] }, "inner_decrease_hidden": func(v *ArrayNode) { v.Child.Offsets[1] = 3 }, "root_terminal": func(v *ArrayNode) { v.Offsets[7] = ^uint32(0) }, "inner_terminal": func(v *ArrayNode) { v.Child.Offsets[7] = ^uint32(0) }, "cycle": func(v *ArrayNode) { v.Child = v }, "extra": func(v *ArrayNode) { v.Int64 = &Int64Vector{} }, "leaf_extra_child": func(v *ArrayNode) { v.Child.Child.Child = v }, "missing_leaf": func(v *ArrayNode) { v.Child.Child.Int64 = nil }, "short_values": func(v *ArrayNode) { v.Child.Child.Int64.Values = v.Child.Child.Int64.Values[:7] }, "encoding": func(v *ArrayNode) { v.Encoding = EncodingConstant },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f, v := nestedListTestFixture(TypeInt64)
			mutate(v)
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, nil)
			if e == nil || o != nil || m.calls != 0 {
				t.Fatalf("empty selection accepted bad full storage: %v calls%d", e, m.calls)
			}
		})
	}
	f, v := nestedListTestFixture(TypeString)
	v.Child.Child.Strings.Offsets[1] = v.Child.Child.Strings.Offsets[2] + 1
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, nil)
	if e == nil || o != nil || m.calls != 0 {
		t.Fatal("hidden String malformed accepted")
	}
}
func TestNestedListOwnershipAndReserveFailures(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, src := nestedListTestFixture(kind)
			rows := []int{6, 4, 2, 1, 0}
			probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			o, e := TakeNestedListRowsOwned(context.Background(), probe, src, f, NestedListParentDomain{}, rows)
			if e != nil {
				t.Fatal(e)
			}
			calls := probe.calls
			o.Close()
			if calls == 0 {
				t.Fatal("no reservation trace")
			}
			for fail := 1; fail <= calls; fail++ {
				m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
				o, e = TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, rows)
				if e == nil || o != nil || m.Used() != 0 {
					t.Fatal("failure rollback", fail)
				}
				m.failAt = 0
				o, e = TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, rows)
				if e != nil {
					t.Fatal(e)
				}
				nestedListTestCheck(t, o, kind, rows, false)
				o.Close()
			}
			m := NewBudgetManager(0)
			old, e := TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, rows)
			if e != nil {
				t.Fatal(e)
			}
			defer old.Close()
			next, e := TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, []int{4})
			if e != nil {
				t.Fatal(e)
			}
			if m.Used() != old.Bytes+next.Bytes {
				t.Fatal("multi root ledger")
			}
			next.Close()
			src.Offsets[0] = 123
			src.Child.Offsets[0] = 123
			src.Validity[0] = 0
			src.Child.Validity[0] = 0
			f.Children[0].Children[0].Name = "poison"
			if kind == TypeInt64 {
				for i := range src.Child.Child.Int64.Values {
					src.Child.Child.Int64.Values[i] = 99
				}
			} else {
				for i := range src.Child.Child.Strings.Data {
					src.Child.Child.Strings.Data[i] = '!'
				}
			}
			nestedListTestCheck(t, old, kind, rows, false)
			if old.Field.Children[0].Children[0].Name != "item" {
				t.Fatal("schema alias")
			}
			old.Close()
			if m.Used() != 0 {
				t.Fatal("old root leak")
			}
		})
	}
}

func TestNestedListAllNullableCombinations(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		for mask := 0; mask < 8; mask++ {
			t.Run(fmt.Sprintf("%s/%d", kind, mask), func(t *testing.T) {
				leaf := Field{Name: "item", Type: kind, Nullable: mask&4 != 0}
				if kind == TypeString {
					leaf.Encoding = EncodingView
				}
				inner := Field{Name: "item", Type: TypeList, Nullable: mask&2 != 0, Children: []Field{leaf}}
				f, e := NestedListField("x", mask&1 != 0, inner)
				if e != nil {
					t.Fatal(e)
				}
				term := &ArrayNode{Kind: kind}
				var vals []any
				if kind == TypeInt64 {
					term.Int64 = &Int64Vector{Values: []int64{-7, 0, 9}}
					vals = []any{int64(-7), int64(0), int64(9)}
					if mask&4 != 0 {
						term.Int64.Validity = Bitmap{3}
					}
				} else {
					term.Strings = &StringViewVector{Offsets: []uint32{0, 0, 3, 6}, Data: []byte("界尾"), Encoding: EncodingView}
					vals = []any{"", "界", "尾"}
					if mask&4 != 0 {
						term.Strings.Validity = Bitmap{3}
					}
				}
				v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1, 2, 3}, Child: &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1, 2, 3}, Child: term}}
				if mask&1 != 0 {
					v.Validity = Bitmap{6}
				}
				if mask&2 != 0 {
					v.Child.Validity = Bitmap{5}
				}
				m := NewBudgetManager(0)
				o, e := CloneNestedListRangeOwned(context.Background(), m, v, f, NestedListParentDomain{}, 0, 3)
				if e != nil {
					t.Fatal(e)
				}
				defer o.Close()
				for row := 0; row < 3; row++ {
					var want any
					value := vals[row]
					if row == 2 && mask&4 != 0 {
						value = nil
					}
					want = []any{[]any{value}}
					if row == 1 && mask&2 != 0 {
						want = []any{nil}
					}
					if row == 0 && mask&1 != 0 {
						want = nil
					}
					if g := nestedListTestRead(o.Vector, row); !reflect.DeepEqual(g, want) {
						t.Fatalf("row%d %#v want%#v", row, g, want)
					}
				}
				if o.Field.Nullable != (mask&1 != 0) || o.Field.Children[0].Nullable != (mask&2 != 0) || o.Field.Children[0].Children[0].Nullable != (mask&4 != 0) {
					t.Fatal("nullable metadata changed")
				}
				o.Close()
				if m.Used() != 0 {
					t.Fatal("leak")
				}
			})
		}
	}
}
func TestNestedListCheckedSchemaDepthAndSnapshot(t *testing.T) {
	f, _ := nestedListTestFixture(TypeInt64)
	if e := f.ValidateNestedList(); e != nil {
		t.Fatal(e)
	}
	if e := f.ValidateList(); e == nil {
		t.Fatal("old validator unexpectedly expanded")
	}
	if _, e := NewSchemaChecked(f); e == nil {
		t.Fatal("old schema unexpectedly expanded")
	}
	for _, depth := range []int{0, 1, 8, 9} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			field, _ := nestedListTestFixture(TypeInt64)
			for i := 0; i < depth; i++ {
				field = Field{Name: fmt.Sprintf("outer%d", i), Type: TypeStruct, Children: []Field{field}}
			}
			schema, e := NewSchemaWithNestedListsChecked(field)
			if depth == 9 {
				if e == nil {
					t.Fatal("depth9 accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(schema.Fields[0], field) {
				t.Fatal("metadata snapshot mismatch")
			}
			field.Children[0].Name = "poison"
			if schema.Fields[0].Children[0].Name == "poison" {
				t.Fatal("metadata aliased")
			}
		})
	}
	for _, n := range []int{0, 1, 8, 9} {
		anc := make([]Bitmap, n)
		_, e := NewNestedListParentDomain(7, anc...)
		if (e != nil) != (n == 9) {
			t.Fatalf("ancestor count%d %v", n, e)
		}
	}
	cycle := Field{Name: "cycle", Type: TypeStruct}
	cycle.Children = make([]Field, 1)
	cycle.Children[0] = cycle
	if _, e := NewSchemaWithNestedListsChecked(cycle); e == nil {
		t.Fatal("cycle accepted")
	}
	third := Field{Name: "item", Type: TypeList, Children: []Field{f.Children[0]}}
	three, e := NestedListField("three", true, third)
	if e != nil {
		t.Fatal("third List rejected", e)
	}
	three.Name = "item"
	if _, e := NestedListField("four", true, three); e == nil {
		t.Fatal("fourth List accepted")
	}
	for _, kind := range []DataType{TypeStruct, TypeDecimal, TypeBool} {
		inner := Field{Name: "item", Type: TypeList, Children: []Field{{Name: "item", Type: kind}}}
		if _, e := NestedListField("bad", true, inner); e == nil {
			t.Fatal("unsupported terminal", kind)
		}
	}
}

func TestNestedListRunningCancelAndRetry(t *testing.T) {
	text := "xx" + strings.Repeat("界", 44000) + "z"
	f := Field{Name: "long", Type: TypeList, Nullable: true, Children: []Field{{Name: "item", Type: TypeList, Nullable: true, Children: []Field{{Name: "item", Type: TypeString, Nullable: true, Encoding: EncodingView}}}}}
	src := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1}, Child: &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 1}, Child: &ArrayNode{Kind: TypeString, Strings: &StringViewVector{Offsets: []uint32{0, uint32(len(text))}, Data: []byte(text), Encoding: EncodingView}}}}
	m := NewBudgetManager(0)
	old, e := CloneNestedListRangeOwned(context.Background(), m, src, f, NestedListParentDomain{}, 0, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	oldCharge := m.Used()
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".copyNestedListBytes", stop: 2}
	out, e := CloneNestedListRangeOwned(ctx, m, src, f, NestedListParentDomain{}, 0, 1)
	if !errors.Is(e, context.Canceled) || out != nil || ctx.checks != 2 || ctx.chargedAtCancel <= oldCharge || m.Used() != oldCharge {
		t.Fatalf("actual copy cancellation err%v checks%d charged%d old%d live%d", e, ctx.checks, ctx.chargedAtCancel, oldCharge, m.Used())
	}
	if got := string(old.Vector.Child.Child.Strings.Data); got != text {
		t.Fatal("old owner corrupted")
	}
	out, e = TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, []int{0, 0})
	if e != nil {
		t.Fatal(e)
	}
	if got := nestedListTestRead(out.Vector, 1); !reflect.DeepEqual(got, []any{[]any{text}}) {
		t.Fatal("retry/repeated long String")
	}
	out.Close()
	old.Close()
	if m.Used() != 0 {
		t.Fatal("cancel retry leak")
	}
	// Context canceled inside validation, not only at entry, must precede Reserve.
	f2, v := nestedListTestFixture(TypeInt64)
	fm := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	pre := &listStructTestCancelContext{Context: context.Background(), manager: fm, target: ".checkNestedListField", stop: 2}
	out, e = TakeNestedListRowsOwned(pre, fm, v, f2, NestedListParentDomain{}, []int{4})
	if !errors.Is(e, context.Canceled) || out != nil || fm.calls != 0 || pre.checks != 2 {
		t.Fatalf("preflight cancel %v calls%d checks%d", e, fm.calls, pre.checks)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	out, e = TakeNestedListRowsOwned(canceled, fm, v, f2, NestedListParentDomain{}, nil)
	if !errors.Is(e, context.Canceled) || out != nil || fm.calls != 0 {
		t.Fatal("pre-cancel")
	}
}
func TestNestedListWideMetadataAndInvalidSelections(t *testing.T) {
	f, v := nestedListTestFixture(TypeString)
	f.Name = strings.Repeat("wide", 16384)
	m := NewBudgetManager(0)
	o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, nil)
	if e != nil {
		t.Fatal(e)
	}
	charge := o.Bytes
	o.Close()
	if charge < int64(len(f.Name)) {
		t.Fatal("wide metadata uncharged")
	}
	short := NewBudgetManager(charge - 1)
	o, e = TakeNestedListRowsOwned(context.Background(), short, v, f, NestedListParentDomain{}, nil)
	if e == nil || o != nil || short.Used() != 0 {
		t.Fatal("wide exact-1")
	}
	exact := NewBudgetManager(charge)
	o, e = TakeNestedListRowsOwned(context.Background(), exact, v, f, NestedListParentDomain{}, nil)
	if e != nil {
		t.Fatal(e)
	}
	o.Close()
	for _, rows := range [][]int{{-1}, {7}, {0, 999}} {
		fm := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
		o, e = TakeNestedListRowsOwned(context.Background(), fm, v, f, NestedListParentDomain{}, rows)
		if e == nil || o != nil || fm.calls != 0 {
			t.Fatal("invalid selected", rows, e)
		}
	}
	for _, r := range [][2]int{{-1, 0}, {0, -1}, {7, 1}, {8, 0}, {int(^uint(0) >> 1), 1}} {
		o, e = CloneNestedListRangeOwned(context.Background(), m, v, f, NestedListParentDomain{}, r[0], r[1])
		if e == nil || o != nil || m.Used() != 0 {
			t.Fatal("invalid range", r, e)
		}
	}
	// Pure checked arithmetic: no large allocation or input buffer.
	if _, e := nestedListAddCount(int(^uint(0)>>1), 1); e == nil {
		t.Fatal("host count overflow accepted")
	}
	if _, e := nestedListAddCount(int(uint64(^uint32(0))), 0); e == nil {
		t.Fatal("offset sentinel overflow accepted")
	}
}
func TestNestedListShortValidityAndAncestry(t *testing.T) {
	for _, axis := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(axis), func(t *testing.T) {
			f, _ := nestedListTestFixture(TypeInt64)
			off := make([]uint32, 66)
			for i := range off {
				off[i] = uint32(i)
			}
			v := &ArrayNode{Kind: TypeList, Offsets: append([]uint32(nil), off...), Child: &ArrayNode{Kind: TypeList, Offsets: append([]uint32(nil), off...), Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{Values: make([]int64, 65)}}}}
			switch axis {
			case 0:
				v.Validity = Bitmap{^uint64(0)}
			case 1:
				v.Child.Validity = Bitmap{^uint64(0)}
			case 2:
				v.Child.Child.Int64.Validity = Bitmap{^uint64(0)}
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, nil)
			if e == nil || o != nil || m.calls != 0 {
				t.Fatal("short bitmap accepted", axis)
			}
		})
	}
	f, v := nestedListTestFixture(TypeString)
	anc := make([]Bitmap, 8)
	anc[7] = Bitmap{95}
	d, e := NewNestedListParentDomain(7, anc...)
	if e != nil {
		t.Fatal(e)
	}
	m := NewBudgetManager(0)
	o, e := TakeNestedListRowsOwned(context.Background(), m, v, f, d, []int{5, 4})
	if e != nil {
		t.Fatal(e)
	}
	nestedListTestCheck(t, o, TypeString, []int{5, 4}, true)
	o.Close()
	if m.Used() != 0 {
		t.Fatal("deep domain leak")
	}
}

func TestNestedListMultiwordAxes(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			const n = 137
			f, _ := nestedListTestFixture(kind)
			root := &ArrayNode{Kind: TypeList, Offsets: []uint32{0}, Validity: make(Bitmap, WordsFor(n))}
			inner := &ArrayNode{Kind: TypeList, Offsets: []uint32{0}}
			leaf := &ArrayNode{Kind: kind}
			root.Child = inner
			inner.Child = leaf
			var expected []any
			var leafValues []int64
			var texts []string
			var innerValid, leafValid []bool
			for r := 0; r < n; r++ {
				var outer = []any{}
				for j := 0; j < r%4+1; j++ {
					in := []any{}
					iv := (r+j)%7 != 0
					innerValid = append(innerValid, iv)
					for k := 0; k < (r+j)%5+1; k++ {
						lv := (r+j+k)%11 != 0
						num := int64(r*10000 + j*100 + k)
						text := fmt.Sprintf("界:%d", num)
						leafValues = append(leafValues, num)
						texts = append(texts, text)
						leafValid = append(leafValid, lv)
						var val any = num
						if kind == TypeString {
							val = text
						}
						if !lv {
							val = nil
						}
						in = append(in, val)
					}
					inner.Offsets = append(inner.Offsets, uint32(len(leafValues)))
					if iv {
						outer = append(outer, in)
					} else {
						outer = append(outer, nil)
					}
				}
				root.Offsets = append(root.Offsets, uint32(len(innerValid)))
				if r%9 != 0 {
					Set(root.Validity, r)
					expected = append(expected, outer)
				} else {
					expected = append(expected, nil)
				}
			}
			inner.Validity = make(Bitmap, WordsFor(len(innerValid)))
			for i, v := range innerValid {
				if v {
					Set(inner.Validity, i)
				}
			}
			valid := make(Bitmap, WordsFor(len(leafValues)))
			for i, v := range leafValid {
				if v {
					Set(valid, i)
				}
			}
			if kind == TypeInt64 {
				leaf.Int64 = &Int64Vector{Values: leafValues, Validity: valid}
			} else {
				sv := &StringViewVector{Offsets: []uint32{0}, Validity: valid, Encoding: EncodingView}
				for _, s := range texts {
					sv.Data = append(sv.Data, s...)
					sv.Offsets = append(sv.Offsets, uint32(len(sv.Data)))
				}
				leaf.Strings = sv
			}
			rows := make([]int, 4097)
			for i := range rows {
				rows[i] = (i*37 + 13) % n
			}
			m := NewBudgetManager(0)
			out, e := TakeNestedListRowsOwned(context.Background(), m, root, f, NestedListParentDomain{}, rows)
			if e != nil {
				t.Fatal(e)
			}
			defer out.Close()
			for i, r := range rows {
				if got := nestedListTestRead(out.Vector, i); !reflect.DeepEqual(got, expected[r]) {
					t.Fatalf("multiword row%d source%d mismatch", i, r)
				}
			}
			if out.Vector.Len() != 4097 || out.Vector.Child.Len() <= 4097 || out.Vector.Child.Child.Len() <= out.Vector.Child.Len() {
				t.Fatal("axes not independently crossed")
			}
			if e := out.Vector.ValidateWithContext(out.Field, 4097, NestedListParentDomain{}, nil); e != nil {
				t.Fatal(e)
			}
			out.Close()
			if m.Used() != 0 {
				t.Fatal("multiword leak")
			}
		})
	}
}
func TestNestedListMetadataAndPublicationCancel(t *testing.T) {
	f, v := nestedListTestFixture(TypeString)
	for _, target := range []string{".cloneRecursiveFieldContext", ".copyNestedListOwned"} {
		t.Run(target, func(t *testing.T) {
			m := NewBudgetManager(0)
			stop := 2
			if target == ".copyNestedListOwned" {
				cal := &listStructTestCancelContext{Context: context.Background(), manager: m, target: target, stop: int(^uint(0) >> 1)}
				o, e := TakeNestedListRowsOwned(cal, m, v, f, NestedListParentDomain{}, []int{4, 6})
				if e != nil {
					t.Fatal(e)
				}
				o.Close()
				stop = cal.checks
				if stop < 10 {
					t.Fatal("publication calibration too short")
				}
				t.Logf("final publication checkpoint calibrated at %d actual checks", stop)
			}
			ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: target, stop: stop}
			o, e := TakeNestedListRowsOwned(ctx, m, v, f, NestedListParentDomain{}, []int{4, 6})
			if !errors.Is(e, context.Canceled) || o != nil || ctx.chargedAtCancel <= 0 || m.Used() != 0 || ctx.checks != stop {
				t.Fatalf("post-reserve cancel %v checks%d/%d charge%d live%d", e, ctx.checks, stop, ctx.chargedAtCancel, m.Used())
			}
			retry, e := TakeNestedListRowsOwned(context.Background(), m, v, f, NestedListParentDomain{}, []int{4, 6})
			if e != nil {
				t.Fatal(e)
			}
			nestedListTestCheck(t, retry, TypeString, []int{4, 6}, false)
			retry.Close()
		})
	}
}

func TestNestedListEmptyInnerAxisCharge(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			var previous int64
			for _, n := range []int{63, 64, 65, 4097} {
				f, _ := nestedListTestFixture(kind)
				leaf := &ArrayNode{Kind: kind}
				if kind == TypeInt64 {
					leaf.Int64 = &Int64Vector{}
				} else {
					leaf.Strings = &StringViewVector{Offsets: []uint32{0}, Encoding: EncodingView}
				}
				src := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, uint32(n)}, Child: &ArrayNode{Kind: TypeList, Offsets: make([]uint32, n+1), Child: leaf}}
				m := NewBudgetManager(0)
				o, e := TakeNestedListRowsOwned(context.Background(), m, src, f, NestedListParentDomain{}, []int{0, 0})
				if e != nil {
					t.Fatal(e)
				}
				charge := o.Bytes
				if charge <= previous || charge < int64((2*n+1)*4+WordsFor(2*n)*8) {
					t.Fatal("empty inner axis fee", n, charge, previous)
				}
				previous = charge
				if o.Vector.Child.Len() != 2*n || o.Vector.Child.Child.Len() != 0 {
					t.Fatal("empty repeated axis counts")
				}
				for i := 0; i < 2*n; i++ {
					if !o.Vector.Child.IsValid(i) || o.Vector.Child.Offsets[i] != 0 {
						t.Fatal("empty inner confused with NULL")
					}
				}
				o.Close()
				if m.Used() != 0 {
					t.Fatal("empty inner leak")
				}
				exact := NewBudgetManager(charge)
				o, e = TakeNestedListRowsOwned(context.Background(), exact, src, f, NestedListParentDomain{}, []int{0, 0})
				if e != nil {
					t.Fatal(e)
				}
				o.Close()
				short := NewBudgetManager(charge - 1)
				o, e = TakeNestedListRowsOwned(context.Background(), short, src, f, NestedListParentDomain{}, []int{0, 0})
				if e == nil || o != nil || short.Used() != 0 {
					t.Fatal("empty axis exact-1")
				}
			}
		})
	}
}
