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

func nestedBatchFixture(t *testing.T, depth int) (Schema, *Batch) {
	t.Helper()
	f, v := nestedListTestFixture(TypeInt64)
	child := NewBatch()
	child.Rows = 7
	child.Arrays[f.Name] = v
	for i := 0; i < depth; i++ {
		f = StructField("inner", true, f)
		node := &StructVector{Rows: 7, Fields: f.Children, Values: child, Encoding: EncodingView}
		if i == depth-1 {
			node.Validity = Bitmap{95}
		}
		child = NewBatch()
		child.Rows = 7
		child.Structs["inner"] = node
	}
	b := NewBatch()
	b.Rows = 7
	if depth > 0 {
		f.Name = "root"
		b.Structs[f.Name] = child.Structs["inner"]
	} else {
		b.Arrays[f.Name] = v
	}
	other, ov := nestedListTestFixture(TypeString)
	other.Name = "other"
	b.Arrays[other.Name] = ov
	b.Int64s["id"] = &Int64Vector{Values: []int64{10, 11, 12, 13, 14, 15, 16}}
	s, e := NewSchemaWithNestedListsChecked(f, other, Field{Name: "id", Type: TypeInt64})
	if e != nil {
		t.Fatal(e)
	}
	return s, b
}
func nestedBatchCheck(t *testing.T, b *Batch, s Schema, depth int) {
	t.Helper()
	if b.Rows != 7 || !reflect.DeepEqual(b.Int64s["id"].Values, []int64{10, 11, 12, 13, 14, 15, 16}) {
		t.Fatal("flat root")
	}
	cur := b
	field := s.Fields[0]
	if depth > 0 {
		node := b.Structs["root"]
		for d := 0; d < depth; d++ {
			if node == nil || node.Rows != 7 || node.Values.Rows != 7 {
				t.Fatal("ancestor shape")
			}
			if d == 0 && node.IsValid(5) {
				t.Fatal("ancestor NULL")
			}
			cur = node.Values
			field = field.Children[0]
			if d+1 < depth {
				node = cur.Structs["inner"]
			}
		}
	}
	rows := []int{0, 1, 2, 3, 4, 5, 6}
	nestedListTestCheck(t, &OwnedNestedList{Vector: cur.Arrays["nested"], Field: field}, TypeInt64, rows, depth > 0)
	nestedListTestCheck(t, &OwnedNestedList{Vector: b.Arrays["other"], Field: s.Fields[1]}, TypeString, rows, false)
}
func TestNestedListBatchOwnedRoots(t *testing.T) {
	for _, depth := range []int{0, 1, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := nestedBatchFixture(t, depth)
			if e := ValidateNestedListsBatch(b, s, nil); e != nil {
				t.Fatal(e)
			}
			m := NewBudgetManager(0)
			out, lease, e := CloneCollectBatchNestedLists(context.Background(), m, b, s)
			if e != nil {
				t.Fatal(e)
			}
			charge := m.Used()
			if charge <= 0 {
				t.Fatal("uncharged")
			}
			nestedBatchCheck(t, out, s, depth)
			out.ReleaseOwned()
			lease.Release()
			if m.Used() != 0 {
				t.Fatal("release")
			}
			for _, limit := range []int64{charge, charge - 1} {
				manager := NewBudgetManager(limit)
				out, lease, e = CloneCollectBatchNestedLists(context.Background(), manager, b, s)
				if limit < charge {
					if e == nil || out != nil || manager.Used() != 0 {
						t.Fatal("exact-1", e)
					}
				} else {
					if e != nil {
						t.Fatal(e)
					}
					out.ReleaseOwned()
					lease.Release()
					if manager.Used() != 0 {
						t.Fatal("exact release")
					}
				}
			}
			// Trace the complete successful call sequence, then fail each real reserve.
			trace := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, lease, e = CloneCollectBatchNestedLists(context.Background(), trace, b, s)
			if e != nil {
				t.Fatal(e)
			}
			calls := trace.calls
			out.ReleaseOwned()
			lease.Release()
			t.Logf("depth%d successful Reserve count=%d", depth, calls)
			for fail := 1; fail <= calls; fail++ {
				fm := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: fail}
				o, l, e := CloneCollectBatchNestedLists(context.Background(), fm, b, s)
				if e == nil || o != nil || fm.Used() != 0 {
					t.Fatalf("failure %d partial/leak %v", fail, e)
				}
				if l != nil {
					l.Release()
				}
				fm.failAt = 0
				o, l, e = CloneCollectBatchNestedLists(context.Background(), fm, b, s)
				if e != nil {
					t.Fatal(e)
				}
				nestedBatchCheck(t, o, s, depth)
				o.ReleaseOwned()
				l.Release()
			}
			out, lease, e = CloneCollectBatchNestedLists(context.Background(), m, b, s)
			if e != nil {
				t.Fatal(e)
			}
			b.Arrays["other"].Child.Child.Strings.Data[0] = '!'
			b.Arrays["other"].Offsets[0] = 999
			b.Int64s["id"].Values[0] = -99
			nestedBatchCheck(t, out, s, depth)
			out.ReleaseOwned()
			lease.Release()
		})
	}
}
func TestNestedListBatchGraphBeforeReserve(t *testing.T) {
	for _, name := range []string{"missing", "typednil", "extra", "cycle", "hidden"} {
		t.Run(name, func(t *testing.T) {
			s, b := nestedBatchFixture(t, 8)
			switch name {
			case "missing":
				delete(b.Arrays, "other")
			case "typednil":
				b.Arrays["other"] = nil
			case "extra":
				b.Arrays["extra"] = b.Arrays["other"]
			case "cycle":
				b.Arrays["other"].Child = b.Arrays["other"]
			case "hidden":
				b.Arrays["other"].Child.Offsets[1] = 99
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			o, l, e := CloneCollectBatchNestedLists(context.Background(), m, b, s)
			if e == nil || o != nil || m.calls != 0 || m.Used() != 0 {
				t.Fatal("graph preReserve", e, m.calls)
			}
			if l != nil {
				l.Release()
			}
		})
	}
	// A flat Struct-list item must reject the newly registered Arrays map,
	// even when a selection is empty and the extra array forms a cycle.
	f, sv, off, valid := listStructTestRaw(t)
	arr := &ArrayNode{Kind: TypeList}
	arr.Child = arr
	sv.Values.Arrays["extra"] = arr
	src := &StructListVector{Offsets: off, Validity: valid, Values: sv}
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	o, e := TakeStructListRowsOwned(context.Background(), m, src, f, StructListParentDomain{}, nil)
	if e == nil || o != nil || m.calls != 0 {
		t.Fatal("flat item extra Arrays admitted", e)
	}
}
func TestNestedListBatchCancelAndDirectStruct(t *testing.T) {
	s, b := nestedBatchFixture(t, 8)
	m := NewBudgetManager(0)
	o, e := CloneNestedListsStructRangeOwned(context.Background(), m, b.Structs["root"], s.Fields[0], 0, 7)
	if e != nil {
		t.Fatal(e)
	}
	if o.Vector.Rows != 7 {
		t.Fatal("direct range")
	}
	o.Close()
	if m.Used() != 0 {
		t.Fatal("direct lease")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, l, e := CloneCollectBatchNestedLists(ctx, m, b, s)
	if !errors.Is(e, context.Canceled) || out != nil || m.Used() != 0 {
		t.Fatal("pre-cancel", e)
	}
	if l != nil {
		l.Release()
	}
}

func TestNestedListNullableInnerOwned(t *testing.T) {
	for _, kind := range []DataType{TypeInt64, TypeString} {
		t.Run(kind.String(), func(t *testing.T) {
			f, root := nestedListTestFixture(kind)
			inner := root.Child
			field := f.Children[0]
			m := NewBudgetManager(0)
			rows := []int{-1, 1, 2, 3, 3, 6}
			o, e := TakeNullableArrayRowsOwned(context.Background(), m, inner, field, rows)
			if e != nil {
				t.Fatal(e)
			}
			var a, b, c any = int64(-9223372036854775808), int64(9223372036854775807), int64(-7)
			if kind == TypeString {
				a, b, c = "界", "", "尾🙂"
			}
			want := []any{nil, nil, []any{}, []any{a, nil, b}, []any{a, nil, b}, []any{c}}
			for i, w := range want {
				if got := nestedListTestRead(o.Vector, i); !reflect.DeepEqual(got, w) {
					t.Fatalf("inner row%d got%#v want%#v", i, got, w)
				}
			}
			if !o.Field.Nullable {
				t.Fatal("nullable result")
			}
			charge := o.Bytes
			o.Close()
			if m.Used() != 0 {
				t.Fatal("inner leak")
			}
			short := NewBudgetManager(charge - 1)
			o, e = TakeNullableArrayRowsOwned(context.Background(), short, inner, field, rows)
			if e == nil || o != nil || short.Used() != 0 {
				t.Fatal("inner exact-1")
			}
			for _, bad := range []int{-2, 7} {
				o, e = TakeNullableArrayRowsOwned(context.Background(), m, inner, field, []int{bad})
				if e == nil || o != nil || m.Used() != 0 {
					t.Fatal("invalid nullable index", bad)
				}
			}
			field.Children[0].Nullable = false
			o, e = TakeNullableArrayRowsOwned(context.Background(), m, inner, field, []int{-1, 2, 6})
			if e != nil {
				t.Fatal(e)
			}
			o.Close()
			o, e = TakeNullableArrayRowsOwned(context.Background(), m, inner, field, []int{3})
			if e == nil || o != nil || m.Used() != 0 {
				t.Fatal("active required leaf")
			}
		})
	}
}

type nestedBatchSecondRootManager struct {
	Manager
	roots        int
	beforeSecond int64
	failed       bool
}

func (m *nestedBatchSecondRootManager) Reserve(ctx context.Context, n int64) (Reservation, error) {
	var pcs [32]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	owned := false
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".copyNestedListOwned") {
			owned = true
			break
		}
		if !more {
			break
		}
	}
	if owned {
		m.roots++
		if m.roots == 2 {
			m.beforeSecond = m.Used()
			m.failed = true
			return nil, fmt.Errorf("second complete array root admission fault")
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNestedListBatchSecondCompleteRootRollback(t *testing.T) {
	s, b := nestedBatchFixture(t, 0)
	probe := NewBudgetManager(0)
	one, e := CloneNestedListRangeOwned(context.Background(), probe, b.Arrays["nested"], s.Fields[0], NestedListParentDomain{}, 0, 7)
	if e != nil {
		t.Fatal(e)
	}
	firstFee := one.Bytes
	one.Close()
	m := &nestedBatchSecondRootManager{Manager: NewBudgetManager(0)}
	o, l, e := CloneCollectBatchNestedLists(context.Background(), m, b, s)
	if e == nil || o != nil || !m.failed || m.roots != 2 || m.beforeSecond <= firstFee || m.Used() != 0 {
		t.Fatalf("second-root rollback err%v roots%d charge%d first%d live%d", e, m.roots, m.beforeSecond, firstFee, m.Used())
	}
	if l != nil {
		l.Release()
	}
	t.Logf("second array-root Reserve reached with first owner plus flat/setup live: %d > %d", m.beforeSecond, firstFee)
	retry := NewBudgetManager(0)
	o, l, e = CloneCollectBatchNestedLists(context.Background(), retry, b, s)
	if e != nil {
		t.Fatal(e)
	}
	nestedBatchCheck(t, o, s, 0)
	o.ReleaseOwned()
	l.Release()
	if retry.Used() != 0 {
		t.Fatal("retry leak")
	}
}

func TestNestedListRangeEstimatorMissingSentinel(t *testing.T) {
	cases := map[string]*ArrayNode{"list_nil": {Kind: TypeList, Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{}}}, "list_empty": {Kind: TypeList, Offsets: []uint32{}, Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{}}}, "string_nil": {Kind: TypeString, Strings: &StringViewVector{}}, "string_empty": {Kind: TypeString, Strings: &StringViewVector{Offsets: []uint32{}}}}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("malformed zero-row storage panicked instead of error: %v", p)
				}
			}()
			if _, e := EstimateArrayRangeBytes(v, 0, 0); e == nil {
				t.Fatal("missing sentinel accepted")
			}
		})
	}
}

func TestNestedListTreeTakeSelectedDomain(t *testing.T) {
	schema, b := nestedBatchFixture(t, 8)
	f := schema.Fields[0]
	fv := &f
	node := b.Structs["root"]
	for d := 0; d < 8; d++ {
		fv = &fv.Children[0]
		if d < 7 {
			node = node.Values.Structs["inner"]
		}
	}
	fv.Children[0].Children[0].Nullable = false
	syncField := f
	syncNode := b.Structs["root"]
	for d := 0; d < 8; d++ {
		syncNode.Fields = syncField.Children
		syncField = syncField.Children[0]
		if d < 7 {
			syncNode = syncNode.Values.Structs["inner"]
		}
	}
	// row4 contains visible NULL terminal; row5 is hidden by the outer ancestor.
	m := NewBudgetManager(0)
	rows := []int{6, 5, 6, 3}
	o, e := TakeNestedListsStructRowsOwned(context.Background(), m, b.Structs["root"], f, rows)
	if e != nil {
		t.Fatal(e)
	}
	v := o.Vector
	for d := 1; d < 8; d++ {
		v = v.Values.Structs["inner"]
	}
	arr := v.Values.Arrays["nested"]
	want := []any{[]any{[]any{int64(-7)}}, nil, []any{[]any{int64(-7)}}, []any{[]any{}}}
	for i, w := range want {
		if got := nestedListTestRead(arr, i); !reflect.DeepEqual(got, w) {
			t.Fatalf("tree row%d %#v want%#v", i, got, w)
		}
	}
	o.Close()
	if m.Used() != 0 {
		t.Fatal("tree take leak")
	}
	o, e = TakeNestedListsStructRowsOwned(context.Background(), m, b.Structs["root"], f, []int{4})
	if e == nil || o != nil || m.Used() != 0 {
		t.Fatal("selected required leaf accepted")
	}
	node.Values.Arrays["nested"].Child.Offsets[1] = 99
	o, e = TakeNestedListsStructRowsOwned(context.Background(), m, b.Structs["root"], f, nil)
	if e == nil || o != nil || m.Used() != 0 {
		t.Fatal("empty selected hidden storage")
	}
}

func TestNestedListBorrowMetadataRunningCancel(t *testing.T) {
	f, _ := nestedListTestFixture(TypeInt64)
	v := &ArrayNode{Kind: TypeList, Offsets: []uint32{0, 4097}, Child: &ArrayNode{Kind: TypeList, Offsets: make([]uint32, 4098), Child: &ArrayNode{Kind: TypeInt64, Int64: &Int64Vector{}}}}
	b := NewBatch()
	b.Rows = 1
	b.Arrays[f.Name] = v
	ctx := &listStructTestCancelContext{Context: context.Background(), target: ".arrayBorrowSize", stop: 100}
	out, e := BorrowBatchRangeWithCheck(b, 0, 1, ctx.Err)
	if !errors.Is(e, context.Canceled) || out != nil || ctx.checks != 100 {
		t.Fatalf("borrow metadata cancellation err%v checks%d", e, ctx.checks)
	}
	retry, e := BorrowBatchRangeWithCheck(b, 0, 1, nil)
	if e != nil {
		t.Fatal(e)
	}
	if retry.Arrays[f.Name].Child.Len() != 4097 {
		t.Fatal("borrow retry")
	}
	retry.ReleaseOwned()
}

func TestNestedListArrowInt32CountBoundary(t *testing.T) {
	const limit = uint64(1<<31 - 1)
	for _, tc := range []struct {
		a, b, want uint64
		bad        bool
	}{{0, limit, limit, false}, {limit, 0, limit, false}, {limit - 1, 1, limit, false}, {limit, 1, 0, true}, {limit + 1, 0, 0, true}, {0, ^uint64(0), 0, true}, {^uint64(0), 1, 0, true}} {
		g, e := addArrayArrowCount(tc.a, tc.b)
		if (e != nil) != tc.bad || (!tc.bad && g != tc.want) {
			t.Fatalf("count %d+%d got%d err%v", tc.a, tc.b, g, e)
		}
	}
}

func TestNestedListCheckedSchemaNestedDecimalRejected(t *testing.T) {
	nested, _ := nestedListTestFixture(TypeInt64)
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			children := []Field{{Name: "decimal", Type: TypeDecimal, DecimalPrecision: 9, DecimalScale: 2}}
			if mixed {
				children = append(children, nested)
			}
			root := StructField("outer", true, children...)
			if _, e := NewSchemaWithNestedListsChecked(root); e == nil {
				t.Fatal("checked descriptor admitted unsupported nested Decimal")
			}
		})
	}
}

// Post-fix coverage for a statically identified unbounded pre-admission walk.
// Deliberately never run against the old detector: stack exhaustion is not a safe red probe.
func TestNestedListCodecUndeclaredRuntimeCycleBeforeReserve(t *testing.T) {
	for _, kind := range []string{"struct", "struct_list"} {
		t.Run(kind, func(t *testing.T) {
			b := NewBatch()
			b.Rows = 1
			b.Int64s["id"] = &Int64Vector{Values: []int64{7}}
			schema := NewSchema(Field{Name: "id", Type: TypeInt64})
			v := &StructVector{Rows: 1, Values: b}
			if kind == "struct" {
				b.Structs["undeclared"] = v
			} else {
				b.StructLists["undeclared"] = &StructListVector{Offsets: []uint32{0, 1}, Values: v}
			}
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			out, e := EncodeColumnarRun(context.Background(), schema, b, m)
			if e == nil || out != nil || m.calls != 0 || m.Used() != 0 {
				t.Fatalf("undeclared cycle codec admission err%v calls%d live%d", e, m.calls, m.Used())
			}
		})
	}
}
