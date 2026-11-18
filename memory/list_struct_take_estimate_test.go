package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func structListTakeTree(t *testing.T, depth int) (Field, *StructVector) {
	t.Helper()
	f, a, _ := listStructTestFixture(t)
	g, b, _ := listStructTestFixture(t)
	g.Name = "other"
	data := NewBatch()
	data.Rows = 7
	data.StructLists[f.Name] = a
	data.StructLists[g.Name] = b
	field := StructField("root", true, f, g)
	v := &StructVector{Rows: 7, Fields: field.Children, Values: data, Encoding: EncodingView}
	for i := 1; i < depth; i++ {
		field = StructField("root", true, field)
		data = NewBatch()
		data.Rows = 7
		data.Structs["root"] = v
		v = &StructVector{Rows: 7, Fields: field.Children, Values: data, Encoding: EncodingView}
	}
	v.Validity = Bitmap{95}
	return field, v
}
func structListTakeTreeCheck(t *testing.T, v *StructVector, f Field, rows []int) {
	t.Helper()
	if v.Rows != len(rows) || v.Values.Rows != len(rows) {
		t.Fatal("tree rows")
	}
	for i, r := range rows {
		if v.IsValid(i) != (r != 5) {
			t.Fatal("outer mask", i, r)
		}
	}
	for len(f.Children) == 1 && f.Children[0].Type == TypeStruct {
		f = f.Children[0]
		v = v.Values.Structs[f.Name]
	}
	for _, c := range f.Children {
		listStructTestCheck(t, &OwnedStructList{Vector: v.Values.StructLists[c.Name], Field: c}, rows)
	}
}
func TestStructListTakeRangeEstimates(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	for _, r := range [][2]int{{0, 0}, {0, 7}, {1, 4}, {2, 3}, {5, 1}, {7, 0}} {
		t.Run(fmt.Sprint(r), func(t *testing.T) {
			n, e := EstimateStructListTakeRangeBytes(context.Background(), v, f, d, r[0], r[1])
			if e != nil {
				t.Fatal(e)
			}
			rows := make([]int, r[1])
			for i := range rows {
				rows[i] = r[0] + r[1] - 1 - i
			}
			m := NewBudgetManager(n)
			o, e := TakeStructListRowsOwned(context.Background(), m, v, f, d, rows)
			if e != nil {
				t.Fatal(e)
			}
			listStructTestCheck(t, o, rows)
			if o.Bytes > n || m.Used() != o.Bytes {
				t.Fatal("permutation estimate", n, o.Bytes, m.Used())
			}
			charge := o.Bytes
			o.Close()
			short := NewBudgetManager(charge - 1)
			bad, e := TakeStructListRowsOwned(context.Background(), short, v, f, d, rows)
			if bad != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
				t.Fatal("short", bad, e)
			}
		})
	}
}
func TestStructListsTreeTakeRangeEstimates(t *testing.T) {
	for _, depth := range []int{1, 7} {
		for _, r := range [][2]int{{0, 0}, {0, 7}, {1, 4}, {5, 1}, {7, 0}} {
			t.Run(fmt.Sprint(depth, r), func(t *testing.T) {
				f, v := structListTakeTree(t, depth)
				n, e := EstimateStructListsStructTakeRangeBytes(context.Background(), v, f, r[0], r[1])
				if e != nil {
					t.Fatal(e)
				}
				rows := make([]int, r[1])
				for i := range rows {
					rows[i] = r[0] + r[1] - 1 - i
				}
				m := NewBudgetManager(n)
				o, e := TakeStructListsStructRowsOwned(context.Background(), m, v, f, rows)
				if e != nil {
					t.Fatal("admitted range permutation", n, e)
				}
				structListTakeTreeCheck(t, o.Vector, f, rows)
				if o.Bytes > n || m.Used() != o.Bytes {
					t.Fatal("tree underestimate", n, o.Bytes, m.Used())
				}
				charge := o.Bytes
				o.Close()
				if m.Used() != 0 {
					t.Fatal("tree leak")
				}
				exact := NewBudgetManager(charge)
				o, e = TakeStructListsStructRowsOwned(context.Background(), exact, v, f, rows)
				if e != nil {
					t.Fatal("exact", e)
				}
				o.Close()
				short := NewBudgetManager(charge - 1)
				bad, e := TakeStructListsStructRowsOwned(context.Background(), short, v, f, rows)
				if bad != nil || !errors.Is(e, ErrBudgetExceeded) || short.Used() != 0 {
					t.Fatal("tree short", bad, e)
				}
			})
		}
	}
}
func TestStructListsTreeTakeRepeatedAndRollback(t *testing.T) {
	f, v := structListTakeTree(t, 7)
	rows := []int{6, 4, 6, 5, 0, 2, 3, 1}
	probe := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	o, e := TakeStructListsStructRowsOwned(context.Background(), probe, v, f, rows)
	if e != nil {
		t.Fatal(e)
	}
	structListTakeTreeCheck(t, o.Vector, f, rows)
	calls := probe.calls
	o.Close()
	if calls < 3 {
		t.Fatal("expected multi-owner admissions", calls)
	}
	for at := 1; at <= calls; at++ {
		t.Run(fmt.Sprint(at), func(t *testing.T) {
			m := &recursiveCollectTestManager{Manager: NewBudgetManager(0), failAt: at}
			out, e := TakeStructListsStructRowsOwned(context.Background(), m, v, f, rows)
			if out != nil || !errors.Is(e, ErrBudgetExceeded) || m.Used() != 0 {
				t.Fatal("rollback", out, e, m.Used())
			}
			m.failAt = 0
			out, e = TakeStructListsStructRowsOwned(context.Background(), m, v, f, rows)
			if e != nil {
				t.Fatal(e)
			}
			structListTakeTreeCheck(t, out.Vector, f, rows)
			out.Close()
			if m.Used() != 0 {
				t.Fatal("retry leak")
			}
		})
	}
}

func TestStructListsTreeTakeDemandedDomain(t *testing.T) {
	f, v := structListTakeTree(t, 7)
	field := &f
	leaf := v
	for len(field.Children) == 1 {
		field = &field.Children[0]
		leaf = leaf.Values.Structs["root"]
	}
	// Required first child's NULL at item2 belongs to parent3. The other list remains nullable.
	field.Children[0].Children[0].Children[0].Nullable = false
	leaf.Fields = field.Children
	leaf.Values.StructLists["items"].Values.Fields = field.Children[0].Children[0].Children
	for _, rows := range [][]int{nil, {6, 0, 1}, {5}, {2}} {
		m := NewBudgetManager(0)
		o, e := TakeStructListsStructRowsOwned(context.Background(), m, v, f, rows)
		if e != nil {
			t.Fatal("inactive required NULL", rows, e)
		}
		structListTakeTreeCheck(t, o.Vector, f, rows)
		o.Close()
		if m.Used() != 0 {
			t.Fatal("domain leak")
		}
	}
	m := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
	o, e := TakeStructListsStructRowsOwned(context.Background(), m, v, f, []int{3})
	if o != nil || e == nil || m.calls != 0 || m.Used() != 0 {
		t.Fatal("active required NULL", o, e, m.calls, m.Used())
	}
	// Range estimates validate only the requested value domain, while checking full storage.
	if _, e := EstimateStructListsStructTakeRangeBytes(context.Background(), v, f, 6, 1); e != nil {
		t.Fatal("tree range excludes required NULL", e)
	}
	if _, e := EstimateStructListsStructTakeRangeBytes(context.Background(), v, f, 7, 0); e != nil {
		t.Fatal("empty tree range", e)
	}
	if _, e := EstimateStructListsStructTakeRangeBytes(context.Background(), v, f, 0, 7); e == nil {
		t.Fatal("full requested range required NULL")
	}
	leaf.Values.StructLists["items"].Offsets[1] = 2
	o, e = TakeStructListsStructRowsOwned(context.Background(), m, v, f, nil)
	if o != nil || e == nil || m.calls != 0 {
		t.Fatal("empty selection concealed malformed offsets", o, e, m.calls)
	}
}
func TestStructListTakeEstimateDomainAndInvalidRanges(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	f.Children[0].Children[0].Nullable = false
	v.Values.Fields = f.Children[0].Children
	if _, e := EstimateStructListTakeRangeBytes(context.Background(), v, f, d, 6, 1); e != nil {
		t.Fatal("range excludes required NULL", e)
	}
	if _, e := EstimateStructListTakeRangeBytes(context.Background(), v, f, d, 3, 1); e == nil {
		t.Fatal("range includes required NULL")
	}
	for _, r := range [][2]int{{-1, 1}, {0, -1}, {8, 0}, {7, 1}, {1, int(^uint(0) >> 1)}} {
		if _, e := EstimateStructListTakeRangeBytes(context.Background(), v, f, d, r[0], r[1]); e == nil {
			t.Fatal("invalid range", r)
		}
	}
	v.Offsets[1] = 2
	if _, e := EstimateStructListTakeRangeBytes(context.Background(), v, f, d, 7, 0); e == nil {
		t.Fatal("zero range hidden malformed storage")
	}
}
func TestStructListsTreeTakeCancelAndOldOwner(t *testing.T) {
	// Real long copy inside a List item, nested under a Struct ancestor.
	item, list, _, text := listStructTestLong(t)
	b := NewBatch()
	b.Rows = 1
	b.StructLists[item.Name] = list
	f := StructField("root", true, item)
	v := &StructVector{Rows: 1, Fields: f.Children, Values: b}
	m := NewBudgetManager(0)
	old, e := TakeStructListsStructRowsOwned(context.Background(), m, v, f, []int{0})
	if e != nil {
		t.Fatal(e)
	}
	oldCharge := m.Used()
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".appendRecursiveString", stop: 2}
	out, e := TakeStructListsStructRowsOwned(ctx, m, v, f, []int{0, 0})
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != oldCharge {
		t.Fatal("actual copy cancellation", out, e, ctx.checks, m.Used(), oldCharge)
	}
	if string(old.Vector.Values.StructLists[item.Name].Values.Values.Strings["s"].Data) != text {
		t.Fatal("old output corrupted")
	}
	out, e = TakeStructListsStructRowsOwned(context.Background(), m, v, f, []int{0, 0})
	if e != nil {
		t.Fatal(e)
	}
	if string(out.Vector.Values.StructLists[item.Name].Values.Values.Strings["s"].Data) != text+text {
		t.Fatal("copy retry")
	}
	out.Close()
	old.Close()
	if m.Used() != 0 {
		t.Fatal("copy final cleanup")
	}
}

func TestStructListTakeEstimateCancellationAndMalformedGraph(t *testing.T) {
	f, v, d := listStructTestFixture(t)
	m := NewBudgetManager(0)
	ctx := &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".validateParent", stop: 2}
	_, e := EstimateStructListTakeRangeBytes(ctx, v, f, d, 0, 7)
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != 0 {
		t.Fatal("list estimate scan cancel", e, ctx.checks)
	}
	tree, src := structListTakeTree(t, 7)
	ctx = &listStructTestCancelContext{Context: context.Background(), manager: m, target: ".selectionRunFieldSize", stop: 10}
	_, e = EstimateStructListsStructTakeRangeBytes(ctx, src, tree, 0, 7)
	if !errors.Is(e, context.Canceled) || ctx.checks != 10 {
		t.Fatal("tree estimate scan cancel", e, ctx.checks)
	}
	for _, kind := range []string{"extra", "cycle", "depth9"} {
		t.Run(kind, func(t *testing.T) {
			f, v := structListTakeTree(t, 7)
			switch kind {
			case "extra":
				_, x, _ := listStructTestFixture(t)
				v.Values.StructLists["extra"] = x
			case "cycle":
				v.Values.Structs["cycle"] = v
			case "depth9":
				f = StructField("outer", true, f)
				b := NewBatch()
				b.Rows = 7
				b.Structs["root"] = v
				v = &StructVector{Rows: 7, Fields: f.Children, Values: b}
			}
			if _, e := EstimateStructListsStructTakeRangeBytes(context.Background(), v, f, 7, 0); e == nil {
				t.Fatal("invalid graph estimate", kind)
			}
			mm := &recursiveCollectTestManager{Manager: NewBudgetManager(0)}
			o, e := TakeStructListsStructRowsOwned(context.Background(), mm, v, f, nil)
			if o != nil || e == nil || mm.calls != 0 || mm.Used() != 0 {
				t.Fatal("invalid graph take", kind, o, e, mm.calls)
			}
		})
	}
}
