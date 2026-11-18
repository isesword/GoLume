package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func listStructGatherList(t *testing.T, name string) (memory.Field, *memory.StructListVector) {
	f, e := memory.ListStructField(name, true, true, memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView}, memory.Field{Name: "b", Type: memory.TypeBool, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Rows = 5
	b.Int64s["n"] = &memory.Int64Vector{Values: []int64{999, 20, 30, 40, 50}, Validity: memory.Bitmap{30}}
	b.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{0, 1, 0, 1, 0}, Validity: memory.Bitmap{30}}
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 6, 7, 10, 10, 11}, Data: []byte("hiddena界z"), Validity: memory.Bitmap{30}, Encoding: memory.EncodingView}
	v := &memory.StructListVector{Offsets: []uint32{0, 1, 1, 3, 5}, Validity: memory.Bitmap{14}, Values: &memory.StructVector{Rows: 5, Fields: f.Children[0].Children, Values: b, Validity: memory.Bitmap{29}}, Encoding: memory.EncodingView}
	return f, v
}
func listStructGatherFixture(t *testing.T) (memory.Schema, *memory.Batch) {
	f, v := listStructGatherList(t, "items")
	b := memory.NewBatch()
	b.Rows = 4
	b.StructLists[f.Name] = v
	field := f
	for i := 0; i < 7; i++ {
		field = memory.StructField("root", true, field)
		node := &memory.StructVector{Rows: 4, Fields: field.Children, Values: b}
		if i == 6 {
			node.Validity = memory.Bitmap{7}
		}
		b = memory.NewBatch()
		b.Rows = 4
		b.Structs["root"] = node
	}
	f1, v1 := listStructGatherList(t, "first")
	f2, v2 := listStructGatherList(t, "second")
	b.StructLists[f1.Name] = v1
	b.StructLists[f2.Name] = v2
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{4, 1, 3, 2}}

	plainLeaf := memory.NewBatch()
	plainLeaf.Rows = 4
	plainLeaf.Int64s["n"] = &memory.Int64Vector{Values: []int64{70, 71, 72, 73}}
	plainChild := memory.StructField("inside", false, memory.Field{Name: "n", Type: memory.TypeInt64})
	plainBatch := memory.NewBatch()
	plainBatch.Rows = 4
	plainBatch.Structs["inside"] = &memory.StructVector{Rows: 4, Fields: plainChild.Children, Values: plainLeaf}
	plainField := memory.StructField("plain", true, plainChild)
	b.Structs["plain"] = &memory.StructVector{Rows: 4, Fields: plainField.Children, Values: plainBatch, Validity: memory.Bitmap{11}}
	b.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte("abcd")}
	b.Int64Lists["oldints"] = &memory.Int64ListVector{Offsets: []uint32{0, 1, 2, 3, 4}, Values: &memory.Int64Vector{Values: []int64{10, 11, 12, 13}}}
	b.StringLists["oldtexts"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2, 3, 4}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte("wxyz")}}
	s, e := memory.NewSchemaWithStructListsChecked(memory.Field{Name: "key", Type: memory.TypeInt64}, field, f1, f2, plainField, memory.Field{Name: "text", Type: memory.TypeString}, memory.ListInt64Field("oldints", false, false), memory.ListStringField("oldtexts", false, false))
	if e != nil {
		t.Fatal(e)
	}
	return s, b
}
func listStructGatherCheck(t *testing.T, b *memory.Batch, rows []int) {
	t.Helper()
	if b.Rows != len(rows) {
		t.Fatal("batch rows")
	}
	root := b.Structs["root"]
	leaf := root
	for i := 0; i < 6; i++ {
		leaf = leaf.Values.Structs["root"]
	}
	for col, v := range []*memory.StructListVector{leaf.Values.StructLists["items"], b.StructLists["first"], b.StructLists["second"]} {
		for i, r := range rows {
			visible := r != 0 && (col != 0 || r != 3)
			if v.IsValid(i) != visible {
				t.Fatal("parent domain", col, i, r)
			}
			lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
			want := 0
			if visible && r >= 2 {
				want = 2
			}
			if hi-lo != want {
				t.Fatal("span", col, r, lo, hi)
			}
			if !visible {
				continue
			}
			for k := 0; k < want; k++ {
				src := 1 + k
				if r == 3 {
					src = 3 + k
				}
				valid := src != 1
				if v.Values.IsValid(lo+k) != valid {
					t.Fatal("item NULL")
				}
				if !valid {
					continue
				}
				n := []int64{999, 20, 30, 40, 50}[src]
				s := []string{"hidden", "a", "界", "", "z"}[src]
				if v.Values.Values.Int64s["n"].Values[lo+k] != n || string(v.Values.Values.Strings["s"].ValueBytes(lo+k)) != s || v.Values.Values.Uint32s["b"].Values[lo+k] != uint32(src%2) {
					t.Fatal("literal item oracle", src)
				}
			}
		}
	}
	for i, r := range rows {
		plain := b.Structs["plain"]
		if plain == nil || plain.IsValid(i) != (r != 2) {
			t.Fatal("old recursive payload missing/mask")
		}
		if r != 2 && plain.Values.Structs["inside"].Values.Int64s["n"].Values[i] != 70+int64(r) {
			t.Fatal("old recursive value")
		}
		if string(b.Strings["text"].ValueBytes(i)) != string("abcd"[r]) {
			t.Fatal("flat string")
		}
		ints := b.Int64Lists["oldints"]
		lo, hi, _ := ints.ValueRange(i)
		if hi-lo != 1 || ints.Values.Values[lo] != 10+int64(r) {
			t.Fatal("old int list")
		}
		texts := b.StringLists["oldtexts"]
		lo, hi, _ = texts.ValueRange(i)
		if hi-lo != 1 || string(texts.Values.ValueBytes(lo)) != string("wxyz"[r]) {
			t.Fatal("old string list")
		}
		if b.Int64s["key"].Values[i] != []int64{4, 1, 3, 2}[r] || b.Uint64s["ordinal"].Values[i] != 100+uint64(r) || root.IsValid(i) != (r != 3) {
			t.Fatal("scalar/stability/root")
		}
	}
}

type listStructGatherManager struct {
	memory.Manager
	calls, failAt int
	peak          int64
	firstTop      int
	beforeTop     int64
}

func (m *listStructGatherManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	var pcs [40]uintptr
	k := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:k])
	top, tree := false, false
	for {
		f, more := frames.Next()
		top = top || strings.HasSuffix(f.Function, ".TakeStructListRowsOwned")
		tree = tree || strings.HasSuffix(f.Function, ".cloneStructListsTree")
		if !more {
			break
		}
	}
	if top && !tree && m.firstTop == 0 {
		m.firstTop = m.calls
		m.beforeTop = m.Used()
	}
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, e
}
func TestStructListInitialGatherPlanOwnership(t *testing.T) {
	for _, r := range [][2]int{{0, 4}, {1, 4}, {0, 0}, {4, 4}} {
		t.Run(fmt.Sprint(r), func(t *testing.T) {
			s, b := listStructGatherFixture(t)
			p, e := planExternalSortInitialGather(s, b, r[0], r[1], 1, 100)
			if e != nil {
				t.Fatal(e)
			}
			m := &listStructGatherManager{Manager: memory.NewBudgetManager(p.gatherBytes)}
			o, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
			if e != nil {
				t.Fatal(e)
			}
			rows := []int{}
			for _, i := range []int{1, 3, 2, 0} {
				if i >= r[0] && i < r[1] {
					rows = append(rows, i)
				}
			}
			listStructGatherCheck(t, o.batch, rows)
			if m.peak > p.gatherBytes || m.Used() > p.gatherBytes {
				t.Fatal("plan underbounds simultaneous owners", m.peak, p.gatherBytes)
			}
			if !reflect.DeepEqual(b.StructLists["first"].Offsets, []uint32{0, 1, 1, 3, 5}) || b.StructLists["first"].Values.Values.Int64s["n"].Values[2] != 30 {
				t.Fatal("source changed")
			}
			b.StructLists["first"].Values.Values.Int64s["n"].Values[2] = -999
			b.Int64s["key"].Values[0] = -999
			listStructGatherCheck(t, o.batch, rows)
			o.close()
			o.close()
			if m.Used() != 0 {
				t.Fatal("owner leak")
			}
		})
	}
}
func TestStructListInitialGatherReserveFailures(t *testing.T) {
	s, b := listStructGatherFixture(t)
	p, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100)
	if e != nil {
		t.Fatal(e)
	}
	m := &listStructGatherManager{Manager: memory.NewBudgetManager(0)}
	o, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if e != nil {
		t.Fatal(e)
	}
	calls, top, before := m.calls, m.firstTop, m.beforeTop
	if top <= 1 || before <= p.gatherBytes-p.recursiveBytes || len(o.recursiveOwners) != 2 || len(o.structListOwners) != 2 {
		t.Fatal("did not calibrate complete tree before first top List", calls, top, before)
	}
	t.Logf("actual Reserve count=%d first standalone root=%d used before=%d", calls, top, before)
	o.close()
	for at := 1; at <= calls; at++ {
		t.Run(fmt.Sprint(at), func(t *testing.T) {
			m := &listStructGatherManager{Manager: memory.NewBudgetManager(0), failAt: at}
			o, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
			if o != nil || !errors.Is(e, memory.ErrBudgetExceeded) || m.Used() != 0 {
				t.Fatal("ordinal failure cleanup", o, e, m.Used())
			}
			m.failAt = 0
			m.calls = 0
			o, e = newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
			if e != nil {
				t.Fatal(e)
			}
			listStructGatherCheck(t, o.batch, []int{1, 3, 2, 0})
			o.close()
			if m.Used() != 0 {
				t.Fatal("retry cleanup")
			}
		})
	}
}

func TestStructListInitialGatherUndeclaredFlatStorage(t *testing.T) {
	f, e := memory.ListStructField("items", true, true, memory.Field{Name: "n", Type: memory.TypeInt64, Nullable: true})
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Rows = 65
	b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, 65)}
	child := memory.NewBatch()
	b.StructLists["items"] = &memory.StructListVector{Offsets: make([]uint32, 66), Values: &memory.StructVector{Fields: f.Children[0].Children, Values: child}}
	child.Int64s["n"] = &memory.Int64Vector{}
	b.Int64s["extra"] = &memory.Int64Vector{Values: make([]int64, 65), Validity: memory.Bitmap{^uint64(0)}}
	s, e := memory.NewSchemaWithStructListsChecked(memory.Field{Name: "key", Type: memory.TypeInt64}, f)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := planExternalSortInitialGather(s, b, 0, 65, 1, 0); e == nil {
		t.Fatal("plan accepted malformed undeclared flat storage before gather")
	}
}

type listStructGatherCancelContext struct {
	context.Context
	target       string
	checks, stop int
	canceled     bool
}

func (c *listStructGatherCancelContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, c.target) {
			c.checks++
			if c.checks == c.stop {
				c.canceled = true
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
func TestStructListInitialGatherCancellationAndOldLive(t *testing.T) {
	s, b := listStructGatherFixture(t)
	ctx := &listStructGatherCancelContext{Context: context.Background(), target: ".selectionRunFieldSize", stop: 3}
	if _, e := planExternalSortInitialGatherValidatedContext(ctx, s, b, 0, 4, 1, 100); !errors.Is(e, context.Canceled) || ctx.checks != 3 {
		t.Fatal("plan preReserve scan cancel", e, ctx.checks)
	}
	p, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100)
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	old, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if e != nil {
		t.Fatal(e)
	}
	defer old.close()
	oldCharge := m.Used()
	f, e := memory.ListStructField("items", false, false, memory.Field{Name: "s", Type: memory.TypeString})
	if e != nil {
		t.Fatal(e)
	}
	text := strings.Repeat("界", 44000)
	values := memory.NewBatch()
	values.Rows = 1
	values.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(text))}, Data: []byte(text)}
	long := memory.NewBatch()
	long.Rows = 1
	long.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
	long.StructLists["items"] = &memory.StructListVector{Offsets: []uint32{0, 1}, Values: &memory.StructVector{Rows: 1, Fields: f.Children[0].Children, Values: values}}
	ls, e := memory.NewSchemaWithStructListsChecked(memory.Field{Name: "key", Type: memory.TypeInt64}, f)
	if e != nil {
		t.Fatal(e)
	}
	lp, e := planExternalSortInitialGather(ls, long, 0, 1, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	ctx = &listStructGatherCancelContext{Context: context.Background(), target: ".appendRecursiveString", stop: 2}
	out, e := newExternalSortInitialGather(ctx, m, ls, long, []SortKey{{Column: "key"}}, "ordinal", 0, lp)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.checks != 2 || m.Used() != oldCharge {
		t.Fatal("gather actual copy cancellation", out, e, ctx.checks, m.Used(), oldCharge)
	}
	listStructGatherCheck(t, old.batch, []int{1, 3, 2, 0})
	if string(values.Strings["s"].Data) != text {
		t.Fatal("source changed")
	}
	out, e = newExternalSortInitialGather(context.Background(), m, ls, long, []SortKey{{Column: "key"}}, "ordinal", 0, lp)
	if e != nil {
		t.Fatal(e)
	}
	if string(out.batch.StructLists["items"].Values.Values.Strings["s"].Data) != text {
		t.Fatal("retry long payload")
	}
	out.close()
	old.close()
	if m.Used() != 0 {
		t.Fatal("final cleanup")
	}
}
func TestStructListInitialGatherHiddenInvalidBeforeReserve(t *testing.T) {
	s, b := listStructGatherFixture(t)
	b.StructLists["first"].Values.Values.Strings["s"].Offsets[1] = 999
	if _, e := planExternalSortInitialGather(s, b, 0, 0, 1, 100); e == nil {
		t.Fatal("empty range bypassed hidden malformed payload")
	}
}
