package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func nestedCompactFixture(t *testing.T, depth int) *memory.Batch {
	t.Helper()
	_, b := listStructCompactFixture(t, false)
	for col := 0; col < 2; col++ {
		kind := memory.TypeInt64
		if col == 1 {
			kind = memory.TypeString
		}
		leaf := memory.Field{Name: "item", Type: kind, Nullable: true}
		if col == 1 {
			leaf.Encoding = memory.EncodingView
		}
		f, e := memory.NestedListField(fmt.Sprintf("array%d", col), true, memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Children: []memory.Field{leaf}})
		if e != nil {
			t.Fatal(e)
		}
		term := &memory.ArrayNode{Kind: kind}
		if col == 0 {
			term.Int64 = &memory.Int64Vector{Values: []int64{0, 21, 22, 23}, Validity: memory.Bitmap{11}}
		} else {
			term.Strings = &memory.StringViewVector{Offsets: []uint32{0, 1, 4, 4, 5}, Data: []byte("a界z"), Validity: memory.Bitmap{11}, Encoding: memory.EncodingView}
		}
		v := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 1, 3}, Child: &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 3, 4}, Child: term}}
		if col == 0 && depth > 0 {
			child := memory.NewBatch()
			child.Rows = 3
			child.Arrays[f.Name] = v
			for i := 0; i < depth; i++ {
				f = memory.StructField("inner", true, f)
				sv := &memory.StructVector{Rows: 3, Fields: f.Children, Values: child}
				if i == depth-1 {
					sv.Validity = memory.Bitmap{5}
				}
				child = memory.NewBatch()
				child.Rows = 3
				child.Structs["inner"] = sv
			}
			b.Structs["tree"] = child.Structs["inner"]
		} else {
			b.Arrays[f.Name] = v
		}
	}
	return b
}
func nestedCompactValue(v *memory.ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	switch v.Kind {
	case memory.TypeList:
		out := make([]any, v.Offsets[row+1]-v.Offsets[row])
		for i := v.Offsets[row]; i < v.Offsets[row+1]; i++ {
			out[i-v.Offsets[row]] = nestedCompactValue(v.Child, int(i))
		}
		return out
	case memory.TypeInt64:
		return v.Int64.Values[row]
	case memory.TypeString:
		return string(v.Strings.ValueBytes(row))
	}
	panic("bad kind")
}
func nestedCompactCheck(t *testing.T, b *memory.Batch, depth int) {
	t.Helper()
	listStructCompactCheck(t, b, false)
	cur := b
	if depth > 0 {
		v := b.Structs["tree"]
		for d := 0; d < depth; d++ {
			if v.Rows != 2 || !v.IsValid(0) || !v.IsValid(1) {
				t.Fatal("tree rows/NULL")
			}
			cur = v.Values
			if d+1 < depth {
				v = cur.Structs["inner"]
			}
		}
	}
	wants := [][]any{{[]any{[]any{int64(0)}}, []any{[]any{int64(21), nil}, []any{int64(23)}}}, {[]any{[]any{"a"}}, []any{[]any{"界", nil}, []any{"z"}}}}
	for col := 0; col < 2; col++ {
		v := b.Arrays["array1"]
		if col == 0 {
			v = cur.Arrays["array0"]
		}
		if v == nil {
			t.Fatal("lost root", col)
		}
		for row := 0; row < 2; row++ {
			if got := nestedCompactValue(v, row); !reflect.DeepEqual(got, wants[col][row]) {
				t.Fatalf("root%d row%d got%#v", col, row, got)
			}
		}
	}
}
func TestNestedListCompactOwnedMixedRoots(t *testing.T) {
	for _, depth := range []int{0, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			src := nestedCompactFixture(t, depth)
			m := &listStructCompactFault{base: memory.NewBudgetManager(0)}
			w := compactWorkspace{manager: m, bounded: true}
			rows := []int{0, 2}
			mask := memory.Bitmap{5}
			out, e := w.compact(context.Background(), src, rows, mask, kernel.New(kernel.ModeScalar))
			if e != nil {
				t.Fatal(e)
			}
			nestedCompactCheck(t, out, depth)
			calls := m.calls
			charge := m.Used()
			w.release()
			if m.Used() != 0 {
				t.Fatal("close")
			}
			t.Logf("successful full mixed-root trace requests=%v", m.requests)
			for fail := 1; fail <= calls; fail++ {
				fm := &listStructCompactFault{base: memory.NewBudgetManager(0), fail: fail}
				fw := compactWorkspace{manager: fm, bounded: true}
				out, e = fw.compact(context.Background(), src, rows, mask, kernel.New(kernel.ModeScalar))
				if e == nil || out != nil {
					t.Fatal("fault returned output", fail, e)
				}
				fw.release()
				if fm.Used() != 0 {
					t.Fatal("fault leak", fail)
				}
				fm.fail = 0
				fw = compactWorkspace{manager: fm, bounded: true}
				out, e = fw.compact(context.Background(), src, rows, mask, kernel.New(kernel.ModeScalar))
				if e != nil {
					t.Fatal(e)
				}
				nestedCompactCheck(t, out, depth)
				fw.release()
			}
			for _, limit := range []int64{charge, charge - 1} {
				bm := memory.NewBudgetManager(limit)
				cw := compactWorkspace{manager: bm, bounded: true}
				out, e = cw.compact(context.Background(), src, rows, mask, kernel.New(kernel.ModeScalar))
				if limit == charge {
					if e != nil {
						t.Fatal("exact", e)
					}
					nestedCompactCheck(t, out, depth)
				} else if e == nil || out != nil {
					t.Fatal("exact-1 unexpectedly succeeds")
				}
				cw.release()
				if bm.Used() != 0 {
					t.Fatal("tight cleanup")
				}
			}
		})
	}
}

type nestedCompactCancel struct {
	context.Context
	manager  memory.Manager
	count    int
	charge   int64
	canceled bool
}

func (c *nestedCompactCancel) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [32]uintptr
	fs := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		f, more := fs.Next()
		if strings.HasSuffix(f.Function, ".copyNestedListBytes") {
			c.count++
			if c.count == 2 {
				c.canceled = true
				c.charge = c.manager.Used()
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
func TestNestedListCompactLongCopyCancel(t *testing.T) {
	src := nestedCompactFixture(t, 8)
	text := "xx" + strings.Repeat("界", 44000)
	s := src.Arrays["array1"].Child.Child.Strings
	s.Data = []byte(text + "界z")
	s.Offsets = []uint32{0, uint32(len(text)), uint32(len(text) + 3), uint32(len(text) + 3), uint32(len(text) + 4)}
	m := memory.NewBudgetManager(0)
	w := compactWorkspace{manager: m, bounded: true}
	ctx := &nestedCompactCancel{Context: context.Background(), manager: m}
	out, e := w.compact(ctx, src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
	if !errors.Is(e, context.Canceled) || out != nil || ctx.count != 2 || ctx.charge <= 0 {
		t.Fatalf("copy cancellation %v count%d charge%d", e, ctx.count, ctx.charge)
	}
	w.release()
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
	if string(src.Arrays["array1"].Child.Child.Strings.Data) != text+"界z" {
		t.Fatal("source changed")
	}
	w = compactWorkspace{manager: m, bounded: true}
	out, e = w.compact(context.Background(), src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	if got := nestedCompactValue(out.Arrays["array1"], 0); !reflect.DeepEqual(got, []any{[]any{text}}) {
		t.Fatal("copy retry")
	}
	w.release()
	if m.Used() != 0 {
		t.Fatal("retry leak")
	}
}

type nestedCompactRootFault struct {
	memory.Manager
	roots                  int
	deny                   bool
	firstFee, beforeSecond int64
}

func (m *nestedCompactRootFault) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	var pcs [32]uintptr
	fs := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	root := false
	for {
		f, more := fs.Next()
		if strings.HasSuffix(f.Function, ".copyNestedListOwned") {
			root = true
			break
		}
		if !more {
			break
		}
	}
	if root {
		m.roots++
		if m.roots == 1 {
			m.firstFee = n
		}
		if m.roots == 2 && m.deny {
			m.beforeSecond = m.Used()
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNestedListCompactSecondCompleteOwnerAndInvalidation(t *testing.T) {
	for _, depth := range []int{0, 8} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth%d/warm%v", depth, warm), func(t *testing.T) {
				src := nestedCompactFixture(t, depth)
				m := &nestedCompactRootFault{Manager: memory.NewBudgetManager(0)}
				w := compactWorkspace{manager: m, bounded: true}
				defer w.release()
				var old *memory.Batch
				var flat int64
				if warm {
					var e error
					old, e = w.compact(context.Background(), src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
					if e != nil {
						t.Fatal(e)
					}
					nestedCompactCheck(t, old, depth)
					flat = w.lease.Bytes()
				}
				m.roots = 0
				m.deny = true
				out, e := w.compact(context.Background(), src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
				if !errors.Is(e, memory.ErrBudgetExceeded) || out != nil || m.roots != 2 || m.beforeSecond <= flat+m.firstFee || m.Used() != flat || w.extended != nil {
					t.Fatalf("complete-root denial e%v roots%d first%d secondlive%d flat%d live%d", e, m.roots, m.firstFee, m.beforeSecond, flat, m.Used())
				}
				if warm && (len(old.Arrays) != 0 || len(old.StructLists) != 0 || len(old.Structs) != 0) {
					t.Fatal("prior borrowed output not invalidated")
				}
				t.Logf("second nested owner denied with first fee %d live, aggregate charge %d (flat %d); map iteration order irrelevant", m.firstFee, m.beforeSecond, flat)
				m.deny = false
				out, e = w.compact(context.Background(), src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
				if e != nil {
					t.Fatal(e)
				}
				nestedCompactCheck(t, out, depth)
				w.release()
				if m.Used() != 0 {
					t.Fatal("final release")
				}
			})
		}
	}
}

func TestNestedListCompactSourcePoison(t *testing.T) {
	src := nestedCompactFixture(t, 8)
	m := memory.NewBudgetManager(0)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	out, e := w.compact(context.Background(), src, []int{0, 2}, memory.Bitmap{5}, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	src.Int64s["id"].Values[0] = -999
	src.Arrays["array1"].Child.Child.Strings.Data[0] = '!'
	src.Arrays["array1"].Offsets[0] = 999
	cur := src.Structs["tree"]
	for d := 1; d < 8; d++ {
		cur = cur.Values.Structs["inner"]
	}
	cur.Values.Arrays["array0"].Child.Child.Int64.Values[0] = -999
	src.StructLists["list0"].Values.Values.Int64s["code"].Values[0] = -999
	nestedCompactCheck(t, out, 8)
	w.release()
	if m.Used() != 0 {
		t.Fatal("poison owner cleanup")
	}
}
