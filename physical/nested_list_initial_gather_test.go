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

func nestedGatherFixture(t *testing.T, depth int) (memory.Schema, *memory.Batch) {
	s, b := listStructGatherFixture(t)
	fields := append([]memory.Field(nil), s.Fields...)
	for col := 0; col < 2; col++ {
		inner := memory.ListInt64Field("item", true, true)
		kind := memory.TypeInt64
		if col == 1 {
			inner = memory.ListStringField("item", true, true)
			kind = memory.TypeString
		}
		f, e := memory.NestedListField(fmt.Sprintf("array%d", col), true, inner)
		if e != nil {
			t.Fatal(e)
		}
		leaf := &memory.ArrayNode{Kind: kind}
		if col == 0 {
			leaf.Int64 = &memory.Int64Vector{Values: []int64{91, 21, 22, 23}, Validity: memory.Bitmap{11}}
		} else {
			leaf.Strings = &memory.StringViewVector{Offsets: []uint32{0, 1, 4, 4, 5}, Data: []byte("a界z"), Validity: memory.Bitmap{11}, Encoding: memory.EncodingView}
		}
		v := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 1, 2, 4}, Validity: memory.Bitmap{14}, Child: &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 1, 3, 4}, Validity: memory.Bitmap{13}, Child: leaf}}
		if col == 0 && depth > 0 {
			child := memory.NewBatch()
			child.Rows = 4
			child.Arrays[f.Name] = v
			for d := 0; d < depth; d++ {
				f = memory.StructField("ancestor", true, f)
				node := &memory.StructVector{Rows: 4, Fields: f.Children, Values: child}
				if d == depth-1 {
					node.Validity = memory.Bitmap{7}
				}
				child = memory.NewBatch()
				child.Rows = 4
				child.Structs[f.Name] = node
			}
			b.Structs[f.Name] = child.Structs[f.Name]
		} else {
			b.Arrays[f.Name] = v
		}
		fields = append(fields, f)
	}
	out, e := memory.NewSchemaWithNestedListsChecked(fields...)
	if e != nil {
		t.Fatal(e)
	}
	return out, b
}
func nestedGatherCheck(t *testing.T, b *memory.Batch, depth int, rows []int) {
	t.Helper()
	listStructGatherCheck(t, b, rows)
	first := b.Arrays["array0"]
	if depth > 0 {
		node := b.Structs["ancestor"]
		for i, r := range rows {
			if node.IsValid(i) != (r != 3) {
				t.Fatal("outer domain")
			}
		}
		for d := 1; d < depth; d++ {
			node = node.Values.Structs["ancestor"]
		}
		first = node.Values.Arrays["array0"]
	}
	expected := [][]any{{nil, []any{}, []any{nil}, []any{[]any{int64(21), nil}, []any{int64(23)}}}, {nil, []any{}, []any{nil}, []any{[]any{"界", nil}, []any{"z"}}}}
	for col, v := range []*memory.ArrayNode{first, b.Arrays["array1"]} {
		if v == nil {
			t.Fatal("missing array")
		}
		for i, r := range rows {
			want := expected[col][r]
			if col == 0 && depth > 0 && r == 3 {
				want = nil
			}
			if got := nestedCompactValue(v, i); !reflect.DeepEqual(got, want) {
				t.Fatalf("col%d row%d got%#v want%#v", col, r, got, want)
			}
		}
	}
}
func TestNestedListInitialGatherOwnershipAndFailures(t *testing.T) {
	for _, depth := range []int{0, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := nestedGatherFixture(t, depth)
			p, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100)
			if e != nil {
				t.Fatal(e)
			}
			m := &listStructGatherManager{Manager: memory.NewBudgetManager(p.gatherBytes)}
			o, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
			if e != nil {
				t.Fatal(e)
			}
			nestedGatherCheck(t, o.batch, depth, []int{1, 3, 2, 0})
			calls := m.calls
			if m.peak > p.gatherBytes {
				t.Fatal("underestimate", m.peak, p.gatherBytes)
			}
			t.Logf("depth=%d actual Reserve calls=%d peak=%d plan=%d", depth, calls, m.peak, p.gatherBytes)
			o.close()
			if m.Used() != 0 {
				t.Fatal("close leak")
			}
			for at := 1; at <= calls; at++ {
				fm := &listStructGatherManager{Manager: memory.NewBudgetManager(0), failAt: at}
				bad, err := newExternalSortInitialGather(context.Background(), fm, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
				if bad != nil || !errors.Is(err, memory.ErrBudgetExceeded) || fm.Used() != 0 {
					t.Fatal("fault rollback", at, err, fm.Used())
				}
				fm.failAt = 0
				good, err := newExternalSortInitialGather(context.Background(), fm, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
				if err != nil {
					t.Fatal(err)
				}
				nestedGatherCheck(t, good.batch, depth, []int{1, 3, 2, 0})
				good.close()
				if fm.Used() != 0 {
					t.Fatal("retry leak")
				}
			}
		})
	}
}

func TestNestedListInitialGatherSecondCompleteRoot(t *testing.T) {
	s, b := nestedGatherFixture(t, 0)
	p, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100)
	if e != nil {
		t.Fatal(e)
	}
	m := &nestedCompactRootFault{Manager: memory.NewBudgetManager(0), deny: true}
	out, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if out != nil || !errors.Is(e, memory.ErrBudgetExceeded) || m.roots != 2 || m.beforeSecond <= m.firstFee || m.Used() != 0 {
		t.Fatal("second actual root rollback", e, m.roots, m.beforeSecond, m.firstFee, m.Used())
	}
	t.Logf("first complete root charge=%d live before second root=%d", m.firstFee, m.beforeSecond)
	m.deny = false
	out, e = newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if e != nil {
		t.Fatal(e)
	}
	nestedGatherCheck(t, out.batch, 0, []int{1, 3, 2, 0})
	out.close()
	if m.Used() != 0 {
		t.Fatal("retry leak")
	}
}
func TestNestedListInitialGatherCopyCancelAndPoison(t *testing.T) {
	s, b := nestedGatherFixture(t, 8)
	text := strings.Repeat("界", 44000)
	v := b.Arrays["array1"].Child.Child.Strings
	v.Data = []byte("a" + text + "z")
	v.Offsets = []uint32{0, 1, uint32(1 + len(text)), uint32(1 + len(text)), uint32(2 + len(text))}
	p, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100)
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	ctx := &nestedCompactCancel{Context: context.Background(), manager: m}
	out, e := newExternalSortInitialGather(ctx, m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if out != nil || !errors.Is(e, context.Canceled) || ctx.count != 2 || ctx.charge <= 0 || m.Used() != 0 {
		t.Fatal("copy rollback", e, ctx.count, ctx.charge, m.Used())
	}
	out, e = newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
	if e != nil {
		t.Fatal(e)
	}
	for i := range v.Data {
		v.Data[i] = 'x'
	}
	want := []any{[]any{text, nil}, []any{"z"}}
	if got := nestedCompactValue(out.batch.Arrays["array1"], 1); !reflect.DeepEqual(got, want) {
		t.Fatal("owned source poison", got)
	}
	out.close()
	if m.Used() != 0 {
		t.Fatal("retry release")
	}
}

func TestNestedListInitialGatherEmptyAndHiddenStorage(t *testing.T) {
	s, b := nestedGatherFixture(t, 8)
	for _, bounds := range [][2]int{{0, 0}, {4, 4}} {
		p, e := planExternalSortInitialGather(s, b, bounds[0], bounds[1], 1, 100)
		if e != nil {
			t.Fatal(e)
		}
		m := memory.NewBudgetManager(p.gatherBytes)
		out, e := newExternalSortInitialGather(context.Background(), m, s, b, []SortKey{{Column: "key"}}, "ordinal", 100, p)
		if e != nil {
			t.Fatal(e)
		}
		nestedGatherCheck(t, out.batch, 8, nil)
		out.close()
		if m.Used() != 0 {
			t.Fatal("empty release")
		}
	}
	b.Arrays["array1"].Child.Offsets[1] = 99
	if _, e := planExternalSortInitialGather(s, b, 0, 0, 1, 100); e == nil {
		t.Fatal("empty range accepted hidden malformed storage")
	}
}

type nestedGatherEstimateContext struct {
	context.Context
	checks   int
	canceled bool
}

func (c *nestedGatherEstimateContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [40]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".EstimateColumnarRunRangeBytesWithCheck") {
			c.checks++
			if c.checks == 10 {
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
func TestNestedListInitialGatherEncodedEstimateCancel(t *testing.T) {
	s, b := nestedGatherFixture(t, 8)
	ctx := &nestedGatherEstimateContext{Context: context.Background()}
	_, e := planExternalSortInitialGatherValidatedContext(ctx, s, b, 0, 4, 1, 100)
	if !errors.Is(e, context.Canceled) || ctx.checks != 10 {
		t.Fatal("last encoded estimator cancellation", e, ctx.checks)
	}
	// Planner has no manager/Reserve path; the same unchanged input retries successfully.
	if _, e := planExternalSortInitialGather(s, b, 0, 4, 1, 100); e != nil {
		t.Fatal(e)
	}
}
