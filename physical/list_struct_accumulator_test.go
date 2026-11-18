package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"runtime"
	"strings"
	"testing"
)

func listStructAccumulatorFixture(t *testing.T) (memory.Schema, *memory.Batch, string) {
	text := strings.Repeat("界", 44000)
	makeList := func(name string) (memory.Field, *memory.StructListVector) {
		f, e := memory.ListStructField(name, true, true, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		b.Rows = 4
		b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, uint32(1 + len(text)), uint32(1 + len(text)), uint32(1 + 2*len(text))}, Data: []byte("a" + text + text), Validity: memory.Bitmap{11}}
		return f, &memory.StructListVector{Offsets: []uint32{0, 1, 2, 3, 4, 4}, Validity: memory.Bitmap{23}, Values: &memory.StructVector{Rows: 4, Fields: f.Children[0].Children, Values: b, Validity: memory.Bitmap{11}}}
	}
	first, a := makeList("first")
	second, b := makeList("second")
	child := memory.NewBatch()
	child.Rows = 5
	child.StructLists["second"] = b
	root := memory.StructField("root", true, second)
	out := memory.NewBatch()
	out.Rows = 5
	out.StructLists["first"] = a
	out.Structs["root"] = &memory.StructVector{Rows: 5, Fields: root.Children, Values: child, Validity: memory.Bitmap{27}}
	schema, e := memory.NewSchemaWithStructListsChecked(first, root)
	if e != nil {
		t.Fatal(e)
	}
	return schema, out, text
}
func listStructAccumulatorData(a *externalSortAccumulator) (*memory.StringViewVector, *memory.StringViewVector) {
	return a.batch.StructLists["first"].Values.Values.Strings["s"], a.batch.Structs["root"].Values.StructLists["second"].Values.Values.Strings["s"]
}

type listStructAccumulatorManager struct {
	memory.Manager
	a               *externalSortAccumulator
	calls, failedAt int
	failSecond      bool
	peak            int64
}

func (m *listStructAccumulatorManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.failSecond && m.a != nil {
		first, second := listStructAccumulatorData(m.a)
		if len(first.Data) > 100000 && len(second.Data) == 1 {
			var pcs [30]uintptr
			k := runtime.Callers(2, pcs[:])
			frames := runtime.CallersFrames(pcs[:k])
			for {
				f, more := frames.Next()
				if strings.Contains(f.Function, "externalSortAccumulator).appendBytes") {
					m.failedAt = m.calls
					return nil, memory.ErrBudgetExceeded
				}
				if !more {
					break
				}
			}
		}
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, e
}
func listStructAccumulatorNew(t *testing.T, s memory.Schema) (*externalSortAccumulator, *sortWorkspace, *listStructAccumulatorManager) {
	t.Helper()
	m := &listStructAccumulatorManager{Manager: memory.NewBudgetManager(0)}
	w := &sortWorkspace{parent: m}
	a, e := newExternalSortAccumulator(context.Background(), s, w, 0)
	if e != nil {
		t.Fatal(e)
	}
	m.a = a
	return a, w, m
}
func listStructAccumulatorTerminal(t *testing.T, a *externalSortAccumulator, w *sortWorkspace, m *listStructAccumulatorManager, b *memory.Batch) {
	t.Helper()
	if !a.failed || a.batch.Rows != 1 {
		t.Fatal("partial row published", a.failed, a.batch.Rows)
	}
	if !errors.Is(a.finish(context.Background()), errExternalSortAccumulatorFailed) || !errors.Is(a.appendRow(context.Background(), b, 0), errExternalSortAccumulatorFailed) {
		t.Fatal("failed accumulator reused")
	}
	a.close()
	w.close()
	if m.Used() != 0 || w.Used() != 0 {
		t.Fatal("terminal graph cleanup", m.Used(), w.Used())
	}
}
func TestStructListAccumulatorLateStringAdmission(t *testing.T) {
	s, b, text := listStructAccumulatorFixture(t)
	a, w, m := listStructAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	m.failSecond = true
	e := a.appendRow(context.Background(), b, 1)
	if !errors.Is(e, memory.ErrBudgetExceeded) || m.failedAt == 0 {
		t.Fatal("did not hit real second-string Reserve", e, m.failedAt)
	}
	first, second := listStructAccumulatorData(a)
	if string(first.Data) != "a"+text || string(second.Data) != "a" {
		t.Fatal("wrong late failure stage")
	}
	if a.reservation.Bytes() < a.metadataBytes+a.capacityBytes || m.Used() == 0 {
		t.Fatal("live buffers lost charge")
	}
	t.Logf("late second String Reserve=%d capacity=%d live parent=%d", m.failedAt, a.capacityBytes, m.Used())
	listStructAccumulatorTerminal(t, a, w, m, b)
}

type listStructAccumulatorCopyContext struct {
	context.Context
	a        *externalSortAccumulator
	checks   int
	canceled bool
	growth   bool
	charged  bool
}

func (c *listStructAccumulatorCopyContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	first, second := listStructAccumulatorData(c.a)
	if len(first.Data) < 100000 {
		return nil
	}
	var pcs [30]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	f, _ := frames.Next()
	match := strings.Contains(f.Function, "externalSortAccumulator).appendBytes") && len(second.Data) > 100000
	if c.growth {
		match = strings.Contains(f.Function, "externalSortGrowBuffer") && len(second.Data) > 32768 && len(first.Data) > len(second.Data)
	}
	if match {
		c.checks++
		if c.checks == 2 {
			c.canceled = true
			c.charged = c.a.reservation.Bytes() >= c.a.metadataBytes+c.a.capacityBytes
			if c.growth {
				c.charged = c.a.reservation.Bytes() >= c.a.metadataBytes+c.a.capacityBytes+int64(len(second.Data)+132000)
			}
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestStructListAccumulatorSecondStringCopyCancellation(t *testing.T) {
	s, b, text := listStructAccumulatorFixture(t)
	a, w, m := listStructAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	ctx := &listStructAccumulatorCopyContext{Context: context.Background(), a: a}
	e := a.appendRow(ctx, b, 1)
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || !ctx.charged {
		t.Fatal("second-string chunk cancel", e, ctx.checks, ctx.charged)
	}
	first, _ := listStructAccumulatorData(a)
	if string(first.Data) != "a"+text {
		t.Fatal("first leaf did not finish before second copy")
	}
	listStructAccumulatorTerminal(t, a, w, m, b)
	// Fresh owner can process all rows after cancellation; source buffers remain intact.
	a, w, m = listStructAccumulatorNew(t, s)
	defer w.close()
	defer a.close()
	for row := 0; row < 5; row++ {
		if e := a.appendRow(context.Background(), b, row); e != nil {
			t.Fatal(e)
		}
	}
	if e := a.finish(context.Background()); e != nil {
		t.Fatal(e)
	}
	for column, v := range []*memory.StructListVector{a.batch.StructLists["first"], a.batch.Structs["root"].Values.StructLists["second"]} {
		want := []uint32{0, 1, 2, 3, 3, 3}
		if column == 1 {
			want = []uint32{0, 1, 2, 2, 2, 2}
		}
		for i, x := range want {
			if v.Offsets[i] != x {
				t.Fatal("NULL/empty canonical span", column, v.Offsets)
			}
		}
		if string(v.Values.Values.Strings["s"].Data) != "a"+text {
			t.Fatal("retry content")
		}
	}
	a.close()
	w.close()
	if m.Used() != 0 {
		t.Fatal("retry cleanup")
	}
}

func TestStructListAccumulatorSecondOldBufferCopyCancellation(t *testing.T) {
	s, b, text := listStructAccumulatorFixture(t)
	a, w, m := listStructAccumulatorNew(t, s)
	for _, row := range []int{0, 1} {
		if e := a.appendRow(context.Background(), b, row); e != nil {
			t.Fatal(e)
		}
	}
	ctx := &listStructAccumulatorCopyContext{Context: context.Background(), a: a, growth: true}
	e := a.appendRow(ctx, b, 1)
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || !ctx.charged || !a.failed || a.batch.Rows != 2 {
		t.Fatal("old/new coexistence copy cancel", e, ctx.checks, ctx.charged, a.batch.Rows)
	}
	first, second := listStructAccumulatorData(a)
	if string(first.Data) != "a"+text+text || string(second.Data) != "a"+text {
		t.Fatal("wrong old-buffer copy stage")
	}
	if !errors.Is(a.finish(context.Background()), errExternalSortAccumulatorFailed) || !errors.Is(a.appendRow(context.Background(), b, 0), errExternalSortAccumulatorFailed) {
		t.Fatal("growth failure reused")
	}
	a.close()
	w.close()
	if m.Used() != 0 || w.Used() != 0 {
		t.Fatal("growth failure leak")
	}
}

func TestStructListAccumulatorWideElementMetadataExact(t *testing.T) {
	var previous int64
	for _, width := range []int{1, 64} {
		fields := make([]memory.Field, width)
		for i := range fields {
			fields[i] = memory.Field{Name: strings.Repeat("n", i+1), Type: memory.TypeString, Nullable: true}
		}
		f, e := memory.ListStructField("items", true, true, fields...)
		if e != nil {
			t.Fatal(e)
		}
		s, e := memory.NewSchemaWithStructListsChecked(f)
		if e != nil {
			t.Fatal(e)
		}
		n, e := externalSortAccumulatorMetadataBytes(s)
		if e != nil {
			t.Fatal(e)
		}
		if n <= previous {
			t.Fatal("wide metadata uncharged", n, previous)
		}
		previous = n
		for _, delta := range []int64{-1, 0} {
			w, parent := externalAccumulatorTestWorkspace(t, n+delta)
			parent.deny = true
			before := parent.Used()
			a, e := newExternalSortAccumulator(context.Background(), s, w, 0)
			if delta < 0 {
				if a != nil || !errors.Is(e, memory.ErrBudgetExceeded) || w.Used() != 0 || parent.Used() != before {
					t.Fatal("metadata minus1", a, e, w.Used())
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				if w.Used() != n || a.reservation.Bytes() != n || len(a.batch.StructLists["items"].Values.Fields) != width {
					t.Fatal("exact descriptor charge")
				}
				a.close()
			}
			w.close()
			if parent.Used() != 0 {
				t.Fatal("metadata owner leak")
			}
		}
	}
}
