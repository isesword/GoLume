package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"runtime"
	"strings"
	"testing"
)

func nestedListAccumulatorFixture(t *testing.T) (memory.Schema, *memory.Batch, string) {
	text := strings.Repeat("界", 44000)
	makeList := func(name string) (memory.Field, *memory.ArrayNode) {
		f, e := memory.NestedListField(name, true, memory.ListStringField("item", true, true))
		if e != nil {
			t.Fatal(e)
		}
		terminal := &memory.ArrayNode{Kind: memory.TypeString, Strings: &memory.StringViewVector{Offsets: []uint32{0, 1, uint32(1 + len(text)), uint32(1 + len(text)), uint32(1 + 2*len(text))}, Data: []byte("a" + text + text), Validity: memory.Bitmap{11}, Encoding: memory.EncodingView}}
		inner := &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 2, 3, 4}, Validity: memory.Bitmap{11}, Child: terminal}
		return f, &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0, 1, 2, 3, 4, 4}, Validity: memory.Bitmap{23}, Child: inner}
	}
	first, a := makeList("first")
	second, b := makeList("second")
	child := memory.NewBatch()
	child.Rows = 5
	child.Arrays["second"] = b
	root := memory.StructField("root", true, second)
	out := memory.NewBatch()
	out.Rows = 5
	out.Arrays["first"] = a
	out.Structs["root"] = &memory.StructVector{Rows: 5, Fields: root.Children, Values: child, Validity: memory.Bitmap{27}}
	schema, e := memory.NewSchemaWithNestedListsChecked(first, root)
	if e != nil {
		t.Fatal(e)
	}
	return schema, out, text
}
func nestedListAccumulatorData(a *externalSortAccumulator) (*memory.StringViewVector, *memory.StringViewVector) {
	return a.batch.Arrays["first"].Child.Child.Strings, a.batch.Structs["root"].Values.Arrays["second"].Child.Child.Strings
}

type nestedListAccumulatorManager struct {
	memory.Manager
	a               *externalSortAccumulator
	calls, failedAt int
	failSecond      bool
	peak            int64
}

func (m *nestedListAccumulatorManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.failSecond && m.a != nil {
		first, second := nestedListAccumulatorData(m.a)
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
func nestedListAccumulatorNew(t *testing.T, s memory.Schema) (*externalSortAccumulator, *sortWorkspace, *nestedListAccumulatorManager) {
	t.Helper()
	m := &nestedListAccumulatorManager{Manager: memory.NewBudgetManager(0)}
	w := &sortWorkspace{parent: m}
	a, e := newExternalSortAccumulator(context.Background(), s, w, 0)
	if e != nil {
		t.Fatal(e)
	}
	m.a = a
	return a, w, m
}
func nestedListAccumulatorTerminal(t *testing.T, a *externalSortAccumulator, w *sortWorkspace, m *nestedListAccumulatorManager, b *memory.Batch) {
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
func TestNestedListAccumulatorLateStringAdmission(t *testing.T) {
	s, b, text := nestedListAccumulatorFixture(t)
	a, w, m := nestedListAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	m.failSecond = true
	e := a.appendRow(context.Background(), b, 1)
	if !errors.Is(e, memory.ErrBudgetExceeded) || m.failedAt == 0 {
		t.Fatal("did not hit real second-string Reserve", e, m.failedAt)
	}
	first, second := nestedListAccumulatorData(a)
	if string(first.Data) != "a"+text || string(second.Data) != "a" {
		t.Fatal("wrong late failure stage")
	}
	if a.reservation.Bytes() < a.metadataBytes+a.capacityBytes || m.Used() == 0 {
		t.Fatal("live buffers lost charge")
	}
	t.Logf("late second String Reserve=%d capacity=%d live parent=%d", m.failedAt, a.capacityBytes, m.Used())
	nestedListAccumulatorTerminal(t, a, w, m, b)
}

type nestedListAccumulatorCopyContext struct {
	context.Context
	a        *externalSortAccumulator
	checks   int
	canceled bool
	growth   bool
	charged  bool
}

func (c *nestedListAccumulatorCopyContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	first, second := nestedListAccumulatorData(c.a)
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
func TestNestedListAccumulatorSecondStringCopyCancellation(t *testing.T) {
	s, b, text := nestedListAccumulatorFixture(t)
	a, w, m := nestedListAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	ctx := &nestedListAccumulatorCopyContext{Context: context.Background(), a: a}
	e := a.appendRow(ctx, b, 1)
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || !ctx.charged {
		t.Fatal("second-string chunk cancel", e, ctx.checks, ctx.charged)
	}
	first, _ := nestedListAccumulatorData(a)
	if string(first.Data) != "a"+text {
		t.Fatal("first leaf did not finish before second copy")
	}
	nestedListAccumulatorTerminal(t, a, w, m, b)
	// Fresh owner can process all rows after cancellation; source buffers remain intact.
	a, w, m = nestedListAccumulatorNew(t, s)
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
	for column, v := range []*memory.ArrayNode{a.batch.Arrays["first"], a.batch.Structs["root"].Values.Arrays["second"]} {
		want := []uint32{0, 1, 2, 3, 3, 3}
		if column == 1 {
			want = []uint32{0, 1, 2, 2, 2, 2}
		}
		for i, x := range want {
			if v.Offsets[i] != x {
				t.Fatal("NULL/empty canonical span", column, v.Offsets)
			}
		}
		if string(v.Child.Child.Strings.Data) != "a"+text {
			t.Fatal("retry content")
		}
	}
	a.close()
	w.close()
	if m.Used() != 0 {
		t.Fatal("retry cleanup")
	}
}

func TestNestedListAccumulatorSecondOldBufferCopyCancellation(t *testing.T) {
	s, b, text := nestedListAccumulatorFixture(t)
	a, w, m := nestedListAccumulatorNew(t, s)
	for _, row := range []int{0, 1} {
		if e := a.appendRow(context.Background(), b, row); e != nil {
			t.Fatal(e)
		}
	}
	ctx := &nestedListAccumulatorCopyContext{Context: context.Background(), a: a, growth: true}
	e := a.appendRow(ctx, b, 1)
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || !ctx.charged || !a.failed || a.batch.Rows != 2 {
		t.Fatal("old/new coexistence copy cancel", e, ctx.checks, ctx.charged, a.batch.Rows)
	}
	first, second := nestedListAccumulatorData(a)
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

func TestNestedListAccumulatorEmptySecondAxisEveryReserve(t *testing.T) {
	s, b, _ := nestedListAccumulatorFixture(t)
	for _, v := range []*memory.ArrayNode{b.Arrays["first"], b.Structs["root"].Values.Arrays["second"]} {
		v.Offsets = []uint32{0, 65, 4162, 4162, 4162, 4162}
		v.Validity = nil
		v.Child.Offsets = make([]uint32, 4163)
		v.Child.Validity = nil
		v.Child.Child.Strings = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	}
	b.Structs["root"].Validity = nil
	run := func(m *listStructGatherManager) (int64, error) {
		w := &sortWorkspace{parent: m}
		defer w.close()
		a, e := newExternalSortAccumulator(context.Background(), s, w, 0)
		if e != nil {
			return 0, e
		}
		defer a.close()
		for _, row := range []int{0, 1} {
			if e = a.appendRow(context.Background(), b, row); e != nil {
				if !a.failed || !errors.Is(a.finish(context.Background()), errExternalSortAccumulatorFailed) || !errors.Is(a.appendRow(context.Background(), b, 0), errExternalSortAccumulatorFailed) {
					t.Fatal("failed axis growth reused")
				}
				return 0, e
			}
		}
		if e = a.finish(context.Background()); e != nil {
			return 0, e
		}
		for _, v := range []*memory.ArrayNode{a.batch.Arrays["first"], a.batch.Structs["root"].Values.Arrays["second"]} {
			if v.Len() != 2 || v.Child.Len() != 4162 || v.Child.Child.Len() != 0 || v.Offsets[1] != 65 || v.Offsets[2] != 4162 {
				t.Fatal("independent R1 oracle", v.Offsets, v.Child.Len())
			}
			for _, off := range v.Child.Offsets {
				if off != 0 {
					t.Fatal("empty R2")
				}
			}
		}
		return m.Used(), nil
	}
	probe := &listStructGatherManager{Manager: memory.NewBudgetManager(0)}
	charge, e := run(probe)
	if e != nil || probe.Used() != 0 || charge <= 0 {
		t.Fatal("probe", e, charge, probe.Used())
	}
	calls, peak := probe.calls, probe.peak
	t.Logf("R0=2 R1=4162 R2=0 per root: reserve=%d live=%d peak=%d", calls, charge, peak)
	for at := 1; at <= calls; at++ {
		m := &listStructGatherManager{Manager: memory.NewBudgetManager(0), failAt: at}
		_, err := run(m)
		if !errors.Is(err, memory.ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("every axis Reserve failure", at, err, m.Used())
		}
	}
	for _, limit := range []int64{peak, peak - 1} {
		m := &listStructGatherManager{Manager: memory.NewBudgetManager(limit)}
		_, err := run(m)
		if limit == peak && err != nil {
			t.Fatal("exact peak", err)
		}
		if limit < peak && !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("peak-1", err)
		}
		if m.Used() != 0 {
			t.Fatal("limit cleanup")
		}
	}
}

func TestNestedListAccumulatorR1ThenR2AndLateNull(t *testing.T) {
	s, b, text := nestedListAccumulatorFixture(t)
	for _, v := range []*memory.ArrayNode{b.Arrays["first"], b.Structs["root"].Values.Arrays["second"]} {
		v.Offsets = []uint32{0, 4097, 4098, 4099, 4099, 4099}
		v.Validity = nil
		v.Child.Offsets = make([]uint32, 4100)
		v.Child.Offsets[4098] = 1
		v.Child.Offsets[4099] = 1
		mask := make(memory.Bitmap, memory.WordsFor(4099))
		for i := 0; i < 4098; i++ {
			mask[i/64] |= uint64(1) << uint(i%64)
		}
		v.Child.Validity = mask
		v.Child.Child.Strings = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(text))}, Data: []byte(text), Encoding: memory.EncodingView}
	}
	b.Structs["root"].Validity = nil
	run := func(m *listStructGatherManager) error {
		w := &sortWorkspace{parent: m}
		defer w.close()
		a, e := newExternalSortAccumulator(context.Background(), s, w, 0)
		if e != nil {
			return e
		}
		defer a.close()
		for _, row := range []int{0, 1, 2} {
			if e = a.appendRow(context.Background(), b, row); e != nil {
				if !a.failed || !errors.Is(a.finish(context.Background()), errExternalSortAccumulatorFailed) || !errors.Is(a.appendRow(context.Background(), b, 0), errExternalSortAccumulatorFailed) {
					t.Fatal("failed mixed axis reused")
				}
				return e
			}
		}
		if e = a.finish(context.Background()); e != nil {
			return e
		}
		for _, v := range []*memory.ArrayNode{a.batch.Arrays["first"], a.batch.Structs["root"].Values.Arrays["second"]} {
			if v.Len() != 3 || v.Child.Len() != 4099 || v.Child.Child.Len() != 1 || v.Child.IsValid(4098) || !v.Child.IsValid(4097) || string(v.Child.Child.Strings.Data) != text {
				t.Fatal("three independent domains")
			}
			for i := 0; i < 4097; i++ {
				if !v.Child.IsValid(i) || v.Child.Offsets[i] != 0 {
					t.Fatal("late bitmap backfill", i)
				}
			}
		}
		return nil
	}
	probe := &listStructGatherManager{Manager: memory.NewBudgetManager(0)}
	if e := run(probe); e != nil {
		t.Fatal(e)
	}
	t.Logf("R1=4097 empty then R2 String then NULL inner: requests=%d peak=%d", probe.calls, probe.peak)
	for at := 1; at <= probe.calls; at++ {
		m := &listStructGatherManager{Manager: memory.NewBudgetManager(0), failAt: at}
		e := run(m)
		if !errors.Is(e, memory.ErrBudgetExceeded) || m.Used() != 0 {
			t.Fatal("mixed-axis rollback", at, e, m.Used())
		}
	}
}

type nestedAccumulatorCheckpointContext struct {
	context.Context
	a            *externalSortAccumulator
	target       string
	stop, checks int
	charge       int64
	canceled     bool
}

func (c *nestedAccumulatorCheckpointContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	var pcs [8]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	f, _ := frames.Next()
	if strings.Contains(f.Function, c.target) {
		c.checks++
		if c.checks == c.stop {
			c.canceled = true
			c.charge = c.a.reservation.Bytes()
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestNestedListAccumulatorFinalPublicationCancel(t *testing.T) {
	s, b, _ := nestedListAccumulatorFixture(t)
	a, w, m := nestedListAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	ctx := &nestedAccumulatorCheckpointContext{Context: context.Background(), a: a, target: "externalSortAccumulator).appendRow", stop: 1}
	e := a.appendRow(ctx, b, 1)
	first, second := nestedListAccumulatorData(a)
	if !errors.Is(e, context.Canceled) || ctx.checks != 1 || ctx.charge <= 0 || len(first.Data) < 100000 || len(second.Data) < 100000 {
		t.Fatal("final publication checkpoint", e, ctx.checks, ctx.charge)
	}
	nestedListAccumulatorTerminal(t, a, w, m, b)
}
func TestNestedListAccumulatorLateBitmapBackfillCancel(t *testing.T) {
	const count = 262145
	s, b, _ := nestedListAccumulatorFixture(t)
	for _, v := range []*memory.ArrayNode{b.Arrays["first"], b.Structs["root"].Values.Arrays["second"]} {
		v.Offsets = []uint32{0, count, count + 1, count + 1, count + 1, count + 1}
		v.Validity = nil
		v.Child.Offsets = make([]uint32, count+2)
		mask := make(memory.Bitmap, memory.WordsFor(count+1))
		for i := range mask {
			mask[i] = ^uint64(0)
		}
		mask[count/64] &= ^(uint64(1) << uint(count%64))
		v.Child.Validity = mask
		v.Child.Child.Strings = &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
	}
	b.Structs["root"].Validity = nil
	a, w, m := nestedListAccumulatorNew(t, s)
	if e := a.appendRow(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	if a.batch.Arrays["first"].Child.Validity != nil {
		t.Fatal("first bitmap not deferred")
	}
	ctx := &nestedAccumulatorCheckpointContext{Context: context.Background(), a: a, target: "externalSortAccumulator).appendValidity", stop: 2}
	e := a.appendRow(ctx, b, 1)
	mask := a.batch.Arrays["first"].Child.Validity
	if !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charge <= 0 || len(mask) != 4097 || mask[0] != ^uint64(0) || mask[4095] != ^uint64(0) || mask[4096] != 0 {
		t.Fatal("actual second backfill checkpoint", e, ctx.checks, len(mask))
	}
	nestedListAccumulatorTerminal(t, a, w, m, b)
}
