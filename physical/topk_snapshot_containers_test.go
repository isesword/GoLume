package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type n9Source struct {
	n8Source
	stable bool
	opens  int
}

func (s *n9Source) Replayability() source.Replayability {
	if s.stable {
		return source.ReplayStable
	}
	return source.ReplayUnknown
}
func (s *n9Source) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return s.n8Source.NewBatchStream(ctx, o)
}
func n9Run(t *testing.T, m memory.Manager, stable bool, k int) error {
	s, b := n8Input(t, 8)
	input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, b}}, stable: stable}
	keys := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, b}}, stable: stable}
	top, e := NewTopKSourceWithKeyInput(input, keys, k, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		return e
	}
	stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if e != nil {
		return e
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := 0
	rows := []int{3, 3, 1, 2, 1, 2, 0, 0}
	if k < len(rows) {
		rows = rows[:k]
	}
	for {
		ok, e := stream.Next(context.Background(), out)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		if seen+out.Rows > len(rows) {
			t.Fatal("excess output")
		}
		nestedGatherCheck(t, out, 8, rows[seen:seen+out.Rows])
		seen += out.Rows
	}
	if seen != len(rows) {
		t.Fatal("missing rows", seen)
	}
	if k == 0 {
		if input.opens != 0 || keys.opens != 0 {
			t.Fatal("K0 opened input")
		}
	} else if stable {
		if input.opens != 1 || keys.opens != 1 {
			t.Fatal("replay passes", input.opens, keys.opens)
		}
	} else if input.opens != 1 || keys.opens != 0 {
		t.Fatal("nonreplay passes", input.opens, keys.opens)
	}
	return nil
}
func TestN9BothStrategies(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, k := range []int{0, 1, 3, 20} {
			t.Run(fmt.Sprint(stable, "/", k), func(t *testing.T) {
				m := memory.NewBudgetManager(8 << 20)
				if e := n9Run(t, m, stable, k); e != nil {
					t.Fatal(e)
				}
				if m.Used() != 0 {
					t.Fatal("leak", m.Used())
				}
			})
		}
	}
}
func TestN9EveryReserveExact(t *testing.T) {
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprint(stable), func(t *testing.T) {
			m := &n8Manager{Manager: memory.NewBudgetManager(0)}
			if e := n9Run(t, m, stable, 3); e != nil {
				t.Fatal(e)
			}
			t.Logf("trace %v peak %d", m.trace, m.peak)
			for fail := 1; fail <= m.calls; fail++ {
				a := &n8Manager{Manager: memory.NewBudgetManager(0), fail: fail}
				if e := n9Run(t, a, stable, 3); !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(fail, e)
				}
				if a.Used() != 0 {
					t.Fatal("leak", fail, a.Used())
				}
			}
			for _, limit := range []int64{m.peak, m.peak - 1} {
				a := &n8Manager{Manager: memory.NewBudgetManager(limit)}
				e := n9Run(t, a, stable, 3)
				if limit == m.peak && e != nil || limit < m.peak && !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(limit, e)
				}
				if a.Used() != 0 {
					t.Fatal("leak", limit, a.Used())
				}
			}
		})
	}
}

func TestN9AtomicReplacementKeepsOldOwner(t *testing.T) {
	s, b := n8Input(t, 8)
	m := &n8Manager{Manager: memory.NewBudgetManager(0)}
	stream := &topKContainerSnapshotStream{owner: &TopKSource{schema: s, input: &n9Source{n8Source: n8Source{schema: s}}, keys: []SortKey{{Column: "sortkey"}}, k: 1}, manager: m, input: b}
	old, e := stream.snapshot(context.Background(), 0)
	if e != nil {
		t.Fatal(e)
	}
	if old.owner.view != nil || old.owner.viewLease != nil || old.batch != old.owner.batch {
		t.Fatal("key snapshot retained redundant output view")
	}
	held := m.Used()
	oldBatch := old.owner.batch
	oldArray := oldBatch.Arrays["array1"]
	oldKey := old.key.cells[0]
	m.calls = 0
	candidate, e := stream.snapshot(context.Background(), 3)
	if e != nil {
		t.Fatal(e)
	}
	calls := m.calls
	if m.Used() <= held || old.owner.batch != oldBatch || !reflect.DeepEqual(old.key.cells[0], oldKey) {
		t.Fatal("old released before new commit")
	}
	candidate.release()
	if m.Used() != held {
		t.Fatal("new release lost old charge")
	}
	for ordinal := 1; ordinal <= calls; ordinal++ {
		m.calls = 0
		m.fail = ordinal
		_, e = stream.snapshot(context.Background(), 3)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(ordinal, e)
		}
		if m.Used() != held || old.owner.batch != oldBatch || oldArray.Child == nil {
			t.Fatal("old invalidated", ordinal, m.Used(), held)
		}
	}
	m.fail = 0
	old.release()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
}

func TestN9HiddenBadNonwinnerBatch(t *testing.T) {
	for _, stable := range []bool{false, true} {
		s, b := n8Input(t, 8)
		_, bad := n8Input(t, 8)
		bad.Arrays["array1"].Child.Child.Strings.Offsets[1] = 999999
		input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, bad}}, stable: stable}
		keys := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, b}}, stable: stable}
		m := memory.NewBudgetManager(8 << 20)
		top, e := NewTopKSourceWithKeyInput(input, keys, 1, []SortKey{{Column: "sortkey"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := stream.Next(context.Background(), memory.NewBatch()); ok || e == nil {
			t.Fatal(ok, e)
		}
		stream.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}

type n9CopyContext struct {
	context.Context
	checks int
	sticky bool
}

func (c *n9CopyContext) Err() error {
	if c.sticky {
		return context.Canceled
	}
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	fs := runtime.CallersFrames(pcs[:n])
	copying, owned := false, false
	for {
		f, more := fs.Next()
		copying = copying || strings.Contains(f.Function, "externalSortAccumulator).appendBytes")
		owned = owned || strings.Contains(f.Function, "topKContainerSnapshotStream).snapshot")
		if !more {
			break
		}
	}
	if copying && owned {
		c.checks++
		if c.checks == 3 {
			c.sticky = true
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestN9CopyCancellationOldOwnerAndBorrow(t *testing.T) {
	s, b, _ := nestedListAccumulatorFixture(t)
	s.Fields = append(s.Fields, memory.Field{Name: "key", Type: memory.TypeInt64})
	s = memory.NewSchema(s.Fields...)
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{4, 3, 2, 1, 0}}
	m := memory.NewBudgetManager(8 << 20)
	stream := &topKContainerSnapshotStream{owner: &TopKSource{schema: s, input: &n9Source{n8Source: n8Source{schema: s}}, keys: []SortKey{{Column: "key"}}, k: 1}, manager: m, input: b}
	old, e := stream.snapshot(context.Background(), 0)
	if e != nil {
		t.Fatal(e)
	}
	held := m.Used()
	ctx := &n9CopyContext{Context: context.Background()}
	_, e = stream.snapshot(ctx, 1)
	if !errors.Is(e, context.Canceled) || ctx.checks != 3 || m.Used() != held {
		t.Fatal(e, ctx.checks, m.Used(), held)
	}
	out := memory.NewBatch()
	if e := old.owner.bindOutput(context.Background(), out, -1); e != nil {
		t.Fatal(e)
	}
	alias := out.Arrays["first"]
	out.ReleaseOwned()
	old.release()
	if alias.Child != nil || m.Used() != 0 {
		t.Fatal("borrow lifetime", m.Used())
	}
}

func n9MixedFixture(t *testing.T) (memory.Schema, *memory.Batch) {
	s, b := n8Input(t, 8)
	f := memory.Field{Name: "label", Type: memory.TypeString, Encoding: memory.EncodingView, Nullable: true}
	leaf := memory.NewBatch()
	leaf.Rows = 4
	leaf.DictStrings["label"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Encoding: memory.EncodingDictionary}
	inner := memory.StructField("inside", false, f)
	mid := memory.NewBatch()
	mid.Rows = 4
	mid.Structs["inside"] = &memory.StructVector{Rows: 4, Fields: inner.Children, Values: leaf}
	root := memory.StructField("dictroot", false, inner)
	b.Structs["dictroot"] = &memory.StructVector{Rows: 4, Fields: root.Children, Values: mid}
	s.Fields = append(s.Fields, root)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	builder := array.NewLargeStringBuilder(alloc)
	builder.AppendValues([]string{"x0", "x1", "x2", "x3"}, nil)
	arr := builder.NewArray()
	builder.Release()
	t.Cleanup(arr.Release)
	b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, Value: arr, ArrowType: arr.DataType().String()}
	s.Fields = append(s.Fields, memory.Field{Name: "opaque", Type: memory.TypeArrow, ArrowType: arr.DataType().String()})
	return memory.NewSchema(s.Fields...), b
}

func TestN9MixedDictionaryOpaqueBothStrategies(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, opaque := range []bool{false, true} {
			s, b := n9MixedFixture(t)
			if !opaque {
				delete(b.Opaque, "opaque")
				s = memory.NewSchema(s.Fields[:len(s.Fields)-1]...)
			}
			input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}, stable: stable}
			m := memory.NewBudgetManager(8 << 20)
			top, e := NewTopKSourceWithManager(input, 2, []SortKey{{Column: "sortkey"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			out := memory.NewBatch()
			seen := 0
			rows := []int{3, 1}
			for {
				ok, e := stream.Next(context.Background(), out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				nestedGatherCheck(t, out, 8, rows[seen:seen+out.Rows])
				d := out.Structs["dictroot"].Values.Structs["inside"].Values.DictStrings["label"]
				for i := 0; i < out.Rows; i++ {
					r := rows[seen+i]
					if string(d.DictionaryValue(d.Indices[i])) != string("ab"[r%2]) {
						t.Fatal("dict")
					}
					if opaque && out.Opaque["opaque"].Value.(*array.LargeString).Value(i) != fmt.Sprint("x", r) {
						t.Fatal("opaque")
					}
				}
				seen += out.Rows
			}
			stream.Close()
			if seen != 2 || m.Used() != 0 {
				t.Fatal(seen, m.Used())
			}
		}
	}
}

// Opaque payload transport is not an opaque ordering-key adapter.
func TestN9OpaqueScalarKeyRetainsLegacyBoundary(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, extended := range []bool{false, true} {
			s, b := n9MixedFixture(t)
			f := s.Fields[len(s.Fields)-1]
			f.Type = memory.TypeString
			b.Opaque["opaque"].Type = memory.TypeString
			if extended {
				s.Fields[len(s.Fields)-1] = f
				s = memory.NewSchema(s.Fields...)
			} else {
				s = memory.NewSchema(f)
				flat := memory.NewBatch()
				flat.Rows = b.Rows
				flat.Opaque["opaque"] = b.Opaque["opaque"]
				b = flat
			}
			input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}, stable: stable}
			m := memory.NewBudgetManager(8 << 20)
			top, e := NewTopKSourceWithManager(input, 3, []SortKey{{Column: "opaque"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			if ok, e := stream.Next(context.Background(), memory.NewBatch()); ok || e == nil || !strings.Contains(e.Error(), "string-view column") {
				t.Fatal(ok, e)
			}
			stream.Close()
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
		}
	}
}

func TestN9FixedKReplacementPeakDoesNotGrowWithHistory(t *testing.T) {
	var reference int64
	for _, batches := range []int{2, 32, 128} {
		var all []*memory.Batch
		var schema memory.Schema
		for i := 0; i < batches; i++ {
			s, b := n8Input(t, 8)
			schema = s
			b.Int64s["sortkey"].Values = []int64{int64(-4 * i), int64(-4*i - 1), int64(-4*i - 2), int64(-4*i - 3)}
			all = append(all, b)
		}
		input := &n9Source{n8Source: n8Source{schema: schema, batches: all}}
		m := &n8Manager{Manager: memory.NewBudgetManager(0)}
		top, e := NewTopKSourceWithManager(input, 1, []SortKey{{Column: "sortkey"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		out := memory.NewBatch()
		if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
			t.Fatal(ok, e)
		}
		ext := stream.(*topKContainerSnapshotStream)
		if len(ext.heap.items) != 1 || cap(ext.heap.items) != 1 {
			t.Fatal("K allocation")
		}
		page := ext.page
		if page == nil {
			t.Fatal("missing typed page")
		}
		t.Logf("inputRows=%d peak=%d retained=%d pageMetadata=%d pageCapacity=%d pageToken=%d view=%d", batches*4, m.peak, m.Used(), page.metadataBytes, page.capacityBytes, page.reservation.Bytes(), page.viewLease.Bytes())

		if reference == 0 {
			reference = m.peak
		} else if m.peak != reference {
			t.Fatal("historical peak growth", batches, m.peak, reference)
		}
		stream.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}

func n9PageInput(t *testing.T) *n9Source {
	s, b := n8Input(t, 8)
	all := make([]*memory.Batch, 49)
	for i := range all {
		all[i] = b
	}
	return &n9Source{n8Source: n8Source{schema: s, batches: all}}
}
func TestN9TypedPageBoundariesAndInvalidation(t *testing.T) {
	for _, k := range []int{65, 129, 195} {
		for _, capRows := range []int{0, 7} {
			input := n9PageInput(t)
			m := memory.NewBudgetManager(64 << 20)
			top, e := NewTopKSourceWithManager(input, k, []SortKey{{Column: "sortkey"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = capRows
			stream, e := source.OpenBatchStream(context.Background(), top, opts)
			if e != nil {
				t.Fatal(e)
			}
			want := make([]int, 0, 196)
			for i := 0; i < 49; i++ {
				want = append(want, 3)
			}
			for i := 0; i < 49; i++ {
				want = append(want, 1, 2)
			}
			for i := 0; i < 49; i++ {
				want = append(want, 0)
			}
			want = want[:k]
			out := memory.NewBatch()
			seen := 0
			pages := 0
			var alias *memory.ArrayNode
			for {
				ok, e := stream.Next(context.Background(), out)
				if e != nil {
					t.Fatal(e)
				}
				if alias != nil && alias.Child != nil {
					t.Fatal("previous page not revoked")
				}
				if !ok {
					break
				}
				if input.closed != 1 || input.opens != 1 {
					t.Fatal("source lifetime")
				}
				limit := 64
				if capRows > 0 {
					limit = capRows
				}
				if out.Rows > limit {
					t.Fatal("page bound", out.Rows)
				}
				nestedGatherCheck(t, out, 8, want[seen:seen+out.Rows])
				seen += out.Rows
				pages++
				alias = out.Arrays["array1"]
			}
			stream.Close()
			if seen != k || pages < 2 || m.Used() != 0 {
				t.Fatal(k, seen, pages, m.Used())
			}
		}
	}
}
func TestN9LaterPageFailureReleasesAll(t *testing.T) {
	input := n9PageInput(t)
	m := &n8Manager{Manager: memory.NewBudgetManager(64 << 20)}
	top, e := NewTopKSourceWithManager(input, 195, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	stream, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
		t.Fatal(ok, e)
	}
	alias := out.Arrays["array1"]
	m.fail = m.calls + 1
	if ok, e := stream.Next(context.Background(), out); ok || !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(ok, e)
	}
	stream.Close()
	stream.Close()
	if m.Used() != 0 || alias.Child != nil {
		t.Fatal("page failure lifetime", m.Used())
	}
}

type n9PageCancelContext struct {
	context.Context
	n      int
	sticky bool
}

func (c *n9PageCancelContext) Err() error {
	if c.sticky {
		return context.Canceled
	}
	var pcs [45]uintptr
	n := runtime.Callers(2, pcs[:])
	fs := runtime.CallersFrames(pcs[:n])
	copying, page := false, false
	for {
		f, more := fs.Next()
		copying = copying || strings.Contains(f.Function, "externalSortAccumulator).appendBytes")
		page = page || strings.Contains(f.Function, "topKContainerSnapshotStream).bindPage")
		if !more {
			break
		}
	}
	if copying && page {
		c.n++
		if c.n == 3 {
			c.sticky = true
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestN9PageLongCopyCancellation(t *testing.T) {
	s, b, _ := nestedListAccumulatorFixture(t)
	s.Fields = append(s.Fields, memory.Field{Name: "key", Type: memory.TypeInt64})
	s = memory.NewSchema(s.Fields...)
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{4, 0, 2, 1, 3}}
	input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
	m := memory.NewBudgetManager(8 << 20)
	top, e := NewTopKSourceWithManager(input, 5, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	ctx := &n9PageCancelContext{Context: context.Background()}
	stream, e := source.OpenBatchStream(ctx, top, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(e, context.Canceled) || ctx.n != 3 {
		t.Fatal(ok, e, ctx.n)
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
}
