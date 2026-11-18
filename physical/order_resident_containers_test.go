package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/scheduler"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"runtime"
	"strings"
	"testing"
)

func n8Input(t *testing.T, depth int) (memory.Schema, *memory.Batch) {
	s, b := nestedGatherFixture(t, depth)
	s.Fields = append(s.Fields, memory.Field{Name: "sortkey", Type: memory.TypeInt64}, memory.Field{Name: "ordinal", Type: memory.TypeUint64})
	b.Uint64s["ordinal"] = &memory.Uint64Vector{Values: []uint64{100, 101, 102, 103}}
	b.Int64s["sortkey"] = &memory.Int64Vector{Values: []int64{2, 1, 1, 0}}
	return memory.NewSchema(s.Fields...), b
}
func TestN8ResidentContainers(t *testing.T) {
	for _, depth := range []int{0, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s, b := n8Input(t, depth)
			input, e := inmemory.New(s, b, b)
			if e != nil {
				t.Fatal(e)
			}
			m := memory.NewBudgetManager(8 << 20)
			sorted, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			out := memory.NewBatch()
			if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
				t.Fatal(ok, e)
			}
			nestedGatherCheck(t, out, depth, []int{3, 3, 1, 2, 1, 2, 0, 0})
			a := out.Arrays["array1"]
			item := out.StructLists["first"].Values
			fields := item.Fields
			if a == b.Arrays["array1"] || item == b.StructLists["first"].Values {
				t.Fatal("borrowed source")
			}
			stream.(*residentSortStream).output.clearOutput()
			if len(fields) == 0 || fields[0].Name == "" {
				t.Fatal("borrow invalidation cleared owner Fields")
			}
			if e := stream.Close(); e != nil {
				t.Fatal(e)
			}
			if a.Child != nil || item.Values != nil || len(out.Arrays) != 0 {
				t.Fatal("stale borrowed descriptors")
			}
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
			// N9 covers TopK ownership in its independent two-strategy tests.

		})
	}
}

type n8Manager struct {
	memory.Manager
	calls, fail int
	peak        int64
	trace       []int64
}

func (m *n8Manager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.trace = append(m.trace, n)
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, e
}
func n8Run(t *testing.T, m *n8Manager) error {
	s, b := n8Input(t, 8)
	input, e := inmemory.New(s, b, b)
	if e != nil {
		t.Fatal(e)
	}
	sorted, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		return e
	}
	stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
	if e != nil {
		return e
	}
	_, e = stream.Next(context.Background(), memory.NewBatch())
	ce := stream.Close()
	if ce != nil {
		t.Fatal(ce)
	}
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
	return e
}
func TestN8ResidentEveryReserveAndExact(t *testing.T) {
	m := &n8Manager{Manager: memory.NewBudgetManager(0)}
	if e := n8Run(t, m); e != nil {
		t.Fatal(e)
	}
	t.Logf("successful reserve trace=%v peak=%d", m.trace, m.peak)
	for fail := 1; fail <= m.calls; fail++ {
		a := &n8Manager{Manager: memory.NewBudgetManager(0), fail: fail}
		if e := n8Run(t, a); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatalf("ordinal %d: %v", fail, e)
		}
	}
	for _, limit := range []int64{m.peak, m.peak - 1} {
		a := &n8Manager{Manager: memory.NewBudgetManager(limit)}
		e := n8Run(t, a)
		if limit == m.peak && e != nil || limit < m.peak && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(limit, e)
		}
	}
}

type n8Source struct {
	schema  memory.Schema
	batches []*memory.Batch
	closed  int
}

func (s *n8Source) Schema() memory.Schema                       { return s.schema }
func (s *n8Source) Stats(context.Context) (source.Stats, error) { return source.Stats{}, nil }
func (s *n8Source) Partitions(context.Context) ([]source.Partition, error) {
	return nil, fmt.Errorf("stream only")
}
func (s *n8Source) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n8Stream{s: s}, nil
}

type n8Stream struct {
	s      *n8Source
	i      int
	closed bool
}

func (s *n8Stream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	b.ClearColumns()
	if s.i == len(s.s.batches) {
		return false, nil
	}
	v := s.s.batches[s.i]
	s.i++
	bindAll(b, v)
	b.Rows = v.Rows
	return true, ctx.Err()
}
func (s *n8Stream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
	}
	return nil
}

func TestN8ResidentSecondBatchHiddenCorruption(t *testing.T) {
	s, b := n8Input(t, 8)
	_, bad := n8Input(t, 8)
	// row0 outer array NULL, but malformed storage is never hidden by its mask.
	bad.Arrays["array1"].Child.Child.Strings.Offsets[1] = 999999
	input := &n8Source{schema: memory.NewSchema(s.Fields...), batches: []*memory.Batch{b, bad}}
	m := memory.NewBudgetManager(8 << 20)
	sorted, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	if ok, e := stream.Next(context.Background(), out); ok || e == nil {
		t.Fatal(ok, e)
	}
	if m.Used() != 0 || input.closed != 1 || out.Rows != 0 {
		t.Fatal("failed batch retained state", m.Used(), input.closed, out.Rows)
	}
	stream.Close()
	stream.Close()
}

type n8CopyContext struct {
	context.Context
	checks int
	sticky bool
}

func (c *n8CopyContext) Err() error {
	if c.sticky {
		return context.Canceled
	}
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	copying, second := false, false
	for {
		f, more := frames.Next()
		copying = copying || strings.Contains(f.Function, "externalSortAccumulator).appendBytes")
		second = second || strings.HasSuffix(f.Function, "newResidentExtendedSorted")
		if !more {
			break
		}
	}
	if copying && second {
		c.checks++
		if c.checks == 3 {
			c.sticky = true
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestN8ResidentSecondOwnerLongCopyCancel(t *testing.T) {
	s, b, _ := nestedListAccumulatorFixture(t)
	s.Fields = append(s.Fields, memory.Field{Name: "key", Type: memory.TypeInt64})
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{1, 0, 2, 3, 4}}
	input := &n8Source{schema: memory.NewSchema(s.Fields...), batches: []*memory.Batch{b}}
	m := memory.NewBudgetManager(16 << 20)
	sorted, e := NewSortSource(input, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	ctx := &n8CopyContext{Context: context.Background()}
	stream, e := source.OpenBatchStream(ctx, sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(e, context.Canceled) || ctx.checks != 3 {
		t.Fatal(ok, e, ctx.checks)
	}
	if m.Used() != 0 || input.closed != 1 {
		t.Fatal("cancel cleanup", m.Used(), input.closed)
	}
	if _, e := stream.Next(context.Background(), memory.NewBatch()); !errors.Is(e, context.Canceled) {
		t.Fatal("nonsticky", e)
	}
	stream.Close()
}

func TestN8ResidentMixedDictionaryOpaque(t *testing.T) {
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
	defer arr.Release()
	b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, Value: arr, ArrowType: arr.DataType().String()}
	s.Fields = append(s.Fields, memory.Field{Name: "opaque", Type: memory.TypeArrow, ArrowType: arr.DataType().String()})
	input := &n8Source{schema: memory.NewSchema(s.Fields...), batches: []*memory.Batch{b}}
	m := memory.NewBudgetManager(8 << 20)
	sorted, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	out := memory.NewBatch()
	for _, r := range []int{3, 1, 2, 0} {
		if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
			t.Fatal(ok, e)
		}
		nestedGatherCheck(t, out, 8, []int{r})
		d := out.Structs["dictroot"].Values.Structs["inside"].Values.DictStrings["label"]
		if d == nil || string(d.DictionaryValue(d.Indices[0])) != string("ab"[r%2]) {
			t.Fatal("dictionary lost")
		}
		if got := out.Opaque["opaque"].Value.(*array.LargeString).Value(0); got != fmt.Sprint("x", r) {
			t.Fatal(got)
		}
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
	delete(b.Opaque, "opaque")
	s.Fields = s.Fields[:len(s.Fields)-1]
	input = &n8Source{schema: memory.NewSchema(s.Fields...), batches: []*memory.Batch{b}}
	sorted, e = NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	stream, e = source.OpenBatchStream(context.Background(), sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
		t.Fatal(ok, e)
	}
	nestedGatherCheck(t, out, 8, []int{3, 1, 2, 0})
	d := out.Structs["dictroot"].Values.Structs["inside"].Values.DictStrings["label"]
	for i, r := range []int{3, 1, 2, 0} {
		if d == nil || string(d.DictionaryValue(d.Indices[i])) != string("ab"[r%2]) {
			t.Fatal("second-owner dictionary lost")
		}
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("second-owner leak", m.Used())
	}

}

func TestN8ResidentParallelNullStable(t *testing.T) {
	pool := scheduler.NewPool(3)
	defer pool.Close()
	for _, desc := range []bool{false, true} {
		for _, last := range []bool{false, true} {
			s, b := n8Input(t, 8)
			b.Int64s["sortkey"].Validity = memory.Bitmap{14}
			input := &n8Source{schema: s, batches: []*memory.Batch{b, b}}
			m := memory.NewBudgetManager(8 << 20)
			sorted, e := NewSortSourceWithOptions(input, []SortKey{{Column: "sortkey", Descending: desc, NullsLast: last}}, m, SortSourceOptions{Workers: 3, Executor: pool})
			if e != nil {
				t.Fatal(e)
			}
			plan := sorted.(*SortSource).StreamPlan()
			if plan.Class != source.StreamBlocking || !strings.Contains(plan.Detail, "resident-stable") {
				t.Fatal(plan)
			}
			stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			out := memory.NewBatch()
			if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
				t.Fatal(ok, e)
			}
			rows := []int{3, 3, 1, 2, 1, 2}
			if desc {
				rows = []int{1, 2, 1, 2, 3, 3}
			}
			if last {
				rows = append(rows, 0, 0)
			} else {
				rows = append([]int{0, 0}, rows...)
			}
			nestedGatherCheck(t, out, 8, rows)
			stream.Close()
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
		}
	}
}
