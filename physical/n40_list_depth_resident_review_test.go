package physical

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

type n40ReviewSource struct {
	schema        memory.Schema
	batch         *memory.Batch
	reads, closes int
	poison        bool
	cancel        context.CancelFunc
}

func (s *n40ReviewSource) Schema() memory.Schema                                  { return s.schema }
func (s *n40ReviewSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n40ReviewSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n40ReviewSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n40ReviewStream{s: s}, nil
}

type n40ReviewStream struct {
	s            *n40ReviewSource
	done, closed bool
}

func (s *n40ReviewStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.reads++
	b.ClearColumns()
	if s.done {
		if s.s.poison {
			n := s.s.batch.Arrays["v"]
			for n.Kind == memory.TypeList {
				n = n.Child
			}
			if n.Int64 != nil {
				for i := range n.Int64.Values {
					n.Int64.Values[i] = -99
				}
			} else {
				for i := range n.Strings.Data {
					n.Strings.Data[i] = 'z'
				}
			}
		}
		return false, nil
	}
	s.done = true
	b.Rows = s.s.batch.Rows
	b.Int64s["k"] = s.s.batch.Int64s["k"]
	b.Arrays["v"] = s.s.batch.Arrays["v"]
	for name, v := range s.s.batch.Opaque {
		b.Opaque[name] = v
	}
	return true, ctx.Err()
}
func (s *n40ReviewStream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closes++
		if s.s.cancel != nil {
			s.s.cancel()
		}
	}
	return nil
}
func n40ReviewFixture(text bool) *n40ReviewSource {
	f := memory.Field{Name: "item", Type: memory.TypeInt64, Nullable: true, Encoding: memory.EncodingPlain}
	n := &memory.ArrayNode{Kind: memory.TypeInt64, Encoding: memory.EncodingView, Int64: &memory.Int64Vector{Values: []int64{20, 10, 11}, Encoding: memory.EncodingPlain}}
	if text {
		f.Type = memory.TypeString
		f.Encoding = memory.EncodingView
		n = &memory.ArrayNode{Kind: memory.TypeString, Encoding: memory.EncodingView, Strings: &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("cab"), Encoding: memory.EncodingView}}
	}
	for i := 0; i < 3; i++ {
		f = memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{f}}
		n = &memory.ArrayNode{Kind: memory.TypeList, Encoding: memory.EncodingView, Offsets: []uint32{0, 1, 2, 3}, Child: n}
	}
	f.Name = "v"
	b := memory.NewBatch()
	b.Rows = 3
	b.Arrays["v"] = n
	b.Int64s["k"] = &memory.Int64Vector{Values: []int64{2, 1, 1}, Encoding: memory.EncodingPlain}
	return &n40ReviewSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64}, f), batch: b}
}
func n40ReviewOpen(t *testing.T, s *n40ReviewSource, ctx context.Context) (source.BatchStream, *memory.BudgetManager) {
	t.Helper()
	m := memory.NewBudgetManager(2 << 20)
	op, e := NewSortSource(s, []SortKey{{Column: "k"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := op.NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m})
	if e != nil {
		t.Fatal(e)
	}
	return st, m
}
func TestN40ReviewStableOwnedBorrow(t *testing.T) {
	for _, text := range []bool{false, true} {
		s := n40ReviewFixture(text)
		s.poison = true
		st, m := n40ReviewOpen(t, s, context.Background())
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil || !ok {
			t.Fatal(e, ok)
		}
		n := b.Arrays["v"]
		if n == nil {
			t.Fatal("missing owned payload")
		}
		for i := 0; i < 3; i++ {
			if n.Kind != memory.TypeList || len(n.Offsets) != 4 {
				t.Fatal("axis shape")
			}
			n = n.Child
		}
		if text {
			if string(n.Strings.Data) != "abc" {
				t.Fatal("unstable/aliased strings", string(n.Strings.Data))
			}
		} else {
			for i, w := range []int64{10, 11, 20} {
				if n.Int64.Values[i] != w {
					t.Fatal("unstable/aliased int", i)
				}
			}
		}
		if m.Used() == 0 || s.closes != 1 {
			t.Fatal("owner or child close")
		}
		if e = st.Close(); e != nil {
			t.Fatal(e)
		}
		st.Close()
		if m.Used() != 0 || b.Rows != 0 || len(b.Arrays) != 0 || s.closes != 1 {
			t.Fatal("borrow/owner not cleared", m.Used(), b.Rows, s.closes)
		}
	}
}
func TestN40ReviewStrictIngress(t *testing.T) {
	cases := []struct {
		name string
		edit func(*n40ReviewSource)
	}{
		{"nil-root", func(s *n40ReviewSource) { s.batch.Arrays["v"] = nil }},
		{"nil-child", func(s *n40ReviewSource) { s.batch.Arrays["v"].Child.Child = nil }},
		{"required-root", func(s *n40ReviewSource) {
			s.schema.Fields[1].Nullable = false
			s.batch.Arrays["v"].Validity = memory.Bitmap{6}
		}},
		{"required-leaf", func(s *n40ReviewSource) {
			s.schema.Fields[1].Children[0].Children[0].Children[0].Nullable = false
			s.batch.Arrays["v"].Child.Child.Child.Int64.Validity = memory.Bitmap{6}
		}},
		{"hidden-offset", func(s *n40ReviewSource) {
			s.batch.Arrays["v"].Validity = memory.Bitmap{6}
			s.batch.Arrays["v"].Child.Offsets[1] = 4
		}},
		{"hidden-bitmap", func(s *n40ReviewSource) {
			s.batch.Arrays["v"].Validity = memory.Bitmap{6}
			s.batch.Arrays["v"].Child.Validity = memory.Bitmap{}
		}},
		{"bad-runtime-encoding", func(s *n40ReviewSource) { s.batch.Arrays["v"].Child.Encoding = memory.EncodingDictionary }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := n40ReviewFixture(false)
			tc.edit(s)
			st, m := n40ReviewOpen(t, s, context.Background())
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e == nil || ok || b.Rows != 0 {
				t.Fatal("invalid input published", ok, e)
			}
			st.Close()
			if m.Used() != 0 || s.closes != 1 {
				t.Fatal("failure cleanup", m.Used(), s.closes)
			}
		})
	}
	s := n40ReviewFixture(false)
	s.schema.Fields[1].Children[0].Children[0].Children[0].Nullable = false
	s.batch.Arrays["v"].Validity = memory.Bitmap{6}
	s.batch.Arrays["v"].Child.Child.Child.Int64.Validity = memory.Bitmap{6}
	st, m := n40ReviewOpen(t, s, context.Background())
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok {
		t.Fatal("hidden required value rejected", e)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("hidden success leak")
	}
}
func TestN40ReviewCloseCancellation(t *testing.T) {
	s := n40ReviewFixture(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.cancel = cancel
	st, m := n40ReviewOpen(t, s, ctx)
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if !errors.Is(e, context.Canceled) || ok || b.Rows != 0 {
		t.Fatal("close cancellation published", ok, e)
	}
	st.Close()
	if m.Used() != 0 || s.closes != 1 {
		t.Fatal("cancel cleanup")
	}
}

func TestN40ReviewLegacyOpaqueListThree(t *testing.T) {
	var dt arrow.DataType = arrow.PrimitiveTypes.Int64
	f := memory.Field{Name: "item", Type: memory.TypeInt64, Nullable: true}
	for i := 0; i < 3; i++ {
		dt = arrow.ListOf(dt)
		f = memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{f}, ArrowType: dt.String()}
	}
	f.Name = "payload"
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	builder := array.NewBuilder(alloc, dt)
	for _, value := range []int64{30, 10, 20} {
		var current array.Builder = builder
		for i := 0; i < 3; i++ {
			l := current.(*array.ListBuilder)
			l.Append(true)
			current = l.ValueBuilder()
		}
		current.(*array.Int64Builder).Append(value)
	}
	arr := builder.NewArray()
	builder.Release()
	defer func() { arr.Release(); alloc.AssertSize(t, 0) }()
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
	b.Opaque["payload"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeList, ArrowType: dt.String(), Value: arr}
	input := &n9Source{n8Source: n8Source{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, f), batches: []*memory.Batch{b}}}
	m := memory.NewBudgetManager(8 << 20)
	op, e := NewSortSource(input, []SortKey{{Column: "key"}}, m)
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(context.Background(), op, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		st.Close()
		if m.Used() != 0 {
			t.Error("opaque leak", m.Used())
		}
	}()
	out := memory.NewBatch()
	for _, want := range []int64{10, 20, 30} {
		ok, e := st.Next(context.Background(), out)
		if e != nil || !ok {
			t.Fatal("legacy opaque rejected", ok, e)
		}
		v := out.Opaque["payload"]
		if v == nil {
			t.Fatal("opaque binding lost")
		}
		a := v.Value.(arrow.Array)
		idx := 0
		for i := 0; i < 3; i++ {
			l := a.(*array.List)
			lo, hi := l.ValueOffsets(idx)
			if hi-lo != 1 {
				t.Fatal("list size")
			}
			idx = int(lo)
			a = l.ListValues()
		}
		if a.(*array.Int64).Value(idx) != want {
			t.Fatal("opaque sort value")
		}
	}
}

func TestN40ReviewNativeOpaqueMixedStillRejects(t *testing.T) {
	for _, rows := range []int{0, 3} {
		s := n40ReviewFixture(false)
		s.batch.Rows = rows
		s.batch.Opaque["raw"] = nil
		st, m := n40ReviewOpen(t, s, context.Background())
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e == nil || !strings.Contains(e.Error(), "opaque storage") || ok || b.Rows != 0 {
			t.Fatal("native mixed opaque bypass", rows, ok, e)
		}
		st.Close()
		if m.Used() != 0 || s.closes != 1 {
			t.Fatal("mixed cleanup")
		}
	}
}
