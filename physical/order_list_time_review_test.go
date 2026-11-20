package physical

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

type n37ReviewSource struct {
	schema        memory.Schema
	pages         []*memory.Batch
	reads, closes int
	poison        bool
	hook          func()
}

func (s *n37ReviewSource) Schema() memory.Schema                                  { return s.schema }
func (s *n37ReviewSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n37ReviewSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n37ReviewSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n37ReviewStream{s: s}, nil
}

type n37ReviewStream struct {
	s      *n37ReviewSource
	i      int
	closed bool
}

func (s *n37ReviewStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.reads++
	b.ClearColumns()
	if s.s.poison && s.i > 0 {
		p := s.s.pages[s.i-1]
		for i := range p.Int64Lists["t"].Values.Values {
			p.Int64Lists["t"].Values.Values[i] = -777
		}
		for i := range p.Int64s["k"].Values {
			p.Int64s["k"].Values[i] = -999
		}
	}
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.i == len(s.s.pages) {
		return false, nil
	}
	p := s.s.pages[s.i]
	s.i++
	b.Rows = p.Rows
	b.Int64s["k"] = p.Int64s["k"]
	b.Int64Lists["t"] = p.Int64Lists["t"]
	if s.s.hook != nil {
		s.s.hook()
	}
	return true, nil
}
func (s *n37ReviewStream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closes++
	}
	return nil
}
func n37ReviewFixture() *n37ReviewSource {
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["k"] = &memory.Int64Vector{Values: []int64{2, 1}, Encoding: memory.EncodingPlain}
	b.Int64Lists["t"] = &memory.Int64ListVector{Offsets: []uint32{0, 1, 2}, Values: &memory.Int64Vector{Values: []int64{20, 10}, Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
	return &n37ReviewSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Encoding: memory.EncodingPlain}, memory.ListTime64Field("t", true, true)), pages: []*memory.Batch{b}}
}
func n37ReviewRead(t *testing.T, ctx context.Context, s *n37ReviewSource) (*memory.Batch, error) {
	t.Helper()
	m := memory.NewBudgetManager(1 << 20)
	op, e := NewSortSource(s, []SortKey{{Column: "k"}}, m)
	if e != nil {
		return nil, e
	}
	st, e := op.NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m})
	if e != nil {
		return nil, e
	}
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if e != nil {
		if ok || b.Rows != 0 {
			t.Fatalf("error published: %v %d", ok, b.Rows)
		}
		_ = st.Close()
		if m.Used() != 0 {
			t.Fatalf("leak %d", m.Used())
		}
		return nil, e
	}
	if !ok {
		t.Fatal("missing output")
	}
	// Inspect while borrowed, then explicitly end the borrow.
	out := memory.NewBatch()
	out.Rows = b.Rows
	out.Int64s["k"] = &memory.Int64Vector{Values: append([]int64(nil), b.Int64s["k"].Values...)}
	v := b.Int64Lists["t"]
	out.Int64Lists["t"] = &memory.Int64ListVector{Offsets: append([]uint32(nil), v.Offsets...), Validity: append(memory.Bitmap(nil), v.Validity...), Values: &memory.Int64Vector{Values: append([]int64(nil), v.Values.Values...), Validity: append(memory.Bitmap(nil), v.Values.Validity...)}}
	if e := st.Close(); e != nil {
		t.Fatal(e)
	}
	if m.Used() != 0 || s.closes != 1 {
		t.Fatalf("cleanup used=%d closes=%d", m.Used(), s.closes)
	}
	return out, nil
}
func TestN37ReviewTimeIngress(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*n37ReviewSource)
	}{
		{"negative", func(s *n37ReviewSource) { s.pages[0].Int64Lists["t"].Values.Values[0] = -1 }},
		{"one-day", func(s *n37ReviewSource) { s.pages[0].Int64Lists["t"].Values.Values[0] = 86400_000_000_000 }},
		{"child-encoding", func(s *n37ReviewSource) { s.pages[0].Int64Lists["t"].Values.Encoding = memory.EncodingDictionary }},
		{"root-encoding", func(s *n37ReviewSource) { s.pages[0].Int64Lists["t"].Encoding = memory.EncodingDictionary }},
		{"required-parent-null", func(s *n37ReviewSource) {
			s.schema.Fields[1].Nullable = false
			s.pages[0].Int64Lists["t"].Validity = memory.Bitmap{2}
		}},
		{"required-child-null", func(s *n37ReviewSource) {
			s.schema.Fields[1].Children[0].Nullable = false
			s.pages[0].Int64Lists["t"].Values.Validity = memory.Bitmap{2}
		}},
		{"decreasing-offset", func(s *n37ReviewSource) { s.pages[0].Int64Lists["t"].Offsets = []uint32{0, 2, 1} }},
		{"empty-malformed", func(s *n37ReviewSource) {
			s.pages[0].Rows = 0
			s.pages[0].Int64s["k"].Values = nil
			s.pages[0].Int64Lists["t"].Offsets = []uint32{1}
			s.pages[0].Int64Lists["t"].Values.Values = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := n37ReviewFixture()
			tc.edit(s)
			if _, e := n37ReviewRead(t, context.Background(), s); e == nil {
				t.Fatal("invalid ingress accepted")
			}
		})
	}
}
func TestN37ReviewHiddenAndOwned(t *testing.T) {
	s := n37ReviewFixture()
	s.poison = true
	b, e := n37ReviewRead(t, context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if b.Int64s["k"].Values[0] != 1 || b.Int64Lists["t"].Values.Values[0] != 10 || b.Int64Lists["t"].Values.Values[1] != 20 {
		t.Fatal("source poison changed owned result")
	}
	for _, parent := range []bool{false, true} {
		s = n37ReviewFixture()
		v := s.pages[0].Int64Lists["t"]
		v.Values.Values[0] = -1
		if parent {
			v.Validity = memory.Bitmap{2}
		} else {
			v.Values.Validity = memory.Bitmap{2}
		}
		if _, e := n37ReviewRead(t, context.Background(), s); e != nil {
			t.Fatalf("hidden payload: %v", e)
		}
	}
}
func TestN37ReviewAdmission(t *testing.T) {
	s := n37ReviewFixture()
	keys := []SortKey{{Column: "k"}}
	m := memory.NewBudgetManager(1 << 20)
	if _, e := NewSortSourceWithOptions(s, keys, m, SortSourceOptions{Spillable: true}); e == nil {
		t.Fatal("spill accepted")
	}
	if _, e := NewTopKSourceWithManager(s, 1, keys, m); e == nil {
		t.Fatal("TopK accepted")
	}
	for _, edit := range []func(*memory.Field){func(f *memory.Field) { f.Children[0].DictionaryDomain = "bad" }, func(f *memory.Field) { f.Children[0].Name = "not-item" }, func(f *memory.Field) { f.Encoding = memory.EncodingDictionary }} {
		x := n37ReviewFixture()
		edit(&x.schema.Fields[1])
		if _, e := NewSortSource(x, keys, m); e == nil {
			t.Fatal("malformed schema accepted")
		}
	}
	x := n37ReviewFixture()
	inner := memory.ListInt64Field("item", true, true)
	nested, e := memory.NestedListField("nested", true, inner)
	if e != nil {
		t.Fatal(e)
	}
	x.schema = memory.NewSchema(append(x.schema.Fields, nested)...)
	if _, e := NewSortSource(x, keys, m); e == nil {
		t.Fatal("unsupported mixed nested admitted")
	}
	if s.reads != 0 || m.Used() != 0 {
		t.Fatal("admission opened input or retained memory")
	}
}
func TestN37ReviewReadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := n37ReviewFixture()
	s.hook = cancel
	if _, e := n37ReviewRead(t, ctx, s); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel: %v", e)
	}
	if s.reads != 1 || s.closes != 1 {
		t.Fatalf("reads=%d closes=%d", s.reads, s.closes)
	}
}
