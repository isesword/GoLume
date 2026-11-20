package physical

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"testing"
)

type n38GuardSource struct {
	schema memory.Schema
	opened int
}

func (s *n38GuardSource) Schema() memory.Schema                                  { return s.schema }
func (s *n38GuardSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n38GuardSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n38GuardSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return nil, errors.New("must not open")
}
func TestN38ReviewConsumerDepthGuards(t *testing.T) {
	f := memory.ListInt64Field("item", true, true)
	for i := 0; i < 2; i++ {
		f = memory.Field{Name: "item", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{f}}
	}
	f.Name = "v"
	scalar := memory.Field{Name: "k", Type: memory.TypeInt64, Encoding: memory.EncodingPlain}
	s := &n38GuardSource{schema: memory.NewSchema(scalar, f)}
	plain := &n38GuardSource{schema: memory.NewSchema(scalar)}
	m := memory.NewBudgetManager(1 << 20)
	keys := []SortKey{{Column: "k"}}
	if _, e := NewSortSource(s, keys, m); e != nil {
		t.Fatal("N40 resident root Sort rejected", e)
	}
	if _, e := NewSortSourceWithOptions(s, keys, m, SortSourceOptions{Spillable: true}); e != nil {
		t.Fatal("N42 root spill Sort rejected", e)
	}
	if _, e := NewTopKSourceWithManager(s, 1, keys, m); e != nil {
		t.Fatal("N41 top-level TopK rejected", e)
	}
	if _, e := NewTopKSourceWithKeyInput(plain, s, 1, keys, m); e == nil {
		t.Fatal("TopK keyInput admitted depth3")
	}
	if _, e := expr.Bind(s.schema, expr.Col("v")); e != nil {
		t.Fatalf("direct transport column rejected: %v", e)
	}
	if _, e := expr.Bind(s.schema, expr.IsNull(expr.Col("v"))); e == nil {
		t.Fatal("depth3 calculation unexpectedly admitted")
	}
	bound, e := expr.Bind(s.schema, expr.ListGet(expr.Col("v"), expr.I64(0)))
	if e != nil {
		t.Fatal("N39 ListGet rejected", e)
	}
	out := bound.OutputField("out")
	if !out.Nullable || out.Encoding != memory.EncodingView || out.ValidateNestedList() != nil || memory.ValidateRepeatedListDepth([]memory.Field{out}, 2) != nil || out.DecimalPrecision != 0 || out.DecimalScale != 0 {
		t.Fatal("N39 wrong typed depth2 output", out)
	}
	b := memory.NewBatch()
	b.Rows = 0
	leaf := &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Encoding: memory.EncodingPlain}, Encoding: memory.EncodingView}
	for i := 0; i < 3; i++ {
		leaf = &memory.ArrayNode{Kind: memory.TypeList, Offsets: []uint32{0}, Child: leaf, Encoding: memory.EncodingView}
	}
	b.Arrays["v"] = leaf
	b.Int64s["k"] = &memory.Int64Vector{Encoding: memory.EncodingPlain}
	if o, e := memory.EncodeColumnarRun(context.Background(), s.schema, b, m); e != nil {
		t.Fatal("N42 canonical empty encode", e)
	} else {
		o.Close()
	}
	restored := &memory.RestoredSpill{Object: memory.SpillObject{Codec: memory.ColumnarRunCodec, Payload: []byte("not a wire run")}}
	if o, e := memory.DecodeColumnarRun(context.Background(), s.schema, restored, m); e == nil || !errors.Is(e, memory.ErrSpillCorrupt) {
		if o != nil {
			_ = o.Close()
		}
		t.Fatalf("decode must reject malformed wire: %v", e)
	}
	if s.opened != 0 || plain.opened != 0 || m.Used() != 0 {
		t.Fatalf("admission side effect opens=%d/%d used=%d", s.opened, plain.opened, m.Used())
	}
}
