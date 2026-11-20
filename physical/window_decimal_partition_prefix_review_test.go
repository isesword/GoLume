package physical

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

type n36ReviewSource struct {
	schema        memory.Schema
	pages         []*memory.Batch
	closes, reads int
	closeErr      error
	afterRead     func()
}

func (s *n36ReviewSource) Schema() memory.Schema                                  { return s.schema }
func (s *n36ReviewSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n36ReviewSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n36ReviewSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "p"}, {Name: "k"}}
}
func (s *n36ReviewSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Class: source.StreamBounded}
}
func (s *n36ReviewSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &n36ReviewStream{src: s}, nil
}

type n36ReviewStream struct {
	src    *n36ReviewSource
	next   int
	closed bool
}

func (s *n36ReviewStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.src.reads++
	b.ClearColumns()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.next == len(s.src.pages) {
		return false, nil
	}
	p := s.src.pages[s.next]
	s.next++
	b.Rows = p.Rows
	b.Int64s["p"] = p.Int64s["p"]
	b.Int64s["k"] = p.Int64s["k"]
	b.Decimals["v"] = p.Decimals["v"]
	if s.src.afterRead != nil {
		s.src.afterRead()
	}
	return true, nil
}
func (s *n36ReviewStream) Close() error {
	if !s.closed {
		s.closed = true
		s.src.closes++
	}
	return s.src.closeErr
}
func n36ReviewPage(parts, keys, values []int64, valid memory.Bitmap) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(keys)
	b.Int64s["p"] = &memory.Int64Vector{Values: parts, Validity: valid, Encoding: memory.EncodingPlain}
	b.Int64s["k"] = &memory.Int64Vector{Values: keys, Encoding: memory.EncodingPlain}
	v := make([]memory.Decimal128, len(values))
	for i, x := range values {
		v[i] = decimal128.FromI64(x)
	}
	b.Decimals["v"] = &memory.Decimal128Vector{Values: v, Precision: 3, Scale: 0}
	return b
}
func n36ReviewFixture(t *testing.T, pages ...*memory.Batch) *n36ReviewSource {
	t.Helper()
	v, err := memory.Decimal128Field("v", true, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	return &n36ReviewSource{schema: memory.NewSchema(memory.Field{Name: "p", Type: memory.TypeInt64, Encoding: memory.EncodingPlain, Nullable: true}, memory.Field{Name: "k", Type: memory.TypeInt64, Encoding: memory.EncodingPlain}, v), pages: pages}
}
func n36ReviewOpen(t *testing.T, ctx context.Context, s *n36ReviewSource, precision int32) (source.BatchStream, *memory.BudgetManager) {
	t.Helper()
	op, err := NewPartitionedDecimalPrefixAverageSource(s, PartitionedDecimalPrefixAverage{Column: "v", PartitionBy: "p", OrderBy: "k", Alias: "avg", Precision: precision, Scale: 0, Rounding: aggregate.DecimalAverageHalfEven, Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(1 << 20)
	st, err := op.NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, m
}
func n36ReviewEmpty(t *testing.T, b *memory.Batch) {
	t.Helper()
	if b.Rows != 0 || len(b.Int64s) != 0 || len(b.Decimals) != 0 {
		t.Fatalf("failed page published data: %+v", b)
	}
}

func TestN36ReviewFalseProofAcrossPages(t *testing.T) {
	for _, tc := range []struct {
		name  string
		part  int64
		valid memory.Bitmap
		key   int64
	}{
		{"partition-regression", 0, nil, 9}, {"NULL-reentry", 999, memory.Bitmap{0}, 9}, {"equal-partition-order", 1, nil, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := n36ReviewPage([]int64{1}, []int64{2}, []int64{4}, nil)
			bad := n36ReviewPage([]int64{tc.part}, []int64{tc.key}, []int64{6}, tc.valid)
			src := n36ReviewFixture(t, first, bad)
			st, m := n36ReviewOpen(t, context.Background(), src, 3)
			b := memory.NewBatch()
			if ok, err := st.Next(context.Background(), b); !ok || err != nil {
				t.Fatalf("first: %v %v", ok, err)
			}
			if ok, err := st.Next(context.Background(), b); ok || err == nil {
				t.Fatalf("false proof accepted: %v %v", ok, err)
			}
			n36ReviewEmpty(t, b)
			if src.closes != 1 || m.Used() != 0 {
				t.Fatalf("cleanup closes=%d used=%d", src.closes, m.Used())
			}
		})
	}
}
func TestN36ReviewLatePageAtomic(t *testing.T) {
	// First page succeeds; second page has a valid first row and overflow only
	// after a partition reset. No prefix of that failing page may escape.
	src := n36ReviewFixture(t, n36ReviewPage([]int64{0}, []int64{1}, []int64{1}, nil), n36ReviewPage([]int64{0, 1}, []int64{2, 1}, []int64{1, 10}, nil))
	st, m := n36ReviewOpen(t, context.Background(), src, 1)
	b := memory.NewBatch()
	if ok, err := st.Next(context.Background(), b); !ok || err != nil {
		t.Fatalf("first: %v %v", ok, err)
	}
	if ok, err := st.Next(context.Background(), b); ok || !errors.Is(err, aggregate.ErrDecimalAverageOverflow) {
		t.Fatalf("late overflow: %v %v", ok, err)
	}
	n36ReviewEmpty(t, b)
	if m.Used() != 0 || src.closes != 1 {
		t.Fatalf("cleanup used=%d closes=%d", m.Used(), src.closes)
	}
}
func TestN36ReviewReadCancelAndCloseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sentinel := errors.New("review child close failure")
	src := n36ReviewFixture(t, n36ReviewPage([]int64{0}, []int64{1}, []int64{1}, nil))
	src.afterRead = cancel
	src.closeErr = sentinel
	st, m := n36ReviewOpen(t, ctx, src, 3)
	b := memory.NewBatch()
	ok, err := st.Next(context.Background(), b)
	if ok || !errors.Is(err, context.Canceled) || !errors.Is(err, sentinel) {
		t.Fatalf("cancel/close provenance: %v %v", ok, err)
	}
	n36ReviewEmpty(t, b)
	_ = st.Close()
	if m.Used() != 0 || src.reads != 1 || src.closes != 1 {
		t.Fatalf("cleanup used=%d reads=%d closes=%d", m.Used(), src.reads, src.closes)
	}
}
func TestN36ReviewEOFAndEarlyClose(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "EOF", true: "early"}[early], func(t *testing.T) {
			sentinel := errors.New("review close")
			src := n36ReviewFixture(t, n36ReviewPage([]int64{17, 99}, []int64{1, 2}, []int64{2, 4}, memory.Bitmap{0}))
			src.closeErr = sentinel
			st, m := n36ReviewOpen(t, context.Background(), src, 3)
			b := memory.NewBatch()
			if ok, err := st.Next(context.Background(), b); !ok || err != nil {
				t.Fatalf("NULL group: %v %v", ok, err)
			}
			if got := b.Decimals["avg"].Values[1]; got != decimal128.FromI64(3) {
				t.Fatalf("NULL poison split partition: %v", got)
			}
			if early {
				if err := st.Close(); !errors.Is(err, sentinel) {
					t.Fatalf("early close: %v", err)
				}
			} else {
				if ok, err := st.Next(context.Background(), b); ok || !errors.Is(err, sentinel) {
					t.Fatalf("EOF: %v %v", ok, err)
				}
				n36ReviewEmpty(t, b)
			}
			_ = st.Close()
			if src.closes != 1 || m.Used() != 0 {
				t.Fatalf("cleanup closes=%d used=%d", src.closes, m.Used())
			}
		})
	}
}
