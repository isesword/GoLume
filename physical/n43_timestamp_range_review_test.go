package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"testing"
)

type n43ReviewSource struct {
	requireQuery  bool
	query         *memory.QueryBudget
	schema        memory.Schema
	pages         []*memory.Batch
	ordering      []source.OrderingColumn
	reads, closes int
	cancel        context.CancelFunc
}

func (s *n43ReviewSource) Schema() memory.Schema                                  { return s.schema }
func (s *n43ReviewSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n43ReviewSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n43ReviewSource) Ordering() []source.OrderingColumn                      { return s.ordering }
func (s *n43ReviewSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}
}
func (s *n43ReviewSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.query = memory.QueryBudgetFromContext(ctx)
	return &n43ReviewStream{s: s}, nil
}

type n43ReviewStream struct {
	s      *n43ReviewSource
	i      int
	closed bool
}

func (s *n43ReviewStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	b.ClearColumns()
	s.s.reads++
	if s.s.requireQuery && memory.QueryBudgetFromContext(ctx) != s.s.query {
		return false, errors.New("review: source lost shared query")
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
	bindAll(b, p)
	return true, nil
}
func (s *n43ReviewStream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closes++
		if s.s.cancel != nil {
			s.s.cancel()
		}
	}
	return nil
}
func n43ReviewPage(ts, vs []int64) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(ts)
	b.Int64s["t"] = &memory.Int64Vector{Values: ts}
	b.Int64s["v"] = &memory.Int64Vector{Values: vs}
	return b
}
func n43ReviewFixture(pages ...*memory.Batch) *n43ReviewSource {
	return &n43ReviewSource{schema: memory.NewSchema(memory.Field{Name: "t", Type: memory.TypeTimestamp}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), ordering: []source.OrderingColumn{{Name: "t"}}, pages: pages}
}
func n43ReviewSpec() TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "t", Column: "v", Frame: TimestampRangeFrame{Kind: WindowFrameRange}, CountRowsAlias: "rows", CountAlias: "count", SumAlias: "sum", AvgAlias: "avg"}
}
func TestN43ReviewAdmission(t *testing.T) {
	for _, kind := range []string{"zero-kind", "negative", "nullable-key", "encoded-key", "foreign-timestamp", "extra", "descending", "missing-proof", "duplicate", "no-output"} {
		t.Run(kind, func(t *testing.T) {
			s := n43ReviewFixture()
			sp := n43ReviewSpec()
			switch kind {
			case "zero-kind":
				sp.Frame.Kind = WindowFrameRows
			case "negative":
				sp.Frame.PrecedingNS = -1
			case "nullable-key":
				s.schema.Fields[0].Nullable = true
			case "encoded-key":
				s.schema.Fields[0].Encoding = memory.EncodingDictionary
			case "foreign-timestamp":
				s.schema.Fields[0].ArrowType = "timestamp[us]"
			case "extra":
				s.schema = memory.NewSchema(append(s.schema.Fields, memory.Field{Name: "extra", Type: memory.TypeInt64})...)
			case "descending":
				s.ordering[0].Descending = true
			case "missing-proof":
				s.ordering = nil
			case "duplicate":
				sp.SumAlias = sp.CountAlias
			case "no-output":
				sp.CountRowsAlias = ""
				sp.CountAlias = ""
				sp.SumAlias = ""
				sp.AvgAlias = ""
			}
			if _, e := NewTimestampRangeSource(s, sp); e == nil {
				t.Fatal("invalid admitted")
			}
			if s.reads != 0 {
				t.Fatal("constructor read")
			}
		})
	}
	op, e := NewTimestampRangeSource(n43ReviewFixture(), n43ReviewSpec())
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"rows", "count", "sum", "avg"} {
		f, ok := op.Schema().Field(n)
		want := memory.TypeInt64
		if n == "avg" {
			want = memory.TypeFloat64
		}
		if !ok || f.Type != want || f.Nullable != (n == "sum" || n == "avg") {
			t.Fatal(n, f)
		}
	}
}
func n43ReviewOpen(t *testing.T, s *n43ReviewSource, sp TimestampRangeSpec, ctx context.Context) (source.BatchStream, *memory.QueryBudget) {
	t.Helper()
	op, e := NewTimestampRangeSource(s, sp)
	if e != nil {
		t.Fatal(e)
	}
	q, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { q.Close() })
	ctx = memory.WithQueryBudget(ctx, q)
	opts := testReadOptions()
	opts.MemoryManager = q
	st, e := source.OpenBatchStream(ctx, op, opts)
	if e != nil {
		t.Fatal("execution", e)
	}
	t.Cleanup(func() { st.Close() })
	return st, q
}
func TestN43ReviewSaturatedBoundAndCompletePeers(t *testing.T) {
	s := n43ReviewFixture(n43ReviewPage([]int64{math.MinInt64}, []int64{2}), n43ReviewPage([]int64{math.MinInt64, math.MinInt64 + 1}, []int64{4, 8}))
	sp := n43ReviewSpec()
	sp.Frame.PrecedingNS = math.MaxInt64
	st, q := n43ReviewOpen(t, s, sp, context.Background())
	b := memory.NewBatch()
	i := 0
	for {
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for r := 0; r < b.Rows; r++ {
			if i >= 3 || b.Int64s["sum"].Values[r] != []int64{6, 6, 14}[i] || b.Int64s["rows"].Values[r] != []int64{2, 2, 3}[i] {
				t.Fatal("peer/bound", i, b)
			}
			i++
		}
	}
	st.Close()
	if i != 3 || q.Used() != 0 || s.closes != 1 {
		t.Fatal(i, q.Used(), s.closes)
	}
}
func TestN43ReviewAverageIndependentOverflow(t *testing.T) {
	s := n43ReviewFixture(n43ReviewPage([]int64{1}, []int64{math.MaxInt64}), n43ReviewPage([]int64{1}, []int64{math.MaxInt64}))
	sp := n43ReviewSpec()
	sp.SumAlias = ""
	st, q := n43ReviewOpen(t, s, sp, context.Background())
	b := memory.NewBatch()
	n := 0
	for {
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for r := 0; r < b.Rows; r++ {
			if !b.Float64s["avg"].IsValid(r) || b.Float64s["avg"].Values[r] != float64(math.MaxInt64) || b.Int64s["count"].Values[r] != 2 {
				t.Fatal("avg", b)
			}
			n++
		}
	}
	st.Close()
	if n != 2 || q.Used() != 0 {
		t.Fatal(n, q.Used())
	}
}
func TestN43ReviewBadWitnessAndCloseCancellation(t *testing.T) {
	for _, cancelClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "descending", true: "close-cancel"}[cancelClose], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := n43ReviewFixture(n43ReviewPage([]int64{2}, []int64{1}), n43ReviewPage([]int64{1}, []int64{2}))
			if cancelClose {
				s.pages = []*memory.Batch{n43ReviewPage([]int64{1}, []int64{1})}
				s.cancel = cancel
			}
			st, q := n43ReviewOpen(t, s, n43ReviewSpec(), ctx)
			b := memory.NewBatch()
			ok, e := st.Next(ctx, b)
			if ok || e == nil || b.Rows != 0 {
				t.Fatal("published invalid peer", ok, e, b.Rows)
			}
			if cancelClose && !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			st.Close()
			if q.Used() != 0 || s.closes != 1 {
				t.Fatal(q.Used(), s.closes)
			}
		})
	}
}

func TestN43ReviewRuntimeMalformed(t *testing.T) {
	for _, kind := range []string{"extra-float", "extra-array", "nil-key", "short-mask", "required-null"} {
		t.Run(kind, func(t *testing.T) {
			p := n43ReviewPage([]int64{1, 1}, []int64{2, 4})
			switch kind {
			case "extra-float":
				p.Float64s["x"] = &memory.Float64Vector{Values: []float64{1, 2}}
			case "extra-array":
				p.Arrays["x"] = &memory.ArrayNode{}
			case "nil-key":
				p.Int64s["t"] = nil
			case "short-mask":
				p.Int64s["t"].Validity = memory.Bitmap{}
			case "required-null":
				p.Int64s["t"].Validity = memory.Bitmap{1}
			}
			s := n43ReviewFixture(p)
			st, q := n43ReviewOpen(t, s, n43ReviewSpec(), context.Background())
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if ok || e == nil || b.Rows != 0 {
				t.Fatal("malformed published", ok, e, b.Rows)
			}
			st.Close()
			if q.Used() != 0 {
				t.Fatal(q.Used())
			}
		})
	}
}
func TestN43ReviewSourceContextSharedQuery(t *testing.T) {
	s := n43ReviewFixture(n43ReviewPage([]int64{1}, []int64{1}))
	s.requireQuery = true
	st, q := n43ReviewOpen(t, s, n43ReviewSpec(), context.Background())
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil {
		t.Fatal("shared query must reach child with plain Next ctx", ok, e)
	}
	st.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}
func TestN43ReviewLimitCannotHideFuturePeerOverflow(t *testing.T) {
	s := n43ReviewFixture(n43ReviewPage([]int64{1}, []int64{math.MaxInt64}), n43ReviewPage([]int64{1}, []int64{1}))
	op, e := NewTimestampRangeSource(s, n43ReviewSpec())
	if e != nil {
		t.Fatal(e)
	}
	limited, e := NewLimitSource(op, 1)
	if e != nil {
		t.Fatal(e)
	}
	q, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := source.OpenBatchStream(ctx, limited, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if ok || e == nil || b.Rows != 0 {
		t.Fatal("Limit hid peer overflow", ok, e, b.Rows)
	}
	st.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

func TestN43ReviewHardBatchLimits(t *testing.T) {
	for _, kind := range []string{"payload", "owned"} {
		t.Run(kind, func(t *testing.T) {
			s := n43ReviewFixture(n43ReviewPage([]int64{1}, []int64{2}))
			op, e := NewTimestampRangeSource(s, n43ReviewSpec())
			if e != nil {
				t.Fatal(e)
			}
			q, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			opts := testReadOptions()
			if kind == "payload" {
				opts.BatchPayloadBytes = 16
			} else {
				opts.BatchBytes = 16
			}
			st, e := source.OpenBatchStream(ctx, op, opts)
			if e != nil {
				return
			}
			defer st.Close()
			b := memory.NewBatch()
			ok, e := st.Next(ctx, b)
			if ok || e == nil || b.Rows != 0 {
				t.Fatal("hard batch limit ignored", kind, ok, e, b.Rows)
			}
			st.Close()
			if q.Used() != 0 {
				t.Fatal(q.Used())
			}
		})
	}
}
func TestN43ReviewMutableSchemaAndForeignQuery(t *testing.T) {
	for _, kind := range []string{"schema", "foreign-query"} {
		t.Run(kind, func(t *testing.T) {
			s := n43ReviewFixture(n43ReviewPage([]int64{1}, []int64{2}))
			op, e := NewTimestampRangeSource(s, n43ReviewSpec())
			if e != nil {
				t.Fatal(e)
			}
			q, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			if kind == "schema" {
				s.schema.Fields[0].Nullable = true
				st, e := source.OpenBatchStream(ctx, op, testReadOptions())
				if e == nil {
					st.Close()
					t.Fatal("mutable schema accepted")
				}
				if s.reads != 0 || q.Used() != 0 {
					t.Fatal(s.reads, q.Used())
				}
				return
			}
			st, e := source.OpenBatchStream(ctx, op, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			other, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
			if e != nil {
				t.Fatal(e)
			}
			defer other.Close()
			b := memory.NewBatch()
			ok, e := st.Next(memory.WithQueryBudget(context.Background(), other), b)
			if ok || e == nil || b.Rows != 0 || s.reads != 0 {
				t.Fatal("foreign query", ok, e, b.Rows, s.reads)
			}
			st.Close()
			if q.Used() != 0 || other.Used() != 0 {
				t.Fatal(q.Used(), other.Used())
			}
		})
	}
}

func TestN43ReviewExactHardOutputSizing(t *testing.T) {
	// Six 8-byte values per row; exactly three nullable masks (v,sum,avg).
	// Thus one row=72 bytes, two rows=120 bytes, independent of manager headers.
	for _, kind := range []string{"payload", "owned"} {
		for _, limit := range []int64{71, 72, 119, 120} {
			t.Run(fmt.Sprintf("%s/%d", kind, limit), func(t *testing.T) {
				s := n43ReviewFixture(n43ReviewPage([]int64{1, 1}, []int64{2, 4}))
				op, e := NewTimestampRangeSource(s, n43ReviewSpec())
				if e != nil {
					t.Fatal(e)
				}
				q, e := memory.NewEngineBudget(4 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 4 << 20})
				if e != nil {
					t.Fatal(e)
				}
				defer q.Close()
				ctx := memory.WithQueryBudget(context.Background(), q)
				opts := testReadOptions()
				opts.BatchRows = 256
				if kind == "payload" {
					opts.BatchPayloadBytes = limit
				} else {
					opts.BatchBytes = limit
				}
				st, e := source.OpenBatchStream(ctx, op, opts)
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				b := memory.NewBatch()
				n, pages := 0, 0
				for {
					ok, e := st.Next(ctx, b)
					if limit == 71 {
						var re *memory.ResourceError
						if ok || !errors.Is(e, memory.ErrBudgetExceeded) || !errors.As(e, &re) || re.RequestedBytes != 72 || re.LimitBytes != 71 || b.Rows != 0 {
							t.Fatal("exact-minus-one", ok, e, b.Rows)
						}
						break
					}
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					pages++
					size := int64(b.Rows*6*8 + 3*memory.WordsFor(b.Rows)*8)
					if size > limit {
						t.Fatal(size, limit)
					}
					for r := 0; r < b.Rows; r++ {
						if b.Int64s["sum"].Values[r] != 6 || b.Int64s["rows"].Values[r] != 2 {
							t.Fatal("split lost peer completeness")
						}
					}
					n += b.Rows
				}
				st.Close()
				if limit >= 72 {
					want := 2
					if limit == 120 {
						want = 1
					}
					if n != 2 || pages != want {
						t.Fatal(n, pages, want)
					}
				}
				if q.Used() != 0 {
					t.Fatal(q.Used())
				}
			})
		}
	}
}
