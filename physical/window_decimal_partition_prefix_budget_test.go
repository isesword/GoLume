package physical

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
)

// This source charges its full live vector backing storage to the very manager
// supplied by the operator. It reuses and poisons buffers, retaining no history.
// Fixture objects and arbitrary Go/runtime allocations are outside this claim.
type n36BudgetInput struct {
	schema                memory.Schema
	total, page, group    int
	opened, closed, reads int
	malformed             string
}

func n36BudgetSource(t *testing.T, total, page, group int) *n36BudgetInput {
	f, e := memory.Decimal128Field("v", true, 9, 0)
	if e != nil {
		t.Fatal(e)
	}
	return &n36BudgetInput{schema: memory.NewSchema(memory.Field{Name: "p", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "k", Type: memory.TypeInt64}, f), total: total, page: page, group: group}
}
func (s *n36BudgetInput) Schema() memory.Schema { return s.schema }
func (s *n36BudgetInput) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.total), Partitions: 1}, nil
}
func (s *n36BudgetInput) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.total)}}, nil
}
func (s *n36BudgetInput) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "p"}, {Name: "k"}}
}
func (s *n36BudgetInput) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Class: source.StreamPipeline}
}
func (s *n36BudgetInput) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &n36BudgetInputStream{src: s, m: o.MemoryManager}, nil
}

type n36BudgetInputStream struct {
	src   *n36BudgetInput
	m     memory.Manager
	lease memory.Reservation
	next  int
	p, k  *memory.Int64Vector
	v     *memory.Decimal128Vector
	done  bool
}

func (s *n36BudgetInputStream) poison() {
	if s.p != nil {
		for i := range s.p.Values {
			s.p.Values[i] = -999
			s.k.Values[i] = -999
			s.v.Values[i] = decimal128.FromI64(-999)
		}
	}
}
func (s *n36BudgetInputStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	b.ClearColumns()
	s.poison()
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
	if e := ctx.Err(); e != nil {
		return false, e
	}
	s.src.reads++
	if s.next == s.src.total {
		return false, nil
	}
	n := min(s.src.page, s.src.total-s.next)
	words := memory.WordsFor(n)
	bytes := int64(n*32 + words*16)
	var e error
	s.lease, e = s.m.Reserve(ctx, bytes)
	if e != nil {
		return false, e
	}
	if s.p == nil || len(s.p.Values) != n {
		s.p = &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, words)}
		s.k = &memory.Int64Vector{Values: make([]int64, n)}
		s.v = &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Validity: make(memory.Bitmap, words), Precision: 9}
	} else {
		clear(s.p.Validity)
		clear(s.v.Validity)
	}
	for i := 0; i < n; i++ {
		index := s.next + i
		part := index / s.src.group
		ord := index % s.src.group
		s.p.Values[i] = int64(part)
		if part > 0 {
			memory.Set(s.p.Validity, i)
		}
		s.k.Values[i] = int64(ord)
		s.v.Values[i] = decimal128.FromI64(int64(2 * (ord + 1)))
		memory.Set(s.v.Validity, i)
	}
	if s.next > 0 {
		switch s.src.malformed {
		case "partition_back":
			s.p.Values[n-1] = -1
			memory.Set(s.p.Validity, n-1)
		case "null_after_valid":
			s.p.Validity[(n-1)/64] &^= uint64(1) << uint((n-1)%64)
		case "order_duplicate":
			s.k.Values[n-1] = s.k.Values[n-2]
		}
	}
	s.next += n
	b.Rows = n
	b.Int64s["p"] = s.p
	b.Int64s["k"] = s.k
	b.Decimals["v"] = s.v
	return true, nil
}
func (s *n36BudgetInputStream) Close() error {
	if !s.done {
		s.done = true
		s.src.closed++
		s.poison()
		if s.lease != nil {
			s.lease.Release()
			s.lease = nil
		}
	}
	return nil
}

type n36BudgetProbe struct {
	memory.Manager
	calls, failAt, cancelAt int
	cancel                  context.CancelFunc
	peak                    int64
	requests                []int64
}

func (m *n36BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.requests = append(m.requests, n)
	if m.calls == m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if e == nil && m.calls == m.cancelAt {
		m.cancel()
	}
	return r, e
}
func n36BudgetSpec() PartitionedDecimalPrefixAverage {
	return PartitionedDecimalPrefixAverage{Column: "v", PartitionBy: "p", OrderBy: "k", Alias: "avg", Precision: 12, Scale: 0, Rounding: aggregate.DecimalAverageHalfEven, Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
}

type n36BudgetMeasure struct {
	peak     int64
	calls    int
	requests []int64
}

func n36BudgetRun(t *testing.T, total, page, group int, limit int64, fail, cancelAt int, early bool, want error) n36BudgetMeasure {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &n36BudgetProbe{Manager: memory.NewBudgetManager(limit), failAt: fail, cancelAt: cancelAt, cancel: cancel}
	input := n36BudgetSource(t, total, page, group)
	op, e := NewPartitionedDecimalPrefixAverageSource(input, n36BudgetSpec())
	if e != nil {
		t.Fatal(e)
	}
	st, e := op.NewBatchStream(ctx, n27Opts(m))
	rows := 0
	if e == nil {
		b := memory.NewBatch()
		for {
			var ok bool
			ok, e = st.Next(ctx, b)
			if e != nil {
				if ok || b.Rows != 0 || len(b.Decimals) != 0 || len(b.Int64s) != 0 {
					t.Fatal("failed page was published")
				}
				break
			}
			if !ok {
				break
			}
			avg := b.Decimals["avg"]
			if avg == nil || avg.Precision != 12 || avg.Scale != 0 {
				t.Fatal("output schema")
			}
			for i := 0; i < b.Rows; i++ {
				index := rows + i
				part := index / group
				ord := index % group
				if b.Int64s["p"].IsValid(i) != (part > 0) || b.Int64s["k"].Values[i] != int64(ord) || !avg.IsValid(i) || avg.Values[i] != decimal128.FromI64(int64(ord+2)) {
					t.Fatalf("independent prefix oracle index=%d avg=%v", index, avg.Values[i])
				}
			}
			// Full input backing storage (32*n + two validity bitmaps) and output
			// are concurrently live; charging only AVG or a fixed adapter is invalid.
			full := int64(b.Rows*32 + memory.WordsFor(b.Rows)*16)
			if m.Used() <= full {
				t.Fatalf("missing simultaneous state/output charge: live=%d fullinput=%d", m.Used(), full)
			}
			rows += b.Rows
			if early {
				break
			}
		}
	}
	got := n36BudgetMeasure{m.peak, m.calls, append([]int64(nil), m.requests...)}
	if want != nil {
		if !errors.Is(e, want) {
			t.Fatalf("want=%v got=%v", want, e)
		}
	} else if e != nil {
		t.Fatal(e)
	} else if !early && rows != total {
		t.Fatalf("rows %d/%d", rows, total)
	}
	m.failAt = 0
	m.cancelAt = 0
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if m.Used() != 0 {
		t.Fatalf("memory leak %d", m.Used())
	}
	if input.closed != input.opened {
		t.Fatalf("input open/close %d/%d", input.opened, input.closed)
	}
	return got
}
func TestN36PartitionPrefixBudgetFixedState(t *testing.T) {
	a := n36BudgetRun(t, 64, 32, 4, 1<<20, 0, 0, false, nil)
	b := n36BudgetRun(t, 4096, 32, 4, 1<<20, 0, 0, false, nil)
	if a.peak != b.peak {
		t.Fatalf("per-partition growth: small=%d large=%d", a.peak, b.peak)
	}
	t.Logf("measured peak=%d shortCalls=%d longCalls=%d firstStateReservation=%d full32rowInput=%d", a.peak, a.calls, b.calls, a.requests[0], 32*32+16)
	t.Run("exact", func(t *testing.T) { n36BudgetRun(t, 64, 32, 4, a.peak, 0, 0, false, nil) })
	t.Run("minus_one", func(t *testing.T) { n36BudgetRun(t, 64, 32, 4, a.peak-1, 0, 0, false, memory.ErrBudgetExceeded) })
	t.Run("cross_page_partition", func(t *testing.T) { n36BudgetRun(t, 60, 3, 10, 1<<20, 0, 0, false, nil) })
	t.Run("early_close", func(t *testing.T) { n36BudgetRun(t, 64, 32, 4, 1<<20, 0, 0, true, nil) })
}
func TestN36PartitionPrefixBudgetReserveFaults(t *testing.T) {
	baseline := n36BudgetRun(t, 64, 16, 8, 1<<20, 0, 0, false, nil)
	for at := 1; at <= baseline.calls; at++ {
		t.Run(fmt.Sprintf("reserve_%d", at), func(t *testing.T) { n36BudgetRun(t, 64, 16, 8, 1<<20, at, 0, false, memory.ErrBudgetExceeded) })
	}
}
func TestN36PartitionPrefixBudgetCancel(t *testing.T) {
	baseline := n36BudgetRun(t, 64, 16, 8, 1<<20, 0, 0, false, nil)
	for at := 1; at <= baseline.calls; at++ {
		t.Run(fmt.Sprintf("reserve_%d", at), func(t *testing.T) { n36BudgetRun(t, 64, 16, 8, 1<<20, 0, at, false, context.Canceled) })
	}
}
func TestN36PartitionPrefixBudgetPublicationBarrier(t *testing.T) {
	for _, bad := range []string{"partition_back", "null_after_valid", "order_duplicate"} {
		t.Run(bad, func(t *testing.T) {
			input := n36BudgetSource(t, 12, 6, 4)
			input.malformed = bad
			m := memory.NewBudgetManager(1 << 20)
			op, e := NewPartitionedDecimalPrefixAverageSource(input, n36BudgetSpec())
			if e != nil {
				t.Fatal(e)
			}
			st, e := op.NewBatchStream(context.Background(), n27Opts(m))
			if e != nil {
				t.Fatal(e)
			}
			b := memory.NewBatch()
			if ok, e := st.Next(context.Background(), b); !ok || e != nil || b.Rows != 6 {
				t.Fatal(ok, e, b.Rows)
			}
			if ok, e := st.Next(context.Background(), b); ok || e == nil || b.Rows != 0 || len(b.Decimals) != 0 || len(b.Int64s) != 0 {
				t.Fatalf("bad page published: ok=%v err=%v rows=%d", ok, e, b.Rows)
			}
			if e := st.Close(); e != nil {
				t.Fatal(e)
			}
			if m.Used() != 0 || input.closed != 1 {
				t.Fatal("failed page leaked")
			}
		})
	}
}
