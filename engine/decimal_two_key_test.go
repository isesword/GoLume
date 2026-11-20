package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source"
)

type n23Key struct {
	x, y   int64
	xv, yv bool
}
type n23Row struct {
	k  n23Key
	d  int64
	dv bool
}
type n23Source struct{ *n17Source }

func (s n23Source) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "fixture", PartitionParallel: true}
}

func (s n23Source) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return nil, errors.New("fixture requires explicit prepared partition sequence")
}
func (s n23Source) NewBatchStreamWithPartitions(_ context.Context, _ source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if len(parts) != len(s.batches) {
		return nil, fmt.Errorf("wrong partition count %d", len(parts))
	}
	for i, p := range parts {
		if p.ID != i || p.Rows != int64(s.batches[i].Rows) {
			return nil, fmt.Errorf("wrong partition descriptor: %v", p)
		}
	}
	return &n17Input{s: s.n17Source}, nil
}

func (s n23Source) Partitions(context.Context) ([]source.Partition, error) {
	p := make([]source.Partition, len(s.batches))
	for i, b := range s.batches {
		p[i] = source.Partition{ID: i, Rows: int64(b.Rows)}
	}
	return p, nil
}

func n23Data(t *testing.T, groups [][]n23Row, proof []source.OrderingColumn) n23Source {
	t.Helper()
	d, e := memory.Decimal128Field("d", true, 38, 1)
	if e != nil {
		t.Fatal(e)
	}
	s := &n17Source{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "y", Type: memory.TypeInt64, Nullable: true}, d), ordering: proof}
	for _, rows := range groups {
		b := memory.NewBatch()
		b.Rows = len(rows)
		x := &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows)}
		y := &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows)}
		d := &memory.Decimal128Vector{Values: make([]memory.Decimal128, b.Rows), Validity: memory.EnsureBitmap(nil, b.Rows), Precision: 38, Scale: 1}
		for i, r := range rows {
			x.Values[i], y.Values[i] = r.k.x, r.k.y
			if r.k.xv {
				memory.Set(x.Validity, i)
			}
			if r.k.yv {
				memory.Set(y.Validity, i)
			}
			d.Values[i] = decimal128.FromI64(r.d)
			if r.dv {
				memory.Set(d.Validity, i)
			}
		}
		b.Int64s["x"], b.Int64s["y"], b.Decimals["d"] = x, y, d
		s.batches = append(s.batches, b)
	}
	return n23Source{s}
}

func TestN23DecimalOrderedTwoKey(t *testing.T) {
	proof := []source.OrderingColumn{{Name: "x", NullsLast: true}, {Name: "y", Descending: true}}
	// y is descending with NULL first, independently of ascending x / NULL last.
	a, b, c, d, e := n23Key{1, 0, true, false}, n23Key{1, 4, true, true}, n23Key{1, 2, true, true}, n23Key{2, 5, true, true}, n23Key{0, 7, false, true}
	src := n23Data(t, [][]n23Row{{{a, 1, true}, {b, 15, true}}, {{b, 35, true}, {c, 7, false}, {d, -25, true}}, {{e, 45, true}}}, proof)
	en := New(Options{ExecutionPolicy: ExecutionMemoryOnly, MemoryLimitBytes: 64 << 20})
	defer en.Close()
	p, err := en.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{GroupBy: []string{"x", "y"}, Aggregates: []expr.AggregateSpec{expr.Sum("d", "s"), expr.Count("n"), expr.CountColumn("d", "valid"), expr.AvgDecimal128("d", 5, 0, expr.DecimalRoundHalfEven, "avg")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.strategy.Kind != planner.StrategySorted || p.decimalResident() {
		t.Fatal("complete proof must prefer sorted execution", p.strategy)
	}
	want := []struct {
		k           n23Key
		sum, avg    int64
		rows, valid uint64
	}{{a, 1, 0, 1, 1}, {b, 50, 2, 2, 2}, {c, 0, 0, 1, 0}, {d, -25, -2, 1, 1}, {e, 45, 4, 1, 1}}
	pos := 0
	check := func(b *memory.Batch) {
		x, y, s, av, n, v := b.Int64s["x"], b.Int64s["y"], b.Decimals["s"], b.Decimals["avg"], b.Uint64s["n"], b.Uint64s["valid"]
		if x == nil || y == nil || s == nil || av == nil || n == nil || v == nil {
			t.Fatal("missing output key or aggregate")
		}
		if s.Precision != 38 || s.Scale != 1 || av.Precision != 5 || av.Scale != 0 {
			t.Fatal("metadata", s.Precision, s.Scale, av.Precision, av.Scale)
		}
		for i := 0; i < b.Rows; i++ {
			if pos >= len(want) {
				t.Fatal("extra group")
			}
			w := want[pos]
			pos++
			if x.IsValid(i) != w.k.xv || y.IsValid(i) != w.k.yv || w.k.xv && x.Values[i] != w.k.x || w.k.yv && y.Values[i] != w.k.y || n.Values[i] != w.rows || v.Values[i] != w.valid {
				t.Fatal("key/count", pos, w)
			}
			has := w.valid != 0
			if s.IsValid(i) != has || av.IsValid(i) != has || has && (s.Values[i] != decimal128.FromI64(w.sum) || av.Values[i] != decimal128.FromI64(w.avg)) {
				t.Fatal("decimal", pos, w, s.Values[i], av.Values[i])
			}
		}
	}
	calls, peak, err := n17Run(t, p, 64<<20, 0, 2, check)
	if err != nil || pos != len(want) || calls == 0 || peak <= 0 {
		t.Fatal(pos, calls, peak, err)
	}
	t.Logf("two-key groups=%d reserve calls=%d peak=%d", pos, calls, peak)
	if _, _, err = n17Run(t, p, peak, 0, 2, nil); err != nil {
		t.Fatal("exact peak", err)
	}
	if _, _, err = n17Run(t, p, peak-1, 0, 2, nil); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal("peak minus one", err)
	}
	for i := 1; i <= calls; i++ {
		if _, _, err = n17Run(t, p, 64<<20, i, 2, nil); !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatal("reserve fault", i, err)
		}
	}
}

func TestN23DecimalOrderedTwoKeyRejects(t *testing.T) {
	ctx := context.Background()
	q := expr.Query{GroupBy: []string{"x", "y"}, Aggregates: []expr.Aggregate{expr.Sum("d", "s")}}
	for _, proof := range [][]source.OrderingColumn{nil, {{Name: "x"}}, {{Name: "x"}, {Name: "d"}, {Name: "y"}}, {{Name: "y"}, {Name: "x"}}} {
		src := n23Data(t, nil, proof)
		for _, policy := range []ExecutionPolicy{ExecutionAuto, ExecutionBounded} {
			en := New(Options{ExecutionPolicy: policy, MemoryLimitBytes: 64 << 20})
			p, err := en.Prepare(ctx, src, q, PrepareOptions{})
			if p != nil {
				p.Close()
			}
			en.Close()
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "order") {
				t.Fatalf("bad ordering proof %v, policy %v: %v", proof, policy, err)
			}
		}
	}
	en := New(Options{ExecutionPolicy: ExecutionMemoryOnly, MemoryLimitBytes: 64 << 20})
	defer en.Close()
	// First key ties across the partition boundary. The bad second key belongs
	// to a NULL Decimal row and must not bypass validation via the filter.
	a, b := n23Key{1, 2, true, true}, n23Key{1, 3, true, true}
	src := n23Data(t, [][]n23Row{{{a, 12, true}}, {{b, 0, false}}}, []source.OrderingColumn{{Name: "x"}, {Name: "y", Descending: true}})
	q.Filter = expr.IsNotNull(expr.Col("d"))
	p, err := en.Prepare(ctx, src, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	seen := 0
	_, _, err = n17Run(t, p, 64<<20, 0, 64, func(*memory.Batch) { seen++ })
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "order") || seen != 0 {
		t.Fatal("invalid second ordering was not rejected", seen, err)
	}
}
