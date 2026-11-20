package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
	"unsafe"
)

func TestN15PrepareMixed(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 38, 0)
	src, e := inmemory.New(memory.NewSchema(f))
	if e != nil {
		t.Fatal(e)
	}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	defer en.Close()
	p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), expr.Count("n"), expr.CountColumn("d", "valid")}}, PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
}

func n15Prepared(t *testing.T, repeat int, filtered bool) (*Engine, *PreparedQuery, *n14StreamSource) {
	t.Helper()
	d, _ := memory.Decimal128Field("d", true, 38, 0)
	eField, _ := memory.Decimal128Field("e", true, 1, 0)
	b := memory.NewBatch()
	b.Rows = 4
	b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38, Validity: memory.Bitmap{11}, Values: []memory.Decimal128{decimal128.FromI64(1), decimal128.FromI64(2), decimal128.New(math.MaxInt64, math.MaxUint64), decimal128.FromI64(-3)}}
	b.Decimals["e"] = &memory.Decimal128Vector{Precision: 1, Validity: memory.Bitmap{5}, Values: []memory.Decimal128{decimal128.FromI64(1), decimal128.New(math.MaxInt64, math.MaxUint64), decimal128.FromI64(2), {}}}
	src := &n14StreamSource{schema: memory.NewSchema(d, eField), batches: []*memory.Batch{b}, repeat: repeat}
	en := New(Options{MemoryLimitBytes: 1 << 20})
	var pred expr.Expr
	if filtered {
		pred = expr.IsNotNull(expr.Col("d"))
	}
	p, err := en.Prepare(context.Background(), src, expr.Query{Filter: pred, Aggregates: []expr.Aggregate{expr.Count("rows"), expr.Sum("d", "sum"), expr.CountColumn("e", "valid"), expr.CountColumn("d", "validd")}}, PrepareOptions{})
	if err != nil {
		en.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(); en.Close() })
	return en, p, src
}
func TestN15CountsAndBorrow(t *testing.T) {
	for _, repeat := range []int{0, 1, 1000} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("repeat%d/filter%v", repeat, filtered), func(t *testing.T) {
				en, p, src := n15Prepared(t, repeat, filtered)
				q, _ := en.BeginQueryMemory()
				defer q.Close()
				ctx := memory.WithQueryBudget(context.Background(), q)
				st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				ok, e := st.Next(ctx, out)
				if !ok || e != nil {
					t.Fatal(ok, e)
				}
				n, v := 4*repeat, 2*repeat
				if filtered {
					n = 3 * repeat
					v = repeat
				}
				for name, want := range map[string]uint64{"rows": uint64(n), "valid": uint64(v), "validd": uint64(3 * repeat)} {
					if out.Uint64s[name].Values[0] != want || !out.Uint64s[name].IsValid(0) {
						t.Fatal(name, want)
					}
				}
				sum := out.Decimals["sum"]
				if sum.IsValid(0) != (repeat != 0) || repeat != 0 && sum.Values[0] != decimal128.FromI64(0) {
					t.Fatal("sum")
				}
				if src.closed != 1 {
					t.Fatal(src.closed)
				}
				c := out.Uint64s["rows"]
				st.Close()
				if len(c.Values) != 0 || out.Rows != 0 || q.Used() != 0 {
					t.Fatal("borrow cleanup")
				}
			})
		}
	}
}
func TestN15BudgetFaultsExactAndCountOverflow(t *testing.T) {
	_, p, _ := n15Prepared(t, 100, true)
	calls, peak, e := n14BudgetRun(t, p, 1<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("direct calls=%d peak=%d", calls, peak)
	for i := 1; i <= calls; i++ {
		_, _, e = n14BudgetRun(t, p, 1<<20, i)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(i, e)
		}
	}
	_, _, e = n14BudgetRun(t, p, peak, 0)
	if e != nil {
		t.Fatal("exact", e)
	}
	_, _, e = n14BudgetRun(t, p, peak-1, 0)
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("minus1", e)
	}
	n := uint64(math.MaxUint64)
	if e = decimalCountIncrement(&n); e == nil || n != math.MaxUint64 {
		t.Fatal("overflow changed counter")
	}
}
func TestN15CountBatchAtomicValidation(t *testing.T) {
	for _, mode := range []string{"coefficient", "metadata", "empty", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			en, p, src := n15Prepared(t, 1, false)
			q, _ := en.BeginQueryMemory()
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
			if e != nil {
				t.Fatal(e)
			}
			s := st.(*decimalGlobalStream)
			switch mode {
			case "coefficient":
				src.batches[0].Decimals["e"].Values[0] = decimal128.FromI64(10)
			case "metadata":
				src.batches[0].Decimals["e"].Scale = 1
			case "empty":
				src.batches[0].Rows = 0
			case "overflow":
				s.counts[3] = math.MaxUint64
			}
			before := append([]uint64(nil), s.counts...)
			if e = s.consume(ctx); e == nil {
				t.Fatal("bad COUNT batch accepted")
			}
			if mode == "overflow" && s.scratch[1].Count() != 1 {
				t.Fatal("did not reach earlier SUM candidate before later COUNT failure")
			}
			for i, c := range before {
				if s.counts[i] != c || s.states[i].Count() != 0 {
					t.Fatal("partial commit", i)
				}
			}
			st.Close()
			if q.Used() != 0 {
				t.Fatal(q.Used())
			}
		})
	}
}
func TestN15NarrowCompileBoundary(t *testing.T) {
	d, _ := memory.Decimal128Field("d", true, 38, 0)
	schema := memory.NewSchema(d, memory.Field{Name: "i", Type: memory.TypeInt64})
	for _, a := range []expr.Aggregate{expr.CountColumn("missing", "bad"), expr.CountColumn("", "bad"), expr.CountColumn("i", "bad"), expr.Avg("d", "bad")} {
		if _, e := compileQuery(schema, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), a}}); e == nil {
			t.Fatal(a)
		}
	}
	if _, e := compileQuery(schema, expr.Query{Aggregates: []expr.Aggregate{expr.Count("n")}}); e == nil {
		t.Fatal("count only")
	}
	rows := expr.Count("n")
	rows.Column = "ignored-old-contract"
	if _, e := compileQuery(schema, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("d", "sum"), rows}}); e != nil {
		t.Fatal(e)
	}
}

func TestN15WideCountCancel(t *testing.T) {
	for _, phase := range []string{"consume", "finalize", "layout", "output"} {
		t.Run(phase, func(t *testing.T) {
			f, _ := memory.Decimal128Field("d", true, 38, 0)
			b := memory.NewBatch()
			b.Decimals["d"] = &memory.Decimal128Vector{Precision: 38}
			repeat := 0
			if phase == "consume" {
				repeat = 1
			}
			src := &n14StreamSource{schema: memory.NewSchema(f), batches: []*memory.Batch{b}, repeat: repeat}
			aggs := make([]expr.Aggregate, 512)
			aggs[0] = expr.Sum("d", "sum")
			for i := 1; i < len(aggs); i++ {
				aggs[i] = expr.Count(fmt.Sprintf("c%d", i))
			}
			en := New(Options{MemoryLimitBytes: 2 << 20})
			defer en.Close()
			p, e := en.Prepare(context.Background(), src, expr.Query{Aggregates: aggs}, PrepareOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			q, _ := en.BeginQueryMemory()
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			st, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
			if e != nil {
				t.Fatal(e)
			}
			c := &n14CancelContext{Context: ctx, function: "(*decimalGlobalStream).Next", at: 3}
			if phase == "consume" {
				c.function = "(*decimalGlobalStream).consume"
				c.at = 50
			}
			if phase == "layout" {
				c.at = 12
			}
			if phase == "output" {
				c.at = 20
			}
			out := memory.NewBatch()
			out.Rows = 7
			caller := &memory.Uint64Vector{Values: []uint64{7}}
			out.Uint64s["caller"] = caller
			if ok, e := st.Next(c, out); ok || !errors.Is(e, context.Canceled) {
				t.Fatal(ok, e, c.calls)
			}
			if q.Used() != 0 || out.Rows != 7 || len(caller.Values) != 1 || caller.Values[0] != 7 || out.Uint64s["caller"] != caller {
				t.Fatal("cleanup", q.Used(), out.Rows)
			}
			if _, e = st.Next(context.Background(), out); !errors.Is(e, context.Canceled) {
				t.Fatal("sticky", e)
			}
			st.Close()
		})
	}
}
func TestN15ConstantState(t *testing.T) {
	var peak int64
	for _, n := range []int{1, 1000} {
		_, p, _ := n15Prepared(t, n, false)
		_, v, e := n14BudgetRun(t, p, 1<<20, 0)
		if e != nil {
			t.Fatal(e)
		}
		if n == 1 {
			peak = v
		} else if v != peak {
			t.Fatal("growing retained state", peak, v)
		}
	}
}

func TestN15CountNoAllocAndOutputSizes(t *testing.T) {
	if n := testing.AllocsPerRun(100, func() {
		var c uint64
		for i := 0; i < 64; i++ {
			_ = decimalCountIncrement(&c)
		}
	}); n != 0 {
		t.Fatal(n)
	}
	t.Logf("output headers Decimal=%d Uint64=%d map header reference=%d; per-output envelope=1024, separate map hints sumSlots/countSlots", unsafe.Sizeof(memory.Decimal128Vector{}), unsafe.Sizeof(memory.Uint64Vector{}), unsafe.Sizeof(map[string]*memory.Uint64Vector{}))
}
