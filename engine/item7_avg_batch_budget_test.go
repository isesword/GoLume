package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"reflect"
	"sync"
	"testing"
)

// Public execution fixture deliberately uses exact dyadic values. Input fixture
// allocations precede execution and are not claimed as query-charged/RSS memory.
func item7AvgBudgetPrepare(t *testing.T, kind string, limit int64) (*Engine, *PreparedQuery, map[string]TypedValue) {
	t.Helper()
	typ := memory.TypeFloat64
	if kind == "f32avg" {
		typ = memory.TypeFloat32
	}
	if kind == "i64avg" {
		typ = memory.TypeInt64
	}
	var batches []*memory.Batch
	var sum, minv float64
	var count uint64
	for start := 0; start < 257; start += 65 {
		n := min(65, 257-start)
		b := memory.NewBatch()
		b.Rows = n
		valid := make(memory.Bitmap, memory.WordsFor(n))
		pred := make([]int32, n)
		fv := make([]float64, n)
		f32 := make([]float32, n)
		iv := make([]int64, n)
		for i := 0; i < n; i++ {
			row := start + i
			v := float64(row%13-6) / 2
			if typ == memory.TypeInt64 {
				v = float64(row%13 - 6)
			}
			fv[i] = v
			f32[i] = float32(v)
			iv[i] = int64(v)
			if row%3 != 0 {
				pred[i] = 1
			}
			if row%7 != 0 {
				memory.Set(valid, i)
				if pred[i] != 0 {
					sum += v
					if count == 0 || v < minv {
						minv = v
					}
					count++
				}
			}
		}
		b.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		switch typ {
		case memory.TypeFloat64:
			b.Float64s["v"] = &memory.Float64Vector{Values: fv, Validity: valid}
		case memory.TypeFloat32:
			b.Float32s["v"] = &memory.Float32Vector{Values: f32, Validity: valid}
		case memory.TypeInt64:
			b.Int64s["v"] = &memory.Int64Vector{Values: iv, Validity: valid}
		}
		batches = append(batches, b)
	}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: typ, Nullable: true}), batches...)
	if e != nil {
		t.Fatal(e)
	}
	ag := []expr.Aggregate{expr.CountColumn("pred", "selected"), expr.CountColumn("v", "count")}
	want := map[string]TypedValue{"selected": {Type: memory.TypeUint64, Valid: true, Bits: 171}, "count": {Type: memory.TypeUint64, Valid: true, Bits: count}}
	if kind != "nonavg" {
		ag = append(ag, expr.Avg("v", "avg"))
		bits := math.Float64bits(sum / float64(count))
		outtyp := memory.TypeFloat64
		if typ == memory.TypeFloat32 {
			bits = uint64(math.Float32bits(float32(sum / float64(count))))
			outtyp = typ
		}
		want["avg"] = TypedValue{Type: outtyp, Valid: true, Bits: bits}
	}
	if kind == "sumavg" || kind == "nonavg" {
		ag = append(ag, expr.Sum("v", "sum"))
		want["sum"] = TypedValue{Type: typ, Valid: true, Bits: math.Float64bits(sum)}
	}
	if kind == "avgmin" {
		ag = append(ag, expr.Min("v", "min"))
		want["min"] = TypedValue{Type: typ, Valid: true, Bits: math.Float64bits(minv)}
	}
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.MemoryLimitBytes = limit
	opts.ExecutionPolicy = ExecutionMemoryOnly
	opts.PreparedCacheBytes = 0
	e1 := New(opts)
	p, e := e1.Prepare(context.Background(), src, expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(0)), Aggregates: ag}, PrepareOptions{MaxConcurrency: 1})
	if e != nil {
		e1.Close()
		t.Fatal(e)
	}
	return e1, p, want
}

type item7AvgBudgetContext struct {
	context.Context
	mu        sync.Mutex
	calls, at int
	stop      context.CancelFunc
}

func (c *item7AvgBudgetContext) Err() error {
	c.mu.Lock()
	c.calls++
	hit := c.at > 0 && c.calls >= c.at
	c.mu.Unlock()
	if hit {
		c.stop()
	}
	return c.Context.Err()
}
func item7AvgBudgetRun(t *testing.T, kind string, limit int64, cancelAt int, wantErr error) (int64, int) {
	t.Helper()
	e, p, want := item7AvgBudgetPrepare(t, kind, limit)
	defer e.Close()
	defer p.Close()
	eligible := kind == "f64avg" || kind == "sumavg" || kind == "nonavg"
	if p.globalAggregateBindingsEligible() != eligible {
		t.Fatalf("%s eligible=%v want%v", kind, p.globalAggregateBindingsEligible(), eligible)
	}
	q, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	base, stop := context.WithCancel(context.Background())
	defer stop()
	obs := &item7AvgBudgetContext{Context: base, at: cancelAt, stop: stop}
	ctx := memory.WithQueryBudget(obs, q)
	out, err := p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true})
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Fatalf("want%v got%v", wantErr, err)
		}
		if len(out.Rows) != 0 {
			t.Fatal("partial failed output")
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Rows) != 1 {
			t.Fatal("rows", len(out.Rows))
		}
		for _, v := range out.Rows[0].Aggregates {
			if v.Value != want[v.Alias] {
				t.Fatalf("%s got%+v want%+v", v.Alias, v.Value, want[v.Alias])
			}
		}
	}
	snap := q.Snapshot()
	obs.mu.Lock()
	calls := obs.calls
	obs.at = 0
	obs.mu.Unlock()
	if q.Used() != 0 || e.Memory().Used() != 0 {
		t.Fatalf("retained query%d engine%d", q.Used(), e.Memory().Used())
	}
	if wantErr == nil {
		again, er := p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true})
		if er != nil || !reflect.DeepEqual(out.Rows, again.Rows) || out.Summary.SelectedRows != again.Summary.SelectedRows || out.Summary.GroupCount != again.Summary.GroupCount || out.Summary.Checksum != again.Summary.Checksum {
			t.Fatalf("prepared reuse %v", er)
		}
		if q.Snapshot().PeakBytes != snap.PeakBytes || q.Used() != 0 {
			t.Fatal("reuse growth/leak")
		}
		p.Close()
		e.Close()
		if !reflect.DeepEqual(out.Rows, again.Rows) {
			t.Fatal("owned result invalidated")
		}
		t.Logf("kind%s peak%d checks%d result%+v", kind, snap.PeakBytes, calls, out.Rows)
	}
	return snap.PeakBytes, calls
}
func TestItem7AvgBatchBudget(t *testing.T) {
	for _, kind := range []string{"f64avg", "sumavg", "nonavg", "f32avg", "i64avg", "avgmin"} {
		t.Run(kind, func(t *testing.T) {
			peak, calls := item7AvgBudgetRun(t, kind, 64<<20, 0, nil)
			t.Run("exact", func(t *testing.T) { item7AvgBudgetRun(t, kind, peak, 0, nil) })
			t.Run("minusone", func(t *testing.T) { item7AvgBudgetRun(t, kind, peak-1, 0, memory.ErrBudgetExceeded) })
			if kind == "sumavg" {
				for at := 1; at <= calls; at++ {
					t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) { item7AvgBudgetRun(t, kind, 64<<20, at, context.Canceled) })
				}
			}
		})
	}
}

func item7AvgBudgetStateEqual(a, b aggregate.NumericResult) bool {
	bits := math.Float64bits(a.AvgSum) == math.Float64bits(b.AvgSum)
	a.AvgSum = 0
	b.AvgSum = 0
	return bits && a == b
}
func TestItem7AvgBatchBudgetSeedAtomic(t *testing.T) {
	seed := aggregate.NumericResult{Type: memory.TypeFloat64, Layout: aggregate.Layout{Sum: true, Avg: true, CountRows: true}, SumBits: math.Float64bits(42), SumValid: true, CountRows: math.MaxUint64 - 9, CountValues: math.MaxUint64 - 3, AvgSum: -19, MinBits: 123, MaxBits: 456}
	values := make([]float64, 257)
	selection := memory.Bitmap{math.MaxUint64, 0, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	valid := make(memory.Bitmap, 5)
	var selected uint64
	for i := range values {
		values[i] = []float64{1e16, 1, -1e16, .1, -.3}[i%5]
		if i%7 != 0 {
			memory.Set(valid, i)
		} else {
			values[i] = math.Float64frombits(0x7ff8000000000042)
		}
		if memory.IsSet(selection, i) {
			selected++
		}
	}
	want := seed
	sum := math.Float64frombits(want.SumBits)
	for i, v := range values {
		if memory.IsSet(selection, i) && memory.IsSet(valid, i) {
			want.AvgSum += v
			sum += v
			want.CountValues++
		}
	}
	want.CountRows += selected
	want.SumBits = math.Float64bits(sum)
	state := seed
	if err := consumeGlobalFloat64AverageBatch(context.Background(), values, valid, selection, selected, &state); err != nil {
		t.Fatal(err)
	}
	if !item7AvgBudgetStateEqual(state, want) {
		t.Fatalf("complete state mismatch got%+v want%+v", state, want)
	}
	base, stop := context.WithCancel(context.Background())
	defer stop()
	obs := &item7AvgBudgetContext{Context: base, stop: stop}
	state = seed
	if e := consumeGlobalFloat64AverageBatch(obs, values, valid, selection, selected, &state); e != nil {
		t.Fatal(e)
	}
	calls := obs.calls
	for at := 1; at <= calls; at++ {
		t.Run(fmt.Sprintf("cancel%d", at), func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			c := &item7AvgBudgetContext{Context: ctx, at: at, stop: stop}
			state := seed
			if e := consumeGlobalFloat64AverageBatch(c, values, valid, selection, selected, &state); !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if !item7AvgBudgetStateEqual(state, seed) {
				t.Fatal("partial helper commit")
			}
		})
	}
	for _, mode := range []string{"null", "unselected", "badbitmap", "badtype", "nilctx"} {
		t.Run(mode, func(t *testing.T) {
			s := seed
			s.SumBits = 1 << 63
			s.AvgSum = math.Float64frombits(0x7ff8000000000042)
			before := s
			sel := selection
			mask := valid
			ctx := context.Background()
			n := selected
			bad := false
			switch mode {
			case "null":
				mask = make(memory.Bitmap, 5)
			case "unselected":
				sel = make(memory.Bitmap, 5)
				n = 0
			case "badbitmap":
				mask = memory.Bitmap{}
				bad = true
			case "badtype":
				s.Type = memory.TypeFloat32
				before = s
				bad = true
			case "nilctx":
				ctx = nil
				bad = true
			}
			e := consumeGlobalFloat64AverageBatch(ctx, values, mask, sel, n, &s)
			if bad {
				if e == nil || !item7AvgBudgetStateEqual(s, before) {
					t.Fatal("bad input mutated state", e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				before.CountRows += n
				if !item7AvgBudgetStateEqual(s, before) {
					t.Fatal("empty-domain seed changed")
				}
			}
		})
	}
	allocs := testing.AllocsPerRun(100, func() {
		s := seed
		if e := consumeGlobalFloat64AverageBatch(context.Background(), values, valid, selection, selected, &s); e != nil {
			panic(e)
		}
	})
	if allocs != 0 {
		t.Fatalf("helper allocs=%g", allocs)
	}
	t.Logf("helper context checkpoints%d; zero helper allocations only", calls)
}
func TestItem7AvgBatchBudgetExceptionalFallback(t *testing.T) {
	for _, mode := range []string{"avg_nan_seed", "avg_inf_seed", "sum_nan_seed", "late_nan", "input_inf", "avg_overflow", "sum_overflow", "avg_only_ignored_sum"} {
		t.Run(mode, func(t *testing.T) {
			seed := aggregate.NumericResult{Type: memory.TypeFloat64, Layout: aggregate.Layout{Sum: true, Avg: true, CountRows: true}, SumBits: math.Float64bits(17), AvgSum: -23, SumValid: true, CountRows: 11, CountValues: 7, MinBits: 19, MaxBits: 29}
			values := make([]float64, 257)
			for i := range values {
				values[i] = 1
			}
			selection := memory.Bitmap{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
			switch mode {
			case "avg_nan_seed":
				seed.AvgSum = math.Float64frombits(0x7ff8000000000042)
			case "avg_inf_seed":
				seed.AvgSum = math.Inf(1)
			case "sum_nan_seed":
				seed.SumBits = 0x7ff8000000000043
			case "late_nan":
				values[256] = math.Float64frombits(0x7ff8000000000044)
			case "input_inf":
				values[129] = math.Inf(-1)
			case "avg_overflow":
				seed.AvgSum = math.MaxFloat64
				values[256] = math.MaxFloat64
			case "sum_overflow":
				seed.SumBits = math.Float64bits(math.MaxFloat64)
				seed.AvgSum = -math.MaxFloat64
				values[256] = math.MaxFloat64
			case "avg_only_ignored_sum":
				seed.Layout.Sum = false
				seed.SumBits = 0x7ff8000000000043
			}
			state := seed
			e := consumeGlobalFloat64AverageBatch(context.Background(), values, nil, selection, 257, &state)
			if mode == "avg_only_ignored_sum" {
				want := seed
				want.AvgSum += 257
				want.CountRows += 257
				want.CountValues += 257
				if e != nil || !item7AvgBudgetStateEqual(state, want) {
					t.Fatalf("ignored SUM changed AVG contract state%+v err%v", state, e)
				}
			} else {
				if !errors.Is(e, errGlobalFloat64AverageFallback) {
					t.Fatalf("expected exceptional fallback got%v", e)
				}
				if !item7AvgBudgetStateEqual(state, seed) {
					t.Fatal("exceptional path partially committed")
				}
			}
		})
	}
}
