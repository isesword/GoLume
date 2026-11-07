package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"sort"
	"testing"
)

func TestQuantileStrategies(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "global"} {
		t.Run(strategy, func(t *testing.T) {
			b := memory.NewBatch()
			b.Rows = 10
			b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}}
			b.Float64s["v"] = &memory.Float64Vector{Values: []float64{1, 1, 3, 7, 0, 2, 4, 6, 8, 10}, Validity: memory.Bitmap{0b1111101111}}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true}), b)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.Workers = 3
			opts.Pipeline.MaxMorselRows = 2
			q := expr.ConfiguredQuery{GroupBy: []string{"k"}, Aggregates: []expr.AggregateSpec{expr.Median("v", "median"), expr.Quantile("v", .25, aggregate.QuantileLinear, "q"), expr.Quantile("v", .25, aggregate.QuantileLower, "lower"), expr.NUnique("v", "distinct"), expr.Count("rows"), expr.Var("v", "var"), expr.First("v", "first"), expr.Sum("v", "sum")}}
			switch strategy {
			case "spill":
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
			case "global":
				q.GroupBy = nil
			default:
				q.Hints.ForceStrategy = strategy
			}
			if strategy == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "k"})
			}
			e := New(opts)
			defer e.Close()
			p, err := e.PrepareConfigured(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if len(p.quantiles().Fields) != 1 {
				t.Fatal("unshared quantile input")
			}
			for repeat := 0; repeat < 2; repeat++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				wantGroups := 2
				if strategy == "global" {
					wantGroups = 1
				}
				if len(r.Rows) != wantGroups {
					t.Fatal(len(r.Rows))
				}
				for _, row := range r.Rows {
					key := uint64(0)
					if len(row.Keys) > 0 {
						key = row.Keys[0].Value.Bits
					}
					want := []float64{2, 1, 1}
					distinct, rows := uint64(4), uint64(5)
					if key == 1 {
						want = []float64{6, 4, 4}
						distinct = 5
					}
					if strategy == "global" {
						want = []float64{4, 2, 2}
						distinct = 9
						rows = 10
					}
					for i, v := range want {
						got, valid := row.Aggregates[i].Value.AsFloat64()
						if !valid || got != v {
							t.Fatalf("key%d agg%d got%g want%g", key, i, got, v)
						}
					}
					if row.Aggregates[3].Value.Bits != distinct || row.Aggregates[4].Value.Bits != rows {
						t.Fatal(row)
					}
				}
			}
		})
	}
}
func TestQuantileEmptyAllNullAndInvalid(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2, 3}, Validity: memory.Bitmap{0}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	e := New(streamTestOptions())
	defer e.Close()
	for _, empty := range []bool{false, true} {
		q := expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("v", "m"), expr.Count("n")}}
		if empty {
			q.Filter = expr.Bool(false)
		}
		p, err := e.PrepareConfigured(context.Background(), src, q, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
		p.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Rows) != 1 || r.Rows[0].Aggregates[0].Value.Valid {
			t.Fatal(r)
		}
	}
	for _, q := range []float64{math.NaN(), math.Inf(1), -1, 2} {
		_, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Quantile("v", q, aggregate.QuantileLinear, "q")}}, PrepareOptions{})
		if err == nil {
			t.Fatal(q)
		}
	}
	_, err = e.Prepare(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{{Kind: expr.AggQuantile, Column: "v"}}}, PrepareOptions{})
	if err == nil {
		t.Fatal("bare quantile accepted")
	}
}
func TestQuantileHotGroupSpill(t *testing.T) {
	const n = 8192
	b := memory.NewBatch()
	b.Rows = n
	v := make([]uint64, n)
	for i := range v {
		v[i] = uint64(n - i)
	}
	b.Uint64s["v"] = &memory.Uint64Vector{Values: v}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeUint64}), b)
	if err != nil {
		t.Fatal(err)
	}
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 2 << 20
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
	opts.Pipeline.MaxMorselRows = 64
	e := New(opts)
	defer e.Close()
	p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("v", "m"), expr.Quantile("v", 1, aggregate.QuantileNearest, "max")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := r.Rows[0].Aggregates[0].Value.AsFloat64()
	if got != (n+1)/2.0 {
		t.Fatal(got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestQuantileNativeWidths(t *testing.T) {
	src, cols := nativeAggregateFixture(t)
	cols = cols[:10]
	q := expr.ConfiguredQuery{}
	for _, c := range cols {
		q.Aggregates = append(q.Aggregates, expr.Median(c.name, c.name))
	}
	e := New(streamTestOptions())
	defer e.Close()
	p, err := e.PrepareConfigured(context.Background(), src, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		values := append([]uint64(nil), c.bits[:5]...)
		sort.SliceStable(values, func(i, j int) bool {
			a, _ := aggregate.QuantileOrderKey(c.typ, values[i])
			b, _ := aggregate.QuantileOrderKey(c.typ, values[j])
			return a < b
		})
		plan, _ := aggregate.PlanQuantile(5, .5, aggregate.QuantileMidpoint)
		bits, _, _ := aggregate.InterpolateQuantile(c.typ, values[2], values[2], plan)
		wantType, _ := aggregate.QuantileOutputType(c.typ)
		got := r.Rows[0].Aggregates[i].Value
		if !got.Valid || got.Type != wantType || got.Bits != bits {
			t.Fatalf("%s got%+v wantbits%x", c.name, got, bits)
		}
	}
}

func TestQuantileSpillAccountingAndQuota(t *testing.T) {
	for _, quota := range []int64{64, 16 << 20} {
		t.Run(fmt.Sprint(quota), func(t *testing.T) {
			var batches []*memory.Batch
			for part := 0; part < 24; part++ {
				b := memory.NewBatch()
				b.Rows = 64
				v := make([]uint64, 64)
				for i := range v {
					v[i] = uint64(part*64 + i)
				}
				b.Uint64s["v"] = &memory.Uint64Vector{Values: v}
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeUint64}), batches...)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.ExecutionPolicy = ExecutionBounded
			opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: quota}
			e := New(opts)
			defer e.Close()
			p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("v", "m"), expr.Quantile("v", .1, aggregate.QuantileLinear, "q")}}, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			budget, err := e.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			stream, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			ok, err := stream.Next(context.Background(), out)
			if quota == 64 {
				if !errors.Is(err, memory.ErrSpillQuotaExceeded) {
					t.Fatalf("quota got %v", err)
				}
			} else {
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if got := out.Float64s["m"].Values[0]; got != 767.5 {
					t.Fatal(got)
				}
				if st := stream.(*distinctAggregateStream).Stats(); st.SpillRuns < 2 || st.SpillBytes == 0 {
					t.Fatal(st)
				}
				ok, err = stream.Next(context.Background(), out)
				if ok || err != nil {
					t.Fatal(ok, err)
				}
			}
			if err = stream.Close(); err != nil {
				t.Fatal(err)
			}
			if budget.Used() != 0 {
				t.Fatal("leaked", budget.Used())
			}
		})
	}
}
func TestQuantileMalformedTuple(t *testing.T) {
	src, _ := nativeAggregateFixture(t)
	e := New(streamTestOptions())
	defer e.Close()
	p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("u64", "m")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := &distinctAggregateStream{p: p, quantileLanes: make([]quantileLaneState, 1), quantileOutputs: make([]quantileOutputState, 1), counts: make([]byte, 9)}
	body := make([]byte, 37)
	body[0] = 3
	binary.BigEndian.PutUint64(body[5:13], 1)
	binary.LittleEndian.PutUint64(body[29:37], 1)
	if err = s.applyQuantile(body); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatal(err)
	}
	s.quantileLanes[0].Count = 1
	s.quantileLanes[0].Planned = false
	for _, bad := range [][]byte{body[:36], append([]byte(nil), body...)} {
		if len(bad) == 37 {
			bad[5] ^= 1
		}
		if err = s.applyQuantile(bad); !errors.Is(err, memory.ErrSpillCorrupt) {
			t.Fatal(err)
		}
	}
	if err = s.finishQuantiles(); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatal(err)
	}
	if err = s.applyQuantile(body); err != nil {
		t.Fatal(err)
	}
	if err = s.applyQuantile(body); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatal(err)
	}
	if err = s.finishQuantiles(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkQuantileAggregate(b *testing.B) {
	for _, spill := range []bool{false, true} {
		b.Run(fmt.Sprint(spill), func(b *testing.B) {
			batch := memory.NewBatch()
			batch.Rows = 4096
			v := make([]uint64, batch.Rows)
			for i := range v {
				v[i] = uint64((i * 137) % 4096)
			}
			batch.Uint64s["v"] = &memory.Uint64Vector{Values: v}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeUint64}), batch)
			if err != nil {
				b.Fatal(err)
			}
			opts := streamTestOptions()
			opts.MemoryLimitBytes = 8 << 20
			if spill {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: b.TempDir(), MaxBytes: 32 << 20}
			}
			e := New(opts)
			defer e.Close()
			p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("v", "m"), expr.Quantile("v", .9, aggregate.QuantileLinear, "q")}}, PrepareOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestQuantileBoundedWithoutSpillRejected(t *testing.T) {
	src, _ := nativeAggregateFixture(t)
	opts := streamTestOptions()
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = SpillOptions{}
	e := New(opts)
	defer e.Close()
	_, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("u64", "m")}}, PrepareOptions{})
	if !errors.Is(err, ErrBoundedSpillRequired) {
		t.Fatal(err)
	}
}
