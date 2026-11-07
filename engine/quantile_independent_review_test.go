package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source/inmemory"
)

func TestReviewQuantileSharedLanesNullGroupsAndBytes(t *testing.T) {
	for _, spill := range []bool{false, true} {
		t.Run(fmt.Sprint(spill), func(t *testing.T) {
			var batches []*memory.Batch
			for part := 0; part < 9; part++ {
				b := memory.NewBatch()
				b.Rows = 3
				b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 1, 2}}
				b.Int64s["v"] = &memory.Int64Vector{Values: []int64{999, int64(part % 3), int64(8 - part)}, Validity: memory.Bitmap{6}}
				b.Float32s["f"] = &memory.Float32Vector{Values: []float32{float32(part), 100, 200}, Validity: memory.Bitmap{1}}
				b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte{byte('a' + part), byte('A' + part), byte(part)}, Validity: memory.Bitmap{6}}
				batches = append(batches, b)
			}
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "f", Type: memory.TypeFloat32, Nullable: true}, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true})
			src, err := inmemory.New(schema, batches...)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.Pipeline.MaxMorselRows = 1
			opts.Workers = 3
			opts.MemoryLimitBytes = 2 << 20
			if spill {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
			}
			e := New(opts)
			defer e.Close()
			specs := []expr.AggregateSpec{expr.Median("v", "m"), expr.Quantile("v", 0, aggregate.QuantileNearest, "min"), expr.Quantile("v", 1, aggregate.QuantileNearest, "max"), expr.Median("f", "fm"), expr.NUnique("v", "nv"), expr.NUnique("s", "ns"), expr.First("s", "first"), expr.Last("s", "last"), expr.Count("rows")}
			p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{GroupBy: []string{"k"}, Aggregates: specs}, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if len(p.quantiles().Fields) != 2 || !strings.Contains(p.Plan().Strategy.Reason, "exact-global-tuple-sort") {
				t.Fatal(p.Plan())
			}
			specs[0] = expr.Median("missing", "bad")
			for repeat := 0; repeat < 2; repeat++ {
				r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Rows) != 3 {
					t.Fatal(r)
				}
				for _, row := range r.Rows {
					key := row.Keys[0].Value.Bits
					a := row.Aggregates
					if a[8].Value.Bits != 9 {
						t.Fatal(a)
					}
					if key == 0 {
						for _, i := range []int{0, 1, 2, 6, 7} {
							if a[i].Value.Valid {
								t.Fatalf("all-null key0 agg%d: %+v", i, a[i])
							}
						}
						v, ok := a[3].Value.AsFloat64()
						if !ok || v != 4 || a[4].Value.Bits != 1 || a[5].Value.Bits != 1 {
							t.Fatal(a)
						}
					} else {
						want := []float64{1, 0, 2}
						nd := uint64(3)
						first, last := "A", "I"
						if key == 2 {
							want = []float64{4, 0, 8}
							nd = 9
							first = string([]byte{0})
							last = string([]byte{8})
						}
						for i, x := range want {
							v, ok := a[i].Value.AsFloat64()
							if !ok || v != x {
								t.Fatalf("key%d agg%d: %v want%v", key, i, v, x)
							}
						}
						if a[3].Value.Valid || a[4].Value.Bits != nd || a[5].Value.Bits != 9 || a[6].Value.Text != first || a[7].Value.Text != last {
							t.Fatal(a)
						}
					}
				}
			}
		})
	}
}

func TestReviewQuantileMemoryOnlyNeverSpills(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 8192
	b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
	for i := range b.Int64s["v"].Values {
		b.Int64s["v"].Values[i] = int64(i)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}), b)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	opts := streamTestOptions()
	opts.ExecutionPolicy = ExecutionMemoryOnly
	opts.MemoryLimitBytes = 128 << 10
	opts.Spill = SpillOptions{Directory: dir, MaxBytes: 16 << 20}
	opts.Pipeline.MaxMorselRows = 32
	e := New(opts)
	defer e.Close()
	p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Median("v", "m")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, err = p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("got %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("memory-only wrote files: %v %v", entries, err)
	}
}

func TestReviewQuantileOnlyStableZeroTies(t *testing.T) {
	for _, reversed := range []bool{false, true} {
		var batches []*memory.Batch
		for i := 0; i < 12; i++ {
			b := memory.NewBatch()
			b.Rows = 1
			negative := i%2 == 0
			if reversed {
				negative = !negative
			}
			v := 0.0
			if negative {
				v = math.Copysign(0, -1)
			}
			b.Float64s["v"] = &memory.Float64Vector{Values: []float64{v}}
			batches = append(batches, b)
		}
		src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}), batches...)
		if err != nil {
			t.Fatal(err)
		}
		opts := streamTestOptions()
		opts.Pipeline.Ordering = pipeline.OrderingRelaxed
		opts.Workers = 4
		opts.Pipeline.MaxMorselRows = 1
		e := New(opts)
		defer e.Close()
		p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{Aggregates: []expr.AggregateSpec{expr.Quantile("v", 0, aggregate.QuantileNearest, "a"), expr.Quantile("v", 1, aggregate.QuantileNearest, "z")}}, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		for i := 0; i < 8; i++ {
			r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			a := r.Rows[0].Aggregates
			first, last := uint64(1)<<63, uint64(0)
			if reversed {
				first, last = last, first
			}
			if a[0].Value.Bits != first || a[1].Value.Bits != last {
				t.Fatalf("unstable tie reversed=%v: %+v", reversed, a)
			}
		}
	}
}

func TestReviewConfiguredCopyAndConcurrentExecutions(t *testing.T) {
	src, _ := bytesStatisticsFixture(t)
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 8 << 20
	e := New(opts)
	defer e.Close()
	groups := []string{"k"}
	spec := expr.Quantile("n", .5, aggregate.QuantileMidpoint, "q")
	specs := []expr.AggregateSpec{spec, expr.First("s", "first")}
	p, err := e.PrepareConfigured(context.Background(), src, expr.ConfiguredQuery{GroupBy: groups, Aggregates: specs}, PrepareOptions{MaxConcurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	groups[0] = "missing"
	spec.Probability = 1
	spec.Column = "absent"
	specs[0] = spec
	specs[1] = expr.Count("wrong")
	done := make(chan error, 12)
	for i := 0; i < 12; i++ {
		go func() {
			r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
			if err != nil {
				done <- err
				return
			}
			if len(r.Rows) != 2 {
				done <- fmt.Errorf("groups: %d", len(r.Rows))
				return
			}
			for _, row := range r.Rows {
				want := float64(2)
				if row.Keys[0].Value.Bits == 1 {
					want = 5
				}
				got, ok := row.Aggregates[0].Value.AsFloat64()
				if !ok || got != want || row.Aggregates[0].Alias != "q" || row.Aggregates[1].Kind != expr.AggFirst {
					done <- fmt.Errorf("mutated or mixed state: %+v", row)
					return
				}
			}
			done <- nil
		}()
	}
	for i := 0; i < 12; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
