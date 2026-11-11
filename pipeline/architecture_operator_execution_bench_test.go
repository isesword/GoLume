package pipeline_test

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sync"
	"testing"
)

// Sequential query executions reuse immutable inputs and compiled operator
// plans. Leaf rebinding is completed before consumer goroutines start, and the
// prior query is fully joined and closed before rebinding. Do not RunParallel
// on this object; a concurrent-query benchmark needs one object per worker.
type architecturePreparedExecution struct {
	src    architectureUnknownSource
	parts  []source.Partition
	leaves [2]*architectureBranchSource
	plans  [2]source.Source
	engine *memory.EngineBudget
	ro     source.ReadOptions
	opts   pipeline.Options
}

func architecturePrepareExecution(kind string) (*architecturePreparedExecution, error) {
	const rows = 65536
	input, schema := architectureFixture(kind, rows)
	base, err := inmemory.New(schema, input)
	if err != nil {
		return nil, err
	}
	p := &architecturePreparedExecution{src: architectureUnknownSource{base}, engine: memory.NewEngineBudget(1 << 30), ro: source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: rows, BatchPayloadTargetBytes: 64 << 10}, opts: pipeline.Options{Workers: 1, TargetMorselBytes: 64 << 10, MaxMorselBytes: 256 << 10, MaxInFlightBytes: 1 << 20, MinMorselRows: 256, MaxMorselRows: rows, Ordering: pipeline.OrderingStable}}
	p.parts, err = p.src.Partitions(context.Background())
	if err != nil {
		return nil, err
	}
	for branch := 0; branch < 2; branch++ {
		p.leaves[branch] = &architectureBranchSource{Source: base}
		predicate := expr.GT(expr.Col("id"), expr.I64(-1))
		if branch == 1 {
			predicate = expr.LT(expr.Col("id"), expr.I64(rows/2))
		}
		filtered, e := physical.NewFilterSource(p.leaves[branch], predicate)
		if e != nil {
			return nil, e
		}
		projected, e := physical.NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Add(expr.Col("id"), expr.I64(1)), "y"), expr.Named(expr.Add(expr.Col("id"), expr.I64(2)), "__tmp")}, true)
		if e != nil {
			return nil, e
		}
		p.plans[branch], e = physical.NewDropColumnsSource(projected, []string{"__tmp"})
		if e != nil {
			return nil, e
		}
	}
	return p, nil
}

type architectureExecutionResult struct {
	rows, items int
	checksum    uint64
	err         error
}

func (p *architecturePreparedExecution) run() (uint64, error) {
	q, err := p.engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
	if err != nil {
		return 0, err
	}
	defer q.Close()
	ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), q))
	defer cancel()
	streams, err := pipeline.OpenFanOut(ctx, p.src, p.ro, p.parts, q, p.opts, 2, nil)
	if err != nil {
		return 0, err
	}
	for i, st := range streams {
		p.leaves[i].stream = st
		p.leaves[i].opened.Store(false)
	}
	results := [2]architectureExecutionResult{}
	var wg sync.WaitGroup
	for branch := 0; branch < 2; branch++ {
		wg.Add(1)
		go func(branch int) {
			defer wg.Done()
			defer streams[branch].Close()
			st, e := source.OpenBatchStream(ctx, p.plans[branch], p.ro)
			if e != nil {
				results[branch].err = e
				cancel()
				return
			}
			defer st.Close()
			out := memory.NewBatch()
			defer out.ClearColumns()
			for {
				ok, e := st.Next(ctx, out)
				if e != nil {
					results[branch].err = e
					cancel()
					return
				}
				if !ok {
					return
				}
				// Low-cost sink: touch every computed scalar output exactly once. No
				// per-row expected-value construction, bytes.Equal, nested oracle or maps
				// inside this loop. Full payload/order checks live in correctness tests.
				for _, v := range out.Int64s["y"].Values[:out.Rows] {
					results[branch].checksum += uint64(v)
				}
				results[branch].rows += out.Rows
				results[branch].items++
				out.ClearColumns()
			}
		}(branch)
	}
	wg.Wait()
	var sum uint64
	for branch, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		n := 65536
		if branch == 1 {
			n /= 2
		}
		want := uint64(n) * uint64(n+1) / 2
		if r.rows != n || r.checksum != want {
			return 0, fmt.Errorf("branch %d rows/checksum=%d/%d want=%d/%d", branch, r.rows, r.checksum, n, want)
		}
		sum += r.checksum
		p.leaves[branch].stream = nil
	}
	snap := q.Snapshot()
	if snap.UsedBytes != 0 || p.engine.Used() != 0 || snap.PeakBytes > 8<<20 {
		return 0, fmt.Errorf("query leak/bound used=%d engine=%d peak=%d", snap.UsedBytes, p.engine.Used(), snap.PeakBytes)
	}
	return sum, nil
}

var architectureExecutionSink uint64

// Main execution-cost entry point. Fixture construction, immutable source
// creation, partition discovery and predicate/project compilation are excluded.
// Each iteration includes query budget, actual OpenFanOut, independent operator
// streams/workspaces, both consumers, cheap checksum, Close and budget cleanup.
// -cpu=1 is the primary configuration; -cpu=4 is an optional separate scheduling
// case, not a change to pipeline workers, limits, data or query concurrency.
func BenchmarkArchitecturePhysicalFanOutExecution(b *testing.B) {
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		b.Run(kind, func(b *testing.B) {
			p, err := architecturePrepareExecution(kind)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sum uint64
			for i := 0; i < b.N; i++ {
				v, err := p.run()
				if err != nil {
					b.Fatal(err)
				}
				sum += v
			}
			b.StopTimer()
			architectureExecutionSink = sum
		})
	}
}

// Correctness-only smoke verifies that the prepared leaf/plan is reusable and
// opens fresh stream-local state. It does not replace full payload-oracle tests.
func TestArchitecturePreparedExecutionReusable(t *testing.T) {
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		t.Run(kind, func(t *testing.T) {
			p, err := architecturePrepareExecution(kind)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if _, err := p.run(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
