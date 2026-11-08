package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"os"
	"path/filepath"
	"testing"
)

func TestSpillAggregateFitsMemoryWithoutFilesystem(t *testing.T) {
	for _, rows := range []int{0, 4096} {
		for _, sink := range []string{"stream", "summary", "materialized", "typed"} {
			t.Run(fmt.Sprintf("rows=%d/%s", rows, sink), func(t *testing.T) {
				opts := streamTestOptions()
				opts.ExecutionPolicy = ExecutionBounded
				// A regular file cannot be opened as a spill directory. Fitting queries
				// must not attempt even directory creation, independently of counters.
				path := filepath.Join(t.TempDir(), "not-a-directory")
				if err := os.WriteFile(path, []byte("sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
				opts.Spill = SpillOptions{Directory: path, MaxBytes: 1 << 20}
				e := New(opts)
				defer e.Close()
				src := &generatedStreamSource{rows: rows, groups: 32}
				q := streamTestQuery()
				q.Hints.ForceStrategy = ""
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				if p.Plan().Strategy.Kind != planner.StrategySpillHash {
					t.Fatal("bypassed spill-capable strategy")
				}
				for repeat := 0; repeat < 2; repeat++ {
					budget, err := e.BeginQueryMemory()
					if err != nil {
						t.Fatal(err)
					}
					ctx := memory.WithQueryBudget(context.Background(), budget)
					groups := 0
					if sink == "stream" {
						stream, err := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 7)
						if err != nil {
							t.Fatal(err)
						}
						s := stream.(*spillableAggregateStream)
						out := memory.NewBatch()
						seen := map[int32]bool{}
						for {
							ok, err := s.Next(context.Background(), out)
							if err != nil {
								t.Fatal(err)
							}
							if !ok {
								break
							}
							if out.Rows > 7 {
								t.Fatalf("unbounded output %d", out.Rows)
							}
							for i, k := range out.Int32s["key"].Values {
								if seen[k] || out.Int64s["sum"].Values[i] != 128 || out.Uint64s["n"].Values[i] != 128 {
									t.Fatalf("invalid group %d", k)
								}
								seen[k] = true
								groups++
							}
						}
						if stats := s.Stats(); stats.SpillRuns != 0 || stats.SpillBytes != 0 || stats.Groups != groups || stats.SelectedRows != rows {
							t.Fatalf("stats %+v", stats)
						}
						if ok, err := s.Next(ctx, out); ok || err != nil {
							t.Fatalf("EOF retry: %v %v", ok, err)
						}
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
					} else if sink == "typed" {
						result, err := p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true})
						if err != nil {
							t.Fatal(err)
						}
						groups = len(result.Rows)
					} else {
						result, err := p.Execute(ctx, ExecuteOptions{Materialize: sink == "materialized"})
						if err != nil {
							t.Fatal(err)
						}
						groups = result.GroupCount
						if result.SelectedRows != rows || result.Checksum != int64(rows) {
							t.Fatalf("result %+v", result)
						}
						if sink == "materialized" && len(result.Rows) != groups {
							t.Fatal("missing result rows")
						}
					}
					want := 32
					if rows == 0 {
						want = 0
					}
					if groups != want {
						t.Fatalf("groups %d want %d", groups, want)
					}
					if budget.Used() != 0 || budget.Snapshot().PeakBytes > budget.Limit() {
						t.Fatalf("budget %+v", budget.Snapshot())
					}
					budget.Close()
				}
				if src.opened.Load() != 2 || src.closed.Load() != 2 {
					t.Fatal("source lifecycle")
				}
			})
		}
	}
}

func TestSpillAggregateResidentOutputCanSpillSuffix(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 1024, 256)
	s.outputRows = 17
	ctx := memory.WithQueryBudget(context.Background(), b)
	out := memory.NewBatch()
	seen := map[int32]bool{}
	check := func() {
		for i, k := range out.Int32s["key"].Values {
			if seen[k] || out.Int64s["sum"].Values[i] != 4 || out.Uint64s["n"].Values[i] != 4 {
				t.Fatalf("duplicate or corrupt key %d", k)
			}
			seen[k] = true
		}
	}
	ok, err := s.Next(ctx, out)
	if err != nil || !ok {
		t.Fatalf("first output %v %v", ok, err)
	}
	check()
	if s.Stats().SpillRuns != 0 || s.SpillableBytes() == 0 {
		t.Fatal("expected resident output")
	}
	// Another operator may reclaim the table between output batches. Previously
	// emitted groups must never appear in the persisted suffix.
	released, err := s.state.RequestHardSpill(ctx, s.SpillableBytes())
	if err != nil || released == 0 {
		t.Fatalf("spill %d %v", released, err)
	}
	for {
		ok, err = s.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		check()
	}
	if len(seen) != 256 || s.Stats().SpillRuns == 0 {
		t.Fatalf("groups=%d stats=%+v", len(seen), s.Stats())
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestSpillAggregateResidentOutputAdmissionFallback(t *testing.T) {
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.ExecutionPolicy = ExecutionBounded
	dir := t.TempDir()
	opts.Spill = SpillOptions{Directory: dir, MaxBytes: 8 << 20}
	e := New(opts)
	defer e.Close()
	q := streamTestQuery()
	q.Hints.ForceStrategy = ""
	q.Aggregates = nil
	for i := 0; i < 64; i++ {
		q.Aggregates = append(q.Aggregates, expr.Sum("value", fmt.Sprintf("s%d", i)))
	}
	p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: 4096, groups: 1024}, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	b, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := memory.WithQueryBudget(context.Background(), b)
	stream, err := p.NewSpillableAggregateStream(ctx, p.resultSchema(), 128)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*spillableAggregateStream)
	defer s.Close()
	if err := s.prepareInput(ctx); err != nil {
		t.Fatal(err)
	}
	needed, err := aggregateOutputBytes(s.schema, s.outputRows, 0)
	if err != nil {
		t.Fatal(err)
	}
	retained := s.SpillableBytes()
	holdBytes := b.Limit() - b.Used() - (needed - retained/3)
	hold, err := b.Operator("CompetingOutput", memory.MemoryRetainedState).ReserveGrowth(ctx, holdBytes)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	count := 0
	for {
		ok, err := s.Next(ctx, out)
		if err != nil {
			t.Fatalf("output=%d retained=%d hold=%d: %v", needed, retained, holdBytes, err)
		}
		if !ok {
			break
		}
		count += out.Rows
		for _, v := range out.Int64s["s0"].Values {
			if v != 4 {
				t.Fatalf("sum %d", v)
			}
		}
	}
	hold.Release()
	if count != 1024 || s.Stats().SpillRuns == 0 {
		t.Fatalf("groups=%d stats=%+v", count, s.Stats())
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestSpillAggregateResidentTerminalCleanup(t *testing.T) {
	for _, mode := range []string{"close-before-next", "close-after-next", "cancel-before-next", "cancel-after-next", "sink-error"} {
		t.Run(mode, func(t *testing.T) {
			s, b, dir := aggregateMetadataStream(t, 1<<20, 128, 32)
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), b))
			defer cancel()
			out := memory.NewBatch()
			if mode == "close-after-next" || mode == "cancel-after-next" {
				if ok, err := s.Next(ctx, out); !ok || err != nil {
					t.Fatal(ok, err)
				}
			}
			if mode == "cancel-before-next" || mode == "cancel-after-next" {
				cancel()
				for i := 0; i < 2; i++ {
					if ok, err := s.Next(ctx, out); ok || !errors.Is(err, context.Canceled) {
						t.Fatal(ok, err)
					}
				}
			}
			if mode == "sink-error" {
				sentinel := errors.New("sink failed")
				_, err := s.consumeRecords(ctx, func(aggregateRecord) error { return sentinel })
				if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if out.Rows != 0 {
				t.Fatal("borrowed output remains live")
			}
			assertAggregateMetadataClean(t, b, dir)
		})
	}
}

func TestSpillAggregateResidentRecordAdmissionRetriesOnlySuffix(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 1024, 256)
	s.outputRows = 32
	ctx := memory.WithQueryBudget(context.Background(), b)
	seen := map[uint64]bool{}
	failed := false
	calls := 0
	stats, err := s.consumeRecords(ctx, func(record aggregateRecord) error {
		calls++
		if len(seen) == 37 && !failed {
			failed = true
			return memory.ErrBudgetExceeded
		}
		if seen[record.a] || record.state.Sum != 4 || record.state.CountRows != 4 {
			return fmt.Errorf("duplicate or corrupt key %d", record.a)
		}
		seen[record.a] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !failed || calls != 257 || len(seen) != 256 || stats.Groups != 256 || stats.SpillRuns == 0 {
		t.Fatalf("calls=%d seen=%d stats=%+v", calls, len(seen), stats)
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestSpillAggregateResidentSpillFailureRetainsSuffix(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 256, 64)
	s.outputRows = 8
	ctx := memory.WithQueryBudget(context.Background(), b)
	out := memory.NewBatch()
	seen := map[int32]bool{}
	take := func() {
		for _, key := range out.Int32s["key"].Values {
			if seen[key] {
				t.Fatal("duplicate key")
			}
			seen[key] = true
		}
	}
	if ok, err := s.Next(ctx, out); !ok || err != nil {
		t.Fatal(ok, err)
	}
	take()
	cursor, emitted := s.outputCursor, s.outputEmitted
	// Fail before admission; a spill error must not advance the authoritative
	// cursor, drop state, or publish any output references.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Spill(canceled, memory.SpillRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.table == nil || s.outputCursor != cursor || s.outputEmitted != emitted || len(s.outputRuns) != 0 {
		t.Fatal("failed spill changed resident suffix")
	}
	if _, err := s.state.RequestHardSpill(ctx, s.SpillableBytes()); err != nil {
		t.Fatal(err)
	}
	for {
		ok, err := s.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		take()
	}
	if len(seen) != 64 {
		t.Fatal(len(seen))
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestTypedCollectorFailedAdmissionKeepsGrownRows(t *testing.T) {
	s, _, _ := aggregateMetadataStream(t, 1<<20, 0, 1)
	manager := memory.NewBudgetManager(1 << 20)
	failing := &failNthMetadataReview{Manager: manager, fail: 2}
	result := TypedResult{}
	collector := typedResultCollector{query: s.query, result: &result, account: resultCollector{budget: failing}}
	defer collector.account.close()
	record := aggregateRecord{a: 1, validMask: 1}
	if err := collector.append(context.Background(), record); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	lease := collector.account.rowsLease
	if len(result.Rows) != 0 || cap(result.Rows) != 16 || lease == nil {
		t.Fatal("successful backing growth lost after row admission failed")
	}
	if err := collector.append(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || collector.account.rowsLease != lease {
		t.Fatal("retry replaced already admitted backing")
	}
	collector.account.close()
	if manager.Used() != 0 {
		t.Fatal("collector leaked memory")
	}
}
