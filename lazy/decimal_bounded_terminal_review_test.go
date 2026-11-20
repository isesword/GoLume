package lazy

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
	"os"
	"path/filepath"
	"testing"
)

func n34ReviewSetup(t *testing.T) (*Session, *memory.QueryBudget, *memory.QuerySpillStore, DecimalBoundedTerminalOptions) {
	t.Helper()
	session := NewSession(engine.Options{MemoryLimitBytes: 8 << 20, Workers: 1})
	t.Cleanup(func() { _ = session.Close() })
	q, e := memory.NewEngineBudget(8 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = q.Close() })
	store, e := memory.NewQuerySpillStore(memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = store.Close() })
	return session, q, store, DecimalBoundedTerminalOptions{Budget: q, Store: store, TableCapacity: 256}
}
func n34ReviewBudgetLive(t *testing.T, q *memory.QueryBudget) {
	t.Helper()
	if q.Snapshot().Closed {
		t.Fatal("borrowed query prematurely closed")
	}
	r, e := q.Operator("ReviewProbe", memory.MemoryWorkspace).Reserve(context.Background(), 1)
	if e != nil {
		t.Fatal(e)
	}
	r.Release()
}
func TestN34ReviewEOFQueryOwnershipAndProjection(t *testing.T) {
	ctx := context.Background()
	s, q, store, opts := n34ReviewSetup(t)
	path := n34PublicParquet(t, []int64{1, 1, 2, 2}, nil, []string{"1", "3", "5", "7"}, 0)
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	result, e := f.Filter(Col("keep").Eq(Lit(1))).Select("d", "k").CollectDecimalBounded(ctx, "k", "d", opts)
	if e != nil {
		t.Fatal(e)
	}
	defer result.Close(ctx)
	n34ReviewBudgetLive(t, q)
	if q.Used() == 0 {
		t.Fatal("result resources were released at upstream EOF")
	}
	b, ok, e := result.Next(ctx)
	if e != nil || !ok || b.Rows != 2 {
		t.Fatalf("result %v %v %v", b, ok, e)
	}
	if b.Records[0].Sum != decimal128.FromI64(3) || b.Records[1].Sum != decimal128.FromI64(7) {
		t.Fatal("filter dependency lost or values misbound")
	}
	_, ok, e = result.Next(ctx)
	if ok || e != nil {
		t.Fatalf("EOF %v %v", ok, e)
	}
	if *b != (engine.DecimalBoundedOutputBatch{}) {
		t.Fatal("borrowed output survives EOF")
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	n34ReviewBudgetLive(t, q)
	if q.Used() != 0 || store.Used() != 0 {
		t.Fatalf("result leak memory=%d disk=%d", q.Used(), store.Used())
	}
}
func TestN34ReviewDifferentContextBudget(t *testing.T) {
	s, q, store, opts := n34ReviewSetup(t)
	other, e := memory.NewEngineBudget(8 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	path := n34PublicParquet(t, []int64{1}, nil, []string{"7"}, 0)
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	result, e := f.CollectDecimalBounded(memory.WithQueryBudget(context.Background(), other), "k", "d", opts)
	if result != nil {
		_ = result.Close(context.Background())
	}
	if e == nil || result != nil {
		t.Fatal("mismatched context query accepted")
	}
	if q.Used() != 0 || other.Used() != 0 || store.Used() != 0 {
		t.Fatal("budget mismatch leaked")
	}
	n34ReviewBudgetLive(t, q)
	n34ReviewBudgetLive(t, other)
}
func TestN34ReviewLateAverageOverflow(t *testing.T) {
	s, q, store, opts := n34ReviewSetup(t)
	opts.Mode = engine.DecimalBoundedAverageCounts
	opts.AveragePrecision = 1
	opts.AverageRounding = aggregate.DecimalAverageHalfEven
	keys := make([]int64, 257)
	values := make([]string, 257)
	for i := range keys {
		keys[i] = int64(i)
		values[i] = "1"
	}
	values[256] = "10"
	path := n34PublicParquet(t, keys, nil, values, 0)
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	result, e := f.CollectDecimalBounded(context.Background(), "k", "d", opts)
	if result != nil {
		_ = result.Close(context.Background())
	}
	if !errors.Is(e, aggregate.ErrDecimalAverageOverflow) || result != nil {
		t.Fatalf("late error lost result=%v err=%v", result, e)
	}
	n34ReviewBudgetLive(t, q)
	if q.Used() != 0 || store.Used() != 0 {
		t.Fatal("late error leak")
	}
}

type n34ReviewCommitCancel struct {
	context.Context
	directory string
	seen      bool
}

func (c *n34ReviewCommitCancel) Err() error {
	if c.seen {
		return context.Canceled
	}
	entries, e := os.ReadDir(c.directory)
	if e != nil {
		return e
	}
	for _, x := range entries {
		if filepath.Ext(x.Name()) == ".gls" {
			c.seen = true
			return context.Canceled
		}
	}
	return nil
}
func TestN34ReviewCancelAfterSpill(t *testing.T) {
	s, q, store, opts := n34ReviewSetup(t)
	opts.TableCapacity = 1
	path := n34PublicParquet(t, []int64{1, 2, 3}, nil, []string{"1", "2", "3"}, 0)
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	ctx := &n34ReviewCommitCancel{Context: context.Background(), directory: store.Directory()}
	result, e := f.CollectDecimalBounded(ctx, "k", "d", opts)
	if result != nil {
		_ = result.Close(context.Background())
	}
	if !ctx.seen || !errors.Is(e, context.Canceled) || result != nil {
		t.Fatalf("postspill cancel seen=%v err=%v result=%v", ctx.seen, e, result)
	}
	n34ReviewBudgetLive(t, q)
	if q.Used() != 0 || store.Used() != 0 {
		t.Fatal("cancel leaked")
	}
}
func TestN34ReviewDeleteRetryKeepsBorrowedQuery(t *testing.T) {
	ctx := context.Background()
	s, q, store, opts := n34ReviewSetup(t)
	path := n34PublicParquet(t, []int64{1}, nil, []string{"7"}, 0)
	f, e := s.ScanParquet(path)
	if e != nil {
		t.Fatal(e)
	}
	result, e := f.CollectDecimalBounded(ctx, "k", "d", opts)
	if e != nil {
		t.Fatal(e)
	}
	b, ok, e := result.Next(ctx)
	if e != nil || !ok {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(store.Directory())
	if e != nil || len(entries) != 1 {
		t.Fatalf("files=%d err=%v", len(entries), e)
	}
	output := filepath.Join(store.Directory(), entries[0].Name())
	backup := filepath.Join(t.TempDir(), "saved")
	if e = os.Rename(output, backup); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(output, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(output, "block"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e == nil {
		t.Fatal("failed unlink not surfaced")
	}
	n34ReviewBudgetLive(t, q)
	if *b != (engine.DecimalBoundedOutputBatch{}) {
		t.Fatal("failed close retains borrowed batch")
	}
	if q.Used() == 0 || store.Used() == 0 {
		t.Fatal("failed close lost owner")
	}
	if e = os.RemoveAll(output); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(backup, output); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if e = result.Close(ctx); e != nil {
		t.Fatal(e)
	}
	n34ReviewBudgetLive(t, q)
	if q.Used() != 0 || store.Used() != 0 {
		t.Fatal("retry leak")
	}
}

func TestN34ReviewOriginalPartitionAndCloneAdmission(t *testing.T) {
	ctx := context.Background()
	keys := make([]int64, 65)
	values := make([]string, 65)
	for i := range keys {
		keys[i] = int64(i)
		values[i] = "1"
	}
	path := n34PublicParquetRows(t, keys, nil, values, 0, 1)
	for _, clone := range []bool{false, true} {
		name := "65-original-rowgroups"
		if clone {
			name = "clone-hides-64-rowgroups"
		}
		t.Run(name, func(t *testing.T) {
			s, q, store, opts := n34ReviewSetup(t)
			src, e := parquetsource.OpenFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var input source.Source = src
			if clone {
				input, _, e = src.ApplyPushdown(ctx, source.PushdownRequest{Projection: []string{"k", "d"}, Predicate: Col("k").Eq(Lit(0))})
				if e != nil {
					t.Fatal(e)
				}
				stats, e := input.Stats(ctx)
				if e != nil || stats.Partitions != 1 {
					t.Fatalf("fixture prune partitions=%d err=%v", stats.Partitions, e)
				}
			}
			result, e := s.FromSource(input).CollectDecimalBounded(ctx, "k", "d", opts)
			if result != nil {
				_ = result.Close(ctx)
			}
			if e == nil || result != nil {
				t.Fatalf("unsafe original metadata admitted clone=%v err=%v", clone, e)
			}
			if q.Used() != 0 || store.Used() != 0 {
				t.Fatal("admission failure leaked")
			}
		})
	}
}
