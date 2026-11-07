package engine

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"testing"
)

func TestDistinctReviewValidityOnlyOpaqueCount(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 1, 2}}
	b.Opaque["o"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeStruct, Validity: memory.Bitmap{5}}
	src := &distinctReviewRawSource{batch: b, schema: memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "o", Type: memory.TypeStruct, Nullable: true})}
	var err error
	if err != nil {
		t.Fatal(err)
	}
	e := New(streamTestOptions())
	defer e.Close()
	r, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), expr.CountColumn("o", "count")}}, ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 1 || r.Rows[0].Aggregates[0].Value.Bits != 2 || r.Rows[0].Aggregates[1].Value.Bits != 2 {
		t.Fatalf("got %+v", r.Rows)
	}
}

func TestDistinctReviewFilteredSourceOrdinals(t *testing.T) {
	for _, sorted := range []bool{false, true} {
		src := statisticsReviewSource(t, sorted)
		q := statisticsReviewQuery()
		q.Aggregates = append(q.Aggregates, expr.NUnique("v", "unique"))
		opts := streamTestOptions()
		opts.Workers = 4
		opts.Pipeline.Workers = 4
		opts.Pipeline.MaxMorselRows = 2
		e := New(opts)
		r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
		e.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range r.Rows {
			key := int(row.Keys[0].Value.Bits)
			first, last, count := -1, -1, 0
			for x := 0; x < 40; x++ {
				k := x % 2
				if sorted {
					k = x / 20
				}
				if k == key && x/8 != 2 && x%8%3 != 0 {
					if first < 0 {
						first = x
					}
					last = x
					count++
				}
			}
			a := row.Aggregates
			if a[6].Value.Bits != uint64(100+first) || a[7].Value.Bits != uint64(100+last) || a[8].Value.Bits != uint64(count) || a[4].Value.Bits != uint64(count) {
				t.Fatalf("wrong ordinals/count %+v", row)
			}
			if a[0].Value.Valid != (last%8 != 7) || a[3].Value.Valid != (first%8 != 7) {
				t.Fatalf("endpoint validity %+v", row)
			}
		}
	}
}

// Raw source deliberately bypasses in-memory fixture validation to exercise the
// operator boundary and validity-only opaque vectors supplied by Arrow sources.
type distinctReviewRawSource struct {
	batch  *memory.Batch
	schema memory.Schema
}

func (s *distinctReviewRawSource) Schema() memory.Schema { return s.schema }
func (s *distinctReviewRawSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.batch.Rows), Partitions: 1}, nil
}
func (s *distinctReviewRawSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.batch.Rows)}}, nil
}
func (s *distinctReviewRawSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	return &distinctReviewRawStream{batch: s.batch}, nil
}
func (s *distinctReviewRawSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, _ []source.Partition) (source.BatchStream, error) {
	return s.NewBatchStream(ctx, o)
}

type distinctReviewRawStream struct {
	batch *memory.Batch
	done  bool
}

func (s *distinctReviewRawStream) Next(_ context.Context, dst *memory.Batch) (bool, error) {
	if s.done {
		return false, nil
	}
	s.done = true
	*dst = *s.batch
	return true, nil
}
func (s *distinctReviewRawStream) Close() error { return nil }

func TestDistinctReviewMalformedNumericLane(t *testing.T) {
	for _, stats := range []bool{false, true} {
		b := memory.NewBatch()
		b.Rows = 3
		b.Int64s["v"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
		b.Int64s["bad"] = &memory.Int64Vector{Values: []int64{1}}
		src := &distinctReviewRawSource{batch: b, schema: memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "bad", Type: memory.TypeInt64})}
		a := expr.Sum("bad", "bad_sum")
		if stats {
			a = expr.First("bad", "bad_first")
		}
		e := New(streamTestOptions())
		_, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), a}}, ExecuteOptions{Materialize: true})
		e.Close()
		if err == nil {
			t.Fatal("malformed lane accepted")
		}
	}
}

func TestDistinctReviewMemoryOnlyNeverSpills(t *testing.T) {
	opts := streamTestOptions()
	opts.ExecutionPolicy = ExecutionMemoryOnly
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
	e := New(opts)
	defer e.Close()
	src := &generatedStreamSource{rows: 8192, groups: 8192}
	_, err := e.ExecuteTyped(context.Background(), src, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("key", "u")}}, ExecuteOptions{Materialize: true})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("want budget error without spilling, got %v", err)
	}
	entries, readErr := os.ReadDir(opts.Spill.Directory)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("spill directory changed: %v %v", entries, readErr)
	}
	if src.opened.Load() != src.closed.Load() {
		t.Fatal("source leaked")
	}
}

func TestDistinctReviewCancelledInputReleasesBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &generatedStreamSource{rows: 8192, groups: 100, cancel: cancel, cancelAt: 128}
	opts := streamTestOptions()
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
	e := New(opts)
	defer e.Close()
	p, err := e.Prepare(ctx, src, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("key", "u"), expr.First("value", "f")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewResidentAggregateStream(memory.WithQueryBudget(ctx, budget), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	_, err = stream.Next(ctx, out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if budget.Used() != 0 {
		t.Fatalf("retained %d", budget.Used())
	}
	if src.opened.Load() != 1 || src.closed.Load() != 1 {
		t.Fatalf("source opens/closes %d/%d", src.opened.Load(), src.closed.Load())
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDistinctReviewMalformedBitmapEvenWhenFiltered(t *testing.T) {
	for _, agg := range []expr.Aggregate{expr.Sum("bad", "s"), expr.First("bad", "f")} {
		b := memory.NewBatch()
		b.Rows = 65
		b.Int64s["v"] = &memory.Int64Vector{Values: make([]int64, 65)}
		b.Int64s["bad"] = &memory.Int64Vector{Values: make([]int64, 65), Validity: memory.Bitmap{0}}
		src := &distinctReviewRawSource{batch: b, schema: memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64}, memory.Field{Name: "bad", Type: memory.TypeInt64, Nullable: true})}
		e := New(streamTestOptions())
		_, err := e.ExecuteTyped(context.Background(), src, expr.Query{Filter: expr.Bool(false), Aggregates: []expr.Aggregate{expr.NUnique("v", "u"), agg}}, ExecuteOptions{Materialize: true})
		e.Close()
		if err == nil {
			t.Fatal("malformed bitmap accepted even though rows filtered")
		}
	}
}

func TestDistinctReviewExplicitCloseContract(t *testing.T) {
	e := New(streamTestOptions())
	defer e.Close()
	p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: 3, groups: 2}, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("key", "u")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewResidentAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Next(context.Background(), memory.NewBatch()); !errors.Is(err, source.ErrBatchStreamClosed) {
		t.Fatalf("explicit-close Next returned %v", err)
	}
}

func TestDistinctReviewEndAndNilContextContract(t *testing.T) {
	for _, mode := range []string{"close_after_last", "natural_eof", "nil_context"} {
		t.Run(mode, func(t *testing.T) {
			e := New(streamTestOptions())
			defer e.Close()
			p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: 3, groups: 2}, expr.Query{Aggregates: []expr.Aggregate{expr.NUnique("key", "u")}}, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			budget, err := e.BeginQueryMemory()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			stream, err := p.NewResidentAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			if mode == "nil_context" {
				if _, err = stream.Next(nil, out); err == nil {
					t.Fatal("nil context accepted")
				}
				if budget.Used() != 0 {
					t.Fatal("nil context leaked")
				}
				return
			}
			if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("first %v %v", ok, err)
			}
			if mode == "close_after_last" {
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err = stream.Next(context.Background(), out); !errors.Is(err, source.ErrBatchStreamClosed) {
					t.Fatalf("close after final row: %v", err)
				}
			} else {
				for repeat := 0; repeat < 2; repeat++ {
					if ok, err := stream.Next(context.Background(), out); ok || err != nil {
						t.Fatalf("EOF %v %v", ok, err)
					}
				}
			}
		})
	}
}
