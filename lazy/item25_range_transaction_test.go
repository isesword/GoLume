package lazy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type item25RangeRow struct {
	ts, value int64
	valid     bool
}
type item25RangeSource struct {
	rows                  []item25RangeRow
	batchRows             int
	opened, closed, nexts int
	live                  int64
	expectedQuery         *memory.QueryBudget
	dir                   string
	stop                  context.CancelFunc
	cancelAt              int
	stagedBytes           int64
	stagedCount           int
}

func (s *item25RangeSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true})
}
func (s *item25RangeSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1}, nil
}
func (s *item25RangeSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *item25RangeSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *item25RangeSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "Scan", Class: source.StreamPipeline}
}
func (s *item25RangeSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opened++
	return &item25RangeStream{s: s, m: o.MemoryManager}, nil
}

type item25RangeStream struct {
	s          *item25RangeSource
	m          memory.Manager
	lease      memory.Reservation
	ts, values []int64
	bits       memory.Bitmap
	at         int
	closed     bool
}

func (s *item25RangeStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.nexts++
	if s.s.nexts == 4 {
		entries, err := os.ReadDir(s.s.dir)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".target.") {
				info, err := entry.Info()
				if err != nil {
					return false, err
				}
				s.s.stagedCount++
				s.s.stagedBytes += info.Size()
			}
		}
	}
	if s.s.cancelAt == s.s.nexts {
		s.s.stop()
	}

	if s.s.expectedQuery != nil && memory.QueryBudgetFromContext(ctx) != s.s.expectedQuery {
		return false, fmt.Errorf("source received different query budget")
	}
	b.ClearColumns()
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.at == len(s.s.rows) {
		return false, nil
	}
	n := s.s.batchRows
	if n < 1 {
		n = 1
	}
	if s.lease == nil {
		charge := int64(4096 + 16*n + 8*memory.WordsFor(n))
		var e error
		s.lease, e = s.m.Reserve(ctx, charge)
		if e != nil {
			return false, e
		}
		s.s.live = charge
		s.ts = make([]int64, n)
		s.values = make([]int64, n)
		s.bits = make(memory.Bitmap, memory.WordsFor(n))
	}
	clear(s.bits)
	n = min(n, len(s.s.rows)-s.at)
	for i := 0; i < n; i++ {
		r := s.s.rows[s.at+i]
		s.ts[i] = r.ts
		s.values[i] = r.value
		if r.valid {
			memory.Set(s.bits, i)
		}
	}
	s.at += n
	b.Rows = n
	b.Int64s["ts"] = &memory.Int64Vector{Values: s.ts[:n]}
	b.Int64s["value"] = &memory.Int64Vector{Values: s.values[:n], Validity: s.bits[:memory.WordsFor(n)]}
	return true, nil
}
func (s *item25RangeStream) Close() error {
	if !s.closed {
		s.closed = true
		s.s.closed++
		for i := range s.values {
			s.values[i] = math.MinInt64
			s.ts[i] = math.MaxInt64
		}
		clear(s.bits)
		if s.lease != nil {
			s.lease.Release()
		}
		s.s.live = 0
	}
	return nil
}

func item25RangeReadAverage(t *testing.T, path, kind string) {
	t.Helper()
	var got []float64
	read := func(r arrow.RecordBatch) {
		idx := r.Schema().FieldIndices("avg")
		if len(idx) != 1 {
			t.Fatal("missing avg")
		}
		a := r.Column(idx[0]).(*array.Float64)
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				t.Fatal("NULL avg")
			}
			got = append(got, a.Value(i))
		}
	}
	if kind == "ipc" {
		f, e := os.Open(path)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		r, e := ipc.NewFileReader(f)
		if e != nil {
			t.Fatal(e)
		}
		defer r.Close()
		for i := 0; i < r.NumRecords(); i++ {
			b, e := r.RecordBatchAt(i)
			if e != nil {
				t.Fatal(e)
			}
			read(b)
			b.Release()
		}
	} else {
		f, e := file.OpenParquetFile(path, false)
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		r, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
		if e != nil {
			t.Fatal(e)
		}
		tab, e := r.ReadTable(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		defer tab.Release()
		rr := array.NewTableReader(tab, 1)
		defer rr.Release()
		for rr.Next() {
			read(rr.RecordBatch())
		}
		if rr.Err() != nil {
			t.Fatal(rr.Err())
		}
	}
	want := []float64{1, 2, math.Ldexp(1, 62), math.Ldexp(1, 62)}
	if len(got) != len(want) {
		t.Fatal("rows", got)
	}
	for i := range got {
		if math.Float64bits(got[i]) != math.Float64bits(want[i]) {
			t.Fatalf("avg%d got%g want%g", i, got[i], want[i])
		}
	}
}
func TestItem25RangeSinkTransactions(t *testing.T) {
	for _, kind := range []string{"ipc", "parquet"} {
		for _, mode := range []string{"lateorder", "overflow", "hidden_sum_overflow", "running_cancel", "avg_only"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				dir := t.TempDir()
				target := filepath.Join(dir, "target."+kind)
				original := []byte("original target must survive failed transaction")
				if e := os.WriteFile(target, original, 0600); e != nil {
					t.Fatal(e)
				}
				root := memory.NewEngineBudget(8 << 20)
				q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 8 << 20})
				if e != nil {
					t.Fatal(e)
				}
				defer q.Close()
				base, stop := context.WithCancel(context.Background())
				defer stop()
				ctx := memory.WithQueryBudget(base, q)
				opts := engine.DefaultOptions()
				opts.Workers = 1
				opts.Pipeline.Workers = 1
				opts.Pipeline.MinMorselRows = 1
				opts.Pipeline.MaxMorselRows = 1
				opts.MemoryLimitBytes = 8 << 20
				opts.ExecutionPolicy = engine.ExecutionMemoryOnly
				opts.PreparedCacheBytes = 0
				session := NewSession(opts)
				defer session.Close()
				rows := []item25RangeRow{{0, 1, true}, {1, 2, true}, {2, math.MaxInt64, true}, {2, 1, true}}
				if mode == "lateorder" {
					rows[3].ts = 1
				}
				if mode == "running_cancel" {
					rows[2].value = 3
					rows[3] = item25RangeRow{3, 4, true}
				}
				src := &item25RangeSource{rows: rows, batchRows: 1, expectedQuery: q, dir: dir, stop: stop}
				if mode == "running_cancel" {
					src.cancelAt = 4
				}
				spec := TimestampRangeSpec{OrderBy: "ts", Column: "value", Frame: TimestampRangeFrame{Kind: WindowFrameRange}, CountRowsAlias: "rows", CountAlias: "count", SumAlias: "sum", AvgAlias: "avg"}
				if mode == "avg_only" {
					spec.SumAlias = ""
				}
				f := session.FromSource(src).WithTimestampRange(spec)
				if mode == "hidden_sum_overflow" {
					f = f.Select("ts")
				}
				if kind == "ipc" {
					_, e = f.SinkIPC(ctx, target, IPCSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
				} else {
					_, e = f.SinkParquet(ctx, target, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}, RowGroupRows: 1})
				}
				// Inspect real cleanup before any fatal assertion; never manually delete staging.
				data, re := os.ReadFile(target)
				entries, de := os.ReadDir(dir)
				used := q.Used()
				snap := q.Snapshot()
				if re != nil || de != nil {
					t.Fatalf("read cleanup %v %v", re, de)
				}
				if used != 0 || src.opened != 1 || src.closed != 1 {
					t.Fatalf("lifecycle used%d opens%d closes%d", used, src.opened, src.closed)
				}
				if len(entries) != 1 || entries[0].Name() != filepath.Base(target) {
					t.Fatalf("staging leftovers %v", entries)
				}
				if src.nexts < 4 || src.stagedCount == 0 || src.stagedBytes == 0 {
					t.Fatalf("no running sink progress nexts%d files%d bytes%d", src.nexts, src.stagedCount, src.stagedBytes)
				}
				if mode == "avg_only" {
					if e != nil {
						t.Fatal(e)
					}
					item25RangeReadAverage(t, target, kind)
				} else {
					if e == nil {
						t.Fatal("failed transaction published")
					}
					if !bytes.Equal(data, original) {
						t.Fatal("old target overwritten")
					}
					if mode == "running_cancel" {
						if !errors.Is(e, context.Canceled) {
							t.Fatal(e)
						}
					} else {
						fragment := "complete-window SUM overflows Int64"
						if mode == "lateorder" {
							fragment = "nondecreasing"
						}
						if !strings.Contains(e.Error(), fragment) {
							t.Fatal("wrong late error", e)
						}
					}
				}
				lease, re := q.Operator("CallerAfterSink", memory.MemoryWorkspace).Reserve(context.Background(), 1)
				if re != nil {
					t.Fatal("caller query closed", re)
				}
				lease.Release()
				t.Logf("nexts%d stagingfiles%d bytesAtFourthNext%d sharedPeak%d err%v", src.nexts, src.stagedCount, src.stagedBytes, snap.PeakBytes, e)
			})
		}
	}
}
