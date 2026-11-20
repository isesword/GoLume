package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"sync/atomic"
)

type n44OracleRow struct {
	ts, value int64
	valid     bool
}

func n44OracleSession() *Session {
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.Pipeline.MinMorselRows = 1
	o.Pipeline.MaxMorselRows = 2
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.MemoryLimitBytes = 16 << 20
	o.PreparedCacheBytes = 0
	return NewSession(o)
}
func n44OracleInput(t *testing.T, rows []n44OracleRow, chunk int) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Encoding: memory.EncodingPlain}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true, Encoding: memory.EncodingPlain})
	var pages []*memory.Batch
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		b := memory.NewBatch()
		b.Rows = end - start
		ts := &memory.Int64Vector{Values: make([]int64, b.Rows)}
		v := &memory.Int64Vector{Values: make([]int64, b.Rows), Validity: make(memory.Bitmap, memory.WordsFor(b.Rows))}
		for i, row := range rows[start:end] {
			ts.Values[i] = row.ts
			v.Values[i] = row.value
			if row.valid {
				memory.Set(v.Validity, i)
			}
		}
		b.Int64s["ts"] = ts
		b.Int64s["v"] = v
		pages = append(pages, b)
	}
	src, err := inmemory.New(schema, pages...)
	if err != nil {
		t.Fatal(err)
	}
	return src.WithOrdering(source.OrderingColumn{Name: "ts"})
}
func n44OracleExpected(rows []n44OracleRow, width int64) [][]any {
	out := make([][]any, len(rows))
	for i, row := range rows {
		total := new(big.Int)
		var count, all int64
		hi := big.NewInt(row.ts)
		lo := new(big.Int).Sub(new(big.Int).Set(hi), big.NewInt(width))
		for _, candidate := range rows {
			ts := big.NewInt(candidate.ts)
			if ts.Cmp(lo) < 0 || ts.Cmp(hi) > 0 {
				continue
			}
			all++
			if candidate.valid {
				count++
				total.Add(total, big.NewInt(candidate.value))
			}
		}
		var value, sum, avg any
		if row.valid {
			value = row.value
		}
		if count > 0 {
			if !total.IsInt64() {
				panic("oracle fixture SUM overflow")
			}
			sum = total.Int64()
			avg, _ = new(big.Rat).SetFrac(total, big.NewInt(count)).Float64()
		}
		out[i] = []any{row.ts, value, all, count, sum, avg}
	}
	return out
}
func n44OracleRead(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch v := a.(type) {
	case *array.Timestamp:
		return int64(v.Value(i))
	case *array.Int64:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.Uint64:
		return v.Value(i)
	default:
		panic(fmt.Sprintf("unexpected output %T", a))
	}
}
func n44OracleCheck(t *testing.T, r arrow.RecordBatch, want [][]any, at *int) {
	t.Helper()
	names := []string{"ts", "v", "cr", "cv", "sum", "avg"}
	if r.NumCols() != 6 {
		t.Fatalf("output columns=%d", r.NumCols())
	}
	for j, name := range names {
		f := r.Schema().Field(j)
		if f.Name != name {
			t.Fatalf("field%d name=%s", j, f.Name)
		}
		expectedNullable := j == 1 || j == 4 || j == 5
		if f.Nullable != expectedNullable {
			t.Fatalf("field%s nullable=%v", name, f.Nullable)
		}
		typ := arrow.INT64
		if j == 0 {
			typ = arrow.TIMESTAMP
		}
		if j == 5 {
			typ = arrow.FLOAT64
		}
		if f.Type.ID() != typ {
			t.Fatalf("field%s type=%v", name, f.Type)
		}
	}
	for i := 0; i < int(r.NumRows()); i++ {
		if *at >= len(want) {
			t.Fatal("extra RANGE row")
		}
		for j, w := range want[*at] {
			got := n44OracleRead(r.Column(j), i)
			if wf, ok := w.(float64); ok {
				gf, valid := got.(float64)
				if !valid || math.Float64bits(gf) != math.Float64bits(wf) {
					t.Fatalf("avg row%d got%v want%.17g", *at, got, wf)
				}
			} else if got != w {
				t.Fatalf("row%d col%d got%v want%v", *at, j, got, w)
			}
		}
		*at++
	}
}
func n44OracleBatches(t *testing.T, q LazyFrame, want [][]any) {
	t.Helper()
	rd, err := q.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatalf("RANGE LOWERING BARRIER: %v", err)
	}
	defer rd.Close()
	at := 0
	for rd.Next(context.Background()) {
		v := rd.Batch()
		r, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
		if err != nil {
			t.Fatal(err)
		}
		n44OracleCheck(t, r, want, &at)
		r.Release()
	}
	err = rd.Err()
	rd.Close()
	if err != nil || at != len(want) {
		t.Fatalf("RANGE rows=%d want=%d err=%v", at, len(want), err)
	}
}

type n44OracleCounted struct {
	source.Source
	opens  atomic.Int64
	nexts  atomic.Int64
	closes atomic.Int64
}

func (s *n44OracleCounted) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "ts"}}
}
func (s *n44OracleCounted) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens.Add(1)
	st, e := source.OpenBatchStream(ctx, s.Source, opts)
	if e != nil {
		return nil, e
	}
	return &n44OracleCountedStream{BatchStream: st, owner: s}, nil
}
func n44OracleSpec(width int64) TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "ts", Column: "v", Frame: TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: width}, CountRowsAlias: "cr", CountAlias: "cv", SumAlias: "sum", AvgAlias: "avg"}
}
func TestN44TimestampRangeOracle(t *testing.T) {
	cases := []struct {
		name  string
		width int64
		rows  []n44OracleRow
	}{
		{"zero_peers", 0, []n44OracleRow{{1, 3, true}, {1, 8, true}, {1, 0, false}, {2, -5, true}}},
		{"inclusive", 3, []n44OracleRow{{-4, 2, true}, {-1, 0, false}, {-1, 9, true}, {2, -3, true}, {3, 7, true}}},
		{"underflow", math.MaxInt64, []n44OracleRow{{math.MinInt64, 7, true}, {math.MinInt64 + 1, -2, true}, {-1, 6, true}, {0, 8, true}}},
		{"wide_cancel", 0, []n44OracleRow{{0, math.MaxInt64, true}, {0, math.MaxInt64, true}, {0, -math.MaxInt64, true}}},
	}
	for _, tc := range cases {
		for _, chunk := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("%s/batch%d", tc.name, chunk), func(t *testing.T) {
				s := n44OracleSession()
				defer s.Close()
				src := &n44OracleCounted{Source: n44OracleInput(t, tc.rows, chunk)}
				defer func() { t.Logf("actual input opens=%d engineUsed=%d", src.opens.Load(), s.engine.Memory().Used()) }()
				f := LazyFrame{session: s, plan: &logical.Scan{Source: src}}.WithTimestampRange(n44OracleSpec(tc.width))
				n44OracleBatches(t, f, n44OracleExpected(tc.rows, tc.width))
			})
		}
	}
}

func TestN44TimestampRangeRoundtrip(t *testing.T) {
	rows := []n44OracleRow{{-2, 0, false}, {0, 3, true}, {0, 8, true}, {0, 0, false}, {1, -2, true}, {5, 0, false}}
	for _, kind := range []string{"ipc", "parquet"} {
		t.Run(kind, func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			q := s.FromSource(n44OracleInput(t, rows, 1)).WithTimestampRange(n44OracleSpec(2))
			want := n44OracleExpected(rows, 2)
			at := 0
			path := filepath.Join(t.TempDir(), "range."+kind)
			if kind == "ipc" {
				if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
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
					rec, e := r.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					n44OracleCheck(t, rec, want, &at)
					rec.Release()
				}
			} else {
				if _, e := q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 2}); e != nil {
					t.Fatal(e)
				}
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
				rr := array.NewTableReader(tab, 2)
				defer rr.Release()
				for rr.Next() {
					n44OracleCheck(t, rr.RecordBatch(), want, &at)
				}
				if rr.Err() != nil {
					t.Fatal(rr.Err())
				}
			}
			if at != len(want) {
				t.Fatalf("roundtrip rows=%d want=%d", at, len(want))
			}
			if s.engine.Memory().Used() != 0 {
				t.Fatal("sink memory leak")
			}
		})
	}
}
func TestN44TimestampRangeCountAndLimits(t *testing.T) {
	for _, bad := range []string{"descending", "overflow"} {
		t.Run(bad, func(t *testing.T) {
			rows := []n44OracleRow{{2, 1, true}, {1, 3, true}}
			fragment := "nondecreasing"
			if bad == "overflow" {
				rows = []n44OracleRow{{1, math.MaxInt64, true}, {1, math.MaxInt64, true}}
				fragment = "overflow"
			}
			s := n44OracleSession()
			defer s.Close()
			q := s.FromSource(n44OracleInput(t, rows, 1)).WithTimestampRange(n44OracleSpec(0))
			for name, f := range map[string]LazyFrame{"count": q.Select("ts").Agg(Count("n")), "limit": q.Limit(1)} {
				t.Run(name, func(t *testing.T) {
					d, e := f.Collect(context.Background())
					if e == nil {
						d.Close()
						t.Fatal("RANGE validation elided")
					}
					if !strings.Contains(strings.ToLower(e.Error()), fragment) {
						t.Fatalf("wrong error: %v", e)
					}
				})
			}
		})
	}
	t.Run("limit_complete_peer", func(t *testing.T) {
		s := n44OracleSession()
		defer s.Close()
		rows := []n44OracleRow{{1, 2, true}, {1, 9, true}, {1, 0, false}, {2, 7, true}}
		q := s.FromSource(n44OracleInput(t, rows, 1)).WithTimestampRange(n44OracleSpec(0))
		n44OracleBatches(t, q.Limit(1), n44OracleExpected(rows, 0)[:1])
	})

	for _, withRange := range []bool{false, true} {
		t.Run(fmt.Sprintf("zero_limit_range%v", withRange), func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			src := &n44OracleCounted{Source: n44OracleInput(t, []n44OracleRow{{1, 2, true}}, 1)}
			q := s.FromSource(src)
			if withRange {
				q = q.WithTimestampRange(n44OracleSpec(0))
			}
			q = q.Limit(0)
			rd, e := q.CollectBatches(context.Background(), BatchOptions{})
			if e != nil {
				t.Fatal(e)
			}
			if rd.Next(context.Background()) {
				t.Fatal("Limit0 published")
			}
			e = rd.Err()
			rd.Close()
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("Limit0 RANGE=%v opens=%d nexts=%d closes=%d used=%d", withRange, src.opens.Load(), src.nexts.Load(), src.closes.Load(), s.engine.Memory().Used())
			if src.nexts.Load() != 0 || src.closes.Load() != 1 || s.engine.Memory().Used() != 0 {
				t.Fatal("Limit0 consumed or leaked")
			}
		})
	}
}

type n44OracleCountedStream struct {
	source.BatchStream
	owner  *n44OracleCounted
	closed bool
}

func (s *n44OracleCountedStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.owner.nexts.Add(1)
	return s.BatchStream.Next(ctx, b)
}
func (s *n44OracleCountedStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes.Add(1)
	}
	return s.BatchStream.Close()
}
