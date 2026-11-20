package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	pq "github.com/apache/arrow-go/v18/parquet"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
)

func TestN42ListDepthExternalPublic(t *testing.T) {
	for _, required := range []bool{false, true} {
		for _, batch := range []int{1, 3} {
			t.Run(fmt.Sprintf("required%v_batch%d", required, batch), func(t *testing.T) {
				path, want, schema := n40Fixture(t, required)
				o := engine.DefaultOptions()
				o.Workers = 1
				o.Pipeline.Workers = 1
				o.Pipeline.MinMorselRows = 1
				o.Pipeline.MaxMorselRows = batch
				o.ExecutionPolicy = engine.ExecutionBounded
				o.MemoryLimitBytes = 32 << 20
				o.PreparedCacheBytes = 0
				o.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
				s := NewSession(o)
				defer s.Close()
				q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
				if err != nil {
					t.Fatal(err)
				}
				q = q.Sort(AscNullsFirst("k"))
				plan, err := q.ExplainPhysical(context.Background())
				if err != nil {
					t.Fatalf("EXTERNAL ADMISSION BARRIER: %v", err)
				}
				t.Logf("synthetic rows=%d rowgroup=2 batch=%d workers=1 native=true limit=33554432 plan=%s", len(want), batch, plan)
				if !strings.Contains(plan, "external-stable") {
					t.Fatalf("not external sort: %s", plan)
				}
				rd, err := q.CollectBatches(context.Background(), BatchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				at := 0
				var old BatchView
				for rd.Next(context.Background()) {
					if old.Valid() {
						t.Fatal("old borrow survived Next")
					}
					v := rd.Batch()
					r, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
					if err != nil {
						rd.Close()
						t.Fatal(err)
					}
					n40Check(t, r, want, &at, schema)
					r.Release()
					old = v
				}
				err = rd.Err()
				rd.Close()
				if err != nil || at != len(want) || old.Valid() {
					t.Fatalf("sorted rows/lifetime rows%d err%v", at, err)
				}
				for _, kind := range []string{"ipc", "parquet"} {
					t.Run(kind, func(t *testing.T) {
						out := filepath.Join(t.TempDir(), "sorted."+kind)
						at := 0
						if kind == "ipc" {
							if _, err := q.SinkIPC(context.Background(), out, IPCSinkOptions{}); err != nil {
								t.Fatal(err)
							}
							f, err := os.Open(out)
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()
							reader, err := ipc.NewFileReader(f)
							if err != nil {
								t.Fatal(err)
							}
							defer reader.Close()
							for j := 0; j < reader.NumRecords(); j++ {
								r, err := reader.RecordBatchAt(j)
								if err != nil {
									t.Fatal(err)
								}
								n40Check(t, r, want, &at, schema)
								r.Release()
							}
						} else {
							if _, err := q.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 2}); err != nil {
								t.Fatal(err)
							}
							f, err := file.OpenParquetFile(out, false)
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()
							reader, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
							if err != nil {
								t.Fatal(err)
							}
							table, err := reader.ReadTable(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							defer table.Release()
							rr := array.NewTableReader(table, 2)
							defer rr.Release()
							for rr.Next() {
								n40Check(t, rr.RecordBatch(), want, &at, schema)
							}
							if rr.Err() != nil {
								t.Fatal(rr.Err())
							}
						}
						if at != len(want) {
							t.Fatalf("sink rows%d want%d", at, len(want))
						}
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("sink leaked memory")
						}
					})
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("query memory leaked")
				}
			})
		}
	}
}

// Independent stress fixture: 48 rows, 24 row groups, unchanged 32MiB/64MiB quotas.
func n42LargeFixture(t *testing.T) (string, [][]any, *arrow.Schema) {
	_, base, schema := n40Fixture(t, false)
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer rb.Release()
	var want [][]any
	for i := 0; i < 48; i++ {
		raw := base[len(base)-1-i%len(base)]
		row := []any{raw[0], raw[1], raw[2], int64(i)}
		if row[0] == nil {
			rb.Field(0).AppendNull()
		} else {
			rb.Field(0).(*array.Int64Builder).Append(row[0].(int64))
		}
		n38Append(rb.Field(1), row[1], 3)
		n38Append(rb.Field(2), row[2], 3)
		rb.Field(3).(*array.Int64Builder).Append(int64(i))
		want = append(want, row)
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer table.Release()
	path := filepath.Join(t.TempDir(), "multilevel.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = pqarrow.WriteTable(table, f, 2, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(want, func(i, j int) bool {
		a, b := want[i][0], want[j][0]
		if a == nil {
			return b != nil
		}
		if b == nil {
			return false
		}
		return a.(int64) < b.(int64)
	})
	return path, want, schema
}
func TestN42ListDepthExternalPublicMultiLevel(t *testing.T) {
	for _, required := range []bool{false} {
		for _, batch := range []int{1} {
			t.Run(fmt.Sprintf("required%v_batch%d", required, batch), func(t *testing.T) {
				path, want, schema := n42LargeFixture(t)
				o := engine.DefaultOptions()
				o.Workers = 1
				o.Pipeline.Workers = 1
				o.Pipeline.MinMorselRows = 1
				o.Pipeline.MaxMorselRows = batch
				o.ExecutionPolicy = engine.ExecutionBounded
				o.MemoryLimitBytes = 32 << 20
				o.PreparedCacheBytes = 0
				o.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
				s := NewSession(o)
				defer s.Close()
				raw, err := parquetsource.OpenFileWithOptions(path, parquetsource.Options{NativeNested: true})
				if err != nil {
					t.Fatal(err)
				}
				poison := &n40PoisonSource{Source: raw}
				observed := &n42ObservedSource{Source: poison}
				q := s.FromSource(observed).Sort(AscNullsFirst("k"))
				plan, err := q.ExplainPhysical(context.Background())
				if err != nil {
					t.Fatalf("EXTERNAL ADMISSION BARRIER: %v", err)
				}
				t.Logf("synthetic rows=%d rowgroup=2 batch=%d workers=1 native=true limit=33554432 plan=%s", len(want), batch, plan)
				if !strings.Contains(plan, "external-stable") {
					t.Fatalf("not external sort: %s", plan)
				}
				report, err := q.Analyze(context.Background())
				if err != nil {
					t.Fatalf("analyze: %v\n%s", err, report.String())
				}
				t.Log(report.String())
				found := false
				for _, op := range report.ExecuteOperators {
					if op.Attributes["strategy"] == "external-stable" {
						levels, _ := strconv.Atoi(op.Attributes["merge_levels"])
						spills, _ := strconv.Atoi(op.Attributes["spill_runs"])
						bytes, _ := strconv.ParseInt(op.Attributes["spill_bytes"], 10, 64)
						if levels < 2 || spills < 16 || bytes <= 0 {
							t.Fatalf("not actual multilevel: %+v", op.Attributes)
						}
						found = true
					}
				}
				if !found {
					t.Fatal("no external sort execution diagnostics")
				}
				scanCtx, scanDone := n38OwnerTrace(t, s, "external-source")
				defer scanDone()
				rd, err := q.CollectBatches(scanCtx, BatchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				at := 0
				var old BatchView
				for rd.Next(scanCtx) {
					if old.Valid() {
						t.Fatal("old borrow survived Next")
					}
					v := rd.Batch()
					r, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
					if err != nil {
						rd.Close()
						t.Fatal(err)
					}
					n40Check(t, r, want, &at, schema)
					r.Release()
					old = v
				}
				err = rd.Err()
				rd.Close()
				scanDone()
				if err != nil || at != len(want) || old.Valid() {
					t.Fatalf("sorted rows/lifetime rows%d err%v", at, err)
				}
				for _, kind := range []string{"ipc", "parquet"} {
					t.Run(kind, func(t *testing.T) {
						sinkCtx, sinkDone := n38OwnerTrace(t, s, "external-"+kind)
						defer sinkDone()
						out := filepath.Join(t.TempDir(), "sorted."+kind)
						at := 0
						if kind == "ipc" {
							if _, err := q.SinkIPC(sinkCtx, out, IPCSinkOptions{}); err != nil {
								t.Fatal(err)
							}
							f, err := os.Open(out)
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()
							reader, err := ipc.NewFileReader(f)
							if err != nil {
								t.Fatal(err)
							}
							defer reader.Close()
							for j := 0; j < reader.NumRecords(); j++ {
								r, err := reader.RecordBatchAt(j)
								if err != nil {
									t.Fatal(err)
								}
								n40Check(t, r, want, &at, schema)
								r.Release()
							}
						} else {
							if _, err := q.SinkParquet(sinkCtx, out, ParquetSinkOptions{RowGroupRows: 2}); err != nil {
								t.Fatal(err)
							}
							f, err := file.OpenParquetFile(out, false)
							if err != nil {
								t.Fatal(err)
							}
							defer f.Close()
							reader, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
							if err != nil {
								t.Fatal(err)
							}
							table, err := reader.ReadTable(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							defer table.Release()
							rr := array.NewTableReader(table, 2)
							defer rr.Release()
							for rr.Next() {
								n40Check(t, rr.RecordBatch(), want, &at, schema)
							}
							if rr.Err() != nil {
								t.Fatal(rr.Err())
							}
						}
						sinkDone()
						if at != len(want) {
							t.Fatalf("sink rows%d want%d", at, len(want))
						}
						if s.Engine().Memory().Used() != 0 {
							t.Fatal("sink leaked memory")
						}
					})
				}
				early, err := q.CollectBatches(context.Background(), BatchOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !early.Next(context.Background()) {
					e := early.Err()
					early.Close()
					t.Fatalf("early first output: %v", e)
				}
				live := early.Batch()
				if !live.Valid() {
					early.Close()
					t.Fatal("early view invalid")
				}
				early.Close()
				if live.Valid() {
					t.Fatal("external output survived earlyClose")
				}
				observed.mu.Lock()
				snap := observed.live
				observed.mu.Unlock()
				t.Logf("actual input Next live query snapshot: %+v", snap)
				if snap.Operators["SourceScan"] <= 0 {
					t.Fatal("full source live budget attribution not observed")
				}
				entries, e := os.ReadDir(o.Spill.Directory)
				if e != nil || len(entries) != 0 {
					t.Fatalf("spill cleanup entries=%d err=%v", len(entries), e)
				}
				if poison.poisoned.Load() == 0 {
					t.Fatal("source invalidation not exercised")
				}
				t.Logf("source invalidations=%d closes=%d; spill directory empty", poison.poisoned.Load(), poison.closed.Load())
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("query memory leaked")
				}
			})
		}
	}
}

type n42ObservedSource struct {
	source.Source
	mu   sync.Mutex
	live memory.BudgetSnapshot
}

func (s *n42ObservedSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	in, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	return &n42ObservedStream{BatchStream: in, owner: s}, nil
}

type n42ObservedStream struct {
	source.BatchStream
	owner *n42ObservedSource
}

func (s *n42ObservedStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	ok, err := s.BatchStream.Next(ctx, b)
	if ok && err == nil {
		if q := memory.QueryBudgetFromContext(ctx); q != nil {
			snap := q.Snapshot()
			s.owner.mu.Lock()
			if snap.Operators["SourceScan"] > s.owner.live.Operators["SourceScan"] {
				s.owner.live = snap
			}
			s.owner.mu.Unlock()
		}
	}
	return ok, err
}
