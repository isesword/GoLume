package lazy

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
)

func TestN41ListDepthTopKPublic(t *testing.T) {
	for _, required := range []bool{false, true} {
		for _, batch := range []int{1, 3} {
			t.Run(fmt.Sprintf("required%v_batch%d", required, batch), func(t *testing.T) {
				path, want, schema := n40Fixture(t, required)
				want = want[:3]
				o := engine.DefaultOptions()
				o.Workers = 1
				o.Pipeline.Workers = 1
				o.Pipeline.MinMorselRows = 1
				o.Pipeline.MaxMorselRows = batch
				o.ExecutionPolicy = engine.ExecutionMemoryOnly
				o.MemoryLimitBytes = 32 << 20
				o.PreparedCacheBytes = 0
				s := NewSession(o)
				defer s.Close()
				q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
				if err != nil {
					t.Fatal(err)
				}
				q = q.Sort(AscNullsFirst("k")).Limit(3)
				plan, err := q.ExplainPhysical(context.Background())
				if err != nil {
					t.Fatalf("TOPK ADMISSION BARRIER: %v", err)
				}
				t.Logf("synthetic rows=%d rowgroup=2 batch=%d workers=1 native=true limit=33554432 plan=%s", len(want), batch, plan)
				if !strings.Contains(plan, "one-pass-snapshots") {
					t.Fatalf("not one-pass TopK: %s", plan)
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

type n41CountSource struct {
	source.Source
	opens atomic.Int64
}

func (s *n41CountSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens.Add(1)
	return source.OpenBatchStream(ctx, s.Source, opts)
}
func n41Len(v any) any {
	if v == nil {
		return nil
	}
	return int64(len(v.([]any)))
}
func n41Get(v any) any {
	if v == nil {
		return nil
	}
	a := v.([]any)
	if len(a) == 0 {
		return nil
	}
	return a[0]
}
func TestN41ListDepthTopKPublicNavigationAndLifetime(t *testing.T) {
	path, want, _ := n40Fixture(t, false)
	want = want[:3]
	raw, err := parquetsource.OpenFileWithOptions(path, parquetsource.Options{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	poison := &n40PoisonSource{Source: raw}
	counted := &n41CountSource{Source: poison}
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.Pipeline.MinMorselRows = 1
	o.Pipeline.MaxMorselRows = 1
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.MemoryLimitBytes = 32 << 20
	o.PreparedCacheBytes = 0
	s := NewSession(o)
	defer s.Close()
	base := s.FromSource(counted)
	zero := base.Sort(AscNullsFirst("k")).Limit(0)
	zr, err := zero.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if zr.Next(context.Background()) {
		zr.Close()
		t.Fatal("K0 output")
	}
	err = zr.Err()
	zr.Close()
	if err != nil {
		t.Fatal(err)
	}
	if counted.opens.Load() != 0 {
		t.Fatal("K0 opened input")
	}
	t.Log("K0 actual input opens=0")
	q := base.Sort(AscNullsFirst("k")).Limit(3)
	picked := ListGet(Col("ints"), expr.I64(0))
	nav := q.SelectExprs(Col("rowid").Alias("rowid"), ListLength(Col("ints")).Alias("length"), picked.Alias("picked"), ListGet(ListGet(picked, expr.I64(0)), expr.I64(0)).Alias("scalar"))
	plan, err := nav.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "one-pass-snapshots") {
		t.Fatalf("navigation lost one-pass plan: %s", plan)
	}
	rd, err := nav.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	at := 0
	for rd.Next(context.Background()) {
		v := rd.Batch()
		r, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(r.NumRows()); i++ {
			if at >= len(want) {
				r.Release()
				t.Fatal("extra navigation row")
			}
			raw := want[at][1]
			selected := n41Get(raw)
			expected := []any{want[at][3], n41Len(raw), selected, n41Get(n41Get(selected))}
			for j, w := range expected {
				if got := n38Read(r.Column(j), i); !reflect.DeepEqual(got, w) {
					r.Release()
					t.Fatalf("navigation row%d col%d got%#v want%#v", at, j, got, w)
				}
			}
			at++
		}
		r.Release()
	}
	err = rd.Err()
	rd.Close()
	if err != nil || at != len(want) {
		t.Fatalf("navigation rows%d err%v", at, err)
	}
	early, err := q.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !early.Next(context.Background()) {
		e := early.Err()
		early.Close()
		t.Fatalf("first output: %v", e)
	}
	v := early.Batch()
	if !v.Valid() {
		early.Close()
		t.Fatal("first view invalid")
	}
	early.Close()
	if v.Valid() {
		t.Fatal("view survived earlyClose")
	}
	if poison.poisoned.Load() == 0 || poison.closed.Load() == 0 {
		t.Fatal("source reuse not exercised")
	}
	t.Logf("source opens=%d invalidations=%d closes=%d", counted.opens.Load(), poison.poisoned.Load(), poison.closed.Load())
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("query memory leak")
	}
}
