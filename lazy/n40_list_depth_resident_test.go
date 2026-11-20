package lazy

import (
	"context"
	"fmt"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
)

func n40Fixture(t *testing.T, required bool) (string, [][]any, *arrow.Schema) {
	t.Helper()
	_, raw, old := n38Fixture(t, required)
	fields := old.Fields()
	fields[0].Nullable = true
	fields = append(fields, arrow.Field{Name: "rowid", Type: arrow.PrimitiveTypes.Int64})
	schema := arrow.NewSchema(fields, nil)
	keys := []any{int64(2), nil, int64(1), int64(2), nil, int64(1)}
	rb := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer rb.Release()
	for i := range raw {
		raw[i][0] = keys[i]
		raw[i] = append(raw[i], int64(i))
		if keys[i] == nil {
			rb.Field(0).AppendNull()
		} else {
			rb.Field(0).(*array.Int64Builder).Append(keys[i].(int64))
		}
		n38Append(rb.Field(1), raw[i][1], 3)
		n38Append(rb.Field(2), raw[i][2], 3)
		rb.Field(3).(*array.Int64Builder).Append(int64(i))
	}
	record := rb.NewRecordBatch()
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer table.Release()
	path := filepath.Join(t.TempDir(), "unsorted.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = pqarrow.WriteTable(table, f, 2, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(raw, func(i, j int) bool {
		a, b := raw[i][0], raw[j][0]
		if a == nil {
			return b != nil
		}
		if b == nil {
			return false
		}
		return a.(int64) < b.(int64)
	})
	return path, raw, schema
}
func n40Check(t *testing.T, r arrow.RecordBatch, want [][]any, at *int, schema *arrow.Schema) {
	t.Helper()
	if len(r.Schema().Fields()) != len(schema.Fields()) {
		t.Fatal("output field count")
	}
	for j, f := range schema.Fields() {
		got := r.Schema().Field(j)
		if got.Name != f.Name || got.Nullable != f.Nullable {
			t.Fatalf("field metadata got %v want %v", got, f)
		}
		n38CheckType(t, got.Type, f.Type)
	}
	for i := 0; i < int(r.NumRows()); i++ {
		if *at >= len(want) {
			t.Fatal("extra sorted row")
		}
		for j, w := range want[*at] {
			if got := n38Read(r.Column(j), i); !reflect.DeepEqual(got, w) {
				t.Fatalf("row%d col%d got%#v want%#v", *at, j, got, w)
			}
		}
		*at++
	}
}
func TestN40ListDepthResidentPublic(t *testing.T) {
	for _, required := range []bool{false, true} {
		for _, batch := range []int{1, 3} {
			t.Run(fmt.Sprintf("required%v_batch%d", required, batch), func(t *testing.T) {
				path, want, schema := n40Fixture(t, required)
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
				q = q.Sort(AscNullsFirst("k"))
				plan, err := q.ExplainPhysical(context.Background())
				if err != nil {
					t.Fatalf("RESIDENT ADMISSION BARRIER: %v", err)
				}
				t.Logf("synthetic rows=%d rowgroup=2 batch=%d workers=1 native=true limit=33554432 plan=%s", len(want), batch, plan)
				if !strings.Contains(plan, "resident-stable") {
					t.Fatalf("not resident: %s", plan)
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
				// N41 separately admits canonical top-level depth3 TopK snapshots.
				if plan, err := q.Limit(2).ExplainPhysical(context.Background()); err != nil || !strings.Contains(plan, "one-pass-snapshots") {
					t.Fatalf("depth3 TopK snapshot plan=%s err=%v", plan, err)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("query memory leaked")
				}
			})
		}
	}
}

func TestN40ListDepthResidentPublicTopKBoundary(t *testing.T) {
	path, fullWant, fullSchema := n40Fixture(t, false)
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.MemoryLimitBytes = 32 << 20
	o.PreparedCacheBytes = 0
	s := NewSession(o)
	defer s.Close()
	q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []int64{0, 2} {
		p := q.Sort(AscNullsFirst("k")).Limit(k)
		plan, bindErr := p.ExplainPhysical(context.Background())
		rd, runErr := p.CollectBatches(context.Background(), BatchOptions{})
		rows := 0
		if runErr == nil {
			for rd.Next(context.Background()) {
				v := rd.Batch()
				r, e := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
				if e != nil {
					rd.Close()
					t.Fatal(e)
				}
				n40Check(t, r, fullWant[:int(k)], &rows, fullSchema)
				r.Release()
			}
			runErr = rd.Err()
			rd.Close()
		}
		t.Logf("deep TopK k=%d explain=%q bindError=%v runtimeError=%v rows=%d", k, plan, bindErr, runErr, rows)
		if bindErr != nil || runErr != nil || rows != int(k) {
			t.Fatalf("depth3 TopK k=%d rows=%d bind=%v runtime=%v", k, rows, bindErr, runErr)
		}
		if k > 0 && !strings.Contains(plan, "one-pass-snapshots") {
			t.Fatal("depth3 TopK not one-pass snapshots")
		}
	}
	// Pruning depth3 before TopK must preserve existing scalar-only support.
	clipped := q.Select("k", "rowid").Sort(AscNullsFirst("k")).Limit(2)
	plan, err := clipped.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("projected TopK actual plan=%s", plan)
	rd, err := clipped.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	want := []int64{1, 4}
	at := 0
	for rd.Next(context.Background()) {
		v := rd.Batch()
		r, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
		if err != nil {
			t.Fatal(err)
		}
		ids := r.Schema().FieldIndices("rowid")
		keys := r.Schema().FieldIndices("k")
		for i := 0; i < int(r.NumRows()); i++ {
			if at >= len(want) || n38Read(r.Column(ids[0]), i) != want[at] || n38Read(r.Column(keys[0]), i) != nil {
				r.Release()
				t.Fatal("projected TopK values/order")
			}
			at++
		}
		r.Release()
	}
	err = rd.Err()
	rd.Close()
	if err != nil || at != 2 {
		t.Fatalf("projected TopK rows%d err%v", at, err)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("boundary leaked memory")
	}
}

// A hostile but contract-compliant stream invalidates borrowed payload values
// at the next Next/Close. This independently tests the sort's retained copy.
type n40PoisonSource struct {
	source.Source
	poisoned atomic.Int64
	closed   atomic.Int64
}

func (s *n40PoisonSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	in, err := source.OpenBatchStream(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	return &n40PoisonStream{in: in, owner: s}, nil
}

type n40PoisonStream struct {
	in     source.BatchStream
	owner  *n40PoisonSource
	last   []*memory.ArrayNode
	closed bool
}

func (s *n40PoisonStream) poison() {
	for _, root := range s.last {
		n := root
		for n != nil && n.Child != nil {
			n = n.Child
		}
		if n == nil {
			continue
		}
		if n.Int64 != nil {
			for i := range n.Int64.Values {
				n.Int64.Values[i] = 987654321
			}
			s.owner.poisoned.Add(1)
		}
		if n.Strings != nil {
			for i := range n.Strings.Data {
				n.Strings.Data[i] = 'X'
			}
			s.owner.poisoned.Add(1)
		}
	}
	s.last = nil
}
func (s *n40PoisonStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.poison()
	ok, err := s.in.Next(ctx, b)
	if ok && err == nil {
		for _, n := range b.Arrays {
			s.last = append(s.last, n)
		}
	}
	return ok, err
}
func (s *n40PoisonStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.poison()
	s.owner.closed.Add(1)
	return s.in.Close()
}
func TestN40ListDepthResidentPublicSourceReuse(t *testing.T) {
	path, want, schema := n40Fixture(t, false)
	raw, err := parquetsource.OpenFileWithOptions(path, parquetsource.Options{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	input := &n40PoisonSource{Source: raw}
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
	q := s.FromSource(input).Sort(AscNullsFirst("k"))
	rd, err := q.CollectBatches(context.Background(), BatchOptions{})
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
		n40Check(t, r, want, &at, schema)
		r.Release()
	}
	err = rd.Err()
	rd.Close()
	if err != nil || at != len(want) {
		t.Fatalf("poison source rows=%d err=%v", at, err)
	}
	if input.poisoned.Load() == 0 || input.closed.Load() == 0 {
		t.Fatal("source poison/Close not exercised")
	}
	// Close before advancing to EOF must independently invalidate a live view.
	early, err := q.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !early.Next(context.Background()) {
		e := early.Err()
		early.Close()
		t.Fatalf("early-close first batch: %v", e)
	}
	live := early.Batch()
	if !live.Valid() {
		early.Close()
		t.Fatal("first borrowed view already invalid")
	}
	early.Close()
	if live.Valid() {
		t.Fatal("live borrowed view survived explicit early Close")
	}
	t.Logf("actual borrowed source invalidations=%d streamCloses=%d", input.poisoned.Load(), input.closed.Load())
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("poison source query leaked memory")
	}
}
