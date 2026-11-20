package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
)

// This is a new synthetic workload, not a reconstruction of the unavailable
// historical 16/64 MiB workload. Coefficients are valid TIME(ns,false) values.
func TestN37ListTimeResidentSynthetic(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		rows, children, rowgroup, batch, workers int
		limit                                    int64
	}{
		{"tiny16MiB", 16, 3, 8, 8, 1, 16 << 20},
		{"pages16MiB", 1024, 16, 128, 64, 1, 16 << 20},
		{"pages64MiB", 1024, 16, 128, 64, 1, 64 << 20},
		{"mixed-null-empty16MiB", 1024, 16, 128, 64, 1, 16 << 20},
		{"ownerTrace16MiB", 1024, 16, 128, 64, 1, 16 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("SYNTHETIC rows=%d childrenPerRow=%d totalChildren=%d rowGroupRows=%d rowGroups=%d requestedBatchRows=%d workers=%d NativeNested=true execution=MemoryOnly queryLimit=%d", tc.rows, tc.children, tc.rows*tc.children, tc.rowgroup, (tc.rows+tc.rowgroup-1)/tc.rowgroup, tc.batch, tc.workers, tc.limit)
			b := memory.NewBatch()
			b.Rows = tc.rows
			k := &memory.Int64Vector{Values: make([]int64, tc.rows)}
			v := &memory.Int64ListVector{Offsets: make([]uint32, tc.rows+1), Encoding: memory.EncodingView, Values: &memory.Int64Vector{Values: make([]int64, tc.rows*tc.children)}}
			for i := 0; i < tc.rows; i++ {
				k.Values[i] = int64(tc.rows - 1 - i)
				v.Offsets[i] = uint32(i * tc.children)
				for j := 0; j < tc.children; j++ {
					v.Values.Values[i*tc.children+j] = k.Values[i]*1000 + int64(j)
				}
			}
			v.Offsets[tc.rows] = uint32(len(v.Values.Values))
			mixed := strings.HasPrefix(tc.name, "mixed")
			if mixed {
				v.Values.Values = nil
				v.Validity = make(memory.Bitmap, memory.WordsFor(tc.rows))
				v.Values.Validity = make(memory.Bitmap, memory.WordsFor(tc.rows*tc.children))
				for i := 0; i < tc.rows; i++ {
					v.Offsets[i] = uint32(len(v.Values.Values))
					if i%7 == 0 {
						continue
					}
					memory.Set(v.Validity, i)
					if i%7 == 1 {
						continue
					}
					for j := 0; j < tc.children; j++ {
						at := len(v.Values.Values)
						v.Values.Values = append(v.Values.Values, k.Values[i]*1000+int64(j))
						if j%5 != 0 {
							memory.Set(v.Values.Validity, at)
						}
					}
				}
				v.Offsets[tc.rows] = uint32(len(v.Values.Values))
			}
			t.Logf("actual children=%d mixedNULLempty=%v", len(v.Values.Values), mixed)
			b.Int64s["k"] = k
			b.Int64Lists["clocks"] = v
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64}, memory.ListTime64Field("clocks", true, true))
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			record, e := memory.ArrowRecord(alloc, schema, b)
			if e != nil {
				t.Fatalf("fixture Arrow barrier: %v", e)
			}
			defer record.Release()
			root := t.TempDir()
			in, out := filepath.Join(root, "input.parquet"), filepath.Join(root, "sorted.parquet")
			fp, e := os.Create(in)
			if e != nil {
				t.Fatal(e)
			}
			defer fp.Close()
			writer, e := pqarrow.NewFileWriter(record.Schema(), fp, pq.NewWriterProperties(pq.WithDictionaryDefault(false), pq.WithMaxRowGroupLength(int64(tc.rowgroup))), pqarrow.DefaultWriterProps())
			if e != nil {
				t.Fatalf("fixture writer barrier: %v", e)
			}
			if e = writer.Write(record); e != nil {
				writer.Close()
				t.Fatalf("fixture encode barrier: %v", e)
			}
			if e = writer.Close(); e != nil {
				t.Fatalf("fixture finish barrier: %v", e)
			}
			opts := engine.DefaultOptions()
			opts.Workers = tc.workers
			opts.Pipeline.Workers = tc.workers
			opts.Pipeline.MinMorselRows = tc.batch
			opts.Pipeline.MaxMorselRows = tc.batch
			opts.MemoryLimitBytes = tc.limit
			opts.ExecutionPolicy = engine.ExecutionMemoryOnly
			opts.PreparedCacheBytes = 0
			s := NewSession(opts)
			defer s.Close()
			q, e := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatalf("source admission barrier: %v", e)
			}
			// Read the source first, distinguishing a decoder failure from ordering.
			reader, e := q.CollectBatches(context.Background(), BatchOptions{})
			if e != nil {
				t.Fatalf("source open barrier: %v", e)
			}
			inputRows, inputPages := 0, 0
			actualMaxRows := 0
			for reader.Next(context.Background()) {
				view := reader.Batch()
				inputRows += int(view.Len())
				actualMaxRows = max(actualMaxRows, int(view.Len()))
				inputPages++
				if _, e := view.Column("clocks"); e != nil {
					t.Fatal(e)
				}
			}
			readerr := reader.Err()
			reader.Close()
			if readerr != nil {
				t.Fatalf("source decode barrier: %v", readerr)
			}
			if inputRows != tc.rows {
				t.Fatalf("source rows %d", inputRows)
			}
			t.Logf("source read success rows=%d actualBatches=%d actualMaxBatchRows=%d", inputRows, inputPages, actualMaxRows)
			sorted := q.Sort(Asc("k"))
			plan, e := sorted.ExplainPhysical(context.Background())
			if e != nil {
				t.Fatalf("sort plan barrier: %v", e)
			}
			t.Logf("actual plan:\n%s", plan)
			if !strings.Contains(strings.ToLower(plan), "resident") {
				t.Fatalf("resident strategy not established: %s", plan)
			}
			sinkctx := context.Background()
			var trace *n37BudgetTrace
			var query *memory.QueryBudget
			if strings.HasPrefix(tc.name, "ownerTrace") {
				query, e = s.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				defer query.Close()
				trace = &n37BudgetTrace{Context: context.Background(), q: query}
				sinkctx = memory.WithQueryBudget(trace, query)
			}
			result, e := sorted.SinkParquet(sinkctx, out, ParquetSinkOptions{RowGroupRows: int64(tc.rowgroup)})
			if trace != nil {
				trace.mu.Lock()
				t.Logf("OBSERVED budget peak=%d snapshots=%+v", query.Snapshot().PeakBytes, trace.samples)
				t.Logf("OBSERVED scan/retained maxima=%+v", trace.scanSample)
				trace.mu.Unlock()
				query.Close()
			}
			if e != nil {
				var resource *memory.ResourceError
				if errors.As(e, &resource) {
					t.Logf("resource operator=%s requested=%d used=%d limit=%d scope=%s", resource.Operator, resource.RequestedBytes, resource.UsedBytes, resource.LimitBytes, resource.Scope)
				}
				t.Fatalf("resident sort→sink failed: %T: %v (engineUsed=%d)", e, e, s.Engine().Memory().Used())
			}
			if result.Rows != int64(tc.rows) {
				t.Fatalf("sink rows %+v", result)
			}
			got, e := s.ScanParquetWithOptions(out, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatalf("readback admission: %v", e)
			}
			rd, e := got.CollectBatches(context.Background(), BatchOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer rd.Close()
			if !reflect.DeepEqual(rd.Schema(), schema) {
				t.Fatalf("full readback schema mismatch got%+v want%+v", rd.Schema(), schema)
			}
			seen := 0
			for rd.Next(context.Background()) {
				page := rd.Batch().state.batches[0]
				ks := page.Int64s["k"]
				ls := page.Int64Lists["clocks"]
				if ls == nil {
					t.Fatal("TIME list payload missing")
				}
				for i := 0; i < page.Rows; i++ {
					if ks.Values[i] != int64(seen) {
						t.Fatal("sort key mismatch")
					}
					lo, hi, valid := ls.ValueRange(i)
					orig := tc.rows - 1 - seen
					wantValid := !mixed || orig%7 != 0
					wantChildren := tc.children
					if mixed && (orig%7 == 0 || orig%7 == 1) {
						wantChildren = 0
					}
					if valid != wantValid || int(hi-lo) != wantChildren {
						t.Fatal("list shape mismatch")
					}
					for j := lo; j < hi; j++ {
						want := int64(seen)*1000 + int64(j-lo)
						wantChildValid := !mixed || int(j-lo)%5 != 0
						if ls.Values.IsValid(j) != wantChildValid || wantChildValid && ls.Values.Values[j] != want {
							t.Fatal(fmt.Sprintf("child mismatch row%d child%d", seen, j-lo))
						}
					}
					seen++
				}
			}
			if rd.Err() != nil || seen != tc.rows {
				t.Fatalf("readback rows=%d err=%v", seen, rd.Err())
			}
			rd.Close()
			if s.Engine().Memory().Used() != 0 {
				t.Fatalf("retained memory%d", s.Engine().Memory().Used())
			}
		})
	}
}

// Err sampling observes live accounting without changing production allocators.
// Snapshots show only states reached at context checks, not every allocation.
type n37BudgetTrace struct {
	context.Context
	q          *memory.QueryBudget
	mu         sync.Mutex
	samples    []memory.BudgetSnapshot
	max        int64
	scanSample memory.BudgetSnapshot
}

func (c *n37BudgetTrace) Err() error {
	e := c.Context.Err()
	s := c.q.Snapshot()
	sink := false
	for name, n := range s.Operators {
		if n > 0 && strings.Contains(strings.ToLower(name), "parquet") {
			sink = true
		}
	}
	c.mu.Lock()
	if s.Operators["SourceScan"] > 0 && s.Operators["ResidentSort"] > 0 && s.UsedBytes > c.scanSample.UsedBytes {
		c.scanSample = s
	}
	if sink && s.UsedBytes > c.max {
		c.max = s.UsedBytes
		c.samples = append(c.samples, s)
	}
	c.mu.Unlock()
	return e
}
