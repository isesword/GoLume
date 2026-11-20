package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

type n13DiagnosticSource struct {
	source.Source
	diagnostics map[string]string
}

func (s *n13DiagnosticSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	st, e := source.OpenBatchStream(ctx, s.Source, o)
	if e != nil {
		return nil, e
	}
	return &n13DiagnosticStream{BatchStream: st, owner: s}, nil
}

type n13DiagnosticStream struct {
	source.BatchStream
	owner *n13DiagnosticSource
}

func (s *n13DiagnosticStream) Close() error {
	if d, ok := s.BatchStream.(source.BatchStreamDiagnostics); ok {
		s.owner.diagnostics = d.StreamDiagnostics()
	}
	return s.BatchStream.Close()
}

func TestDecimalSpillForcedMultipass(t *testing.T) {
	for _, ps := range [][2]int32{{1, 0}, {38, 0}, {38, 38}} {
		for _, scenario := range []string{"key", "payload", "multi"} {
			payloadOnly := scenario == "payload"
			t.Run(fmt.Sprintf("p%d-s%d-%s", ps[0], ps[1], scenario), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.ExecutionPolicy = engine.ExecutionBounded
				opts.MemoryLimitBytes = 64 << 20
				dir := t.TempDir()
				opts.Spill = memory.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
				s := NewSession(opts)
				defer s.Close()
				rec := decimalResidentRecord(t, ps[0], ps[1])
				defer rec.Release()
				if scenario == "multi" {
					builder := array.NewDecimal128Builder(am.DefaultAllocator, &arrow.Decimal128Type{Precision: 1, Scale: 0})
					for i := 0; i < 65; i++ {
						builder.Append(decimal128.FromI64(int64(i%3 - 1)))
					}
					extra := builder.NewDecimal128Array()
					builder.Release()
					extended := array.NewRecordBatch(arrow.NewSchema(append(append([]arrow.Field(nil), rec.Schema().Fields()...), arrow.Field{Name: "second", Type: extra.DataType()}), nil), append(append([]arrow.Array(nil), rec.Columns()...), extra), 65)
					extra.Release()
					rec = extended
					defer extended.Release()
				}
				input, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeNested: true, NativeDecimal128: true}, rec)
				if e != nil {
					t.Fatal(e)
				}
				defer input.Close()
				// One row per source batch forces 65 initial runs without changing RAM/data.
				observed := &nestedTopKObserved{Source: input, stable: true, batchRows: 1}
				key := "amount"
				if payloadOnly {
					key = "seq"
				}
				keys := []physical.SortKey{{Column: key, Descending: true, NullsLast: true}}
				if scenario == "multi" {
					keys = append(keys, physical.SortKey{Column: "second", NullsLast: true})
				}
				sorted, e := physical.NewSortSourceWithOptions(observed, keys, nil, physical.SortSourceOptions{Spillable: true})
				if e != nil {
					t.Fatal(e)
				}
				diag := &n13DiagnosticSource{Source: sorted}
				df, e := s.FromSource(diag).Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				order := decimalResidentOrder(ps[0], true, false)
				if scenario == "multi" {
					sort.SliceStable(order, func(i, j int) bool {
						a, b := order[i], order[j]
						an, bn := a%7 == 0, b%7 == 0
						if an != bn {
							return !an
						}
						if !an {
							c := decimalResidentWant(a, ps[0]).Cmp(decimalResidentWant(b, ps[0]))
							if c != 0 {
								return c > 0
							}
						}
						return a%3 < b%3
					})
				}
				if payloadOnly {
					for i := range order {
						order[i] = 64 - i
					}
				}
				decimalResidentCheck(t, df, ps[0], ps[1], order)
				runs, _ := strconv.Atoi(diag.diagnostics["spill_runs"])
				levels, _ := strconv.Atoi(diag.diagnostics["merge_levels"])
				if diag.diagnostics["strategy"] != "external-stable" || runs < 65 || levels < 2 {
					t.Fatal("not actual multipass", diag.diagnostics)
				}
				t.Log(diag.diagnostics)
				s.Close()
				input.Close()
				decimalResidentCheck(t, df, ps[0], ps[1], order)
				df.Close()
				recursiveCNoFiles(t, dir)
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("retained")
				}
			})
		}
	}
}
func TestDecimalSpillNativeParquetIO(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 64 << 20
	dir := t.TempDir()
	opts.Spill = memory.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	r := decimalResidentRecord(t, 38, 38)
	defer r.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	input := filepath.Join(t.TempDir(), "input.parquet")
	if _, e = q.SinkParquet(context.Background(), input, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(input, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	sorted := read.Sort(Desc("amount"))
	want := decimalResidentOrder(38, true, false)
	df, e := sorted.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	decimalResidentCheck(t, df, 38, 38, want)
	df.Close()
	ipcPath := filepath.Join(t.TempDir(), "out.arrow")
	if _, e = sorted.SinkIPC(context.Background(), ipcPath, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(ipcPath)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := ipc.NewFileReader(f)
	if e != nil {
		t.Fatal(e)
	}
	seen := 0
	for i := 0; i < rd.NumRecords(); i++ {
		record, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		a := record.Column(record.Schema().FieldIndices("amount")[0]).(*array.Decimal128)
		dt := a.DataType().(*arrow.Decimal128Type)
		if dt.Precision != 38 || dt.Scale != 38 {
			t.Fatal("IPC metadata")
		}
		for j := 0; j < a.Len(); j++ {
			row := want[seen]
			valid := row%7 != 0
			if a.IsValid(j) != valid || valid && a.Value(j).BigInt().Cmp(decimalResidentWant(row, 38)) != 0 {
				t.Fatal("independent IPC coefficient", seen)
			}
			seen++
		}
		record.Release()
	}
	rd.Close()
	f.Close()
	if seen != 65 {
		t.Fatal("IPC rows", seen)
	}
	output := filepath.Join(t.TempDir(), "out.parquet")
	if _, e = sorted.SinkParquet(context.Background(), output, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	again, e := s.ScanParquetWithOptions(output, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	df, e = again.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	decimalResidentCheck(t, df, 38, 38, want)
	df.Close()
	recursiveCNoFiles(t, dir)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}
