package lazy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func TestN26ParquetListTimeLazySink(t *testing.T) {
	const day int64 = 86400000000000
	ctx := context.Background()
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Int64Lists["clocks"] = &memory.Int64ListVector{
		Offsets: []uint32{0, 3, 4, 4, 5}, Validity: memory.Bitmap{13}, Encoding: memory.EncodingView,
		Values: &memory.Int64Vector{Values: []int64{0, -17, day - 1, day, 123456789}, Validity: memory.Bitmap{29}},
	}
	f := memory.ListTime64Field("clocks", true, true)
	schema := memory.NewSchema(f)
	for _, e := range []expr.Expr{expr.ListGet(Col("clocks"), expr.I64(0)), expr.ListLength(Col("clocks"))} {
		if _, err := expr.Bind(schema, e); err == nil {
			t.Fatalf("unsupported List<TIME> expression bound: %v", e)
		}
	}
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	r, err := memory.ArrowRecord(alloc, memory.NewSchema(f), batch)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	root := t.TempDir()
	in, out := filepath.Join(root, "in.parquet"), filepath.Join(root, "out.parquet")
	fp, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()
	wr, err := pqarrow.NewFileWriter(r.Schema(), fp, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = wr.Write(r); err != nil {
		_ = wr.Close()
		t.Fatal(err)
	}
	if err = wr.Close(); err != nil {
		t.Fatal(err)
	}
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, err := s.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	res, err := q.SinkParquet(ctx, out, ParquetSinkOptions{RowGroupRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 4 || len(res.Files) != 1 || s.Engine().Memory().Used() != 0 {
		t.Fatalf("sink result %+v memory=%d", res, s.Engine().Memory().Used())
	}
	got, err := s.ScanParquetWithOptions(out, ParquetScanOptions{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	rd, err := got.CollectBatches(ctx, BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	field, ok := rd.Schema().Field("clocks")
	if !ok || !reflect.DeepEqual(field, f) {
		t.Fatalf("schema got %+v want %+v", field, f)
	}
	var rows [][]int64
	var valid [][]bool
	for rd.Next(ctx) {
		b := rd.Batch().state.batches[0]
		v := b.Int64Lists["clocks"]
		for i := 0; i < b.Rows; i++ {
			lo, hi, ok := v.ValueRange(i)
			if !ok {
				rows = append(rows, nil)
				valid = append(valid, nil)
				continue
			}
			rows = append(rows, append(make([]int64, 0, hi-lo), v.Values.Values[lo:hi]...))
			bits := make([]bool, hi-lo)
			for j := lo; j < hi; j++ {
				bits[j-lo] = v.Values.IsValid(j)
			}
			valid = append(valid, bits)
		}
	}
	if err := rd.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, [][]int64{{0, 0, day - 1}, nil, {}, {123456789}}) || !reflect.DeepEqual(valid, [][]bool{{true, false, true}, nil, {}, {true}}) {
		t.Fatalf("rows=%v valid=%v", rows, valid)
	}
	before, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	tinyOptions := engine.DefaultOptions()
	tinyOptions.MemoryLimitBytes = 1
	tiny := NewSession(tinyOptions)
	defer tiny.Close()
	tinyQ, err := tiny.ScanParquetWithOptions(in, ParquetScanOptions{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tinyQ.SinkParquet(ctx, out, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
	if !errors.Is(err, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
		t.Fatalf("budget err=%v memory=%d", err, tiny.Engine().Memory().Used())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = q.SinkParquet(canceled, out, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
	if !errors.Is(err, context.Canceled) || s.Engine().Memory().Used() != 0 {
		t.Fatalf("cancellation err=%v memory=%d", err, s.Engine().Memory().Used())
	}
	after, err := os.ReadFile(out)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("existing output changed: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatalf("staging leak: %v %v", entries, err)
	}

}
