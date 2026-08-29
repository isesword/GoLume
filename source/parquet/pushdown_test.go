package parquet

import (
	"context"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func TestApplyPushdownProjectionAndRowGroupPruning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pushdown.parquet")
	writePushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"pred", "key", "value"},
		Predicate:  expr.GT(expr.Col("pred"), expr.Lit(150)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.TotalPartitions != 3 || report.SelectedPartitions != 2 || report.PrunedPartitions != 1 {
		t.Fatalf("partition report=%+v", report)
	}
	if report.TotalRows != 300 || report.SelectedRows != 200 || report.PrunedRows != 100 {
		t.Fatalf("row report=%+v", report)
	}
	if report.TotalBytes <= 0 || report.SelectedBytes <= 0 || report.SelectedBytes >= report.TotalBytes {
		t.Fatalf("expected projection/pruning to reduce compressed column bytes: %+v", report)
	}
	if len(pushed.Schema().Fields) != 3 {
		t.Fatalf("schema=%+v", pushed.Schema().Fields)
	}
	if _, ok := pushed.Schema().Field("unused"); ok {
		t.Fatal("unused column survived projection pushdown")
	}
	parts, err := pushed.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].ID != 1 || parts[1].ID != 2 {
		t.Fatalf("parts=%v", parts)
	}

	r, err := pushed.NewReader(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	batch := newTestBatch()
	if err := r.ReadPartition(context.Background(), parts[0], batch); err != nil {
		t.Fatal(err)
	}
	if _, ok := batch.Int64s["unused"]; ok {
		t.Fatal("reader decoded unused column")
	}
	if len(batch.Int64s["pred"].Values) != 100 || batch.Int64s["pred"].Values[0] != 100 {
		t.Fatalf("unexpected decoded predicate values")
	}
}

func TestApplyPushdownConservativeOr(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pushdown.parquet")
	writePushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{
		Projection: []string{"pred", "key"},
		Predicate: expr.Or(
			expr.LT(expr.Col("pred"), expr.Lit(50)),
			expr.GT(expr.Col("pred"), expr.Lit(250)),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SelectedPartitions != 2 || report.PrunedPartitions != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func newTestBatch() *memory.Batch { return memory.NewBatch() }

func writePushdownFixture(t *testing.T, path string) {
	t.Helper()
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("pred", arrowparquet.Repetitions.Required, -1),
		schema.NewInt32Node("key", arrowparquet.Repetitions.Required, -1),
		schema.NewInt64Node("value", arrowparquet.Repetitions.Required, -1),
		schema.NewInt64Node("unused", arrowparquet.Repetitions.Required, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	props := arrowparquet.NewWriterProperties(
		arrowparquet.WithCompression(compress.Codecs.Uncompressed),
		arrowparquet.WithDictionaryDefault(false),
		arrowparquet.WithMaxRowGroupLength(100),
	)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for rgIndex := 0; rgIndex < 3; rgIndex++ {
		pred := make([]int64, 100)
		key := make([]int32, 100)
		value := make([]int64, 100)
		unused := make([]int64, 100)
		base := rgIndex * 100
		for i := range pred {
			pred[i] = int64(base + i)
			key[i] = int32((base + i) % 17)
			value[i] = int64(base + i + 1)
			unused[i] = int64(100000 + base + i)
		}
		rg := w.AppendRowGroup()
		writeI64Column(t, rg, pred)
		writeI32Column(t, rg, key)
		writeI64Column(t, rg, value)
		writeI64Column(t, rg, unused)
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeI64Column(t *testing.T, rg file.SerialRowGroupWriter, values []int64) {
	t.Helper()
	col, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeI32Column(t *testing.T, rg file.SerialRowGroupWriter, values []int32) {
	t.Helper()
	col, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
}
