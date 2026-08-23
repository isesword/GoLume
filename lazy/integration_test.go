package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

func TestLazyParquetAggregateEndToEnd(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.Workers = 4
	session := NewSession(opts)
	defer session.Close()

	frame, err := session.ScanParquet("../testdata/bench-1m.parquet")
	if err != nil {
		t.Fatal(err)
	}
	frame = frame.
		Filter(Col("pred").Gt(Lit(32767))).
		Select("key", "value").
		GroupBy("key").
		Agg(Sum("value", "sum"), Count("count"))

	optimized, err := frame.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(optimized, "columns=[pred, key, value]") {
		t.Fatalf("optimized explain did not retain predicate dependency:\n%s", optimized)
	}
	if !strings.Contains(optimized, "pushed_predicate=(pred > 32767)") {
		t.Fatalf("optimized explain missing pushed predicate:\n%s", optimized)
	}

	physical, err := frame.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(physical, "PUSHDOWN columns=[pred, key, value]") {
		t.Fatalf("physical explain missing projection report:\n%s", physical)
	}

	result, err := frame.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 219196 || result.SelectedRows != 472475 || result.Checksum != 236716301 {
		t.Fatalf("unexpected result groups=%d selected=%d checksum=%d", result.GroupCount, result.SelectedRows, result.Checksum)
	}
}

func TestLazyPreparedRepeatedExecution(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.Workers = 4
	session := NewSession(opts)
	defer session.Close()
	frame, err := session.ScanParquet("../testdata/bench-1m.parquet")
	if err != nil {
		t.Fatal(err)
	}
	frame = frame.
		Filter(Col("pred").Gt(Lit(32767))).
		GroupBy("key").
		Agg(Sum("value", "sum"), Count("count"))
	prepared, err := frame.Prepare(context.Background(), engine.PrepareOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	for i := 0; i < 2; i++ {
		result, err := prepared.Execute(context.Background(), engine.ExecuteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if result.GroupCount != 219196 || result.Checksum != 236716301 {
			t.Fatalf("run %d result=%+v", i, result)
		}
	}
}

func TestLazyRowGroupPruningEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune.parquet")
	writeLazyPruneFixture(t, path)
	opts := engine.DefaultOptions()
	opts.Workers = 2
	session := NewSession(opts)
	defer session.Close()
	frame, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	frame = frame.
		Filter(Col("pred").Gt(Lit(150))).
		Select("key", "value").
		WithHints(expr.Hints{Selectivity: .5, Cardinality: 17, ForceStrategy: "bitmap-direct"}).
		GroupBy("key").
		Agg(Sum("value", "sum"), Count("count"))
	physical, err := frame.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(physical, "row_groups=2/3 pruned=1 rows=200/300") {
		t.Fatalf("expected one pruned row group:\n%s", physical)
	}
	if strings.Contains(physical, "columns=[pred, key, value, unused]") {
		t.Fatalf("unused column survived physical projection:\n%s", physical)
	}
	result, err := frame.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 17 || result.SelectedRows != 149 || result.Checksum != 33674 {
		t.Fatalf("result groups=%d selected=%d checksum=%d", result.GroupCount, result.SelectedRows, result.Checksum)
	}
}

func writeLazyPruneFixture(t *testing.T, path string) {
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
	fileHandle, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := file.NewParquetWriter(fileHandle, root, file.WithWriterProps(props))
	for group := 0; group < 3; group++ {
		pred := make([]int64, 100)
		keys := make([]int32, 100)
		values := make([]int64, 100)
		unused := make([]int64, 100)
		base := group * 100
		for i := 0; i < 100; i++ {
			pred[i] = int64(base + i)
			keys[i] = int32((base + i) % 17)
			values[i] = int64(base + i + 1)
			unused[i] = int64(base + i + 10000)
		}
		rg := writer.AppendRowGroup()
		writeLazyI64(t, rg, pred)
		writeLazyI32(t, rg, keys)
		writeLazyI64(t, rg, values)
		writeLazyI64(t, rg, unused)
		if err := rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLazyI64(t *testing.T, rg file.SerialRowGroupWriter, values []int64) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLazyI32(t *testing.T, rg file.SerialRowGroupWriter, values []int32) {
	t.Helper()
	column, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := column.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := column.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLazyAggregateWithoutFilter(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32})
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 3}}
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 1
	session := NewSession(opts)
	defer session.Close()
	frame := session.FromSource(src).GroupBy("key").Agg(Count("rows"))
	result, err := frame.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 3 || result.SelectedRows != 4 {
		t.Fatalf("groups=%d selected=%d", result.GroupCount, result.SelectedRows)
	}
}
