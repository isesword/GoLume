package parquet

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
)

func TestOpenFileReadsNativeFloat32(t *testing.T) {
	path := filepath.Join(t.TempDir(), "float32.parquet")
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{
		schema.NewFloat32Node("f", arrowparquet.Repetitions.Optional, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithCompression(compress.Codecs.Uncompressed))))
	rg := w.AppendRowGroup()
	col, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.(*file.Float32ColumnChunkWriter).WriteBatch([]float32{1.5, -0.0, float32(math.NaN())}, []int16{1, 0, 1, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := src.Schema().Fields[0].Type; got != memory.TypeFloat32 {
		t.Fatalf("schema type=%s", got)
	}
	r, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b := memory.NewBatch()
	if ok, err := r.Next(context.Background(), b); !ok || err != nil {
		t.Fatalf("first batch: ok=%v err=%v", ok, err)
	}
	v, err := b.Float32("f")
	if err != nil {
		t.Fatal(err)
	}
	if v.Values[0] != 1.5 || v.Values[1] != 0 || math.Float32bits(v.Values[1]) != 0 || !math.IsNaN(float64(v.Values[3])) || v.IsValid(1) {
		t.Fatalf("values=%v validity=%v", v.Values, v.Validity)
	}
}
