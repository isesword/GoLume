package parquet

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
)

func TestOpenFileReadsNativeParquetIntegerLogicalTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "narrow.parquet")
	i8, err := schema.NewPrimitiveNodeLogical("i8", arrowparquet.Repetitions.Optional, schema.NewIntLogicalType(8, true), arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	u16, err := schema.NewPrimitiveNodeLogical("u16", arrowparquet.Repetitions.Required, schema.NewIntLogicalType(16, false), arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{i8, u16}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithCompression(compress.Codecs.Uncompressed))))
	rg := w.AppendRowGroup()
	c, _ := rg.NextColumn()
	if _, err := c.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{-128, 127}, []int16{1, 0, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, _ = rg.NextColumn()
	if _, err := c.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{1, 65535, 42}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
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
	if src.Schema().Fields[0].Type != memory.TypeInt8 || src.Schema().Fields[1].Type != memory.TypeUint16 {
		t.Fatalf("schema=%+v", src.Schema().Fields)
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
	vi, err := b.Int8("i8")
	if err != nil {
		t.Fatal(err)
	}
	vu, err := b.Uint16("u16")
	if err != nil {
		t.Fatal(err)
	}
	if len(vi.Values) != 3 || vi.Values[0] != -128 || vi.Values[1] != 0 || vi.Values[2] != 127 || vi.IsValid(1) {
		t.Fatalf("i8=%v validity=%v", vi.Values, vi.Validity)
	}
	if len(vu.Values) != 3 || vu.Values[1] != 65535 {
		t.Fatalf("u16=%v", vu.Values)
	}
}

func TestParquetRejectsValueOutsideLogicalIntegerWidth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-narrow.parquet")
	node, err := schema.NewPrimitiveNodeLogical("x", arrowparquet.Repetitions.Required, schema.NewIntLogicalType(8, true), arrowparquet.Types.Int32, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{node}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root)
	rg := w.AppendRowGroup()
	c, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{128}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
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
	r, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(context.Background(), memory.NewBatch()); err == nil {
		t.Fatal("expected overflow rejection instead of int8 wrapping")
	}
}
