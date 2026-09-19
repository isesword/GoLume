package fixturegen

import (
	"bytes"
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"

	"github.com/apache/arrow-go/v18/parquet/file"
)

func allTypesSpec(rows int64) DatasetSpec {
	truthy := 0.3
	return DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "i", Type: ColumnTypeInt64, Cardinality: 1000, NullRate: 0.1},
			{Name: "f", Type: ColumnTypeFloat64, FloatMin: -10, FloatMax: 10, NullRate: 0.1, NaNRate: 0.05},
			{Name: "s", Type: ColumnTypeString, Cardinality: 50, NullRate: 0.1},
			{Name: "b", Type: ColumnTypeBool, TrueRatio: &truthy, NullRate: 0.1},
		},
	}
}

func TestWriteParquetIsByteDeterministic(t *testing.T) {
	spec := allTypesSpec(8192)
	first, err := Generate(spec, 42)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	second, err := Generate(spec, 42)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	encode := func(table *Table) []byte {
		t.Helper()
		var buffer bytes.Buffer
		if err := WriteParquet(&buffer, table, ParquetOptions{Compression: CompressionSnappy}); err != nil {
			t.Fatalf("WriteParquet: %v", err)
		}
		return buffer.Bytes()
	}
	firstBytes := encode(first)
	secondBytes := encode(second)
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("identical seed and spec produced different Parquet bytes")
	}
	other, err := Generate(spec, 43)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if bytes.Equal(firstBytes, encode(other)) {
		t.Fatal("different seeds produced identical Parquet bytes")
	}
}

func TestWriteParquetNilArguments(t *testing.T) {
	table, err := Generate(allTypesSpec(16), 1)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := WriteParquet(nil, table, ParquetOptions{}); err == nil {
		t.Fatal("expected an error for a nil writer")
	}
	var buffer bytes.Buffer
	if err := WriteParquet(&buffer, nil, ParquetOptions{}); err == nil {
		t.Fatal("expected an error for a nil table")
	}
	if err := WriteParquet(&buffer, table, ParquetOptions{Compression: "brotli"}); err == nil {
		t.Fatal("expected an error for an unsupported codec")
	}
}

func TestWriteParquetFileOverwritePolicy(t *testing.T) {
	table, err := Generate(allTypesSpec(512), 5)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	size, err := WriteParquetFile(path, table, ParquetOptions{}, false)
	if err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}
	if size <= 0 {
		t.Fatalf("reported size %d, want positive", size)
	}
	if _, err := WriteParquetFile(path, table, ParquetOptions{}, false); !errors.Is(err, ErrTargetExists) {
		t.Fatalf("expected ErrTargetExists, got %v", err)
	}
	if _, err := WriteParquetFile(path, table, ParquetOptions{}, true); err != nil {
		t.Fatalf("overwrite write failed: %v", err)
	}
	if _, err := WriteParquetFile("", table, ParquetOptions{}, false); err == nil {
		t.Fatal("expected an error for an empty path")
	}
}

// TestParquetRowGroupsAndArrowReadback checks the physical layout and full
// row fidelity for every supported column type through the Arrow Parquet
// reader.
func TestParquetRowGroupsAndArrowReadback(t *testing.T) {
	const rows = 2500
	table, err := Generate(allTypesSpec(rows), 13)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	options := ParquetOptions{Compression: CompressionZstd, RowGroupRows: 1000}
	if _, err := WriteParquetFile(path, table, options, false); err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}
	reader, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("OpenParquetFile: %v", err)
	}
	defer func() { _ = reader.Close() }()
	meta := reader.MetaData()
	if meta.NumRows != rows {
		t.Fatalf("parquet rows=%d, want %d", meta.NumRows, rows)
	}
	if got := reader.NumRowGroups(); got != 3 {
		t.Fatalf("row groups=%d, want 3", got)
	}
	if got := meta.Schema.NumColumns(); got != 4 {
		t.Fatalf("columns=%d, want 4", got)
	}
}

// TestParquetReadBackThroughSource verifies that the engine's own
// source/parquet reader can open generated files and reproduces every value
// of every fixture column type (int64, float64, string, bool), including NULL
// placement and NaN fidelity.
func TestParquetReadBackThroughSource(t *testing.T) {
	const rows = 5000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "i", Type: ColumnTypeInt64, Cardinality: 700, NullRate: 0.2},
			{Name: "f", Type: ColumnTypeFloat64, FloatMin: -10, FloatMax: 10, NullRate: 0.2, NaNRate: 0.05},
			{Name: "s", Type: ColumnTypeString, Cardinality: 90, MinLen: 4, MaxLen: 10, NullRate: 0.2},
			{Name: "b", Type: ColumnTypeBool, NullRate: 0.2},
		},
	}
	table, err := Generate(spec, 19)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	if _, err := WriteParquetFile(path, table, ParquetOptions{RowGroupRows: 2048}, false); err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}
	src, err := parquetsource.OpenFile(path)
	if err != nil {
		t.Fatalf("source/parquet OpenFile: %v", err)
	}
	ctx := context.Background()
	stats, err := src.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Rows != rows {
		t.Fatalf("source stats rows=%d, want %d", stats.Rows, rows)
	}
	for name, want := range map[string]memory.DataType{
		"i": memory.TypeInt64,
		"f": memory.TypeFloat64,
		"s": memory.TypeString,
		"b": memory.TypeBool,
	} {
		field, ok := src.Schema().Field(name)
		if !ok || field.Type != want || !field.Nullable {
			t.Fatalf("unexpected schema field for %s: %+v present=%v", name, field, ok)
		}
	}
	parts, err := src.Partitions(ctx)
	if err != nil {
		t.Fatalf("Partitions: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("partitions=%d, want 3", len(parts))
	}
	reader, err := src.NewReader(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	wantInts := table.Batch().Int64s["i"]
	wantFloats := table.Batch().Float64s["f"]
	wantStrings := table.Batch().Strings["s"]
	wantBools := table.Batch().Uint32s["b"]
	batch := memory.NewBatch()
	offset := 0
	for _, partition := range parts {
		if err := reader.ReadPartition(ctx, partition, batch); err != nil {
			t.Fatalf("ReadPartition %d: %v", partition.ID, err)
		}
		gotInts := batch.Int64s["i"]
		gotFloats := batch.Float64s["f"]
		gotStrings := batch.Strings["s"]
		gotBools := batch.Uint32s["b"]
		for row := 0; row < batch.Rows; row++ {
			global := offset + row
			if gotInts.IsValid(row) != wantInts.IsValid(global) {
				t.Fatalf("row %d int validity mismatch", global)
			}
			if gotInts.IsValid(row) && gotInts.Values[row] != wantInts.Values[global] {
				t.Fatalf("row %d int value %d, want %d", global, gotInts.Values[row], wantInts.Values[global])
			}
			if gotFloats.IsValid(row) != wantFloats.IsValid(global) {
				t.Fatalf("row %d float validity mismatch", global)
			}
			if gotFloats.IsValid(row) {
				got, want := gotFloats.Values[row], wantFloats.Values[global]
				if math.IsNaN(got) != math.IsNaN(want) {
					t.Fatalf("row %d float NaN mismatch: %v vs %v", global, got, want)
				}
				if !math.IsNaN(want) && math.Float64bits(got) != math.Float64bits(want) {
					t.Fatalf("row %d float value %v, want %v", global, got, want)
				}
			}
			if gotStrings.IsValid(row) != wantStrings.IsValid(global) {
				t.Fatalf("row %d string validity mismatch", global)
			}
			if gotStrings.IsValid(row) && !bytes.Equal(gotStrings.ValueBytes(row), wantStrings.ValueBytes(global)) {
				t.Fatalf("row %d string value %q, want %q", global, gotStrings.ValueBytes(row), wantStrings.ValueBytes(global))
			}
			if gotBools.IsValid(row) != wantBools.IsValid(global) {
				t.Fatalf("row %d bool validity mismatch", global)
			}
			if gotBools.IsValid(row) && gotBools.Values[row] != wantBools.Values[global] {
				t.Fatalf("row %d bool value %d, want %d", global, gotBools.Values[row], wantBools.Values[global])
			}
		}
		offset += batch.Rows
	}
	if offset != rows {
		t.Fatalf("read back %d rows, want %d", offset, rows)
	}
}

// TestParquetPreservesNaN verifies NaN payloads survive the Parquet round
// trip through the Arrow file reader.
func TestParquetPreservesNaN(t *testing.T) {
	const rows = 10000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "f", Type: ColumnTypeFloat64, NaNRate: 0.5},
		},
	}
	table, err := Generate(spec, 41)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "fixture.parquet")
	if _, err := WriteParquetFile(path, table, ParquetOptions{}, false); err != nil {
		t.Fatalf("WriteParquetFile: %v", err)
	}
	reader, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("OpenParquetFile: %v", err)
	}
	defer func() { _ = reader.Close() }()
	chunk, err := reader.RowGroup(0).Column(0)
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	typed, ok := chunk.(*file.Float64ColumnChunkReader)
	if !ok {
		t.Fatalf("column reader is %T, want *file.Float64ColumnChunkReader", chunk)
	}
	values := make([]float64, rows)
	total, read, err := typed.ReadBatch(rows, values, nil, nil)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if int(total) != rows || read != rows {
		t.Fatalf("read total=%d values=%d, want %d", total, read, rows)
	}
	nans := 0
	for row, value := range values {
		if math.IsNaN(value) {
			nans++
		}
		if math.IsNaN(value) != math.IsNaN(table.Batch().Float64s["f"].Values[row]) {
			t.Fatalf("row %d NaN mismatch after round trip", row)
		}
	}
	if nans == 0 {
		t.Fatal("expected NaN values after round trip")
	}
}
