package lazy

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

func TestNativeDecimal128ScanCollectAndLifecycle(t *testing.T) {
	field, _ := memory.Decimal128Field("amount", true, 38, 12)
	srcBatch := memory.NewBatch()
	srcBatch.Rows = 3
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	srcBatch.Decimals[field.Name] = &memory.Decimal128Vector{Values: []memory.Decimal128{max, {}, max.Negate()}, Validity: memory.Bitmap{5}, Precision: 38, Scale: 12}
	src, err := inmemory.New(memory.NewSchema(field), srcBatch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 1
	session := NewSession(opts)
	defer session.Close()
	frame, err := session.FromSource(src).Limit(2).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	series, err := frame.Column("amount")
	if err != nil {
		t.Fatal(err)
	}
	if series.NullCount() != 1 {
		t.Fatalf("nulls=%d", series.NullCount())
	}
	chunks, err := series.Decimal128Chunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0].Precision() != 38 || chunks[0].Scale() != 12 || chunks[0].Len() != 2 {
		t.Fatal("lost decimal chunk metadata")
	}
	if got, valid, err := chunks[0].Value(0); err != nil || !valid || got != max {
		t.Fatalf("coefficient=%v valid=%v error=%v", got, valid, err)
	}
	srcBatch.Decimals[field.Name].Values[0] = decimal128.FromI64(17)
	if got, _, _ := chunks[0].Value(0); got != max {
		t.Fatal("Collect returned borrowed coefficient")
	}
	if _, _, err := chunks[0].Value(2); err == nil {
		t.Fatal("out of bounds value accepted")
	}
	if _, err := series.Float64Chunks(); err == nil {
		t.Fatal("decimal implicitly converted to float")
	}
	frame.Close()
	if _, _, err := chunks[0].Value(0); !errors.Is(err, ErrDataFrameClosed) {
		t.Fatalf("expired chunk error=%v", err)
	}
	if session.Engine().Memory().Used() != 0 {
		t.Fatal("decimal Collect leaked budget")
	}
}

func TestNativeDecimal128SinkParquetAndReadBack(t *testing.T) {
	field, _ := memory.Decimal128Field("amount", true, 38, 18)
	batch := memory.NewBatch()
	batch.Rows = 3
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	batch.Decimals[field.Name] = &memory.Decimal128Vector{Values: []memory.Decimal128{max, {}, max.Negate()}, Validity: memory.Bitmap{5}, Precision: 38, Scale: 18}
	src, err := inmemory.New(memory.NewSchema(field), batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 1
	session := NewSession(opts)
	defer session.Close()
	path := filepath.Join(t.TempDir(), "exact.parquet")
	if _, err := session.FromSource(src).SinkParquet(context.Background(), path, ParquetSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	scan, err := session.ScanParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scan.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	series, err := result.Column(field.Name)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := series.Decimal128Chunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || series.NullCount() != 1 {
		t.Fatal("round trip shape mismatch")
	}
	for i, want := range batch.Decimals[field.Name].Values {
		got, valid, err := chunks[0].Value(i)
		if err != nil || valid != (i != 1) || valid && got != want || chunks[0].Scale() != 18 || chunks[0].Precision() != 38 {
			t.Fatalf("row=%d got=%v valid=%v err=%v", i, got, valid, err)
		}
	}
}
