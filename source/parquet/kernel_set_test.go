package parquet

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/source"
	"path/filepath"
	"testing"
	"time"
)

func TestBatchStreamRetainsExactKernelSetAndRejectsZeroOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kernels.parquet")
	writeTypedFixture(t, path, time.Unix(1, 234).UTC())
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.NewBatchStream(context.Background(), source.ReadOptions{}); err == nil {
		t.Fatal("zero ReadOptions must fail")
	}
	kernels := kernel.New(kernel.ModeSIMD)
	got, err := src.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	parquetReader, ok := got.(*batchStream)
	if !ok {
		t.Fatalf("reader type %T", got)
	}
	if parquetReader.decode.kernels != kernels {
		t.Fatal("Parquet reader did not retain the exact Set handle")
	}
}
