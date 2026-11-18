package parquet

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
)

func TestN2ZeroOptionsPreserveFlatRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flat.parquet")
	writeBatchStreamFixture(t, path, []int{0, 1, 65, 131, 0}, 257)
	legacy, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := OpenFileWithOptions(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy.Schema(), zero.Schema()) {
		t.Fatal("zero options changed flat schema")
	}
	want := arrowBatchStreamRows(t, legacy)
	for _, src := range []*Source{legacy, zero} {
		m := memory.NewBudgetManager(1 << 20)
		opts := testReadOptions()
		opts.BatchRows, opts.BatchBytes, opts.MemoryManager = 7, 32<<10, m
		st, err := source.OpenBatchStream(context.Background(), src, opts)
		if err != nil {
			t.Fatal(err)
		}
		got := collectBatchStreamRows(t, st, src.Schema(), opts)
		if !reflect.DeepEqual(got, want) {
			t.Fatal("zero/default flat rows differ from Arrow")
		}
		if m.Used() != 0 {
			t.Fatal("zero/default flat lease leak", m.Used())
		}
	}
}
