package engine_test

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	parquetsource "gocolumn/source/parquet"
	"testing"
)

func TestRadixHonorsMemoryBudget(t *testing.T) {
	src, err := parquetsource.OpenFile(testPath(t))
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	opts.MemoryLimitBytes = 1024
	eng := engine.New(opts)
	defer eng.Close()
	_, err = eng.Execute(context.Background(), src, query("radix", 4), engine.ExecuteOptions{})
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("expected memory budget error, got %v", err)
	}
	if eng.Memory().Used() != 0 {
		t.Fatalf("memory reservation leak: used=%d", eng.Memory().Used())
	}
}
