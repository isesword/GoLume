package engine_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/kernel"
	parquetsource "gocolumn/source/parquet"
	"testing"
)

func benchmarkStrategy(b *testing.B, strategy string, workers int) {
	src, err := parquetsource.Open(testPath(b), parquetsource.Columns{PredicateIndex: 0, GroupIndex: 1, ValueIndex: 2})
	if err != nil {
		b.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = workers
	opts.KernelMode = kernel.ModeAuto
	eng := engine.New(opts)
	defer eng.Close()
	q := query(strategy, workers)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if r.GroupCount != 219196 || r.Checksum != 236716301 || r.SelectedRows != 472475 {
			b.Fatal("correctness mismatch")
		}
	}
}

func BenchmarkDirectFused(b *testing.B)  { benchmarkStrategy(b, "direct-fused", 1) }
func BenchmarkBitmapDirect(b *testing.B) { benchmarkStrategy(b, "bitmap-direct", 1) }
func BenchmarkRadixW4(b *testing.B)      { benchmarkStrategy(b, "radix", 4) }

func BenchmarkPreparedRadixW4(b *testing.B) {
	src, err := parquetsource.Open(testPath(b), parquetsource.Columns{PredicateIndex: 0, GroupIndex: 1, ValueIndex: 2})
	if err != nil {
		b.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	opts.KernelMode = kernel.ModeAuto
	eng := engine.New(opts)
	defer eng.Close()
	p, err := eng.Prepare(context.Background(), src, query("radix", 4), engine.PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Execute(context.Background(), engine.ExecuteOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := p.Execute(context.Background(), engine.ExecuteOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if r.GroupCount != 219196 || r.Checksum != 236716301 || r.SelectedRows != 472475 {
			b.Fatal("correctness mismatch")
		}
	}
}
