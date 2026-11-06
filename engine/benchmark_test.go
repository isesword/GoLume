package engine_test

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	parquetsource "gocolumn/source/parquet"
	"testing"
)

func benchmarkStrategy(b *testing.B, strategy string, workers int) {
	src, err := parquetsource.OpenFile(testPath(b))
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
func BenchmarkStreamW4(b *testing.B)     { benchmarkStrategy(b, "", 4) }

func BenchmarkPreparedStreamW4(b *testing.B) {
	benchmarkPreparedStreamW4(b, kernel.ModeAuto)
}

func BenchmarkPreparedStreamKernelMode(b *testing.B) {
	for _, mode := range []struct {
		name string
		mode kernel.Mode
	}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
		b.Run(mode.name, func(b *testing.B) { benchmarkPreparedStreamW4(b, mode.mode) })
	}
}

func benchmarkPreparedStreamW4(b *testing.B, mode kernel.Mode) {
	src, err := parquetsource.OpenFile(testPath(b))
	if err != nil {
		b.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	opts.KernelMode = mode
	eng := engine.New(opts)
	defer eng.Close()
	p, err := eng.Prepare(context.Background(), src, query("", 4), engine.PrepareOptions{MaxConcurrency: 1})
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

func BenchmarkInt32ComparisonKernelMode(b *testing.B) {
	const rows = 1 << 20
	batch := memory.NewBatch()
	batch.Rows = rows
	batch.Int32s["pred"] = &memory.Int32Vector{Values: make([]int32, rows)}
	batch.Int32s["key"] = &memory.Int32Vector{Values: make([]int32, rows)}
	batch.Int64s["value"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for row := 0; row < rows; row++ {
		batch.Int32s["pred"].Values[row] = int32((uint32(row)*1664525 + 1013904223) % 100)
		batch.Int32s["key"].Values[row] = int32(row & 1023)
		batch.Int64s["value"].Values[row] = 1
	}
	schema := memory.NewSchema(
		memory.Field{Name: "pred", Type: memory.TypeInt32},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		b.Fatal(err)
	}
	for _, strategy := range []string{"direct-fused", "bitmap-direct"} {
		for _, selectedPercent := range []int{1, 10, 50, 90, 100} {
			threshold := int64(99 - selectedPercent)
			if selectedPercent == 100 {
				threshold = -1
			}
			for _, mode := range []struct {
				name string
				mode kernel.Mode
			}{{"mode=scalar", kernel.ModeScalar}, {"mode=simd", kernel.ModeSIMD}} {
				b.Run(strategy+"/select="+benchmarkPercent(selectedPercent)+"/"+mode.name, func(b *testing.B) {
					opts := engine.DefaultOptions()
					opts.KernelMode = mode.mode
					eng := engine.New(opts)
					defer eng.Close()
					query := expr.Query{
						Filter: expr.GT(expr.Col("pred"), expr.I64(threshold)), GroupBy: []string{"key"},
						Aggregates: []expr.Aggregate{expr.Sum("value", "sum")},
						Hints:      expr.Hints{ForceStrategy: strategy, Cardinality: 1024, Selectivity: float64(selectedPercent) / 100},
					}
					prepared, err := eng.Prepare(context.Background(), src, query, engine.PrepareOptions{MaxConcurrency: 1})
					if err != nil {
						b.Fatal(err)
					}
					defer prepared.Close()
					if _, err := prepared.Execute(context.Background(), engine.ExecuteOptions{}); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := prepared.Execute(context.Background(), engine.ExecuteOptions{}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func benchmarkPercent(percent int) string {
	if percent == 100 {
		return "100"
	}
	if percent >= 10 {
		return string([]byte{'0' + byte(percent/10), '0' + byte(percent%10)})
	}
	return string([]byte{'0' + byte(percent)})
}
