package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	parquetsource "gocolumn/source/parquet"
	"testing"
)

func BenchmarkPreparedLazyParquet(b *testing.B) {
	benchmarkPreparedLazyParquet(b, kernel.ModeAuto)
}

func BenchmarkPreparedLazyParquetKernelMode(b *testing.B) {
	for _, choice := range []struct {
		name string
		mode kernel.Mode
	}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
		b.Run(choice.name, func(b *testing.B) { benchmarkPreparedLazyParquet(b, choice.mode) })
	}
}

func benchmarkPreparedLazyParquet(b *testing.B, mode kernel.Mode) {
	opts := engine.DefaultOptions()
	opts.Workers = 4
	opts.KernelMode = mode
	session := NewSession(opts)
	defer session.Close()
	frame, err := session.ScanParquet("../testdata/bench-1m.parquet")
	if err != nil {
		b.Fatal(err)
	}
	frame = frame.
		Filter(Col("pred").Gt(Lit(32767))).
		Select("key", "value").
		WithHints(expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: "radix", ForceWorkerCount: 4}).
		GroupBy("key").
		Agg(Sum("value", "sum"), Count("count"))
	prepared, err := frame.Prepare(context.Background(), engine.PrepareOptions{MaxConcurrency: 1})
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
		result, err := prepared.Execute(context.Background(), engine.ExecuteOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if result.GroupCount != 219196 || result.SelectedRows != 472475 || result.Checksum != 236716301 {
			b.Fatal("correctness mismatch")
		}
	}
}

func BenchmarkPreparedRadixJoinKernelMode(b *testing.B) {
	for _, choice := range []struct {
		name string
		mode kernel.Mode
	}{{"mode=scalar", kernel.ModeScalar}, {"mode=auto", kernel.ModeAuto}} {
		b.Run(choice.name, func(b *testing.B) {
			const leftRows, rightRows = 1 << 16, 1 << 14
			leftBatch, rightBatch := memory.NewBatch(), memory.NewBatch()
			leftKeys, rightKeys := make([]int32, leftRows), make([]int32, rightRows)
			for i := range leftKeys {
				leftKeys[i] = int32(i % rightRows)
			}
			for i := range rightKeys {
				rightKeys[i] = int32(i)
			}
			leftBatch.Int32s["key"] = &memory.Int32Vector{Values: leftKeys}
			rightBatch.Int32s["key"] = &memory.Int32Vector{Values: rightKeys}
			leftBatch.Reset(leftRows)
			rightBatch.Reset(rightRows)
			schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32})
			left, err := inmemory.New(schema, leftBatch)
			if err != nil {
				b.Fatal(err)
			}
			right, err := inmemory.New(schema, rightBatch)
			if err != nil {
				b.Fatal(err)
			}
			opts := engine.DefaultOptions()
			opts.Workers, opts.KernelMode = 4, choice.mode
			session := NewSession(opts)
			defer session.Close()
			frame := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}, Suffix: "_right", RadixBits: 8, Workers: 4})
			prepared, err := frame.PrepareFrame(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			defer prepared.Close()
			warm, err := prepared.Execute(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			_ = warm.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := prepared.Execute(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if got.Rows != leftRows {
					b.Fatalf("rows=%d want=%d", got.Rows, leftRows)
				}
				_ = got.Close()
			}
		})
	}
}

func BenchmarkPreparedExecutionLayerComparison(b *testing.B) {
	ctx := context.Background()
	query := expr.Query{
		Filter:     expr.GT(expr.Col("pred"), expr.Lit(32767)),
		GroupBy:    []string{"key"},
		Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")},
		Hints:      expr.Hints{Selectivity: .451, Cardinality: 1 << 18, ForceStrategy: "radix", ForceWorkerCount: 4},
	}

	b.Run("direct-engine-prepared", func(b *testing.B) {
		src, err := parquetsource.OpenFile("../testdata/bench-1m.parquet")
		if err != nil {
			b.Fatal(err)
		}
		pushed, _, err := src.ApplyPushdown(ctx, source.PushdownRequest{Projection: []string{"pred", "key", "value"}, Predicate: query.Filter})
		if err != nil {
			b.Fatal(err)
		}
		opts := engine.DefaultOptions()
		opts.Workers = 4
		eng := engine.New(opts)
		defer eng.Close()
		prepared, err := eng.Prepare(ctx, pushed, query, engine.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer prepared.Close()
		if _, err := prepared.Execute(ctx, engine.ExecuteOptions{}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if result.GroupCount != 219196 {
				b.Fatal("correctness mismatch")
			}
		}
	})

	b.Run("lazy-prepared", func(b *testing.B) {
		opts := engine.DefaultOptions()
		opts.Workers = 4
		session := NewSession(opts)
		defer session.Close()
		frame, err := session.ScanParquet("../testdata/bench-1m.parquet")
		if err != nil {
			b.Fatal(err)
		}
		frame = frame.Filter(Col("pred").Gt(Lit(32767))).WithHints(query.Hints).GroupBy("key").Agg(Sum("value", "sum"), Count("count"))
		prepared, err := frame.Prepare(ctx, engine.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer prepared.Close()
		if _, err := prepared.Execute(ctx, engine.ExecuteOptions{}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
			if err != nil {
				b.Fatal(err)
			}
			if result.GroupCount != 219196 {
				b.Fatal("correctness mismatch")
			}
		}
	})
}

func BenchmarkJoinFramePreparationComparison(b *testing.B) {
	const leftRows = 1 << 16
	const rightRows = 1 << 14
	leftBatch := memory.NewBatch()
	leftBatch.Rows = leftRows
	leftKeys := make([]int32, leftRows)
	leftValues := make([]int64, leftRows)
	for i := range leftKeys {
		leftKeys[i] = int32(i % rightRows)
		leftValues[i] = int64(i)
	}
	leftBatch.Int32s["key"] = &memory.Int32Vector{Values: leftKeys}
	leftBatch.Int64s["value"] = &memory.Int64Vector{Values: leftValues}
	left, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), leftBatch)
	if err != nil {
		b.Fatal(err)
	}
	rightBatch := memory.NewBatch()
	rightBatch.Rows = rightRows
	rightKeys := make([]int32, rightRows)
	rightValues := make([]int64, rightRows)
	for i := range rightKeys {
		rightKeys[i] = int32(i)
		rightValues[i] = int64(i * 2)
	}
	rightBatch.Int32s["key"] = &memory.Int32Vector{Values: rightKeys}
	rightBatch.Int64s["bonus"] = &memory.Int64Vector{Values: rightValues}
	right, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "bonus", Type: memory.TypeInt64},
	), rightBatch)
	if err != nil {
		b.Fatal(err)
	}

	opts := engine.DefaultOptions()
	opts.Workers = 4
	session := NewSession(opts)
	defer session.Close()
	lf := session.FromSource(left).
		Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).
		WithColumns(Col("value").Add(Col("bonus")).Alias("total")).
		SelectExprs(Col("key").Alias("key"), Col("total").Alias("total"))

	b.Run("one-shot-collect-frame", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			frame, err := lf.CollectFrame(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if frame.Rows != leftRows {
				b.Fatalf("rows=%d want=%d", frame.Rows, leftRows)
			}
		}
	})

	b.Run("prepared-frame", func(b *testing.B) {
		prepared, err := lf.PrepareFrame(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		defer prepared.Close()
		if _, err := prepared.Execute(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			frame, err := prepared.Execute(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if frame.Rows != leftRows {
				b.Fatalf("rows=%d want=%d", frame.Rows, leftRows)
			}
		}
	})
}
