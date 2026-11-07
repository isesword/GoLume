package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

func expressionDomainSource(t *testing.T) *inmemory.Source {
	t.Helper()
	batch := memory.NewBatch()
	batch.Rows = 3
	batch.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 300, 400}}
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 1}}
	batch.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{1, 0, 0}, Validity: memory.Bitmap{3}}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "x", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "keep", Type: memory.TypeBool, Nullable: true},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestLazySequentialFiltersKeepNullAndErrorDomainsAcrossTerminals(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(mode.String(), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode
			session := NewSession(opts)
			defer session.Close()
			frame := session.FromSource(expressionDomainSource(t)).
				Filter(expr.Col("keep")).
				Filter(expr.GT(expr.CastTo(expr.Col("x"), memory.TypeInt8), expr.I64(0)))
			ctx := context.Background()
			data, err := frame.Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if rows, _ := data.Shape(); rows != 1 {
				t.Fatalf("collect rows=%d", rows)
			}
			data.Close()
			report, err := frame.Analyze(ctx)
			if err != nil || report.OutputRows != 1 {
				t.Fatalf("analyze report=%+v err=%v", report, err)
			}
			agg := frame.GroupBy("key").Agg(expr.Count("rows"))
			result, err := agg.CollectResult(ctx)
			if err != nil || result.SelectedRows != 1 || result.GroupCount != 1 {
				t.Fatalf("aggregate result=%+v err=%v", result, err)
			}
			prepared, err := agg.PrepareResult(ctx, engine.PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Close()
			for i := 0; i < 2; i++ {
				result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
				if err != nil || result.SelectedRows != 1 || result.GroupCount != 1 {
					t.Fatalf("prepared run %d result=%+v err=%v", i, result, err)
				}
			}
		})
	}
}

func TestLazyRetainedAndUnusedFallibleProjectionDemand(t *testing.T) {
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame := session.FromSource(expressionDomainSource(t)).WithColumns(expr.Named(expr.CastTo(expr.Col("x"), memory.TypeInt8), "checked"))
	if out, err := frame.Filter(expr.Bool(false)).Collect(context.Background()); err == nil {
		out.Close()
		t.Fatal("FALSE filter suppressed a retained projection error")
	}
	unused, err := frame.Select("key").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unused.Close()
	if rows, _ := unused.Shape(); rows != 3 {
		t.Fatalf("unused checked output was demanded, rows=%d", rows)
	}
}
