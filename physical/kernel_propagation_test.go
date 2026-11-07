package physical

import (
	"context"
	"gocolumn/expr"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

type kernelRecordingSource struct {
	source.Source
	seen        []kernel.Set
	seenOptions []source.ReadOptions
}

func (s *kernelRecordingSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	s.seen = append(s.seen, opts.Kernels)
	s.seenOptions = append(s.seenOptions, opts)
	return source.OpenBatchStream(ctx, s.Source, opts)
}

func propagationBase(t *testing.T) source.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	batch := memory.NewBatch()
	batch.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	batch.Reset(3)
	base, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	return base
}

func TestPhysicalWrappersPropagateExactKernelSet(t *testing.T) {
	kernels := kernel.New(kernel.ModeSIMD)
	opts := source.ReadOptions{Kernels: kernels, BatchRows: 3, BatchBytes: 128, MemoryManager: memory.NewBudgetManager(1 << 20)}
	factories := []struct {
		name string
		new  func(source.Source) (source.Source, error)
	}{
		{"profile", func(input source.Source) (source.Source, error) { return &ProfileSource{input: input}, nil }},
		{"filter", func(input source.Source) (source.Source, error) {
			return NewFilterSource(input, expr.GT(expr.Col("value"), expr.I64(0)))
		}},
		{"project", func(input source.Source) (source.Source, error) {
			return NewProjectSource(input, []expr.NamedExpr{expr.Named(expr.Col("value"), "projected")}, false)
		}},
		{"drop", func(input source.Source) (source.Source, error) {
			return NewDropColumnsSource(input, []string{"value"})
		}},
		{"sort", func(input source.Source) (source.Source, error) {
			return NewSortSource(input, []SortKey{{Column: "id"}}, nil)
		}},
		{"limit", func(input source.Source) (source.Source, error) { return NewLimitSource(input, 2) }},
	}
	for _, test := range factories {
		t.Run(test.name, func(t *testing.T) {
			recorder := &kernelRecordingSource{Source: propagationBase(t)}
			wrapped, err := test.new(recorder)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := source.OpenBatchStream(context.Background(), wrapped, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if len(recorder.seen) != 1 || recorder.seen[0] != kernels {
				t.Fatalf("received kernel sets=%v, want exact engine handle", recorder.seen)
			}
			if len(recorder.seenOptions) != 1 || recorder.seenOptions[0] != opts {
				t.Fatalf("received read options=%+v want=%+v", recorder.seenOptions, opts)
			}
			if _, err := source.OpenBatchStream(context.Background(), wrapped, source.ReadOptions{}); err == nil {
				t.Fatal("zero ReadOptions must fail")
			}
		})
	}
}

func TestJoinPropagatesAndRequiresExactKernelSet(t *testing.T) {
	kernels := kernel.New(kernel.ModeSIMD)
	left := &kernelRecordingSource{Source: propagationBase(t)}
	right := &kernelRecordingSource{Source: propagationBase(t)}
	joined, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "_right", nil, kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Close()
	if len(right.seen) != 1 || right.seen[0] != kernels {
		t.Fatalf("build side received %v", right.seen)
	}
	reader, err := source.OpenBatchStream(context.Background(), joined, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if len(left.seen) != 1 || left.seen[0] != kernels {
		t.Fatalf("probe side received %v", left.seen)
	}
	if _, err := source.OpenBatchStream(context.Background(), joined, source.ReadOptions{Kernels: kernel.New(kernel.ModeSIMD)}); err == nil {
		t.Fatal("join accepted a different Set with the same mode")
	}
}

func TestJoinStrategiesPropagateCompleteReadOptions(t *testing.T) {
	for _, strategy := range []string{"merge", "spill"} {
		t.Run(strategy, func(t *testing.T) {
			kernels := kernel.New(kernel.ModeScalar)
			left := &kernelRecordingSource{Source: propagationBase(t)}
			right := &kernelRecordingSource{Source: propagationBase(t)}
			var leftSource, rightSource source.Source = left, right
			if strategy == "merge" {
				leftSource = &orderedReadOptionsSource{kernelRecordingSource: left}
				rightSource = &orderedReadOptionsSource{kernelRecordingSource: right}
			}
			root := memory.NewEngineBudget(1 << 20)
			budget, err := root.BeginQuery(memory.BudgetOptions{Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			ctx := memory.WithQueryBudget(context.Background(), budget)
			joined, err := NewJoinSourceWithOptions(ctx, leftSource, rightSource, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "_right", budget, JoinSourceOptions{Kernels: kernels, Spillable: strategy == "spill", SpillConfigured: true, MemoryLimitBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer joined.Close()
			if strategy == "merge" && !joined.merge || strategy == "spill" && !joined.spillable {
				t.Fatalf("unexpected strategy %s", joined.Strategy())
			}
			opts := source.ReadOptions{Kernels: kernels, BatchRows: 3, BatchBytes: 128, MemoryManager: budget.Operator("SourceScan", memory.MemoryWorkspace)}
			stream, err := joined.NewBatchStream(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.DrainBatchStream(ctx, stream); err != nil {
				t.Fatal(err)
			}
			for _, recorder := range []*kernelRecordingSource{left, right} {
				if len(recorder.seenOptions) != 1 || recorder.seenOptions[0] != opts {
					t.Fatalf("received options=%+v want=%+v", recorder.seenOptions, opts)
				}
			}
		})
	}
}

type orderedReadOptionsSource struct{ *kernelRecordingSource }

func (*orderedReadOptionsSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "id"}}
}
