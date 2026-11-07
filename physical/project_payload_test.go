package physical

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"strings"
	"testing"
)

type projectRequestLimit struct {
	base   *memory.BudgetManager
	max    int64
	denied int
}

func (m *projectRequestLimit) Used() int64  { return m.base.Used() }
func (m *projectRequestLimit) Limit() int64 { return m.base.Limit() }

func (m *projectRequestLimit) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if n > m.max {
		m.denied++
		return nil, memory.ErrBudgetExceeded
	}
	return m.base.Reserve(ctx, n)
}

func TestProjectRetriesBudgetCandidatesWithoutAdvancingInput(t *testing.T) {
	input := memory.NewBatch()
	input.Rows = 200
	input.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 200)}
	for i := range input.Int64s["id"].Values {
		input.Int64s["id"].Values[i] = int64(i)
	}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), input)
	if err != nil {
		t.Fatal(err)
	}
	manager := &projectRequestLimit{base: memory.NewBudgetManager(1 << 20), max: 4096}
	literal := strings.Repeat("x", 100)
	projected, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Str(literal), "text")}, true, manager)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := projected.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	count := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < out.Rows; row++ {
			if out.Int64s["id"].Values[row] != int64(count) || string(out.Strings["text"].ValueBytes(row)) != literal {
				t.Fatalf("output row %d", count)
			}
			count++
		}
	}
	if count != 200 || manager.denied == 0 {
		t.Fatalf("rows=%d denied=%d", count, manager.denied)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.base.Used() != 0 {
		t.Fatalf("candidate workspace leaked %d", manager.base.Used())
	}
}

func TestProjectLiteralPayloadRangeAndSingleRowFailure(t *testing.T) {
	for _, limit := range []int64{256, 64} {
		t.Run(map[int64]string{256: "bounded", 64: "oversize"}[limit], func(t *testing.T) {
			input := memory.NewBatch()
			input.Rows = 70
			input.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 70)}
			for i := range input.Int64s["id"].Values {
				input.Int64s["id"].Values[i] = int64(i)
			}
			base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), input)
			if err != nil {
				t.Fatal(err)
			}
			manager := memory.NewBudgetManager(1 << 20)
			literal := strings.Repeat("z", 100)
			projected, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Str(literal), "text"), expr.Named(expr.Col("id"), "alias")}, true, manager)
			if err != nil {
				t.Fatal(err)
			}
			opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 70, BatchPayloadBytes: limit, BatchPayloadTargetBytes: limit / 2, MemoryManager: manager}
			stream, err := projected.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			count := 0
			for {
				ok, err := stream.Next(context.Background(), out)
				if limit == 64 {
					if ok || !errors.Is(err, memory.ErrBudgetExceeded) || out.Rows != 0 {
						t.Fatalf("oversize=(%v,%v) rows=%d", ok, err, out.Rows)
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				size, err := memory.EstimateBatchRangeBytes(out, 0, out.Rows)
				if err != nil {
					t.Fatal(err)
				}
				if size > limit || out.Rows > 1 && size > limit/2 {
					t.Fatalf("payload=%d rows=%d", size, out.Rows)
				}
				for row := 0; row < out.Rows; row++ {
					if out.Int64s["id"].Values[row] != int64(count) || out.Int64s["alias"].Values[row] != int64(count) || string(out.Strings["text"].ValueBytes(row)) != literal {
						t.Fatalf("row %d", count)
					}
					count++
				}
			}
			if limit == 256 && count != 70 {
				t.Fatalf("rows=%d", count)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.Used() != 0 {
				t.Fatalf("workspace leak=%d", manager.Used())
			}
		})
	}
}

func TestProjectRetryPreservesSourceOwnedArrowSlice(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{10, 20, 30, 40}, nil)
	values := builder.NewInt64Array()
	builder.Release()
	defer values.Release()
	input := memory.NewBatch()
	input.Rows = 4
	input.Opaque["raw"] = &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, ArrowType: "int64", Value: values}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "int64"}), input)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(1 << 20)
	literal := strings.Repeat("x", 100)
	projected, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Str(literal), "text")}, true, manager)
	if err != nil {
		t.Fatal(err)
	}
	// The three-row native source batch owns an Arrow slice. Projection of
	// all three rows exceeds the soft target and must retry a smaller range.
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 3, BatchPayloadTargetBytes: 128, BatchPayloadBytes: 512, MemoryManager: manager}
	stream, err := projected.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	count := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		raw := out.Opaque["raw"].Value.(*array.Int64)
		for row := 0; row < out.Rows; row++ {
			if raw.Value(row) != int64((count+1)*10) || string(out.Strings["text"].ValueBytes(row)) != literal {
				t.Fatalf("row %d corrupted", count)
			}
			count++
		}
	}
	if count != 4 || values.Value(0) != 10 {
		t.Fatalf("rows=%d source released or changed", count)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("workspace leaked %d", manager.Used())
	}
}

func TestProjectUsesActiveExternalQueryBudget(t *testing.T) {
	preparedEngine := memory.NewEngineBudget(1 << 20)
	executingEngine := memory.NewEngineBudget(1 << 20)
	query, err := executingEngine.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer query.Close()
	ctx := memory.WithQueryBudget(context.Background(), query)
	input := memory.NewBatch()
	input.Rows = 2
	input.Int64s["id"] = &memory.Int64Vector{Values: []int64{1, 2}}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}), input)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &kernelRecordingSource{Source: base}
	projected, err := NewProjectSource(recorder, []expr.NamedExpr{expr.Named(expr.Str("allocated output"), "text")}, false, preparedEngine)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := projected.NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: preparedEngine})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if len(recorder.seenOptions) != 1 || recorder.seenOptions[0].MemoryManager != query.Operator("SourceScan", memory.MemoryWorkspace) {
		t.Fatalf("upstream query budget was not propagated: %+v", recorder.seenOptions)
	}
	if ok, err := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("external budget=(%v,%v)", ok, err)
	}
	_ = stream.Close()
	if preparedEngine.Used() != 0 || executingEngine.Used() != 0 || query.Used() != 0 {
		t.Fatalf("budget bypass/leak prepared=%d executing=%d query=%d", preparedEngine.Used(), executingEngine.Used(), query.Used())
	}
}

type projectOpaqueValue struct{ releases int }

func (v *projectOpaqueValue) Release() { v.releases++ }
func (v *projectOpaqueValue) Retain()  {}
func TestProjectWholeOpaquePassthroughKeepsSourceOwnership(t *testing.T) {
	value := &projectOpaqueValue{}
	input := memory.NewBatch()
	input.Rows = 2
	input.Opaque["raw"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: "custom", Value: value, Owned: true}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "custom"}), input)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(base, []expr.NamedExpr{expr.Named(expr.Str("value"), "text")}, true)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := projected.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("whole opaque=(%v,%v)", ok, err)
	}
	if out.Opaque["raw"].Value != value || out.Opaque["raw"].Owned {
		t.Fatal("projection claimed source-owned opaque value")
	}
	out.ClearColumns()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if value.releases != 0 {
		t.Fatalf("projection released source value %d times", value.releases)
	}
	input.ClearColumns()
	if value.releases != 1 {
		t.Fatalf("source releases=%d", value.releases)
	}
}
