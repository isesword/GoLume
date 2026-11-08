package expr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gocolumn/memory"
)

func bindingDiamond(depth int) Expr {
	var e Expr = Col("x")
	for range depth {
		e = Sub(e, e)
	}
	return e
}

func TestBindDAGSharedChildren(t *testing.T) {
	// This small case also runs against the old binder without exhausting memory.
	bound, err := Bind(livenessSchema(), bindingDiamond(8))
	if err != nil {
		t.Fatal(err)
	}
	for level := 8; level > 0; level-- {
		if bound.Children[0] != bound.Children[1] {
			t.Fatalf("source node expanded more than once at level %d", level)
		}
		bound = bound.Children[0]
	}
}

func TestBindDAGSizeAndMultipleOutputs(t *testing.T) {
	const depth = 512
	value := bindingDiamond(depth)
	binder := newExpressionBinder(livenessSchema())
	first, err := binder.bind(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(binder.nodes) != depth+1 {
		t.Fatalf("bound source handles=%d, want %d", len(binder.nodes), depth+1)
	}
	second, err := binder.bind(Add(value, value))
	if err != nil || second.Children[0] != first || second.Children[1] != first || len(binder.nodes) != depth+2 {
		t.Fatalf("cross-output reuse failed: nodes=%d err=%v", len(binder.nodes), err)
	}
	outputs := make([]NamedExpr, 64)
	for i := range outputs {
		outputs[i] = Named(value, fmt.Sprint(i))
	}
	p, err := CompileExpressions(livenessSchema(), outputs)
	if err != nil {
		t.Fatal(err)
	}
	if p.InstructionCount() != depth+1 || len(p.outputs) != len(outputs) {
		t.Fatalf("instructions=%d outputs=%d", p.InstructionCount(), len(p.outputs))
	}
	for _, output := range p.outputs {
		if output.root != p.outputs[0].root {
			t.Fatal("shared output lost structural CSE")
		}
	}
	if got := Columns(value); !reflect.DeepEqual(got, []string{"x"}) {
		t.Fatalf("DAG dependencies=%v", got)
	}
}

func TestBindDAGAllocationGrowth(t *testing.T) {
	// An allocation invariant, not a timing assertion. Quadrupling the number
	// of unique nodes must not approximate expansion of the source tree.
	measure := func(depth int) float64 {
		value := bindingDiamond(depth)
		return testing.AllocsPerRun(5, func() {
			if _, err := Compile(livenessSchema(), value); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := measure(32), measure(128)
	if large > 5*small {
		t.Fatalf("compile allocations grew faster than DAG: %.0f -> %.0f", small, large)
	}
}

func TestCompileDAGOutputBindingGrowth(t *testing.T) {
	value := bindingDiamond(128)
	measure := func(count int) float64 {
		outputs := make([]NamedExpr, count)
		for i := range outputs {
			outputs[i] = Named(value, fmt.Sprint(i))
		}
		return testing.AllocsPerRun(3, func() {
			if _, err := CompileExpressions(livenessSchema(), outputs); err != nil {
				t.Fatal(err)
			}
		})
	}
	one, many := measure(1), measure(64)
	if many > 2*one {
		t.Fatalf("shared output rebinding multiplied allocations: %.0f -> %.0f", one, many)
	}
}

func TestBindDAGSchemaIsolationAndSourceMutation(t *testing.T) {
	var shared Expr = Col("x")
	firstSchema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt8, Nullable: true})
	secondSchema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeFloat32})
	first, err := Bind(firstSchema, shared)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Bind(secondSchema, shared)
	if err != nil || first == second || first.Field.Type != memory.TypeInt8 || second.Field.Type != memory.TypeFloat32 || second.Field.Nullable {
		t.Fatalf("schema leaked across bind operations: first=%+v second=%+v err=%v", first.Field, second.Field, err)
	}
	values := []Expr{Null(memory.TypeInt64), I64(7)}
	var source Expr = CoalesceExpr{Values: values}
	before, err := Bind(memory.NewSchema(), source)
	if err != nil {
		t.Fatal(err)
	}
	values[1] = I64(19)
	after, err := Bind(memory.NewSchema(), source)
	if err != nil {
		t.Fatal(err)
	}
	if before.Children[1].Expr.(Int64Literal).Value != 7 || after.Children[1].Expr.(Int64Literal).Value != 19 || before.Expr.(CoalesceExpr).Values[1].(Int64Literal).Value != 7 {
		t.Fatal("bound nodes retained source slices or a stale global cache")
	}
	// A shared integer value has different parent promotions. Binding a parent
	// must not rewrite the shared child to the parent's arithmetic/result type.
	var integer Expr = I64(3)
	bound, err := Bind(memory.NewSchema(), If(Bool(true), Add(integer, F64(1)), integer))
	if err != nil {
		t.Fatal(err)
	}
	child := bound.Children[1].Children[0]
	if child != bound.Children[2] || child.Field.Type != memory.TypeInt64 || bound.Field.Type != memory.TypeFloat64 {
		t.Fatal("parent coercion mutated shared input type")
	}
}

func TestBindDAGValidationAndErrorOrder(t *testing.T) {
	var bad Expr = Col("missing-first")
	for _, e := range []Expr{
		If(Bool(true), I64(1), bad),
		Coalesce(I64(1), bad),
		In(I64(1), I64(1), bad),
		Case([]WhenThen{{When: Bool(true), Then: I64(1)}}, bad),
		Add(bad, Col("missing-second")),
	} {
		if _, err := Bind(memory.NewSchema(), e); err == nil || !strings.Contains(err.Error(), "missing-first") {
			t.Fatalf("structural validation/order changed: %v", err)
		}
	}
	var partial Expr = Add(I64(math.MaxInt64), I64(1))
	bound, err := Bind(memory.NewSchema(), If(Bool(false), partial, I64(0)))
	if err != nil || !bound.MayError {
		t.Fatalf("binding executed or dropped partial value: %v", err)
	}
	// Failed roots must not leave active-cycle sentinels on their traversal path.
	binder := newExpressionBinder(memory.NewSchema())
	root := Add(partial, bad)
	for range 2 {
		if _, err := binder.bind(root); err == nil || !strings.Contains(err.Error(), "missing-first") {
			t.Fatalf("failed bind poisoned memo: %v", err)
		}
	}
}

func TestBindDAGConcurrentSchemas(t *testing.T) {
	value := bindingDiamond(64)
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Go(func() {
			typ := memory.TypeInt16
			if worker%2 != 0 {
				typ = memory.TypeFloat32
			}
			schema := memory.NewSchema(memory.Field{Name: "x", Type: typ})
			for range 8 {
				bound, err := Bind(schema, value)
				if err != nil {
					t.Error(err)
					return
				}
				for len(bound.Children) != 0 {
					bound = bound.Children[0]
				}
				if bound.Field.Type != typ {
					t.Errorf("concurrent schema contamination: got %s want %s", bound.Field.Type, typ)
				}
			}
		})
	}
	wg.Wait()
}

func TestCompileGuardedDAGMetadataAndSelection(t *testing.T) {
	var value Expr = Col("x")
	const depth = 128
	for range depth {
		value = If(Col("c"), value, value)
	}
	p, err := Compile(livenessSchema(), value)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.distinctInstructionCount(); got != 2*depth+1 {
		t.Fatalf("guarded programs expanded: got %d distinct instructions", got)
	}
	if p.InstructionCount() != int(^uint(0)>>1) {
		t.Fatal("logical instruction count did not saturate")
	}
	if !p.SupportsSelection() {
		t.Fatal("guarded numeric DAG lost selection support")
	}
	budget := memory.NewBudgetManager(4 << 20)
	w, err := NewWorkspace(context.Background(), p, budget)
	if err != nil {
		t.Fatal(err)
	}
	batch := livenessBatch(65, true)
	out, err := w.EvaluateSelected(context.Background(), batch, []int{0, 1, 2, 4, 6, 64}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range []int{0, 1, 2, 4, 6, 64} {
		got, want := out[0].Int64, batch.Int64s["x"]
		if got.IsValid(i) != want.IsValid(row) || got.IsValid(i) && got.Values[i] != want.Values[row] {
			t.Fatalf("selection row %d changed value/null semantics", row)
		}
	}
	if budget.Used() != w.MemoryBytes() {
		t.Fatal("shared program changed workspace ownership accounting")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Evaluate(ctx, batch, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	if budget.Used() != 0 || w.MemoryBytes() != 0 {
		t.Fatal("cancellation retained DAG workspace memory")
	}
}

func TestCompileGuardedDAGLogicalCount(t *testing.T) {
	var e Expr = I64(7)
	for depth := 0; depth < 10; depth++ {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		want := 3*(1<<depth) - 2
		if p.InstructionCount() != want {
			t.Fatalf("depth=%d logical count=%d want=%d", depth, p.InstructionCount(), want)
		}
		e = If(Bool(true), e, e)
	}
}

func TestCompileGuardedDAGSelectionPlanAllocation(t *testing.T) {
	var value Expr = Col("x")
	for range 32 {
		value = If(Col("c"), value, value)
	}
	p, err := Compile(livenessSchema(), value)
	if err != nil {
		t.Fatal(err)
	}
	if p.selection == nil || len(p.selection.columns) != 2 {
		t.Fatal("selection dependencies were not prepared at compile time")
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = p.SupportsSelection() }); allocs != 0 {
		t.Fatalf("selection capability check allocated %.0f objects", allocs)
	}
	budget := memory.NewBudgetManager(2 << 20)
	w, err := NewWorkspace(context.Background(), p, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	batch, rows := livenessBatch(65, true), []int{0, 1, 2, 3, 4, 64}
	ctx, kernels := context.Background(), testKernelSet()
	if _, err := w.EvaluateSelected(ctx, batch, rows, kernels); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(10, func() {
		if _, err := w.EvaluateSelected(ctx, batch, rows, kernels); err != nil {
			t.Fatal(err)
		}
	})
	// ScalarValue exports one borrowed vector header per output. Shared code
	// and selection metadata must add no per-evaluation traversal allocations.
	if allocs > 1 {
		t.Fatalf("warm selected execution allocated %.0f objects", allocs)
	}
	for _, column := range p.selection.columns {
		if column.validity != nil {
			t.Fatal("execution mutated immutable selection metadata")
		}
	}
}

func (p Program) distinctInstructionCount() int {
	n := 0
	p.visitInstructions(func(in instruction) bool {
		n++
		return true
	})
	return n
}

func TestCompileGuardedDAGSelectionAdmissionRetry(t *testing.T) {
	var value Expr = Col("x")
	for range 64 {
		value = If(Col("c"), value, value)
	}
	p, err := Compile(livenessSchema(), value)
	if err != nil {
		t.Fatal(err)
	}
	manager := &scalarTestManager{base: memory.NewBudgetManager(0), limit: 1}
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	batch, rows := livenessBatch(65, false), []int{1, 2, 3, 64}
	if _, err := w.EvaluateSelected(context.Background(), batch, rows, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("metadata admission=%v", err)
	}
	if w.selected.columns != nil || w.selected.initialized || w.closed || manager.Used() != 0 {
		t.Fatal("failed metadata admission allocated, committed, or closed workspace")
	}
	manager.limit = 0
	out, err := w.EvaluateSelected(context.Background(), batch, rows, testKernelSet())
	if err != nil || out[0].Int64.Values[0] != 1 {
		t.Fatalf("metadata retry: %v", err)
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("retry retained reservations after close")
	}
}
