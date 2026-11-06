package engine

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestAggregatePredicateWorkspaceChargesActualCapacities(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "budget-v1"})
	program, _, err := expr.CompilePredicate(schema, expr.EQ(expr.Col("s"), expr.Str("one")))
	if err != nil {
		t.Fatal(err)
	}
	engineBudget := memory.NewEngineBudget(1 << 20)
	budget, err := engineBudget.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	workspace := aggregatePredicateWorkspace{manager: budget.Operator("predicate", memory.MemoryWorkspace)}
	for _, rows := range []int{1, 65, 131, 64} {
		batch := memory.NewBatch()
		batch.Rows = rows
		codes := make([]uint32, rows)
		for row := range codes {
			codes[row] = uint32(row % 3)
		}
		batch.DictStrings["s"] = &memory.DictionaryStringVector{Indices: codes, Offsets: []uint32{0, 3, 6, 11}, Data: []byte("onetwothree"), Encoding: memory.EncodingDictionary, DictionaryDomain: "budget-v1"}
		if _, err := workspace.evaluate(context.Background(), program, batch, kernel.New(kernel.ModeScalar)); err != nil {
			t.Fatal(err)
		}
		if used, actual := budget.Used(), workspace.memoryBytes(); used != actual {
			t.Fatalf("rows=%d charged=%d actual=%d", rows, used, actual)
		}
	}
	workspace.close()
	workspace.close()
	if budget.Used() != 0 || workspace.memoryBytes() != 0 {
		t.Fatalf("closed budget=%d workspace=%d", budget.Used(), workspace.memoryBytes())
	}
}

func TestAggregatePredicateWorkspaceGrowthReservesBeforeAllocation(t *testing.T) {
	program, _, err := expr.CompilePredicate(memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt32}), expr.GT(expr.Col("pred"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := program.WorkspaceBytes(1)
	after, _ := program.WorkspaceBytes(131)
	manager := memory.NewBudgetManager(before + after - 1)
	workspace := aggregatePredicateWorkspace{manager: manager}
	batch := memory.NewBatch()
	batch.Rows = 1
	batch.Int32s["pred"] = &memory.Int32Vector{Values: make([]int32, 131)}
	if _, err := workspace.evaluate(context.Background(), program, batch, kernel.New(kernel.ModeScalar)); err != nil {
		t.Fatal(err)
	}
	actual := workspace.memoryBytes()
	batch.Rows = 131
	if _, err := workspace.evaluate(context.Background(), program, batch, kernel.New(kernel.ModeScalar)); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("overlap error=%v", err)
	}
	if workspace.memoryBytes() != actual || manager.Used() != before {
		t.Fatalf("failed growth changed ownership: actual=%d used=%d", workspace.memoryBytes(), manager.Used())
	}
	workspace.close()
	if manager.Used() != 0 {
		t.Fatalf("leaked=%d", manager.Used())
	}
}
