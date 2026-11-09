package physical

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func decimalCompactBatch(rows int, nullable bool) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = rows
	v := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Precision: 38, Scale: 12}
	if nullable {
		v.Validity = make(memory.Bitmap, memory.WordsFor(rows))
	}
	max := decimal128.GetScaleMultiplier(38).Sub(decimal128.FromI64(1))
	for row := range rows {
		v.Values[row] = max.Sub(decimal128.FromI64(int64(row)))
		if nullable && row%3 != 1 {
			memory.Set(v.Validity, row)
		}
	}
	b.Decimals["amount"] = v
	return b
}

func TestDecimalCompactionExactBudgetAndRollback(t *testing.T) {
	ctx := context.Background()
	m := &filterRecordingManager{base: memory.NewBudgetManager(0), deny: true}
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	b := decimalCompactBatch(130, true)
	rows := []int{0, 1, 63, 64, 65, 129}
	selection := make(memory.Bitmap, 3)
	for _, row := range rows {
		memory.Set(selection, row)
	}
	if _, err := w.compact(ctx, b, rows, selection, testReadOptions().Kernels); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.batch != nil || m.Used() != 0 {
		t.Fatal("denied output allocated")
	}
	m.deny = false
	first, err := w.compact(ctx, b, rows, selection, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	v := first.Decimals["amount"]
	if m.Used() != 6*16+8 || v.Precision != 38 || v.Scale != 12 {
		t.Fatal("wrong decimal payload charge or metadata")
	}
	for i, row := range rows {
		if v.Values[i] != b.Decimals["amount"].Values[row] || v.IsValid(i) != b.Decimals["amount"].IsValid(row) {
			t.Fatal("decimal compact lost precision/nulls")
		}
	}
	pointer := &v.Values[0]
	small, err := w.compact(ctx, b, []int{64}, memory.Bitmap{0, 1, 0}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	if &small.Decimals["amount"].Values[0] != pointer || m.Used() != 104 {
		t.Fatal("decimal capacities not reused")
	}
	big := []int{0, 1, 2, 3, 4, 5, 6}
	m.deny = true
	if _, err := w.compact(ctx, b, big, memory.Bitmap{127, 0, 0}, testReadOptions().Kernels); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.batch != first || m.Used() != 104 || m.requests[len(m.requests)-1] != 120 || m.live[len(m.live)-1] != 104 {
		t.Fatal("decimal growth rollback lost accounting")
	}
	m.deny = false
	if _, err := w.compact(ctx, b, big, memory.Bitmap{127, 0, 0}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	// Nullable-to-all-valid-to-nullable batches retain enough admitted validity capacity.
	plain := decimalCompactBatch(130, false)
	if _, err := w.compact(ctx, plain, rows, selection, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if _, err := w.compact(ctx, b, rows, selection, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	w.release()
	w.release()
	if m.Used() != 0 {
		t.Fatal("decimal compaction leaked")
	}
}

func TestDecimalFilterPayloadAndIncludeInput(t *testing.T) {
	ctx := context.Background()
	batch := decimalCompactBatch(67, true)
	batch.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, batch.Rows)}
	for i := range batch.Rows {
		batch.Int64s["id"].Values[i] = int64(i)
	}
	field, err := memory.Decimal128Field("amount", true, 38, 12)
	if err != nil {
		t.Fatal(err)
	}
	base, err := inmemory.New(memory.NewSchema(field, memory.Field{Name: "id", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(base, expr.GT(expr.Col("id"), expr.I64(62)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("amount"), "copy")}, true)
	if err != nil {
		t.Fatal(err)
	}
	budget := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.MemoryManager = budget
	stream, err := source.OpenBatchStream(ctx, projected, opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(ctx, out); !ok || err != nil {
		t.Fatalf("next %v %v", ok, err)
	}
	if out.Rows != 4 || out.Decimals["copy"] == nil || out.Decimals["amount"] == nil {
		t.Fatal("includeInput dropped native decimals")
	}
	for i := range 4 {
		for _, name := range []string{"amount", "copy"} {
			v := out.Decimals[name]
			if v.IsValid(i) != batch.Decimals["amount"].IsValid(63+i) || v.IsValid(i) && v.Values[i] != batch.Decimals["amount"].Values[63+i] {
				t.Fatal("filter projected wrong rows")
			}
		}
	}
	stream.Close()
	if budget.Used() != 0 {
		t.Fatal("native decimal filter/project leaked")
	}
}

func TestDecimalSelectionProjectUsesGatheredDomain(t *testing.T) {
	ctx := context.Background()
	batch := decimalCompactBatch(130, true)
	batch.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, batch.Rows)}
	for i := range batch.Rows {
		batch.Int64s["id"].Values[i] = int64(i % 4)
	}
	field, err := memory.Decimal128Field("amount", true, 38, 12)
	if err != nil {
		t.Fatal(err)
	}
	base, err := inmemory.New(memory.NewSchema(field, memory.Field{Name: "id", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := NewFilterSource(base, expr.EQ(expr.Col("id"), expr.I64(1)))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Col("amount"), "amount")}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !projected.selectionEnabled {
		t.Fatal("native decimal projection did not negotiate selection")
	}
	manager := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.MemoryManager = manager
	stream, err := projected.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	row := 1
	for {
		ok, err := stream.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if stream.(*projectBatchStream).selection.output.batch != nil {
			t.Fatal("selected decimal projection materialized full filter output")
		}
		for i := range out.Rows {
			v := out.Decimals["amount"]
			if v.IsValid(i) != batch.Decimals["amount"].IsValid(row) || v.IsValid(i) && v.Values[i] != batch.Decimals["amount"].Values[row] {
				t.Fatalf("wrong selected row %d", row)
			}
			row += 4
		}
	}
	if row != 133 {
		t.Fatalf("selected rows ended at %d", row)
	}
	stream.Close()
	if manager.Used() != 0 {
		t.Fatal("selected projection leaked")
	}
}
