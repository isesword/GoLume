package expr

import (
	"context"
	"errors"
	"gocolumn/memory"
	"strings"
	"testing"
)

type scalarTestManager struct {
	base  *memory.BudgetManager
	limit int64
	calls int
	peak  int64
}

func (m *scalarTestManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.limit > 0 && m.Used()+n > m.limit {
		return nil, memory.ErrBudgetExceeded
	}
	r, err := m.base.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, err
}
func (m *scalarTestManager) Used() int64  { return m.base.Used() }
func (m *scalarTestManager) Limit() int64 { return m.limit }
func scalarBudgetBatch(rows int) *memory.Batch {
	b := memory.NewBatch()
	b.Reset(rows)
	b.Int64s["a"] = &memory.Int64Vector{Values: make([]int64, rows)}
	for i := range b.Int64s["a"].Values {
		b.Int64s["a"].Values[i] = int64(i + 1)
	}
	return b
}
func TestScalarBudgetGrowthRollbackAndReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64})
	p, err := CompileScalar(schema, Add(Col("a"), I64(2)))
	if err != nil {
		t.Fatal(err)
	}
	m := &scalarTestManager{base: memory.NewBudgetManager(0)}
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), m)
	small := scalarBudgetBatch(8)
	if _, err := p.Evaluate(small, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	used, calls := m.Used(), m.calls
	for range 5 {
		w.SetMemoryManager(context.Background(), m)
		if _, err := p.Evaluate(small, &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
	}
	if m.Used() != used || m.calls != calls {
		t.Fatalf("warm execution grew budget: used=%d/%d calls=%d/%d", m.Used(), used, m.calls, calls)
	}
	m.limit = used + 1
	if _, err := p.Evaluate(scalarBudgetBatch(4096), &w, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("growth error=%v", err)
	}
	if m.Used() != 0 || w.regs != nil || w.owned != nil {
		t.Fatal("failed evaluation did not discard all owned state")
	}
	m.limit = 0
	if _, err := p.Evaluate(small, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	_ = w.Close()
	if m.Used() != 0 {
		t.Fatal("Close leaked budget")
	}
}
func TestScalarBudgetReplacementReservesOverlap(t *testing.T) {
	m := &scalarTestManager{base: memory.NewBudgetManager(0), limit: 20}
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), m)
	var dst []int64
	var reservation memory.Reservation
	if err := scalarResize(&dst, 1, &reservation, w.acquire); err != nil {
		t.Fatal(err)
	}
	dst[0] = 42
	if err := scalarResize(&dst, 2, &reservation, w.acquire); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("expected old+new=24 to exceed 20: %v", err)
	}
	if len(dst) != 1 || dst[0] != 42 || m.Used() != 8 {
		t.Fatal("rejected replacement modified old allocation")
	}
	_ = w.Close()
}
func TestScalarBudgetBorrowedInputsAreNeverReused(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true})
	column, _ := CompileScalar(schema, Col("a"))
	literal, _ := CompileScalar(schema, I64(99))
	b := scalarBudgetBatch(4)
	b.Int64s["a"].Validity = memory.Bitmap{5}
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), memory.NewBudgetManager(1<<20))
	defer w.Close()
	if _, err := column.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if _, err := literal.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	for i, value := range b.Int64s["a"].Values {
		if value != int64(i+1) {
			t.Fatal("borrowed input was overwritten")
		}
	}
	if b.Int64s["a"].Validity[0] != 5 {
		t.Fatal("borrowed validity was overwritten")
	}
}
func TestScalarBudgetCancellationAndManagerSwitch(t *testing.T) {
	p, _ := CompileScalar(memory.NewSchema(), I64(7))
	b := memory.NewBatch()
	b.Reset(8)
	m1, m2 := memory.NewBudgetManager(1<<20), memory.NewBudgetManager(1<<20)
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), m1)
	if _, err := p.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	w.SetMemoryManager(context.Background(), m2)
	if m1.Used() != 0 {
		t.Fatal("manager switch retained old charge")
	}
	if _, err := p.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.SetMemoryManager(ctx, m2)
	cancel()
	if _, err := p.Evaluate(b, &w, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if m2.Used() != 0 {
		t.Fatal("cancel leaked budget")
	}
}
func TestScalarStringAndListBudgetBeforeGrowth(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString})
	for name, e := range map[string]Expr{
		"concat":  Concat(Col("s"), Str("!")),
		"upper":   Upper(Col("s")),
		"replace": ReplaceAll(Col("s"), Str(""), Str("expanded")),
		"split":   Split(Col("s"), Str(",")),
		"regex":   RegexExtractAll(Col("s"), Str("(a)"), I64(1)),
	} {
		t.Run(name, func(t *testing.T) {
			p, err := CompileScalar(schema, e)
			if err != nil {
				t.Fatal(err)
			}
			b := memory.NewBatch()
			b.Reset(1)
			b.Strings["s"] = testStringVector([]string{"a,b"}, nil)
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			var w ScalarWorkspace
			w.SetMemoryManager(context.Background(), m)
			if _, err := p.Evaluate(b, &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			m.limit = m.Used() + 1
			b.Strings["s"] = testStringVector([]string{strings.Repeat("a,b,", 4096)}, nil)
			if _, err := p.Evaluate(b, &w, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("error=%v", err)
			}
			if m.Used() != 0 {
				t.Fatalf("failed expression retained %d", m.Used())
			}
		})
	}
}
func TestScalarProgramSizeChurnKeepsOwnedBuffers(t *testing.T) {
	b := scalarBudgetBatch(8)
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64})
	small, _ := CompileScalar(schema, I64(3))
	large, _ := CompileScalar(schema, Add(Add(Col("a"), I64(1)), I64(2)))
	m := memory.NewBudgetManager(1 << 20)
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), m)
	defer w.Close()
	if _, err := large.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if _, err := small.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if _, err := large.Evaluate(b, &w, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	used := m.Used()
	for range 10 {
		if _, err := small.Evaluate(b, &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		if _, err := large.Evaluate(b, &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
	}
	if m.Used() != used {
		t.Fatalf("program churn accumulates reservations: %d -> %d", used, m.Used())
	}
}
func BenchmarkScalarBudgetWarmArithmetic(b *testing.B) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64})
	p, _ := CompileScalar(schema, Add(Col("a"), I64(2)))
	batch := scalarBudgetBatch(8192)
	for _, budgeted := range []bool{false, true} {
		name := "unbudgeted"
		if budgeted {
			name = "budgeted"
		}
		b.Run(name, func(b *testing.B) {
			var w ScalarWorkspace
			if budgeted {
				w.SetMemoryManager(context.Background(), memory.NewBudgetManager(1<<20))
			}
			defer w.Close()
			kernels := testKernelSet()
			if _, err := p.Evaluate(batch, &w, kernels); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := p.Evaluate(batch, &w, kernels); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestScalarBudgetInvalidResultsReleaseWorkspace(t *testing.T) {
	p, err := CompileScalar(memory.NewSchema(), Str("value"))
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Reset(2)
	for _, mode := range []string{"root", "output", "type"} {
		t.Run(mode, func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			var w ScalarWorkspace
			w.SetMemoryManager(context.Background(), manager)
			if _, err := p.Evaluate(batch, &w, testKernelSet()); err != nil {
				t.Fatal(err)
			}
			if manager.Used() == 0 {
				t.Fatal("test did not allocate")
			}
			var err error
			switch mode {
			case "root":
				bad := p
				bad.Root = -1
				_, err = bad.Evaluate(batch, &w, testKernelSet())
			case "output":
				bad := ProjectionProgram{Instructions: p.Instructions, Outputs: []ProjectionOutput{{Name: "bad", Root: len(p.Instructions)}}}
				_, err = bad.Evaluate(batch, &w, testKernelSet())
			case "type":
				_, err = p.EvaluateInt64(batch, &w, testKernelSet())
			}
			if err == nil {
				t.Fatal("invalid result succeeded")
			}
			if manager.Used() != 0 || w.regs != nil || w.owned != nil || w.outputs != nil {
				t.Fatal("invalid result retained workspace")
			}
		})
	}
}

func TestScalarBudgetEnsureFailureIsReported(t *testing.T) {
	p, _ := CompileScalar(memory.NewSchema(), I64(1))
	batch := memory.NewBatch()
	batch.Reset(1)
	manager := &scalarTestManager{base: memory.NewBudgetManager(0), limit: 1}
	var w ScalarWorkspace
	w.SetMemoryManager(context.Background(), manager)
	w.Ensure(1, 10)
	if !errors.Is(w.ensureErr, memory.ErrBudgetExceeded) {
		t.Fatalf("Ensure error=%v", w.ensureErr)
	}
	manager.limit = 0
	if _, err := p.Evaluate(batch, &w, testKernelSet()); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("lost Ensure error: %v", err)
	}
	if manager.Used() != 0 {
		t.Fatal("Ensure failure leaked reservations")
	}
	if _, err := p.Evaluate(batch, &w, testKernelSet()); err != nil {
		t.Fatalf("failed recovery: %v", err)
	}
	_ = w.Close()
}
