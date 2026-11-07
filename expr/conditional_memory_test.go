package expr

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
)

func conditionalMemoryFixture(tb testing.TB, kind string, branches, rows int) (Program, *memory.Batch) {
	tb.Helper()
	batch := memory.NewBatch()
	batch.Rows = rows
	fields := []memory.Field{{Name: "x", Type: memory.TypeInt64}}
	x := &memory.Int64Vector{Values: make([]int64, rows)}
	for row := range rows {
		x.Values[row] = int64(row % branches)
	}
	batch.Int64s["x"] = x
	var expression Expr
	if kind == "case" {
		arms := make([]WhenThen, branches)
		for i := range branches {
			arms[i] = WhenThen{When: EQ(Col("x"), I64(int64(i))), Then: Add(Col("x"), I64(int64(i)))}
		}
		expression = Case(arms, I64(-1))
	} else {
		arms := make([]Expr, branches)
		for i := range branches {
			name := fmt.Sprintf("c%d", i)
			fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true})
			v := &memory.Int64Vector{Values: make([]int64, rows), Validity: make(memory.Bitmap, memory.WordsFor(rows))}
			for row := range rows {
				v.Values[row] = int64(i)
				if row%branches == i {
					memory.Set(v.Validity, row)
				}
			}
			batch.Int64s[name] = v
			arms[i] = Add(Col(name), I64(int64(i)))
		}
		expression = Coalesce(arms...)
	}
	p, err := Compile(memory.NewSchema(fields...), expression)
	if err != nil {
		tb.Fatal(err)
	}
	return p, batch
}

func TestConditionalMemoryBoundedAndWarm(t *testing.T) {
	for _, kind := range []string{"case", "coalesce"} {
		var first int64
		for _, branches := range []int{2, 8, 32, 64} {
			p, batch := conditionalMemoryFixture(t, kind, branches, 4097)
			m := &scalarTestManager{base: memory.NewBudgetManager(0)}
			w, err := NewWorkspace(context.Background(), p, m)
			if err != nil {
				t.Fatal(err)
			}
			for iteration := range 3 {
				before := m.calls
				values, err := w.Evaluate(context.Background(), batch, kernel.New(kernel.ModeScalar))
				if err != nil {
					t.Fatal(err)
				}
				for row := range batch.Rows {
					v := values[0].Int64
					if !v.IsValid(row) || v.Values[row] != 2*int64(row%branches) {
						t.Fatalf("%s branches=%d row=%d", kind, branches, row)
					}
				}
				if iteration > 0 && m.calls != before {
					t.Fatal("warm execution admitted new storage")
				}
				if m.Used() != w.MemoryBytes() {
					t.Fatalf("accounting used=%d retained=%d", m.Used(), w.MemoryBytes())
				}
			}
			if first == 0 {
				first = m.Used()
			} else if m.Used() > first {
				t.Fatalf("%s retention scales with arms: %d -> %d", kind, first, m.Used())
			}
			w.Close()
			if m.Used() != 0 {
				t.Fatal("leaked reservation")
			}
		}
	}
}

// These metrics count managed engine-owned capacities, not RSS or allocator
// overhead. Cold includes admission and Close; warm excludes initial setup.
func BenchmarkConditionalMemory(b *testing.B) {
	for _, kind := range []string{"case", "coalesce"} {
		for _, branches := range []int{2, 8, 32, 64} {
			p, batch := conditionalMemoryFixture(b, kind, branches, 4096)
			for _, warm := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/arms=%d/warm=%v", kind, branches, warm), func(b *testing.B) {
					m := &scalarTestManager{base: memory.NewBudgetManager(0)}
					w, err := NewWorkspace(context.Background(), p, m)
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					if warm {
						if _, err := w.Evaluate(context.Background(), batch, testKernelSet()); err != nil {
							b.Fatal(err)
						}
					}
					m.peak = m.Used()
					b.ReportAllocs()
					b.ResetTimer()
					var retained int64
					for range b.N {
						values, err := w.Evaluate(context.Background(), batch, testKernelSet())
						if err != nil {
							b.Fatal(err)
						}
						if values[0].Int64.Values[branches-1] != int64(2*(branches-1)) {
							b.Fatal("bad result")
						}
						retained = m.Used()
						if !warm {
							w.Close()
							w, err = NewWorkspace(context.Background(), p, m)
							if err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(m.peak), "peak-managed-B")
					b.ReportMetric(float64(retained), "retained-managed-B")
				})
			}
		}
	}
}
