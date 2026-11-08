package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"reflect"
	"testing"
)

func TestSelectedExpressionDenseDomain(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "c", Type: memory.TypeBool, Nullable: true})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("x"), "x"), Named(Add(Col("x"), I64(7)), "sum"), Named(If(Col("c"), Add(Col("x"), I64(2)), I64(-3)), "guard"), Named(GT(Col("x"), I64(4)), "compare")})
	if err != nil {
		t.Fatal(err)
	}
	if !p.SupportsSelection() {
		t.Fatal("expected support")
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		budget := memory.NewBudgetManager(0)
		w, _ := NewWorkspace(context.Background(), p, budget)
		for _, count := range []int{130, 1, 65, 0, 257, 63} {
			b := memory.NewBatch()
			b.Rows = count
			x := &memory.Int64Vector{Values: make([]int64, count), Validity: make(memory.Bitmap, memory.WordsFor(count))}
			c := &memory.Uint32Vector{Values: make([]uint32, count), Validity: make(memory.Bitmap, memory.WordsFor(count))}
			b.Int64s["x"], b.Uint32s["c"] = x, c
			for i := range count {
				x.Values[i] = int64(i)
				c.Values[i] = uint32(i % 2)
				if i%5 != 0 {
					memory.Set(x.Validity, i)
				}
				if i%3 != 0 {
					memory.Set(c.Validity, i)
				}
			}
			for _, step := range []int{1, 2, 10, 101} {
				selected := make([]int, 0)
				dense := memory.NewBatch()
				dx := &memory.Int64Vector{Validity: make(memory.Bitmap, memory.WordsFor(count))}
				dc := &memory.Uint32Vector{Validity: make(memory.Bitmap, memory.WordsFor(count))}
				for i := 0; i < count; i += step {
					j := len(selected)
					selected = append(selected, i)
					dx.Values = append(dx.Values, x.Values[i])
					dc.Values = append(dc.Values, c.Values[i])
					if x.IsValid(i) {
						memory.Set(dx.Validity, j)
					}
					if c.IsValid(i) {
						memory.Set(dc.Validity, j)
					}
				}
				dense.Rows = len(selected)
				dense.Int64s["x"], dense.Uint32s["c"] = dx, dc
				oracle, _ := NewWorkspace(context.Background(), p, nil)
				want, err := oracle.Evaluate(context.Background(), dense, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				got, err := w.EvaluateSelected(context.Background(), b, selected, kernel.New(mode))
				if err != nil {
					t.Fatal(err)
				}
				// Compare validity and values row-wise; NULL payloads have no meaning.
				for col := range got {
					for row := range selected {
						switch {
						case got[col].Int64 != nil:
							g, v := got[col].Int64, want[col].Int64
							if g.IsValid(row) != v.IsValid(row) || g.IsValid(row) && g.Values[row] != v.Values[row] {
								t.Fatalf("count=%d step=%d col=%d row=%d", count, step, col, row)
							}
						case got[col].Uint32 != nil:
							g, v := got[col].Uint32, want[col].Uint32
							if g.IsValid(row) != v.IsValid(row) || g.IsValid(row) && g.Values[row] != v.Values[row] {
								t.Fatalf("bool mismatch")
							}
						default:
							t.Fatal("unexpected output")
						}
					}
				}
				oracle.Close()
				if w.MemoryBytes() != budget.Used() {
					t.Fatalf("capacity=%d charge=%d", w.MemoryBytes(), budget.Used())
				}
			}
		}
		w.Close()
		if budget.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestSelectedExpressionErrorsAndLifetime(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})
	p, _ := Compile(schema, CastTo(Col("x"), memory.TypeInt8))
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 1000, 3}}
	budget := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, budget)
	got, err := w.EvaluateSelected(context.Background(), b, []int{0, 2}, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[0].Int8.Values, []int8{1, 3}) {
		t.Fatal(got)
	}
	b.Int64s["x"].Values[0] = 99
	if got[0].Int8.Values[0] != 1 {
		t.Fatal("result borrowed unselected input")
	}
	if _, err = w.EvaluateSelected(context.Background(), b, []int{1}, testKernelSet()); err == nil {
		t.Fatal("selected overflow suppressed")
	}
	if budget.Used() != 0 {
		t.Fatal("semantic error leaked")
	}
	for _, rows := range [][]int{{-1}, {3}, {1, 1}, {2, 0}} {
		w, _ = NewWorkspace(context.Background(), p, budget)
		if _, err = w.EvaluateSelected(context.Background(), b, rows, testKernelSet()); err == nil {
			t.Fatalf("accepted %v", rows)
		}
		if budget.Used() != 0 {
			t.Fatal("invalid selection leaked")
		}
	}
	w, _ = NewWorkspace(context.Background(), p, budget)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = w.EvaluateSelected(ctx, b, []int{0}, testKernelSet()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatal("cancellation leaked")
	}
}

type selectedFaultManager struct {
	memory.Manager
	calls, fail int
}

func (m *selectedFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestSelectedExpressionAdmissionRetry(t *testing.T) {
	p, _ := Compile(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), Add(Col("x"), I64(1)))
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{1, 2, 3}}
	for fail := 1; fail <= 8; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			manager := &selectedFaultManager{Manager: memory.NewBudgetManager(0), fail: fail}
			w, _ := NewWorkspace(context.Background(), p, manager)
			_, err := w.EvaluateSelected(context.Background(), b, []int{0, 2}, testKernelSet())
			if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatal(err)
			}
			got, err := w.EvaluateSelected(context.Background(), b, []int{0, 2}, testKernelSet())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got[0].Int64.Values, []int64{2, 4}) {
				t.Fatal(got)
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("leak")
			}
		})
	}
}
func TestSelectedExpressionUnsupported(t *testing.T) {
	p, _ := Compile(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), CastTo(Col("s"), memory.TypeInt64))
	if p.SupportsSelection() {
		t.Fatal("string input eligible")
	}
}

func TestSelectedAllPhysicalWidths(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool, memory.TypeDate32, memory.TypeTime32, memory.TypeDate64, memory.TypeTime64, memory.TypeTimestamp, memory.TypeDuration}
	for _, typ := range types {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			p, err := Compile(memory.NewSchema(memory.Field{Name: "x", Type: typ, Nullable: true}), Col("x"))
			if err != nil {
				t.Fatal(err)
			}
			b := memory.NewBatch()
			b.Rows = 3
			// Reflection only constructs typed fixtures, not the selected-row oracle.
			c := selectedColumn{name: "x", typ: typ, validity: make(memory.Bitmap, 1)}
			allocateSelectedColumn(b, c, 3)
			var vector reflect.Value
			for _, field := range []string{"Int8s", "Int16s", "Int32s", "Int64s", "Uint8s", "Uint16s", "Uint32s", "Uint64s", "Float32s", "Float64s"} {
				v := reflect.ValueOf(b).Elem().FieldByName(field).MapIndex(reflect.ValueOf("x"))
				if v.IsValid() {
					vector = v.Elem()
					break
				}
			}
			vals := vector.FieldByName("Values")
			for i := 0; i < 3; i++ {
				switch vals.Index(i).Kind() {
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					vals.Index(i).SetInt(int64(i + 1))
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					vals.Index(i).SetUint(uint64(i % 2))
				default:
					vals.Index(i).SetFloat(float64(i + 1))
				}
			}
			w, _ := NewWorkspace(context.Background(), p, nil)
			for _, nullable := range []bool{false, true, false, true} {
				if nullable {
					memory.Set(c.validity, 0)
					vector.FieldByName("Validity").Set(reflect.ValueOf(c.validity))
				} else {
					vector.FieldByName("Validity").SetZero()
				}
				got, err := w.EvaluateSelected(context.Background(), b, []int{0, 2}, testKernelSet())
				if err != nil {
					t.Fatal(err)
				}
				r := reflect.ValueOf(got[0])
				var out reflect.Value
				for i := 0; i < r.NumField(); i++ {
					f := r.Field(i)
					if f.Kind() == reflect.Pointer && !f.IsNil() {
						out = f.Elem()
						break
					}
				}
				if !out.IsValid() {
					t.Fatal("no output vector")
				}
				values, valid := out.FieldByName("Values"), out.FieldByName("Validity")
				if values.Len() != 2 {
					t.Fatal("wrong dense count")
				}
				if !reflect.DeepEqual(values.Index(0).Interface(), vals.Index(0).Interface()) {
					t.Fatal("first selected value mismatch")
				}
				if nullable {
					if valid.Len() == 0 || valid.Index(0).Uint()&3 != 1 {
						t.Fatal("selected validity mismatch")
					}
				} else if valid.Len() != 0 && valid.Index(0).Uint()&3 != 3 {
					t.Fatal("all valid mismatch")
				}
			}
			w.Close()
		})
	}
}

func TestSelectedNilIdentityAcceptsStrings(t *testing.T) {
	p, _ := Compile(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), Col("s"))
	b := memory.NewBatch()
	b.Rows = 1
	b.Strings["s"] = testStringVector([]string{"ok"}, nil)
	w, _ := NewWorkspace(context.Background(), p, nil)
	defer w.Close()
	out, err := w.EvaluateSelected(context.Background(), b, nil, testKernelSet())
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0].String.ValueBytes(0)) != "ok" {
		t.Fatal("identity mismatch")
	}
}

func TestSelectedGrowthDropsSkippedRegionBorrows(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "c", Type: memory.TypeBool})
	p, err := Compile(schema, If(Col("c"), If(Col("c"), Col("x"), Col("x")), I64(-1)))
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w, _ := NewWorkspace(context.Background(), p, manager)
	// Establish larger physical executor capacities first. Later selection growth
	// must reuse them, rather than incidentally clear regions through prepareProgram.
	b := memory.NewBatch()
	b.Rows = 130
	b.Int64s["x"] = &memory.Int64Vector{Values: make([]int64, 130)}
	b.Uint32s["c"] = &memory.Uint32Vector{Values: make([]uint32, 130)}
	for i := range 130 {
		b.Int64s["x"].Values[i] = int64(i)
		b.Uint32s["c"].Values[i] = 1
	}
	if _, err = w.Evaluate(context.Background(), b, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	if _, err = w.EvaluateSelected(context.Background(), b, []int{0}, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	var region *evaluationState
	for i := range w.storage.owned {
		if len(w.storage.owned[i].regions) > 0 {
			region = w.storage.owned[i].regions[0]
			break
		}
	}
	if region == nil {
		t.Fatal("guarded region not executed")
	}
	borrowed := false
	for _, r := range region.regs {
		if r != nil && len(r.i64) > 0 {
			borrowed = true
		}
	}
	if !borrowed {
		t.Fatal("fixture did not retain selected column")
	}
	for i := range 130 {
		b.Uint32s["c"].Values[i] = 0
	}
	rows := make([]int, 65)
	for i := range rows {
		rows[i] = i * 2
	}
	if _, err = w.EvaluateSelected(context.Background(), b, rows, testKernelSet()); err != nil {
		t.Fatal(err)
	}
	for _, r := range region.regs {
		if r != nil && len(r.i64) > 0 {
			t.Fatal("skipped region retained expired selected input")
		}
	}
	for _, r := range region.borrowed[:cap(region.borrowed)] {
		if len(r.i64) > 0 {
			t.Fatal("skipped region retained expired borrowed column")
		}
	}
	if w.MemoryBytes() != manager.Used() {
		t.Fatalf("capacity=%d charge=%d", w.MemoryBytes(), manager.Used())
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("leak")
	}
}
