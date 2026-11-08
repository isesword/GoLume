package engine

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func globalBindingTestStream(t *testing.T, schema memory.Schema, q expr.Query) (*sortedAggregateStream, *memory.QueryBudget) {
	t.Helper()
	src, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	e := New(Options{MemoryLimitBytes: 1 << 20})
	p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	stream, err := p.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*sortedAggregateStream)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		if budget.Used() != 0 {
			t.Errorf("retained %d bytes", budget.Used())
		}
		budget.Close()
		p.Close()
		e.Close()
	})
	return s, budget
}

type globalBindingTestInput struct {
	next   func(context.Context, *memory.Batch) (bool, error)
	closed int
}

func (s *globalBindingTestInput) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	return s.next(ctx, b)
}
func (s *globalBindingTestInput) Close() error { s.closed++; return nil }

func assertGlobalBindingsCleared(t *testing.T, s *sortedAggregateStream) {
	t.Helper()
	for lane := range s.globalBindings {
		if !reflect.DeepEqual(s.globalBindings[lane], globalAggregateLaneBinding{}) {
			t.Fatalf("lane %d retained a batch binding", lane)
		}
	}
	if s.binding.batch != nil || s.binding.values != nil {
		t.Fatal("retained previous batch binding")
	}
}

func TestGlobalAggregateBindingsEligibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*CompiledMultiAggregate, *PreparedQuery)
		want bool
	}{
		{"sum-count", nil, true},
		{"count-only", func(m *CompiledMultiAggregate, _ *PreparedQuery) {
			m.Types[0] = memory.TypeStruct
			m.Layouts[0] = aggregate.Layout{CountValues: true}
		}, true},
		{"synthetic-count", func(m *CompiledMultiAggregate, _ *PreparedQuery) {
			m.Columns[0] = ""
			m.Layouts[0] = aggregate.Layout{CountRows: true}
		}, true},
		{"grouped", func(_ *CompiledMultiAggregate, p *PreparedQuery) { p.cq.GroupShape = GroupShapeI32 }, false},
		{"non-native", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Native = false }, false},
		{"min", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Layouts[0].Min = true }, false},
		{"max", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Layouts[0].Max = true }, false},
		{"avg", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Layouts[0].Avg = true }, false},
		{"statistics", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Statistics = &CompiledStatistics{} }, false},
		{"distinct", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Distinct = &CompiledDistinct{} }, false},
		{"quantiles", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Quantiles = &CompiledQuantiles{} }, false},
		{"unsupported-sum", func(m *CompiledMultiAggregate, _ *PreparedQuery) { m.Types[0] = memory.TypeDate64 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &CompiledMultiAggregate{Native: true, Columns: []string{"v"}, Types: []memory.DataType{memory.TypeFloat64}, Layouts: []aggregate.Layout{{Sum: true, CountRows: true, CountValues: true}}}
			p := &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeGlobal, MultiAggregate: m}}
			if tc.edit != nil {
				tc.edit(m, p)
			}
			if got := p.globalAggregateBindingsEligible(); got != tc.want {
				t.Fatalf("eligible=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestGlobalAggregateBindingsAllWidths(t *testing.T) {
	_, cols := nativeAggregateFixture(t)
	b := memory.NewBatch()
	b.Rows = 6
	m := &CompiledMultiAggregate{Native: true}
	for _, c := range cols[:10] {
		fillNativeTestColumn(b, c)
		m.Columns = append(m.Columns, c.name)
		m.Types = append(m.Types, c.typ)
		m.Layouts = append(m.Layouts, aggregate.Layout{Sum: true, CountValues: true})
	}
	p := &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeGlobal, MultiAggregate: m}}
	if _, err := p.bindSortedBatch(b); err != nil {
		t.Fatal(err)
	}
	s := &sortedAggregateStream{query: p, inputBatch: b, globalBindings: make([]globalAggregateLaneBinding, len(m.Columns))}
	s.bindGlobalAggregateInputs()
	for lane, c := range cols[:10] {
		for row, want := range c.bits {
			bits, valid := s.globalBindings[lane].bits(row)
			if bits != want || valid != memory.IsSet(c.valid, row) {
				t.Fatalf("%s row=%d bits=%x want=%x valid=%v", c.name, row, bits, want, valid)
			}
		}
	}
	// Prove the hot reader is bound rather than repeating a column map lookup.
	clear(b.Float64s)
	if got, valid := s.globalBindings[9].bits(1); got != math.Float64bits(7) || !valid {
		t.Fatalf("bound read after map removal: %x %v", got, valid)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for lane := range s.globalBindings {
			s.globalBindings[lane].bits(0)
		}
	}); allocs != 0 {
		t.Fatalf("per-row allocations=%v", allocs)
	}
}

func TestGlobalAggregateBindingsCountValidityOnly(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Strings["plain"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 0}, Validity: memory.Bitmap{5}}
	b.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: []uint32{0, 0, 0}, Validity: memory.Bitmap{6}}
	b.Structs["struct"] = &memory.StructVector{Rows: 3, Validity: memory.Bitmap{3}}
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 0, 0, 0}, Validity: memory.Bitmap{1}}
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 3, Validity: memory.Bitmap{4}}
	// Opaque fallback must work even when the declared logical type is numeric.
	b.Opaque["numeric"] = &memory.OpaqueVector{Rows: 3, Validity: memory.Bitmap{2}}
	m := &CompiledMultiAggregate{Native: true,
		Columns: []string{"plain", "dict", "struct", "list", "date", "opaque", "numeric", ""},
		Types:   []memory.DataType{memory.TypeString, memory.TypeBinary, memory.TypeStruct, memory.TypeList, memory.TypeDate32, memory.TypeArrow, memory.TypeFloat64, memory.TypeInt64},
		Layouts: make([]aggregate.Layout, 8),
	}
	for lane := range m.Layouts {
		m.Layouts[lane].CountValues = true
	}
	m.Layouts[7] = aggregate.Layout{CountRows: true}
	p := &PreparedQuery{cq: CompiledQuery{GroupShape: GroupShapeGlobal, MultiAggregate: m}}
	if _, err := p.bindSortedBatch(b); err != nil {
		t.Fatal(err)
	}
	s := &sortedAggregateStream{query: p, inputBatch: b, globalBindings: make([]globalAggregateLaneBinding, 8)}
	s.bindGlobalAggregateInputs()
	for lane, mask := range []uint64{5, 6, 3, 1, 7, 4, 2, 0} {
		for row := 0; row < 3; row++ {
			bits, valid := s.globalBindings[lane].bits(row)
			if bits != 0 || valid != (mask&(1<<row) != 0) {
				t.Fatalf("lane=%d row=%d bits=%x valid=%v", lane, row, bits, valid)
			}
		}
		bound := s.globalBindings[lane]
		bound.validity, bound.rowsOnly = nil, false
		if !reflect.DeepEqual(bound, globalAggregateLaneBinding{}) {
			t.Fatalf("count-only lane %d retained a value vector", lane)
		}
	}
}

func TestGlobalAggregateBindingsRebindBorrowedHeaders(t *testing.T) {
	s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true}), expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.CountColumn("v", "nonnull"), expr.Count("rows")}})
	wantBytes := int64(len(s.globalBindings)) * int64(unsafe.Sizeof(globalAggregateLaneBinding{}))
	if s.globalBindingLease == nil || s.globalBindingLease.Bytes() != wantBytes {
		t.Fatalf("unaccounted bindings: lease=%v bytes=%d", s.globalBindingLease, wantBytes)
	}
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	v := &memory.Float64Vector{}
	index := 0
	input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
		assertGlobalBindingsCleared(t, s)
		b.ClearColumns()
		// One header is recycled while backing buffers and validity change.
		switch index {
		case 0:
			v.Values, v.Validity = []float64{1e16, 999, 1}, memory.Bitmap{5}
		case 1:
			v.Values, v.Validity = nil, nil // Empty batch still binds and validates.
		case 2:
			v.Values, v.Validity = []float64{-1e16}, nil
		case 3:
			v.Values, v.Validity = []float64{999, 3}, memory.Bitmap{2}
		default:
			return false, nil
		}
		index++
		b.Float64s["v"], b.Rows = v, len(v.Values)
		return true, nil
	}}
	s.input = input
	out := memory.NewBatch()
	if ok, err := s.Next(context.Background(), out); err != nil || !ok {
		t.Fatalf("next=%v err=%v", ok, err)
	}
	if got := out.Float64s["sum"].Values[0]; math.Float64bits(got) != math.Float64bits(3) || out.Uint64s["nonnull"].Values[0] != 4 || out.Uint64s["rows"].Values[0] != 6 {
		t.Fatalf("sum=%v nonnull=%d rows=%d", got, out.Uint64s["nonnull"].Values[0], out.Uint64s["rows"].Values[0])
	}
	assertGlobalBindingsCleared(t, s)
	if s.inputBatch.Rows != 0 || len(s.inputBatch.Float64s) != 0 || input.closed != 1 {
		t.Fatal("input not released at EOF")
	}
	if ok, err := s.Next(context.Background(), memory.NewBatch()); ok || err != nil || budget.Used() != 0 || out.Rows != 0 || s.globalBindings != nil || s.globalBindingLease != nil {
		t.Fatalf("cleanup: ok=%v err=%v bytes=%d", ok, err, budget.Used())
	}
}

func TestGlobalAggregateBindingsValidationAndTermination(t *testing.T) {
	for _, mode := range []string{"missing", "short-values", "short-validity", "cancel", "input-error", "close"} {
		t.Run(mode, func(t *testing.T) {
			s, budget := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}), expr.Query{Filter: expr.Bool(false), Aggregates: []expr.Aggregate{expr.Sum("v", "sum")}})
			s.input.Close()
			injected := errors.New("injected input error")
			input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
				assertGlobalBindingsCleared(t, s)
				b.ClearColumns()
				b.Rows = 65
				switch mode {
				case "missing":
				case "short-values":
					b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, 64)}
				case "short-validity":
					b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, 65), Validity: memory.Bitmap{1}}
				default:
					return false, injected
				}
				return true, nil
			}}
			s.input = input
			// Seed old references to test cleanup before producer recycling as well.
			s.globalBindings[0].float64s = &memory.Float64Vector{Values: []float64{1}}
			ctx := context.Background()
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if mode == "close" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				ok, err := s.Next(ctx, memory.NewBatch())
				if ok || err == nil {
					t.Fatalf("next=%v err=%v", ok, err)
				}
				switch mode {
				case "cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "input-error":
					if !errors.Is(err, injected) {
						t.Fatal(err)
					}
				case "missing":
					if !strings.Contains(err.Error(), `aggregate column "v" has no supported float64 vector`) {
						t.Fatal(err)
					}
				default:
					if !strings.Contains(err.Error(), `aggregate column "v" shorter than batch`) {
						t.Fatal(err)
					}
				}
				if ok, again := s.Next(context.Background(), memory.NewBatch()); ok || again != err {
					t.Fatalf("terminal error changed: %v %v", ok, again)
				}
			}
			if budget.Used() != 0 || s.globalBindings != nil || s.globalBindingLease != nil || input.closed != 1 {
				t.Fatalf("termination leaked: bytes=%d closed=%d", budget.Used(), input.closed)
			}
		})
	}
}

func TestGlobalAggregateBindingsConstructorBudgetRollback(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64})
	s, _ := globalBindingTestStream(t, schema, expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum")}})
	// All earlier state allocations fit; precisely the new descriptors do not.
	limit := s.budget.Used() - s.globalBindingLease.Bytes()
	budget, err := memory.NewEngineBudget(0).BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := s.query.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), budget), s.query.resultSchema(), 1)
	if stream != nil || !errors.Is(err, memory.ErrBudgetExceeded) || !strings.Contains(err.Error(), "reserve global aggregate bindings") {
		t.Fatalf("stream=%v err=%v limit=%d", stream, err, limit)
	}
	if budget.Used() != 0 {
		t.Fatalf("failed constructor retained %d bytes", budget.Used())
	}
	// The failed acquirePrepared=true constructor must release exactly one owner.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.query.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGlobalAggregateBindingsEmptyAndNullIdentity(t *testing.T) {
	for _, mode := range []string{"empty", "all-null", "filtered"} {
		t.Run(mode, func(t *testing.T) {
			q := expr.Query{Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.CountColumn("v", "nonnull"), expr.Count("rows")}}
			if mode == "filtered" {
				q.Filter = expr.Bool(false)
			}
			s, _ := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat32, Nullable: true}), q)
			if len(s.globalBindings) != 1 {
				t.Fatal("native global path was not enabled")
			}
			if mode != "empty" {
				s.input.Close()
				done := false
				s.input = &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
					b.ClearColumns()
					if done {
						return false, nil
					}
					done = true
					b.Rows = 3
					b.Float32s["v"] = &memory.Float32Vector{Values: []float32{1, 2, 3}, Validity: memory.Bitmap{0}}
					return true, nil
				}}
			}
			out := memory.NewBatch()
			if ok, err := s.Next(context.Background(), out); !ok || err != nil {
				t.Fatalf("next=%v %v", ok, err)
			}
			wantRows := uint64(0)
			if mode == "all-null" {
				wantRows = 3
			}
			if out.Float32s["sum"].IsValid(0) || out.Uint64s["nonnull"].Values[0] != 0 || out.Uint64s["rows"].Values[0] != wantRows {
				t.Fatal("incorrect null or empty aggregate identity")
			}
			assertGlobalBindingsCleared(t, s)
		})
	}
}
