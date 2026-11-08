package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

// This harness deliberately uses baseline APIs only: the identical test source
// can be overlaid on the pre-binding tree to produce a comparable executable.
type globalBindingCase struct {
	lanes, rows, batchRows int
	pattern, control       string
	mode                   kernel.Mode
}

func (c globalBindingCase) name() string {
	family := "global"
	if c.control != "" {
		family = c.control
	}
	return fmt.Sprintf("%s/lanes=%d/%s/rows=%d/batch=%d/mode=%s", family, c.lanes, c.pattern, c.rows, c.batchRows, c.mode)
}

func globalBindingCases() []globalBindingCase {
	var cases []globalBindingCase
	for _, lanes := range []int{2, 4} {
		for _, pattern := range []string{"dense", "nullable", "selective"} {
			for _, size := range [][2]int{{63, 63}, {4097, 4097}, {32771, 32771}, {32771, 1021}} {
				cases = append(cases, globalBindingCase{lanes: lanes, rows: size[0], batchRows: size[1], pattern: pattern, mode: kernel.ModeAuto})
			}
		}
	}
	cases = append(cases,
		globalBindingCase{2, 32771, 32771, "dense", "grouped", kernel.ModeAuto},
		globalBindingCase{4, 32771, 1021, "nullable", "grouped", kernel.ModeAuto},
		globalBindingCase{2, 32771, 32771, "dense", "minmax", kernel.ModeAuto},
		globalBindingCase{4, 32771, 1021, "nullable", "statistics", kernel.ModeAuto})
	for _, lanes := range []int{2, 4} {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			cases = append(cases, globalBindingCase{lanes, 32771, 1021, "selective", "", mode})
		}
	}
	for _, lanes := range []int{2, 4} {
		cases = append(cases, globalBindingCase{lanes, 262147, 262147, "nullable", "", kernel.ModeAuto}, globalBindingCase{lanes, 32771, 32771, "nullable", "countonly", kernel.ModeAuto})
	}
	return cases
}

type globalBindingColumn struct {
	name  string
	typ   memory.DataType
	bits  []uint64
	valid []bool
}

type globalBindingFixture struct {
	source   *inmemory.Source
	query    expr.Query
	columns  []globalBindingColumn
	selected []bool
}

func makeGlobalBindingFixture(tb testing.TB, c globalBindingCase) globalBindingFixture {
	tb.Helper()
	names := []string{"u", "f", "i", "g"}
	types := []memory.DataType{memory.TypeUint64, memory.TypeFloat64, memory.TypeInt32, memory.TypeFloat32}
	f := globalBindingFixture{selected: make([]bool, c.rows)}
	for lane := 0; lane < c.lanes; lane++ {
		col := globalBindingColumn{names[lane], types[lane], make([]uint64, c.rows), make([]bool, c.rows)}
		for row := 0; row < c.rows; row++ {
			col.valid[row] = c.pattern == "dense" || (row+lane*3)%11 != 0
			if c.pattern == "allnull" {
				col.valid[row] = false
			}
			switch lane {
			case 0:
				col.bits[row] = uint64(1)<<63 + uint64(row%97)
			case 1:
				// Cancellation across SIMD widths and source-batch boundaries detects
				// reassociation. Ordinary values are intentionally not all exact halves.
				values := [...]float64{1e16, 1, -1e16, 0.1, -0.3, 7, math.Copysign(0, -1)}
				col.bits[row] = math.Float64bits(values[row%len(values)])
			case 2:
				col.bits[row] = uint64(int64(int32(math.MaxInt32 - row%91)))
			case 3:
				values := [...]float32{1 << 24, 1, -(1 << 24), 0.1, -0.3, 7, float32(math.Copysign(0, -1))}
				col.bits[row] = uint64(math.Float32bits(values[row%len(values)]))
			}
			if c.pattern == "exceptional" && (lane == 1 || lane == 3) && row < 3 {
				values := [...]float64{math.Inf(1), math.Inf(-1), math.NaN()}
				if lane == 1 {
					col.bits[row] = math.Float64bits(values[row])
				} else {
					col.bits[row] = uint64(math.Float32bits(float32(values[row])))
				}
				col.valid[row] = true
			}
		}
		f.columns = append(f.columns, col)
		if c.control != "countonly" || lane != 0 {
			f.query.Aggregates = append(f.query.Aggregates, expr.Sum(col.name, col.name+"_sum"))
		}
		f.query.Aggregates = append(f.query.Aggregates, expr.CountColumn(col.name, col.name+"_count"))
	}
	f.query.Aggregates = append(f.query.Aggregates, expr.Count("rows"))
	switch c.control {
	case "grouped":
		f.query.GroupBy = []string{"key"}
		f.query.Hints = expr.Hints{Cardinality: 64, ForceStrategy: "bitmap-direct"}
	case "minmax":
		f.query.Aggregates = append(f.query.Aggregates, expr.Min("f", "f_min"), expr.Max("f", "f_max"))
	case "statistics":
		f.query.Aggregates = append(f.query.Aggregates, expr.First("f", "f_first"), expr.Last("g", "g_last"))
	}
	if c.pattern == "selective" || c.pattern == "emptyselection" {
		f.query.Filter = expr.GE(expr.Col("pred"), expr.I64(0))
	}
	fields := []memory.Field{{Name: "key", Type: memory.TypeInt32}, {Name: "pred", Type: memory.TypeInt32}}
	for _, col := range f.columns {
		fields = append(fields, memory.Field{Name: col.name, Type: col.typ, Nullable: c.pattern != "dense"})
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		batch := memory.NewBatch()
		batch.Rows = n
		keys, pred := make([]int32, n), make([]int32, n)
		for j := 0; j < n; j++ {
			row := start + j
			keys[j] = int32(row % 64)
			pred[j] = int32(row%5) - 4
			if c.pattern == "emptyselection" {
				pred[j] = -1
			}
			f.selected[row] = f.query.Filter == nil || pred[j] >= 0
		}
		batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
		batch.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		for _, col := range f.columns {
			var validity memory.Bitmap
			if c.pattern != "dense" {
				validity = make(memory.Bitmap, (n+63)/64)
				for j := 0; j < n; j++ {
					if col.valid[start+j] {
						memory.Set(validity, j)
					}
				}
			}
			switch col.typ {
			case memory.TypeUint64:
				values := append([]uint64(nil), col.bits[start:start+n]...)
				batch.Uint64s[col.name] = &memory.Uint64Vector{Values: values, Validity: validity}
			case memory.TypeFloat64:
				values := make([]float64, n)
				for j := range values {
					values[j] = math.Float64frombits(col.bits[start+j])
				}
				batch.Float64s[col.name] = &memory.Float64Vector{Values: values, Validity: validity}
			case memory.TypeInt32:
				values := make([]int32, n)
				for j := range values {
					values[j] = int32(col.bits[start+j])
				}
				batch.Int32s[col.name] = &memory.Int32Vector{Values: values, Validity: validity}
			case memory.TypeFloat32:
				values := make([]float32, n)
				for j := range values {
					values[j] = math.Float32frombits(uint32(col.bits[start+j]))
				}
				batch.Float32s[col.name] = &memory.Float32Vector{Values: values, Validity: validity}
			}
		}
		batches = append(batches, batch)
	}
	var err error
	f.source, err = inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	return f
}

// The oracle is row ordered and has no dependency on aggregate/kernel helpers.
// It checks native wrapping sums, Float32 step rounding, nullable counts, and
// source order, including across deliberately unaligned batch boundaries.
func globalBindingOracle(c globalBindingCase, f globalBindingFixture) map[int][]TypedValue {
	out := make(map[int][]TypedValue)
	columnByName := make(map[string]globalBindingColumn)
	for _, col := range f.columns {
		columnByName[col.name] = col
	}
	fresh := func() []TypedValue {
		values := make([]TypedValue, len(f.query.Aggregates))
		for i, a := range f.query.Aggregates {
			values[i].Type = columnByName[a.Column].typ
			if a.Kind == expr.AggCountRows || a.Kind == expr.AggCountValues {
				values[i] = TypedValue{Type: memory.TypeUint64, Valid: true}
			}
		}
		return values
	}
	if c.control != "grouped" {
		out[0] = fresh()
	}
	seen := make(map[int]bool)
	for row, selected := range f.selected {
		if !selected {
			continue
		}
		key := 0
		if c.control == "grouped" {
			key = row % 64
		}
		values, ok := out[key]
		if !ok {
			values = fresh()
			out[key] = values
		}
		for i, a := range f.query.Aggregates {
			value := &values[i]
			if a.Kind == expr.AggCountRows {
				value.Bits++
				continue
			}
			col := columnByName[a.Column]
			bits, valid := col.bits[row], col.valid[row]
			if a.Kind == expr.AggFirst {
				if !seen[key] {
					value.Bits, value.Valid = bits, valid
				}
				continue
			}
			if a.Kind == expr.AggLast {
				value.Bits, value.Valid = bits, valid
				continue
			}
			if !valid {
				continue
			}
			switch a.Kind {
			case expr.AggCountValues:
				value.Bits++
			case expr.AggSum:
				switch col.typ {
				case memory.TypeUint64:
					value.Bits += bits
				case memory.TypeInt32:
					value.Bits = uint64(int64(int32(value.Bits + bits)))
				case memory.TypeFloat32:
					value.Bits = uint64(math.Float32bits(math.Float32frombits(uint32(value.Bits)) + math.Float32frombits(uint32(bits))))
				case memory.TypeFloat64:
					value.Bits = math.Float64bits(math.Float64frombits(value.Bits) + math.Float64frombits(bits))
				}
				value.Valid = true
			case expr.AggMin, expr.AggMax:
				if !value.Valid {
					value.Bits = bits
				} else {
					x, y := math.Float64frombits(value.Bits), math.Float64frombits(bits)
					if a.Kind == expr.AggMin {
						value.Bits = math.Float64bits(math.Min(x, y))
					} else {
						value.Bits = math.Float64bits(math.Max(x, y))
					}
				}
				value.Valid = true
			}
		}
		seen[key] = true
	}
	return out
}

func checkGlobalBindingResult(tb testing.TB, c globalBindingCase, f globalBindingFixture, result TypedResult) {
	tb.Helper()
	want := globalBindingOracle(c, f)
	selected := 0
	for _, yes := range f.selected {
		if yes {
			selected++
		}
	}
	if len(result.Rows) != len(want) || result.Summary.GroupCount != len(want) || result.Summary.SelectedRows != selected {
		tb.Fatalf("result summary: rows=%d want=%d summary=%+v selected=%d", len(result.Rows), len(want), result.Summary, selected)
	}
	for _, row := range result.Rows {
		key := 0
		if c.control == "grouped" {
			key = int(int32(row.Keys[0].Value.Bits))
		}
		expected, ok := want[key]
		if !ok {
			tb.Fatalf("unexpected key %d", key)
		}
		delete(want, key)
		for i, got := range row.Aggregates {
			expectedValue := expected[i]
			if got.Value.Type != expectedValue.Type || got.Value.Valid != expectedValue.Valid || (got.Value.Valid && got.Value.Bits != expectedValue.Bits) {
				tb.Fatalf("key=%d aggregate=%s got=%+v want=%+v", key, got.Alias, got.Value, expectedValue)
			}
		}
	}
}

func prepareGlobalBinding(tb testing.TB, c globalBindingCase, f globalBindingFixture) (*Engine, *PreparedQuery) {
	tb.Helper()
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = c.mode
	opts.MemoryLimitBytes = 64 << 20
	// A 32771-row source batch fits one scan view. The 262147-row source batch
	// is split at MaxMorselRows into five views. Explicit 1021-row source-batch
	// boundaries force more frequent rebinding in the multi-batch cases.
	opts.Pipeline.TargetMorselBytes = 4 << 20
	opts.Pipeline.MaxMorselBytes = 8 << 20
	opts.Pipeline.MaxMorselRows = 65536
	e := New(opts)
	p, err := e.Prepare(context.Background(), f.source, f.query, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	if !p.nativeAggregate() {
		tb.Fatal("workload unexpectedly missed native aggregate compilation")
	}
	if c.control != "grouped" && (!p.IsSortedAggregate() || p.cq.GroupShape != GroupShapeGlobal) {
		tb.Fatalf("unexpected global plan: %+v", p.Plan())
	}
	return e, p
}

func verifyGlobalBindingExecution(tb testing.TB, c globalBindingCase, f globalBindingFixture, e *Engine, p *PreparedQuery) int64 {
	tb.Helper()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		tb.Fatal(err)
	}
	result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
	if err != nil {
		budget.Close()
		tb.Fatal(err)
	}
	checkGlobalBindingResult(tb, c, f, result)
	snapshot := budget.Snapshot()
	if snapshot.UsedBytes != 0 || e.Memory().Used() != 0 {
		tb.Fatalf("execution leaked: query=%d engine=%d", snapshot.UsedBytes, e.Memory().Used())
	}
	if err := budget.Close(); err != nil {
		tb.Fatal(err)
	}
	return snapshot.PeakBytes
}

func TestGlobalBindingWholeQueryOracle(t *testing.T) {
	cases := globalBindingCases()
	cases = append(cases[:28], cases[32:]...)
	for _, pattern := range []string{"allnull", "emptyselection", "exceptional"} {
		cases = append(cases, globalBindingCase{4, 4097, 127, pattern, "", kernel.ModeAuto})
	}
	cases = append(cases, globalBindingCase{4, 0, 127, "dense", "", kernel.ModeAuto})
	for _, c := range cases {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			c.mode = mode
			t.Run(c.name(), func(t *testing.T) {
				f := makeGlobalBindingFixture(t, c)
				e, p := prepareGlobalBinding(t, c, f)
				defer e.Close()
				defer p.Close()
				peak := verifyGlobalBindingExecution(t, c, f, e, p)
				if repeat := verifyGlobalBindingExecution(t, c, f, e, p); repeat != peak {
					t.Fatalf("unstable charged peak %d -> %d", peak, repeat)
				}
				// The eligibility description uses only common baseline metadata. The
				// candidate's implementation/profile separately proves actual dispatch.
				t.Logf("plan=%+v native=%t binding_scope=%t lanes=%d statistics=%t layouts=%+v peak-charged-B=%d used-after=0 mode=%s arch=%s", p.Plan(), p.nativeAggregate(), c.control == "" || c.control == "countonly", len(p.cq.MultiAggregate.Columns), p.statistics() != nil, p.cq.MultiAggregate.Layouts, peak, e.KernelCapabilities().Mode(), e.KernelCapabilities().Arch())
			})
		}
	}
}

var globalBindingResultSink TypedResult

// BenchmarkGlobalBindingWholeQuery measures complete ExecuteTyped execution:
// source/state open, scan, reduction, result materialization and close are
// timed. Immutable input, plan preparation, exact oracle and peak inspection
// are excluded. Peak is query-charged memory, not fixture bytes or process RSS.
func BenchmarkGlobalBindingWholeQuery(b *testing.B) {
	for _, c := range globalBindingCases() {
		b.Run(c.name(), func(b *testing.B) {
			f := makeGlobalBindingFixture(b, c)
			e, p := prepareGlobalBinding(b, c, f)
			defer e.Close()
			defer p.Close()
			peak := verifyGlobalBindingExecution(b, c, f, e, p)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					b.Fatal(err)
				}
				globalBindingResultSink = result
			}
			b.StopTimer()
			b.ReportMetric(float64(peak), "peak-charged-B")
			if e.Memory().Used() != 0 {
				b.Fatalf("execution retained %d charged bytes", e.Memory().Used())
			}
		})
	}
}
