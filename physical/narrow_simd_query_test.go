package physical

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Narrow whole-query tests deliberately use baseline APIs only. The same file
// can be overlaid on the preceding tree for before/after executable comparisons.
// Expected values use independent widened Go comparisons, never kernel helpers.
type narrowQueryCase struct {
	typ             memory.DataType
	op              kernel.CompareOp
	literal         int64
	rows, batchRows int
	pattern         string
	chained         bool
}

func (c narrowQueryCase) name() string {
	return fmt.Sprintf("type=%s/op=%s/lit=%d/rows=%d/batch=%d/%s/chained=%t", c.typ, c.op, c.literal, c.rows, c.batchRows, c.pattern, c.chained)
}
func narrowQueryCompare(a, b int64, op kernel.CompareOp) bool {
	switch op {
	case kernel.CompareGT:
		return a > b
	case kernel.CompareGE:
		return a >= b
	case kernel.CompareLT:
		return a < b
	case kernel.CompareLE:
		return a <= b
	case kernel.CompareEQ:
		return a == b
	}
	panic("bad op")
}
func narrowQueryPredicate(c narrowQueryCase) expr.Expr {
	l, r := expr.Col("pred"), expr.I64(c.literal)
	switch c.op {
	case kernel.CompareGT:
		return expr.GT(l, r)
	case kernel.CompareGE:
		return expr.GE(l, r)
	case kernel.CompareLT:
		return expr.LT(l, r)
	case kernel.CompareLE:
		return expr.LE(l, r)
	case kernel.CompareEQ:
		return expr.EQ(l, r)
	}
	panic("bad op")
}
func narrowQueryValue(typ memory.DataType, row int, pattern string) int64 {
	// Mix edge values and pseudo-random low/high halves. These unsigned values
	// deliberately straddle the signed SIMD comparison cut-over.
	if pattern == "dense" || pattern == "selective" {
		return int64((uint32(row)*1664525 + 1013904223) % 100)
	}
	var edge []int64
	switch typ {
	case memory.TypeInt16:
		edge = []int64{math.MinInt16, math.MinInt16 + 1, -1, 0, 1, math.MaxInt16 - 1, math.MaxInt16}
	case memory.TypeUint8:
		edge = []int64{0, 1, 127, 128, 129, 254, 255}
	case memory.TypeUint16:
		edge = []int64{0, 1, 32767, 32768, 32769, 65534, 65535}
	}
	if row%3 != 2 {
		return edge[(row/3+row)%len(edge)]
	}
	n := uint32(row)*1664525 + 1013904223
	switch typ {
	case memory.TypeInt16:
		return int64(int16(n))
	case memory.TypeUint8:
		return int64(uint8(n))
	case memory.TypeUint16:
		return int64(uint16(n))
	}
	panic("bad type")
}
func narrowQueryValid(row int, pattern string) bool {
	return pattern != "allnull" && (pattern != "nullable" || row%11 != 0)
}

type narrowQueryExpected struct {
	rows, pred, extra []int64
	extraValid        []bool
}

func narrowQueryFixture(tb testing.TB, c narrowQueryCase) (source.Source, narrowQueryExpected) {
	tb.Helper()
	var batches []*memory.Batch
	expected := narrowQueryExpected{}
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		ids, extras, gate := make([]int64, n), make([]int64, n), make([]int32, n)
		v16, vu8, vu16 := make([]int16, n), make([]uint8, n), make([]uint16, n)
		var valid memory.Bitmap
		if c.pattern == "nullable" || c.pattern == "allnull" {
			valid = make(memory.Bitmap, memory.WordsFor(n))
		}
		ev := make(memory.Bitmap, memory.WordsFor(n))
		for j := 0; j < n; j++ {
			row := start + j
			v := narrowQueryValue(c.typ, row, c.pattern)
			ids[j] = int64(row)
			extras[j] = int64(row)*13 - 51
			gate[j] = int32(row % 3)
			v16[j] = int16(v)
			vu8[j] = uint8(v)
			vu16[j] = uint16(v)
			if valid != nil && narrowQueryValid(row, c.pattern) {
				memory.Set(valid, j)
			}
			if row%7 != 0 {
				memory.Set(ev, j)
			}
			if narrowQueryValid(row, c.pattern) && narrowQueryCompare(v, c.literal, c.op) && (!c.chained || gate[j] > 0) {
				expected.rows = append(expected.rows, int64(row)+7)
				expected.pred = append(expected.pred, v)
				expected.extra = append(expected.extra, extras[j])
				expected.extraValid = append(expected.extraValid, row%7 != 0)
			}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Int64s["extra"] = &memory.Int64Vector{Values: extras, Validity: ev}
		b.Int32s["gate"] = &memory.Int32Vector{Values: gate}
		switch c.typ {
		case memory.TypeInt16:
			b.Int16s["pred"] = &memory.Int16Vector{Values: v16, Validity: valid}
		case memory.TypeUint8:
			b.Uint8s["pred"] = &memory.Uint8Vector{Values: vu8, Validity: valid}
		case memory.TypeUint16:
			b.Uint16s["pred"] = &memory.Uint16Vector{Values: vu16, Validity: valid}
		}
		batches = append(batches, b)
	}
	s, err := inmemory.New(memory.NewSchema(memory.Field{Name: "pred", Type: c.typ, Nullable: c.pattern == "nullable" || c.pattern == "allnull"}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "extra", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "gate", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	return s, expected
}
func narrowQueryRunner(tb testing.TB, c narrowQueryCase, mode kernel.Mode) (func(bool), *selectionPeakManager, kernel.KernelCapability) {
	tb.Helper()
	input, want := narrowQueryFixture(tb, c)
	var err error
	if c.chained {
		input, err = NewFilterSource(input, expr.GT(expr.Col("gate"), expr.I64(0)))
		if err != nil {
			tb.Fatal(err)
		}
	}
	input, err = NewFilterSource(input, narrowQueryPredicate(c))
	if err != nil {
		tb.Fatal(err)
	}
	manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
	project, err := NewProjectSource(input, []expr.NamedExpr{expr.Named(expr.Add(expr.Col("id"), expr.I64(7)), "rowid"), expr.Named(expr.Col("pred"), "value"), expr.Named(expr.Col("extra"), "extra")}, false, manager)
	if err != nil {
		tb.Fatal(err)
	}
	kernels := kernel.New(mode)
	id, _ := kernel.ComparisonID(c.typ, c.op)
	capability, _ := kernels.Capability(id)
	opts := source.ReadOptions{Kernels: kernels, BatchRows: c.batchRows, MemoryManager: manager}
	ctx := context.Background()
	out := memory.NewBatch()
	drain := func(check bool) {
		stream, err := project.NewBatchStream(ctx, opts)
		if err != nil {
			tb.Fatal(err)
		}
		defer func() {
			if err := stream.Close(); err != nil {
				tb.Error(err)
			}
			if manager.Used() != 0 {
				tb.Errorf("retained managed bytes=%d", manager.Used())
			}
		}()
		count := 0
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				tb.Fatal(err)
			}
			if !ok {
				break
			}
			if check {
				ids, extras := out.Int64s["rowid"], out.Int64s["extra"]
				if ids == nil || extras == nil {
					tb.Fatal("missing materialized columns")
				}
				for j := 0; j < out.Rows; j++ {
					k := count + j
					if k >= len(want.rows) {
						tb.Fatal("extra output row")
					}
					var v int64
					var valid bool
					switch c.typ {
					case memory.TypeInt16:
						x := out.Int16s["value"]
						if x == nil {
							tb.Fatal("wrong int16 output")
						}
						v, valid = int64(x.Values[j]), x.IsValid(j)
					case memory.TypeUint8:
						x := out.Uint8s["value"]
						if x == nil {
							tb.Fatal("wrong uint8 output")
						}
						v, valid = int64(x.Values[j]), x.IsValid(j)
					case memory.TypeUint16:
						x := out.Uint16s["value"]
						if x == nil {
							tb.Fatal("wrong uint16 output")
						}
						v, valid = int64(x.Values[j]), x.IsValid(j)
					}
					if ids.Values[j] != want.rows[k] || !ids.IsValid(j) || !valid || v != want.pred[k] || extras.IsValid(j) != want.extraValid[k] || (extras.IsValid(j) && extras.Values[j] != want.extra[k]) {
						tb.Fatalf("row %d got id=%d pred=%d valid=%t extra=%d/%t want id=%d pred=%d extra=%d/%t", k, ids.Values[j], v, valid, extras.Values[j], extras.IsValid(j), want.rows[k], want.pred[k], want.extra[k], want.extraValid[k])
					}
				}
			}
			count += out.Rows
		}
		if count != len(want.rows) {
			tb.Fatalf("rows=%d want=%d", count, len(want.rows))
		}
	}
	return drain, manager, capability
}
func TestNarrowSIMDWholeQueryOracle(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		literals := []int64{0, 1, 127, 128, 255, 256, 32767, 32768, 65535, 65536, math.MaxInt64}
		if typ == memory.TypeInt16 {
			literals = append(literals, math.MinInt64, -32769, -32768, -32767, -1)
		}
		for _, lit := range literals {
			for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
				for _, pattern := range []string{"mixed", "nullable", "allnull"} {
					c := narrowQueryCase{typ, op, lit, 259, 67, pattern, false}
					var peak int64
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
						t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) {
							run, m, _ := narrowQueryRunner(t, c, mode)
							run(true)
							if mode == kernel.ModeScalar {
								peak = m.peak
							} else if m.peak != peak {
								t.Fatalf("peak=%d scalar=%d", m.peak, peak)
							}
						})
					}
				}
			}
		}
		for _, rows := range []int{0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 4097} {
			for _, chained := range []bool{false, true} {
				for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
					c := narrowQueryCase{typ, op, 1, rows, 127, "nullable", chained}
					for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
						t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) { run, _, _ := narrowQueryRunner(t, c, mode); run(true) })
					}
				}
			}
		}
	}
}
func narrowQueryBenchmarkCases() []narrowQueryCase {
	var out []narrowQueryCase
	for _, typ := range []memory.DataType{memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		for _, pattern := range []string{"dense", "selective", "nullable"} {
			lit := int64(9)
			if pattern == "selective" {
				lit = 98
			}
			if pattern == "nullable" {
				lit = 127
			}
			for _, rows := range []int{7, 65543} {
				out = append(out, narrowQueryCase{typ, kernel.CompareGT, lit, rows, 4096, pattern, false})
			}
		}
	}
	return out
}
func BenchmarkNarrowSIMDWholeQuery(b *testing.B) {
	for _, c := range narrowQueryBenchmarkCases() {
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			b.Run(c.name()+"/mode="+mode.String(), func(b *testing.B) {
				run, m, cap := narrowQueryRunner(b, c, mode)
				run(true)
				m.peak = 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run(false)
				}
				b.StopTimer()
				b.ReportMetric(float64(m.peak), "peak-charged-B")
				if cap.HardwareSIMD {
					b.ReportMetric(1, "hardware-simd")
				}
				b.ReportMetric(float64(c.rows), "source-rows/op")
			})
		}
	}
}
