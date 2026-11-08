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

// These tests use independent Go operators, not scalar kernels, as the oracle.
// 8199 rows produce two full 4096-row batches and a seven-row SIMD/bitmap tail.
// Float32 arithmetic intentionally checks its existing Float64 promotion semantics;
// it is a control, not coverage of native Float32Arithmetic dispatch.
// ModeSIMD is a request, not proof of native execution on unsupported builds.
const numericQueryRows = 8199

type numericQueryCase struct {
	typ, op string
	column  bool
}

func numericQueryCases() []numericQueryCase {
	var cases []numericQueryCase
	for _, typ := range []string{"int8", "int32", "int64", "uint32", "uint64", "float64"} {
		for _, op := range []string{"gt", "ge", "lt", "le", "eq"} {
			cases = append(cases, numericQueryCase{typ, op, false})
			if typ == "float64" {
				cases = append(cases, numericQueryCase{typ, op, true})
			}
		}
	}
	cases = append(cases, numericQueryCase{"float64", "add", true})
	for _, op := range []string{"add", "sub", "mul", "div"} {
		cases = append(cases, numericQueryCase{"float32", op, true})
	}
	return cases
}
func (c numericQueryCase) arithmetic() bool {
	return c.op == "add" || c.op == "sub" || c.op == "mul" || c.op == "div"
}
func numericQueryExpression(c numericQueryCase) expr.Expr {
	var right expr.Expr = expr.I64(1)
	if c.typ == "float64" {
		right = expr.F64(1)
	}
	if c.column {
		right = expr.Col("right")
	}
	left := expr.Col("left")
	switch c.op {
	case "gt":
		return expr.GT(left, right)
	case "ge":
		return expr.GE(left, right)
	case "lt":
		return expr.LT(left, right)
	case "le":
		return expr.LE(left, right)
	case "eq":
		return expr.EQ(left, right)
	case "add":
		return expr.Add(left, right)
	case "sub":
		return expr.Sub(left, right)
	case "mul":
		return expr.Mul(left, right)
	case "div":
		return expr.Div(left, right)
	}
	panic("invalid query case")
}
func numericQueryCompare[T ~int8 | ~int32 | ~int64 | ~uint32 | ~uint64 | ~float64](op string, a, b T) bool {
	switch op {
	case "gt":
		return a > b
	case "ge":
		return a >= b
	case "lt":
		return a < b
	case "le":
		return a <= b
	case "eq":
		return a == b
	}
	panic("invalid comparison")
}

type numericQueryExpected struct {
	rows   []int64
	values []float64
	valid  []bool
}

func numericQueryFixture(t testing.TB, c numericQueryCase, nullable bool) (source.Source, numericQueryExpected) {
	t.Helper()
	batch := memory.NewBatch()
	batch.Rows = numericQueryRows
	payload := make([]int64, numericQueryRows)
	var lv, rv memory.Bitmap
	if nullable {
		lv = make(memory.Bitmap, memory.WordsFor(numericQueryRows))
		rv = make(memory.Bitmap, memory.WordsFor(numericQueryRows))
	}
	leftValid := func(row int) bool { return !nullable || row%11 != 0 }
	rightValid := func(row int) bool { return !nullable || row%13 != 0 }
	for row := range payload {
		payload[row] = int64(row)
		if nullable && leftValid(row) {
			lv[row>>6] |= uint64(1) << uint(row&63)
		}
		if nullable && rightValid(row) {
			rv[row>>6] |= uint64(1) << uint(row&63)
		}
	}
	batch.Int64s["payload"] = &memory.Int64Vector{Values: payload}
	expected := numericQueryExpected{}
	dtype := memory.TypeInt64
	switch c.typ {
	case "int8":
		dtype = memory.TypeInt8
		samples := []int8{-128, -127, -1, 0, 1, 2, 126, 127}
		left, right := make([]int8, numericQueryRows), make([]int8, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if valid && numericQueryCompare(c.op, left[row], 1) {
				expected.rows = append(expected.rows, int64(row))
			}
		}
		batch.Int8s["left"] = &memory.Int8Vector{Values: left, Validity: lv}
		batch.Int8s["right"] = &memory.Int8Vector{Values: right, Validity: rv}
	case "int32":
		dtype = memory.TypeInt32
		samples := []int32{math.MinInt32, -1, 0, 1, 2, math.MaxInt32}
		left, right := make([]int32, numericQueryRows), make([]int32, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if valid && numericQueryCompare(c.op, left[row], 1) {
				expected.rows = append(expected.rows, int64(row))
			}
		}
		batch.Int32s["left"] = &memory.Int32Vector{Values: left, Validity: lv}
		batch.Int32s["right"] = &memory.Int32Vector{Values: right, Validity: rv}
	case "int64":
		dtype = memory.TypeInt64
		samples := []int64{math.MinInt64, -1, 0, 1, 2, math.MaxInt64}
		left, right := make([]int64, numericQueryRows), make([]int64, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if valid && numericQueryCompare(c.op, left[row], 1) {
				expected.rows = append(expected.rows, int64(row))
			}
		}
		batch.Int64s["left"] = &memory.Int64Vector{Values: left, Validity: lv}
		batch.Int64s["right"] = &memory.Int64Vector{Values: right, Validity: rv}
	case "uint32":
		dtype = memory.TypeUint32
		samples := []uint32{0, 1, 2, 1<<31 - 1, 1 << 31, math.MaxUint32}
		left, right := make([]uint32, numericQueryRows), make([]uint32, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if valid && numericQueryCompare(c.op, left[row], 1) {
				expected.rows = append(expected.rows, int64(row))
			}
		}
		batch.Uint32s["left"] = &memory.Uint32Vector{Values: left, Validity: lv}
		batch.Uint32s["right"] = &memory.Uint32Vector{Values: right, Validity: rv}
	case "uint64":
		dtype = memory.TypeUint64
		samples := []uint64{0, 1, 2, 1<<63 - 1, 1 << 63, math.MaxUint64}
		left, right := make([]uint64, numericQueryRows), make([]uint64, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if valid && numericQueryCompare(c.op, left[row], 1) {
				expected.rows = append(expected.rows, int64(row))
			}
		}
		batch.Uint64s["left"] = &memory.Uint64Vector{Values: left, Validity: lv}
		batch.Uint64s["right"] = &memory.Uint64Vector{Values: right, Validity: rv}
	case "float64":
		dtype = memory.TypeFloat64
		samples := []float64{math.Inf(-1), -math.MaxFloat64, -1, math.Copysign(0, -1), 0, 1, math.Nextafter(1, 2), 2, math.MaxFloat64, math.Inf(1), math.NaN()}
		left, right := make([]float64, numericQueryRows), make([]float64, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if c.arithmetic() {
				value := left[row] + right[row]
				switch c.op {
				case "sub":
					value = left[row] - right[row]
				case "mul":
					value = left[row] * right[row]
				case "div":
					value = left[row] / right[row]
				}
				expected.rows = append(expected.rows, int64(row))
				expected.values = append(expected.values, float64(value))
				expected.valid = append(expected.valid, valid)
			} else {
				r := float64(1)
				if c.column {
					r = right[row]
				}
				if valid && numericQueryCompare(c.op, left[row], r) {
					expected.rows = append(expected.rows, int64(row))
				}
			}
		}
		batch.Float64s["left"] = &memory.Float64Vector{Values: left, Validity: lv}
		batch.Float64s["right"] = &memory.Float64Vector{Values: right, Validity: rv}
	case "float32":
		dtype = memory.TypeFloat32
		samples := []float32{float32(math.Inf(-1)), -math.MaxFloat32, -1, float32(math.Copysign(0, -1)), 0, 1, math.Nextafter32(1, 2), 2, math.MaxFloat32, float32(math.Inf(1)), float32(math.NaN())}
		left, right := make([]float32, numericQueryRows), make([]float32, numericQueryRows)
		for row := range left {
			left[row] = samples[(row*7)%len(samples)]
			right[row] = samples[(row*3+1)%len(samples)]
			valid := leftValid(row) && (!c.column || rightValid(row))
			if c.arithmetic() {
				value := float64(left[row]) + float64(right[row])
				switch c.op {
				case "sub":
					value = float64(left[row]) - float64(right[row])
				case "mul":
					value = float64(left[row]) * float64(right[row])
				case "div":
					value = float64(left[row]) / float64(right[row])
				}
				expected.rows = append(expected.rows, int64(row))
				expected.values = append(expected.values, float64(value))
				expected.valid = append(expected.valid, valid)
			}
		}
		batch.Float32s["left"] = &memory.Float32Vector{Values: left, Validity: lv}
		batch.Float32s["right"] = &memory.Float32Vector{Values: right, Validity: rv}
	}
	input, err := inmemory.New(memory.NewSchema(memory.Field{Name: "left", Type: dtype, Nullable: nullable}, memory.Field{Name: "right", Type: dtype, Nullable: nullable}, memory.Field{Name: "payload", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	return input, expected
}

func numericQueryRunner(t testing.TB, input source.Source, expected numericQueryExpected, c numericQueryCase, mode kernel.Mode) (func(bool), *selectionPeakManager) {
	t.Helper()
	manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
	var err error
	names := []expr.NamedExpr{expr.Named(expr.Add(expr.Col("payload"), expr.I64(7)), "id")}
	if c.arithmetic() {
		names = append(names, expr.Named(numericQueryExpression(c), "value"))
	} else {
		input, err = NewFilterSource(input, numericQueryExpression(c))
		if err != nil {
			t.Fatal(err)
		}
	}
	project, err := NewProjectSource(input, names, false, manager)
	if err != nil {
		t.Fatal(err)
	}
	opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: 4096, MemoryManager: manager}
	ctx, out := context.Background(), memory.NewBatch()
	drain := func(check bool) {
		stream, err := project.NewBatchStream(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		// Always close, including assertion failures, so failure cannot mask a leak.
		defer func() {
			if err := stream.Close(); err != nil {
				t.Error(err)
			}
			if manager.Used() != 0 {
				t.Errorf("retained %d managed bytes", manager.Used())
			}
		}()
		count := 0
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if check {
				ids := out.Int64s["id"]
				if ids == nil || len(ids.Values) != out.Rows {
					t.Fatal("missing id vector")
				}
				for row, id := range ids.Values {
					index := count + row
					if index >= len(expected.rows) || id != expected.rows[index]+7 || !ids.IsValid(row) {
						t.Fatalf("wrong row identity at %d: %d", index, id)
					}
					if c.arithmetic() {
						var value float64
						var valid bool
						v := out.Float64s["value"]
						if v == nil {
							t.Fatal("missing float64 result (including promoted float32 arithmetic)")
						}
						value, valid = v.Values[row], v.IsValid(row)
						want := expected.values[index]
						if valid != expected.valid[index] || (valid && !(math.IsNaN(value) && math.IsNaN(want)) && math.Float64bits(value) != math.Float64bits(want)) {
							t.Fatalf("row %d got %g valid=%t, want %g valid=%t", index, value, valid, want, expected.valid[index])
						}
					}
				}
			}
			count += out.Rows
		}
		if count != len(expected.rows) {
			t.Fatalf("got %d rows, want %d", count, len(expected.rows))
		}
	}
	return drain, manager
}

func TestNumericSIMDWholeQuery(t *testing.T) {
	for _, c := range numericQueryCases() {
		for _, nullable := range []bool{false, true} {
			input, expected := numericQueryFixture(t, c, nullable)
			var scalarPeak int64
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				name := fmt.Sprintf("type=%s/op=%s/column=%t/nullable=%t/mode=%s", c.typ, c.op, c.column, nullable, mode)
				t.Run(name, func(t *testing.T) {
					drain, manager := numericQueryRunner(t, input, expected, c, mode)
					drain(true)
					if manager.peak == 0 {
						t.Fatal("query did not account for managed workspace")
					}
					if mode == kernel.ModeScalar {
						scalarPeak = manager.peak
					} else if manager.peak != scalarPeak {
						t.Fatalf("managed peak %d differs from scalar %d", manager.peak, scalarPeak)
					}
				})
			}
		}
	}
}

// Open/drain/close timings include source and workspace setup, validity and
// materialization. Fixtures and compilation are excluded. peak_bytes is managed
// execution workspace, not fixture storage or RSS. ReportAllocs measures Go heap.
func BenchmarkNumericSIMDWholeQuery(b *testing.B) {
	for _, c := range []numericQueryCase{{"float64", "add", true}, {"float64", "gt", false}, {"float64", "le", true}, {"float32", "add", true}, {"float32", "sub", true}, {"float32", "mul", true}, {"float32", "div", true}, {"int8", "gt", false}, {"int32", "gt", false}, {"int64", "gt", false}, {"uint32", "gt", false}, {"uint64", "gt", false}} {
		for _, nullable := range []bool{false, true} {
			input, expected := numericQueryFixture(b, c, nullable)
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				name := fmt.Sprintf("type=%s/op=%s/column=%t/nullable=%t/mode=%s", c.typ, c.op, c.column, nullable, mode)
				b.Run(name, func(b *testing.B) {
					drain, manager := numericQueryRunner(b, input, expected, c, mode)
					drain(true)
					manager.peak = 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						drain(false)
					}
					b.StopTimer()
					b.ReportMetric(float64(manager.peak), "peak_bytes")
					b.ReportMetric(numericQueryRows, "source_rows/op")
					b.ReportMetric(float64(len(expected.rows)), "projected_rows/op")
				})
			}
		}
	}
}

// Compare allocations in one process rather than pinning a compiler-dependent
// absolute count. Native dispatch must not add a per-query heap allocation.
func TestNumericSIMDWholeQueryAllocations(t *testing.T) {
	for _, c := range []numericQueryCase{{"float64", "add", true}, {"float64", "gt", false}, {"float64", "le", true}, {"int8", "gt", false}, {"int32", "gt", false}, {"int64", "gt", false}, {"uint32", "gt", false}, {"uint64", "gt", false}, {"float32", "div", true}} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("type=%s/op=%s/column=%t/nullable=%t", c.typ, c.op, c.column, nullable), func(t *testing.T) {
				input, expected := numericQueryFixture(t, c, nullable)
				var scalarAllocs float64
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					drain, _ := numericQueryRunner(t, input, expected, c, mode)
					drain(true)
					allocs := testing.AllocsPerRun(10, func() { drain(false) })
					if mode == kernel.ModeScalar {
						scalarAllocs = allocs
					} else if allocs > scalarAllocs {
						t.Fatalf("%s allocates %.0f times/query, scalar %.0f", mode, allocs, scalarAllocs)
					}
				}
			})
		}
	}
}
