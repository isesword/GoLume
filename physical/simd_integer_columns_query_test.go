package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// Baseline-compatible harness: only pre-stage public APIs are used, so this
// source can be overlaid on the preceding revision for identical benchmarks.
// uint64 bit patterns retain every unsigned extreme; signed comparisons below
// explicitly interpret those bits as int64, independently of kernel helpers.
type integerColumnsCase struct {
	typ         memory.DataType
	op          kernel.CompareOp
	rows, batch int
	pattern     string
	chained     bool
	logical     string
}

func (c integerColumnsCase) name() string {
	return fmt.Sprintf("type=%s/op=%s/rows=%d/batch=%d/%s/chained=%t/logic=%s", c.typ, c.op, c.rows, c.batch, c.pattern, c.chained, c.logical)
}

var integerColumnsTypes = []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint64}
var integerColumnsModes = []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD}

func integerColumnsSigned(t memory.DataType) bool {
	return t == memory.TypeInt8 || t == memory.TypeInt16 || t == memory.TypeInt64
}
func integerColumnsValue(t memory.DataType, row int) uint64 {
	var edges []uint64
	switch t {
	case memory.TypeInt8:
		edges = []uint64{uint64(0xffffffffffffff80), uint64(0xffffffffffffff81), ^uint64(0), 0, 1, 126, 127}
	case memory.TypeInt16:
		edges = []uint64{uint64(0xffffffffffff8000), uint64(0xffffffffffff8001), ^uint64(0), 0, 1, 32766, 32767}
	case memory.TypeInt64:
		edges = []uint64{1 << 63, 1<<63 | 1, ^uint64(0), 0, 1, 1<<63 - 2, 1<<63 - 1}
	case memory.TypeUint8:
		edges = []uint64{0, 1, 127, 128, 129, 254, 255}
	case memory.TypeUint16:
		edges = []uint64{0, 1, 32767, 32768, 32769, 65534, 65535}
	case memory.TypeUint64:
		edges = []uint64{0, 1, 1<<63 - 1, 1 << 63, 1<<63 | 1, ^uint64(0) - 1, ^uint64(0)}
	}
	return edges[row%len(edges)]
}
func integerColumnsCompare(t memory.DataType, a, b uint64, op kernel.CompareOp) bool {
	if integerColumnsSigned(t) {
		x, y := int64(a), int64(b)
		switch op {
		case kernel.CompareGT:
			return x > y
		case kernel.CompareGE:
			return x >= y
		case kernel.CompareLT:
			return x < y
		case kernel.CompareLE:
			return x <= y
		case kernel.CompareEQ:
			return x == y
		}
	} else {
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
	}
	panic("invalid comparison")
}
func integerColumnsPredicate(op kernel.CompareOp) expr.Expr {
	a, b := expr.Col("a"), expr.Col("b")
	switch op {
	case kernel.CompareGT:
		return expr.GT(a, b)
	case kernel.CompareGE:
		return expr.GE(a, b)
	case kernel.CompareLT:
		return expr.LT(a, b)
	case kernel.CompareLE:
		return expr.LE(a, b)
	case kernel.CompareEQ:
		return expr.EQ(a, b)
	}
	panic("invalid comparison")
}

func integerColumnsFilter(c integerColumnsCase) expr.Expr {
	p := integerColumnsPredicate(c.op)
	switch c.logical {
	case "not":
		return expr.Not(p)
	case "and":
		return expr.And(p, expr.GT(expr.Col("gate"), expr.I64(0)))
	case "or":
		return expr.Or(p, expr.GT(expr.Col("gate"), expr.I64(0)))
	}
	return p
}

type integerColumnsRow struct {
	aValid, bValid bool
	id             int64
	a, b           uint64
	extra          int64
	valid          bool
}

func integerColumnsPair(c integerColumnsCase, row int) (uint64, uint64) {
	a, b := integerColumnsValue(c.typ, row), integerColumnsValue(c.typ, row/7)
	switch c.pattern {
	case "dense":
		a, b = 10, 1
	case "selective":
		a, b = 0, 1
		if row%101 == 0 {
			a = 2
		}
	case "zero":
		switch c.op {
		case kernel.CompareGT, kernel.CompareGE:
			a, b = 0, 1
		case kernel.CompareLT, kernel.CompareLE:
			a, b = 1, 0
		case kernel.CompareEQ:
			a, b = 0, 1
		}
	}
	return a, b
}
func integerColumnsFixture(tb testing.TB, c integerColumnsCase) (source.Source, []integerColumnsRow) {
	tb.Helper()
	var batches []*memory.Batch
	var want []integerColumnsRow
	for start := 0; start < c.rows; start += c.batch {
		n := min(c.batch, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		av, bv := make([]uint64, n), make([]uint64, n)
		ids, extras, gate := make([]int64, n), make([]int64, n), make([]int32, n)
		var va, vb memory.Bitmap
		if c.pattern == "nullable" || c.pattern == "allnull" {
			va = make(memory.Bitmap, memory.WordsFor(n))
			vb = make(memory.Bitmap, memory.WordsFor(n))
		}
		ev := make(memory.Bitmap, memory.WordsFor(n))
		for j := 0; j < n; j++ {
			row := start + j
			a, z := integerColumnsPair(c, row)
			av[j], bv[j] = a, z
			ids[j] = int64(row)
			extras[j] = int64(row)*13 - 51
			gate[j] = int32(row % 3)
			leftOK := c.pattern != "allnull" && (c.pattern != "nullable" || row%11 != 0)
			rightOK := c.pattern != "allnull" && (c.pattern != "nullable" || row%13 != 1)
			if va != nil && leftOK {
				memory.Set(va, j)
			}
			if vb != nil && rightOK {
				memory.Set(vb, j)
			}
			if row%7 != 0 {
				memory.Set(ev, j)
			}
			selected := leftOK && rightOK && integerColumnsCompare(c.typ, a, z, c.op)
			switch c.logical {
			case "not":
				selected = leftOK && rightOK && !integerColumnsCompare(c.typ, a, z, c.op)
			case "and":
				selected = selected && gate[j] > 0
			case "or":
				selected = selected || gate[j] > 0
			}
			if selected && (!c.chained || gate[j] > 0) {
				want = append(want, integerColumnsRow{leftOK, rightOK, int64(row) + 7, a, z, extras[j], row%7 != 0})
			}
		}
		integerColumnsPut(b, "a", c.typ, av, va)
		integerColumnsPut(b, "b", c.typ, bv, vb)
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Int64s["extra"] = &memory.Int64Vector{Values: extras, Validity: ev}
		b.Int32s["gate"] = &memory.Int32Vector{Values: gate}
		batches = append(batches, b)
	}
	s, err := inmemory.New(memory.NewSchema(memory.Field{Name: "a", Type: c.typ, Nullable: true}, memory.Field{Name: "b", Type: c.typ, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "extra", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "gate", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	return s, want
}

// Offset value slices deliberately begin three elements into backing storage.
func integerColumnsPut(b *memory.Batch, name string, t memory.DataType, values []uint64, v memory.Bitmap) {
	switch t {
	case memory.TypeInt8:
		x := make([]int8, len(values)+3)[3:]
		for i, z := range values {
			x[i] = int8(z)
		}
		b.Int8s[name] = &memory.Int8Vector{Values: x, Validity: v}
	case memory.TypeInt16:
		x := make([]int16, len(values)+3)[3:]
		for i, z := range values {
			x[i] = int16(z)
		}
		b.Int16s[name] = &memory.Int16Vector{Values: x, Validity: v}
	case memory.TypeInt64:
		x := make([]int64, len(values)+3)[3:]
		for i, z := range values {
			x[i] = int64(z)
		}
		b.Int64s[name] = &memory.Int64Vector{Values: x, Validity: v}
	case memory.TypeUint8:
		x := make([]uint8, len(values)+3)[3:]
		for i, z := range values {
			x[i] = uint8(z)
		}
		b.Uint8s[name] = &memory.Uint8Vector{Values: x, Validity: v}
	case memory.TypeUint16:
		x := make([]uint16, len(values)+3)[3:]
		for i, z := range values {
			x[i] = uint16(z)
		}
		b.Uint16s[name] = &memory.Uint16Vector{Values: x, Validity: v}
	case memory.TypeUint64:
		x := make([]uint64, len(values)+3)[3:]
		for i, z := range values {
			x[i] = uint64(z)
		}
		b.Uint64s[name] = &memory.Uint64Vector{Values: x, Validity: v}
	}
}

func integerColumnsCases() []integerColumnsCase {
	var cases []integerColumnsCase
	for _, typ := range integerColumnsTypes {
		for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
			for _, pattern := range []string{"mixed", "nullable", "allnull", "zero"} {
				for _, chain := range []bool{false, true} {
					cases = append(cases, integerColumnsCase{typ, op, 259, 65, pattern, chain, ""})
				}
			}
			for _, logic := range []string{"not", "and", "or"} {
				cases = append(cases, integerColumnsCase{typ, op, 259, 65, "nullable", false, logic})
			}
			for _, n := range []int{0, 1, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129} {
				cases = append(cases, integerColumnsCase{typ, op, n, 65, "nullable", false, ""})
			}
		}
	}
	return cases
}
func integerColumnsBenchmarkCases() []integerColumnsCase {
	var cases []integerColumnsCase
	for _, typ := range integerColumnsTypes {
		for _, n := range []int{65536, 1048576} {
			for _, pattern := range []string{"dense", "nullable", "selective", "chained"} {
				p := pattern
				if p == "chained" {
					p = "mixed"
				}
				cases = append(cases, integerColumnsCase{typ, kernel.CompareGT, n, 4096, p, pattern == "chained", ""})
			}
		}
	}
	return cases
}

func integerColumnsRead(tb testing.TB, b *memory.Batch, name string, t memory.DataType, i int) (uint64, bool) {
	tb.Helper()
	switch t {
	case memory.TypeInt8:
		x := b.Int8s[name]
		if x == nil {
			tb.Fatalf("missing native int8 column %s", name)
		}
		return uint64(int64(x.Values[i])), x.IsValid(i)
	case memory.TypeInt16:
		x := b.Int16s[name]
		if x == nil {
			tb.Fatalf("missing native int16 column %s", name)
		}
		return uint64(int64(x.Values[i])), x.IsValid(i)
	case memory.TypeInt64:
		x := b.Int64s[name]
		if x == nil {
			tb.Fatalf("missing native int64 column %s", name)
		}
		return uint64(int64(x.Values[i])), x.IsValid(i)
	case memory.TypeUint8:
		x := b.Uint8s[name]
		if x == nil {
			tb.Fatalf("missing native uint8 column %s", name)
		}
		return uint64(x.Values[i]), x.IsValid(i)
	case memory.TypeUint16:
		x := b.Uint16s[name]
		if x == nil {
			tb.Fatalf("missing native uint16 column %s", name)
		}
		return uint64(x.Values[i]), x.IsValid(i)
	case memory.TypeUint64:
		x := b.Uint64s[name]
		if x == nil {
			tb.Fatalf("missing native uint64 column %s", name)
		}
		return uint64(x.Values[i]), x.IsValid(i)
	}
	panic("invalid type")
}

func integerColumnsRunner(tb testing.TB, c integerColumnsCase, mode kernel.Mode) func(bool) {
	tb.Helper()
	input, want := integerColumnsFixture(tb, c)
	var err error
	if c.chained {
		input, err = NewFilterSource(input, expr.GT(expr.Col("gate"), expr.I64(0)))
		if err != nil {
			tb.Fatal(err)
		}
	}
	input, err = NewFilterSource(input, integerColumnsFilter(c))
	if err != nil {
		tb.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	project, err := NewProjectSource(input, []expr.NamedExpr{expr.Named(expr.Add(expr.Col("id"), expr.I64(7)), "rowid"), expr.Named(expr.Col("a"), "a"), expr.Named(expr.Col("b"), "b"), expr.Named(expr.Col("extra"), "extra")}, false, manager)
	if err != nil {
		tb.Fatal(err)
	}
	kernels := kernel.New(mode)
	cap, ok := kernels.ColumnComparisonCapability(c.typ, c.op)
	if !ok {
		tb.Fatal("missing column capability")
	}
	if mode == kernel.ModeScalar && cap.HardwareSIMD {
		tb.Fatal("scalar claims SIMD")
	}
	return func(check bool) {
		ctx := context.Background()
		s, err := project.NewBatchStream(ctx, source.ReadOptions{Kernels: kernels, BatchRows: c.batch, MemoryManager: manager})
		if err != nil {
			tb.Fatal(err)
		}
		defer func() {
			if err := s.Close(); err != nil {
				tb.Error(err)
			}
			if manager.Used() != 0 {
				tb.Errorf("retained bytes=%d", manager.Used())
			}
		}()
		out := memory.NewBatch()
		count := 0
		for {
			ok, err := s.Next(ctx, out)
			if err != nil {
				tb.Fatal(err)
			}
			if !ok {
				break
			}
			if check {
				for j := 0; j < out.Rows; j++ {
					if count+j >= len(want) {
						tb.Fatal("extra row")
					}
					w := want[count+j]
					a, va := integerColumnsRead(tb, out, "a", c.typ, j)
					z, vb := integerColumnsRead(tb, out, "b", c.typ, j)
					id, extra := out.Int64s["rowid"], out.Int64s["extra"]
					if id == nil || extra == nil {
						tb.Fatal("missing projected columns")
					}
					if va != w.aValid || vb != w.bValid || (va && a != w.a) || (vb && z != w.b) || !id.IsValid(j) || id.Values[j] != w.id || extra.IsValid(j) != w.valid || (w.valid && extra.Values[j] != w.extra) {
						tb.Fatalf("row=%d got a=%x b=%x id=%d extra=%d/%t want=%+v", count+j, a, z, id.Values[j], extra.Values[j], extra.IsValid(j), w)
					}
				}
			}
			count += out.Rows
		}
		if count != len(want) {
			tb.Fatalf("selected=%d want=%d", count, len(want))
		}
	}
}
func TestIntegerColumnsWholeQueryOracle(t *testing.T) {
	for _, c := range integerColumnsCases() {
		for _, mode := range integerColumnsModes {
			t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) { integerColumnsRunner(t, c, mode)(true) })
		}
	}
}
func BenchmarkIntegerColumnsWholeQuery(b *testing.B) {
	for _, c := range integerColumnsBenchmarkCases() {
		for _, mode := range integerColumnsModes {
			b.Run(c.name()+"/mode="+mode.String(), func(b *testing.B) {
				run := integerColumnsRunner(b, c, mode)
				run(true)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run(false)
				}
				b.StopTimer()
				cap, _ := kernel.New(mode).ColumnComparisonCapability(c.typ, c.op)
				if cap.HardwareSIMD {
					b.ReportMetric(1, "hardware-simd")
				}
				b.ReportMetric(float64(c.rows), "source-rows/op")
			})
		}
	}
}
