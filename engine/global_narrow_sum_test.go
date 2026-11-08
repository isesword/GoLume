package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"testing"
)

// This harness uses baseline APIs so its exact source can also measure the
// pre-SIMD implementation. The shared oracle independently widens each row.
func makeGlobalNarrowSumFixture(tb testing.TB, c globalBindingCase, typ memory.DataType) globalBindingFixture {
	tb.Helper()
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16}
	if typ != memory.TypeInvalid {
		types = []memory.DataType{typ}
	}
	f := globalBindingFixture{selected: make([]bool, c.rows)}
	fields := []memory.Field{{Name: "key", Type: memory.TypeInt32}, {Name: "pred", Type: memory.TypeInt32}}
	for lane, typ := range types {
		name := typ.String()
		col := globalBindingColumn{name, typ, make([]uint64, c.rows), make([]bool, c.rows)}
		for row := 0; row < c.rows; row++ {
			// Repeated extrema expose accidental byte/word wrapping. Negative
			// signed values and high unsigned values exercise both extensions.
			x := [...]int64{-32768, 32767, -128, 127, -1, 0, 1, 65535, 255, 256, -257}[row%11]
			switch typ {
			case memory.TypeInt8:
				col.bits[row] = uint64(int64(int8(x)))
			case memory.TypeInt16:
				col.bits[row] = uint64(int64(int16(x)))
			case memory.TypeUint8:
				col.bits[row] = uint64(uint8(x))
			case memory.TypeUint16:
				col.bits[row] = uint64(uint16(x))
			}
			col.valid[row] = c.pattern == "dense" || (c.pattern != "allnull" && (row+lane*3)%11 != 0)
		}
		f.columns = append(f.columns, col)
		fields = append(fields, memory.Field{Name: name, Type: typ, Nullable: c.pattern != "dense"})
		if c.control != "countonly" {
			f.query.Aggregates = append(f.query.Aggregates, expr.Sum(name, name+"_sum"))
		}
		f.query.Aggregates = append(f.query.Aggregates, expr.CountColumn(name, name+"_count"))
	}
	f.query.Aggregates = append(f.query.Aggregates, expr.Count("rows"))
	if c.control == "grouped" {
		f.query.GroupBy = []string{"key"}
		f.query.Hints = expr.Hints{Cardinality: 64, ForceStrategy: "bitmap-direct"}
	}
	if c.pattern == "selective" || c.pattern == "emptyselection" {
		f.query.Filter = expr.GE(expr.Col("pred"), expr.I64(0))
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		batch := memory.NewBatch()
		batch.Rows = n
		keys, pred := make([]int32, n), make([]int32, n)
		for row := 0; row < n; row++ {
			ordinal := start + row
			keys[row], pred[row] = int32(ordinal%64), int32(ordinal%5)-4
			if c.pattern == "emptyselection" {
				pred[row] = -1
			}
			f.selected[ordinal] = f.query.Filter == nil || pred[row] >= 0
		}
		batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
		batch.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		for _, col := range f.columns {
			var validity memory.Bitmap
			if c.pattern != "dense" {
				validity = make(memory.Bitmap, memory.WordsFor(n))
				for row := 0; row < n; row++ {
					if col.valid[start+row] {
						memory.Set(validity, row)
					}
				}
				validity[len(validity)-1] |= ^memory.TailMask(n)
			}
			fillNativeTestColumn(batch, nativeTestColumn{name: col.name, typ: col.typ, bits: col.bits[start : start+n], valid: validity})
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

func TestGlobalNarrowSumWholeQuery(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 129, 4097} {
		for _, pattern := range []string{"dense", "nullable", "selective", "allnull", "emptyselection"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				c := globalBindingCase{lanes: 4, rows: n, batchRows: 127, pattern: pattern, mode: mode}
				t.Run(c.name(), func(t *testing.T) {
					f := makeGlobalNarrowSumFixture(t, c, memory.TypeInvalid)
					e, p := prepareGlobalBinding(t, c, f)
					defer e.Close()
					defer p.Close()
					if !p.globalAggregateBindingsEligible() {
						t.Fatal("missed global SUM/COUNT path")
					}
					verifyGlobalBindingExecution(t, c, f, e, p)
					verifyGlobalBindingExecution(t, c, f, e, p)
				})
			}
		}
	}
}

func BenchmarkGlobalNarrowSumWholeQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeInvalid} {
		name := typ.String()
		if typ == memory.TypeInvalid {
			name = "mixed"
		}
		var cases []globalBindingCase
		for _, pattern := range []string{"dense", "nullable", "selective"} {
			cases = append(cases, globalBindingCase{4, 32771, 32771, pattern, "", kernel.ModeAuto})
			if typ == memory.TypeInvalid {
				for _, size := range [][2]int{{63, 63}, {4097, 4097}, {32771, 1021}, {262147, 262147}} {
					cases = append(cases, globalBindingCase{4, size[0], size[1], pattern, "", kernel.ModeAuto})
				}
				cases = append(cases, globalBindingCase{4, 32771, 32771, pattern, "", kernel.ModeScalar})
			}
		}
		if typ == memory.TypeInvalid {
			cases = append(cases, globalBindingCase{4, 32771, 1021, "nullable", "grouped", kernel.ModeAuto}, globalBindingCase{4, 32771, 32771, "nullable", "countonly", kernel.ModeAuto}, globalBindingCase{4, 32771, 1021, "allnull", "", kernel.ModeAuto})
		}
		for _, c := range cases {
			if typ != memory.TypeInvalid {
				c.lanes = 1
			}
			b.Run(fmt.Sprintf("%s/%s", name, c.name()), func(b *testing.B) {
				f := makeGlobalNarrowSumFixture(b, c, typ)
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
					b.Fatalf("retained %d bytes", e.Memory().Used())
				}
			})
		}
	}
}
