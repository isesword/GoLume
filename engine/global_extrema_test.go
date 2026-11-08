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

// All helpers use baseline APIs so identical fixtures can be overlaid on the
// pre-extrema tree for paired complete-query benchmarks.
func makeGlobalExtremaFixture(tb testing.TB, c globalBindingCase, typ memory.DataType, layout string) (globalBindingFixture, []TypedValue) {
	tb.Helper()
	_, seeds := nativeAggregateFixture(tb)
	var columns []nativeTestColumn
	for _, seed := range seeds[:10] {
		if seed.typ == memory.TypeInt8 || seed.typ == memory.TypeInt16 || seed.typ == memory.TypeUint8 || seed.typ == memory.TypeUint16 {
			continue
		}
		if typ == memory.TypeInvalid || typ == seed.typ {
			columns = append(columns, seed)
		}
	}
	f := globalBindingFixture{selected: make([]bool, c.rows)}
	fields := []memory.Field{{Name: "pred", Type: memory.TypeInt32}}
	f.query.Aggregates = []expr.Aggregate{expr.Count("rows")}
	want := []TypedValue{{Type: memory.TypeUint64, Valid: true}}
	// Pure Int64-only queries use the separate legacy Int64 engine. A count
	// on the Int32 predicate column selects the mixed native numeric engine.
	first := 1
	if typ == memory.TypeInt64 {
		f.query.Aggregates = append(f.query.Aggregates, expr.CountColumn("pred", "pred_count"))
		want = append(want, TypedValue{Type: memory.TypeUint64, Valid: true})
		first++
	}
	for _, seed := range columns {
		fields = append(fields, memory.Field{Name: seed.name, Type: seed.typ, Nullable: c.pattern != "dense"})
		if layout != "max" {
			f.query.Aggregates = append(f.query.Aggregates, expr.Min(seed.name, seed.name+"_min"))
			want = append(want, TypedValue{Type: seed.typ})
		}
		if layout != "min" {
			f.query.Aggregates = append(f.query.Aggregates, expr.Max(seed.name, seed.name+"_max"))
			want = append(want, TypedValue{Type: seed.typ})
		}
		if layout == "mixed" {
			f.query.Aggregates = append(f.query.Aggregates, expr.Sum(seed.name, seed.name+"_sum"), expr.CountColumn(seed.name, seed.name+"_count"))
			want = append(want, TypedValue{Type: seed.typ}, TypedValue{Type: memory.TypeUint64, Valid: true})
		}
	}
	if c.pattern == "selective" || c.pattern == "emptyselection" {
		f.query.Filter = expr.GT(expr.Col("pred"), expr.I64(0))
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		pred := make([]int32, n)
		for row := range pred {
			selected := c.pattern != "emptyselection" && (c.pattern != "selective" || (start+row)%5 == 0)
			if selected {
				pred[row] = 1
				want[0].Bits++
				if first == 2 {
					want[1].Bits++
				}
			}
			f.selected[start+row] = selected
		}
		b.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		at := first
		for lane, seed := range columns {
			col := nativeTestColumn{name: seed.name, typ: seed.typ, bits: make([]uint64, n)}
			if c.pattern != "dense" {
				col.valid = make(memory.Bitmap, memory.WordsFor(n))
			}
			for row := 0; row < n; row++ {
				ordinal := start + row
				raw := seed.bits[ordinal%len(seed.bits)]
				// Ordinary benchmark floats stress encounter-order SUM, both zeros and
				// cancellation; exact NaN payload cases are tested independently.
				if seed.typ == memory.TypeFloat32 {
					x := [...]float32{1 << 24, 1, -(1 << 24), .1, -.3, 7, 0, float32(math.Copysign(0, -1))}
					raw = uint64(math.Float32bits(x[ordinal%len(x)]))
				}
				if seed.typ == memory.TypeFloat64 {
					x := [...]float64{1e16, 1, -1e16, .1, -.3, 7, 0, math.Copysign(0, -1)}
					raw = math.Float64bits(x[ordinal%len(x)])
				}
				col.bits[row] = raw
				valid := c.pattern == "dense" || (c.pattern != "allnull" && (ordinal+lane*3)%11 != 0)
				if valid && col.valid != nil {
					memory.Set(col.valid, row)
				}
				if !valid || pred[row] == 0 {
					continue
				}
				index := at
				if layout != "max" {
					updateExtremaOracle(&want[index], raw, true)
					index++
				}
				if layout != "min" {
					updateExtremaOracle(&want[index], raw, false)
					index++
				}
				if layout == "mixed" {
					s := &want[index]
					s.Valid = true
					switch seed.typ {
					case memory.TypeInt32:
						s.Bits = uint64(int64(int32(s.Bits) + int32(raw)))
					case memory.TypeUint32:
						s.Bits = uint64(uint32(s.Bits) + uint32(raw))
					case memory.TypeFloat32:
						s.Bits = uint64(math.Float32bits(math.Float32frombits(uint32(s.Bits)) + math.Float32frombits(uint32(raw))))
					case memory.TypeFloat64:
						s.Bits = math.Float64bits(math.Float64frombits(s.Bits) + math.Float64frombits(raw))
					default:
						s.Bits += raw
					}
					want[index+1].Bits++
				}
			}
			if len(col.valid) > 0 {
				col.valid[len(col.valid)-1] |= ^memory.TailMask(n)
			}
			fillNativeTestColumn(b, col)
			if layout == "min" || layout == "max" {
				at++
			} else if layout == "mixed" {
				at += 4
			} else {
				at += 2
			}
		}
		batches = append(batches, b)
	}
	var err error
	f.source, err = inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	return f, want
}

// Independent row oracle does not call the aggregate or kernel implementation.
func updateExtremaOracle(out *TypedValue, raw uint64, minimum bool) {
	if !out.Valid {
		out.Bits, out.Valid = raw, true
		return
	}
	var less, greater bool
	switch out.Type {
	case memory.TypeInt32, memory.TypeInt64:
		less, greater = int64(raw) < int64(out.Bits), int64(raw) > int64(out.Bits)
	case memory.TypeUint32, memory.TypeUint64:
		less, greater = raw < out.Bits, raw > out.Bits
	case memory.TypeFloat32, memory.TypeFloat64:
		a, b := math.Float64frombits(out.Bits), math.Float64frombits(raw)
		if out.Type == memory.TypeFloat32 {
			a, b = float64(math.Float32frombits(uint32(out.Bits))), float64(math.Float32frombits(uint32(raw)))
		}
		if math.IsNaN(a) {
			out.Bits = raw
			return
		}
		if math.IsNaN(b) {
			return
		}
		less = b < a || (b == 0 && a == 0 && math.Signbit(b))
		greater = b > a || (b == 0 && a == 0 && !math.Signbit(b))
	}
	if (minimum && less) || (!minimum && greater) {
		out.Bits = raw
	}
}

func verifyGlobalExtremaExecution(tb testing.TB, c globalBindingCase, want []TypedValue, e *Engine, p *PreparedQuery) int64 {
	tb.Helper()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		tb.Fatal(err)
	}
	defer budget.Close()
	result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
	if err != nil {
		tb.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0].Aggregates) != len(want) {
		tb.Fatalf("unexpected result %+v", result)
	}
	for i, w := range want {
		if got := result.Rows[0].Aggregates[i].Value; got != w {
			tb.Fatalf("aggregate %d got=%+v want=%+v", i, got, w)
		}
	}
	if result.Summary.SelectedRows != int(want[0].Bits) || e.Memory().Used() != 0 || budget.Used() != 0 {
		tb.Fatalf("summary=%+v retained=%d query=%d", result.Summary, e.Memory().Used(), budget.Used())
	}
	return budget.Snapshot().PeakBytes
}

func TestGlobalExtremaWholeQuery(t *testing.T) {
	for _, rows := range []int{0, 1, 63, 64, 65, 129, 4097} {
		for _, pattern := range []string{"dense", "nullable", "selective", "allnull", "emptyselection"} {
			for _, layout := range []string{"min", "max", "minmax", "mixed"} {
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					c := globalBindingCase{6, rows, 127, pattern, "", mode}
					t.Run(layout+"/"+c.name(), func(t *testing.T) {
						f, want := makeGlobalExtremaFixture(t, c, memory.TypeInvalid, layout)
						e, p := prepareGlobalBinding(t, c, f)
						defer e.Close()
						defer p.Close()
						if !p.globalAggregateBindingsEligible() {
							t.Fatal("missed global extrema path")
						}
						peak := verifyGlobalExtremaExecution(t, c, want, e, p)
						if again := verifyGlobalExtremaExecution(t, c, want, e, p); again != peak {
							t.Fatalf("peak changed %d -> %d", peak, again)
						}
					})
				}
			}
		}
	}
}

func BenchmarkGlobalExtremaWholeQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeInvalid} {
		name := typ.String()
		if typ == memory.TypeInvalid {
			name = "mixed"
		} else if typ == memory.TypeInt64 {
			name = "int64-with-count"
		}
		for _, pattern := range []string{"dense", "nullable", "selective"} {
			cases := []globalBindingCase{{1, 32771, 32771, pattern, "", kernel.ModeAuto}}
			if typ == memory.TypeInvalid {
				cases[0].lanes = 6
				for _, size := range [][2]int{{63, 63}, {4097, 4097}, {32771, 1021}, {262147, 262147}} {
					cases = append(cases, globalBindingCase{6, size[0], size[1], pattern, "", kernel.ModeAuto})
				}
				cases = append(cases, globalBindingCase{6, 32771, 32771, pattern, "", kernel.ModeScalar})
			}
			for _, c := range cases {
				for _, layout := range []string{"minmax", "mixed"} {
					b.Run(fmt.Sprintf("%s/%s/%s", name, layout, c.name()), func(b *testing.B) {
						f, want := makeGlobalExtremaFixture(b, c, typ, layout)
						e, p := prepareGlobalBinding(b, c, f)
						defer e.Close()
						defer p.Close()
						peak := verifyGlobalExtremaExecution(b, c, want, e, p)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
							if err != nil {
								b.Fatal(err)
							}
							globalBindingResultSink = r
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
	}
}
