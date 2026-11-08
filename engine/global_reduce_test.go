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

// Global word reduction must agree with an independent row-ordered oracle,
// including widths that have different SUM output and wrapping rules.
func TestGlobalReductionWordBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 127, 129, 4097} {
		for _, pattern := range []string{"dense", "nullable", "sparse", "allnull"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
				t.Run(fmt.Sprintf("%d/%s/%s", n, pattern, mode), func(t *testing.T) {
					_, seeds := nativeAggregateFixture(t)
					seeds = seeds[:10]
					fields := []memory.Field{{Name: "pred", Type: memory.TypeInt32}}
					q := expr.Query{Aggregates: []expr.Aggregate{expr.Count("rows")}}
					if pattern == "sparse" {
						q.Filter = expr.GT(expr.Col("pred"), expr.I64(0))
					}
					want := []TypedValue{{Type: memory.TypeUint64, Valid: true}}
					for _, seed := range seeds {
						fields = append(fields, memory.Field{Name: seed.name, Type: seed.typ, Nullable: true})
						q.Aggregates = append(q.Aggregates, expr.Sum(seed.name, seed.name+"_sum"), expr.CountColumn(seed.name, seed.name+"_count"))
						output := seed.typ
						if output == memory.TypeInt8 || output == memory.TypeInt16 || output == memory.TypeUint8 || output == memory.TypeUint16 {
							output = memory.TypeInt64
						}
						want = append(want, TypedValue{Type: output}, TypedValue{Type: memory.TypeUint64, Valid: true})
					}
					var batches []*memory.Batch
					// Cross-word and deliberately unaligned source partitions both matter:
					// a floating subtotal merged after each word or batch is not equivalent.
					for start := 0; start < n; start += 127 {
						rows := min(127, n-start)
						b := memory.NewBatch()
						b.Rows = rows
						pred := make([]int32, rows)
						for row := range pred {
							if pattern != "sparse" || (start+row)%17 == 0 {
								pred[row] = 1
								want[0].Bits++
							}
						}
						b.Int32s["pred"] = &memory.Int32Vector{Values: pred}
						for lane, seed := range seeds {
							col := nativeTestColumn{name: seed.name, typ: seed.typ, bits: make([]uint64, rows)}
							if pattern != "dense" {
								col.valid = make(memory.Bitmap, memory.WordsFor(rows))
							}
							for row := 0; row < rows; row++ {
								ordinal := start + row
								value := seed.bits[ordinal%len(seed.bits)]
								if seed.typ == memory.TypeFloat32 {
									x := [...]float32{1 << 24, 1, -(1 << 24), .1, -.3, 7, float32(math.Copysign(0, -1))}
									value = uint64(math.Float32bits(x[ordinal%len(x)]))
								}
								if seed.typ == memory.TypeFloat64 {
									x := [...]float64{1e16, 1, -1e16, .1, -.3, 7, math.Copysign(0, -1)}
									value = math.Float64bits(x[ordinal%len(x)])
								}
								col.bits[row] = value
								valid := pattern == "dense" || (pattern != "allnull" && (ordinal+lane*3)%11 != 0)
								if col.valid != nil && valid {
									memory.Set(col.valid, row)
								}
								if !valid || pred[row] == 0 {
									continue
								}
								sum := &want[1+2*lane]
								sum.Valid = true
								want[2+2*lane].Bits++
								switch seed.typ {
								case memory.TypeInt8:
									sum.Bits += uint64(int64(int8(value)))
								case memory.TypeInt16:
									sum.Bits += uint64(int64(int16(value)))
								case memory.TypeInt32:
									sum.Bits = uint64(int64(int32(sum.Bits) + int32(value)))
								case memory.TypeUint8:
									sum.Bits += uint64(uint8(value))
								case memory.TypeUint16:
									sum.Bits += uint64(uint16(value))
								case memory.TypeUint32:
									sum.Bits = uint64(uint32(sum.Bits) + uint32(value))
								case memory.TypeFloat32:
									sum.Bits = uint64(math.Float32bits(math.Float32frombits(uint32(sum.Bits)) + math.Float32frombits(uint32(value))))
								case memory.TypeFloat64:
									sum.Bits = math.Float64bits(math.Float64frombits(sum.Bits) + math.Float64frombits(value))
								default:
									sum.Bits += value
								}
							}
							// Poison nonlogical validity bits; they must not become input rows.
							if len(col.valid) > 0 {
								col.valid[len(col.valid)-1] |= ^memory.TailMask(rows)
							}
							fillNativeTestColumn(b, col)
						}
						batches = append(batches, b)
					}
					src, err := inmemory.New(memory.NewSchema(fields...), batches...)
					if err != nil {
						t.Fatal(err)
					}
					opts := DefaultOptions()
					opts.KernelMode = mode
					opts.Workers = 1
					opts.Pipeline.Workers = 1
					e := New(opts)
					defer e.Close()
					p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					defer p.Close()
					if !p.globalAggregateBindingsEligible() {
						t.Fatal("missed global SUM/COUNT path")
					}
					for repeat := 0; repeat < 2; repeat++ {
						result, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
						if err != nil {
							t.Fatal(err)
						}
						if len(result.Rows) != 1 || len(result.Rows[0].Aggregates) != len(want) {
							t.Fatalf("unexpected result %+v", result)
						}
						for i, v := range want {
							if got := result.Rows[0].Aggregates[i].Value; got != v {
								t.Fatalf("aggregate %d got=%+v want=%+v", i, got, v)
							}
						}
						if result.Summary.SelectedRows != int(want[0].Bits) || e.Memory().Used() != 0 {
							t.Fatalf("summary=%+v retained=%d", result.Summary, e.Memory().Used())
						}
					}
				})
			}
		}
	}
}

// This baseline-compatible harness isolates all-count and integer-only queries
// in addition to the existing mixed numeric whole-query workload. It measures
// ExecuteTyped, including source, state and output setup/teardown, not Prepare.
func BenchmarkGlobalReductionWholeQuery(b *testing.B) {
	for _, family := range []string{"integers", "counts"} {
		for _, pattern := range []string{"dense", "nullable", "selective"} {
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto} {
				c := globalBindingCase{lanes: 4, rows: 32771, batchRows: 1021, pattern: pattern, mode: mode}
				b.Run(family+"/"+pattern+"/"+mode.String(), func(b *testing.B) {
					f := makeGlobalBindingFixture(b, c)
					out := f.query.Aggregates[:0]
					for _, a := range f.query.Aggregates {
						if family == "counts" && a.Kind == expr.AggSum {
							continue
						}
						if family == "integers" && (a.Column == "f" || a.Column == "g") {
							continue
						}
						out = append(out, a)
					}
					f.query.Aggregates = out
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
}
