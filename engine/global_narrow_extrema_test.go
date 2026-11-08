package engine

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

func TestGlobalNarrowExtremaWholeQuery(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 129, 4097} {
		for _, pattern := range []string{"dense", "nullable", "selective", "allnull", "emptyselection"} {
			for _, layout := range []string{"min", "max", "minmax", "mixed"} {
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					c := globalBindingCase{lanes: 10, rows: n, batchRows: 127, pattern: pattern, mode: mode}
					t.Run(layout+"/"+c.name(), func(t *testing.T) {
						f, want := makeGlobalExtremaFixtureWithNarrow(t, c, memory.TypeInvalid, layout, true)
						e, p := prepareGlobalBinding(t, c, f)
						defer e.Close()
						defer p.Close()
						if !p.globalAggregateBindingsEligible() {
							t.Fatal("narrow extrema disabled mixed native batch path")
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

// This benchmark and its fixture use pre-optimization APIs only. Copy this file
// and global_extrema_test.go to the baseline for identical source and timing.
// Sources and prepared queries are built outside timing. ExecuteTyped closes
// its stream; the explicit query budget is also closed inside each iteration.
func BenchmarkGlobalNarrowExtremaWholeQuery(b *testing.B) {
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64, memory.TypeInvalid} {
		for _, control := range []string{"native", "legacy"} {
			if control == "legacy" && typ != memory.TypeInt64 {
				continue
			}
			name := typ.String()
			if typ == memory.TypeInvalid {
				name = "mixed-all-widths"
			}
			for _, pattern := range []string{"dense", "nullable", "selective"} {
				sizes := [][2]int{{32771, 32771}}
				if typ == memory.TypeInvalid {
					sizes = append(sizes, [2]int{63, 63}, [2]int{4097, 4097}, [2]int{32771, 1021}, [2]int{262147, 262147})
				}
				for _, size := range sizes {
					for _, mode := range []kernel.Mode{kernel.ModeAuto, kernel.ModeScalar} {
						for _, layout := range []string{"minmax", "mixed"} {
							lanes := 1
							if typ == memory.TypeInvalid {
								lanes = 10
							}
							c := globalBindingCase{lanes: lanes, rows: size[0], batchRows: size[1], pattern: pattern, control: control, mode: mode}
							b.Run(fmt.Sprintf("%s/%s/%s", name, layout, c.name()), func(b *testing.B) {
								f, want := makeGlobalExtremaFixtureWithNarrow(b, c, typ, layout, true)
								if control == "legacy" {
									f.query.Aggregates = append(f.query.Aggregates[:1], f.query.Aggregates[2:]...)
									want = append(want[:1], want[2:]...)
								}
								e, p := prepareGlobalExtremaControl(b, c, f)
								defer e.Close()
								defer p.Close()
								peak := verifyGlobalExtremaExecution(b, c, want, e, p)
								width := int64(8)
								switch typ {
								case memory.TypeInt8, memory.TypeUint8:
									width = 1
								case memory.TypeInt16, memory.TypeUint16:
									width = 2
								case memory.TypeInt32, memory.TypeUint32:
									width = 4
								case memory.TypeInvalid:
									width = 42
								}
								b.SetBytes(int64(c.rows) * (width + 4))
								b.ReportAllocs()
								b.ResetTimer()
								for i := 0; i < b.N; i++ {
									budget, err := e.BeginQueryMemory()
									if err != nil {
										b.Fatal(err)
									}
									r, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
									budget.Close()
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
	}
}

// Pure Int64 queries intentionally retain the legacy aggregate engine.
func prepareGlobalExtremaControl(tb testing.TB, c globalBindingCase, f globalBindingFixture) (*Engine, *PreparedQuery) {
	tb.Helper()
	if c.control != "legacy" {
		return prepareGlobalBinding(tb, c, f)
	}
	opts := DefaultOptions()
	opts.Workers = 1
	opts.Pipeline.Workers = 1
	opts.PreparedCacheBytes = 0
	opts.KernelMode = c.mode
	opts.MemoryLimitBytes = 64 << 20
	opts.Pipeline.TargetMorselBytes = 4 << 20
	opts.Pipeline.MaxMorselBytes = 8 << 20
	opts.Pipeline.MaxMorselRows = 65536
	e := New(opts)
	p, err := e.Prepare(context.Background(), f.source, f.query, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	if p.nativeAggregate() {
		p.Close()
		e.Close()
		tb.Fatal("pure Int64 query unexpectedly uses native aggregate")
	}
	return e, p
}

func TestGlobalNarrowExtremaLegacyBoundary(t *testing.T) {
	for _, control := range []string{"native", "legacy"} {
		for _, pattern := range []string{"dense", "nullable", "selective", "allnull", "emptyselection"} {
			c := globalBindingCase{lanes: 1, rows: 129, batchRows: 63, pattern: pattern, control: control, mode: kernel.ModeAuto}
			t.Run(c.name(), func(t *testing.T) {
				f, want := makeGlobalExtremaFixtureWithNarrow(t, c, memory.TypeInt64, "mixed", true)
				if control == "legacy" {
					f.query.Aggregates = append(f.query.Aggregates[:1], f.query.Aggregates[2:]...)
					want = append(want[:1], want[2:]...)
				}
				e, p := prepareGlobalExtremaControl(t, c, f)
				defer e.Close()
				defer p.Close()
				if p.globalAggregateBindingsEligible() != (control == "native") {
					t.Fatal("native/legacy boundary changed")
				}
				verifyGlobalExtremaExecution(t, c, want, e, p)
			})
		}
	}
}
