package physical

import (
	"context"
	"fmt"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// BenchmarkCompare32FilterProjectKernelMode is the whole-query admission gate
// for column-column Int32/Uint32 comparisons. Each iteration opens, drains and
// closes a filter -> computed projection pipeline. It includes reader setup,
// validity propagation, selection, projection and workspace cleanup, but not
// fixture construction or expression compilation. mode=simd explicitly tests
// candidates: mode=auto alone is not evidence while Auto remains scalar.
//
// Profiles deliberately avoid a full Cartesian matrix while covering sparse,
// medium and dense matches, both validity representations, and SIMD/bitmap
// tails (8199 source rows read in batches of 4096). pct is the predicate match
// rate BEFORE independently distributed nulls. peak_bytes measures managed
// execution workspace, not the immutable source fixture or total process RSS.
func BenchmarkCompare32FilterProjectKernelMode(b *testing.B) {
	const rows = 8199
	profiles := []struct {
		pct      int
		nullable bool
	}{{1, false}, {50, false}, {50, true}, {99, true}}
	for _, typ := range []string{"int32", "uint32"} {
		for _, op := range []string{"gt", "eq"} {
			for _, profile := range profiles {
				input, expected := compare32QueryFixture(b, rows, typ, op, profile.pct, profile.nullable)
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					modeName := mode.String()
					if mode == kernel.ModeSIMD {
						modeName = "simd"
					}
					name := fmt.Sprintf("type=%s/op=%s/pct=%d/nullable=%t/mode=%s", typ, op, profile.pct, profile.nullable, modeName)
					b.Run(name, func(b *testing.B) {
						predicate := expr.GT(expr.Col("left"), expr.Col("right"))
						if op == "eq" {
							predicate = expr.EQ(expr.Col("left"), expr.Col("right"))
						}
						filtered, err := NewFilterSource(input, predicate)
						if err != nil {
							b.Fatal(err)
						}
						manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
						project, err := NewProjectSource(filtered, []expr.NamedExpr{
							expr.Named(expr.Add(expr.Mul(expr.Col("payload"), expr.I64(3)), expr.I64(7)), "score"),
						}, false, manager)
						if err != nil {
							b.Fatal(err)
						}
						opts := source.ReadOptions{Kernels: kernel.New(mode), BatchRows: 4096, MemoryManager: manager}
						ctx, out := context.Background(), memory.NewBatch()
						drain := func(check bool) {
							stream, err := project.NewBatchStream(ctx, opts)
							if err != nil {
								b.Fatal(err)
							}
							count := 0
							for {
								ok, err := stream.Next(ctx, out)
								if err != nil {
									_ = stream.Close()
									b.Fatal(err)
								}
								if !ok {
									break
								}
								if check {
									score := out.Int64s["score"]
									if score == nil || len(score.Values) != out.Rows {
										_ = stream.Close()
										b.Fatal("invalid score vector")
									}
									for row, value := range score.Values {
										if count+row >= len(expected) || value != expected[count+row] || (len(score.Validity) != 0 && score.Validity[row>>6]&(uint64(1)<<uint(row&63)) == 0) {
											_ = stream.Close()
											b.Fatalf("wrong score at output row %d: %d", count+row, value)
										}
									}
								}
								count += out.Rows
							}
							if err := stream.Close(); err != nil {
								b.Fatal(err)
							}
							if check && count != len(expected) {
								b.Fatalf("got %d rows, want %d", count, len(expected))
							}
							if check && manager.Used() != 0 {
								b.Fatalf("retained %d managed bytes after close", manager.Used())
							}
						}
						// Independent, row-for-row oracle in every mode, outside the timer.
						drain(true)
						manager.peak = 0
						b.ReportAllocs()
						b.SetBytes(rows * (4 + 4 + 8))
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							drain(false)
						}
						b.StopTimer()
						if manager.Used() != 0 {
							b.Fatalf("retained %d managed bytes after close", manager.Used())
						}
						b.ReportMetric(float64(manager.peak), "peak_bytes")
						b.ReportMetric(rows, "source_rows/op")
						b.ReportMetric(float64(len(expected)), "projected_rows/op")
					})
				}
			}
		}
	}
}

func compare32QueryFixture(t testing.TB, rows int, typ, op string, pct int, nullable bool) (source.Source, []int64) {
	t.Helper()
	batch := memory.NewBatch()
	batch.Rows = rows
	left, right := make([]uint32, rows), make([]uint32, rows)
	var lv, rv memory.Bitmap
	if nullable {
		lv, rv = make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
	}
	payload := make([]int64, rows)
	expected := make([]int64, 0, rows)
	for row := 0; row < rows; row++ {
		payload[row] = int64(row)
		match := (row*37)%100 < pct
		// High-bit values catch accidental signed Uint32 comparison; Int32 also
		// alternates negative and positive values, without overflowing increments.
		base := uint32(0x80000000) + uint32(row%251)
		if row%2 == 0 {
			base = uint32(row % 251)
		}
		left[row], right[row] = base, base
		if op == "gt" {
			if match {
				left[row]++
			} else {
				right[row]++
			}
		} else if !match {
			right[row]++
		}
		// Every third pair straddles the sign boundary, exercising unsigned order.
		if op == "gt" && row%3 == 0 {
			left[row], right[row] = 0x80000000, 0x7fffffff
			if (typ == "uint32") != match {
				left[row], right[row] = right[row], left[row]
			}
		}
		leftValid, rightValid := !nullable || row%11 != 0, !nullable || row%13 != 0
		if nullable {
			if leftValid {
				lv[row>>6] |= uint64(1) << uint(row&63)
			}
			if rightValid {
				rv[row>>6] |= uint64(1) << uint(row&63)
			}
		}
		actual := left[row] == right[row]
		if op == "gt" {
			if typ == "int32" {
				actual = int32(left[row]) > int32(right[row])
			} else {
				actual = left[row] > right[row]
			}
		}
		if actual && leftValid && rightValid {
			expected = append(expected, payload[row]*3+7)
		}
	}
	dtype := memory.TypeUint32
	if typ == "int32" {
		dtype = memory.TypeInt32
		l, r := make([]int32, rows), make([]int32, rows)
		for i := range left {
			l[i], r[i] = int32(left[i]), int32(right[i])
		}
		batch.Int32s["left"], batch.Int32s["right"] = &memory.Int32Vector{Values: l, Validity: lv}, &memory.Int32Vector{Values: r, Validity: rv}
	} else {
		batch.Uint32s["left"], batch.Uint32s["right"] = &memory.Uint32Vector{Values: left, Validity: lv}, &memory.Uint32Vector{Values: right, Validity: rv}
	}
	batch.Int64s["payload"] = &memory.Int64Vector{Values: payload}
	input, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "left", Type: dtype, Nullable: nullable},
		memory.Field{Name: "right", Type: dtype, Nullable: nullable},
		memory.Field{Name: "payload", Type: memory.TypeInt64},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	return input, expected
}
