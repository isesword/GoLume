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
// fixture construction or expression compilation. Scalar is the reference;
// Auto and SIMD exercise native kernels when the build and CPU support them.
//
// Profiles deliberately avoid a full Cartesian matrix while covering sparse,
// medium and dense matches, both validity representations, and SIMD/bitmap
// tails (8199 source rows read in batches of 4096). pct is the predicate match
// rate BEFORE independently distributed nulls. peak_bytes measures managed
// execution workspace, not the immutable source fixture or total process RSS.
func BenchmarkCompare32FilterProjectKernelMode(b *testing.B) {
	const rows = 8199
	for _, typ := range []string{"int32", "uint32"} {
		for _, op := range compare32QueryOps {
			for _, profile := range compare32QueryProfiles {
				input, expected := compare32QueryFixture(b, rows, typ, op, profile.pct, profile.nullable)
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					modeName := mode.String()
					if mode == kernel.ModeSIMD {
						modeName = "simd"
					}
					name := fmt.Sprintf("type=%s/op=%s/pct=%d/nullable=%t/mode=%s", typ, op, profile.pct, profile.nullable, modeName)
					b.Run(name, func(b *testing.B) {
						drain, manager := compare32QueryRunner(b, input, expected, op, mode)
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

var compare32QueryOps = []string{"gt", "ge", "lt", "le", "eq"}

var compare32QueryProfiles = []struct {
	pct      int
	nullable bool
}{{1, false}, {50, false}, {50, true}, {99, true}}

// Keep the benchmark's independent oracle in ordinary test runs as well.
func TestCompare32FilterProjectKernelMode(t *testing.T) {
	for _, typ := range []string{"int32", "uint32"} {
		for _, op := range compare32QueryOps {
			for _, profile := range compare32QueryProfiles {
				input, expected := compare32QueryFixture(t, 8199, typ, op, profile.pct, profile.nullable)
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
					name := fmt.Sprintf("type=%s/op=%s/pct=%d/nullable=%t/mode=%s", typ, op, profile.pct, profile.nullable, mode)
					t.Run(name, func(t *testing.T) {
						drain, _ := compare32QueryRunner(t, input, expected, op, mode)
						drain(true)
					})
				}
			}
		}
	}
}

// Validate selectivity independently of the operand-based oracle, and keep
// equality, both signed-boundary directions and the final source tail covered.
func TestCompare32QueryFixture(t *testing.T) {
	const rows = 8199
	for _, typ := range []string{"int32", "uint32"} {
		for _, op := range compare32QueryOps {
			for _, profile := range compare32QueryProfiles {
				name := fmt.Sprintf("type=%s/op=%s/pct=%d/nullable=%t", typ, op, profile.pct, profile.nullable)
				t.Run(name, func(t *testing.T) {
					input, expected := compare32QueryFixture(t, rows, typ, op, profile.pct, profile.nullable)
					count := 0
					for row := 0; row < rows; row++ {
						if (row*37)%100 >= profile.pct || (profile.nullable && (row%11 == 0 || row%13 == 0)) {
							continue
						}
						if count >= len(expected) || expected[count] != int64(row)*3+7 {
							t.Fatalf("fixture selectivity mismatch at source row %d", row)
						}
						count++
					}
					if count != len(expected) {
						t.Fatalf("fixture oracle has %d rows, want %d", len(expected), count)
					}

					ctx := context.Background()
					stream, err := input.NewBatchStream(ctx, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 4096})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := stream.Close(); err != nil {
							t.Error(err)
						}
					})
					out := memory.NewBatch()
					var relations [3]int // less, equal, greater (valid pairs only)
					var boundary [2]int  // sign-crossing pairs in each typed order
					batches, seen := 0, 0
					for {
						ok, err := stream.Next(ctx, out)
						if err != nil {
							t.Fatal(err)
						}
						if !ok {
							break
						}
						wantRows := 4096
						if batches == 2 {
							wantRows = 7
						}
						if batches >= 3 || out.Rows != wantRows {
							t.Fatalf("source batch %d has %d rows, want %d", batches, out.Rows, wantRows)
						}
						for row := 0; row < out.Rows; row++ {
							index := seen + row
							if profile.nullable && (index%11 == 0 || index%13 == 0) {
								continue
							}
							var l, r int64
							if typ == "int32" {
								l, r = int64(out.Int32s["left"].Values[row]), int64(out.Int32s["right"].Values[row])
							} else {
								l, r = int64(out.Uint32s["left"].Values[row]), int64(out.Uint32s["right"].Values[row])
							}
							if l < r {
								relations[0]++
							} else if l == r {
								relations[1]++
							} else {
								relations[2]++
							}
							if (uint32(l)^uint32(r))&0x80000000 != 0 {
								if l < r {
									boundary[0]++
								} else {
									boundary[1]++
								}
							}
						}
						seen += out.Rows
						batches++
					}
					if seen != rows || batches != 3 {
						t.Fatalf("source has %d rows in %d batches, want %d in 3", seen, batches, rows)
					}
					if relations[0] == 0 || relations[1] == 0 || relations[2] == 0 || boundary[0] == 0 || boundary[1] == 0 {
						t.Fatalf("missing equality/order/boundary coverage: relations=%v boundary=%v", relations, boundary)
					}
				})
			}
		}
	}
}

func compare32QueryPredicate(t testing.TB, op string) expr.Expr {
	t.Helper()
	left, right := expr.Col("left"), expr.Col("right")
	switch op {
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
	default:
		t.Fatalf("unsupported comparison %q", op)
		return nil
	}
}

func compare32QueryRunner(t testing.TB, input source.Source, expected []int64, op string, mode kernel.Mode) (func(bool), *selectionPeakManager) {
	t.Helper()
	filtered, err := NewFilterSource(input, compare32QueryPredicate(t, op))
	if err != nil {
		t.Fatal(err)
	}
	manager := &selectionPeakManager{base: memory.NewBudgetManager(0)}
	project, err := NewProjectSource(filtered, []expr.NamedExpr{
		expr.Named(expr.Add(expr.Mul(expr.Col("payload"), expr.I64(3)), expr.I64(7)), "score"),
	}, false, manager)
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
		count := 0
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				_ = stream.Close()
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if check {
				score := out.Int64s["score"]
				if score == nil || len(score.Values) != out.Rows {
					_ = stream.Close()
					t.Fatal("invalid score vector")
				}
				for row, value := range score.Values {
					if count+row >= len(expected) || value != expected[count+row] || (len(score.Validity) != 0 && score.Validity[row>>6]&(uint64(1)<<uint(row&63)) == 0) {
						_ = stream.Close()
						t.Fatalf("wrong score at output row %d: %d", count+row, value)
					}
				}
			}
			count += out.Rows
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if check && count != len(expected) {
			t.Fatalf("got %d rows, want %d", count, len(expected))
		}
		if check && manager.Used() != 0 {
			t.Fatalf("retained %d managed bytes after close", manager.Used())
		}
	}
	return drain, manager
}

func compare32QueryFixture(t testing.TB, rows int, typ, op string, pct int, nullable bool) (source.Source, []int64) {
	t.Helper()
	if typ != "int32" && typ != "uint32" {
		t.Fatalf("unsupported comparison type %q", typ)
	}
	if rows < 0 || pct < 0 || pct > 100 {
		t.Fatalf("invalid fixture size/selectivity: rows=%d pct=%d", rows, pct)
	}
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
		// Alternate equality/strict order independently of the 100-row match
		// pattern, so even 1% and 99% fixtures distinguish > from >= and <
		// from <=. relation is the intended typed ordering, not the oracle.
		relation := -1
		switch op {
		case "gt", "ge":
			if match {
				relation = 1
			}
			if row/100%2 == 0 && (op == "ge") == match {
				relation = 0
			}
		case "lt", "le":
			if !match {
				relation = 1
			}
			if row/100%2 == 0 && (op == "le") == match {
				relation = 0
			}
		case "eq":
			if match {
				relation = 0
			} else if row/100%2 == 0 {
				relation = 1
			}
		default:
			t.Fatalf("unsupported comparison %q", op)
		}
		left[row], right[row] = base, base
		if relation > 0 {
			left[row]++
		} else if relation < 0 {
			right[row]++
		}
		// Unequal pairs straddle the sign boundary in both directions for
		// every operator. Signed ordering reverses at this boundary.
		if relation != 0 && row%3 == 0 {
			left[row], right[row] = 0x80000000, 0x7fffffff
			if (typ == "uint32") != (relation > 0) {
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
		// Compute expected rows directly from the actual operands, without
		// using kernels or the match/relation decisions that built the data.
		l, r := int64(left[row]), int64(right[row])
		if typ == "int32" {
			l, r = int64(int32(left[row])), int64(int32(right[row]))
		}
		var actual bool
		switch op {
		case "gt":
			actual = l > r
		case "ge":
			actual = l >= r
		case "lt":
			actual = l < r
		case "le":
			actual = l <= r
		case "eq":
			actual = l == r
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
