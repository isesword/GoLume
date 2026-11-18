package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"testing"
)

// varianceReviewOracle performs rational arithmetic over the exact input values;
// its sole rounding is big.Rat.Float64, independent of the fixed-word kernel.
func varianceReviewOracle(values []*big.Rat, special int, sample bool) (float64, bool) {
	n := len(values) + special
	if n == 0 || sample && n < 2 {
		return 0, false
	}
	if special > 0 {
		return math.NaN(), true
	}
	sum, squares := new(big.Rat), new(big.Rat)
	for _, x := range values {
		sum.Add(sum, x)
		squares.Add(squares, new(big.Rat).Mul(x, x))
	}
	numerator := new(big.Rat).Sub(new(big.Rat).Mul(big.NewRat(int64(n), 1), squares), new(big.Rat).Mul(sum, sum))
	denominator := new(big.Int).Mul(big.NewInt(int64(n)), big.NewInt(int64(n)))
	if sample {
		denominator.Mul(big.NewInt(int64(n)), big.NewInt(int64(n-1)))
	}
	numerator.Quo(numerator, new(big.Rat).SetInt(denominator))
	result, _ := numerator.Float64()
	return result, true
}
func varianceReviewFloatOracle(values []float64, valid []bool, sample bool) (float64, bool) {
	var rs []*big.Rat
	special := 0
	for i, x := range values {
		if !valid[i] {
			continue
		}
		if math.IsNaN(x) || math.IsInf(x, 0) {
			special++
			continue
		}
		rs = append(rs, new(big.Rat).SetFloat64(x))
	}
	return varianceReviewOracle(rs, special, sample)
}
func varianceReviewAssert(t *testing.T, got float64, valid bool, want float64, wantValid bool) {
	t.Helper()
	if valid != wantValid {
		t.Fatalf("valid=%v want %v", valid, wantValid)
	}
	if !valid {
		return
	}
	if math.IsNaN(want) {
		if !math.IsNaN(got) {
			t.Fatalf("got %g want NaN", got)
		}
		return
	}
	if math.Float64bits(got) != math.Float64bits(want) {
		t.Fatalf("got %.17g/%016x want %.17g/%016x", got, math.Float64bits(got), want, math.Float64bits(want))
	}
}

func TestVarianceReviewKernelExactExtremes(t *testing.T) {
	cases := [][]float64{
		{}, {1}, {math.MaxFloat64, math.MaxFloat64}, {math.MaxFloat64, -math.MaxFloat64},
		{1e16, math.Nextafter(1e16, math.Inf(1)), 1e16},
		{math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64},
		{math.Ldexp(1, -536), -math.Ldexp(1, -536), 0},
		{math.Ldexp(1, 500), math.Nextafter(math.Ldexp(1, 500), math.Inf(1))},
		{math.Copysign(0, -1), 0}, {math.Inf(1)}, {math.NaN(), 1}, {math.Inf(-1), math.Inf(1), 3},
	}
	for index, values := range cases {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			var state windowExactMoments
			valid := make([]bool, len(values))
			for i, x := range values {
				valid[i] = true
				if err := state.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: x}, 1); err != nil {
					t.Fatal(err)
				}
			}
			for removed := 0; removed <= len(values); removed++ {
				for _, sample := range []bool{false, true} {
					want, wv := varianceReviewFloatOracle(values[removed:], valid[removed:], sample)
					got, gv, err := state.result(sample)
					if err != nil {
						t.Fatal(err)
					}
					varianceReviewAssert(t, got, gv, want, wv)
				}
				if removed < len(values) {
					if err := state.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: values[removed]}, -1); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := state.addCell(memory.TypeFloat64, windowNumericCell{valid: false, f64: math.NaN()}, 1); err != nil {
				t.Fatal(err)
			}
			got, gv, err := state.result(false)
			if err != nil {
				t.Fatal(err)
			}
			varianceReviewAssert(t, got, gv, 0, false)
		})
	}
	for _, typ := range []memory.DataType{memory.TypeInt64, memory.TypeUint64} {
		t.Run(typ.String(), func(t *testing.T) {
			var state windowExactMoments
			var rs []*big.Rat
			cells := []windowNumericCell{{valid: true, i64: math.MaxInt64, u64: math.MaxUint64}, {valid: true, i64: math.MaxInt64 - 1, u64: math.MaxUint64 - 1}, {valid: true, i64: math.MaxInt64 - 2, u64: math.MaxUint64 - 2}}
			for _, c := range cells {
				if err := state.addCell(typ, c, 1); err != nil {
					t.Fatal(err)
				}
				v := new(big.Int).SetInt64(c.i64)
				if typ == memory.TypeUint64 {
					v.SetUint64(c.u64)
				}
				rs = append(rs, new(big.Rat).SetInt(v))
			}
			for _, sample := range []bool{false, true} {
				want, wv := varianceReviewOracle(rs, 0, sample)
				got, gv, err := state.result(sample)
				if err != nil {
					t.Fatal(err)
				}
				varianceReviewAssert(t, got, gv, want, wv)
			}
		})
	}
}

func varianceReviewInput(t *testing.T, order []int, values []float64, valid []bool) *percentRankReviewSource {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	var batches []*memory.Batch
	// Independent one-row inputs prevent poisoning future unpublished rows.
	for _, id := range order {
		b := memory.NewBatch()
		b.Reset(1)
		g := int64(0)
		if id >= len(values)/2 {
			g = 1
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
		b.Int64s["g"] = &memory.Int64Vector{Values: []int64{g}}
		bits := memory.Bitmap{0}
		if valid[id] {
			memory.Set(bits, 0)
		}
		b.Float64s["v"] = &memory.Float64Vector{Values: []float64{values[id]}, Validity: bits}
		batches = append(batches, b)
	}
	in, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	return &percentRankReviewSource{Source: in}
}

func TestVarianceReviewThreePathsRollingAndAliases(t *testing.T) {
	values := []float64{1e16, math.Nextafter(1e16, math.Inf(1)), math.NaN(), 3, 5, math.Inf(1), 7, 9, 11, 13, math.MaxFloat64, math.MaxFloat64, 0, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, 1, math.Inf(-1), 2, 4, 6}
	valid := make([]bool, len(values))
	for i := range valid {
		valid[i] = true
	}
	valid[3] = false
	valid[12] = false
	frames := []WindowFrame{
		{Start: WindowBound{Kind: WindowPreceding, Offset: 2}, End: WindowBound{Kind: WindowCurrentRow}},
		{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 1}},
		{Start: WindowBound{Kind: WindowFollowing, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 2}},
		{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}},
		{Start: WindowBound{Kind: WindowPreceding, Offset: 3}, End: WindowBound{Kind: WindowPreceding, Offset: 1}},
		{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}},
	}
	for fi, frame := range frames {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("frame%d/mode%d", fi, mode), func(t *testing.T) {
				order := make([]int, len(values))
				for i := range order {
					order[i] = i
				}
				if mode != WindowExecutionOrdered {
					for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
						order[i], order[j] = order[j], order[i]
					}
				}
				in := varianceReviewInput(t, order, values, valid)
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				configured := []ConfiguredWindowExpression{
					{Expr: WindowExpression{Function: WindowVarPop, Column: "v", Frame: frame, Alias: "v"}},
					{Expr: WindowExpression{Function: WindowVarSamp, Column: "v", Frame: frame, Alias: "sample"}},
					{Expr: WindowExpression{Function: WindowPercentRank, Alias: "pr"}},
					{Expr: WindowExpression{Function: WindowCumeDist, Alias: "cd"}},
					{Expr: WindowExpression{Function: WindowNTile, Alias: "tile"}, Buckets: 3},
				}
				fields := []memory.Field{{Name: "v", Type: memory.TypeFloat64, Nullable: true}, {Name: "sample", Type: memory.TypeFloat64, Nullable: true}, {Name: "pr", Type: memory.TypeFloat64}, {Name: "cd", Type: memory.TypeFloat64}, {Name: "tile", Type: memory.TypeInt64}}
				win, err := NewConfiguredWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "id"}}, configured, fields, q, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				seen := 0
				for {
					out := memory.NewBatch()
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					for row, id64 := range out.Int64s["id"].Values {
						id := int(id64)
						if seen >= len(order) || order[seen] != id {
							t.Fatal("original order", seen, id)
						}
						seen++
						base := 0
						if id >= 10 {
							base = 10
						}
						pos := id - base
						lo, hi := pos-2, pos
						switch fi {
						case 1:
							lo, hi = pos-1, pos+1
						case 2:
							lo, hi = pos+1, pos+2
						case 3:
							lo, hi = 0, pos
						case 4:
							lo, hi = pos-3, pos-1
						case 5:
							lo, hi = pos, pos
						}
						lo = max(0, min(10, lo))
						hi = max(-1, min(9, hi))
						var xs []float64
						var vs []bool
						if lo <= hi {
							xs = values[base+lo : base+hi+1]
							vs = valid[base+lo : base+hi+1]
						}
						for name, sample := range map[string]bool{"v": false, "sample": true} {
							want, wv := varianceReviewFloatOracle(xs, vs, sample)
							v := out.Float64s[name]
							if v == nil {
								t.Fatal("missing", name)
							}
							varianceReviewAssert(t, v.Values[row], v.IsValid(row), want, wv)
						}
						if out.Float64s["pr"].Values[row] != float64(pos)/9 || out.Float64s["cd"].Values[row] != float64(pos+1)/10 {
							t.Fatal("distribution", id)
						}
						wantTile := []int64{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}[pos]
						if out.Int64s["tile"].Values[row] != wantTile {
							t.Fatal("tile", id)
						}
					}
				}
				if seen != len(values) {
					t.Fatal("count", seen)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if in.opens != 1 || in.closes != 1 || q.Used() != 0 {
					t.Fatal("lifecycle", in.opens, in.closes, q.Used())
				}
			})
		}
	}
}

func TestVarianceReviewCancelBorrowedOutput(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			in := varianceReviewInput(t, []int{0, 1, 2, 3}, []float64{1, 2, 3, 4}, []bool{true, true, true, true})
			base, q := percentRankReviewQuery(t, 8<<20, true)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			frame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
			win, err := NewWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowVarPop, Column: "v", Frame: frame, Alias: "variance"}}, []memory.Field{{Name: "variance", Type: memory.TypeFloat64, Nullable: true}}, q, WindowSourceOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ok, err := st.Next(ctx, memory.NewBatch())
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			cancel()
			_, err = st.Next(ctx, memory.NewBatch())
			if !errors.Is(err, context.Canceled) {
				t.Fatal("cancel", err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || in.opens != 1 || in.closes != 1 {
				t.Fatal("close", q.Used(), in.opens, in.closes)
			}
		})
	}
}

func TestVarianceReviewFlatProfileThreePaths(t *testing.T) {
	for _, both := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprint(mode, "/both=", both), func(t *testing.T) {
				schema, expected := rankSpoolFlatProfile(t)
				oracle, e := memory.ArrowRecord(am.DefaultAllocator, schema, expected)
				if e != nil {
					t.Fatal(e)
				}
				defer oracle.Release()
				ids := []int{3, 0, 4, 1, 2}
				if mode == WindowExecutionOrdered {
					ids = []int{0, 1, 2, 3, 4}
				}
				var batches []*memory.Batch
				for _, id := range ids {
					b, e := memory.CloneBatchRange(expected, id, 1)
					if e != nil {
						t.Fatal(e)
					}
					batches = append(batches, b)
				}
				input, e := inmemory.New(schema, batches...)
				if e != nil {
					t.Fatal(e)
				}
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				expressions := []WindowExpression{{Function: WindowVarPop, Column: "id", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}, Alias: "cd"}}
				fields := []memory.Field{{Name: "cd", Type: memory.TypeFloat64, Nullable: true}}
				if both {
					expressions = append(expressions, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
					fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
				}
				win, e := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, expressions, fields, q, WindowSourceOptions{Mode: mode})
				if e != nil {
					t.Fatal(e)
				}
				st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				count := 0
				for {
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					rec, e := memory.ArrowRecord(am.DefaultAllocator, win.Schema(), out)
					if e != nil {
						t.Fatal(e)
					}
					for row, id := range out.Int64s["id"].Values {
						if int(id) != ids[count] {
							t.Fatal("row order", id, ids[count])
						}
						count++
						if both && (out.Float64s["pr"].Values[row] != float64(id)/4 || !out.Float64s["pr"].IsValid(row)) {
							t.Fatal("rank", id)
						}
						if out.Float64s["cd"].Values[row] != []float64{0, 0.25, 2.0 / 3, 1.25, 2}[id] || !out.Float64s["cd"].IsValid(row) {
							t.Fatal("cume", id)
						}
						for _, f := range fields {
							got, _ := win.Schema().Field(f.Name)
							if got.Type != f.Type || got.Nullable != f.Nullable {
								t.Fatal("distribution metadata", got)
							}
						}
						for col, f := range schema.Fields {
							if !arrow.TypeEqual(rec.Schema().Field(col).Type, oracle.Schema().Field(col).Type) {
								t.Fatal("type", f.Name)
							}
							gotField, wantField := rec.Schema().Field(col), oracle.Schema().Field(col)
							if gotField.Name != wantField.Name || gotField.Nullable != wantField.Nullable || !gotField.Metadata.Equal(wantField.Metadata) {
								t.Fatal("metadata", f.Name)
							}
							if rec.Column(col).IsNull(row) != oracle.Column(col).IsNull(int(id)) {
								t.Fatal("NULL", f.Name, id)
							}
							actual := array.NewSlice(rec.Column(col), int64(row), int64(row+1))
							want := array.NewSlice(oracle.Column(col), id, id+1)
							equal := array.Equal(actual, want)
							actual.Release()
							want.Release()
							if !equal {
								t.Fatal("raw value", f.Name, id)
							}
						}
					}
					rec.Release()
				}
				if count != 5 {
					t.Fatal(count)
				}
				if e = st.Close(); e != nil {
					t.Fatal(e)
				}
				if q.Used() != 0 {
					t.Fatal("leak", q.Used())
				}
			})
		}
	}
}

func TestVarianceReviewValidation(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, kind := range []string{"emptycolumn", "wrongtype", "nonnull", "encoding", "followingunbounded", "unboundedend", "reversed"} {
			t.Run(fmt.Sprintf("%d/%s", mode, kind), func(t *testing.T) {
				in := varianceReviewInput(t, []int{0, 1}, []float64{1, 2}, []bool{true, true})
				_, q := percentRankReviewQuery(t, 8<<20, true)
				e := WindowExpression{Function: WindowVarPop, Column: "v", Alias: "var", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
				f := memory.Field{Name: "var", Type: memory.TypeFloat64, Nullable: true}
				switch kind {
				case "emptycolumn":
					e.Column = ""
				case "wrongtype":
					f.Type = memory.TypeInt64
				case "nonnull":
					f.Nullable = false
				case "encoding":
					f.Encoding = memory.EncodingDictionary
				case "followingunbounded":
					e.Frame.End = WindowBound{Kind: WindowFollowing, Offset: 1}
				case "unboundedend":
					e.Frame.End = WindowBound{Kind: WindowUnboundedFollowing}
				case "reversed":
					e.Frame = WindowFrame{Start: WindowBound{Kind: WindowFollowing, Offset: 2}, End: WindowBound{Kind: WindowCurrentRow}}
				}
				if _, err := NewWindowSourceWithOptions(in, nil, []SortKey{{Column: "id"}}, []WindowExpression{e}, []memory.Field{f}, q, WindowSourceOptions{Mode: mode}); err == nil {
					t.Fatal("invalid admitted", kind)
				}
				if in.opens != 0 || q.Used() != 0 {
					t.Fatal("validation lifecycle", in.opens, q.Used())
				}
			})
		}
	}
}

func TestVarianceReviewEmptyAndSingletonThreePaths(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, kind := range []string{"empty", "finite", "infinity", "null"} {
			t.Run(fmt.Sprintf("%d/%s", mode, kind), func(t *testing.T) {
				rows := 1
				value := float64(7)
				valid := true
				switch kind {
				case "empty":
					rows = 0
				case "infinity":
					value = math.Inf(1)
				case "null":
					valid = false
				}
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
				batch := memory.NewBatch()
				batch.Reset(rows)
				batch.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
				values := make([]float64, rows)
				bitmap := make(memory.Bitmap, memory.WordsFor(rows))
				if rows > 0 {
					values[0] = value
					if valid {
						memory.Set(bitmap, 0)
					}
				}
				batch.Float64s["v"] = &memory.Float64Vector{Values: values, Validity: bitmap}
				in, err := inmemory.New(schema, batch)
				if err != nil {
					t.Fatal(err)
				}
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				frame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
				fields := []memory.Field{{Name: "pop", Type: memory.TypeFloat64, Nullable: true}, {Name: "sample", Type: memory.TypeFloat64, Nullable: true}}
				win, err := NewWindowSourceWithOptions(in, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowVarPop, Column: "v", Frame: frame, Alias: "pop"}, {Function: WindowVarSamp, Column: "v", Frame: frame, Alias: "sample"}}, fields, q, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				for _, field := range fields {
					actual, ok := win.Schema().Field(field.Name)
					if !ok || actual.Type != memory.TypeFloat64 || !actual.Nullable || !canonicalNTileField(actual) {
						t.Fatal("typed schema", actual)
					}
				}
				st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				count := 0
				for {
					out := memory.NewBatch()
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					for row := 0; row < out.Rows; row++ {
						count++
						pop, sample := out.Float64s["pop"], out.Float64s["sample"]
						if pop == nil || sample == nil {
							t.Fatal("missing typed output")
						}
						want := float64(0)
						if kind == "infinity" {
							want = math.NaN()
						}
						varianceReviewAssert(t, pop.Values[row], pop.IsValid(row), want, valid)
						varianceReviewAssert(t, sample.Values[row], sample.IsValid(row), 0, false)
					}
				}
				if count != rows {
					t.Fatal("cardinality", count, rows)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 {
					t.Fatal("retained", q.Used())
				}
			})
		}
	}
}

func TestVarianceReviewGenericEvaluatorRejectsUnadmittedState(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64})
	input := memory.NewBatch()
	input.Reset(1)
	input.Float64s["v"] = &memory.Float64Vector{Values: []float64{7}}
	for _, function := range []WindowFunction{WindowVarPop, WindowVarSamp} {
		output := memory.NewBatch()
		output.Reset(1)
		expression := WindowExpression{Function: function, Column: "v", Alias: "variance", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
		err := evaluateWindowExpression(context.Background(), input, schema, output, []int{0}, nil, expression)
		if err == nil || err.Error() != "variance requires admitted moment state" {
			t.Fatalf("generic evaluator must reject unadmitted moments: %v", err)
		}
		if output.Float64s["variance"] != nil {
			t.Fatal("rejected evaluator allocated output")
		}
		if input.Float64s["v"].Values[0] != 7 {
			t.Fatal("rejected evaluator changed input")
		}
	}
}
