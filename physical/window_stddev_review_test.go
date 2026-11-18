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

// Search the Float64 lattice using exact rational squares, then compare the
// exact midpoint. No rounded variance or finite-precision sqrt is the oracle.
func stddevReviewSqrt(x *big.Rat) float64 {
	if x.Sign() == 0 {
		return 0
	}
	const maxFinite uint64 = 0x7fefffffffffffff
	lo, hi := uint64(0), maxFinite
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		r := new(big.Rat).SetFloat64(math.Float64frombits(mid))
		square := new(big.Rat).Mul(r, r)
		if square.Cmp(x) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	lower := new(big.Rat).SetFloat64(math.Float64frombits(lo))
	var upper *big.Rat
	if lo == maxFinite {
		upper = new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 1024))
	} else {
		upper = new(big.Rat).SetFloat64(math.Float64frombits(lo + 1))
	}
	midpoint := new(big.Rat).Quo(new(big.Rat).Add(lower, upper), big.NewRat(2, 1))
	cmp := x.Cmp(new(big.Rat).Mul(midpoint, midpoint))
	if cmp > 0 || cmp == 0 && lo&1 != 0 {
		if lo == maxFinite {
			return math.Inf(1)
		}
		lo++
	}
	return math.Float64frombits(lo)
}
func stddevReviewFloatOracle(values []float64, valid []bool, sample bool) (float64, bool) {
	sum, squares := new(big.Rat), new(big.Rat)
	n := int64(0)
	special := false
	for i, v := range values {
		if !valid[i] {
			continue
		}
		n++
		if math.IsNaN(v) || math.IsInf(v, 0) {
			special = true
			continue
		}
		x := new(big.Rat).SetFloat64(v)
		sum.Add(sum, x)
		squares.Add(squares, new(big.Rat).Mul(x, x))
	}
	if n == 0 || sample && n < 2 {
		return 0, false
	}
	if special {
		return math.NaN(), true
	}
	numerator := new(big.Rat).Sub(new(big.Rat).Mul(big.NewRat(n, 1), squares), new(big.Rat).Mul(sum, sum))
	other := n
	if sample {
		other--
	}
	denominator := new(big.Int).Mul(big.NewInt(n), big.NewInt(other))
	numerator.Quo(numerator, new(big.Rat).SetInt(denominator))
	return stddevReviewSqrt(numerator), true
}

func TestStdDevReviewThreePathsRollingAndAliases(t *testing.T) {
	values := []float64{-1e200, 1e200, math.NaN(), 3, 5, math.Inf(1), 7, 9, 11, 13, -1e-200, 1e-200, 0, math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, 1, math.Inf(-1), 2, 4, 6}
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
					{Expr: WindowExpression{Function: WindowStdDevPop, Column: "v", Frame: frame, Alias: "v"}},
					{Expr: WindowExpression{Function: WindowStdDevSamp, Column: "v", Frame: frame, Alias: "sample"}},
					{Expr: WindowExpression{Function: WindowPercentRank, Alias: "pr"}},
					{Expr: WindowExpression{Function: WindowCumeDist, Alias: "cd"}},
					{Expr: WindowExpression{Function: WindowNTile, Alias: "tile"}, Buckets: 3},
					{Expr: WindowExpression{Function: WindowVarPop, Column: "v", Frame: frame, Alias: "variance"}},
				}
				fields := []memory.Field{{Name: "v", Type: memory.TypeFloat64, Nullable: true}, {Name: "sample", Type: memory.TypeFloat64, Nullable: true}, {Name: "pr", Type: memory.TypeFloat64}, {Name: "cd", Type: memory.TypeFloat64}, {Name: "tile", Type: memory.TypeInt64}, {Name: "variance", Type: memory.TypeFloat64, Nullable: true}}
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
							want, wv := stddevReviewFloatOracle(xs, vs, sample)
							v := out.Float64s[name]
							if v == nil {
								t.Fatal("missing", name)
							}
							varianceReviewAssert(t, v.Values[row], v.IsValid(row), want, wv)
						}
						wantVar, varValid := varianceReviewFloatOracle(xs, vs, false)
						vv := out.Float64s["variance"]
						varianceReviewAssert(t, vv.Values[row], vv.IsValid(row), wantVar, varValid)
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

func TestStdDevReviewCancelBorrowedOutput(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			in := varianceReviewInput(t, []int{0, 1, 2, 3}, []float64{1, 2, 3, 4}, []bool{true, true, true, true})
			base, q := percentRankReviewQuery(t, 8<<20, true)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			frame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
			win, err := NewWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowStdDevPop, Column: "v", Frame: frame, Alias: "variance"}}, []memory.Field{{Name: "variance", Type: memory.TypeFloat64, Nullable: true}}, q, WindowSourceOptions{Mode: mode})
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

func TestStdDevReviewFlatProfileThreePaths(t *testing.T) {
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
				expressions := []WindowExpression{{Function: WindowStdDevPop, Column: "id", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}, Alias: "cd"}}
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
						var prefix []float64
						var prefixValid []bool
						for j := int64(0); j <= id; j++ {
							prefix = append(prefix, float64(j))
							prefixValid = append(prefixValid, true)
						}
						expectedSD, _ := stddevReviewFloatOracle(prefix, prefixValid, false)
						if math.Float64bits(out.Float64s["cd"].Values[row]) != math.Float64bits(expectedSD) || !out.Float64s["cd"].IsValid(row) {
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

func TestStdDevReviewValidation(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, kind := range []string{"emptycolumn", "wrongtype", "nonnull", "encoding", "followingunbounded", "unboundedend", "reversed"} {
			t.Run(fmt.Sprintf("%d/%s", mode, kind), func(t *testing.T) {
				in := varianceReviewInput(t, []int{0, 1}, []float64{1, 2}, []bool{true, true})
				_, q := percentRankReviewQuery(t, 8<<20, true)
				e := WindowExpression{Function: WindowStdDevPop, Column: "v", Alias: "var", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
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

func TestStdDevReviewEmptyAndSingletonThreePaths(t *testing.T) {
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
				win, err := NewWindowSourceWithOptions(in, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowStdDevPop, Column: "v", Frame: frame, Alias: "pop"}, {Function: WindowStdDevSamp, Column: "v", Frame: frame, Alias: "sample"}}, fields, q, WindowSourceOptions{Mode: mode})
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

func TestStdDevReviewHandComputedMidpoints(t *testing.T) {
	minSub := math.SmallestNonzeroFloat64
	a := float64(1)
	b := math.Nextafter(a, math.Inf(1))
	c := math.Nextafter(b, math.Inf(1))
	cases := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"normal-even-down", []float64{-a, b}, a},
		{"normal-even-up", []float64{-b, c}, c},
		{"half-min-subnormal", []float64{0, minSub}, 0},
		{"one-and-half-min-subnormal", []float64{-minSub, 2 * minSub}, 2 * minSub},
		{"negative-odd-log", []float64{-minSub, 0, minSub}, minSub},
		{"negative-odd-log-underflow", []float64{0, 0, minSub}, 0},
		{"variance-overflow-root-finite", []float64{-1e200, 1e200}, 1e200},
		{"variance-underflow-root-nonzero", []float64{-1e-200, 1e-200}, 1e-200},
		{"max-root-finite", []float64{-math.MaxFloat64, math.MaxFloat64}, math.MaxFloat64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var moments windowExactMoments
			for _, x := range tc.values {
				if err := moments.addCell(memory.TypeFloat64, windowNumericCell{valid: true, f64: x}, 1); err != nil {
					t.Fatal(err)
				}
			}
			// Alternate APIs: variance's destructive division scratch must never affect sqrt.
			for iteration := 0; iteration < 3; iteration++ {
				if _, _, err := moments.result(false); err != nil {
					t.Fatal(err)
				}
				got, valid, err := moments.stddev(false)
				if err != nil {
					t.Fatal(err)
				}
				varianceReviewAssert(t, got, valid, tc.want, true)
				flags := make([]bool, len(tc.values))
				for i := range flags {
					flags[i] = true
				}
				want, wv := stddevReviewFloatOracle(tc.values, flags, true)
				got, valid, err = moments.stddev(true)
				if err != nil {
					t.Fatal(err)
				}
				varianceReviewAssert(t, got, valid, want, wv)
			}
		})
	}
}

func TestStdDevReviewGenericEvaluatorRejectsUnadmittedState(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64})
	input := memory.NewBatch()
	input.Reset(1)
	input.Float64s["v"] = &memory.Float64Vector{Values: []float64{7}}
	for _, function := range []WindowFunction{WindowStdDevPop, WindowStdDevSamp} {
		out := memory.NewBatch()
		out.Reset(1)
		err := evaluateWindowExpression(context.Background(), input, schema, out, []int{0}, nil, WindowExpression{Function: function, Column: "v", Alias: "sd", Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}})
		if err == nil {
			t.Fatal("generic path admitted uncharged moments")
		}
		if out.Float64s["sd"] != nil || input.Float64s["v"].Values[0] != 7 {
			t.Fatal("rejected path mutated data")
		}
	}
}

func TestStdDevReviewPagingActivationScope(t *testing.T) {
	for _, kind := range []string{"external-variance", "ordered-variance", "external-legacy", "external-variance-shift", "external-variance-rank", "external-variance-pr"} {
		t.Run(kind, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			_, src := variancePageReviewFixture(t, q, 31, true)
			mode := WindowExecutionExternalSort
			if kind == "ordered-variance" {
				mode = WindowExecutionOrdered
			}
			frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 1}}
			es := []WindowExpression{{Function: WindowStdDevPop, Column: "v", Alias: "variance", Frame: frame}}
			fields := []memory.Field{{Name: "variance", Type: memory.TypeFloat64, Nullable: true}}
			switch kind {
			case "external-legacy":
				es[0].Function = WindowAvg
			case "external-variance-shift":
				bound := WindowBound{Kind: WindowFollowing, Offset: 1}
				es = append(es, WindowExpression{Function: WindowShift, Column: "v", Alias: "lead", Frame: WindowFrame{Start: bound, End: bound}})
				fields = append(fields, memory.Field{Name: "lead", Type: memory.TypeInt64, Nullable: true})
			case "external-variance-rank":
				es = append(es, WindowExpression{Function: WindowRank, Alias: "rank"})
				fields = append(fields, memory.Field{Name: "rank", Type: memory.TypeInt64})
			case "external-variance-pr":
				es = append(es, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
				fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
			}
			planned, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, es, fields, q, WindowSourceOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			owner := planned.(*WindowSource)
			// Invoke the internal evaluator entry with the original mode, exactly as the
			// external pipeline does after front-sort construction. No source reads needed.
			st, err := newOrderedWindowStream(ctx, owner, src, owner.output, source.ReadOptions{BatchRows: 7, Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ordered := st.(*orderedWindowStream)
			future, ok := ordered.child.(*futureWindowStream)
			if !ok {
				t.Fatalf("expected lookahead, got %T", ordered.child)
			}
			want := kind == "external-variance" || kind == "external-variance-rank"
			if future.owner.pageVariance != want || (future.pageRows > 0) != want {
				t.Fatal("activation", kind, future.owner.pageVariance, future.pageRows, want)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 {
				t.Fatal("scope lifecycle", q.Used())
			}
		})
	}
}
