package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"testing"
)

// exactReviewOracle deliberately enumerates the frame and uses rational
// arithmetic, not the production accumulator, frame-range helper or evaluator.
func exactReviewOracle(values []*big.Rat, specials []float64, valid []bool, groups []int64, row, lo, hi int, avg bool) (float64, bool) {
	total := new(big.Rat)
	count, pos, neg, nan := int64(0), false, false, false
	for j := range values {
		if groups[j] != groups[row] || j-row < lo || j-row > hi || !valid[j] {
			continue
		}
		count++
		if values[j] != nil {
			total.Add(total, values[j])
			continue
		}
		v := specials[j]
		nan = nan || math.IsNaN(v)
		pos = pos || math.IsInf(v, 1)
		neg = neg || math.IsInf(v, -1)
	}
	if count == 0 {
		return 0, false
	}
	if nan || pos && neg {
		return math.NaN(), true
	}
	if pos {
		return math.Inf(1), true
	}
	if neg {
		return math.Inf(-1), true
	}
	if avg {
		total.Quo(total, new(big.Rat).SetInt64(count))
	}
	v, _ := total.Float64()
	return v, true
}

func TestWindowExactReviewOracle(t *testing.T) {
	cases := []struct {
		name   string
		floats []float64
		ints   []int64
		uints  []uint64
		f32    bool
	}{
		{name: "cancellation", floats: []float64{1e16, 1, -1e16, 2, 3, 4}},
		{name: "removal", floats: []float64{1e16, 1, 1, 1, 1, 2}},
		{name: "overflow", floats: []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1, 2, 3}},
		{name: "average_single_round", floats: []float64{1, math.Ldexp(1, -53), 0, 1, math.Ldexp(1, -52), 0}},
		{name: "subnormal_zero", floats: []float64{math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64, math.Copysign(0, -1), math.Copysign(0, -1), 0}},
		{name: "special_recovery", floats: []float64{math.NaN(), 2, 3, math.Inf(1), math.Inf(-1), 5, 6, 7, math.Inf(-1), 8, 9, 10}},
		{name: "float32_promotion", floats: []float64{math.MaxFloat32, 1, -math.MaxFloat32, math.SmallestNonzeroFloat32, 2, 3}, f32: true},
		{name: "int64_exact_average", ints: []int64{1<<53 | 1, -(1 << 53), math.MaxInt64, math.MinInt64, 1, 2}},
		{name: "uint64_exact_average", uints: []uint64{1<<53 | 1, 1<<53 | 2, math.MaxUint64, math.MaxUint64 - 2, 1, 2}},
	}
	frames := []struct {
		name    string
		lo, hi  int
		frame   WindowFrame
		bounded bool
	}{
		{"current", 0, 0, WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}, true},
		{"trailing2", -1, 0, WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowCurrentRow}}, true},
		{"trailing3", -2, 0, WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 2}, End: WindowBound{Kind: WindowCurrentRow}}, true},
		{"prefix", -10000, 0, WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}, true},
		{"whole", -10000, 10000, WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowUnboundedFollowing}}, false},
		{"suffix", 0, 10000, WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowUnboundedFollowing}}, false},
		{"preceding_only", -4, -2, WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 4}, End: WindowBound{Kind: WindowPreceding, Offset: 2}}, false},
		{"following_only", 2, 4, WindowFrame{Start: WindowBound{Kind: WindowFollowing, Offset: 2}, End: WindowBound{Kind: WindowFollowing, Offset: 4}}, false},
		{"centered", -2, 2, WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 2}, End: WindowBound{Kind: WindowFollowing, Offset: 2}}, false},
	}
	for _, tc := range cases {
		for _, fr := range frames {
			for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
				if mode != WindowExecutionResident && !fr.bounded {
					continue
				}
				for _, batchRows := range []int{1, 3, 17} {
					t.Run(fmt.Sprintf("%s/%s/mode%d/batch%d", tc.name, fr.name, mode, batchRows), func(t *testing.T) {
						ctx, _, mgr := spillOutputQuery(t)
						base := len(tc.floats)
						if tc.ints != nil {
							base = len(tc.ints)
						}
						if tc.uints != nil {
							base = len(tc.uints)
						}
						// Repeated data with independent partitions, an all-null partition and
						// a nullable copy ensure special state cannot leak across boundaries.
						n := 3 * base
						b := memory.NewBatch()
						b.Rows = n
						ids, groups := make([]int64, n), make([]int64, n)
						valid := make([]bool, n)
						bitmap := memory.EnsureBitmap(nil, n)
						rats := make([]*big.Rat, n)
						special := make([]float64, n)
						floats := make([]float64, n)
						f32 := make([]float32, n)
						ints := make([]int64, n)
						uints := make([]uint64, n)
						typ := memory.TypeFloat64
						if tc.f32 {
							typ = memory.TypeFloat32
						}
						if tc.ints != nil {
							typ = memory.TypeInt64
						}
						if tc.uints != nil {
							typ = memory.TypeUint64
						}
						for j := 0; j < n; j++ {
							ids[j] = int64(j)
							groups[j] = int64(j / base)
							k := j % base
							valid[j] = j/base == 0 || j/base == 2 && k%3 != 1
							if valid[j] {
								memory.Set(bitmap, j)
							}
							switch typ {
							case memory.TypeInt64:
								ints[j] = tc.ints[k]
								rats[j] = new(big.Rat).SetInt64(ints[j])
							case memory.TypeUint64:
								uints[j] = tc.uints[k]
								rats[j] = new(big.Rat).SetInt(new(big.Int).SetUint64(uints[j]))
							default:
								floats[j] = tc.floats[k]
								if tc.f32 {
									f32[j] = float32(floats[j])
									floats[j] = float64(f32[j])
								}
								special[j] = floats[j]
								rats[j] = new(big.Rat).SetFloat64(floats[j])
							}
						}
						b.Int64s["id"] = &memory.Int64Vector{Values: ids}
						b.Int64s["g"] = &memory.Int64Vector{Values: groups}
						switch typ {
						case memory.TypeInt64:
							b.Int64s["v"] = &memory.Int64Vector{Values: ints, Validity: bitmap}
						case memory.TypeUint64:
							b.Uint64s["v"] = &memory.Uint64Vector{Values: uints, Validity: bitmap}
						case memory.TypeFloat32:
							b.Float32s["v"] = &memory.Float32Vector{Values: f32, Validity: bitmap}
						default:
							b.Float64s["v"] = &memory.Float64Vector{Values: floats, Validity: bitmap}
						}
						schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: typ, Nullable: true})
						src, err := inmemory.New(schema, b)
						if err != nil {
							t.Fatal(err)
						}
						expr := []WindowExpression{{Function: WindowAvg, Column: "v", Alias: "avg", Frame: fr.frame}}
						fields := []memory.Field{{Name: "avg", Type: memory.TypeFloat64, Nullable: true}}
						if tc.ints == nil && tc.uints == nil {
							expr = append(expr, WindowExpression{Function: WindowSum, Column: "v", Alias: "sum", Frame: fr.frame})
							fields = append(fields, memory.Field{Name: "sum", Type: memory.TypeFloat64, Nullable: true})
						}
						// A future Shift forces lookahead around the unchanged trailing
						// aggregates. Keep other batch sizes as standalone-path checks.
						mixedFuture := mode != WindowExecutionResident && fr.name == "trailing2" && batchRows == 1 && typ == memory.TypeFloat64
						if mixedFuture {
							future := WindowBound{Kind: WindowFollowing, Offset: 2}
							expr = append(expr, WindowExpression{Function: WindowShift, Column: "v", Alias: "future", Frame: WindowFrame{Start: future, End: future}})
							fields = append(fields, memory.Field{Name: "future", Type: memory.TypeFloat64, Nullable: true})
						}
						win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, expr, fields, mgr, WindowSourceOptions{Mode: mode})
						if err != nil {
							t.Fatal(err)
						}
						stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batchRows})
						if err != nil {
							t.Fatal(err)
						}
						defer stream.Close()
						out := memory.NewBatch()
						seen := 0
						for {
							ok, err := stream.Next(ctx, out)
							if err != nil {
								t.Fatal(err)
							}
							if !ok {
								break
							}
							for j := 0; j < out.Rows; j++ {
								row := int(out.Int64s["id"].Values[j])
								seen++
								for _, f := range fields {
									want, present := exactReviewOracle(rats, special, valid, groups, row, fr.lo, fr.hi, f.Name == "avg")
									if f.Name == "future" {
										next := row + 2
										present = next < n && groups[next] == groups[row] && valid[next]
										if present {
											want = floats[next]
										}
									}

									got := out.Float64s[f.Name]
									if got == nil {
										t.Fatalf("missing %s", f.Name)
									}
									if got.IsValid(j) != present {
										t.Fatalf("row%d %s validity=%v want=%v", row, f.Name, got.IsValid(j), present)
									}
									if present && !(math.IsNaN(want) && math.IsNaN(got.Values[j])) && math.Float64bits(got.Values[j]) != math.Float64bits(want) {
										t.Errorf("row%d %s got=%g [%016x] want=%g [%016x]", row, f.Name, got.Values[j], math.Float64bits(got.Values[j]), want, math.Float64bits(want))
									}
								}
							}
						}
						if seen != n {
							t.Fatalf("rows=%d want=%d", seen, n)
						}
						checkSpillOutputCleanup(t, stream, mgr)
					})
				}
			}
		}
	}
}

// Exercise every finite exponent, cross-word carries and arbitrary reversible
// update histories against big.Rat rather than against another execution mode.
func TestWindowExactReviewRandomAccumulator(t *testing.T) {
	var acc windowExactSum
	total := new(big.Rat)
	var history []float64
	seed := uint64(0x41e867a9d2c5b30f)
	random := func() uint64 { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed }
	for step := 0; step < 12000; step++ {
		var v float64
		direction := int64(1)
		if len(history) > 0 && step%3 == 0 {
			k := int(random() % uint64(len(history)))
			v = history[k]
			history[k] = history[len(history)-1]
			history = history[:len(history)-1]
			direction = -1
		} else {
			bits := random()
			if bits>>52&0x7ff == 0x7ff {
				bits ^= uint64(1) << 52
			}
			v = math.Float64frombits(bits)
			history = append(history, v)
		}
		acc.addFloat(v, direction)
		r := new(big.Rat).SetFloat64(v)
		if direction < 0 {
			r.Neg(r)
		}
		total.Add(total, r)
		for _, divisor := range []int64{1, 3, int64(len(history)) + 1, math.MaxInt64} {
			want, _ := new(big.Rat).Quo(total, new(big.Rat).SetInt64(divisor)).Float64()
			got := acc.result(divisor)
			if math.Float64bits(got) != math.Float64bits(want) {
				t.Fatalf("step%d divisor%d got=%016x want=%016x", step, divisor, math.Float64bits(got), math.Float64bits(want))
			}
		}
	}
	for _, v := range history {
		acc.addFloat(v, -1)
	}
	if math.Float64bits(acc.result(1)) != 0 {
		t.Fatal("removal did not restore +0")
	}
}

func TestWindowExactReviewActualSpill(t *testing.T) {
	ctx, _, query := spillOutputQuery(t)
	const n = 2048
	b := memory.NewBatch()
	b.Rows = n
	ids := make([]int64, n)
	values := make([]float64, n)
	pattern := []float64{1e16, 1, 1, math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 2, 3, math.NaN(), 4, 5, math.Inf(1), math.Inf(-1), 6, 7, math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64}
	for j := range ids {
		ids[j] = int64(n - 1 - j)
		values[j] = pattern[int(ids[j])%len(pattern)]
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	b.Float64s["v"] = &memory.Float64Vector{Values: values}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64})
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 2}, End: WindowBound{Kind: WindowCurrentRow}}
	expr := []WindowExpression{{Function: WindowSum, Column: "v", Alias: "sum", Frame: frame}, {Function: WindowAvg, Column: "v", Alias: "avg", Frame: frame}}
	fields := []memory.Field{{Name: "sum", Type: memory.TypeFloat64, Nullable: true}, {Name: "avg", Type: memory.TypeFloat64, Nullable: true}}
	win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, expr, fields, query, WindowSourceOptions{Mode: WindowExecutionExternalSort})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 17})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := 0
	for {
		ok, err := stream.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for j := 0; j < out.Rows; j++ {
			id := int(out.Int64s["id"].Values[j])
			if id != n-1-seen {
				t.Fatal("external output order changed")
			}
			seen++
			lo := max(0, id-2)
			rats := make([]*big.Rat, id-lo+1)
			special := make([]float64, len(rats))
			valid := make([]bool, len(rats))
			groups := make([]int64, len(rats))
			for k := range rats {
				special[k] = pattern[(lo+k)%len(pattern)]
				rats[k] = new(big.Rat).SetFloat64(special[k])
				valid[k] = true
			}
			for _, name := range []string{"sum", "avg"} {
				want, _ := exactReviewOracle(rats, special, valid, groups, 0, 0, 2, name == "avg")
				got := out.Float64s[name]
				if !got.IsValid(j) || !(math.IsNaN(want) && math.IsNaN(got.Values[j])) && math.Float64bits(want) != math.Float64bits(got.Values[j]) {
					t.Fatalf("id%d %s got=%g want=%g", id, name, got.Values[j], want)
				}
			}
		}
	}
	if seen != n || query.Snapshot().SpilledBytes == 0 {
		t.Fatalf("rows=%d snapshot=%+v", seen, query.Snapshot())
	}
	t.Logf("actual spilled bytes=%d", query.Snapshot().SpilledBytes)
	checkSpillOutputCleanup(t, stream, query)
}

// Target the partial-division rounding boundary directly, including omitted
// tails exactly at, one unit below, and one unit above half an output ulp.
// Build the numerator through public numeric updates (52-bit float chunks),
// not by setting the optimized accumulator's internal representation.
func TestWindowExactReviewPartialDivisionMidpoints(t *testing.T) {
	for _, divisor := range []int64{1, 2, 3, 5, 17, 1<<32 - 1, 1 << 32, 1<<32 + 1, math.MaxInt64 - 1, math.MaxInt64} {
		for _, shift := range []uint{0, 1, 2, 11, 12, 51, 52, 53, 63, 64, 65, 127, 128, 129, 1022, 1074, 1900} {
			for _, odd := range []int64{0, 1} {
				q := new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 52), big.NewInt(odd))
				// N = (q + 1/2) * divisor * 2^shift. Use doubled numerator
				// first so odd divisors at shift zero also exercise the nearest integers.
				center := new(big.Int).Lsh(q, 1)
				center.Add(center, big.NewInt(1))
				center.Mul(center, big.NewInt(divisor))
				center.Lsh(center, shift)
				center.Rsh(center, 1)
				for _, delta := range []int64{-2, -1, 0, 1, 2} {
					n := new(big.Int).Add(center, big.NewInt(delta))
					for _, negative := range []bool{false, true} {
						var acc windowExactSum
						remaining := new(big.Int).Set(n)
						mask := uint64(1)<<52 - 1
						for bit := 0; remaining.Sign() != 0; bit += 52 {
							chunk := remaining.Uint64() & mask
							value := math.Ldexp(float64(chunk), bit-1074)
							if negative {
								value = -value
							}
							acc.addFloat(value, 1)
							remaining.Rsh(remaining, 52)
						}
						rational := new(big.Rat).SetFrac(n, new(big.Int).Lsh(big.NewInt(divisor), 1074))
						if negative {
							rational.Neg(rational)
						}
						want, _ := rational.Float64()
						got := acc.result(divisor)
						if math.Float64bits(got) != math.Float64bits(want) {
							t.Fatalf("divisor%d shift%d odd%d delta%d negative%v got=%016x want=%016x", divisor, shift, odd, delta, negative, math.Float64bits(got), math.Float64bits(want))
						}
					}
				}
			}
		}
	}
}

// Exhaust clipped/disjoint/single-row finite frames plus enormous offsets.
// Permuted physical row IDs ensure the sliding cursor follows partition order,
// not contiguous storage positions. Bounds are tested by row differences only.
func TestWindowExactReviewResidentClippedFrames(t *testing.T) {
	values := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1e16, 1, -1e16, math.NaN(), 2, 3, math.Inf(1), math.Inf(-1), 4, 5, math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64, math.Copysign(0, -1)}
	n := len(values)
	rows := make([]int, n)
	physical := make([]float64, n)
	valid := make([]bool, n)
	bitmap := memory.EnsureBitmap(nil, n)
	rats := make([]*big.Rat, n)
	groups := make([]int64, n)
	for i, v := range values {
		rows[i] = (i * 7) % n
		physical[rows[i]] = v
		valid[i] = i%7 != 4
		if valid[i] {
			memory.Set(bitmap, rows[i])
		}
		rats[i] = new(big.Rat).SetFloat64(v)
	}
	input := memory.NewBatch()
	input.Rows = n
	input.Float64s["v"] = &memory.Float64Vector{Values: physical, Validity: bitmap}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	offsets := []int64{-math.MaxInt64}
	for i := int64(-20); i <= 20; i++ {
		offsets = append(offsets, i)
	}
	offsets = append(offsets, math.MaxInt64)
	bound := func(v int64) WindowBound {
		if v < 0 {
			return WindowBound{Kind: WindowPreceding, Offset: -v}
		}
		if v > 0 {
			return WindowBound{Kind: WindowFollowing, Offset: v}
		}
		return WindowBound{Kind: WindowCurrentRow}
	}
	for _, lo := range offsets {
		for _, hi := range offsets {
			if hi < lo {
				continue
			}
			for _, avg := range []bool{false, true} {
				output := memory.NewBatch()
				output.Rows = n
				fn := WindowSum
				if avg {
					fn = WindowAvg
				}
				expression := WindowExpression{Function: fn, Column: "v", Alias: "out", Frame: WindowFrame{Start: bound(lo), End: bound(hi)}}
				if err := evaluateExactWindowFloat(context.Background(), input, schema, output, rows, expression, avg); err != nil {
					t.Fatal(err)
				}
				for i, row := range rows {
					// Clamp only for passing to an int oracle, avoiding architecture-dependent
					// conversions of extreme int64 offsets.
					oracleLo, oracleHi := int(max(-int64(n), min(int64(n), lo))), int(max(-int64(n), min(int64(n), hi)))
					want, present := exactReviewOracle(rats, values, valid, groups, i, oracleLo, oracleHi, avg)
					got := output.Float64s["out"]
					if got.IsValid(row) != present || present && !(math.IsNaN(want) && math.IsNaN(got.Values[row])) && math.Float64bits(want) != math.Float64bits(got.Values[row]) {
						t.Fatalf("frame[%d,%d] avg%v row%d got=%g/%v want=%g/%v", lo, hi, avg, i, got.Values[row], got.IsValid(row), want, present)
					}
				}
			}
		}
	}
}
