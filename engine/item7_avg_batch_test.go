package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"reflect"
	"testing"
)

// Independent row-ordered Float64 AVG oracle, deliberately not an exact sum.
func item7AvgCheck(t testing.TB, c globalBindingCase, f globalBindingFixture, r TypedResult) {
	t.Helper()
	if len(r.Rows) != 1 || len(r.Rows[0].Aggregates) != len(f.query.Aggregates) {
		t.Fatal("typed result shape")
	}
	base := globalBindingOracle(c, f)[0]
	for j, a := range f.query.Aggregates {
		got := r.Rows[0].Aggregates[j]
		want := base[j]
		if (a.Kind == expr.AggMin || a.Kind == expr.AggMax) && a.Column == "u" {
			want = TypedValue{Type: memory.TypeUint64}
			col := f.columns[0]
			for i, x := range col.bits {
				if !f.selected[i] || !col.valid[i] {
					continue
				}
				if !want.Valid || (a.Kind == expr.AggMin && x < want.Bits) || (a.Kind == expr.AggMax && x > want.Bits) {
					want.Bits = x
				}
				want.Valid = true
			}
		}

		if a.Kind == expr.AggAvg {
			var sum float64
			var n uint64
			for _, col := range f.columns {
				if col.name != a.Column {
					continue
				}
				for i, bits := range col.bits {
					if f.selected[i] && col.valid[i] {
						sum += math.Float64frombits(bits)
						n++
					}
				}
			}
			want = TypedValue{Type: memory.TypeFloat64, Valid: n > 0}
			if n > 0 {
				want.Bits = math.Float64bits(sum / float64(n))
			}
		}
		if got.Alias != a.Alias || got.Value.Type != want.Type || got.Value.Valid != want.Valid {
			t.Fatalf("metadata=%+v want=%+v", got, want)
		}
		if want.Valid {
			if want.Type == memory.TypeFloat64 && math.IsNaN(math.Float64frombits(want.Bits)) {
				if !math.IsNaN(math.Float64frombits(got.Value.Bits)) {
					t.Fatal("NaN class")
				}
			} else if got.Value.Bits != want.Bits {
				t.Fatalf("bits=%x want=%x", got.Value.Bits, want.Bits)
			}
		}
	}
}
func TestItem7AvgEligibility(t *testing.T) {
	c := globalBindingCase{2, 129, 63, "selective", "", kernel.ModeScalar}
	f := makeGlobalBindingFixture(t, c)
	f.query.Aggregates = []expr.Aggregate{expr.Avg("f", "mean")}
	e, p := prepareGlobalBinding(t, c, f)
	defer e.Close()
	defer p.Close()
	got, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	item7AvgCheck(t, c, f, got)
	t.Log("existing Float64 AVG typed result matches independent encounter-order oracle")
	q, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	st, err := p.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), q), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	s := st.(*sortedAggregateStream)
	bound := len(s.globalBindings)
	st.Close()
	t.Logf("AVG bindings=%d afterCloseUsed=%d", bound, q.Used())
	if bound == 0 {
		t.Fatal("ELIGIBILITY RED: same Float64 AVG request has no global batch bindings")
	}
}

func item7AvgExecute(p *PreparedQuery, generic bool) (TypedResult, AggregateStreamStats, int64, error) {
	var result TypedResult
	var stats AggregateStreamStats
	q, e := p.e.BeginQueryMemory()
	if e != nil {
		return result, stats, 0, e
	}
	ctx := memory.WithQueryBudget(context.Background(), q)
	raw, e := p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
	if e != nil {
		q.Close()
		return result, stats, 0, e
	}
	s, ok := raw.(*sortedAggregateStream)
	if !ok {
		raw.Close()
		q.Close()
		return result, stats, 0, fmt.Errorf("not sorted stream")
	}
	if len(s.globalBindings) == 0 || s.globalBindingLease == nil {
		raw.Close()
		q.Close()
		return result, stats, 0, fmt.Errorf("not batch eligible")
	}
	if generic {
		s.globalBindings = nil
	}
	collector := typedResultCollector{query: p, result: &result, account: resultCollector{budget: q.Operator("CollectTypedResult", memory.MemoryRetainedState)}}
	stats, e = s.consumeRecords(ctx, func(rec aggregateRecord) error { return collector.append(ctx, rec) })
	ce := s.Close()
	collector.account.close()
	peak := q.Snapshot().PeakBytes
	if e == nil {
		e = ce
	}
	if e == nil && q.Used() != 0 {
		e = fmt.Errorf("retained charge %d", q.Used())
	}
	qe := q.Close()
	if e == nil {
		e = qe
	}
	result.Summary.GroupCount = stats.Groups
	result.Summary.SelectedRows = stats.SelectedRows
	result.Summary.Checksum = stats.Checksum
	return result, stats, peak, e
}

func item7AvgQuery(f *globalBindingFixture, shape string) {
	f.query.Aggregates = []expr.Aggregate{expr.Avg("f", "avg")}
	if shape == "sumavg" {
		f.query.Aggregates = append(f.query.Aggregates, expr.Sum("f", "sum"), expr.CountColumn("f", "count"))
	}
	if shape == "mixed" {
		f.query.Aggregates = append(f.query.Aggregates, expr.Avg("f", "avg_again"), expr.Sum("u", "u_sum"), expr.Count("rows"))
	}
}
func TestItem7AvgBatchEquivalence(t *testing.T) {
	for _, pattern := range []string{"dense", "nullable", "selective", "allnull", "emptyselection", "exceptional"} {
		for _, shape := range []string{"avg", "sumavg", "mixed"} {
			for _, n := range []int{0, 1, 65, 129} {
				c := globalBindingCase{2, n, 63, pattern, "", kernel.ModeScalar}
				t.Run(fmt.Sprintf("%s/%s/rows%d", pattern, shape, n), func(t *testing.T) {
					f := makeGlobalBindingFixture(t, c)
					item7AvgQuery(&f, shape)
					e, p := prepareGlobalBinding(t, c, f)
					defer e.Close()
					defer p.Close()
					var prev TypedResult
					var prevStats AggregateStreamStats
					for i, generic := range []bool{false, true} {
						r, stats, _, err := item7AvgExecute(p, generic)
						if err != nil {
							t.Fatal(err)
						}
						item7AvgCheck(t, c, f, r)
						if i == 0 {
							prev = r
							prevStats = stats
						} else if !reflect.DeepEqual(prev.Rows, r.Rows) || prevStats.Checksum != stats.Checksum || prevStats.SelectedRows != stats.SelectedRows || prevStats.Groups != stats.Groups {
							t.Fatalf("batch vs row bits/diagnostics differ: batch=%+v row=%+v batchstats=%+v rowstats=%+v", prev.Rows, r.Rows, prevStats, stats)
						}
					}
				})
			}
		}
	}
}

var item7AvgSink TypedResult

func BenchmarkItem7AvgControlled(b *testing.B) {
	for _, pattern := range []string{"dense", "nullable", "selective"} {
		for _, shape := range []string{"avg", "sumavg", "mixed"} {
			for _, generic := range []bool{false, true} {
				arm := "batch"
				if generic {
					arm = "row"
				}
				b.Run(pattern+"/"+shape+"/"+arm, func(b *testing.B) {
					c := globalBindingCase{2, 32771, 1021, pattern, "", kernel.ModeScalar}
					f := makeGlobalBindingFixture(b, c)
					item7AvgQuery(&f, shape)
					e, p := prepareGlobalBinding(b, c, f)
					defer e.Close()
					defer p.Close()
					r, _, peak, err := item7AvgExecute(p, generic)
					if err != nil {
						b.Fatal(err)
					}
					item7AvgCheck(b, c, f, r)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						r, _, _, err = item7AvgExecute(p, generic)
						if err != nil {
							b.Fatal(err)
						}
						item7AvgSink = r
					}
					b.StopTimer()
					b.ReportMetric(float64(peak), "warmup-peak-charged-B")
				})
			}
		}
	}
}

func TestItem7AvgTwoIndependentColumns(t *testing.T) {
	c := globalBindingCase{2, 129, 17, "selective", "", kernel.ModeScalar}
	f := globalBindingFixture{selected: make([]bool, c.rows), query: expr.Query{Filter: expr.GE(expr.Col("pred"), expr.I64(0)), Aggregates: []expr.Aggregate{expr.Avg("a", "a_avg"), expr.Avg("b", "b_avg"), expr.Sum("a", "a_sum"), expr.CountColumn("b", "b_count"), expr.Count("rows")}}}
	for lane, name := range []string{"a", "b"} {
		col := globalBindingColumn{name: name, typ: memory.TypeFloat64, bits: make([]uint64, c.rows), valid: make([]bool, c.rows)}
		for i := 0; i < c.rows; i++ {
			v := float64(i%13 - 6)
			if lane == 1 {
				v = float64(i%7)*0.25 - 2
			}
			col.bits[i] = math.Float64bits(v)
			col.valid[i] = (i+lane)%5 != 0
		}
		f.columns = append(f.columns, col)
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		pred := make([]int32, n)
		for j := 0; j < n; j++ {
			i := start + j
			pred[j] = -1
			if i%3 == 0 || i%11 == 0 {
				pred[j] = 0
				f.selected[i] = true
			}
		}
		b.Int32s["pred"] = &memory.Int32Vector{Values: pred}
		for _, col := range f.columns {
			v := &memory.Float64Vector{Values: make([]float64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
			for j := 0; j < n; j++ {
				v.Values[j] = math.Float64frombits(col.bits[start+j])
				if col.valid[start+j] {
					memory.Set(v.Validity, j)
				}
			}
			b.Float64s[col.name] = v
		}
		batches = append(batches, b)
	}
	var err error
	f.source, err = inmemory.New(memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt32}, memory.Field{Name: "a", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeFloat64, Nullable: true}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	e, p := prepareGlobalBinding(t, c, f)
	defer e.Close()
	defer p.Close()
	var prior TypedResult
	var ps AggregateStreamStats
	for arm, generic := range []bool{false, true} {
		r, stats, _, err := item7AvgExecute(p, generic)
		if err != nil {
			t.Fatal(err)
		}
		item7AvgCheck(t, c, f, r)
		if arm == 0 {
			prior = r
			ps = stats
		} else if !reflect.DeepEqual(prior.Rows, r.Rows) || ps.SelectedRows != stats.SelectedRows || ps.Checksum != stats.Checksum {
			t.Fatal("two independent AVG lanes differ")
		}
	}
}

func TestItem7AvgFirstStateDivergence(t *testing.T) {
	c := globalBindingCase{2, 65, 63, "exceptional", "", kernel.ModeScalar}
	f := makeGlobalBindingFixture(t, c)
	col := f.columns[1]
	for _, sumEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("sum%v", sumEnabled), func(t *testing.T) {
			layout := aggregate.Layout{Avg: true, Sum: sumEnabled, CountRows: true, CountValues: true}
			v, err := aggregate.NewNumericVector(1, memory.TypeFloat64, layout)
			if err != nil {
				t.Fatal(err)
			}
			var first bool
			for end := 1; end <= c.rows; end++ {
				v.AddBits(0, col.bits[end-1], col.valid[end-1])
				ref := v.Result(0)
				state := aggregate.NumericResult{Type: memory.TypeFloat64, Layout: layout}
				for start := 0; start < end; start += c.batchRows {
					n := min(c.batchRows, end-start)
					values := make([]float64, n)
					valid := make(memory.Bitmap, memory.WordsFor(n))
					sel := make(memory.Bitmap, memory.WordsFor(n))
					for j := 0; j < n; j++ {
						values[j] = math.Float64frombits(col.bits[start+j])
						memory.Set(sel, j)
						if col.valid[start+j] {
							memory.Set(valid, j)
						}
					}
					before := state
					if err := consumeGlobalFloat64AverageBatch(context.Background(), values, valid, sel, uint64(n), &state); err != nil {
						if err != errGlobalFloat64AverageFallback {
							t.Fatal(err)
						}
						if math.Float64bits(state.AvgSum) != math.Float64bits(before.AvgSum) || state.SumBits != before.SumBits || state.CountValues != before.CountValues || state.CountRows != before.CountRows {
							t.Fatal("fallback sentinel committed partial state")
						}
						replay, e := aggregate.NewNumericVector(1, memory.TypeFloat64, layout)
						if e != nil {
							t.Fatal(e)
						}
						if e = replay.InitResult(0, state); e != nil {
							t.Fatal(e)
						}
						for j := 0; j < n; j++ {
							replay.AddBits(0, math.Float64bits(values[j]), memory.IsSet(valid, j))
						}
						state = replay.Result(0)
					}
				}
				if math.Float64bits(state.AvgSum) != math.Float64bits(ref.AvgSum) || state.SumBits != ref.SumBits || state.CountValues != ref.CountValues || state.CountRows != ref.CountRows {
					if !first {
						t.Logf("first divergent prefixRows=%d operand=%016x valid=%v candidate AvgSum=%016x SumBits=%016x counts=%d/%d fallback AvgSum=%016x SumBits=%016x counts=%d/%d", end, col.bits[end-1], col.valid[end-1], math.Float64bits(state.AvgSum), state.SumBits, state.CountValues, state.CountRows, math.Float64bits(ref.AvgSum), ref.SumBits, ref.CountValues, ref.CountRows)
						first = true
					}
				}
			}
			if first {
				t.Fatal("IEEE state divergence; diagnostics independent")
			}
		})
	}
}

func item7AvgOverflowFixture(tb testing.TB, c globalBindingCase) globalBindingFixture {
	f := makeGlobalBindingFixture(tb, c)
	col := &f.columns[1]
	for i := range col.bits {
		col.bits[i] = math.Float64bits(math.MaxFloat64)
		col.valid[i] = true
	}
	var pages []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		b.Uint64s["u"] = &memory.Uint64Vector{Values: append([]uint64(nil), f.columns[0].bits[start:start+n]...)}
		v := make([]float64, n)
		for j := range v {
			v[j] = math.Float64frombits(col.bits[start+j])
		}
		b.Float64s["f"] = &memory.Float64Vector{Values: v}
		pages = append(pages, b)
	}
	var e error
	f.source, e = inmemory.New(memory.NewSchema(memory.Field{Name: "u", Type: memory.TypeUint64}, memory.Field{Name: "f", Type: memory.TypeFloat64}), pages...)
	if e != nil {
		tb.Fatal(e)
	}
	return f
}
func TestItem7AvgFallbackMixedLanes(t *testing.T) {
	for _, kind := range []string{"exceptional", "finiteoverflow"} {
		t.Run(kind, func(t *testing.T) {
			c := globalBindingCase{2, 129, 63, "exceptional", "", kernel.ModeScalar}
			f := makeGlobalBindingFixture(t, c)
			if kind == "finiteoverflow" {
				c.pattern = "dense"
				f = item7AvgOverflowFixture(t, c)
			}
			item7AvgQuery(&f, "mixed")
			f.query.Aggregates = append(f.query.Aggregates, expr.Min("u", "umin"), expr.Max("u", "umax"))
			e, p := prepareGlobalBinding(t, c, f)
			defer e.Close()
			defer p.Close()
			var prior TypedResult
			var ps AggregateStreamStats
			for arm, generic := range []bool{false, true} {
				r, stats, _, err := item7AvgExecute(p, generic)
				if err != nil {
					t.Fatal(err)
				}
				item7AvgCheck(t, c, f, r)
				if arm == 0 {
					prior = r
					ps = stats
				} else if !reflect.DeepEqual(prior.Rows, r.Rows) || ps.Checksum != stats.Checksum || ps.SelectedRows != stats.SelectedRows {
					t.Fatalf("mixed lane duplicate/skip: batch=%+v row=%+v", prior, r)
				}
			}
		})
	}
}
func BenchmarkItem7AvgNegativeControls(b *testing.B) {
	for _, kind := range []string{"tiny63", "batch1", "exceptional", "finiteoverflow"} {
		for _, generic := range []bool{false, true} {
			arm := "batch"
			if generic {
				arm = "row"
			}
			b.Run(kind+"/"+arm, func(b *testing.B) {
				c := globalBindingCase{2, 4097, 127, "dense", "", kernel.ModeScalar}
				if kind == "tiny63" {
					c.rows = 63
					c.batchRows = 63
				}
				if kind == "batch1" {
					c.batchRows = 1
				}
				if kind == "exceptional" {
					c.pattern = "exceptional"
				}
				f := makeGlobalBindingFixture(b, c)
				if kind == "finiteoverflow" {
					f = item7AvgOverflowFixture(b, c)
				}
				item7AvgQuery(&f, "sumavg")
				e, p := prepareGlobalBinding(b, c, f)
				defer e.Close()
				defer p.Close()
				r, _, peak, err := item7AvgExecute(p, generic)
				if err != nil {
					b.Fatal(err)
				}
				item7AvgCheck(b, c, f, r)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, _, _, err = item7AvgExecute(p, generic)
					if err != nil {
						b.Fatal(err)
					}
					item7AvgSink = r
				}
				b.StopTimer()
				b.ReportMetric(float64(peak), "warmup-peak-charged-B")
			})
		}
	}
}
