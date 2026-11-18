package lazy

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestFiniteSumAvgOrderedCrossing(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	b.Float64s["x"] = &memory.Float64Vector{Values: []float64{1, 2, 3}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeFloat64}), b)
	if err != nil {
		t.Fatal(err)
	}
	src.WithOrdering(source.OrderingColumn{Name: "id", NullsLast: true})
	s := finiteSumTestSession(t, 256<<10)
	defer s.Close()
	frame := RowsBetween(Preceding(1), Following(1))
	q := s.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RollingSum("x", frame, "s"), RollingAvg("x", frame, "a"))
	plan, err := q.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "strategy=ordered-state") {
		t.Fatal(plan)
	}
	df, err := q.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	averages := finiteSumFrameFloat64(t, df, "a")
	for i, want := range []float64{1.5, 2, 2.5} {
		if averages[i] != want {
			t.Fatal(averages)
		}
	}
	got := finiteSumFrameFloat64(t, df, "s")
	for i, want := range []float64{3, 6, 5} {
		if got[i] != want {
			t.Fatal(got)
		}
	}
}

// finiteSumOracle enumerates logical partition positions and accumulates exact
// rationals. It neither calls window internals nor rounds integer inputs to
// float64 before accumulation. NaN and infinity are tracked separately.
type finiteSumOverflow struct{}

func finiteSumOracle(values reflect.Value, valid []bool, id int, lo, hi int64, average bool) any {
	total := new(big.Rat)
	count := int64(0)
	nan, pos, neg := false, false, false
	for j := id / 7 * 7; j < (id/7+1)*7; j++ {
		delta := int64(j - id)
		if delta < lo || delta > hi || !valid[j] {
			continue
		}
		count++
		v := values.Index(j % 7)
		r := new(big.Rat)
		switch v.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			r.SetInt64(v.Int())
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			r.SetInt(new(big.Int).SetUint64(v.Uint()))
		default:
			f := v.Float()
			if math.IsNaN(f) {
				nan = true
				continue
			}
			if math.IsInf(f, 1) {
				pos = true
				continue
			}
			if math.IsInf(f, -1) {
				neg = true
				continue
			}
			r.SetFloat64(f)
		}
		total.Add(total, r)
	}
	if count == 0 {
		return nil
	}
	if nan || pos && neg {
		return math.NaN()
	}
	if pos {
		return math.Inf(1)
	}
	if neg {
		return math.Inf(-1)
	}
	if !average {
		switch values.Type().Elem().Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !total.Num().IsInt64() {
				return finiteSumOverflow{}
			}
			return total.Num().Int64()
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if !total.Num().IsUint64() {
				return finiteSumOverflow{}
			}
			return total.Num().Uint64()
		}
	}
	if average {
		total.Quo(total, new(big.Rat).SetInt64(count))
	}
	out, _ := total.Float64()
	return out
}

func TestWindowFiniteSumIndependentNumericOracle(t *testing.T) {
	cases := []struct {
		typ       memory.DataType
		values    any
		vectorMap string
	}{
		{memory.TypeInt8, []int8{127, -128, 0, 0, 42, -1, 5}, "Int8s"},
		{memory.TypeInt16, []int16{32767, -32768, 0, 0, 42, -1, 5}, "Int16s"},
		{memory.TypeInt32, []int32{math.MaxInt32, math.MinInt32, 0, 0, 42, -1, 5}, "Int32s"},
		{memory.TypeInt64, []int64{math.MaxInt64, math.MinInt64, 0, 0, 1<<53 + 1, -1, 5}, "Int64s"},
		{memory.TypeUint8, []uint8{255, 0, 1, 1, 42, 7, 5}, "Uint8s"},
		{memory.TypeUint16, []uint16{65535, 0, 1, 1, 42, 7, 5}, "Uint16s"},
		{memory.TypeUint32, []uint32{math.MaxUint32, 0, 1, 1, 42, 7, 5}, "Uint32s"},
		{memory.TypeUint64, []uint64{math.MaxUint64, 0, 1<<63 + 1, 1<<63 + 1, 1<<53 + 1, 7, 5}, "Uint64s"},
		{memory.TypeFloat32, []float32{float32(math.Copysign(0, -1)), 0, float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), 3, -2}, "Float32s"},
		{memory.TypeFloat64, []float64{math.Copysign(0, -1), 0, math.NaN(), math.Inf(1), math.Inf(-1), 3, -2}, "Float64s"},
		{memory.TypeFloat64, []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1e16, 1, -1e16, 1}, "Float64s"},
	}
	bounds := [][2]int64{{-3, -1}, {-1, 1}, {0, 0}, {1, 3}, {2, 2}}
	for ci, tc := range cases {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, shape := range []string{"valid", "nullable", "empty"} {
				t.Run(fmt.Sprintf("%d_%s/%s/%s", ci, tc.typ, mode, shape), func(t *testing.T) {
					order := []int{9, 0, 13, 5, 7, 2, 12, 1, 8, 6, 3, 11, 4, 10}
					if mode == "ordered" {
						order = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
					}
					if shape == "empty" {
						order = nil
					}
					valid := make([]bool, 14)
					for i := range valid {
						valid[i] = shape == "valid" || i < 7 && i != 1 && i != 5
					}
					vals := reflect.ValueOf(tc.values)
					var batches []*memory.Batch
					for start := 0; start < len(order); start += 3 {
						ids := order[start:min(start+3, len(order))]
						b := memory.NewBatch()
						b.Reset(len(ids))
						iv, priv := make([]int64, len(ids)), make([]int64, len(ids))
						groups := make([]int32, len(ids))
						typed := reflect.MakeSlice(vals.Type(), len(ids), len(ids))
						bits := memory.EnsureBitmap(nil, len(ids))
						for row, id := range ids {
							iv[row] = int64(id)
							priv[row] = int64(700 + id)
							groups[row] = int32(id / 7)
							typed.Index(row).Set(vals.Index(id % 7))
							if valid[id] {
								memory.Set(bits, row)
							}
						}
						b.Int64s["id"] = &memory.Int64Vector{Values: iv}
						b.Int32s["grp"] = &memory.Int32Vector{Values: groups}
						b.Int64s["__golume_window_future_0"] = &memory.Int64Vector{Values: priv}
						b.Int64s["__golume_window_ordinal"] = &memory.Int64Vector{Values: priv}
						m := reflect.ValueOf(b).Elem().FieldByName(tc.vectorMap)
						v := reflect.New(m.Type().Elem().Elem())
						v.Elem().FieldByName("Values").Set(typed)
						if shape != "valid" {
							v.Elem().FieldByName("Validity").Set(reflect.ValueOf(bits))
						}
						m.SetMapIndex(reflect.ValueOf("x"), v)
						batches = append(batches, b)
					}
					schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt32}, memory.Field{Name: "x", Type: tc.typ, Nullable: true}, memory.Field{Name: "__golume_window_future_0", Type: memory.TypeInt64}, memory.Field{Name: "__golume_window_ordinal", Type: memory.TypeInt64})
					src, err := inmemory.New(schema, batches...)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "ordered" {
						src.WithOrdering(source.OrderingColumn{Name: "grp"}, source.OrderingColumn{Name: "id", NullsLast: true})
					}
					session := NewSession(engine.DefaultOptions())
					if mode == "external" {
						session.Close()
						session = finiteSumTestSession(t, 512<<10)
					}
					defer func() {
						session.Close()
						if used := session.Engine().Memory().Used(); used != 0 {
							t.Errorf("leaked %d", used)
						}
					}()
					for _, pair := range bounds {
						lo, hi := pair[0], pair[1]
						t.Run(fmt.Sprintf("%d_%d", lo, hi), func(t *testing.T) {
							frame := RowsBetween(finiteSumRowsBound(lo), finiteSumRowsBound(hi))
							q := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))), RollingSum("x", frame, "sum"), RollingAvg("x", frame, "avg"), RollingSum("x", frame, "x"), RollingAvg("x", frame, "id"))
							plan, err := q.ExplainPhysical(context.Background())
							strategy := map[string]string{"resident": "resident", "ordered": "ordered-state", "external": "external-sort-restore"}[mode]
							if err != nil || !strings.Contains(plan, "strategy="+strategy) {
								t.Fatalf("strategy %s plan %s err %v", strategy, plan, err)
							}
							wantOverflow := false
							for _, id := range order {
								if _, overflow := finiteSumOracle(vals, valid, id, lo, hi, false).(finiteSumOverflow); overflow {
									wantOverflow = true
								}
							}
							result, err := q.Collect(context.Background())
							if wantOverflow {
								if err == nil {
									result.Close()
									t.Fatal("expected checked integer Sum overflow")
								}
								if !strings.Contains(err.Error(), "overflows") {
									t.Fatal(err)
								}
								if used := session.Engine().Memory().Used(); used != 0 {
									t.Fatalf("overflow leaked %d", used)
								}
								// AVG must not inherit SUM's output-width overflow:
								// compute it independently on the same large integers.
								avgQuery := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))), RollingAvg("x", frame, "avg"))
								avgResult, avgErr := avgQuery.Collect(context.Background())
								if avgErr != nil {
									t.Fatal(avgErr)
								}
								defer avgResult.Close()
								avgCol, avgErr := avgResult.Column("avg")
								if avgErr != nil {
									t.Fatal(avgErr)
								}
								avgValues, avgErr := avgCol.ToAnySlice()
								if avgErr != nil {
									t.Fatal(avgErr)
								}
								if len(avgValues) != len(order) {
									t.Fatal("average length")
								}
								for row, id := range order {
									want := finiteSumOracle(vals, valid, id, lo, hi, true)
									if !finiteSumRowsEqual(avgValues[row], want) {
										t.Fatalf("wide AVG row %d got %v want %v", id, avgValues[row], want)
									}
								}
								return
							}
							if err != nil {
								t.Fatal(err)
							}
							defer result.Close()
							for name, kind := range map[string]string{"sum": "sum", "avg": "avg", "x": "sum", "id": "avg", "__golume_window_future_0": "private", "__golume_window_ordinal": "private"} {
								col, err := result.Column(name)
								if err != nil {
									t.Fatal(err)
								}
								got, err := col.ToAnySlice()
								if err != nil {
									t.Fatal(err)
								}
								if len(got) != len(order) {
									t.Fatal("length")
								}
								for row, id := range order {
									var want any
									if kind == "private" {
										want = int64(700 + id)
									} else {
										want = finiteSumOracle(vals, valid, id, lo, hi, kind == "avg")
									}
									if !finiteSumRowsEqual(got[row], want) {
										t.Fatalf("%s row id=%d got %v (%T) want %v (%T)", name, id, got[row], got[row], want, want)
									}
								}
							}
						})
					}
				})
			}
		}
	}
}

// Equivalent zero-offset spellings must not leave exact-state followers pointing
// at a leader moved into the future-window stage.
func TestWindowFiniteSumZeroOffsetSharedLeader(t *testing.T) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, reverse := range []bool{false, true} {
			for _, precedingEnd := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/reverse=%v/precedingEnd=%v", mode, reverse, precedingEnd), func(t *testing.T) {
					order := []int{2, 0, 3, 1}
					if mode == "ordered" {
						order = []int{0, 1, 2, 3}
					}
					values := []float64{3, 999, -7, 0.5}
					var batches []*memory.Batch
					for _, id := range order {
						b := memory.NewBatch()
						b.Reset(1)
						bits := memory.EnsureBitmap(nil, 1)
						if id != 1 {
							memory.Set(bits, 0)
						}
						b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
						b.Float64s["x"] = &memory.Float64Vector{Values: []float64{values[id]}, Validity: bits}
						batches = append(batches, b)
					}
					src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: memory.TypeFloat64, Nullable: true}), batches...)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "ordered" {
						src.WithOrdering(source.OrderingColumn{Name: "id", NullsLast: true})
					}
					s := NewSession(engine.DefaultOptions())
					if mode == "external" {
						s.Close()
						s = finiteSumTestSession(t, 512<<10)
					}
					defer func() {
						s.Close()
						if n := s.Engine().Memory().Used(); n != 0 {
							t.Errorf("leaked %d", n)
						}
					}()
					first := RowsBetween(Following(0), Following(0))
					if precedingEnd {
						first = RowsBetween(Following(0), Preceding(0))
					}
					second := RowsBetween(CurrentRow, CurrentRow)
					if reverse {
						first, second = second, first
					}
					q := s.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RollingSum("x", first, "s1"), RollingAvg("x", first, "a1"), RollingSum("x", second, "s2"), RollingAvg("x", second, "a2"))
					plan, err := q.ExplainPhysical(context.Background())
					strategy := map[string]string{"resident": "resident", "ordered": "ordered-state", "external": "external-sort-restore"}[mode]
					if err != nil || !strings.Contains(plan, "strategy="+strategy) {
						t.Fatalf("plan=%s err=%v", plan, err)
					}
					df, err := q.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer df.Close()
					for _, name := range []string{"s1", "a1", "s2", "a2"} {
						col, err := df.Column(name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := col.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						if len(got) != len(order) {
							t.Fatalf("%s length %d", name, len(got))
						}
						for row, id := range order {
							var want any = values[id]
							if id == 1 {
								want = nil
							}
							if !finiteSumRowsEqual(got[row], want) {
								t.Fatalf("%s row %d got=%v want=%v", name, row, got[row], want)
							}
						}
					}
				})
			}
		}
	}
}

// finiteSumTestSession is copied from phase34_spill_join_test.go to keep this scoped test self-contained.
func finiteSumTestSession(t testing.TB, limit int64) *Session {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = limit
	opts.QueryMemorySoftLimitBytes = limit * 3 / 4
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	return NewSession(opts)
}

// finiteSumFrameFloat64 is copied from window_test.go to keep this scoped test self-contained.
func finiteSumFrameFloat64(t *testing.T, frame DataFrame, name string) []float64 {
	t.Helper()
	var out []float64
	for _, batch := range frame.batches() {
		vector, err := batch.Float64(name)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, vector.Values...)
	}
	return out
}

// finiteSumRowsBound is copied from bounded_rows_test.go to keep this scoped test self-contained.
func finiteSumRowsBound(offset int64) RowBound {
	if offset < 0 {
		return Preceding(-offset)
	}
	if offset > 0 {
		return Following(offset)
	}
	return CurrentRow
}

// finiteSumRowsEqual is copied from bounded_rows_test.go to keep this scoped test self-contained.
func finiteSumRowsEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	if x.Kind() == reflect.Float32 || x.Kind() == reflect.Float64 {
		f, g := x.Float(), y.Float()
		return math.IsNaN(f) && math.IsNaN(g) || f == g && (f != 0 || math.Signbit(f) == math.Signbit(g))
	}
	return reflect.DeepEqual(a, b)
}
