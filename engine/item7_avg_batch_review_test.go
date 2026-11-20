package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
)

// These tests exercise the real stream with recycled input headers. The row arm
// retains the identical admission reservation and changes only the dispatch.
func item7AvgReviewRun(t *testing.T, raw []uint64, chunk int, generic, nullable bool) []uint64 {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	s, q := globalBindingTestStream(t, schema, expr.Query{Aggregates: []expr.Aggregate{expr.Avg("v", "avg"), expr.Sum("v", "sum"), expr.CountColumn("v", "count"), expr.Count("rows")}})
	if len(s.globalBindings) == 0 || s.globalBindingLease == nil {
		t.Fatal("AVG did not select batch route")
	}
	if generic {
		s.globalBindings = nil
	}
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	pos := 0
	v := &memory.Float64Vector{}
	in := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
		for i := range v.Values {
			v.Values[i] = math.Float64frombits(0x7ff8000000000999)
		}
		b.ClearColumns()
		if pos == len(raw) {
			return false, nil
		}
		n := min(chunk, len(raw)-pos)
		v.Values = make([]float64, n)
		v.Validity = nil
		if nullable {
			v.Validity = make(memory.Bitmap, memory.WordsFor(n))
		}
		for i := 0; i < n; i++ {
			v.Values[i] = math.Float64frombits(raw[pos+i])
			if nullable && (pos+i)%3 != 1 {
				memory.Set(v.Validity, i)
			}
		}
		pos += n
		b.Rows = n
		b.Float64s["v"] = v
		return true, nil
	}}
	s.input = in
	out := memory.NewBatch()
	ok, err := s.Next(context.Background(), out)
	if err != nil || !ok {
		t.Fatalf("Next=%v %v", ok, err)
	}
	if out.Rows != 1 || len(out.Float64s) != 2 || len(out.Uint64s) != 2 {
		t.Fatalf("wrong typed output rows=%d", out.Rows)
	}
	ans := []uint64{}
	for _, name := range []string{"avg", "sum"} {
		vec := out.Float64s[name]
		if vec == nil {
			t.Fatal(name)
		}
		valid := uint64(0)
		if memory.IsSet(vec.Validity, 0) {
			valid = 1
		}
		ans = append(ans, math.Float64bits(vec.Values[0]), valid)
	}
	ans = append(ans, out.Uint64s["count"].Values[0], out.Uint64s["rows"].Values[0])
	if err = s.Close(); err != nil || q.Used() != 0 || in.closed != 1 {
		t.Fatalf("close=%v used=%d inputclose=%d", err, q.Used(), in.closed)
	}
	return ans
}

func TestItem7AvgReviewIEEERebind(t *testing.T) {
	bits := func(v ...float64) []uint64 {
		r := make([]uint64, len(v))
		for i, x := range v {
			r[i] = math.Float64bits(x)
		}
		return r
	}
	cases := [][]uint64{nil, bits(1e16, 1, -1e16, 0.1, -0.3, 7), bits(math.MaxFloat64, math.MaxFloat64), bits(math.Inf(1), math.Inf(-1)), {1 << 63, 0, 1 << 63}, {1, 1, 1, 1}, {0x7ff8000000000001, 0xfff8000000000042, 0x3ff0000000000000}, {0x7ff0000000000001, 0x4000000000000000}}
	for index, base := range cases {
		for _, nullable := range []bool{false, true} {
			for _, chunk := range []int{1, 63, 64, 65} {
				t.Run(fmt.Sprintf("case%d/null%v/chunk%d", index, nullable, chunk), func(t *testing.T) {
					raw := base
					if len(base) > 0 {
						raw = make([]uint64, 129)
						for i := range raw {
							raw[i] = base[i%len(base)]
						}
					}
					batch := item7AvgReviewRun(t, raw, chunk, false, nullable)
					row := item7AvgReviewRun(t, raw, chunk, true, nullable)
					if !reflect.DeepEqual(batch, row) {
						t.Fatalf("bitwise batch=%x row=%x", batch, row)
					}
					// Finite arithmetic oracle is independent ordinary Go, in source order.
					sum := float64(0)
					count := uint64(0)
					for i, b := range raw {
						if !nullable || i%3 != 1 {
							sum += math.Float64frombits(b)
							count++
						}
					}
					if batch[4] != count || batch[5] != uint64(len(raw)) {
						t.Fatal("count mismatch")
					}
					if count == 0 {
						if batch[1] != 0 || batch[3] != 0 {
							t.Fatal("empty validity")
						}
					} else if !math.IsNaN(sum) {
						if batch[0] != math.Float64bits(sum/float64(count)) || batch[2] != math.Float64bits(sum) {
							t.Fatal("independent order oracle")
						}
					}
				})
			}
		}
	}
}

func TestItem7AvgReviewEligibility(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64}, memory.Field{Name: "i", Type: memory.TypeInt64}, memory.Field{Name: "f", Type: memory.TypeFloat32})
	cases := []struct {
		name string
		aggs []expr.Aggregate
		want bool
	}{
		{"avg-only", []expr.Aggregate{expr.Avg("v", "a")}, true},
		{"mixed-existing", []expr.Aggregate{expr.Avg("v", "a"), expr.Sum("i", "s"), expr.Min("f", "min")}, true},
		{"same-lane-extrema", []expr.Aggregate{expr.Avg("v", "a"), expr.Min("v", "min")}, false},
		{"integer-second-avg", []expr.Aggregate{expr.Avg("v", "a"), expr.Avg("i", "b")}, false},
		{"float32-second-avg", []expr.Aggregate{expr.Avg("v", "a"), expr.Avg("f", "b")}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := globalBindingTestStream(t, schema, expr.Query{Aggregates: c.aggs})
			if got := len(s.globalBindings) > 0; got != c.want {
				t.Fatalf("eligible=%v want=%v", got, c.want)
			}
		})
	}
}

func TestItem7AvgReviewMalformedFalseFilter(t *testing.T) {
	for _, kind := range []string{"missing", "short-values", "short-mask"} {
		t.Run(kind, func(t *testing.T) {
			s, q := globalBindingTestStream(t, memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true}), expr.Query{Filter: expr.Bool(false), Aggregates: []expr.Aggregate{expr.Avg("v", "avg")}})
			if len(s.globalBindings) == 0 {
				t.Fatal("not batch")
			}
			s.input.Close()
			in := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
				b.ClearColumns()
				b.Rows = 65
				switch kind {
				case "short-values":
					b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, 64)}
				case "short-mask":
					b.Float64s["v"] = &memory.Float64Vector{Values: make([]float64, 65), Validity: memory.Bitmap{1}}
				}
				return true, nil
			}}
			s.input = in
			out := memory.NewBatch()
			ok, err := s.Next(context.Background(), out)
			if err == nil || ok || out.Rows != 0 {
				t.Fatalf("invalid input accepted %v %v rows%d", ok, err, out.Rows)
			}
			s.Close()
			if q.Used() != 0 || in.closed != 1 {
				t.Fatalf("used=%d closes=%d", q.Used(), in.closed)
			}
		})
	}
}
