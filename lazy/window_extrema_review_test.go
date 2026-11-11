package lazy

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Exercise the public Series materialization boundary, not just schema metadata
// or a widened physical vector. Values are ordered high, low, middle twice.
func TestWindowExtremaReviewNumericTypes(t *testing.T) {
	cases := []struct {
		typ       memory.DataType
		values    any
		vectorMap string
	}{
		{memory.TypeInt8, []int8{127, -128, 0, 127, -128, 0}, "Int8s"},
		{memory.TypeInt16, []int16{32767, -32768, 0, 32767, -32768, 0}, "Int16s"},
		{memory.TypeInt32, []int32{math.MaxInt32, math.MinInt32, 0, math.MaxInt32, math.MinInt32, 0}, "Int32s"},
		{memory.TypeInt64, []int64{math.MaxInt64, math.MinInt64, 1, math.MaxInt64, math.MinInt64, 1}, "Int64s"},
		{memory.TypeUint8, []uint8{255, 0, 1, 255, 0, 1}, "Uint8s"},
		{memory.TypeUint16, []uint16{65535, 0, 1, 65535, 0, 1}, "Uint16s"},
		{memory.TypeUint32, []uint32{math.MaxUint32, 0, 1, math.MaxUint32, 0, 1}, "Uint32s"},
		{memory.TypeUint64, []uint64{math.MaxUint64, 0, 1<<63 + 1, math.MaxUint64, 0, 1<<63 + 1}, "Uint64s"},
		{memory.TypeFloat32, []float32{math.MaxFloat32, -math.MaxFloat32, 0.25, math.MaxFloat32, -math.MaxFloat32, 0.25}, "Float32s"},
		{memory.TypeFloat64, []float64{math.MaxFloat64, -math.MaxFloat64, 0.25, math.MaxFloat64, -math.MaxFloat64, 0.25}, "Float64s"},
	}
	for _, tc := range cases {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, shape := range []string{"values", "nulls", "empty"} {
				t.Run(fmt.Sprintf("%s/%s/%s", tc.typ, mode, shape), func(t *testing.T) {
					n := 6
					if shape == "empty" {
						n = 0
					}
					vals := reflect.ValueOf(tc.values)
					var batches []*memory.Batch
					for start := 0; start < n; start += 2 {
						b := memory.NewBatch()
						b.Reset(2)
						b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(start), int64(start + 1)}}
						b.Int32s["group"] = &memory.Int32Vector{Values: []int32{int32(start / 3), int32((start + 1) / 3)}}
						b.Strings["other"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Encoding: memory.EncodingView}
						m := reflect.ValueOf(b).Elem().FieldByName(tc.vectorMap)
						v := reflect.New(m.Type().Elem().Elem())
						v.Elem().FieldByName("Values").Set(vals.Slice(start, start+2))
						if shape == "nulls" {
							bits := memory.EnsureBitmap(nil, 2)
							for row := 0; row < 2; row++ {
								if start+row == 0 || start+row == 2 {
									memory.Set(bits, row)
								}
							}
							v.Elem().FieldByName("Validity").Set(reflect.ValueOf(bits))
						}
						m.SetMapIndex(reflect.ValueOf("x"), v)
						batches = append(batches, b)
					}
					schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "group", Type: memory.TypeInt32}, memory.Field{Name: "x", Type: tc.typ, Nullable: true}, memory.Field{Name: "other", Type: memory.TypeString, Encoding: memory.EncodingView})
					src, err := inmemory.New(schema, batches...)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "ordered" {
						src.WithOrdering(source.OrderingColumn{Name: "group"}, source.OrderingColumn{Name: "id", NullsLast: true})
					}
					session := NewSession(engine.DefaultOptions())
					if mode == "external" {
						session.Close()
						session = boundedJoinSession(t, 512<<10)
					}
					defer func() {
						session.Close()
						if used := session.Engine().Memory().Used(); used != 0 {
							t.Errorf("leaked %d bytes", used)
						}
					}()
					bounds := RowsBetween(Preceding(1), CurrentRow)
					query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("group"), OrderBy(Asc("id"))), RollingMin("x", bounds, "min"), RollingMax("x", bounds, "max"), RollingMin("x", bounds, "x"), RollingMax("x", bounds, "other"))
					plan, err := query.ExplainPhysical(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					strategy := map[string]string{"resident": "strategy=resident", "ordered": "strategy=ordered-state", "external": "strategy=external-sort-restore"}[mode]
					if !strings.Contains(plan, strategy) {
						t.Fatalf("missing %s: %s", strategy, plan)
					}
					result, err := query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer result.Close()
					for _, name := range []string{"min", "max", "x", "other"} {
						field, ok := result.Schema().Field(name)
						if !ok || field.Type != tc.typ || !field.Nullable {
							t.Fatalf("%s field=%+v", name, field)
						}
						col, err := result.Column(name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := col.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						if len(got) != n {
							t.Fatalf("%s len=%d want %d", name, len(got), n)
						}
						indexes := []int{0, 1, 1, 3, 4, 4}
						if name == "max" || name == "other" {
							indexes = []int{0, 0, 2, 3, 3, 5}
						}
						if shape == "nulls" {
							indexes = []int{0, 0, 2, -1, -1, -1}
						}
						for i, value := range got {
							var want any
							if indexes[i] >= 0 {
								want = vals.Index(indexes[i]).Interface()
							}
							if !reflect.DeepEqual(value, want) {
								t.Errorf("%s[%d]=%v (%T), want %v (%T)", name, i, value, value, want, want)
							}
						}
					}
				})
			}
		}
	}
}

func TestWindowExtremaReviewFloatEdges(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, unbounded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/unbounded-%t", typ, mode, unbounded), func(t *testing.T) {
					values := []float64{math.Copysign(0, -1), 0, math.NaN(), math.Inf(1), math.Inf(-1), 0}
					var batches []*memory.Batch
					for i, value := range values {
						b := memory.NewBatch()
						b.Reset(1)
						b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(i)}}
						if typ == memory.TypeFloat32 {
							b.Float32s["x"] = &memory.Float32Vector{Values: []float32{float32(value)}}
						} else {
							b.Float64s["x"] = &memory.Float64Vector{Values: []float64{value}}
						}
						batches = append(batches, b)
					}
					src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "x", Type: typ}), batches...)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "ordered" {
						src.WithOrdering(source.OrderingColumn{Name: "id", NullsLast: true})
					}
					session := NewSession(engine.DefaultOptions())
					if mode == "external" {
						session.Close()
						session = boundedJoinSession(t, 512<<10)
					}
					defer session.Close()
					bound := Preceding(1)
					if unbounded {
						bound = UnboundedPreceding
					}
					query := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), RollingMin("x", RowsBetween(bound, CurrentRow), "min"), RollingMax("x", RowsBetween(bound, CurrentRow), "max"))
					frame, err := query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer frame.Close()
					wantMin := []float64{values[0], values[0], 0, math.Inf(1), math.Inf(-1), math.Inf(-1)}
					wantMax := []float64{values[0], values[0], math.NaN(), math.NaN(), math.Inf(1), 0}
					if unbounded {
						wantMin = []float64{values[0], values[0], values[0], values[0], math.Inf(-1), math.Inf(-1)}
						wantMax = []float64{values[0], values[0], math.NaN(), math.NaN(), math.NaN(), math.NaN()}
					}
					for name, want := range map[string][]float64{"min": wantMin, "max": wantMax} {
						col, err := frame.Column(name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := col.ToAnySlice()
						if err != nil {
							t.Fatal(err)
						}
						if len(got) != len(want) {
							t.Fatalf("%s len=%d", name, len(got))
						}
						for i, v := range got {
							var f float64
							if typ == memory.TypeFloat32 {
								f = float64(v.(float32))
							} else {
								f = v.(float64)
							}
							if math.IsNaN(want[i]) {
								if !math.IsNaN(f) {
									t.Errorf("%s[%d]=%v want NaN", name, i, f)
								}
							} else if f != want[i] || f == 0 && math.Signbit(f) != math.Signbit(want[i]) {
								t.Errorf("%s[%d]=%v signbit=%t want %v signbit=%t", name, i, f, math.Signbit(f), want[i], math.Signbit(want[i]))
							}
						}
					}
				})
			}
		}
	}
}
