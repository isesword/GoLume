package physical

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestWindowCountResidentClippedFrames(t *testing.T) {
	input := memory.NewBatch()
	input.Rows = 5
	input.Int64s["id"] = &memory.Int64Vector{Values: []int64{4, 0, 2, 1, 3}}
	input.Uint32s["value"] = &memory.Uint32Vector{Values: []uint32{0, 0, 0, 1, 1}, Validity: memory.Bitmap{0b11011}}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeBool, Nullable: true})
	src, err := inmemory.New(schema, input)
	if err != nil {
		t.Fatal(err)
	}
	current := WindowBound{Kind: WindowCurrentRow}
	cases := []struct {
		name         string
		frame        WindowFrame
		values, rows []int64
	}{
		{"current", WindowFrame{current, current}, []int64{1, 1, 0, 1, 1}, []int64{1, 1, 1, 1, 1}},
		{"preceding", WindowFrame{WindowBound{Kind: WindowPreceding, Offset: 2}, WindowBound{Kind: WindowPreceding, Offset: 1}}, []int64{1, 0, 2, 1, 1}, []int64{2, 0, 2, 1, 2}},
		{"following", WindowFrame{WindowBound{Kind: WindowFollowing, Offset: 1}, WindowBound{Kind: WindowFollowing, Offset: 2}}, []int64{0, 1, 2, 1, 1}, []int64{0, 2, 2, 2, 1}},
		{"huge_before", WindowFrame{WindowBound{Kind: WindowPreceding, Offset: math.MaxInt64}, WindowBound{Kind: WindowPreceding, Offset: math.MaxInt64}}, make([]int64, 5), make([]int64, 5)},
		{"huge_after", WindowFrame{WindowBound{Kind: WindowFollowing, Offset: math.MaxInt64}, WindowBound{Kind: WindowFollowing, Offset: math.MaxInt64}}, make([]int64, 5), make([]int64, 5)},
		{"whole", WindowFrame{WindowBound{Kind: WindowUnboundedPreceding}, WindowBound{Kind: WindowUnboundedFollowing}}, []int64{4, 4, 4, 4, 4}, []int64{5, 5, 5, 5, 5}},
		{"zero", WindowFrame{WindowBound{Kind: WindowPreceding}, WindowBound{Kind: WindowFollowing}}, []int64{1, 1, 0, 1, 1}, []int64{1, 1, 1, 1, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr := []WindowExpression{{Function: WindowCountValues, Column: "value", Alias: "value", Frame: tc.frame}, {Function: WindowCountRows, Alias: "rows", Frame: tc.frame}}
			win, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, expr, []memory.Field{{Name: "value", Type: memory.TypeInt64}, {Name: "rows", Type: memory.TypeInt64}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := source.OpenBatchStream(context.Background(), win, source.ReadOptions{Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			out := memory.NewBatch()
			defer out.ClearColumns()
			ok, err := stream.Next(context.Background(), out)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			for name, want := range map[string][]int64{"value": tc.values, "rows": tc.rows} {
				v := out.Int64s[name]
				if v == nil || v.Validity != nil || !reflect.DeepEqual(v.Values, want) {
					t.Errorf("%s=%+v want %v", name, v, want)
				}
			}
		})
	}
}

func TestWindowCountInvalidFramesAllModes(t *testing.T) {
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, fn := range []WindowFunction{WindowCountRows, WindowCountValues} {
			for _, bound := range []WindowBound{{Kind: WindowPreceding, Offset: -1}, {Kind: WindowFollowing, Offset: math.MinInt64}, {Kind: WindowCurrentRow, Offset: 1}, {Kind: WindowUnboundedPreceding, Offset: 1}, {Kind: WindowUnboundedFollowing, Offset: 1}, {Kind: WindowBoundKind(255)}} {
				for _, start := range []bool{false, true} {
					t.Run(fmt.Sprintf("mode%d/fn%d/%d/%d/start%t", mode, fn, bound.Kind, bound.Offset, start), func(t *testing.T) {
						frame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
						if start {
							frame.Start = bound
						} else {
							frame.End = bound
						}
						column := ""
						if fn == WindowCountValues {
							column = "id"
						}
						_, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: fn, Column: column, Alias: "n", Frame: frame}}, []memory.Field{{Name: "n", Type: memory.TypeInt64}}, nil, WindowSourceOptions{Mode: mode})
						if err == nil {
							t.Fatal("accepted invalid frame")
						}
					})
				}
			}
		}
	}
}

func TestWindowCountRejectInvalidBindings(t *testing.T) {
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	frame := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	cases := []struct {
		name          string
		fn            WindowFunction
		column, alias string
		field         memory.Field
	}{
		{"missing input", WindowCountValues, "missing", "n", memory.Field{Name: "n", Type: memory.TypeInt64}},
		{"empty input", WindowCountValues, "", "n", memory.Field{Name: "n", Type: memory.TypeInt64}},
		{"rows with column", WindowCountRows, "id", "n", memory.Field{Name: "n", Type: memory.TypeInt64}},
		{"empty alias", WindowCountRows, "", "", memory.Field{Type: memory.TypeInt64}},
		{"dictionary output", WindowCountValues, "id", "n", memory.Field{Name: "n", Type: memory.TypeInt64, Encoding: memory.EncodingDictionary}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: tc.fn, Column: tc.column, Alias: tc.alias, Frame: frame}}, []memory.Field{tc.field}, nil)
			if err == nil {
				t.Fatal("accepted invalid count binding")
			}
		})
	}
}
