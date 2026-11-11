package physical

import (
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
)

// Independent regression: false, empty containers and all-null children are
// values. Parent nullability alone determines the count, even across chunks.
func TestWindowCountReviewParentValidityAndReplacement(t *testing.T) {
	for _, kind := range []string{"bool", "list", "struct"} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("%s/%d", kind, mode), func(t *testing.T) {
				ctx, _, query := spillOutputQuery(t)
				b := memory.NewBatch()
				b.Rows = 6
				b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3, 4, 5}}
				b.Int64s["grp"] = &memory.Int64Vector{Values: []int64{0, 0, 0, 0, 1, 1}}
				var field memory.Field
				validity := memory.Bitmap{0b110101}
				switch kind {
				case "bool":
					field = memory.Field{Name: "v", Type: memory.TypeBool, Nullable: true}
					b.Uint32s["v"] = &memory.Uint32Vector{Values: []uint32{0, 1, 1, 0, 0, 1}, Validity: validity}
				case "list":
					field = memory.ListStringField("v", true, true)
					b.StringLists["v"] = &memory.StringListVector{Offsets: []uint32{0, 0, 1, 2, 2, 3, 3}, Validity: validity, Encoding: memory.EncodingView, Values: &memory.StringViewVector{Offsets: []uint32{0, 0, 0, 0}, Validity: memory.Bitmap{0}, Encoding: memory.EncodingView}}
				case "struct":
					field = memory.StructField("v", true, memory.Field{Name: "child", Type: memory.TypeInt64, Nullable: true})
					child := memory.NewBatch()
					child.Rows = 6
					child.Int64s["child"] = &memory.Int64Vector{Values: make([]int64, 6), Validity: memory.Bitmap{0}}
					b.Structs["v"] = &memory.StructVector{Rows: 6, Fields: field.Children, Values: child, Validity: validity, Encoding: memory.EncodingView}
				}
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt64}, field)
				src, err := inmemory.New(schema, b)
				if err != nil {
					t.Fatal(err)
				}
				frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowCurrentRow}}
				expr := []WindowExpression{{Function: WindowCountRows, Alias: "v", Frame: frame}, {Function: WindowCountValues, Column: "v", Alias: "n", Frame: frame}}
				fields := []memory.Field{{Name: "v", Type: memory.TypeInt64}, {Name: "n", Type: memory.TypeInt64}}
				win, err := NewWindowSourceWithOptions(src, []string{"grp"}, []SortKey{{Column: "id"}}, expr, fields, query, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				defer out.ClearColumns()
				var counts, rows []int64
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					for _, name := range []string{"v", "n"} {
						v := out.Int64s[name]
						if v == nil || v.Validity != nil {
							t.Fatalf("%s is not non-null int64: %+v", name, v)
						}
					}
					counts = append(counts, out.Int64s["n"].Values...)
					rows = append(rows, out.Int64s["v"].Values...)
					if _, ok := out.Uint32s["v"]; ok {
						t.Fatal("stale bool binding")
					}
					if _, ok := out.StringLists["v"]; ok {
						t.Fatal("stale list binding")
					}
					if _, ok := out.Structs["v"]; ok {
						t.Fatal("stale struct binding")
					}
				}
				if !reflect.DeepEqual(counts, []int64{1, 1, 1, 1, 1, 2}) {
					t.Fatalf("counts=%v", counts)
				}
				if !reflect.DeepEqual(rows, []int64{1, 2, 2, 2, 1, 2}) {
					t.Fatalf("rows=%v", rows)
				}
				checkSpillOutputCleanup(t, stream, query)
			})
		}
	}
}

func TestWindowCountReviewRejectInvalidContract(t *testing.T) {
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	good := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	badFrames := []WindowFrame{
		{Start: WindowBound{Kind: WindowPreceding, Offset: -1}, End: good.End},
		{Start: WindowBound{Kind: WindowCurrentRow, Offset: 1}, End: good.End},
		{Start: WindowBound{Kind: WindowBoundKind(99)}, End: good.End},
		{Start: WindowBound{Kind: WindowFollowing, Offset: 1}, End: good.End},
		{Start: good.Start, End: WindowBound{Kind: WindowUnboundedFollowing, Offset: 1}},
	}
	for _, fn := range []WindowFunction{WindowCountRows, WindowCountValues} {
		column := ""
		if fn == WindowCountValues {
			column = "id"
		}
		for _, frame := range badFrames {
			_, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: fn, Column: column, Alias: "n", Frame: frame}}, []memory.Field{{Name: "n", Type: memory.TypeInt64}}, nil)
			if err == nil {
				t.Errorf("function %v accepted frame %+v", fn, frame)
			}
		}
		for _, field := range []memory.Field{{Name: "n", Type: memory.TypeInt64, Nullable: true}, {Name: "n", Type: memory.TypeInt32}, {Name: "wrong", Type: memory.TypeInt64}} {
			_, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: fn, Column: column, Alias: "n", Frame: good}}, []memory.Field{field}, nil)
			if err == nil {
				t.Errorf("function %v accepted output %+v", fn, field)
			}
		}
	}
}

// Opaque compatibility is intentional: count reads only the parent bitmap,
// and must not inspect or decode an opaque payload to decide membership.
func TestWindowCountReviewOpaqueFallback(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeBool, memory.TypeList, memory.TypeStruct, memory.TypeArrow} {
		b := memory.NewBatch()
		b.Rows = 3
		b.Opaque["v"] = &memory.OpaqueVector{Rows: 3, Type: typ, Validity: memory.Bitmap{5}}
		schema := memory.NewSchema(memory.Field{Name: "v", Type: typ, Nullable: true})
		for row, want := range []bool{true, false, true} {
			got, err := windowValueValid(b, schema, "v", row)
			if err != nil || got != want {
				t.Fatalf("%s row %d: %v, %v", typ, row, got, err)
			}
		}
		b.Opaque["v"].Validity = nil
		for row := 0; row < 3; row++ {
			got, err := windowValueValid(b, schema, "v", row)
			if err != nil || !got {
				t.Fatalf("%s nil validity row %d: %v, %v", typ, row, got, err)
			}
		}
		delete(b.Opaque, "v")
		if _, err := windowValueValid(b, schema, "v", 0); err == nil {
			t.Fatalf("%s missing binding accepted", typ)
		}
	}
}

func TestWindowCountReviewEmptyAndExtremeFrames(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 3
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	b.Int64s["v"] = &memory.Int64Vector{Values: []int64{0, 0, 0}, Validity: memory.Bitmap{0}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true}), b)
	if err != nil {
		t.Fatal(err)
	}
	for _, bound := range []WindowBound{{Kind: WindowPreceding, Offset: 1<<63 - 1}, {Kind: WindowFollowing, Offset: 1<<63 - 1}, {Kind: WindowUnboundedPreceding}, {Kind: WindowUnboundedFollowing}} {
		ctx, _, query := spillOutputQuery(t)
		frame := WindowFrame{Start: bound, End: bound}
		win, err := NewWindowSource(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowCountRows, Alias: "rows", Frame: frame}, {Function: WindowCountValues, Column: "v", Alias: "n", Frame: frame}}, []memory.Field{{Name: "rows", Type: memory.TypeInt64}, {Name: "n", Type: memory.TypeInt64}}, query)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels()})
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		ok, err := stream.Next(ctx, out)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		for _, name := range []string{"rows", "n"} {
			if !reflect.DeepEqual(out.Int64s[name].Values, []int64{0, 0, 0}) {
				t.Fatalf("%+v %s: %v", bound, name, out.Int64s[name].Values)
			}
		}
		out.ClearColumns()
		checkSpillOutputCleanup(t, stream, query)
	}
}
