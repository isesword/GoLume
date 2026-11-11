package lazy

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestRollingCountNativeParentsAndAliases(t *testing.T) {
	for _, kind := range []string{"bool", "list", "struct"} {
		for _, mode := range []string{"resident", "ordered", "external"} {
			for _, shape := range []string{"mixed", "valid", "null", "empty"} {
				for _, start := range []RowBound{CurrentRow, Preceding(2), UnboundedPreceding} {
					t.Run(fmt.Sprintf("%s/%s/%s/start%d", kind, mode, shape, start.Kind), func(t *testing.T) {
						order := []int{3, 0, 6, 1, 4, 7, 2, 5}
						if mode == "ordered" {
							order = []int{0, 1, 2, 3, 4, 5, 6, 7}
						}
						if shape == "empty" {
							order = nil
						}
						valid := []bool{true, false, true, true, false, false, false, false}
						for i := range valid {
							if shape == "valid" {
								valid[i] = true
							}
							if shape == "null" {
								valid[i] = false
							}
						}
						var field memory.Field
						switch kind {
						case "bool":
							field = memory.Field{Name: "x", Type: memory.TypeBool, Nullable: true}
						case "list":
							field = memory.ListStringField("x", true, true)
						case "struct":
							field = memory.StructField("x", true, memory.Field{Name: "child", Type: memory.TypeInt64, Nullable: true})
						}
						schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt32}, field)
						var batches []*memory.Batch
						for _, id := range order {
							b := memory.NewBatch()
							b.Rows = 1
							b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(id)}}
							b.Int32s["grp"] = &memory.Int32Vector{Values: []int32{int32(id / 4)}}
							var bits memory.Bitmap
							if shape != "valid" {
								bits = memory.EnsureBitmap(nil, 1)
								if valid[id] {
									memory.Set(bits, 0)
								}
							}
							switch kind {
							case "bool":
								b.Uint32s["x"] = &memory.Uint32Vector{Values: []uint32{uint32(id % 2)}, Validity: bits}
							case "list":
								// Valid empty lists and lists containing NULL both count as one.
								child := &memory.StringViewVector{Offsets: []uint32{0}, Encoding: memory.EncodingView}
								end := uint32(0)
								if id%3 != 0 {
									end = 1
									child.Offsets = []uint32{0, 1}
									child.Data = []byte("x")
									child.Validity = memory.EnsureBitmap(nil, 1)
									if id%2 == 1 {
										memory.Set(child.Validity, 0)
									}
								}
								b.StringLists["x"] = &memory.StringListVector{Offsets: []uint32{0, end}, Values: child, Validity: bits, Encoding: memory.EncodingView}
							case "struct":
								child := memory.NewBatch()
								child.Rows = 1
								childBits := memory.EnsureBitmap(nil, 1)
								if !valid[id] {
									memory.Set(childBits, 0)
								}
								child.Int64s["child"] = &memory.Int64Vector{Values: []int64{999}, Validity: childBits}
								b.Structs["x"] = &memory.StructVector{Rows: 1, Fields: field.Children, Values: child, Validity: bits, Encoding: memory.EncodingView}
							}
							batches = append(batches, b)
						}
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
							session = boundedJoinSession(t, 512<<10)
						}
						defer func() {
							session.Close()
							if used := session.Engine().Memory().Used(); used != 0 {
								t.Errorf("leaked %d bytes", used)
							}
						}()
						bounds := RowsBetween(start, CurrentRow)
						query := session.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("id"))),
							RollingCount("x", bounds, "x"), RollingCount("x", bounds, "count"), RollingCount("x", bounds, "id"), RollingCount("id", bounds, "original_id_count"), RollingCountRows(bounds, "rows"))
						plan, err := query.ExplainPhysical(context.Background())
						if err != nil {
							t.Fatal(err)
						}
						strategy := map[string]string{"resident": "strategy=resident", "ordered": "strategy=ordered-state", "external": "strategy=external-sort-restore"}[mode]
						if !strings.Contains(plan, strategy) {
							t.Fatalf("missing %s: %s", strategy, plan)
						}
						// Repeated execution must neither mutate the source nor retain state.
						for repeat := 0; repeat < 2; repeat++ {
							result, err := query.Collect(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							for _, name := range []string{"x", "count", "id", "original_id_count", "rows"} {
								f, ok := result.Schema().Field(name)
								if !ok || f.Type != memory.TypeInt64 || f.Nullable {
									t.Fatalf("%s field=%+v", name, f)
								}
								col, err := result.Column(name)
								if err != nil {
									t.Fatal(err)
								}
								got, err := col.ToAnySlice()
								if err != nil {
									t.Fatal(err)
								}
								want := make([]any, len(order))
								for row, id := range order {
									lo := id / 4 * 4
									if start.Kind == logical.RowCurrent {
										lo = id
									}
									if start.Kind == logical.RowPreceding {
										lo = max(lo, id-int(start.Offset))
									}
									var count int64
									for j := lo; j <= id; j++ {
										if valid[j] || name == "rows" || name == "original_id_count" {
											count++
										}
									}
									want[row] = count
								}
								if !reflect.DeepEqual(got, want) {
									t.Errorf("%s=%v want %v", name, got, want)
								}
							}
							for _, batch := range result.batches() {
								maps := reflect.ValueOf(batch).Elem()
								for i := 0; i < maps.NumField(); i++ {
									m := maps.Field(i)
									if m.Kind() == reflect.Map {
										present := m.MapIndex(reflect.ValueOf("x")).IsValid()
										if present != (maps.Type().Field(i).Name == "Int64s") {
											t.Errorf("alias x binding %s=%t", maps.Type().Field(i).Name, present)
										}
									}
								}
							}
							result.Close()
						}
					})
				}
			}
		}
	}
}

func TestRollingCountFrameValidation(t *testing.T) {
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}))
	if err != nil {
		t.Fatal(err)
	}
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	bad := []RowsFrame{
		RowsBetween(Following(1), Preceding(1)), RowsBetween(Preceding(-1), CurrentRow), RowsBetween(Preceding(math.MinInt64), CurrentRow), RowsBetween(CurrentRow, Following(-1)),
		RowsBetween(RowBound{Kind: logical.RowBoundKind(255)}, CurrentRow), RowsBetween(CurrentRow, RowBound{Kind: logical.RowBoundKind(255)}),
		RowsBetween(RowBound{Kind: logical.RowCurrent, Offset: 1}, CurrentRow), RowsBetween(UnboundedPreceding, RowBound{Kind: logical.RowUnboundedFollowing, Offset: 1}),
		RowsBetween(RowBound{Kind: logical.RowUnboundedPreceding, Offset: 1}, CurrentRow),
	}
	for i, bounds := range bad {
		for _, rows := range []bool{false, true} {
			t.Run(fmt.Sprintf("invalid%d/rows%t", i, rows), func(t *testing.T) {
				e := RollingCount("id", bounds, "n")
				if rows {
					e = RollingCountRows(bounds, "n")
				}
				if _, err := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), e).Schema(); err == nil {
					t.Fatal("accepted invalid frame")
				}
			})
		}
	}
	for _, bounds := range []RowsFrame{RowsBetween(Preceding(math.MaxInt64), Following(math.MaxInt64)), RowsBetween(Preceding(0), Following(0)), RowsBetween(UnboundedPreceding, UnboundedFollowing)} {
		for _, e := range []WindowExpr{RollingCount("id", bounds, "n"), RollingCountRows(bounds, "n")} {
			if _, err := session.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), e).Schema(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
