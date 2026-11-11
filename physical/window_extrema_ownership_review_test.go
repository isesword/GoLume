package physical

import (
	"fmt"
	"reflect"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

func TestWindowExtremaReviewNestedReplacement(t *testing.T) {
	for _, kind := range []string{"list", "struct"} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprintf("%s/mode%d", kind, mode), func(t *testing.T) {
				ctx, _, query := spillOutputQuery(t)
				input := memory.NewBatch()
				input.Rows = 3
				input.Int8s["v"] = &memory.Int8Vector{Values: []int8{3, 1, 2}}
				input.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
				var field memory.Field
				if kind == "list" {
					field = memory.ListStringField("other", false, false)
					input.StringLists["other"] = &memory.StringListVector{Offsets: []uint32{0, 1, 2, 3}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc"), Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
				} else {
					child := memory.NewBatch()
					child.Rows = 3
					child.Int64s["child"] = &memory.Int64Vector{Values: []int64{9, 8, 7}}
					field = memory.StructField("other", false, memory.Field{Name: "child", Type: memory.TypeInt64})
					input.Structs["other"] = &memory.StructVector{Rows: 3, Fields: field.Children, Values: child, Encoding: memory.EncodingView}
				}
				schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt8}, memory.Field{Name: "id", Type: memory.TypeInt64}, field)
				src, err := inmemory.New(schema, input)
				if err != nil {
					t.Fatal(err)
				}
				expr := WindowExpression{Function: WindowMin, Column: "v", Alias: "other", Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowCurrentRow}}}
				window, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{expr}, []memory.Field{{Name: "other", Type: memory.TypeInt8, Nullable: true}}, query, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(ctx, window, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				defer out.ClearColumns()
				row := 0
				for {
					ok, err := stream.Next(ctx, out)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					maps := reflect.ValueOf(out).Elem()
					for i := 0; i < maps.NumField(); i++ {
						m := maps.Field(i)
						if m.Kind() == reflect.Map {
							got := m.MapIndex(reflect.ValueOf("other")).IsValid()
							want := maps.Type().Field(i).Name == "Int8s"
							if got != want {
								t.Fatalf("stale or missing alias binding: %s present=%t", maps.Type().Field(i).Name, got)
							}
						}
					}
					for i, v := range out.Int8s["other"].Values {
						want := []int8{3, 1, 1}[row]
						if v != want || !out.Int8s["other"].IsValid(i) {
							t.Fatalf("row%d=%d want %d", row, v, want)
						}
						row++
					}
				}
				if row != 3 {
					t.Fatalf("rows=%d", row)
				}
				if kind == "list" {
					if got := string(input.StringLists["other"].Values.Data); got != "abc" {
						t.Fatalf("mutated input %q", got)
					}
				} else {
					if !reflect.DeepEqual(input.Structs["other"].Values.Int64s["child"].Values, []int64{9, 8, 7}) {
						t.Fatal("mutated struct input")
					}
				}
				checkSpillOutputCleanup(t, stream, query)
			})
		}
	}
}

type extremaReviewRef struct{ releases int }

func (r *extremaReviewRef) Release() {
	r.releases++
	if r.releases > 1 {
		panic("double release")
	}
}

func TestWindowExtremaReviewDetachOwnership(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, ownedClear := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested-%t/owned-clear-%t", nested, ownedClear), func(t *testing.T) {
				ref, untouched := &extremaReviewRef{}, &extremaReviewRef{}
				input := memory.NewBatch()
				input.Opaque["other"] = &memory.OpaqueVector{Value: ref, Owned: true}
				if nested {
					child := input
					input = memory.NewBatch()
					input.Structs["other"] = &memory.StructVector{Values: child}
				}
				input.Opaque["keep"] = &memory.OpaqueVector{Value: untouched, Owned: true}
				if ownedClear {
					clearOwnedWindowOutput(input, "other")
				} else {
					borrowed := memory.NewBatch()
					bindAll(borrowed, input)
					clearWindowOutput(borrowed, "other")
				}
				want := 0
				if ownedClear {
					want = 1
				}
				if ref.releases != want {
					t.Fatalf("release count=%d want %d", ref.releases, want)
				}
				// The original source still owns borrowed payloads; owned deletion
				// must leave every other alias alive.
				if untouched.releases != 0 {
					t.Fatal("unrelated alias released")
				}
				if !ownedClear {
					input.ReleaseOwned()
					if ref.releases != 1 {
						t.Fatalf("source release count=%d", ref.releases)
					}
				} else {
					input.ReleaseOwned()
				}
			})
		}
	}
}
