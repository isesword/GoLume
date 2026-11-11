package lazy

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/arrowrecord"
)

func TestArrowBooleanRollingCountPublicPipeline(t *testing.T) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, shape := range []string{"mixed", "valid", "null", "empty"} {
			t.Run(mode+"/"+shape, func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer alloc.AssertSize(t, 0)
				schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "group", Type: arrow.PrimitiveTypes.Int64}, {Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}, {Name: "original", Type: arrow.FixedWidthTypes.Boolean, Nullable: true}}, nil)
				builder := array.NewRecordBuilder(alloc, schema)
				defer builder.Release()
				order := []int{3, 0, 6, 1, 4, 7, 2, 5}
				if mode == "ordered" {
					order = []int{0, 1, 2, 3, 4, 5, 6, 7}
				}
				if shape == "empty" {
					order = nil
				}
				valid := func(id int) bool { return shape == "valid" || shape == "mixed" && id%3 != 1 }
				// Prefix makes actual Arrow values non-byte-aligned. It is never exposed.
				builder.Field(0).(*array.Int64Builder).Append(-1)
				builder.Field(1).(*array.Int64Builder).Append(-1)
				for _, i := range []int{2, 3} {
					builder.Field(i).(*array.BooleanBuilder).Append(false)
				}
				for _, id := range order {
					builder.Field(0).(*array.Int64Builder).Append(int64(id))
					builder.Field(1).(*array.Int64Builder).Append(int64(id / 4))
					for _, i := range []int{2, 3} {
						if valid(id) {
							builder.Field(i).(*array.BooleanBuilder).Append(id%2 == 1)
						} else {
							builder.Field(i).(*array.BooleanBuilder).AppendNull()
						}
					}
				}
				record := builder.NewRecordBatch()
				defer record.Release()
				middle := min(4, len(order)) + 1
				first, second := record.NewSlice(1, int64(middle)), record.NewSlice(int64(middle), int64(len(order)+1))
				defer first.Release()
				defer second.Release()
				base, err := arrowrecord.NewSource(first, second)
				if err != nil {
					t.Fatal(err)
				}
				defer base.Close()
				var input source.Source = base
				if mode == "ordered" {
					input = &orderedWindowTestSource{Source: base, ordering: []source.OrderingColumn{{Name: "group"}, {Name: "id", NullsLast: true}}}
				}
				opts := engine.DefaultOptions()
				opts.MemoryLimitBytes = 8 << 20
				opts.Pipeline = pipeline.Options{Workers: 1, TargetMorselBytes: 256, MaxMorselBytes: 1024, MaxInFlightBytes: 4096, MinMorselRows: 1, MaxMorselRows: 2}
				if mode == "external" {
					opts.ExecutionPolicy = engine.ExecutionBounded
					opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
				}
				session := NewSession(opts)
				defer session.Close()
				frame := RowsBetween(Preceding(1), CurrentRow)
				query := session.FromSource(input).WithWindows(NewWindow(PartitionBy("group"), OrderBy(Asc("id"))), RollingCount("flag", frame, "flag"), RollingCount("flag", frame, "n"), RollingCountRows(frame, "rows"))
				plan, err := query.ExplainPhysical(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				strategy := map[string]string{"resident": "strategy=resident", "ordered": "strategy=ordered-state", "external": "strategy=external-sort-restore"}[mode]
				if !strings.Contains(plan, strategy) {
					t.Fatalf("missing %s: %s", strategy, plan)
				}
				for repeat := 0; repeat < 2; repeat++ {
					result, err := query.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					for _, name := range []string{"flag", "n", "rows", "original"} {
						field, _ := result.Schema().Field(name)
						if name != "original" && (field.Type != memory.TypeInt64 || field.Nullable) {
							t.Fatalf("field %+v", field)
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
						for i, id := range order {
							if name == "original" {
								if valid(id) {
									want[i] = id%2 == 1
								}
								continue
							}
							var count int64
							for prev := max(id/4*4, id-1); prev <= id; prev++ {
								if name == "rows" || valid(prev) {
									count++
								}
							}
							want[i] = count
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("repeat%d %s=%v want %v", repeat, name, got, want)
						}
					}
					for _, b := range result.batches() {
						if b.Uint32s["flag"] != nil || b.Opaque["flag"] != nil {
							t.Fatal("replaced bool binding survived")
						}
						exported, err := memory.ArrowRecord(alloc, result.Schema(), b)
						if err != nil {
							t.Fatal(err)
						}
						exported.Release()
					}
					result.Close()
					if used := session.Engine().Memory().Used(); used != 0 {
						t.Fatalf("query retained %d bytes", used)
					}
				}
			})
		}
	}
}
