package physical

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/memory"
	ar "gocolumn/source/arrowrecord"
	"strings"
	"testing"
)

func TestN38ReviewRealArrowProvenanceCaps(t *testing.T) {
	for _, parents := range []int{0, 1, 8} {
		for _, depth := range []int{2, 3} {
			var dt arrow.DataType = arrow.PrimitiveTypes.Int64
			for i := 0; i < depth; i++ {
				dt = arrow.ListOf(dt)
			}
			for i := 0; i < parents; i++ {
				dt = arrow.StructOf(arrow.Field{Name: "inside", Type: dt, Nullable: true})
			}
			schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "v", Type: dt, Nullable: true}, {Name: "legacy", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), Nullable: true}}, nil)
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			builder := array.NewRecordBuilder(alloc, schema)
			rec := builder.NewRecordBatch()
			builder.Release()
			src, e := ar.NewSourceWithOptions(ar.Options{NativeNested: true}, rec)
			rec.Release()
			if e != nil {
				t.Fatal(parents, depth, e)
			}
			f, _ := src.Schema().Field("v")
			legacy, _ := src.Schema().Field("legacy")
			if legacy.ArrowType == "" || legacy.Children[0].ArrowType == "" {
				t.Fatal("unrelated depth2 sibling provenance lost")
			}
			if depth == 2 {
				if f.ArrowType == "" {
					t.Fatal("old depth2 provenance lost")
				}
				src.Close()
				alloc.AssertSize(t, 0)
				continue
			}
			path := f
			for {
				if path.ArrowType != "" {
					t.Fatal("native3 ancestor provenance can bypass cap", parents, path.Name)
				}
				if len(path.Children) == 0 {
					break
				}
				path = path.Children[0]
			}
			m := memory.NewBudgetManager(1 << 20)
			keys := []SortKey{{Column: "k"}}
			reject := func(name string, e error) {
				t.Helper()
				if e == nil || !strings.Contains(e.Error(), "at most 2") {
					t.Fatalf("%s must hit explicit cap: %v", name, e)
				}
			}
			_, e = NewSortSource(src, keys, m)
			if parents == 0 {
				if e != nil {
					t.Fatal("N40 root Sort rejected", e)
				}
			} else {
				reject("Sort", e)
			}
			_, e = NewSortSourceWithOptions(src, keys, m, SortSourceOptions{Spillable: true})
			if parents == 0 {
				if e != nil {
					t.Fatal("N42 root spill Sort", e)
				}
			} else {
				reject("spill Sort", e)
			}
			_, e = NewTopKSourceWithManager(src, 1, keys, m)
			if parents == 0 {
				if e != nil {
					t.Fatal("N41 top-level TopK", e)
				}
			} else {
				reject("TopK", e)
			}
			plain := &n38GuardSource{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64})}
			_, e = NewTopKSourceWithKeyInput(plain, src, 1, keys, m)
			reject("keyInput", e)
			bound, getErr := expr.Bind(src.Schema(), expr.ListGet(expr.Col("v"), expr.I64(0)))
			if parents == 0 {
				if getErr != nil {
					t.Fatal("N39 root ListGet", getErr)
				}
				out := bound.OutputField("out")
				if !out.Nullable || out.Encoding != memory.EncodingView || out.ValidateNestedList() != nil || memory.ValidateRepeatedListDepth([]memory.Field{out}, 2) != nil || out.DecimalPrecision != 0 || out.DecimalScale != 0 {
					t.Fatal("N39 output metadata", out)
				}
			} else if getErr == nil {
				t.Fatal("Struct root ListGet admitted")
			}
			b := memory.NewBatch()
			_, e = memory.EncodeColumnarRun(context.Background(), src.Schema(), b, m)
			if parents == 0 {
				if e == nil {
					t.Fatal("missing batch storage accepted")
				}
			} else {
				reject("encode", e)
			}
			_, e = memory.DecodeColumnarRun(context.Background(), src.Schema(), &memory.RestoredSpill{Object: memory.SpillObject{Codec: memory.ColumnarRunCodec}}, m)
			if parents == 0 {
				if e == nil {
					t.Fatal("empty wire accepted")
				}
			} else {
				reject("decode", e)
			}
			if m.Used() != 0 {
				t.Fatal("cap leaked", m.Used())
			}
			src.Close()
			alloc.AssertSize(t, 0)
		}
	}
}
func TestN38ReviewArrowTypedDeepStillRejects(t *testing.T) {
	var dt arrow.DataType = arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))
	for i := 0; i < 9; i++ {
		dt = arrow.StructOf(arrow.Field{Name: "inside", Type: dt})
	}
	alloc := am.NewCheckedAllocator(am.DefaultAllocator)
	b := array.NewRecordBuilder(alloc, arrow.NewSchema([]arrow.Field{{Name: "v", Type: dt}}, nil))
	r := b.NewRecordBatch()
	b.Release()
	s, e := ar.NewSourceWithOptions(ar.Options{NativeNested: true}, r)
	r.Release()
	if e == nil {
		s.Close()
		t.Fatal("typed Struct depth9 accepted")
	}
	alloc.AssertSize(t, 0)
}
