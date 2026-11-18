package physical

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"strings"
	"testing"
)

// A real Arrow-backed opaque binding retains its descriptive Children. The
// default arrowrecord adapter itself rejects these Struct types in N9 and N10;
// this fixture tests the already supported Source opaque-binding contract.
func TestN10OpaqueStructDecimalMetadataPreservesLegacy(t *testing.T) {
	for _, depth := range []int{1, 14} {
		for _, native := range []bool{false, true} {
			var dt arrow.DataType = &arrow.Decimal128Type{Precision: 9, Scale: 2}
			f := memory.Field{Name: "item", Type: memory.TypeDecimal, ArrowType: dt.String()}
			for i := 0; i < depth; i++ {
				dt = arrow.StructOf(arrow.Field{Name: "item", Type: dt, Nullable: true})
				f = memory.StructField("item", true, f)
			}
			f.Name = "payload"
			builder := array.NewBuilder(am.DefaultAllocator, dt)
			var add func(array.Builder, int64)
			add = func(b array.Builder, n int64) {
				if v, ok := b.(*array.StructBuilder); ok {
					v.Append(true)
					add(v.FieldBuilder(0), n)
				} else {
					b.(*array.Decimal128Builder).Append(decimal128.FromI64(n))
				}
			}
			for _, n := range []int64{30, 10, 20} {
				add(builder, n)
			}
			arr := builder.NewArray()
			builder.Release()
			batch := memory.NewBatch()
			batch.Rows = 3
			batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 1, 2}}
			batch.Opaque["payload"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeStruct, ArrowType: dt.String(), Value: arr}
			fields := []memory.Field{{Name: "key", Type: memory.TypeInt64}, f}
			key := "key"
			if native {
				d, _ := memory.Decimal128Field("d", false, 9, 2)
				fields = append(fields, d)
				batch.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(30), decimal128.FromI64(10), decimal128.FromI64(20)}, Precision: 9, Scale: 2}
				key = "d"
			}
			input := &n9Source{n8Source: n8Source{schema: memory.NewSchema(fields...), batches: []*memory.Batch{batch}}}
			m := memory.NewBudgetManager(8 << 20)
			sorted, e := NewSortSource(input, []SortKey{{Column: key}}, m)
			if e != nil {
				arr.Release()
				t.Fatal(depth, native, e)
			}
			stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
			if e != nil {
				arr.Release()
				t.Fatal(e)
			}
			out := memory.NewBatch()
			for _, want := range []int64{10, 20, 30} {
				ok, e := stream.Next(context.Background(), out)
				if !ok || e != nil {
					t.Fatal(depth, native, ok, e)
				}
				v := out.Opaque["payload"]
				if v == nil {
					t.Fatal("opaque binding lost")
				}
				a := v.Value.(arrow.Array)
				for i := 0; i < depth; i++ {
					a = a.(*array.Struct).Field(0)
				}
				if a.(*array.Decimal128).Value(0) != decimal128.FromI64(want) {
					t.Fatal("opaque value")
				}
				if native && out.Decimals["d"].Values[0] != decimal128.FromI64(want) {
					t.Fatal("native key")
				}
			}
			stream.Close()
			arr.Release()
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
		}
	}
}
func TestN10ActualTypedNestedDecimalStillRejected(t *testing.T) {
	for _, rows := range []int{0, 1} {
		child, _ := memory.Decimal128Field("d", true, 9, 2)
		f := memory.StructField("payload", true, child)
		values := memory.NewBatch()
		values.Rows = rows
		values.Decimals["d"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Precision: 9, Scale: 2}
		b := memory.NewBatch()
		b.Rows = rows
		b.Structs["payload"] = &memory.StructVector{Rows: rows, Fields: f.Children, Values: values}
		b.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, rows)}
		input := &n9Source{n8Source: n8Source{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, f), batches: []*memory.Batch{b}}}
		m := memory.NewBudgetManager(1 << 20)
		sorted, e := NewSortSource(input, []SortKey{{Column: "key"}}, m)
		if e != nil {
			t.Fatal("schema alone must not classify opaque/native", e)
		}
		stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := stream.Next(context.Background(), memory.NewBatch()); ok || e == nil || !strings.Contains(e.Error(), "native nested Decimal") {
			t.Fatal(rows, ok, e)
		}
		stream.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}
