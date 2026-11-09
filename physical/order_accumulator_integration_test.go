package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
	"unsafe"
)

// This is deliberately a lower bound independent of the production planner:
// public descriptors and actual slice capacities, without any map buckets,
// reservation objects, allocator rounding or admission convention. A token
// below even this amount cannot cover the live output graph.
func sortOutputStructuralMinimum(batch *memory.Batch) int64 {
	if batch == nil {
		return 0
	}
	total := int64(unsafe.Sizeof(memory.Batch{}))
	value := reflect.ValueOf(batch).Elem()
	for i := 0; i < value.NumField(); i++ {
		columns := value.Field(i)
		if columns.Kind() != reflect.Map {
			continue
		}
		iter := columns.MapRange()
		for iter.Next() {
			vector := iter.Value()
			if vector.IsNil() {
				continue
			}
			descriptor := vector.Elem()
			total += int64(descriptor.Type().Size())
			for j := 0; j < descriptor.NumField(); j++ {
				field := descriptor.Field(j)
				if field.Kind() == reflect.Slice {
					total += int64(field.Cap()) * int64(field.Type().Elem().Size())
				}
			}
		}
	}
	for _, vector := range batch.StringLists {
		if vector.Values == nil {
			continue
		}
		total += int64(unsafe.Sizeof(memory.StringViewVector{})) + int64(cap(vector.Values.Offsets))*4 + int64(cap(vector.Values.Data)) + int64(cap(vector.Values.Validity))*8
	}
	for _, vector := range batch.Structs {
		total += sortOutputStructuralMinimum(vector.Values)
	}
	for _, vector := range batch.DictStrings {
		total += int64(len(vector.DictionaryDomain))
	}
	return total
}

func TestExternalSortWideOutputOwnsMetadata(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprintf("struct=%t", nested), func(t *testing.T) {
			const columns = 2048
			fields := make([]memory.Field, columns)
			values := memory.NewBatch()
			values.Rows = 1
			for i := range fields {
				name := fmt.Sprintf("v_%04d", i)
				fields[i] = memory.Field{Name: name, Type: memory.TypeInt8}
				values.Int8s[name] = &memory.Int8Vector{Values: []int8{int8(i)}}
			}
			batch := values
			if nested {
				batch = memory.NewBatch()
				batch.Rows = 1
				batch.Structs["nested"] = &memory.StructVector{Rows: 1, Fields: fields, Values: values, Encoding: memory.EncodingView}
				fields = []memory.Field{{Name: "nested", Type: memory.TypeStruct, Children: fields}}
			}
			batch.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
			fields = append(fields, memory.Field{Name: "key", Type: memory.TypeInt64})
			src, err := inmemory.New(memory.NewSchema(fields...), batch)
			if err != nil {
				t.Fatal(err)
			}
			stream, query, _, ctx := openSortLifecycle(t, 16<<20, src, t.TempDir())
			stream.chunkRows = 1
			dst := memory.NewBatch()
			if ok, err := stream.Next(ctx, dst); !ok || err != nil {
				t.Fatal(ok, err)
			}
			if stream.finalHeap.Len() != 0 {
				t.Fatal("fixture must close the last decoded cursor")
			}
			want := sortOutputStructuralMinimum(dst)
			if got := stream.workspace.Used(); got < want {
				t.Fatalf("live output undercharged after decoded owner ended: token=%d actual descriptor/capacity lower bound=%d", got, want)
			}
			out := dst
			if nested {
				out = dst.Structs["nested"].Values
			}
			for i := 0; i < columns; i++ {
				if v := out.Int8s[fmt.Sprintf("v_%04d", i)]; v == nil || len(v.Values) != 1 || v.Values[0] != int8(i) {
					t.Fatal("wide output changed", i)
				}
			}
			if ok, err := stream.Next(context.Background(), dst); ok || err != nil {
				t.Fatal(ok, err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if query.Used() != 0 {
				t.Fatal("wide output leaked", query.Used())
			}
			// The output's own Struct.Fields and columns must not clear the source.
			if len(values.Int8s) != columns || values.Int8s["v_0000"] == nil {
				t.Fatal("cleanup changed source columns")
			}
		})
	}
}
