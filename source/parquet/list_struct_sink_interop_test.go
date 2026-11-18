package parquet_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ps "github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/lazy"
)

func sinkInteropFields(nullable bool) []arrow.Field {
	types := []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16, arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64, arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64, arrow.FixedWidthTypes.Boolean, arrow.FixedWidthTypes.Date32, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, arrow.BinaryTypes.String}
	names := []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "bool", "date", "ts", "text.中"}
	fields := make([]arrow.Field, len(types))
	for i, dt := range types {
		fields[i] = arrow.Field{Name: names[i], Type: dt, Nullable: nullable}
	}
	return fields
}

// Values are literal external-format expectations, not values obtained from
// either GoLume encoder or GoLume reader.
func sinkInteropElement(row, seed int, null bool) map[string]any {
	vals := []any{int8(-128), int16(-32768), int32(-2147483648), int64(math.MinInt64) + int64(row), uint8(255), uint16(65535), uint32(math.MaxUint32), uint64(math.MaxUint64), float32(1.25), float64(-1.125), row%2 == 0, arrow.Date32(-1), arrow.Timestamp(-1000000001 + int64(row)), []string{"", "界🙂", "a\x00b"}[(row+seed)%3]}
	m := map[string]any{}
	for i, f := range sinkInteropFields(true) {
		if null {
			m[f.Name] = nil
		} else {
			m[f.Name] = vals[i]
		}
	}
	return m
}
func sinkInteropList(row, seed int, L, E, C bool) any {
	switch (row + seed) % 6 {
	case 0:
		if L {
			return nil
		}
	case 1:
		return []any{}
	case 2:
		if E {
			return []any{nil}
		}
	case 3:
		if C {
			return []any{sinkInteropElement(row, seed, true)}
		}
	}
	return []any{sinkInteropElement(row, seed, false), sinkInteropElement(row+1, seed, false)}
}
func sinkInteropAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch a := b.(type) {
	case *array.StructBuilder:
		a.Append(true)
		m := v.(map[string]any)
		for i, f := range a.Type().(*arrow.StructType).Fields() {
			sinkInteropAppend(a.FieldBuilder(i), m[f.Name])
		}
	case *array.ListBuilder:
		a.Append(true)
		for _, e := range v.([]any) {
			sinkInteropAppend(a.ValueBuilder(), e)
		}
	case *array.Int8Builder:
		a.Append(v.(int8))
	case *array.Int16Builder:
		a.Append(v.(int16))
	case *array.Int32Builder:
		a.Append(v.(int32))
	case *array.Int64Builder:
		a.Append(v.(int64))
	case *array.Uint8Builder:
		a.Append(v.(uint8))
	case *array.Uint16Builder:
		a.Append(v.(uint16))
	case *array.Uint32Builder:
		a.Append(v.(uint32))
	case *array.Uint64Builder:
		a.Append(v.(uint64))
	case *array.Float32Builder:
		a.Append(v.(float32))
	case *array.Float64Builder:
		a.Append(v.(float64))
	case *array.BooleanBuilder:
		a.Append(v.(bool))
	case *array.Date32Builder:
		a.Append(v.(arrow.Date32))
	case *array.TimestampBuilder:
		a.Append(v.(arrow.Timestamp))
	case *array.StringBuilder:
		a.Append(v.(string))
	default:
		panic(fmt.Sprintf("fixture builder %T", b))
	}
}
func sinkInteropWrapped(row, depth int, L, E, C bool) any {
	var v any = sinkInteropList(row, 1, L, E, C)
	for d := depth - 1; d >= 0; d-- {
		if d%2 == 0 && row%11 == d {
			v = nil
		} else {
			v = map[string]any{"node": v}
		}
	}
	return v
}
func sinkInteropRecord(t *testing.T, alloc am.Allocator, depth, n int, L, E, C bool) arrow.RecordBatch {
	item := arrow.Field{Name: "item", Type: arrow.StructOf(sinkInteropFields(C)...), Nullable: E}
	list := arrow.ListOfField(item)
	var second arrow.DataType = list
	for d := depth - 1; d >= 0; d-- {
		childNullable := L
		if d < depth-1 {
			childNullable = (d+1)%2 == 0
		}
		second = arrow.StructOf(arrow.Field{Name: "node", Type: second, Nullable: childNullable})
	}
	secondNullable := L
	if depth > 0 {
		secondNullable = true
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "first", Type: list, Nullable: L}, {Name: "second.literal", Type: second, Nullable: secondNullable}}, nil)
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	for r := 0; r < n; r++ {
		sinkInteropAppend(b.Field(0), sinkInteropList(r, 0, L, E, C))
		sinkInteropAppend(b.Field(1), sinkInteropWrapped(r, depth, L, E, C))
	}
	return b.NewRecordBatch()
}
func sinkInteropValue(a arrow.Array, row int) any {
	if a.IsNull(row) {
		return nil
	}
	switch v := a.(type) {
	case *array.Struct:
		m := map[string]any{}
		for i, f := range v.DataType().(*arrow.StructType).Fields() {
			m[f.Name] = sinkInteropValue(v.Field(i), row)
		}
		return m
	case *array.List:
		lo, hi := v.ValueOffsets(row)
		out := make([]any, hi-lo)
		for i := lo; i < hi; i++ {
			out[i-lo] = sinkInteropValue(v.ListValues(), int(i))
		}
		return out
	case *array.Int8:
		return v.Value(row)
	case *array.Int16:
		return v.Value(row)
	case *array.Int32:
		return v.Value(row)
	case *array.Int64:
		return v.Value(row)
	case *array.Uint8:
		return v.Value(row)
	case *array.Uint16:
		return v.Value(row)
	case *array.Uint32:
		return v.Value(row)
	case *array.Uint64:
		return v.Value(row)
	case *array.Float32:
		return v.Value(row)
	case *array.Float64:
		return v.Value(row)
	case *array.Boolean:
		return v.Value(row)
	case *array.Date32:
		return v.Value(row)
	case *array.Timestamp:
		return v.Value(row)
	case *array.String:
		return v.Value(row)
	default:
		panic(fmt.Sprintf("external reader type %T", a))
	}
}
func sinkInteropCheckField(t *testing.T, f arrow.Field, depth int, L, E, C bool) {
	t.Helper()
	for d := 0; d < depth; d++ {
		if f.Nullable != (d%2 == 0) {
			t.Fatalf("ancestor %d nullable=%v", d, f.Nullable)
		}
		s, ok := f.Type.(*arrow.StructType)
		if !ok || s.NumFields() != 1 {
			t.Fatalf("ancestor shape %v", f)
		}
		f = s.Field(0)
	}
	if f.Nullable != L {
		t.Fatalf("List nullable=%v want %v", f.Nullable, L)
	}
	l, ok := f.Type.(*arrow.ListType)
	if !ok {
		t.Fatalf("not List: %v", f)
	}
	if l.ElemField().Nullable != E {
		t.Fatal("element nullable")
	}
	s, ok := l.Elem().(*arrow.StructType)
	if !ok {
		t.Fatal("not Struct element")
	}
	want := sinkInteropFields(C)
	if s.NumFields() != 14 {
		t.Fatal("child count")
	}
	for i, w := range want {
		g := s.Field(i)
		if g.Name != w.Name || g.Nullable != w.Nullable || !arrow.TypeEqual(g.Type, w.Type) {
			t.Fatalf("child %d got %v want %v", i, g, w)
		}
	}
}
func TestListStructSinkIndependentInterop(t *testing.T) {
	for _, depth := range []int{0, 7} {
		for mask := 0; mask < 8; mask++ {
			for _, n := range []int{0, 19} {
				t.Run(fmt.Sprintf("depth%d/null%d/rows%d", depth, mask, n), func(t *testing.T) {
					L, E, C := mask&1 != 0, mask&2 != 0, mask&4 != 0
					alloc := am.NewCheckedAllocator(am.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					rec := sinkInteropRecord(t, alloc, depth, n, L, E, C)
					defer rec.Release()
					session := lazy.NewSession(engine.DefaultOptions())
					defer session.Close()
					q, err := session.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeStructList: true}, rec)
					if err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(t.TempDir(), "interop.parquet")
					if _, err = q.SinkParquet(context.Background(), path, lazy.ParquetSinkOptions{RowGroupRows: 4}); err != nil {
						t.Fatal(err)
					}
					f, err := os.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					rd, err := file.NewParquetReader(f)
					if err != nil {
						t.Fatal(err)
					}
					defer rd.Close()
					if rd.NumRows() != int64(n) {
						t.Fatalf("file rows %d", rd.NumRows())
					}
					if n > 0 && rd.NumRowGroups() < 2 {
						t.Fatal("missing real multiple rowgroups")
					}
					if rd.MetaData().Schema.NumColumns() != 28 {
						t.Fatal("physical leaf count")
					}
					for leaf := 0; leaf < 28; leaf++ {
						col := rd.MetaData().Schema.Column(leaf)
						defs := int16(1)
						if L {
							defs++
						}
						if E {
							defs++
						}
						if C {
							defs++
						}
						if leaf >= 14 {
							defs += int16((depth + 1) / 2)
						}
						if col.MaxDefinitionLevel() != defs || col.MaxRepetitionLevel() != 1 {
							t.Fatalf("leaf%d def/rep %d/%d want%d/1", leaf, col.MaxDefinitionLevel(), col.MaxRepetitionLevel(), defs)
						}
						wantPhysical := pq.Types.Int32
						switch leaf % 14 {
						case 3, 7, 12:
							wantPhysical = pq.Types.Int64
						case 8:
							wantPhysical = pq.Types.Float
						case 9:
							wantPhysical = pq.Types.Double
						case 10:
							wantPhysical = pq.Types.Boolean
						case 13:
							wantPhysical = pq.Types.ByteArray
						}

						switch leaf % 14 {
						case 0, 1, 2, 4, 5, 6, 7:
							lt, ok := col.LogicalType().(ps.IntLogicalType)
							bits := []int8{8, 16, 32, 64, 8, 16, 32, 64}[leaf%14]
							if !ok || lt.BitWidth() != bits || lt.IsSigned() != (leaf%14 < 4) {
								t.Fatalf("integer annotation %v", col.LogicalType())
							}
						case 11:
							if _, ok := col.LogicalType().(ps.DateLogicalType); !ok {
								t.Fatalf("date annotation %v", col.LogicalType())
							}
						case 12:
							lt, ok := col.LogicalType().(ps.TimestampLogicalType)
							if !ok || !lt.IsAdjustedToUTC() || lt.TimeUnit() != ps.TimeUnitNanos {
								t.Fatalf("timestamp annotation %v", col.LogicalType())
							}
						case 13:
							if _, ok := col.LogicalType().(ps.StringLogicalType); !ok {
								t.Fatalf("string annotation %v", col.LogicalType())
							}
						}
						if col.PhysicalType() != wantPhysical {
							t.Fatalf("physical %d %v", leaf, col.PhysicalType())
						}
					}
					if L && E && C && n > 0 {
						var defs, reps []int16
						for group := 0; group < rd.NumRowGroups(); group++ {
							cr, e := rd.RowGroup(group).Column(0)
							if e != nil {
								t.Fatal(e)
							}
							var d, r [128]int16
							var vals [128]int32
							got, _, e := cr.(*file.Int32ColumnChunkReader).ReadBatch(128, vals[:], d[:], r[:])
							if e != nil {
								t.Fatal(e)
							}
							defs = append(defs, d[:got]...)
							reps = append(reps, r[:got]...)
						}
						var wd, wr []int16
						for row := 0; row < n; row++ {
							switch row % 6 {
							case 0:
								wd = append(wd, 0)
								wr = append(wr, 0)
							case 1:
								wd = append(wd, 1)
								wr = append(wr, 0)
							case 2:
								wd = append(wd, 2)
								wr = append(wr, 0)
							case 3:
								wd = append(wd, 3)
								wr = append(wr, 0)
							default:
								wd = append(wd, 4, 4)
								wr = append(wr, 0, 1)
							}
						}
						if !reflect.DeepEqual(defs, wd) || !reflect.DeepEqual(reps, wr) {
							t.Fatalf("actual level stream defs%v reps%v want%v/%v", defs, reps, wd, wr)
						}
					}
					external, err := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, alloc)
					if err != nil {
						t.Fatal(err)
					}
					tab, err := external.ReadTable(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer tab.Release()
					sinkInteropCheckField(t, tab.Schema().Field(0), 0, L, E, C)
					sinkInteropCheckField(t, tab.Schema().Field(1), depth, L, E, C)
					reader := array.NewTableReader(tab, 3)
					defer reader.Release()
					row := 0
					for reader.Next() {
						r := reader.RecordBatch()
						for i := 0; i < int(r.NumRows()); i++ {
							want := []any{sinkInteropList(row, 0, L, E, C), sinkInteropWrapped(row, depth, L, E, C)}
							for j := 0; j < 2; j++ {
								if got := sinkInteropValue(r.Column(j), i); !reflect.DeepEqual(got, want[j]) {
									t.Fatalf("row%d column%d got%#v want%#v", row, j, got, want[j])
								}
							}
							row++
						}
					}
					if reader.Err() != nil || row != n {
						t.Fatalf("external read count/error %d/%v", row, reader.Err())
					}
				})
			}
		}
	}
}

func TestListStructSinkIndependentShards(t *testing.T) {
	for _, n := range []int{0, 19} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			alloc := am.NewCheckedAllocator(am.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			rec := sinkInteropRecord(t, alloc, 7, n, true, true, true)
			defer rec.Release()
			s := lazy.NewSession(engine.DefaultOptions())
			defer s.Close()
			q, e := s.ScanArrowWithOptions(lazy.ArrowScanOptions{NativeStructList: true}, rec)
			if e != nil {
				t.Fatal(e)
			}
			result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "parts"), lazy.ParquetSinkOptions{SinkOptions: lazy.SinkOptions{MaxRowsPerFile: 7}, RowGroupRows: 3})
			if e != nil {
				t.Fatal(e)
			}
			// The existing sink opens one schema-bearing part even for empty input; nonempty sources split 7,7,5.
			wantFiles := 3
			if n == 0 {
				wantFiles = 1
			}
			if len(result.Files) != wantFiles {
				t.Fatalf("parts=%d want%d", len(result.Files), wantFiles)
			}
			row := 0
			for _, path := range result.Files {
				func() {
					f, e := os.Open(path)
					if e != nil {
						t.Fatal(e)
					}
					defer f.Close()
					rd, e := file.NewParquetReader(f)
					if e != nil {
						t.Fatal(e)
					}
					defer rd.Close()
					if rd.NumRows() > 7 || (n > 0 && rd.NumRowGroups() < 2) {
						t.Fatalf("part rows/groups %d/%d", rd.NumRows(), rd.NumRowGroups())
					}
					ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, alloc)
					if e != nil {
						t.Fatal(e)
					}
					tab, e := ar.ReadTable(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					defer tab.Release()
					sinkInteropCheckField(t, tab.Schema().Field(1), 7, true, true, true)
					tr := array.NewTableReader(tab, 2)
					defer tr.Release()
					for tr.Next() {
						r := tr.RecordBatch()
						for i := 0; i < int(r.NumRows()); i++ {
							for j, w := range []any{sinkInteropList(row, 0, true, true, true), sinkInteropWrapped(row, 7, true, true, true)} {
								if g := sinkInteropValue(r.Column(j), i); !reflect.DeepEqual(g, w) {
									t.Fatalf("shard row%d col%d got%#v want%#v", row, j, g, w)
								}
							}
							row++
						}
					}
					if tr.Err() != nil {
						t.Fatal(tr.Err())
					}
				}()
			}
			if row != n {
				t.Fatalf("shard total %d want%d", row, n)
			}
		})
	}
}
