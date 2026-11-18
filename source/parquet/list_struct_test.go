package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type listStructReaderConfig struct {
	twin                           bool
	ancestorShift                  int
	depth                          int
	L, E, C                        bool
	all, multi, long, large, mixed bool
	bad                            string
}

func listStructReaderKinds(all bool) []fixedTestKind {
	ks := fixedTestKinds()
	if !all {
		ks = []fixedTestKind{ks[3]}
	} else {
		ks = append(ks[:12:12], ks[14])
	}
	return append(ks, fixedTestKind{"text", memory.TypeString, pq.Types.ByteArray, schema.StringLogicalType{}, "Strings", 1})
}
func listStructReaderDomain(depth, row int) (int16, bool) {
	for i := 0; i < depth; i++ {
		if row%17 == i {
			return int16(i), false
		}
	}
	return int16(depth), true
}
func listStructReaderCount(c listStructReaderConfig, row int) int {
	if c.long && row == 11 {
		return 4097
	}
	if row%7 == 1 {
		return 0
	}
	if row%7 == 2 {
		return 1
	}
	return 3
}
func listStructReaderText(row, item int) string {
	if item%3 == 0 {
		return ""
	}
	if item%3 == 1 {
		return "界"
	}
	return fmt.Sprintf("r%d.%d", row, item)
}
func listStructReaderValue(row, item int) int64 { return int64((row%5)*10 + item%7 + 1) }
func listStructReaderWrite(t *testing.T, c listStructReaderConfig, groups []int, props ...pq.WriterProperty) string {
	t.Helper()
	req, opt := pq.Repetitions.Required, pq.Repetitions.Optional
	kinds := listStructReaderKinds(c.all)
	makeList := func(name string) schema.Node {
		children := schema.FieldList{}
		for _, k := range kinds {
			rep := req
			if c.C {
				rep = opt
			}
			n, e := schema.NewPrimitiveNodeLogical(k.name, rep, k.logical, k.physical, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			children = append(children, n)
		}
		rep := req
		if c.E {
			rep = opt
		}
		el, e := schema.NewGroupNode("element", rep, children, -1)
		if e != nil {
			t.Fatal(e)
		}
		mid, e := schema.NewGroupNode("list", pq.Repetitions.Repeated, schema.FieldList{el}, -1)
		if e != nil {
			t.Fatal(e)
		}
		rep = req
		if c.L {
			rep = opt
		}
		n, e := schema.NewGroupNodeLogical(name, rep, schema.FieldList{mid}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	var main schema.Node = makeList("items")
	for i := c.depth - 1; i >= 0; i-- {
		n, e := schema.NewGroupNode(fmt.Sprintf("node%d", i), opt, schema.FieldList{main}, -1)
		if e != nil {
			t.Fatal(e)
		}
		main = n
	}
	id, e := schema.NewPrimitiveNodeLogical("id", req, schema.NoLogicalType{}, pq.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	top := schema.FieldList{id, main}
	if c.multi {
		if c.twin {
			var other schema.Node = makeList("items")
			for i := c.depth - 1; i >= 0; i-- {
				name := fmt.Sprintf("node%d", i)
				if i == 0 {
					name = "node0.node1"
				}
				n, e := schema.NewGroupNode(name, opt, schema.FieldList{other}, -1)
				if e != nil {
					t.Fatal(e)
				}
				other = n
			}
			top = append(top, other)
		} else {
			top = append(top, makeList("other"))
		}
	}
	if c.mixed {
		text, _ := schema.NewPrimitiveNodeLogical("flat_text", req, schema.StringLogicalType{}, pq.Types.ByteArray, -1, -1)
		n, _ := schema.NewPrimitiveNodeLogical("n", req, schema.NoLogicalType{}, pq.Types.Int64, -1, -1)
		plain, _ := schema.NewGroupNode("plain.dot", opt, schema.FieldList{n}, -1)
		top = append(top, text, plain)
		for _, kind := range []bool{false, true} {
			name := "oldints"
			typ := pq.Types.Int64
			var logical schema.LogicalType = schema.NoLogicalType{}
			if kind {
				name = "oldtexts"
				typ = pq.Types.ByteArray
				logical = schema.StringLogicalType{}
			}
			el, _ := schema.NewPrimitiveNodeLogical("element", req, logical, typ, -1, -1)
			mid, _ := schema.NewGroupNode("list", pq.Repetitions.Repeated, schema.FieldList{el}, -1)
			outer, _ := schema.NewGroupNodeLogical(name, req, schema.FieldList{mid}, schema.ListLogicalType{}, -1)
			top = append(top, outer)
		}
	}
	root, e := schema.NewGroupNode("schema", req, top, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "liststruct.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]pq.WriterProperty{pq.WithDataPageSize(128), pq.WithBatchSize(5)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(props...)))
	base := 0
	for _, count := range groups {
		rg := w.AppendRowGroup()
		col, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		ids := make([]int64, count)
		for i := range ids {
			ids[i] = int64(base + i)
		}
		if _, e = col.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = col.Close(); e != nil {
			t.Fatal(e)
		}
		roots := 1
		if c.multi {
			roots = 2
		}
		for group := 0; group < roots; group++ {
			depth := c.depth
			if group == 1 && !c.twin {
				depth = 0
			}
			for ki := range kinds {
				col, e = rg.NextColumn()
				if e != nil {
					t.Fatal(e)
				}
				var defs, reps []int16
				var i32 []int32
				var i64 []int64
				var f32 []float32
				var f64 []float64
				var bv []bool
				var sv []pq.ByteArray
				emit := func(d, r int16, valid bool, row, item int) {
					defs = append(defs, d)
					reps = append(reps, r)
					if !valid {
						return
					}
					n := listStructReaderInteger(kinds[ki], row, item)
					i32 = append(i32, int32(n))
					i64 = append(i64, n)
					f32 = append(f32, float32(n)+.25)
					f64 = append(f64, float64(n)+.25)
					bv = append(bv, n%2 == 1)
					sv = append(sv, pq.ByteArray(listStructReaderPayload(c, row, item)))
				}
				for row := base; row < base+count; row++ {
					domainRow := row + c.ancestorShift
					if group == 1 && c.twin {
						domainRow++
					}
					a, visible := listStructReaderDomain(depth, domainRow)
					if !visible {
						emit(a, 0, false, row, 0)
						continue
					}
					d := a
					if c.L {
						if row%7 == 0 {
							emit(d, 0, false, row, 0)
							continue
						}
						d++
					}
					n := listStructReaderCount(c, row)
					if n == 0 {
						emit(d, 0, false, row, 0)
						continue
					}
					for j := 0; j < n; j++ {
						r := int16(0)
						if j > 0 {
							r = 1
						}
						dd := d + 1
						itemValid := !c.E || j%3 != 1
						if c.bad == "item_witness" && ki == len(kinds)-1 && row == 4 && (j == 0 || j == 1) {
							itemValid = !itemValid
						}
						if c.bad == "rep" && ki == len(kinds)-1 && row == 4 {
							if j == 0 {
								r = 1
							}
							if j == 1 {
								r = 0
							}
						}
						if !itemValid {
							emit(dd, r, false, row, j)
							continue
						}
						if c.E {
							dd++
						}
						childValid := !c.C || (row+j)%5 != 2
						if childValid && c.C {
							dd++
						}
						emit(dd, r, childValid, row, j)
					}
				}
				switch x := col.(type) {
				case *file.Int32ColumnChunkWriter:
					_, e = x.WriteBatch(i32, defs, reps)
				case *file.Int64ColumnChunkWriter:
					_, e = x.WriteBatch(i64, defs, reps)
				case *file.Float32ColumnChunkWriter:
					_, e = x.WriteBatch(f32, defs, reps)
				case *file.Float64ColumnChunkWriter:
					_, e = x.WriteBatch(f64, defs, reps)
				case *file.BooleanColumnChunkWriter:
					_, e = x.WriteBatch(bv, defs, reps)
				case *file.ByteArrayColumnChunkWriter:
					_, e = x.WriteBatch(sv, defs, reps)
				default:
					t.Fatalf("writer %T", col)
				}
				if e != nil {
					t.Fatal(e)
				}
				if e = col.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
		if c.mixed {
			for which := 0; which < 4; which++ {
				col, e = rg.NextColumn()
				if e != nil {
					t.Fatal(e)
				}
				var iv []int64
				var sv []pq.ByteArray
				defs, reps := make([]int16, count), make([]int16, count)
				for i := 0; i < count; i++ {
					iv = append(iv, int64(base+i+100))
					sv = append(sv, pq.ByteArray(fmt.Sprintf("flat%d", base+i)))
					defs[i] = 1
				}
				switch x := col.(type) {
				case *file.Int64ColumnChunkWriter:
					if which == 1 {
						_, e = x.WriteBatch(iv, defs, nil)
					} else {
						_, e = x.WriteBatch(iv, defs, reps)
					}
				case *file.ByteArrayColumnChunkWriter:
					if which == 0 {
						_, e = x.WriteBatch(sv, nil, nil)
					} else {
						_, e = x.WriteBatch(sv, defs, reps)
					}
				}
				if e != nil {
					t.Fatal(e)
				}
				if e = col.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
		if e = rg.Close(); e != nil {
			t.Fatal(e)
		}
		base += count
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func listStructReaderCheck(t *testing.T, b *memory.Batch, c listStructReaderConfig, start int) {
	t.Helper()
	for i := 0; i < b.Rows; i++ {
		row := start + i
		if c.mixed {
			if string(b.Strings["flat_text"].ValueBytes(i)) != fmt.Sprintf("flat%d", row) || b.Structs["plain.dot"].Values.Int64s["n"].Values[i] != int64(row+100) {
				t.Fatal("old scalar/Struct synchronization")
			}
			iv := b.Int64Lists["oldints"]
			lo, hi, _ := iv.ValueRange(i)
			if hi-lo != 1 || iv.Values.Values[lo] != int64(row+100) {
				t.Fatal("old Int64 List")
			}
			sv := b.StringLists["oldtexts"]
			lo, hi, _ = sv.ValueRange(i)
			if hi-lo != 1 || string(sv.Values.ValueBytes(lo)) != fmt.Sprintf("flat%d", row) {
				t.Fatal("old String List")
			}
		}
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("row sync")
		}
		roots := 1
		if c.multi {
			roots = 2
		}
		for group := 0; group < roots; group++ {
			current := b
			depth := c.depth
			name := "items"
			if group == 1 && !c.twin {
				depth = 0
				name = "other"
			}
			domainRow := row + c.ancestorShift
			if group == 1 && c.twin {
				domainRow++
			}
			for d := 0; d < depth; d++ {
				nodeName := fmt.Sprintf("node%d", d)
				if group == 1 && c.twin && d == 0 {
					nodeName = "node0.node1"
				}
				v := current.Structs[nodeName]
				_, present := listStructReaderDomain(d+1, domainRow)
				if v.IsValid(i) != present || v.Rows != b.Rows || v.Values.Rows != b.Rows {
					t.Fatal("ancestor/crop", row, d)
				}
				current = v.Values
			}
			v := current.StructLists[name]
			expected := listStructReaderKinds(c.all)
			if len(v.Values.Fields) != len(expected) {
				t.Fatal("item fields")
			}
			for j, k := range expected {
				f := v.Values.Fields[j]
				if f.Name != k.name || f.Type != k.typ || f.Nullable != c.C {
					t.Fatal("item metadata", f, k)
				}
			}
			_, present := listStructReaderDomain(depth, domainRow)
			visible := present && (!c.L || row%7 != 0)
			if v.IsValid(i) != visible {
				t.Fatal("List parent", row)
			}
			lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
			n := 0
			if visible {
				n = listStructReaderCount(c, row)
			}
			if hi-lo != n {
				t.Fatal("element span", row, lo, hi, n)
			}
			for j := 0; j < n; j++ {
				item := lo + j
				iv := !c.E || j%3 != 1
				if v.Values.IsValid(item) != iv {
					t.Fatal("item mask", row, j)
				}
				for _, k := range listStructReaderKinds(c.all) {
					valid := iv && (!c.C || (row+j)%5 != 2)
					var vv reflect.Value
					if k.typ == memory.TypeString {
						x := v.Values.Values.Strings[k.name]
						if x.IsValid(item) != valid {
							t.Fatal("string NULL")
						}
						if valid && string(x.ValueBytes(item)) != listStructReaderPayload(c, row, j) {
							t.Fatal("string value")
						}
						continue
					}
					switch k.vector {
					case "Int8s":
						vv = reflect.ValueOf(v.Values.Values.Int8s[k.name])
					case "Int16s":
						vv = reflect.ValueOf(v.Values.Values.Int16s[k.name])
					case "Int32s":
						vv = reflect.ValueOf(v.Values.Values.Int32s[k.name])
					case "Int64s":
						vv = reflect.ValueOf(v.Values.Values.Int64s[k.name])
					case "Uint8s":
						vv = reflect.ValueOf(v.Values.Values.Uint8s[k.name])
					case "Uint16s":
						vv = reflect.ValueOf(v.Values.Values.Uint16s[k.name])
					case "Uint32s":
						vv = reflect.ValueOf(v.Values.Values.Uint32s[k.name])
					case "Uint64s":
						vv = reflect.ValueOf(v.Values.Values.Uint64s[k.name])
					case "Float32s":
						vv = reflect.ValueOf(v.Values.Values.Float32s[k.name])
					case "Float64s":
						vv = reflect.ValueOf(v.Values.Values.Float64s[k.name])
					}
					bitmap := vv.Elem().FieldByName("Validity").Interface().(memory.Bitmap)
					if (bitmap == nil || memory.IsSet(bitmap, item)) != valid {
						t.Fatal("child NULL", k.name, row, j)
					}
					if !valid {
						continue
					}
					got := vv.Elem().FieldByName("Values").Index(item)
					n := listStructReaderInteger(k, row, j)
					switch got.Kind() {
					case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						if got.Int() != n*k.scale {
							t.Fatal("integer", k.name, got, n)
						}
					case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						want := uint64(n)
						if k.typ == memory.TypeBool {
							want = uint64(n % 2)
						}
						if got.Uint() != want {
							t.Fatal("unsigned", k.name)
						}
					case reflect.Float32, reflect.Float64:
						if got.Float() != float64(n)+.25 {
							t.Fatal("float", k.name)
						}
					}
				}
			}
			if v.Values.Rows != int(v.Offsets[b.Rows]) || v.Values.Values.Rows != v.Values.Rows {
				t.Fatal("element axis crop")
			}
		}
	}
}
func TestNativeNestedListStructDecodeMatrix(t *testing.T) {
	for _, depth := range []int{0, 7} {
		for mask := 0; mask < 8; mask++ {
			for _, version := range []pq.DataPageVersion{pq.DataPageV1, pq.DataPageV2} {
				t.Run(fmt.Sprintf("depth%d/optional%d/page%d", depth, mask, version), func(t *testing.T) {
					c := listStructReaderConfig{depth: depth, L: mask&1 != 0, E: mask&2 != 0, C: mask&4 != 0, all: true, multi: true}
					path := listStructReaderWrite(t, c, []int{13, 0, 24}, pq.WithDataPageVersion(version), pq.WithDictionaryDefault(mask%2 == 0))
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}

					m := memory.NewBudgetManager(0)
					opts := testReadOptions()
					opts.BatchRows = 5
					opts.MemoryManager = m
					st, e := src.NewBatchStream(context.Background(), opts)
					if e != nil {
						t.Fatal(e)
					}
					defer st.Close()
					seen := 0
					for {
						b := memory.NewBatch()
						ok, e := st.Next(context.Background(), b)
						if e != nil {
							t.Fatal(e)
						}
						if !ok {
							break
						}
						if e := memory.ValidateStructListsBatch(b, src.Schema(), nil); e != nil {
							t.Fatal("full output storage/domain validation", e)
						}
						listStructReaderCheck(t, b, c, seen)
						seen += b.Rows
					}
					if seen != 37 {
						t.Fatal("rows", seen)
					}
					st.Close()
					if m.Used() != 0 {
						t.Fatal("leases", m.Used())
					}
				})
			}
		}
	}
}
func TestNativeNestedListStructWitnessMismatch(t *testing.T) {
	for _, bad := range []string{"item_witness", "rep"} {
		t.Run(bad, func(t *testing.T) {
			c := listStructReaderConfig{L: true, E: true, C: true, bad: bad}
			path := listStructReaderWrite(t, c, []int{9})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}

			opts := testReadOptions()
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			var got error
			for {
				ok, e := st.Next(context.Background(), memory.NewBatch())
				if e != nil {
					got = e
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			if got == nil || m.Used() != 0 {
				t.Fatal("mismatched grouped leaves", got, m.Used())
			}
		})
	}
}

func TestNativeNestedListStructScopeAndSnapshot(t *testing.T) {
	c := listStructReaderConfig{depth: 7, L: true, E: true, C: true, multi: true}
	path := listStructReaderWrite(t, c, []int{20})
	for _, o := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, o); e == nil {
			t.Fatal("old scope admitted ListStruct", o)
		}
	}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	original := src.Schema()
	changed := src.Schema()
	changed.Fields[1].Children[0].Name = "poison"
	if !reflect.DeepEqual(src.Schema().Fields, original.Fields) {
		t.Fatal("Schema leaked mutable children")
	}
	projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id", "other"}})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), projected, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	seen := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if len(b.Structs) != 0 || len(b.StructLists) != 1 {
			t.Fatal("projected earlier tree retained")
		}
		v := b.StructLists["other"]
		for i := 0; i < b.Rows; i++ {
			row := seen + i
			if b.Int64s["id"].Values[i] != int64(row) || v.IsValid(i) != (row%7 != 0) {
				t.Fatal("projected row/domain")
			}
			n := 0
			if row%7 != 0 {
				n = listStructReaderCount(c, row)
			}
			if int(v.Offsets[i+1]-v.Offsets[i]) != n {
				t.Fatal("projected elements")
			}
		}
		seen += b.Rows
	}
	st.Close()
	if seen != 20 || m.Used() != 0 {
		t.Fatal("projection cleanup", seen, m.Used())
	}
	bad := c
	bad.depth = 8
	path = listStructReaderWrite(t, bad, []int{1})
	if _, e := OpenFileWithOptions(path, Options{NativeNested: true}); e == nil {
		t.Fatal("item-inclusive depth9 admitted")
	}
}
func TestNativeNestedListStructPageReality(t *testing.T) {
	for _, version := range []pq.DataPageVersion{pq.DataPageV1, pq.DataPageV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			c := listStructReaderConfig{L: true, E: true, C: true, long: true}
			path := listStructReaderWrite(t, c, []int{37}, pq.WithDataPageVersion(version), pq.WithDictionaryDefault(false))
			r, e := file.OpenParquetFile(path, false)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			for _, col := range []int{1, 2} {
				pages, e := r.RowGroup(0).GetColumnPageReader(col)
				if e != nil {
					t.Fatal(e)
				}
				count, max := 0, int32(0)
				for pages.Next() {
					p := pages.Page()
					if p.Type() == file.PageTypeDataPage || p.Type() == file.PageTypeDataPageV2 {
						count++
						if p.NumValues() > max {
							max = p.NumValues()
						}
					}
				}
				if e := pages.Err(); e != nil {
					t.Fatal(e)
				}
				pages.Close()
				t.Logf("version=%v leaf=%d actual pages=%d maxlevels=%d", version, col, count, max)
				if count < 2 {
					t.Fatal("fixture not multipage")
				}
				if version == pq.DataPageV1 && max >= 4097 {
					t.Fatal("V1 fixture does not prove giant row crossing pages")
				}
				if version == pq.DataPageV2 && max < 4097 {
					t.Fatal("unexpected V2 row split")
				}
			}
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 5
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			seen := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				listStructReaderCheck(t, b, c, seen)
				seen += b.Rows
			}
			st.Close()
			if seen != 37 || m.Used() != 0 {
				t.Fatal("pages cleanup", seen, m.Used())
			}
		})
	}
}
func TestNativeNestedListStructFooterCounts(t *testing.T) {
	for _, col := range []int{1, 2} {
		for _, delta := range []int64{-1, 1} {
			t.Run(fmt.Sprint(col, delta), func(t *testing.T) {
				c := listStructReaderConfig{L: true, E: true, C: true}
				path := listStructReaderWrite(t, c, []int{9})
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				src.meta.RowGroups[0].Columns[col].MetaData.NumValues += delta
				footer, e := src.meta.Serialize(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				raw, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				old := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
				raw = append(raw[:len(raw)-8-old], footer...)
				var n [4]byte
				binary.LittleEndian.PutUint32(n[:], uint32(len(footer)))
				raw = append(raw, n[:]...)
				raw = append(raw, []byte("PAR1")...)
				if e = os.WriteFile(path, raw, 0600); e != nil {
					t.Fatal(e)
				}
				src, e = OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					return
				}
				opts := testReadOptions()
				m := memory.NewBudgetManager(0)
				opts.MemoryManager = m
				st, e := src.NewBatchStream(context.Background(), opts)
				if e != nil {
					if m.Used() != 0 {
						t.Fatal("open error leaked")
					}
					return
				}
				var got error
				for {
					ok, e := st.Next(context.Background(), memory.NewBatch())
					if e != nil {
						got = e
						break
					}
					if !ok {
						break
					}
				}
				st.Close()
				if got == nil || m.Used() != 0 {
					t.Fatal("footer mismatch", got, m.Used())
				}
			})
		}
	}
}

func listStructReaderPayload(c listStructReaderConfig, row, item int) string {
	if c.large && row == 4 && item == 2 {
		return "xx" + strings.Repeat("界", 44000)
	}
	return listStructReaderText(row, item)
}
func TestNativeNestedListStructReserveFailures(t *testing.T) {
	c := listStructReaderConfig{L: true, E: true, C: true, multi: true}
	path := listStructReaderWrite(t, c, []int{9})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	run := func(m *fixedTestFailManager) (int, error) {
		opts := testReadOptions()
		opts.BatchRows = 4
		opts.MemoryManager = m
		st, e := src.NewBatchStream(context.Background(), opts)
		if e != nil {
			return 0, e
		}
		defer st.Close()
		seen := 0
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				return seen, e
			}
			if !ok {
				return seen, nil
			}
			listStructReaderCheck(t, b, c, seen)
			seen += b.Rows
		}
	}
	probe := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
	seen, e := run(probe)
	if e != nil || seen != 9 || probe.Used() != 0 {
		t.Fatal("calibration", seen, e, probe.Used())
	}
	calls := probe.calls
	t.Logf("actual Reserve sequence %d", calls)
	for fail := 1; fail <= calls; fail++ {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			m := &fixedTestFailManager{Manager: memory.NewBudgetManager(0), fail: fail}
			seen, e := run(m)
			if m.calls < fail || m.Used() != 0 || (e != nil && !errors.Is(e, memory.ErrBudgetExceeded)) {
				t.Fatal("injected reserve", fail, seen, e, m.calls, m.Used())
			}
			if e == nil && seen != 9 {
				t.Fatal("prefix recovery lost rows", seen)
			}
			retry := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
			seen, e = run(retry)
			if e != nil || seen != 9 || retry.Used() != 0 {
				t.Fatal("fresh retry", seen, e, retry.Used())
			}
		})
	}
}
func TestNativeNestedListStructBudgetAndEarlyClose(t *testing.T) {
	c := listStructReaderConfig{L: true, E: true, C: true, large: true}
	path := listStructReaderWrite(t, c, []int{9})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, limit := range []int64{1, 32 << 10} {
		m := memory.NewBudgetManager(limit)
		opts := testReadOptions()
		opts.BatchRows = 9
		opts.MemoryManager = m
		st, e := src.NewBatchStream(context.Background(), opts)
		if e == nil {
			for {
				ok, err := st.Next(context.Background(), memory.NewBatch())
				if err != nil {
					e = err
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
		}
		if e == nil || m.Used() != 0 {
			t.Fatal("tight budget", limit, e, m.Used())
		}
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 2
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	listStructReaderCheck(t, b, c, 0)
	st.Close()
	st.Close()
	if m.Used() != 0 {
		t.Fatal("early close leak")
	}
	ctx, cancel := context.WithCancel(context.Background())
	st, e = src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	ok, e = st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || m.Used() != 0 {
		t.Fatal("cancel", ok, e, m.Used())
	}
}

type listStructReaderFaultManager struct {
	memory.Manager
	native         *nestedListStream
	candidate, hit bool
	count          int
	pending        string
}

func (m *listStructReaderFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		ns := m.native
		if !m.candidate && ns.current == 1 {
			c := ns.cursors[1]
			g := c.group
			if g != nil && g.count(&g.stage) >= 8 && g.count(&g.output) > 0 && !c.complete {
				m.hit = true
				m.count = g.count(&g.stage)
				m.pending = string(g.leaves[len(g.leaves)-1].bytes[0])
				return nil, memory.ErrBudgetExceeded
			}
		}
		if m.candidate && ns.current == len(ns.cursors) && ns.cursors[0].replacementLease != nil {
			m.hit = true
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeNestedListStructPartialAndCandidateRetry(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		t.Run(fmt.Sprint(candidate), func(t *testing.T) {
			c := listStructReaderConfig{L: true, E: true, C: true, multi: true, long: true}
			path := listStructReaderWrite(t, c, []int{15}, pq.WithDataPageVersion(pq.DataPageV1))
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			m := &listStructReaderFaultManager{Manager: memory.NewBudgetManager(0), candidate: candidate}
			opts := testReadOptions()
			opts.BatchRows = 20
			opts.BatchBytes = 8 << 20
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			native, ok := st.(*nestedListStream)
			if !ok {
				t.Fatalf("not shared coordinator: %T", st)
			}
			m.native = native
			b := memory.NewBatch()
			ok, e = st.Next(context.Background(), b)
			if e != nil || !ok || !m.hit || b.Rows == 15 {
				t.Fatal("did not produce recoverable prefix", ok, e, m.hit, b.Rows)
			}
			listStructReaderCheck(t, b, c, 0)
			seen := b.Rows
			if candidate {
				for _, cursor := range native.cursors {
					if cursor.replacementLease != nil || cursor.group.candidate.vector != nil {
						t.Fatal("candidate rollback retained replacement")
					}
				}
			}
			if !candidate {
				g := native.cursors[1].group
				if g.count(&g.stage) != m.count || !native.cursors[0].complete || string(g.leaves[len(g.leaves)-1].bytes[0]) != m.pending {
					t.Fatal("partial/borrowed state changed on prefix")
				}
			}
			for {
				b = memory.NewBatch()
				ok, e = st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				listStructReaderCheck(t, b, c, seen)
				seen += b.Rows
			}
			st.Close()
			if seen != 15 || m.Used() != 0 {
				t.Fatal("retry rows/leases", seen, m.Used())
			}
		})
	}
}

type listStructReaderCopyContext struct {
	context.Context
	native   *nestedListStream
	checks   int
	canceled bool
	charge   int64
}

func (c *listStructReaderCopyContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	if c.native == nil {
		return c.Context.Err()
	}
	active := false
	for _, cur := range c.native.cursors {
		if cur.group != nil {
			for _, l := range cur.group.leaves {
				if len(l.bytes[0]) > 65536 {
					active = true
				}
			}
		}
	}
	if !active {
		return c.Context.Err()
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "stringListStream).copyBytes") {
			c.checks++
			if c.checks == 2 {
				c.canceled = true
				c.charge = c.native.base.opts.MemoryManager.Used()
				return context.Canceled
			}
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}
func TestNativeNestedListStructActualCopyCancel(t *testing.T) {
	c := listStructReaderConfig{L: true, E: true, C: true, multi: true, large: true}
	path := listStructReaderWrite(t, c, []int{9})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 9
	opts.BatchBytes = 8 << 20
	opts.MemoryManager = m
	ctx := &listStructReaderCopyContext{Context: context.Background()}
	st, e := src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	ctx.native = st.(*nestedListStream)
	ok, e := st.Next(ctx, memory.NewBatch())
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || ctx.charge <= 0 || m.Used() != 0 {
		t.Fatal("actual grouped String chunk cancel", ok, e, ctx.checks, ctx.charge, m.Used())
	}
	st, e = src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	seen := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		listStructReaderCheck(t, b, c, seen)
		seen += b.Rows
	}
	st.Close()
	if seen != 9 || m.Used() != 0 {
		t.Fatal("copy retry", seen, m.Used())
	}
}

func TestNativeNestedListStructMixedOldPayload(t *testing.T) {
	c := listStructReaderConfig{depth: 7, L: true, E: true, C: true, multi: true, mixed: true}
	path := listStructReaderWrite(t, c, []int{3, 0, 17})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := src.NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	seen := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		listStructReaderCheck(t, b, c, seen)
		seen += b.Rows
	}
	st.Close()
	if seen != 20 || m.Used() != 0 {
		t.Fatal("mixed cleanup")
	}
}

func TestNativeNestedListStructMetadataExactAndEmpty(t *testing.T) {
	previous := int64(0)
	for _, all := range []bool{false, true} {
		c := listStructReaderConfig{depth: 7, L: true, E: true, C: true, all: all, multi: true}
		path := listStructReaderWrite(t, c, []int{0})
		src, e := OpenFileWithOptions(path, Options{NativeNested: true})
		if e != nil {
			t.Fatal(e)
		}
		opts := testReadOptions()
		probe := memory.NewBudgetManager(0)
		opts.MemoryManager = probe
		st, e := src.NewBatchStream(context.Background(), opts)
		if e != nil {
			t.Fatal(e)
		}
		charge := probe.Used()
		st.Close()
		if charge <= previous || probe.Used() != 0 {
			t.Fatal("group metadata width charge", charge, previous)
		}
		previous = charge
		for _, delta := range []int64{0, -1} {
			m := memory.NewBudgetManager(charge + delta)
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if delta < 0 {
				if e == nil {
					st.Close()
					t.Fatal("metadata minus1 accepted")
				}
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(e)
				}
			} else {
				if e != nil {
					t.Fatal("exact metadata", e)
				}
				st.Close()
			}
			if m.Used() != 0 {
				t.Fatal("metadata cleanup")
			}
		}
		opts.MemoryManager = probe
		st, e = src.NewBatchStream(context.Background(), opts)
		if e != nil {
			t.Fatal(e)
		}
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			if b.Rows != 0 {
				t.Fatal("empty file yielded rows")
			}
		}
		st.Close()
		if probe.Used() != 0 {
			t.Fatal("empty cleanup")
		}
	}
}

func listStructReaderInteger(k fixedTestKind, row, item int) int64 {
	if row == 4 && item == 2 {
		switch k.typ {
		case memory.TypeInt8:
			return -128
		case memory.TypeInt16:
			return -32768
		case memory.TypeInt32:
			return -2147483648
		case memory.TypeInt64:
			return -9223372036854775808
		case memory.TypeUint8:
			return 255
		case memory.TypeUint16:
			return 65535
		case memory.TypeUint32:
			return 4294967295
		case memory.TypeUint64:
			return -1
		case memory.TypeDate32:
			return -1
		case memory.TypeTimestamp:
			return -9223372036854775808
		}
	}
	n := listStructReaderValue(row, item)
	switch k.typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeDate32, memory.TypeTimestamp:
		if row%2 == 1 {
			return -n
		}
	}
	return n
}

func TestNativeNestedListStructJointHeaderPeak(t *testing.T) {
	var single int64
	for _, multi := range []bool{false, true} {
		c := listStructReaderConfig{L: true, E: true, C: true, all: true, multi: multi}
		path := listStructReaderWrite(t, c, []int{1})
		src, e := OpenFileWithOptions(path, Options{NativeNested: true})
		if e != nil {
			t.Fatal(e)
		}
		run := func(m memory.Manager) error {
			opts := testReadOptions()
			opts.BatchRows = 1
			opts.MemoryManager = m
			st, e := src.NewBatchStream(context.Background(), opts)
			if e != nil {
				return e
			}
			defer st.Close()
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				return e
			}
			if !ok {
				t.Fatal("missing NULL-list row")
			}
			listStructReaderCheck(t, b, c, 0)
			return nil
		}
		m := newBatchStreamPeakManager(0)
		if e = run(m); e != nil || m.Used() != 0 {
			t.Fatal("peak probe", e, m.Used())
		}
		peak := m.peak.Load()
		if !multi {
			single = peak
		} else if peak <= single {
			t.Fatal("second group omitted from global peak", peak, single)
		}
		t.Logf("multi=%v exact NULL-row managed peak=%d", multi, peak)
		exact := memory.NewBudgetManager(peak)
		if e = run(exact); e != nil || exact.Used() != 0 {
			t.Fatal("exact peak", e, exact.Used())
		}
		short := memory.NewBudgetManager(peak - 1)
		if e = run(short); e == nil || short.Used() != 0 {
			t.Fatal("peak minus1", e, short.Used())
		}
	}
}

type listStructReaderTransferContext struct {
	context.Context
	native  *nestedListStream
	vectors []*memory.StructVector
	leases  []memory.Reservation
}

func (c *listStructReaderTransferContext) Err() error {
	if c.native == nil || len(c.vectors) != 0 {
		return c.Context.Err()
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	inCommit := false
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "nestedListStream).commit") {
			inCommit = true
			break
		}
		if !more {
			break
		}
	}
	if !inCommit {
		return c.Context.Err()
	}
	for _, cur := range c.native.cursors {
		g := cur.group
		if g == nil || !cur.complete || g.count(&g.output) != 0 || g.count(&g.stage) == 0 || g.stage.lease == nil {
			return c.Context.Err()
		}
	}
	for _, cur := range c.native.cursors {
		c.vectors = append(c.vectors, cur.group.stage.vector)
		c.leases = append(c.leases, cur.group.stage.lease)
	}
	return c.Context.Err()
}
func TestNativeNestedListStructCompleteBundleTransfer(t *testing.T) {
	c := listStructReaderConfig{all: true, multi: true}
	path := listStructReaderWrite(t, c, []int{2})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.BatchRows = 1
	opts.MemoryManager = m
	ctx := &listStructReaderTransferContext{Context: context.Background()}
	st, e := src.NewBatchStream(ctx, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	ctx.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if e != nil || !ok {
		t.Fatal(ok, e)
	}
	if len(ctx.vectors) != 2 {
		t.Fatal("did not capture complete paid stage bundles")
	}
	for i, cur := range ctx.native.cursors {
		g := cur.group
		if g.output.vector != ctx.vectors[i] || g.output.lease != ctx.leases[i] || g.vector.Values != ctx.vectors[i] || g.stage.vector != nil || g.stage.lease != nil {
			t.Fatal("whole bundle not transferred exactly once", i)
		}
	}
	listStructReaderCheck(t, b, c, 0)
	st.Close()
	if m.Used() != 0 {
		t.Fatal("transferred bundle cleanup")
	}
}

func TestNativeNestedListStructDeepRootProjectionIdentity(t *testing.T) {
	c := listStructReaderConfig{depth: 7, L: true, E: true, C: true, all: true, multi: true, twin: true}
	path := listStructReaderWrite(t, c, []int{13, 24})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, project := range []bool{false, true} {
		t.Run(fmt.Sprint(project), func(t *testing.T) {
			var scan source.Source = src
			if project {
				scan, _, e = src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"id", "node0.node1"}})
				if e != nil {
					t.Fatal(e)
				}
			}
			opts := testReadOptions()
			opts.BatchRows = 5
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), scan, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			seen := 0
			for {
				b := memory.NewBatch()
				ok, e := st.Next(context.Background(), b)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if !project {
					listStructReaderCheck(t, b, c, seen)
				} else {
					if len(b.Structs) != 1 || b.Structs["node0.node1"] == nil {
						t.Fatal("wrong retained root")
					}
					alias := *b
					alias.Structs = map[string]*memory.StructVector{"node0": b.Structs["node0.node1"]}
					oracle := c
					oracle.multi = false
					oracle.twin = false
					oracle.ancestorShift = 1
					listStructReaderCheck(t, &alias, oracle, seen)
				}
				seen += b.Rows
			}
			st.Close()
			if seen != 37 || m.Used() != 0 {
				t.Fatal("deep projection cleanup", seen, m.Used())
			}
		})
	}
}
