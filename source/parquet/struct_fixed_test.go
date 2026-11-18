package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type fixedTestKind struct {
	name     string
	typ      memory.DataType
	physical arrowparquet.Type
	logical  schema.LogicalType
	vector   string
	scale    int64
}

func fixedTestKinds() []fixedTestKind {
	return []fixedTestKind{
		{"i8", memory.TypeInt8, arrowparquet.Types.Int32, schema.NewIntLogicalType(8, true), "Int8s", 1},
		{"i16", memory.TypeInt16, arrowparquet.Types.Int32, schema.NewIntLogicalType(16, true), "Int16s", 1},
		{"i32", memory.TypeInt32, arrowparquet.Types.Int32, schema.NewIntLogicalType(32, true), "Int32s", 1},
		{"i64", memory.TypeInt64, arrowparquet.Types.Int64, schema.NewIntLogicalType(64, true), "Int64s", 1},
		{"u8", memory.TypeUint8, arrowparquet.Types.Int32, schema.NewIntLogicalType(8, false), "Uint8s", 1},
		{"u16", memory.TypeUint16, arrowparquet.Types.Int32, schema.NewIntLogicalType(16, false), "Uint16s", 1},
		{"u32", memory.TypeUint32, arrowparquet.Types.Int32, schema.NewIntLogicalType(32, false), "Uint32s", 1},
		{"u64", memory.TypeUint64, arrowparquet.Types.Int64, schema.NewIntLogicalType(64, false), "Uint64s", 1},
		{"f32", memory.TypeFloat32, arrowparquet.Types.Float, schema.NoLogicalType{}, "Float32s", 1},
		{"f64", memory.TypeFloat64, arrowparquet.Types.Double, schema.NoLogicalType{}, "Float64s", 1},
		{"bool", memory.TypeBool, arrowparquet.Types.Boolean, schema.NoLogicalType{}, "Uint32s", 1},
		{"date", memory.TypeDate32, arrowparquet.Types.Int32, schema.DateLogicalType{}, "Int32s", 1},
		{"ms", memory.TypeTimestamp, arrowparquet.Types.Int64, schema.NewTimestampLogicalType(true, schema.TimeUnitMillis), "Int64s", 1000000},
		{"us", memory.TypeTimestamp, arrowparquet.Types.Int64, schema.NewTimestampLogicalType(true, schema.TimeUnitMicros), "Int64s", 1000},
		{"ns", memory.TypeTimestamp, arrowparquet.Types.Int64, schema.NewTimestampLogicalType(true, schema.TimeUnitNanos), "Int64s", 1},
	}
}
func fixedTestWrite(t *testing.T, optional bool, groups []int, props ...arrowparquet.WriterProperty) string {
	return fixedTestWriteVariant(t, optional, groups, false, props...)
}
func fixedTestWriteVariant(t *testing.T, optional bool, groups []int, inconsistent bool, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	kinds := fixedTestKinds()
	children := schema.FieldList{}
	for j, k := range kinds {
		rep := arrowparquet.Repetitions.Required
		if j%2 == 0 {
			rep = arrowparquet.Repetitions.Optional
		}
		node, e := schema.NewPrimitiveNodeLogical(k.name, rep, k.logical, k.physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		children = append(children, node)
	}
	rep := arrowparquet.Repetitions.Required
	if optional {
		rep = arrowparquet.Repetitions.Optional
	}
	parent, e := schema.NewGroupNode("record", rep, children, -1)
	if e != nil {
		t.Fatal(e)
	}
	id, e := schema.NewPrimitiveNodeLogical("id", arrowparquet.Repetitions.Required, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	root, e := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{id, parent}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "fixed.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(64), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		ids := make([]int64, n)
		for r := range ids {
			ids[r] = int64(base + r)
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
		for j := range kinds {
			c, e = rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			defs := make([]int16, n)
			var i32 []int32
			var i64 []int64
			var f32 []float32
			var f64 []float64
			var bs []bool
			for r := 0; r < n; r++ {
				row := base + r
				pv := !optional || row%5 != 0
				if inconsistent && j == 1 && row == 0 {
					pv = true
				}
				cv := j%2 != 0 || row%3 != 1
				if !pv {
					continue
				}
				if optional {
					defs[r]++
				}
				if !cv {
					continue
				}
				if j%2 == 0 {
					defs[r]++
				}
				v := int64(row%50 + 1)
				i32 = append(i32, int32(v))
				i64 = append(i64, v)
				f32 = append(f32, float32(v)+0.25)
				f64 = append(f64, float64(v)+0.25)
				bs = append(bs, row%2 == 0)
			}
			switch col := c.(type) {
			case *file.Int32ColumnChunkWriter:
				_, e = col.WriteBatch(i32, defs, nil)
			case *file.Int64ColumnChunkWriter:
				_, e = col.WriteBatch(i64, defs, nil)
			case *file.Float32ColumnChunkWriter:
				_, e = col.WriteBatch(f32, defs, nil)
			case *file.Float64ColumnChunkWriter:
				_, e = col.WriteBatch(f64, defs, nil)
			case *file.BooleanColumnChunkWriter:
				_, e = col.WriteBatch(bs, defs, nil)
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = c.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if e = rg.Close(); e != nil {
			t.Fatal(e)
		}
		base += n
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func fixedTestCheck(t *testing.T, b *memory.Batch, optional bool, wantIDs []int) {
	t.Helper()
	if b.Rows != len(wantIDs) {
		t.Fatal("rows")
	}
	v := b.Structs["record"]
	if v == nil {
		t.Fatal("native Struct missing")
	}
	for i, row := range wantIDs {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("partition/order", i, row)
		}
		pv := !optional || row%5 != 0
		if v.IsValid(i) != pv {
			t.Fatal("parent validity", row)
		}
		for j, k := range fixedTestKinds() {
			ptr := reflect.ValueOf(v.Values).Elem().FieldByName(k.vector).MapIndex(reflect.ValueOf(k.name))
			if !ptr.IsValid() {
				t.Fatal("child missing", k.name)
			}
			valid := ptr.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(i)})[0].Bool()
			wantValid := pv && (j%2 != 0 || row%3 != 1)
			if valid != wantValid {
				t.Fatal("child validity", k.name, row)
			}
			if !wantValid {
				continue
			}
			x := ptr.Elem().FieldByName("Values").Index(i)
			value := int64(row%50 + 1)
			switch k.typ {
			case memory.TypeFloat32, memory.TypeFloat64:
				if x.Float() != float64(value)+0.25 {
					t.Fatal("float", k.name, row)
				}
			case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeBool:
				want := uint64(value)
				if k.typ == memory.TypeBool {
					want = 0
					if row%2 == 0 {
						want = 1
					}
				}
				if x.Uint() != want {
					t.Fatal("unsigned/bool", k.name, row)
				}
			default:
				if x.Int() != value*k.scale {
					t.Fatal("signed/date/timestamp", k.name, row, x.Int())
				}
			}
		}
	}
}
func fixedTestCollect(t *testing.T, stream source.BatchStream, opts source.ReadOptions, optional bool, want []int) {
	t.Helper()
	defer stream.Close()
	cursor := 0
	for {
		b := memory.NewBatch()
		ok, e := stream.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if b.Rows > opts.BatchRows || memory.BatchBytes(b) > opts.BatchBytes {
			t.Fatal("batch bounds", b.Rows, memory.BatchBytes(b))
		}
		if cursor+b.Rows > len(want) {
			t.Fatal("extra rows")
		}
		fixedTestCheck(t, b, optional, want[cursor:cursor+b.Rows])
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("lost rows", cursor, len(want))
	}
}
func TestNativeFixedStructParquetDecode(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, dict := range []bool{false, true} {
			for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
				t.Run(fmt.Sprintf("optional%v/dict%v/v%d", optional, dict, version), func(t *testing.T) {
					path := fixedTestWrite(t, optional, []int{0, 67, 130}, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					sf := src.Schema().Fields
					if len(sf) != 2 || sf[1].Type != memory.TypeStruct || sf[1].Nullable != optional {
						t.Fatal("schema")
					}
					for j, k := range fixedTestKinds() {
						f := sf[1].Children[j]
						if f.Type != k.typ || f.Nullable != (j%2 == 0) {
							t.Fatal("child metadata", k.name)
						}
					}
					for _, rows := range []int{1, 63, 65} {
						m := memory.NewBudgetManager(0)
						opts := testReadOptions()
						opts.BatchRows = rows
						opts.BatchBytes = 64 << 10
						opts.MemoryManager = m
						stream, e := source.OpenBatchStream(context.Background(), src, opts)
						if e != nil {
							t.Fatal(e)
						}
						want := make([]int, 197)
						for i := range want {
							want[i] = i
						}
						fixedTestCollect(t, stream, opts, optional, want)
						if m.Used() != 0 {
							t.Fatal("close leak")
						}
					}
				})
			}
		}
	}
}
func TestNativeFixedStructParquetPartitionsAndResources(t *testing.T) {
	path := fixedTestWrite(t, true, []int{3, 5, 7})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, subset := range [][]source.Partition{nil, {parts[2], parts[0]}} {
		opts := testReadOptions()
		opts.BatchRows = 2
		opts.BatchBytes = 64 << 10
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, subset)
		if e != nil {
			t.Fatal(e)
		}
		want := []int{}
		if len(subset) > 0 {
			want = []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}
		}
		fixedTestCollect(t, st, opts, true, want)
		if m.Used() != 0 {
			t.Fatal("prepared leak")
		}
	}
	for _, bytes := range []int64{1, 40960, 65536} {
		opts := testReadOptions()
		opts.BatchRows = 1000
		opts.BatchBytes = bytes
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if bytes == 1 {
			if e == nil {
				_, e = st.Next(context.Background(), memory.NewBatch())
				st.Close()
			}
			if e == nil {
				t.Fatal("single row limit not enforced")
			}
		} else {
			if e != nil {
				t.Fatal(e)
			}
			want := make([]int, 15)
			for i := range want {
				want[i] = i
			}
			fixedTestCollect(t, st, opts, true, want)
		}
		if m.Used() != 0 {
			t.Fatal("byte limit leak")
		}
	}
	opts := testReadOptions()
	opts.BatchRows = 2
	opts.BatchBytes = 64 << 10
	reject := &rejectBatchStreamManager{}
	opts.MemoryManager = reject
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e == nil {
		_, e = st.Next(context.Background(), memory.NewBatch())
		st.Close()
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("budget admission", e)
	}
	for _, cancelBefore := range []bool{false, true} {
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if !cancelBefore {
			ok, e := st.Next(ctx, memory.NewBatch())
			if !ok || e != nil {
				t.Fatal(e)
			}
		}
		cancel()
		if _, e = st.Next(ctx, memory.NewBatch()); !errors.Is(e, context.Canceled) {
			t.Fatal("cancel", e)
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("cancel close leak")
		}
	}
	for _, compressed := range []bool{false, true} {
		opts := testReadOptions()
		opts.BatchRows = 2
		opts.BatchBytes = 64 << 10
		if compressed {
			opts.MaxCompressedPageBytes = 1
		} else {
			opts.MaxUncompressedPageBytes = 1
		}
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e == nil {
			_, e = st.Next(context.Background(), memory.NewBatch())
			st.Close()
		}
		if e == nil {
			t.Fatal("page cap ignored")
		}
		if m.Used() != 0 {
			t.Fatal("page cap leak")
		}
	}
}
func TestNativeFixedStructParquetRejectUnsupported(t *testing.T) {
	nodes := []schema.Node{}
	for _, tc := range []struct {
		name     string
		logical  schema.LogicalType
		physical arrowparquet.Type
	}{{"string", schema.StringLogicalType{}, arrowparquet.Types.ByteArray}, {"decimal", schema.NewDecimalLogicalType(5, 2), arrowparquet.Types.Int32}, {"time", schema.NewTimeLogicalType(true, schema.TimeUnitMillis), arrowparquet.Types.Int32}, {"local_ts", schema.NewTimestampLogicalType(false, schema.TimeUnitNanos), arrowparquet.Types.Int64}} {
		n, e := schema.NewPrimitiveNodeLogical(tc.name, arrowparquet.Repetitions.Optional, tc.logical, tc.physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		nodes = append(nodes, n)
	}
	leaf, _ := schema.NewPrimitiveNodeLogical("x", arrowparquet.Repetitions.Required, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	// NativeNested now admits depth 2..8. The enclosing record adds one
	// level, so retain the recursive rejection at depth 9 instead.
	var nested schema.Node = leaf
	for depth := 0; depth < 8; depth++ {
		n, err := schema.NewGroupNode("nested", arrowparquet.Repetitions.Optional, schema.FieldList{nested}, -1)
		if err != nil {
			t.Fatal(err)
		}
		nested = n
	}
	nodes = append(nodes, nested)
	for _, node := range nodes {
		t.Run(node.Name(), func(t *testing.T) {
			record, _ := schema.NewGroupNode("record", arrowparquet.Repetitions.Optional, schema.FieldList{node}, -1)
			root, _ := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{record}, -1)
			path := filepath.Join(t.TempDir(), "unsupported.parquet")
			f, e := os.Create(path)
			if e != nil {
				t.Fatal(e)
			}
			w := file.NewParquetWriter(f, root)
			if e = w.Close(); e != nil {
				t.Fatal(e)
			}
			_, e = OpenFileWithOptions(path, Options{NativeNested: true})
			if node.Name() == "string" {
				if e != nil {
					t.Fatal("bounded UTF8 child rejected", e)
				}
			} else if e == nil {
				t.Fatal("unsupported opt-in accepted")
			}
		})
	}
	path := fixedTestWrite(t, true, []int{3})
	if _, e := OpenFile(path); e == nil {
		t.Fatal("default unexpectedly widened")
	}
	if _, e := OpenFileWithOptions(path, Options{}); e == nil {
		t.Fatal("zero options unexpectedly widened")
	}
}

func TestNativeFixedStructParquetInconsistentParent(t *testing.T) {
	path := fixedTestWriteVariant(t, true, []int{3}, true)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.BatchBytes = 64 << 10
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	_, e = st.Next(context.Background(), memory.NewBatch())
	st.Close()
	if e == nil {
		t.Fatal("inconsistent sibling parent definition accepted")
	}
	if m.Used() != 0 {
		t.Fatal("malformed cleanup")
	}
}

type fixedTestFailManager struct {
	memory.Manager
	calls, fail int
}

func (m *fixedTestFailManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}

type fixedTestCancelContext struct {
	context.Context
	checks int
	armed  bool
}

func (c *fixedTestCancelContext) Err() error {
	if c.armed {
		c.checks++
		if c.checks >= 40 {
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestNativeFixedStructParquetAdmissionAndRunningCancel(t *testing.T) {
	path := fixedTestWrite(t, true, []int{17, 19})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 7
	opts.BatchBytes = 64 << 10
	run := func(m memory.Manager) error {
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e != nil {
			return e
		}
		defer st.Close()
		for {
			ok, e := st.Next(context.Background(), memory.NewBatch())
			if e != nil {
				return e
			}
			if !ok {
				return nil
			}
		}
	}
	probe := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
	if e = run(probe); e != nil {
		t.Fatal(e)
	}
	for fail := 1; fail <= probe.calls; fail++ {
		m := &fixedTestFailManager{Manager: memory.NewBudgetManager(0), fail: fail}
		if e = run(m); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("injected admission", fail, e)
		}
		if m.Used() != 0 {
			t.Fatal("failed lease leak")
		}
		m.fail = 0
		if e = run(m); e != nil {
			t.Fatal("fresh stream retry", e)
		}
		if m.Used() != 0 {
			t.Fatal("retry leak")
		}
	}
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	ctx := &fixedTestCancelContext{Context: context.Background()}
	st, e := source.OpenBatchStream(ctx, src, opts)
	if e != nil {
		t.Fatal(e)
	}
	ctx.armed = true
	dst := memory.NewBatch()
	ok, e := st.Next(ctx, dst)
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 40 || dst.Rows != 0 {
		t.Fatal("mid-decode cancel", ok, e, ctx.checks, dst.Rows)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("mid-decode leak")
	}
	opts.MemoryManager = m
	st, e = source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e = st.Next(context.Background(), memory.NewBatch()); !ok || e != nil {
		t.Fatal(e)
	}
	st.Close()
	st.Close()
	if m.Used() != 0 {
		t.Fatal("early/double close leak")
	}
}

func TestNativeFixedStructParquetSchemaSnapshot(t *testing.T) {
	path := fixedTestWrite(t, true, []int{3})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	exposed := src.Schema()
	exposed.Fields[0].Name = "poison_id"
	exposed.Fields[1].Children[0].Name = "poison_child"
	exposed.Fields[1].Children[0].Type = memory.TypeString
	if src.Schema().Fields[0].Name != "id" || src.Schema().Fields[1].Children[0].Name != "i8" || src.Schema().Fields[1].Children[0].Type != memory.TypeInt8 {
		t.Fatal("Schema exposes mutable source fields/children")
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.BatchBytes = 64 << 10
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	fixedTestCollect(t, st, opts, true, []int{0, 1, 2})
}
func TestNativeFixedStructParquetNumValuesMismatch(t *testing.T) {
	for _, count := range []int64{2, 4} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			path := fixedTestWrite(t, true, []int{3})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			// Rewrite a real on-disk footer using Arrow's public metadata serializer.
			// Page bytes/offsets and row-group row count remain unchanged. Only one
			// nested primitive's declared number of levels changes from3 to2 or4.
			src.meta.RowGroups[0].Columns[1].MetaData.NumValues = count
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
			var size [4]byte
			binary.LittleEndian.PutUint32(size[:], uint32(len(footer)))
			raw = append(raw, size[:]...)
			raw = append(raw, []byte("PAR1")...)
			if e = os.WriteFile(path, raw, 0600); e != nil {
				t.Fatal(e)
			}
			src, e = OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				return
			}
			opts := testReadOptions()
			opts.BatchRows = 3
			opts.BatchBytes = 64 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e == nil {
				_, e = st.Next(context.Background(), memory.NewBatch())
				st.Close()
			}
			if e == nil {
				t.Fatal("mismatched leaf NumValues accepted", count)
			}
			if m.Used() != 0 {
				t.Fatal("count mismatch leak")
			}
		})
	}
}
