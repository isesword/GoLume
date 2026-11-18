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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type listNativeRow struct {
	valid  bool
	values []int64
	valids []bool
}

func listNativeExpected(row, large int, outer, item bool) listNativeRow {
	r := listNativeRow{valid: true}
	switch row % 7 {
	case 0:
		r.valid = !outer
	case 1:
	case 2:
		if item {
			r.values = []int64{0}
			r.valids = []bool{false}
		} else {
			r.values = []int64{0}
			r.valids = []bool{true}
		}
	case 3:
		r.values = []int64{math.MinInt64, math.MaxInt64, 0, -1}
		r.valids = []bool{true, true, true, true}
	case 4:
		for i := 0; i < large; i++ {
			r.values = append(r.values, int64(row*1000+i))
			r.valids = append(r.valids, !item || i%5 != 1)
		}
	default:
		r.values = []int64{int64(row), -int64(row)}
		r.valids = []bool{true, true}
	}
	return r
}

// corrupt1: first continuation;2: empty placeholder then continuation.
func listNativeFixture(t *testing.T, groups []int, large int, outer, item bool, corrupt int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	er, lr := req, req
	if item {
		er = opt
	}
	if outer {
		lr = opt
	}
	element, e := schema.NewPrimitiveNodeLogical("element", er, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{element}, -1)
	if e != nil {
		t.Fatal(e)
	}
	list, e := schema.NewGroupNodeLogical("items", lr, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
	if e != nil {
		t.Fatal(e)
	}
	id, e := schema.NewPrimitiveNodeLogical("id", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{id, list}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "list.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		c, e := rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		idn := n
		if corrupt != 0 {
			idn--
		}
		ids := make([]int64, idn)
		for i := range ids {
			ids[i] = int64(base + i)
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
		c, e = rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		defs, reps := []int16{}, []int16{}
		values := []int64{}
		L, E := int16(0), int16(0)
		if outer {
			L = 1
		}
		if item {
			E = 1
		}
		for i := 0; i < n; i++ {
			row := listNativeExpected(base+i, large, outer, item)
			if !row.valid {
				defs = append(defs, 0)
				reps = append(reps, 0)
				continue
			}
			if len(row.values) == 0 {
				defs = append(defs, L)
				reps = append(reps, 0)
				continue
			}
			for j, v := range row.values {
				rep := int16(0)
				if j > 0 {
					rep = 1
				}
				d := L + 1
				if row.valids[j] {
					d += E
					values = append(values, v)
				}
				defs = append(defs, d)
				reps = append(reps, rep)
			}
		}
		if corrupt == 1 && len(reps) > 0 {
			reps[0] = 1
		}
		if corrupt == 2 && len(reps) > 1 {
			reps[1] = 1
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, reps); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
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
func listNativeCheck(t *testing.T, b *memory.Batch, want []int, large int, outer, item bool) {
	t.Helper()
	v := b.Int64Lists["items"]
	if v == nil || len(v.Offsets) != b.Rows+1 || v.Offsets[0] != 0 {
		t.Fatal("native list shape")
	}
	for i, row := range want {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("fixed sibling order")
		}
		r := listNativeExpected(row, large, outer, item)
		if v.IsValid(i) != r.valid {
			t.Fatal("parent validity", row)
		}
		lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
		if hi-lo != len(r.values) {
			t.Fatal("list length", row, hi-lo, len(r.values))
		}
		for j, x := range r.values {
			if v.Values.IsValid(lo+j) != r.valids[j] {
				t.Fatal("element NULL", row, j)
			}
			if !r.valids[j] {
				x = 0
			}
			if v.Values.Values[lo+j] != x {
				t.Fatal("exact element", row, j)
			}
		}
	}
}
func listNativeCollect(t *testing.T, st source.BatchStream, opts source.ReadOptions, want []int, large int, outer, item bool) {
	t.Helper()
	defer st.Close()
	cursor := 0
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if b.Rows < 1 || b.Rows > opts.BatchRows || memory.BatchBytes(b) > opts.BatchBytes {
			t.Fatal("batch bounds")
		}
		if cursor+b.Rows > len(want) {
			t.Fatal("duplicate parent")
		}
		listNativeCheck(t, b, want[cursor:cursor+b.Rows], large, outer, item)
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("missing parent", cursor, len(want))
	}
}
func TestNativeInt64ListDecodeMatrix(t *testing.T) {
	for _, outer := range []bool{false, true} {
		for _, item := range []bool{false, true} {
			for _, dict := range []bool{false, true} {
				for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
					t.Run(fmt.Sprintf("outer%v/item%v/dict%v/v%d", outer, item, dict, version), func(t *testing.T) {
						path := listNativeFixture(t, []int{0, 17, 66}, 257, outer, item, 0, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
						src, e := OpenFileWithOptions(path, Options{NativeNested: true})
						if e != nil {
							t.Fatal(e)
						}
						field := src.Schema().Fields[1]
						if field.Type != memory.TypeList || field.Nullable != outer || field.Children[0].Type != memory.TypeInt64 || field.Children[0].Nullable != item {
							t.Fatal("List metadata")
						}
						for _, rows := range []int{1, 7, 65} {
							opts := testReadOptions()
							opts.BatchRows = rows
							opts.BatchBytes = 64 << 10
							m := memory.NewBudgetManager(0)
							opts.MemoryManager = m
							st, e := source.OpenBatchStream(context.Background(), src, opts)
							if e != nil {
								t.Fatal(e)
							}
							want := make([]int, 83)
							for i := range want {
								want[i] = i
							}
							listNativeCollect(t, st, opts, want, 257, outer, item)
							if m.Used() != 0 {
								t.Fatal("EOF leak")
							}
						}
					})
				}
			}
		}
	}
}
func TestNativeInt64ListPartitionsScope(t *testing.T) {
	path := listNativeFixture(t, []int{3, 5, 7}, 17, true, true, 0)
	for _, opts := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, opts); e == nil {
			t.Fatal("old/exclusive scope", opts)
		}
	}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 2
	opts.BatchBytes = 64 << 10
	for _, subset := range [][]source.Partition{nil, {parts[2], parts[0]}} {
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, subset)
		if e != nil {
			t.Fatal(e)
		}
		want := []int{}
		if len(subset) > 0 {
			want = []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}
		}
		listNativeCollect(t, st, opts, want, 17, true, true)
	}
}
func TestNativeInt64ListAdmissionCancelAndTooLarge(t *testing.T) {
	path := listNativeFixture(t, []int{17, 19}, 257, true, true, 0)
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
		cursor := 0
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				return e
			}
			if !ok {
				if cursor != 36 {
					t.Fatal("recovery missing row", cursor)
				}
				return nil
			}
			want := make([]int, b.Rows)
			for i := range want {
				want[i] = cursor + i
			}
			listNativeCheck(t, b, want, 257, true, true)
			cursor += b.Rows
		}
	}
	probe := &fixedTestFailManager{Manager: memory.NewBudgetManager(0)}
	if e = run(probe); e != nil {
		t.Fatal(e)
	}
	for fail := 1; fail <= probe.calls; fail++ {
		m := &fixedTestFailManager{Manager: memory.NewBudgetManager(0), fail: fail}
		if e = run(m); e != nil && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("stage admission", fail, e)
		}
		if m.Used() != 0 {
			t.Fatal("failure leak")
		}
		m.fail = 0
		if e = run(m); e != nil {
			t.Fatal(e)
		}
		if m.Used() != 0 {
			t.Fatal("retry leak")
		}
	}
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	ctx := &fixedTestCancelContext{Context: context.Background(), armed: true}
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 40 || b.Rows != 0 {
		t.Fatal("mid-level cancellation", ok, e, ctx.checks, b.Rows)
	}
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
	path = listNativeFixture(t, []int{5}, 1<<16, true, true, 0)
	src, e = OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	st, e = source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
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
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("huge list must reject", e)
	}
	if m.Used() != 0 {
		t.Fatal("huge cleanup")
	}
}
func TestNativeInt64ListMalformedLevels(t *testing.T) {
	for _, kind := range []int{1, 2} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			path := listNativeFixture(t, []int{5}, 4, true, true, kind)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				return
			}
			opts := testReadOptions()
			opts.BatchRows = 7
			opts.BatchBytes = 64 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e == nil {
				_, e = st.Next(context.Background(), memory.NewBatch())
				st.Close()
			}
			if e == nil {
				t.Fatal("malformed repetitions accepted")
			}
			if m.Used() != 0 {
				t.Fatal("malformed leak")
			}
		})
	}
}
func TestNativeInt64ListLevelCountMetadata(t *testing.T) {
	for _, delta := range []int64{-1, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			path := listNativeFixture(t, []int{5}, 17, true, true, 0)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			md := src.meta.RowGroups[0].Columns[1].MetaData
			if md.NumValues <= 5 {
				t.Fatal("fixture must have more levels than parent rows")
			}
			md.NumValues += delta
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
			opts.BatchRows = 2
			opts.BatchBytes = 64 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
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
			if e == nil {
				t.Fatal("wrong level count accepted")
			}
			if m.Used() != 0 {
				t.Fatal("metadata cleanup")
			}
		})
	}
}
func TestNativeInt64ListRejectedSchemas(t *testing.T) {
	for _, kind := range []string{"unsigned", "string", "legacy", "two_lists", "struct_sibling"} {
		t.Run(kind, func(t *testing.T) {
			logical, physical := schema.LogicalType(schema.NewIntLogicalType(64, true)), arrowparquet.Types.Int64
			if kind == "unsigned" {
				logical = schema.NewIntLogicalType(64, false)
			}
			if kind == "string" {
				logical = schema.StringLogicalType{}
				physical = arrowparquet.Types.ByteArray
			}
			rep := arrowparquet.Repetitions.Optional
			if kind == "legacy" {
				rep = arrowparquet.Repetitions.Repeated
			}
			el, e := schema.NewPrimitiveNodeLogical("element", rep, logical, physical, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			var child schema.Node = el
			if kind != "legacy" {
				child, e = schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
				if e != nil {
					t.Fatal(e)
				}
			}
			list, e := schema.NewGroupNodeLogical("items", arrowparquet.Repetitions.Optional, schema.FieldList{child}, schema.ListLogicalType{}, -1)
			if e != nil {
				t.Fatal(e)
			}
			fields := schema.FieldList{list}
			if kind == "two_lists" {
				other, e := schema.NewGroupNodeLogical("other", arrowparquet.Repetitions.Optional, schema.FieldList{child}, schema.ListLogicalType{}, -1)
				if e != nil {
					t.Fatal(e)
				}
				fields = append(fields, other)
			}
			if kind == "struct_sibling" {
				other, e := schema.NewGroupNode("record", arrowparquet.Repetitions.Optional, schema.FieldList{el}, -1)
				if e != nil {
					t.Fatal(e)
				}
				fields = append(fields, other)
			}
			root, e := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
			if e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(t.TempDir(), "scope.parquet")
			f, e := os.Create(path)
			if e != nil {
				t.Fatal(e)
			}
			w := file.NewParquetWriter(f, root)
			if e = w.Close(); e != nil {
				t.Fatal(e)
			}
			_, e = OpenFileWithOptions(path, Options{NativeNested: true})
			supported := kind == "string" || kind == "two_lists" || kind == "struct_sibling"
			if supported && e != nil {
				t.Fatal("bounded forest shape rejected", e)
			}
			if !supported && e == nil {
				t.Fatal("unsupported schema accepted")
			}
		})
	}
}

type listNativeStageFailure struct {
	memory.Manager
	native *nestedListCursor
	hit    bool
}

func (m *listNativeStageFailure) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit && len(m.native.stage) >= 8 && m.native.output != nil && len(m.native.output.Values.Values) > 0 {
		m.hit = true
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeInt64ListPartialRowResume(t *testing.T) {
	path := listNativeFixture(t, []int{17}, 257, true, true, 0)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 17
	opts.BatchBytes = 64 << 10
	m := &listNativeStageFailure{Manager: memory.NewBudgetManager(0)}
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	m.native = st.(*nestedListStream).cursors[0]
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil || !m.hit || b.Rows != 4 || len(m.native.stage) != 8 || m.native.complete || !m.native.tuple.present || m.native.stageLease == nil {
		t.Fatal("partial row was not retained", ok, e, m.hit, b.Rows, len(m.native.stage), m.native.complete)
	}
	listNativeCheck(t, b, []int{0, 1, 2, 3}, 257, true, true)
	want := make([]int, 13)
	for i := range want {
		want[i] = i + 4
	}
	listNativeCollect(t, st, opts, want, 257, true, true)
	if m.Used() != 0 {
		t.Fatal("partial cleanup")
	}
}

type listNativeObserveContext struct {
	context.Context
	native     *nestedListCursor
	lease      memory.Reservation
	ptr        *int64
	cancelCopy bool
	checks     int
}

func (c *listNativeObserveContext) Err() error {
	if len(c.native.stage) >= 4097 {
		c.lease = c.native.stageLease
		c.ptr = &c.native.stage[0]
	}
	if c.cancelCopy && len(c.native.stage) >= 2048 {
		var pcs [24]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".copyElements") {
				c.checks++
				if c.checks == 2 {
					return context.Canceled
				}
				break
			}
			if !more {
				break
			}
		}
	}
	return c.Context.Err()
}
func TestNativeInt64ListStageTransferAndCopyCancel(t *testing.T) {
	path := listNativeFixture(t, []int{4, 1}, 4097, true, true, 0)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 65
	opts.BatchBytes = 256 << 10
	for _, cancelCopy := range []bool{false, true} {
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		native := st.(*nestedListStream).cursors[0]
		ctx := &listNativeObserveContext{Context: context.Background(), native: native, cancelCopy: cancelCopy}
		b := memory.NewBatch()
		ok, e := st.Next(ctx, b)
		if cancelCopy {
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
				t.Fatal("stage copy cancellation", ok, e, ctx.checks)
			}
		} else {
			if !ok || e != nil {
				t.Fatal(e)
			}
			listNativeCheck(t, b, []int{4}, 4097, true, true)
			if ctx.lease == nil || native.outputLease != ctx.lease || native.stageLease != nil || native.stage != nil || &native.output.Values.Values[0] != ctx.ptr {
				t.Fatal("first row did not transfer exact stage lease/buffer")
			}
			if ok, e = st.Next(context.Background(), memory.NewBatch()); ok || e != nil {
				t.Fatal("last-group lookahead EOF", ok, e)
			}
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("transfer/cancel leak")
		}
	}
}
func TestNativeInt64ListProjectionAndPayloadTarget(t *testing.T) {
	path := listNativeFixture(t, []int{17}, 257, true, true, 0)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 65
	opts.BatchBytes = 64 << 10
	opts.BatchPayloadBytes = 8 << 10
	opts.BatchPayloadTargetBytes = 128
	for _, name := range []string{"items", "id"} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{name}})
		if e != nil {
			t.Fatal(e)
		}
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), projected, opts)
		if e != nil {
			t.Fatal(e)
		}
		row := 0
		for {
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			if name == "id" {
				if len(b.Int64Lists) != 0 {
					t.Fatal("projection leaked List")
				}
				for i := 0; i < b.Rows; i++ {
					if b.Int64s["id"].Values[i] != int64(row+i) {
						t.Fatal("id projection order")
					}
				}
			} else {
				if len(b.Int64s) != 0 {
					t.Fatal("projection leaked fixed sibling")
				}
				ids := make([]int64, b.Rows)
				want := make([]int, b.Rows)
				for i := range ids {
					ids[i] = int64(row + i)
					want[i] = row + i
				}
				b.Int64s["id"] = &memory.Int64Vector{Values: ids}
				listNativeCheck(t, b, want, 257, true, true)
			}
			row += b.Rows
		}
		st.Close()
		if row != 17 || m.Used() != 0 {
			t.Fatal("projection count/cleanup", row, m.Used())
		}
	}
}
