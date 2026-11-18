package parquet

import (
	"context"
	"errors"
	"fmt"
	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type nestedTestKind struct {
	name                string
	text, outer, item   bool
	shift, count, bytes int
}

// Values are generated independently of the decoder; levels encode the row model,
// not another decoded result. Different shifts give each leaf different boundaries.
func nestedTestFixture(t *testing.T, kinds []nestedTestKind, groups []int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	id, e := schema.NewPrimitiveNodeLogical("id", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	fields := schema.FieldList{id}
	for _, k := range kinds {
		lr, er := req, req
		if k.outer {
			lr = opt
		}
		if k.item {
			er = opt
		}
		var el *schema.PrimitiveNode
		if k.text {
			el, e = schema.NewPrimitiveNodeLogical("element", er, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
		} else {
			el, e = schema.NewPrimitiveNodeLogical("element", er, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
		}
		if e != nil {
			t.Fatal(e)
		}
		middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
		if e != nil {
			t.Fatal(e)
		}
		node, e := schema.NewGroupNodeLogical(k.name, lr, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		fields = append(fields, node)
	}
	root, e := schema.NewGroupNode("schema", req, fields, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "mixed.parquet")
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
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(base + i)
		}
		if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e = c.Close(); e != nil {
			t.Fatal(e)
		}
		for _, k := range kinds {
			c, e = rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			var defs, reps []int16
			var ints []int64
			var texts []arrowparquet.ByteArray
			ld, ed := int16(0), int16(0)
			if k.outer {
				ld = 1
			}
			if k.item {
				ed = 1
			}
			for i := 0; i < n; i++ {
				valid := false
				var valids []bool
				var iv []int64
				var sv []string
				if k.text {
					r := listStringExpected(base+i+k.shift, k.count, k.bytes, k.outer, k.item)
					valid, valids, sv = r.valid, r.valids, r.values
				} else {
					r := listNativeExpected(base+i+k.shift, k.count, k.outer, k.item)
					valid, valids, iv = r.valid, r.valids, r.values
				}
				if !valid {
					defs = append(defs, 0)
					reps = append(reps, 0)
					continue
				}
				if len(valids) == 0 {
					defs = append(defs, ld)
					reps = append(reps, 0)
					continue
				}
				for j, ok := range valids {
					d, r := ld+1, int16(0)
					if j > 0 {
						r = 1
					}
					if ok {
						d += ed
						if k.text {
							texts = append(texts, arrowparquet.ByteArray(sv[j]))
						} else {
							ints = append(ints, iv[j])
						}
					}
					defs = append(defs, d)
					reps = append(reps, r)
				}
			}
			if k.text {
				_, e = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(texts, defs, reps)
			} else {
				_, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(ints, defs, reps)
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

func nestedTestCheck(t *testing.T, b *memory.Batch, kinds []nestedTestKind, want []int) {
	t.Helper()
	for i, row := range want {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("fixed row order", i, row)
		}
	}
	for _, k := range kinds {
		for i, row := range want {
			if k.text {
				r := listStringExpected(row+k.shift, k.count, k.bytes, k.outer, k.item)
				v := b.StringLists[k.name]
				if v == nil || len(v.Offsets) != b.Rows+1 || v.IsValid(i) != r.valid {
					t.Fatal("String list shape/NULL", k.name, row)
				}
				lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
				if hi-lo != len(r.values) {
					t.Fatal("String length", k.name, row)
				}
				for j, x := range r.values {
					if v.Values.IsValid(lo+j) != r.valids[j] {
						t.Fatal("String child NULL", k.name, row, j)
					}
					if !r.valids[j] {
						x = ""
					}
					if string(v.Values.ValueBytes(lo+j)) != x {
						t.Fatal("String bytes", k.name, row, j)
					}
				}
			} else {
				r := listNativeExpected(row+k.shift, k.count, k.outer, k.item)
				v := b.Int64Lists[k.name]
				if v == nil || len(v.Offsets) != b.Rows+1 || v.IsValid(i) != r.valid {
					t.Fatal("Int64 list shape/NULL", k.name, row)
				}
				lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
				if hi-lo != len(r.values) {
					t.Fatal("Int64 length", k.name, row)
				}
				for j, x := range r.values {
					if v.Values.IsValid(lo+j) != r.valids[j] {
						t.Fatal("Int64 child NULL", k.name, row, j)
					}
					if !r.valids[j] {
						x = 0
					}
					if v.Values.Values[lo+j] != x {
						t.Fatal("Int64 value", k.name, row, j)
					}
				}
			}
		}
	}
}

func TestNativeNestedMixedIndependentLevels(t *testing.T) {
	layouts := [][]nestedTestKind{
		{{"text", true, true, true, 0, 17, 0}, {"ints", false, false, true, 2, 129, 0}},
		{{"first", true, true, true, 0, 3, 0}, {"ints", false, true, false, 2, 257, 0}, {"last", true, false, true, 4, 129, 0}},
		{{"first", false, false, true, 4, 129, 0}, {"text", true, true, false, 2, 31, 0}, {"last", false, true, true, 0, 257, 0}},
	}
	for l, kinds := range layouts {
		for _, dict := range []bool{false, true} {
			for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
				t.Run(fmt.Sprintf("layout%d/dict%v/page%d", l, dict, version), func(t *testing.T) {
					path := nestedTestFixture(t, kinds, []int{0, 17, 66}, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					for _, rows := range []int{1, 7, 65} {
						opts := testReadOptions()
						opts.BatchRows = rows
						opts.BatchBytes = 256 << 10
						m := memory.NewBudgetManager(0)
						opts.MemoryManager = m
						st, e := source.OpenBatchStream(context.Background(), src, opts)
						if e != nil {
							t.Fatal(e)
						}
						cursor := 0
						for {
							b := memory.NewBatch()
							ok, e := st.Next(context.Background(), b)
							if e != nil {
								st.Close()
								t.Fatal(e)
							}
							if !ok {
								break
							}
							if b.Rows < 1 || b.Rows > rows || cursor+b.Rows > 83 || memory.BatchBytes(b) > opts.BatchBytes {
								t.Fatal("batch bounds")
							}
							want := make([]int, b.Rows)
							for i := range want {
								want[i] = cursor + i
							}
							nestedTestCheck(t, b, kinds, want)
							cursor += b.Rows
						}
						if e = st.Close(); e != nil {
							t.Fatal(e)
						}
						if cursor != 83 || m.Used() != 0 {
							t.Fatal("lost rows/leak", cursor, m.Used())
						}
					}
				})
			}
		}
	}
}

// Fail a particular coordinator reservation, not an arbitrary reader allocation.
type nestedTestFaultManager struct {
	memory.Manager
	native         *nestedListStream
	stage, ordinal int
	hit            bool
	pending        string
	count          int
}

func (m *nestedTestFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		s := m.native
		if m.stage >= 0 && s.current == m.stage {
			c := s.cursors[m.stage]
			if c.stageCount() >= 8 && c.outputCount() > 0 {
				m.hit = true
				m.pending = string(c.bytes[0])
				m.count = c.stageCount()
				return nil, memory.ErrBudgetExceeded
			}
		}
		if m.ordinal > 0 && s.current == len(s.cursors) {
			leases := 0
			for _, c := range s.cursors {
				if c.replacementLease != nil {
					leases++
				}
			}
			if leases == m.ordinal-1 {
				m.hit = true
				return nil, memory.ErrBudgetExceeded
			}
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeNestedAtomicMixedRecovery(t *testing.T) {
	kinds := []nestedTestKind{{"first", false, true, true, 0, 129, 0}, {"second", true, true, true, 0, 129, 0}, {"last", true, true, true, 0, 129, 0}}
	for _, tc := range []struct {
		name           string
		stage, ordinal int
	}{{"second_partial", 1, 0}, {"third_partial", 2, 0}, {"second_replacement", -1, 2}, {"third_replacement", -1, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			path := nestedTestFixture(t, kinds, []int{17})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			m := &nestedTestFaultManager{Manager: memory.NewBudgetManager(0), stage: tc.stage, ordinal: tc.ordinal}
			opts := testReadOptions()
			opts.BatchRows = 17
			opts.BatchBytes = 256 << 10
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			m.native = st.(*nestedListStream)
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if !ok || e != nil || !m.hit || b.Rows < 1 || b.Rows >= 17 {
				t.Fatal("fault did not publish prefix", ok, e, m.hit, b.Rows)
			}
			want := make([]int, b.Rows)
			for i := range want {
				want[i] = i
			}
			nestedTestCheck(t, b, kinds, want)
			cursor := b.Rows
			if tc.stage >= 0 {
				c := m.native.cursors[tc.stage]
				if c.complete || !c.tuple.present || c.stageCount() != m.count || string(c.bytes[0]) != m.pending {
					t.Fatal("partial borrowed state lost")
				}
				for i, p := range m.native.cursors {
					if i < tc.stage && !p.complete {
						t.Fatal("previous column incomplete")
					}
					if i > tc.stage && p.mode != 0 {
						t.Fatal("future column consumed")
					}
				}
			} else {
				for _, c := range m.native.cursors {
					if !c.complete || c.replacementLease != nil {
						t.Fatal("replacement rollback lost complete row")
					}
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
				want = make([]int, b.Rows)
				for i := range want {
					want[i] = cursor + i
				}
				nestedTestCheck(t, b, kinds, want)
				cursor += b.Rows
			}
			if cursor != 17 {
				t.Fatal("lost/repeated rows", cursor)
			}
			st.Close()
			if m.Used() != 0 {
				t.Fatal("lease leak", m.Used())
			}
		})
	}
}

type nestedTestCopyContext struct {
	context.Context
	native *nestedListStream
	checks int
}

func (c *nestedTestCopyContext) Err() error {
	s := c.native
	if s.current == 2 && len(s.cursors[2].bytes[0]) > 64<<10 && s.cursors[0].complete && s.cursors[1].complete {
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			f, more := frames.Next()
			if strings.HasSuffix(f.Function, ".copyBytes") {
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
func TestNativeNestedLaterCopyCancelAndHardRow(t *testing.T) {
	kinds := []nestedTestKind{{"ints", false, true, true, 0, 17, 0}, {"first", true, true, true, 0, 17, 0}, {"last", true, true, true, 0, 17, 131073}}
	path := nestedTestFixture(t, kinds, []int{4, 1})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 1
	opts.BatchBytes = 1 << 20
	m := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = m
	st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
	if e != nil {
		t.Fatal(e)
	}
	ctx := &nestedTestCopyContext{Context: context.Background(), native: st.(*nestedListStream)}
	b := memory.NewBatch()
	ok, e := st.Next(ctx, b)
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
		t.Fatal("later copy cancellation", ok, e, ctx.checks, b.Rows)
	}
	st.Close()
	if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
		t.Fatal("peak/cleanup")
	}
	// Each list is individually small; the aggregate logical row must obey one cap.
	kinds = []nestedTestKind{{"ints", false, false, false, 0, 257, 0}, {"first", true, false, false, 0, 257, 0}, {"last", true, false, false, 0, 257, 0}}
	path = nestedTestFixture(t, kinds, []int{4, 1})
	src, e = OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e = src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	opts.BatchPayloadBytes = 4096
	m = newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = m
	st, e = source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
	if e != nil {
		t.Fatal(e)
	}
	ok, e = st.Next(context.Background(), memory.NewBatch())
	if ok || !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("aggregate hard limit", ok, e)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("hard limit leak")
	}
}
func TestNativeNestedScopeAndPartitionOrder(t *testing.T) {
	kinds := []nestedTestKind{{"strings", true, true, true, 0, 17, 0}, {"ints", false, true, true, 2, 31, 0}, {"more", true, false, true, 4, 3, 0}}
	path := nestedTestFixture(t, kinds, []int{3, 5, 7})
	for _, o := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, o); e == nil {
			t.Fatal("old scope/flag conflict accepted", o)
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
	opts.BatchRows = 3
	opts.BatchBytes = 128 << 10
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[2], parts[0]})
	if e != nil {
		t.Fatal(e)
	}
	want := []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}
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
		if cursor+b.Rows > len(want) {
			t.Fatal("extra rows")
		}
		nestedTestCheck(t, b, kinds, want[cursor:cursor+b.Rows])
		cursor += b.Rows
	}
	st.Close()
	if cursor != len(want) || m.Used() != 0 {
		t.Fatal("partition order/cleanup")
	}
}

func TestNativeNestedUnionEntrypoints(t *testing.T) {
	for _, kind := range []string{"struct_string", "struct_list"} {
		t.Run(kind, func(t *testing.T) {
			var path string
			if kind == "struct_string" {
				path = stringStructFixture(t, []int{3, 14}, 31)
			} else {
				path = structListFixture(t, []int{3, 14}, 129, true, true, true, false)
			}
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 3
			opts.BatchBytes = 128 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
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
				want := make([]int, b.Rows)
				for i := range want {
					want[i] = cursor + i
				}
				if kind == "struct_string" {
					stringStructCheck(t, b, want, 31)
				} else {
					structListCheck(t, b, want, 129, true, true, true)
				}
				cursor += b.Rows
			}
			st.Close()
			if cursor != 17 || m.Used() != 0 {
				t.Fatal("union rows/lease", cursor, m.Used())
			}
			if kind == "struct_string" {
				path = stringStructFixtureVariant(t, []int{17}, 31, true)
			} else {
				path = structListFixture(t, []int{17}, 129, true, true, true, true)
			}
			src, e = OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			st, e = source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for {
				ok, err := st.Next(context.Background(), memory.NewBatch())
				if err != nil {
					if !strings.Contains(err.Error(), "parent") {
						t.Fatal("wrong malformed error", err)
					}
					found = true
					break
				}
				if !ok {
					break
				}
			}
			st.Close()
			if !found || m.Used() != 0 {
				t.Fatal("malformed union accepted/leak", found, m.Used())
			}
		})
	}
}
func TestNativeNestedHomogeneousEntrypoints(t *testing.T) {
	for _, kinds := range [][]nestedTestKind{
		{{"a", true, true, true, 0, 17, 0}, {"b", true, false, true, 4, 129, 0}},
		{{"a", false, true, true, 0, 17, 0}, {"b", false, false, true, 4, 129, 0}},
		{{"a", true, true, true, 0, 17, 0}}, {{"a", false, true, true, 0, 17, 0}},
	} {
		t.Run(fmt.Sprintf("count%d/string%v", len(kinds), kinds[0].text), func(t *testing.T) {
			path := nestedTestFixture(t, kinds, []int{17})
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			if len(kinds) == 2 && kinds[0].text {
				for _, o := range []Options{{}} {
					if _, e = OpenFileWithOptions(path, o); e == nil {
						t.Fatal("old scope broadened")
					}
				}
			}
			opts := testReadOptions()
			opts.BatchRows = 7
			opts.BatchBytes = 128 << 10
			m := memory.NewBudgetManager(0)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
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
				want := make([]int, b.Rows)
				for i := range want {
					want[i] = cursor + i
				}
				nestedTestCheck(t, b, kinds, want)
				cursor += b.Rows
			}
			st.Close()
			if cursor != 17 || m.Used() != 0 {
				t.Fatal("homogeneous rows/leak")
			}
		})
	}
}

func TestNativeNestedFixedStructAndUnsupportedNesting(t *testing.T) {
	path := fixedTestWrite(t, true, []int{3, 14})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.BatchBytes = 128 << 10
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	want := make([]int, 17)
	for i := range want {
		want[i] = i
	}
	fixedTestCollect(t, st, opts, true, want)
	if m.Used() != 0 {
		t.Fatal("fixed leak")
	}
	for _, mode := range []string{"struct_string_list", "struct_plus_top_list"} {
		t.Run(mode, func(t *testing.T) {
			req := arrowparquet.Repetitions.Required
			var el *schema.PrimitiveNode
			if mode == "struct_string_list" {
				el, e = schema.NewPrimitiveNodeLogical("element", req, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
			} else {
				el, e = schema.NewPrimitiveNodeLogical("element", req, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
			}
			if e != nil {
				t.Fatal(e)
			}
			middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
			if e != nil {
				t.Fatal(e)
			}
			list, e := schema.NewGroupNodeLogical("items", req, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
			if e != nil {
				t.Fatal(e)
			}
			child, e := schema.NewPrimitiveNodeLogical("n", req, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
			if e != nil {
				t.Fatal(e)
			}
			children := schema.FieldList{child}
			if mode == "struct_string_list" {
				children = append(children, list)
			}
			record, e := schema.NewGroupNode("record", req, children, -1)
			if e != nil {
				t.Fatal(e)
			}
			fields := schema.FieldList{record}
			if mode == "struct_plus_top_list" {
				fields = append(fields, list)
			}
			root, e := schema.NewGroupNode("schema", req, fields, -1)
			if e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(t.TempDir(), "unsupported.parquet")
			f, e := os.Create(path)
			if e != nil {
				t.Fatal(e)
			}
			w := file.NewParquetWriter(f, root)
			if e = w.Close(); e != nil {
				t.Fatal(e)
			}
			expanded, err := OpenFileWithOptions(path, Options{NativeNested: true})
			if err != nil {
				t.Fatal("new one-level forest scope", err)
			}
			if len(expanded.Schema().Fields) != len(fields) {
				t.Fatal("expanded schema field count")
			}
			for _, legacy := range []Options{{}} {
				if _, err := OpenFileWithOptions(path, legacy); err == nil {
					t.Fatal("legacy scope broadened", legacy)
				}
			}
			// Nonempty values for both expanded shapes are independently verified
			// by TestNativeNestedForestIndependentLevels in nested_struct_test.go.
			stream, err := source.OpenBatchStream(context.Background(), expanded, opts)
			if err != nil {
				t.Fatal(err)
			}
			batch := memory.NewBatch()
			ok, err := stream.Next(context.Background(), batch)
			if err != nil || ok && batch.Rows != 0 {
				t.Fatal("empty expanded schema", ok, err)
			}
			stream.Close()
		})
	}
}
