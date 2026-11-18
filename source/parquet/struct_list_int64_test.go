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
	"runtime"
	"strings"
	"testing"
)

func structListFixture(t *testing.T, groups []int, large int, A, L, E bool, inconsistent bool, props ...arrowparquet.WriterProperty) string {
	return structListFixtureVariant(t, groups, large, A, L, E, inconsistent, 0, props...)
}
func structListFixtureVariant(t *testing.T, groups []int, large int, A, L, E bool, inconsistent bool, corrupt int, props ...arrowparquet.WriterProperty) string {
	return structListFixtureLayout(t, groups, large, A, L, E, inconsistent, corrupt, 0, props...)
}
func structListFixtureLayout(t *testing.T, groups []int, large int, A, L, E bool, inconsistent bool, corrupt, layout int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	ar, lr, er := req, req, req
	if A {
		ar = opt
	}
	if L {
		lr = opt
	}
	if E {
		er = opt
	}
	prim := func(name string, rep arrowparquet.Repetition) schema.Node {
		n, e := schema.NewPrimitiveNodeLogical(name, rep, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	el := prim("element", er)
	middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
	if e != nil {
		t.Fatal(e)
	}
	list, e := schema.NewGroupNodeLogical("items", lr, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
	if e != nil {
		t.Fatal(e)
	}
	name := "n"
	if layout == 4 {
		name = "id"
	}
	children := schema.FieldList{prim(name, req), list, prim("optional", opt)}
	order := []int{0, 1, 2, 3}
	switch layout {
	case 1:
		children = schema.FieldList{list}
		order = []int{0, 2}
	case 2:
		children = schema.FieldList{list, prim(name, req), prim("optional", opt)}
		order = []int{0, 2, 1, 3}
	case 3:
		children = schema.FieldList{prim(name, req), prim("optional", opt), list}
		order = []int{0, 1, 3, 2}
	}
	record, e := schema.NewGroupNode("record", ar, children, -1)
	if e != nil {
		t.Fatal(e)
	}
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{prim("id", req), record}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "struct-list.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	ad, ld, ed := int16(0), int16(0), int16(0)
	if A {
		ad = 1
	}
	if L {
		ld = 1
	}
	if E {
		ed = 1
	}
	for _, n := range groups {
		rg := w.AppendRowGroup()
		for _, col := range order {
			c, e := rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			values := []int64{}
			defs, reps := []int16{}, []int16{}
			for i := 0; i < n; i++ {
				row := base + i
				pv := !A || row%11 != 0
				if corrupt == 3 && col == 1 && row == 4 {
					pv = false
				}
				if inconsistent && col == 1 && row == 0 {
					pv = true
				}
				if col == 0 {
					values = append(values, int64(row))
					continue
				}
				if !pv {
					defs = append(defs, 0)
					if col == 2 {
						reps = append(reps, 0)
					}
					continue
				}
				if col == 1 {
					defs = append(defs, ad)
					values = append(values, int64(row+19))
					continue
				}
				if col == 3 {
					d := ad
					if row%3 != 1 {
						d++
						values = append(values, int64(-row-19))
					}
					defs = append(defs, d)
					continue
				}
				r := listNativeExpected(row, large, L, E)
				if !r.valid {
					defs = append(defs, ad)
					reps = append(reps, 0)
					continue
				}
				if len(r.values) == 0 {
					defs = append(defs, ad+ld)
					reps = append(reps, 0)
					continue
				}
				for j, x := range r.values {
					d := ad + ld + 1
					rep := int16(0)
					if j > 0 {
						rep = 1
					}
					if r.valids[j] {
						d += ed
						values = append(values, x)
					}
					defs = append(defs, d)
					reps = append(reps, rep)
				}
			}
			if col == 0 {
				defs = nil
			}
			if col != 2 {
				reps = nil
			}
			if col == 2 && len(defs) > 0 {
				if corrupt == 1 {
					defs[0] = ad + ld + ed + 2
				}
				if corrupt == 2 {
					for j := 1; j < len(reps); j++ {
						if reps[j] == 1 {
							reps[j] = 0
							break
						}
					}
					reps[0] = 1
				}
			}
			if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, reps); e != nil {
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
func structListCheck(t *testing.T, b *memory.Batch, want []int, large int, A, L, E bool) {
	t.Helper()
	record := b.Structs["record"]
	if record == nil || record.Rows != b.Rows || record.Values.Rows != b.Rows {
		t.Fatal("Struct shape")
	}
	if err := record.Validate(memory.StructField("record", A, record.Fields...), b.Rows); err != nil {
		t.Fatal("Struct validation", err)
	}
	v := record.Values.Int64Lists["items"]
	if v == nil || len(v.Offsets) != b.Rows+1 || v.Offsets[0] != 0 {
		t.Fatal("List shape")
	}
	for i, row := range want {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("top sibling shifted")
		}
		pv := !A || row%11 != 0
		if record.IsValid(i) != pv {
			t.Fatal("ancestor mask", row)
		}
		r := listNativeExpected(row, large, L, E)
		if !pv {
			r = listNativeRow{}
		}
		if v.IsValid(i) != r.valid {
			t.Fatal("raw List mask", row)
		}
		lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
		if hi-lo != len(r.values) {
			t.Fatal("offset count", row)
		}
		for j, x := range r.values {
			if !r.valids[j] {
				x = 0
			}
			if v.Values.IsValid(lo+j) != r.valids[j] || v.Values.Values[lo+j] != x {
				t.Fatal("element", row, j)
			}
		}
		n, o := record.Values.Int64s["n"], record.Values.Int64s["optional"]
		if n.IsValid(i) != pv || o.IsValid(i) != (pv && row%3 != 1) {
			t.Fatal("fixed child masks")
		}
		if pv && n.Values[i] != int64(row+19) {
			t.Fatal("required sibling")
		}
		if pv && row%3 != 1 && o.Values[i] != int64(-row-19) {
			t.Fatal("optional sibling")
		}
	}
}
func structListCollect(t *testing.T, st source.BatchStream, opts source.ReadOptions, want []int, large int, A, L, E bool) {
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
		if b.Rows < 1 || b.Rows > opts.BatchRows || memory.BatchBytes(b) > opts.BatchBytes || cursor+b.Rows > len(want) {
			t.Fatal("batch bounds")
		}
		structListCheck(t, b, want[cursor:cursor+b.Rows], large, A, L, E)
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("missing parent", cursor, len(want))
	}
}
func TestNativeStructInt64ListDefinitionMatrix(t *testing.T) {
	for _, A := range []bool{false, true} {
		for _, L := range []bool{false, true} {
			for _, E := range []bool{false, true} {
				for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
					t.Run(fmt.Sprintf("A%v/L%v/E%v/pageEnum%d", A, L, E, version), func(t *testing.T) {
						path := structListFixture(t, []int{0, 17, 66}, 257, A, L, E, false, arrowparquet.WithDataPageVersion(version), arrowparquet.WithDictionaryDefault(E))
						src, e := OpenFileWithOptions(path, Options{NativeNested: true})
						if e != nil {
							t.Fatal(e)
						}
						f := src.Schema().Fields[1]
						if f.Type != memory.TypeStruct || f.Nullable != A || f.Children[1].Type != memory.TypeList || f.Children[1].Nullable != L || f.Children[1].Children[0].Nullable != E {
							t.Fatal("nested nullable metadata")
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
							structListCollect(t, st, opts, want, 257, A, L, E)
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
func TestNativeStructInt64ListPartitionProjectionScope(t *testing.T) {
	path := structListFixture(t, []int{3, 5, 7}, 17, true, true, true, false)
	for _, opts := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, opts); e == nil {
			t.Fatal("old/exclusive scope")
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
	st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[2], parts[0]})
	if e != nil {
		t.Fatal(e)
	}
	structListCollect(t, st, opts, []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}, 17, true, true, true)
	for _, name := range []string{"id", "record"} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{name}})
		if e != nil {
			t.Fatal(e)
		}
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
				if len(b.Structs) != 0 {
					t.Fatal("projection leaks Struct")
				}
				for i := 0; i < b.Rows; i++ {
					if b.Int64s["id"].Values[i] != int64(row+i) {
						t.Fatal("id projection")
					}
				}
			} else {
				if len(b.Int64s) != 0 {
					t.Fatal("projection leaks top sibling")
				}
				want := make([]int, b.Rows)
				ids := make([]int64, b.Rows)
				for i := range want {
					want[i] = row + i
					ids[i] = int64(row + i)
				}
				b.Int64s["id"] = &memory.Int64Vector{Values: ids}
				structListCheck(t, b, want, 17, true, true, true)
			}
			row += b.Rows
		}
		st.Close()
		if row != 15 {
			t.Fatal("projection row count")
		}
	}
}
func TestNativeStructInt64ListFaultCancelAndParentConsensus(t *testing.T) {
	path := structListFixture(t, []int{17, 19}, 257, true, true, true, false)
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
					t.Fatal("recovery lost rows")
				}
				return nil
			}
			want := make([]int, b.Rows)
			for i := range want {
				want[i] = cursor + i
			}
			structListCheck(t, b, want, 257, true, true, true)
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
			t.Fatal("fault", fail, e)
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
		t.Fatal("cancel cleanup")
	}
	path = structListFixture(t, []int{5}, 17, true, true, true, true)
	src, e = OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	st, e = source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	_, e = st.Next(context.Background(), memory.NewBatch())
	st.Close()
	if e == nil {
		t.Fatal("inconsistent ancestor validity accepted")
	}
	if m.Used() != 0 {
		t.Fatal("ancestor failure leak")
	}
}
func TestNativeStructInt64ListFooterCountMismatch(t *testing.T) {
	for _, delta := range []int64{-1, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			path := structListFixture(t, []int{5}, 17, true, true, true, false)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			src.meta.RowGroups[0].Columns[2].MetaData.NumValues += delta
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
				t.Fatal("bad repeated count accepted")
			}
			if m.Used() != 0 {
				t.Fatal("metadata cleanup")
			}
		})
	}
}

type structListStageFailure struct {
	memory.Manager
	native *nestedListStream
	hit    bool
}

func (m *structListStageFailure) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		c := m.native.cursors[0]
		if len(c.stage) >= 8 && c.output != nil && len(c.output.Values.Values) > 0 {
			m.hit = true
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeStructInt64ListPartialRowParentCrop(t *testing.T) {
	path := structListFixture(t, []int{17}, 257, true, true, true, false)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 17
	opts.BatchBytes = 64 << 10
	m := &structListStageFailure{Manager: memory.NewBudgetManager(0)}
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	m.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	c := m.native.cursors[0]
	if !ok || e != nil || !m.hit || b.Rows != 4 || len(c.stage) != 8 || c.complete || !c.tuple.present || !c.ancestorValid || c.stageLease == nil {
		t.Fatal("partial ancestor state", ok, e, b.Rows, len(c.stage))
	}
	structListCheck(t, b, []int{0, 1, 2, 3}, 257, true, true, true)
	want := make([]int, 13)
	for i := range want {
		want[i] = i + 4
	}
	structListCollect(t, st, opts, want, 257, true, true, true)
	if m.Used() != 0 {
		t.Fatal("partial parent cleanup")
	}
}

type structListObserveContext struct {
	context.Context
	native     *nestedListStream
	lease      memory.Reservation
	ptr        *int64
	cancelCopy bool
	checks     int
}

func (c *structListObserveContext) Err() error {
	v := c.native.cursors[0]
	if len(v.stage) >= 4097 {
		c.lease = v.stageLease
		c.ptr = &v.stage[0]
	}
	if c.cancelCopy && len(v.stage) >= 2048 {
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
func TestNativeStructInt64ListTransferAndCopyCancel(t *testing.T) {
	path := structListFixture(t, []int{4, 1}, 4097, true, true, true, false)
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
		m := newBatchStreamPeakManager(opts.BatchBytes)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		native := st.(*nestedListStream)
		ctx := &structListObserveContext{Context: context.Background(), native: native, cancelCopy: cancelCopy}
		b := memory.NewBatch()
		ok, e := st.Next(ctx, b)
		if cancelCopy {
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
				t.Fatal("copy cancellation", ok, e, ctx.checks)
			}
		} else {
			if !ok || e != nil {
				t.Fatal(e)
			}
			structListCheck(t, b, []int{4}, 4097, true, true, true)
			c := native.cursors[0]
			if c.outputLease != ctx.lease || ctx.lease == nil || c.stageLease != nil || c.stage != nil || &c.output.Values.Values[0] != ctx.ptr {
				t.Fatal("stage transfer")
			}
			if ok, e = st.Next(context.Background(), memory.NewBatch()); ok || e != nil {
				t.Fatal("final EOF", ok, e)
			}
		}
		st.Close()
		if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
			t.Fatal("copy/transfer budget cleanup")
		}
	}
}
func TestNativeStructInt64ListMalformedDefinitionAndFirstRep(t *testing.T) {
	for _, corrupt := range []int{1, 2} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			path := structListFixtureVariant(t, []int{5}, 17, true, true, true, false, corrupt)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 5
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
				t.Fatal("illegal ancestor list levels accepted")
			}
			if m.Used() != 0 {
				t.Fatal("malformed cleanup")
			}
		})
	}
}

func TestNativeStructInt64ListChildLayouts(t *testing.T) {
	for _, layout := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(layout), func(t *testing.T) {
			path := structListFixtureLayout(t, []int{17}, 257, true, false, true, false, 0, layout)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 7
			opts.BatchBytes = 64 << 10
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
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
				v := b.Structs["record"]
				if e = v.Validate(src.Schema().Fields[1], b.Rows); e != nil {
					t.Fatal(e)
				}
				for i := 0; i < b.Rows; i++ {
					r := listNativeExpected(row, 257, false, true)
					pv := row%11 != 0
					if !pv {
						r = listNativeRow{}
					}
					list := v.Values.Int64Lists["items"]
					if v.IsValid(i) != pv || list.IsValid(i) != r.valid || int(list.Offsets[i+1]-list.Offsets[i]) != len(r.values) {
						t.Fatal("layout masks/offsets")
					}
					if b.Int64s["id"].Values[i] != int64(row) {
						t.Fatal("top id")
					}
					if layout != 1 && pv {
						name := "n"
						if layout == 4 {
							name = "id"
						}
						if v.Values.Int64s[name].Values[i] != int64(row+19) {
							t.Fatal("same-name child binding")
						}
					}
					row++
				}
			}
			if row != 17 {
				t.Fatal("layout rows")
			}
		})
	}
}

func TestNativeStructInt64ListParentBitmapTightBudget(t *testing.T) {
	path := structListFixture(t, []int{1, 1}, 1, true, true, true, false)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	parts, e := src.Partitions(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	for _, budget := range []int64{10304, 10312} {
		opts := testReadOptions()
		opts.BatchRows = 1
		opts.BatchBytes = budget
		m := newBatchStreamPeakManager(budget)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if budget == 10304 {
			if ok || !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatal("missing ancestor bitmap8bytes should reject", ok, e)
			}
		} else {
			if !ok || e != nil {
				t.Fatal("exact metadata10240+headers72", ok, e)
			}
			structListCheck(t, b, []int{1}, 1, true, true, true)
			if m.peak.Load() != 10312 {
				t.Fatal("exact bitmap charge", m.peak.Load())
			}
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("tight header cleanup")
		}
	}
}
func TestNativeStructInt64ListAllFixedChildren(t *testing.T) {
	kinds := fixedTestKinds()
	el, _ := schema.NewPrimitiveNodeLogical("element", arrowparquet.Repetitions.Optional, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
	middle, _ := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
	list, _ := schema.NewGroupNodeLogical("items", arrowparquet.Repetitions.Optional, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
	fields := schema.FieldList{list}
	for _, k := range kinds {
		n, e := schema.NewPrimitiveNodeLogical(k.name, arrowparquet.Repetitions.Optional, k.logical, k.physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		fields = append(fields, n)
	}
	record, _ := schema.NewGroupNode("record", arrowparquet.Repetitions.Optional, fields, -1)
	root, _ := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{record}, -1)
	path := filepath.Join(t.TempDir(), "allfixed.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w := file.NewParquetWriter(f, root)
	rg := w.AppendRowGroup()
	c, e := rg.NextColumn()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.(*file.Int64ColumnChunkWriter).WriteBatch([]int64{42}, []int16{0, 2, 4}, []int16{0, 0, 0}); e != nil {
		t.Fatal(e)
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	for range kinds {
		c, e = rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		defs := []int16{0, 1, 2}
		switch v := c.(type) {
		case *file.Int32ColumnChunkWriter:
			_, e = v.WriteBatch([]int32{1}, defs, nil)
		case *file.Int64ColumnChunkWriter:
			_, e = v.WriteBatch([]int64{1}, defs, nil)
		case *file.Float32ColumnChunkWriter:
			_, e = v.WriteBatch([]float32{1.25}, defs, nil)
		case *file.Float64ColumnChunkWriter:
			_, e = v.WriteBatch([]float64{1.25}, defs, nil)
		case *file.BooleanColumnChunkWriter:
			_, e = v.WriteBatch([]bool{true}, defs, nil)
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
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 3
	opts.BatchBytes = 128 << 10
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil {
		t.Fatal(e)
	}
	r := b.Structs["record"]
	if e = r.Validate(src.Schema().Fields[0], 3); e != nil {
		t.Fatal(e)
	}
	if r.IsValid(0) || !r.IsValid(1) || !r.IsValid(2) {
		t.Fatal("ancestor masks")
	}
	listv := r.Values.Int64Lists["items"]
	if listv.IsValid(0) || !listv.IsValid(1) || listv.Offsets[2] != 0 || listv.Values.Values[0] != 42 {
		t.Fatal("list independent masks")
	}
	for j, k := range kinds {
		if src.Schema().Fields[0].Children[j+1].Type != k.typ {
			t.Fatal("fixed logical type", k.name)
		}
		v := reflect.ValueOf(r.Values).Elem().FieldByName(k.vector).MapIndex(reflect.ValueOf(k.name))
		for i := 0; i < 3; i++ {
			valid := v.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(i)})[0].Bool()
			if valid != (i == 2) {
				t.Fatal("fixed validity", k.name, i)
			}
		}
		x := v.Elem().FieldByName("Values").Index(2)
		switch x.Kind() {
		case reflect.Float32, reflect.Float64:
			if x.Float() != 1.25 {
				t.Fatal("fixed float")
			}
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if x.Uint() != 1 {
				t.Fatal("fixed unsigned/bool")
			}
		default:
			if x.Int() != k.scale {
				t.Fatal("fixed temporal/integer", k.name)
			}
		}
	}
}

type structListLookaheadFailure struct {
	memory.Manager
	native *nestedListStream
	hit    bool
}

func (m *structListLookaheadFailure) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		c := m.native.cursors[0]
		if c.complete && c.ancestorValid && c.tuple.present && c.tuple.def[0] == 0 && len(c.output.Values.Values) > 0 {
			m.hit = true
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeStructInt64ListAncestorLookaheadRecovery(t *testing.T) {
	path := structListFixture(t, []int{17}, 257, true, true, true, false)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 17
	opts.BatchBytes = 128 << 10
	m := &structListLookaheadFailure{Manager: memory.NewBudgetManager(0)}
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	m.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	c := m.native.cursors[0]
	if !ok || e != nil || !m.hit || b.Rows != 10 || !c.complete || !c.ancestorValid || c.tuple.def[0] != 0 {
		t.Fatal("current parent overwritten by next NULL parent", ok, e, b.Rows)
	}
	want := make([]int, 10)
	for i := range want {
		want[i] = i
	}
	structListCheck(t, b, want, 257, true, true, true)
	structListCollect(t, st, opts, []int{10, 11, 12, 13, 14, 15, 16}, 257, true, true, true)
	if m.Used() != 0 {
		t.Fatal("lookahead cleanup")
	}
}
func TestNativeStructInt64ListLateParentMismatch(t *testing.T) {
	path := structListFixtureVariant(t, []int{5}, 257, true, true, true, false, 3)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 5
	opts.BatchBytes = 64 << 10
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	_, e = st.Next(context.Background(), memory.NewBatch())
	st.Close()
	if e == nil || !strings.Contains(e.Error(), "parent") {
		t.Fatal("late replacement parent consensus", e)
	}
	if m.Used() != 0 {
		t.Fatal("late parent cleanup")
	}
}
