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
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type listStringRow struct {
	valid  bool
	values []string
	valids []bool
}

func listStringExpected(row, count, bytes int, L, E bool) listStringRow {
	r := listStringRow{valid: true}
	switch row % 7 {
	case 0:
		r.valid = !L
	case 1:
	case 2:
		r.values = []string{""}
		r.valids = []bool{!E}
	case 3:
		r.values = []string{"", "界é", "end"}
		r.valids = []bool{true, true, true}
	case 4:
		for j := 0; j < count; j++ {
			v := fmt.Sprintf("r%d/e%d/界", row, j)
			if bytes < 0 {
				v = ""
			}
			if j%7 == 0 {
				v = ""
			}
			if j == 0 && bytes > 0 {
				v = "xx" + strings.Repeat("界", bytes/3) + "z"
			}
			r.values = append(r.values, v)
			r.valids = append(r.valids, !E || j%5 != 1)
		}
	default:
		r.values = []string{fmt.Sprint(row), ""}
		r.valids = []bool{true, true}
	}
	return r
}
func listStringFixture(t *testing.T, groups []int, count, bytes int, L, E bool, corrupt int, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	lr, er := req, req
	if L {
		lr = opt
	}
	if E {
		er = opt
	}
	el, e := schema.NewPrimitiveNodeLogical("element", er, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
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
	path := filepath.Join(t.TempDir(), "list-string.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(7)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	ld, ed := int16(0), int16(0)
	if L {
		ld = 1
	}
	if E {
		ed = 1
	}
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
		c, e = rg.NextColumn()
		if e != nil {
			t.Fatal(e)
		}
		values := []arrowparquet.ByteArray{}
		defs, reps := []int16{}, []int16{}
		for i := 0; i < n; i++ {
			r := listStringExpected(base+i, count, bytes, L, E)
			if !r.valid {
				defs = append(defs, 0)
				reps = append(reps, 0)
				continue
			}
			if len(r.values) == 0 {
				defs = append(defs, ld)
				reps = append(reps, 0)
				continue
			}
			for j, x := range r.values {
				d, rep := ld+1, int16(0)
				if j > 0 {
					rep = 1
				}
				if r.valids[j] {
					d += ed
					if corrupt == 3 && base+i == 3 && j == 1 {
						x = string([]byte{255})
					}
					values = append(values, arrowparquet.ByteArray(x))
				}
				defs = append(defs, d)
				reps = append(reps, rep)
			}
		}
		if corrupt == 1 && len(reps) > 0 {
			for j := 1; j < len(reps); j++ {
				if reps[j] == 1 {
					reps[j] = 0
					break
				}
			}
			reps[0] = 1
		}
		if corrupt == 2 && len(reps) > 1 {
			reps[1] = 1
			for j := 2; j < len(reps); j++ {
				if reps[j] == 1 {
					reps[j] = 0
					break
				}
			}
		}
		if _, e = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, defs, reps); e != nil {
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
func listStringCheck(t *testing.T, b *memory.Batch, want []int, count, bytes int, L, E bool) {
	t.Helper()
	v := b.StringLists["items"]
	if v == nil || len(v.Offsets) != b.Rows+1 || v.Offsets[0] != 0 {
		t.Fatal("List shape")
	}
	for i, row := range want {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("fixed sibling order")
		}
		r := listStringExpected(row, count, bytes, L, E)
		if v.IsValid(i) != r.valid {
			t.Fatal("List NULL", row)
		}
		lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
		if hi-lo != len(r.values) {
			t.Fatal("element count", row, hi-lo, len(r.values))
		}
		for j, x := range r.values {
			if v.Values.IsValid(lo+j) != r.valids[j] {
				t.Fatal("element NULL", row, j)
			}
			if !r.valids[j] {
				x = ""
			}
			if string(v.Values.ValueBytes(lo+j)) != x {
				t.Fatal("exact UTF8 bytes", row, j)
			}
		}
	}
}
func listStringCollect(t *testing.T, st source.BatchStream, opts source.ReadOptions, want []int, count, bytes int, L, E bool) {
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
		listStringCheck(t, b, want[cursor:cursor+b.Rows], count, bytes, L, E)
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("lost pending parent", cursor, len(want))
	}
}
func TestNativeStringListDecodeMatrix(t *testing.T) {
	for _, L := range []bool{false, true} {
		for _, E := range []bool{false, true} {
			for _, dict := range []bool{false, true} {
				for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
					t.Run(fmt.Sprintf("L%v/E%v/dict%v/pageEnum%d", L, E, dict, version), func(t *testing.T) {
						path := listStringFixture(t, []int{0, 17, 66}, 129, 0, L, E, 0, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
						src, e := OpenFileWithOptions(path, Options{NativeNested: true})
						if e != nil {
							t.Fatal(e)
						}
						f := src.Schema().Fields[1]
						if f.Type != memory.TypeList || f.Nullable != L || f.Children[0].Type != memory.TypeString || f.Children[0].Nullable != E {
							t.Fatal("metadata")
						}
						for _, rows := range []int{1, 7, 65} {
							opts := testReadOptions()
							opts.BatchRows = rows
							opts.BatchBytes = 128 << 10
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
							listStringCollect(t, st, opts, want, 129, 0, L, E)
							if m.Used() != 0 {
								t.Fatal("EOF cleanup")
							}
						}
					})
				}
			}
		}
	}
}
func TestNativeStringListScopePartitions(t *testing.T) {
	path := listStringFixture(t, []int{3, 5, 7}, 17, 0, true, true, 0)
	for _, opts := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, opts); e == nil {
			t.Fatal("scope expanded")
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
	listStringCollect(t, st, opts, []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}, 17, 0, true, true)
}
func TestNativeStringListFaultAndCancel(t *testing.T) {
	path := listStringFixture(t, []int{17, 19}, 33, 0, true, true, 0)
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
			listStringCheck(t, b, want, 33, 0, true, true)
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
			t.Fatal("admission", fail, e)
		}
		if m.Used() != 0 {
			t.Fatal("failed admission cleanup")
		}
		m.fail = 0
		if e = run(m); e != nil {
			t.Fatal(e)
		}
		if m.Used() != 0 {
			t.Fatal("retry cleanup")
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
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 40 || b.Rows != 0 {
		t.Fatal("level cancellation", ok, e, ctx.checks, b.Rows)
	}
	if m.Used() != 0 {
		t.Fatal("cancel cleanup")
	}
}
func TestNativeStringListMalformedAndFooter(t *testing.T) {
	for _, corrupt := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("levels%d", corrupt), func(t *testing.T) {
			path := listStringFixture(t, []int{5}, 17, 0, true, true, corrupt)
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
				t.Fatal("malformed rep/UTF8 accepted")
			}
			if m.Used() != 0 {
				t.Fatal("malformed cleanup")
			}
		})
	}
	for _, delta := range []int64{-1, 1} {
		t.Run(fmt.Sprintf("count%d", delta), func(t *testing.T) {
			path := listStringFixture(t, []int{5}, 17, 0, true, true, 0)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			src.meta.RowGroups[0].Columns[1].MetaData.NumValues += delta
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
				t.Fatal("footer level count mismatch accepted")
			}
			if m.Used() != 0 {
				t.Fatal("footer cleanup")
			}
		})
	}
}

func TestNativeStringListIndependentElementAndByteBounds(t *testing.T) {
	for _, tc := range []struct {
		name         string
		count, bytes int
	}{{"empty_elements", 65536, -1}, {"one_huge_element", 1, 1 << 20}} {
		t.Run(tc.name, func(t *testing.T) {
			path := listStringFixture(t, []int{5}, tc.count, tc.bytes, true, true, 0)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 5
			opts.BatchBytes = 64 << 10
			m := newBatchStreamPeakManager(opts.BatchBytes)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
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
				t.Fatal("independent resource axis not bounded", e)
			}
			if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
				t.Fatal("axis peak/cleanup", m.Used(), m.peak.Load())
			}
		})
	}
}

type listStringFaultManager struct {
	memory.Manager
	native           *nestedListCursor
	replacement, hit bool
	count            int
	pending          string
	oldLease         memory.Reservation
}

func (m *listStringFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		v := m.native
		if v.strings.output != nil && v.outputCount() > 0 && ((m.replacement && v.complete) || (!m.replacement && v.stageCount() >= 8 && !v.complete)) {
			m.hit = true
			m.count = v.stageCount()
			m.pending = string(v.bytes[0])
			m.oldLease = v.strings.outputLease
			return nil, memory.ErrBudgetExceeded
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeStringListPartialAndReplacementRecovery(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			path := listStringFixture(t, []int{17}, 129, 0, true, true, 0)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 17
			opts.BatchBytes = 128 << 10
			m := &listStringFaultManager{Manager: memory.NewBudgetManager(0), replacement: replacement}
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			m.native = st.(*nestedListStream).cursors[0]
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if !ok || e != nil || !m.hit || b.Rows < 1 || b.Rows >= 17 {
				t.Fatal("exact prefix required", ok, e, m.hit, b.Rows)
			}
			v := m.native
			if v.stageCount() != m.count || v.strings.stageLease == nil || v.strings.outputLease != m.oldLease || v.complete != replacement || !v.tuple.present || string(v.bytes[0]) != m.pending {
				t.Fatal("pending stage/page or atomic output changed")
			}
			want := make([]int, b.Rows)
			for i := range want {
				want[i] = i
			}
			listStringCheck(t, b, want, 129, 0, true, true)
			want = make([]int, 17-b.Rows)
			for i := range want {
				want[i] = b.Rows + i
			}
			listStringCollect(t, st, opts, want, 129, 0, true, true)
			if m.Used() != 0 {
				t.Fatal("recovery cleanup")
			}
		})
	}
}

type listStringObserveContext struct {
	context.Context
	native     *nestedListCursor
	lease      memory.Reservation
	offset     *uint32
	data       *byte
	cancelCopy bool
	checks     int
}

func (c *listStringObserveContext) Err() error {
	v := c.native
	if v.complete && v.stageCount() == 17 && len(v.strings.stageData) > 64<<10 {
		c.lease = v.strings.stageLease
		c.offset = &v.strings.stageOffsets[0]
		c.data = &v.strings.stageData[0]
	}
	if c.cancelCopy && len(v.strings.stageData) > 64<<10 && len(v.bytes[0]) > 64<<10 {
		var pcs [24]uintptr
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
func TestNativeStringListTransferAndChunkCancellation(t *testing.T) {
	path := listStringFixture(t, []int{4, 1}, 17, 131073, true, true, 0)
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
	opts.BatchBytes = 1 << 20
	for _, cancelCopy := range []bool{false, true} {
		m := newBatchStreamPeakManager(opts.BatchBytes)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		native := st.(*nestedListStream).cursors[0]
		ctx := &listStringObserveContext{Context: context.Background(), native: native, cancelCopy: cancelCopy}
		b := memory.NewBatch()
		ok, e := st.Next(ctx, b)
		if cancelCopy {
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
				t.Fatal("in-copy cancellation", ok, e, ctx.checks)
			}
		} else {
			if !ok || e != nil {
				t.Fatal(e)
			}
			listStringCheck(t, b, []int{4}, 17, 131073, true, true)
			if ctx.lease == nil || native.strings.outputLease != ctx.lease || native.strings.stageLease != nil || &native.strings.output.Values.Offsets[0] != ctx.offset || &native.strings.output.Values.Data[0] != ctx.data {
				t.Fatal("joint lease/offset/data transfer")
			}
			if ok, e = st.Next(context.Background(), memory.NewBatch()); ok || e != nil {
				t.Fatal("final EOF", ok, e)
			}
		}
		st.Close()
		if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
			t.Fatal("copy/transfer cleanup")
		}
	}
}
func TestNativeStringListTightCapacityReplacement(t *testing.T) {
	for _, mirror := range []bool{false, true} {
		t.Run(fmt.Sprint(mirror), func(t *testing.T) {
			path := listStringFixture(t, []int{1}, 1, 0, true, true, 0)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 1
			opts.BatchBytes = 8344
			slots, live, dataCap, dataLen, needed, newBytes, oldCharge := 256, 17, 0, 0, 18, 1024, int64(1060)
			if mirror {
				opts.BatchBytes = 11856
				slots, live, dataCap, dataLen, needed, newBytes, oldCharge = 1, 1, 4096, 1024, 129, 1024, 4112
			}
			m := newBatchStreamPeakManager(opts.BatchBytes)
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			v := st.(*nestedListStream).cursors[0]
			if e = v.owner.allocate(context.Background(), 1); e != nil {
				t.Fatal(e)
			}
			lease, e := m.Reserve(context.Background(), oldCharge)
			if e != nil {
				t.Fatal(e)
			}
			v.strings.stageLease = lease
			v.strings.stageOffsets = make([]uint32, live+1, slots+1)
			v.strings.stageData = make([]byte, dataLen, dataCap)
			v.strings.stageValid = make(memory.Bitmap, memory.WordsFor(slots))
			for i := 0; i < live; i++ {
				memory.Set(v.strings.stageValid, i)
			}
			for i := range v.strings.stageData {
				v.strings.stageData[i] = 'x'
			}
			if mirror {
				v.strings.stageOffsets[1] = 1024
			}
			if e = v.strings.growStage(context.Background(), needed, newBytes); e != nil {
				t.Fatal("exact legal replacement rejected", e)
			}
			for i := 0; i < live; i++ {
				if !memory.IsSet(v.strings.stageValid, i) {
					t.Fatal("live bitmap lost")
				}
			}
			if !mirror && cap(v.strings.stageOffsets) != 19 {
				t.Fatal("fixture did not shrink spare slots across bitmap words", cap(v.strings.stageOffsets))
			}
			if mirror && (cap(v.strings.stageData) != 1024 || string(v.strings.stageData) != strings.Repeat("x", 1024)) {
				t.Fatal("data capacity shrink lost live bytes")
			}
			if m.peak.Load() != opts.BatchBytes {
				t.Fatal("exact replacement peak", m.peak.Load(), opts.BatchBytes)
			}
			st.Close()
			if m.Used() != 0 {
				t.Fatal("replacement cleanup")
			}
		})
	}
	if stringElementBytes(math.MaxInt, 0) >= 0 {
		t.Fatal("terminal offset host overflow not rejected")
	}
}
func TestNativeStringListOversizedBorrowedBeforeCopy(t *testing.T) {
	path := listStringFixture(t, []int{5}, 1, 1<<20, true, true, 0)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 5
	opts.BatchBytes = 64 << 10
	m := newBatchStreamPeakManager(opts.BatchBytes)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	v := st.(*nestedListStream).cursors[0]
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil || b.Rows != 4 || !v.tuple.present || len(v.bytes[0]) < 1<<20 || v.stageCount() != 0 || cap(v.strings.stageData) != 0 || v.strings.stageLease != nil {
		t.Fatal("oversized pending allocated before admission", ok, e, b.Rows, v.stageCount(), cap(v.strings.stageData))
	}
	listStringCheck(t, b, []int{0, 1, 2, 3}, 1, 1<<20, true, true)
	if _, e = st.Next(context.Background(), memory.NewBatch()); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal(e)
	}
	st.Close()
	if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
		t.Fatal("oversized cleanup/peak")
	}
}
func TestNativeStringListProjectionPayloadAndEarlyClose(t *testing.T) {
	path := listStringFixture(t, []int{17}, 129, 0, true, true, 0)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"id", "items"} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{name}})
		if e != nil {
			t.Fatal(e)
		}
		opts := testReadOptions()
		opts.BatchRows = 65
		opts.BatchBytes = 64 << 10
		opts.BatchPayloadTargetBytes = 128
		opts.BatchPayloadBytes = 16 << 10
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
				if len(b.StringLists) != 0 {
					t.Fatal("projection leaked List")
				}
				for i := 0; i < b.Rows; i++ {
					if b.Int64s["id"].Values[i] != int64(row+i) {
						t.Fatal("id order")
					}
				}
			} else {
				if len(b.Int64s) != 0 {
					t.Fatal("projection leaked sibling")
				}
				ids := make([]int64, b.Rows)
				want := make([]int, b.Rows)
				for i := range ids {
					ids[i] = int64(row + i)
					want[i] = row + i
				}
				b.Int64s["id"] = &memory.Int64Vector{Values: ids}
				listStringCheck(t, b, want, 129, 0, true, true)
			}
			row += b.Rows
		}
		st.Close()
		if row != 17 || m.Used() != 0 {
			t.Fatal("projection cleanup/count")
		}
		st, e = source.OpenBatchStream(context.Background(), projected, opts)
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := st.Next(context.Background(), memory.NewBatch()); !ok || e != nil {
			t.Fatal(e)
		}
		st.Close()
		st.Close()
		if m.Used() != 0 {
			t.Fatal("early/double Close")
		}
	}
}

func TestNativeStringListAllFixedSiblings(t *testing.T) {
	kinds := fixedTestKinds()
	el, _ := schema.NewPrimitiveNodeLogical("element", arrowparquet.Repetitions.Optional, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	mid, _ := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
	list, _ := schema.NewGroupNodeLogical("items", arrowparquet.Repetitions.Optional, schema.FieldList{mid}, schema.ListLogicalType{}, -1)
	fields := schema.FieldList{list}
	for _, k := range kinds {
		n, e := schema.NewPrimitiveNodeLogical(k.name, arrowparquet.Repetitions.Optional, k.logical, k.physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		fields = append(fields, n)
	}
	root, _ := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
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
	if _, e = c.(*file.ByteArrayColumnChunkWriter).WriteBatch([]arrowparquet.ByteArray{[]byte("界")}, []int16{0, 1, 3}, []int16{0, 0, 0}); e != nil {
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
		defs := []int16{0, 0, 1}
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
	v := b.StringLists["items"]
	if v.IsValid(0) || !v.IsValid(1) || v.Offsets[2] != 0 || string(v.Values.ValueBytes(0)) != "界" {
		t.Fatal("list masks/value")
	}
	for i, k := range kinds {
		if src.Schema().Fields[i+1].Type != k.typ {
			t.Fatal("fixed sibling metadata")
		}
		ptr := reflect.ValueOf(b).Elem().FieldByName(k.vector).MapIndex(reflect.ValueOf(k.name))
		for row := 0; row < 3; row++ {
			valid := ptr.MethodByName("IsValid").Call([]reflect.Value{reflect.ValueOf(row)})[0].Bool()
			if valid != (row == 2) {
				t.Fatal("sibling NULL", k.name)
			}
		}
		x := ptr.Elem().FieldByName("Values").Index(2)
		switch x.Kind() {
		case reflect.Float32, reflect.Float64:
			if x.Float() != 1.25 {
				t.Fatal("float")
			}
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if x.Uint() != 1 {
				t.Fatal("unsigned/bool")
			}
		default:
			if x.Int() != k.scale {
				t.Fatal("fixed/timestamp", k.name)
			}
		}
	}
}
