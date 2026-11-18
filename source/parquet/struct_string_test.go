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

func stringStructValue(row, col, large int) string {
	if large == -1 && row == 2 && col == 1 {
		return string([]byte{0xff})
	}
	if row%7 == 0 {
		return ""
	}
	if row%5 == 4 {
		return "xx" + strings.Repeat(fmt.Sprintf("界%d", col), large) + fmt.Sprint(row)
	}
	return fmt.Sprintf("列%d/row%d/é", col, row)
}
func stringStructFixture(t *testing.T, groups []int, large int, props ...arrowparquet.WriterProperty) string {
	return stringStructFixtureVariant(t, groups, large, false, props...)
}
func stringStructFixtureVariant(t *testing.T, groups []int, large int, inconsistent bool, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	prim := func(name string, rep arrowparquet.Repetition, logical schema.LogicalType, physical arrowparquet.Type) schema.Node {
		n, e := schema.NewPrimitiveNodeLogical(name, rep, logical, physical, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	req, opt := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Optional
	children := schema.FieldList{prim("n", req, schema.NoLogicalType{}, arrowparquet.Types.Int64), prim("required", req, schema.StringLogicalType{}, arrowparquet.Types.ByteArray), prim("optional", opt, schema.StringLogicalType{}, arrowparquet.Types.ByteArray)}
	record, e := schema.NewGroupNode("record", opt, children, -1)
	if e != nil {
		t.Fatal(e)
	}
	root, e := schema.NewGroupNode("schema", req, schema.FieldList{prim("id", req, schema.NoLogicalType{}, arrowparquet.Types.Int64), prim("text", opt, schema.StringLogicalType{}, arrowparquet.Types.ByteArray), record}, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "strings.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	props = append([]arrowparquet.WriterProperty{arrowparquet.WithDataPageSize(128), arrowparquet.WithBatchSize(3)}, props...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(props...)))
	base := 0
	for _, n := range groups {
		rg := w.AppendRowGroup()
		for col := 0; col < 5; col++ {
			c, e := rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			var nums []int64
			var values []arrowparquet.ByteArray
			defs := make([]int16, n)
			for i := 0; i < n; i++ {
				row := base + i
				parent := row%5 != 0
				if inconsistent && row == 2 && col == 3 {
					parent = false
				}
				valid := true
				switch col {
				case 0:
					nums = append(nums, int64(row))
					continue
				case 1:
					valid = row%3 != 1
					if valid {
						defs[i] = 1
					}
				case 2, 3:
					valid = parent
					if parent {
						defs[i] = 1
					}
				case 4:
					if parent {
						defs[i] = 1
					}
					valid = parent && row%3 != 1
					if valid {
						defs[i] = 2
					}
				}
				if !valid {
					continue
				}
				if col == 2 {
					nums = append(nums, int64(row+19))
				} else {
					values = append(values, arrowparquet.ByteArray(stringStructValue(row, col, large)))
				}
			}
			if col == 0 {
				_, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(nums, nil, nil)
			} else if col == 2 {
				_, e = c.(*file.Int64ColumnChunkWriter).WriteBatch(nums, defs, nil)
			} else {
				_, e = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(values, defs, nil)
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
func stringStructCheck(t *testing.T, b *memory.Batch, want []int, large int) {
	t.Helper()
	for i, row := range want {
		if b.Int64s["id"].Values[i] != int64(row) {
			t.Fatal("row order")
		}
		r := b.Structs["record"]
		pv := row%5 != 0
		if r.IsValid(i) != pv {
			t.Fatal("parent NULL")
		}
		for _, col := range []int{1, 3, 4} {
			v := b.Strings["text"]
			valid := row%3 != 1
			if col == 3 {
				v = r.Values.Strings["required"]
				valid = pv
			}
			if col == 4 {
				v = r.Values.Strings["optional"]
				valid = pv && row%3 != 1
			}
			if v.IsValid(i) != valid {
				t.Fatal("string NULL", col, row)
			}
			if valid && string(v.ValueBytes(i)) != stringStructValue(row, col, large) {
				t.Fatal("string exact bytes", col, row)
			}
		}
		if pv && r.Values.Int64s["n"].Values[i] != int64(row+19) {
			t.Fatal("fixed sibling shifted")
		}
	}
}
func stringStructCollect(t *testing.T, st source.BatchStream, opts source.ReadOptions, want []int, large int) {
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
			t.Fatal("batch bounds", b.Rows, memory.BatchBytes(b))
		}
		if cursor+b.Rows > len(want) {
			t.Fatal("duplicate rows")
		}
		stringStructCheck(t, b, want[cursor:cursor+b.Rows], large)
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("missing pending row", cursor, len(want))
	}
}
func TestNativeStructStringDecodeAndPending(t *testing.T) {
	for _, dict := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("dict%v/v%d", dict, version), func(t *testing.T) {
				path := stringStructFixture(t, []int{0, 17, 66}, 1800, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				fields := src.Schema().Fields
				if fields[1].Type != memory.TypeString || !fields[1].Nullable || fields[2].Type != memory.TypeStruct || !fields[2].Nullable || fields[2].Children[1].Nullable || !fields[2].Children[2].Nullable {
					t.Fatal("schema/nullability")
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
					stringStructCollect(t, st, opts, want, 1800)
					if m.Used() != 0 {
						t.Fatal("EOF leak")
					}
				}
			})
		}
	}
}
func TestNativeStructStringScopePartitionsAndProjection(t *testing.T) {
	path := stringStructFixture(t, []int{3, 5, 7}, 4)
	for _, opts := range []Options{{}} {
		if _, e := OpenFileWithOptions(path, opts); e == nil {
			t.Fatal("scope unexpectedly widened", opts)
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
		stringStructCollect(t, st, opts, want, 4)
	}
	projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"record"}})
	if e != nil {
		t.Fatal(e)
	}
	st, e := source.OpenBatchStream(context.Background(), projected, opts)
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
		if len(b.Strings) != 0 || len(b.Int64s) != 0 || b.Structs["record"] == nil {
			t.Fatal("projection leaked or flattened")
		}
		for i := 0; i < b.Rows; i++ {
			pv := row%5 != 0
			r := b.Structs["record"]
			if r.IsValid(i) != pv {
				t.Fatal("project parent")
			}
			if pv && string(r.Values.Strings["required"].ValueBytes(i)) != stringStructValue(row, 3, 4) {
				t.Fatal("project exact")
			}
			row++
		}
	}
	if row != 15 {
		t.Fatal("projection row count")
	}
}
func TestNativeStructStringAdmissionAndClose(t *testing.T) {
	path := stringStructFixture(t, []int{17, 19}, 1800)
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
					t.Fatal("recovered stream missing rows", cursor)
				}
				return nil
			}
			ids := make([]int, b.Rows)
			for i := range ids {
				ids[i] = cursor + i
			}
			stringStructCheck(t, b, ids, 1800)
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
			t.Fatal("failure leak")
		}
		m.fail = 0
		if e = run(m); e != nil {
			t.Fatal("fresh retry", e)
		}
		if m.Used() != 0 {
			t.Fatal("retry leak")
		}
	}
	for _, pre := range []bool{false, true} {
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStream(context.Background(), src, opts)
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if !pre {
			if ok, e := st.Next(ctx, memory.NewBatch()); !ok || e != nil {
				t.Fatal(e)
			}
		}
		cancel()
		if _, e := st.Next(ctx, memory.NewBatch()); !errors.Is(e, context.Canceled) {
			t.Fatal("cancel", e)
		}
		st.Close()
		st.Close()
		if m.Used() != 0 {
			t.Fatal("cancel/doubleclose leak")
		}
	}
	huge := stringStructFixture(t, []int{5}, 1<<18)
	src, e = OpenFileWithOptions(huge, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(0)
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
	if e == nil {
		t.Fatal("oversized row accepted")
	}
	if m.Used() != 0 {
		t.Fatal("oversized row leak")
	}
}

func TestNativeStructStringUTF8AndPayloadLimits(t *testing.T) {
	bad := stringStructFixture(t, []int{3}, -1)
	src, e := OpenFileWithOptions(bad, Options{NativeNested: true})
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
		t.Fatal("invalid visible UTF8 accepted")
	}
	if m.Used() != 0 {
		t.Fatal("UTF8 failure leak")
	}
	path := stringStructFixture(t, []int{17}, 1800)
	src, e = OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts.BatchRows = 65
	opts.BatchPayloadTargetBytes = 256
	opts.BatchPayloadBytes = 32 << 10
	opts.MemoryManager = m
	st, e = source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	want := make([]int, 17)
	for i := range want {
		want[i] = i
	}
	stringStructCollect(t, st, opts, want, 1800)
	if m.Used() != 0 {
		t.Fatal("payload target leak")
	}
}

type stringStructCopyContext struct {
	context.Context
	stream *nestedListStream
	checks int
}

func (c *stringStructCopyContext) Err() error {
	large := false
	for i := range c.stream.cursors {
		if len(c.stream.cursors[i].replacementData) > 64<<10 {
			large = true
		}
	}
	if !large {
		return c.Context.Err()
	}
	var pcs [24]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, ".copyBytes") {
			c.checks++
			if c.checks == 4 {
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
func TestNativeStructStringCopyCancellation(t *testing.T) {
	path := stringStructFixture(t, []int{5}, 32768)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 5
	opts.BatchBytes = 2 << 20
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	native, ok := st.(*nestedListStream)
	if !ok {
		t.Fatalf("wrong stream %T", st)
	}
	ctx := &stringStructCopyContext{Context: context.Background(), stream: native}
	dst := memory.NewBatch()
	ok, e = st.Next(ctx, dst)
	st.Close()
	if ok || !errors.Is(e, context.Canceled) || ctx.checks != 4 || dst.Rows != 0 {
		t.Fatal("large copy checkpoint cancellation", ok, e, ctx.checks, dst.Rows)
	}
	if m.Used() != 0 {
		t.Fatal("copy cancel lease leak")
	}
	// The prefix places a three-byte rune across the32KiB checkpoint.
	st, e = source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	stringStructCollect(t, st, opts, []int{0, 1, 2, 3, 4}, 32768)
	if m.Used() != 0 {
		t.Fatal("long UTF8 success cleanup")
	}
}

func TestNativeStructStringPendingHeaderRebuild(t *testing.T) {
	path := stringStructFixture(t, []int{200}, 180)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 1000
	opts.BatchBytes = 18 << 10
	m := memory.NewBudgetManager(0)
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	native := st.(*nestedListStream)
	cursor, rebuild := 0, false
	for {
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if native.capacity == 1 && cursor < 190 {
			rebuild = true
		}
		want := make([]int, b.Rows)
		for i := range want {
			want[i] = cursor + i
		}
		stringStructCheck(t, b, want, 180)
		cursor += b.Rows
	}
	st.Close()
	if cursor != 200 || !rebuild {
		t.Fatal("header rebuild branch not exercised", cursor, rebuild)
	}
	if m.Used() != 0 {
		t.Fatal("pending rebuild leak")
	}
}
func TestNativeStructStringLateParentMismatch(t *testing.T) {
	path := stringStructFixtureVariant(t, []int{3}, 4, true)
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
	if e == nil || !strings.Contains(e.Error(), "parent") {
		t.Fatal("inconsistent parent after String growth", e)
	}
	if m.Used() != 0 {
		t.Fatal("late parent failure leak")
	}
}

type stringStructSecondLeaseFailure struct {
	memory.Manager
	native *nestedListStream
	hit    bool
}

func (m *stringStructSecondLeaseFailure) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		for i := range m.native.cursors {
			if m.native.cursors[i].replacementLease != nil {
				m.hit = true
				return nil, memory.ErrBudgetExceeded
			}
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeStructStringSecondReplacementRollback(t *testing.T) {
	path := stringStructFixture(t, []int{17}, 180)
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	opts := testReadOptions()
	opts.BatchRows = 17
	opts.BatchBytes = 64 << 10
	m := &stringStructSecondLeaseFailure{Manager: memory.NewBudgetManager(0)}
	opts.MemoryManager = m
	st, e := source.OpenBatchStream(context.Background(), src, opts)
	if e != nil {
		t.Fatal(e)
	}
	m.native = st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil || !m.hit || b.Rows != 2 || !stringStructPending(m.native) {
		t.Fatal("second replacement must publish exact prefix", ok, e, m.hit, b.Rows, stringStructPending(m.native))
	}
	stringStructCheck(t, b, []int{0, 1}, 180)
	for i := range m.native.cursors {
		if m.native.cursors[i].replacementLease != nil || m.native.cursors[i].replacementData != nil {
			t.Fatal("replacement rollback incomplete")
		}
	}
	want := make([]int, 15)
	for i := range want {
		want[i] = i + 2
	}
	stringStructCollect(t, st, opts, want, 180)
	if m.Used() != 0 {
		t.Fatal("second replacement cleanup")
	}
}
func TestNativeStructStringOversizedPendingBeforeOwnedCopy(t *testing.T) {
	path := stringStructFixture(t, []int{5}, 1<<18)
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
	native := st.(*nestedListStream)
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil || b.Rows != 4 || !stringStructPending(native) {
		t.Fatal("oversized pending prefix", ok, e, b.Rows)
	}
	borrowed := false
	for i := range native.cursors {
		slot := native.cursors[i]
		if len(slot.bytes[0]) > 1<<20 {
			borrowed = true
		}
		if slot.replacementData != nil || slot.replacementLease != nil {
			t.Fatal("oversized unadmitted owned replacement")
		}
		if slot.scalarOutput != nil && len(slot.scalarOutput.Data) > 1024 {
			t.Fatal("unadmitted bytes copied into output")
		}
	}
	if !borrowed {
		t.Fatal("fixture did not prefetch oversized borrowed view")
	}
	if _, e = st.Next(context.Background(), memory.NewBatch()); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("single row budget", e)
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("oversized cleanup")
	}
}

// Unified scalar cursors retain paid tuples until atomic commit.
func stringStructPending(s *nestedListStream) bool {
	for _, c := range s.cursors {
		if c.tuple.present || c.complete {
			return true
		}
	}
	return false
}
