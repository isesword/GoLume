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
	"runtime"
	"strings"
	"testing"
)

type multiListKind struct {
	name         string
	outer, item  bool
	shift, large int
}

var multiListKinds = []multiListKind{{"alpha", false, true, 0, 3}, {"beta", true, false, 2, 31}, {"gamma", true, true, 4, 257}}

func multiListFixture(t *testing.T, groups []int, corruptLast bool, props ...arrowparquet.WriterProperty) string {
	return multiListFixtureKinds(t, groups, corruptLast, multiListKinds, props...)
}
func multiListFixtureKinds(t *testing.T, groups []int, corruptLast bool, kinds []multiListKind, props ...arrowparquet.WriterProperty) string {
	t.Helper()
	id, e := schema.NewPrimitiveNodeLogical("id", arrowparquet.Repetitions.Required, schema.NoLogicalType{}, arrowparquet.Types.Int64, -1, -1)
	if e != nil {
		t.Fatal(e)
	}
	fields := schema.FieldList{id}
	for _, k := range kinds {
		er, lr := arrowparquet.Repetitions.Required, arrowparquet.Repetitions.Required
		if k.outer {
			lr = arrowparquet.Repetitions.Optional
		}
		if k.item {
			er = arrowparquet.Repetitions.Optional
		}
		el, e := schema.NewPrimitiveNodeLogical("element", er, schema.NewIntLogicalType(64, true), arrowparquet.Types.Int64, -1, -1)
		if e != nil {
			t.Fatal(e)
		}
		middle, e := schema.NewGroupNode("list", arrowparquet.Repetitions.Repeated, schema.FieldList{el}, -1)
		if e != nil {
			t.Fatal(e)
		}
		list, e := schema.NewGroupNodeLogical(k.name, lr, schema.FieldList{middle}, schema.ListLogicalType{}, -1)
		if e != nil {
			t.Fatal(e)
		}
		fields = append(fields, list)
	}
	root, e := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, fields, -1)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "multi.parquet")
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
		for col, k := range kinds {
			c, e = rg.NextColumn()
			if e != nil {
				t.Fatal(e)
			}
			values := []int64{}
			defs, reps := []int16{}, []int16{}
			L, E := int16(0), int16(0)
			if k.outer {
				L = 1
			}
			if k.item {
				E = 1
			}
			for i := 0; i < n; i++ {
				r := listNativeExpected(base+i+k.shift, k.large, k.outer, k.item)
				if !r.valid {
					defs = append(defs, 0)
					reps = append(reps, 0)
					continue
				}
				if len(r.values) == 0 {
					defs = append(defs, L)
					reps = append(reps, 0)
					continue
				}
				for j, v := range r.values {
					d, rep := L+1, int16(0)
					if j > 0 {
						rep = 1
					}
					if r.valids[j] {
						d += E
						values = append(values, v)
					}
					defs = append(defs, d)
					reps = append(reps, rep)
				}
			}
			// Keep the number of rep0 rows unchanged but move the first boundary to
			// an interior continuation. This creates a malformed first level in the
			// final List without relying on mismatched writer row counts.
			if corruptLast && col == 2 && len(reps) > 1 {
				reps[0] = 1
				reps[1] = 0
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
func multiListCheck(t *testing.T, b *memory.Batch, want []int, names []string) {
	t.Helper()
	for _, name := range names {
		if name == "id" {
			for i, row := range want {
				if b.Int64s["id"].Values[i] != int64(row) {
					t.Fatal("fixed sibling order")
				}
			}
			continue
		}
		var k multiListKind
		for _, candidate := range multiListKinds {
			if candidate.name == name {
				k = candidate
			}
		}
		v := b.Int64Lists[name]
		if v == nil || len(v.Offsets) != b.Rows+1 || v.Offsets[0] != 0 {
			t.Fatal("list shape", name)
		}
		for i, row := range want {
			r := listNativeExpected(row+k.shift, k.large, k.outer, k.item)
			if v.IsValid(i) != r.valid {
				t.Fatal("parent validity", name, row)
			}
			lo, hi := int(v.Offsets[i]), int(v.Offsets[i+1])
			if hi-lo != len(r.values) {
				t.Fatal("length", name, row, hi-lo, len(r.values))
			}
			for j, x := range r.values {
				if v.Values.IsValid(lo+j) != r.valids[j] {
					t.Fatal("element validity", name, row, j)
				}
				if !r.valids[j] {
					x = 0
				}
				if v.Values.Values[lo+j] != x {
					t.Fatal("element value", name, row, j)
				}
			}
		}
	}
}
func multiListCollect(t *testing.T, st source.BatchStream, opts source.ReadOptions, want []int, names []string) {
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
		multiListCheck(t, b, want[cursor:cursor+b.Rows], names)
		cursor += b.Rows
	}
	if cursor != len(want) {
		t.Fatal("lost coordinated row", cursor, len(want))
	}
}
func TestNativeInt64ListsIndependentLevels(t *testing.T) {
	for _, dict := range []bool{false, true} {
		for _, version := range []arrowparquet.DataPageVersion{arrowparquet.DataPageV1, arrowparquet.DataPageV2} {
			t.Run(fmt.Sprintf("dict%v/pageEnum%d", dict, version), func(t *testing.T) {
				path := multiListFixture(t, []int{0, 17, 66}, false, arrowparquet.WithDictionaryDefault(dict), arrowparquet.WithDataPageVersion(version))
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				fields := src.Schema().Fields
				for j, k := range multiListKinds {
					f := fields[j+1]
					if f.Name != k.name || f.Type != memory.TypeList || f.Nullable != k.outer || f.Children[0].Nullable != k.item {
						t.Fatal("metadata")
					}
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
					multiListCollect(t, st, opts, want, []string{"id", "alpha", "beta", "gamma"})
					if m.Used() != 0 {
						t.Fatal("EOF leak")
					}
				}
			})
		}
	}
}
func TestNativeInt64ListsPartitionProjectionScope(t *testing.T) {
	path := multiListFixture(t, []int{3, 5, 7}, false)
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
	for _, names := range [][]string{{"gamma", "id", "alpha"}, {"beta"}, {"id"}} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: names})
		if e != nil {
			t.Fatal(e)
		}
		opts := testReadOptions()
		opts.BatchRows = 2
		opts.BatchBytes = 64 << 10
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), projected, opts, []source.Partition{parts[2], parts[0]})
		if e != nil {
			t.Fatal(e)
		}
		multiListCollect(t, st, opts, []int{8, 9, 10, 11, 12, 13, 14, 0, 1, 2}, names)
	}
}
func TestNativeInt64ListsFailureCancelAndLastMalformed(t *testing.T) {
	path := multiListFixture(t, []int{17, 19}, false)
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
					t.Fatal("recover lost parent", cursor)
				}
				return nil
			}
			want := make([]int, b.Rows)
			for i := range want {
				want[i] = cursor + i
			}
			multiListCheck(t, b, want, []string{"id", "alpha", "beta", "gamma"})
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
			t.Fatal("fault leak")
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
		t.Fatal("multi middecode cancellation", ok, e, ctx.checks, b.Rows)
	}
	if m.Used() != 0 {
		t.Fatal("cancel leak")
	}
	path = multiListFixture(t, []int{5}, true)
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
		t.Fatal("last List malformed levels accepted")
	}
	if m.Used() != 0 {
		t.Fatal("last list cleanup")
	}
}

func TestNativeInt64ListsIndependentMetadataCounts(t *testing.T) {
	for column := 1; column <= 3; column++ {
		for _, delta := range []int64{-1, 1} {
			t.Run(fmt.Sprintf("column%d/delta%d", column, delta), func(t *testing.T) {
				path := multiListFixture(t, []int{5}, false)
				src, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				src.meta.RowGroups[0].Columns[column].MetaData.NumValues += delta
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
					t.Fatal("per-list count mismatch accepted")
				}
				if m.Used() != 0 {
					t.Fatal("metadata failure cleanup")
				}
			})
		}
	}
}
func TestNativeInt64ListsAggregateHardPayload(t *testing.T) {
	kinds := []multiListKind{{"alpha", true, true, 0, 257}, {"beta", true, true, 7, 257}, {"gamma", true, true, 14, 257}}
	path := multiListFixtureKinds(t, []int{4, 1}, false, kinds)
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
	opts.BatchBytes = 64 << 10
	opts.BatchPayloadBytes = 4096
	for _, names := range [][]string{{"alpha"}, {"beta"}, {"gamma"}, {"alpha", "beta", "gamma"}} {
		projected, _, e := src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: names})
		if e != nil {
			t.Fatal(e)
		}
		m := memory.NewBudgetManager(0)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), projected, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if len(names) == 1 {
			if !ok || e != nil || len(b.Int64Lists[names[0]].Values.Values) != 257 {
				t.Fatal("individual legal row", e)
			}
		} else if ok || !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("aggregate hard limit", ok, e)
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("aggregate cleanup")
		}
	}
}

type multiListFaultManager struct {
	memory.Manager
	native                         *nestedListStream
	stageIndex, replacementOrdinal int
	hit                            bool
}

func (m *multiListFaultManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	if m.native != nil && !m.hit {
		if m.stageIndex >= 0 && m.native.current == m.stageIndex {
			c := m.native.cursors[m.stageIndex]
			if len(c.stage) >= 8 && len(c.output.Values.Values) > 0 {
				m.hit = true
				return nil, memory.ErrBudgetExceeded
			}
		}
		if m.replacementOrdinal != 0 && m.native.current == len(m.native.cursors) {
			ordinal := m.replacementOrdinal
			mixed := false
			if ordinal < 0 {
				ordinal = -ordinal
				for _, c := range m.native.cursors {
					if len(c.output.Values.Values) == 0 && len(c.stage) > 0 {
						mixed = true
					}
				}
				if !mixed {
					return m.Manager.Reserve(ctx, n)
				}
			}
			leases := 0
			for _, c := range m.native.cursors {
				if c.replacementLease != nil {
					leases++
				}
			}
			if leases == ordinal-1 {
				m.hit = true
				return nil, memory.ErrBudgetExceeded
			}
		}
	}
	return m.Manager.Reserve(ctx, n)
}
func TestNativeInt64ListsAtomicBudgetRecovery(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stage, ordinal int
	}{{"stage_second", 1, 0}, {"stage_third", 2, 0}, {"replacement_second", -1, 2}, {"replacement_third", -1, 3}, {"mixed_transfer_replacement", -1, -2}} {
		t.Run(tc.name, func(t *testing.T) {
			path := multiListFixture(t, []int{17}, false)
			src, e := OpenFileWithOptions(path, Options{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			opts := testReadOptions()
			opts.BatchRows = 17
			opts.BatchBytes = 128 << 10
			m := &multiListFaultManager{Manager: memory.NewBudgetManager(0), stageIndex: tc.stage, replacementOrdinal: tc.ordinal}
			opts.MemoryManager = m
			st, e := source.OpenBatchStream(context.Background(), src, opts)
			if e != nil {
				t.Fatal(e)
			}
			m.native = st.(*nestedListStream)
			b := memory.NewBatch()
			ok, e := st.Next(context.Background(), b)
			if !ok || e != nil || !m.hit || b.Rows == 0 || b.Rows >= 17 {
				t.Fatal("must publish only committed prefix", ok, e, m.hit, b.Rows)
			}
			prefix := b.Rows
			want := make([]int, prefix)
			for i := range want {
				want[i] = i
			}
			multiListCheck(t, b, want, []string{"id", "alpha", "beta", "gamma"})
			if tc.stage >= 0 {
				if m.native.current != tc.stage || m.native.cursors[tc.stage].complete || len(m.native.cursors[tc.stage].stage) != 8 || !m.native.cursors[tc.stage].tuple.present {
					t.Fatal("partial cursor lost")
				}
				for i, c := range m.native.cursors {
					if i < tc.stage && !c.complete {
						t.Fatal("earlier complete cursor lost")
					}
					if i > tc.stage && c.mode != 0 {
						t.Fatal("later cursor consumed prematurely")
					}
				}
			} else {
				for _, c := range m.native.cursors {
					if !c.complete || c.replacementLease != nil || c.replacement != nil {
						t.Fatal("atomic replacement rollback incomplete")
					}
				}
			}
			want = make([]int, 17-prefix)
			for i := range want {
				want[i] = prefix + i
			}
			multiListCollect(t, st, opts, want, []string{"id", "alpha", "beta", "gamma"})
			if m.Used() != 0 {
				t.Fatal("coordinated retry leak")
			}
		})
	}
}
func TestNativeInt64ListsTightStagePeak(t *testing.T) {
	// Exact native peak: metadata10240 + one-row headers64 + old128-element
	// stage1040 + replacement256-element stage2080 =13424, below13500.
	// The second List's one NULL element needs16 more bytes after that growth;
	// it must not be charged for the already-live first List a second time.
	kinds := []multiListKind{{"alpha", true, true, 0, 256}, {"beta", true, true, 5, 1}, {"gamma", true, true, 4, 1}}
	path := multiListFixtureKinds(t, []int{4, 1}, false, kinds)
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
	opts.BatchBytes = 13500
	m := newBatchStreamPeakManager(13500)
	opts.MemoryManager = m
	st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if !ok || e != nil {
		t.Fatalf("legal exact staging peak rejected: ok%v err%v peak%d", ok, e, m.peak.Load())
	}
	if len(b.Int64Lists["alpha"].Values.Values) != 256 || len(b.Int64Lists["beta"].Values.Values) != 1 || b.Int64Lists["beta"].Values.IsValid(0) || len(b.Int64Lists["gamma"].Values.Values) != 0 {
		t.Fatal("tight-budget row values")
	}
	if m.peak.Load() > 13500 {
		t.Fatal("peak exceeded")
	}
	st.Close()
	if m.Used() != 0 {
		t.Fatal("tight budget leak")
	}
}

type multiListObserveContext struct {
	context.Context
	native     *nestedListStream
	leases     []memory.Reservation
	ptrs       []*int64
	cancelCopy bool
	checks     int
}

func (c *multiListObserveContext) Err() error {
	all := true
	for _, v := range c.native.cursors {
		if len(v.stage) < 4097 {
			all = false
		}
	}
	if all {
		c.leases = nil
		c.ptrs = nil
		for _, v := range c.native.cursors {
			c.leases = append(c.leases, v.stageLease)
			c.ptrs = append(c.ptrs, &v.stage[0])
		}
	}
	if c.cancelCopy && c.native.current == 2 && len(c.native.cursors[2].stage) >= 2048 {
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
func TestNativeInt64ListsAllStageTransferAndLateCopyCancel(t *testing.T) {
	kinds := []multiListKind{{"alpha", true, true, 0, 4097}, {"beta", true, true, 7, 4097}, {"gamma", true, true, 14, 4097}}
	path := multiListFixtureKinds(t, []int{4, 1}, false, kinds)
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
	opts.BatchBytes = 512 << 10
	for _, cancelCopy := range []bool{false, true} {
		m := newBatchStreamPeakManager(opts.BatchBytes)
		opts.MemoryManager = m
		st, e := source.OpenBatchStreamWithPartitions(context.Background(), src, opts, []source.Partition{parts[1]})
		if e != nil {
			t.Fatal(e)
		}
		native := st.(*nestedListStream)
		ctx := &multiListObserveContext{Context: context.Background(), native: native, cancelCopy: cancelCopy}
		b := memory.NewBatch()
		ok, e := st.Next(ctx, b)
		if cancelCopy {
			if ok || !errors.Is(e, context.Canceled) || ctx.checks != 2 || b.Rows != 0 {
				t.Fatal("last cursor copy cancel", ok, e, ctx.checks)
			}
		} else {
			if !ok || e != nil || len(ctx.leases) != 3 {
				t.Fatal("allstage observation", ok, e, len(ctx.leases))
			}
			for i, k := range kinds {
				v := b.Int64Lists[k.name]
				r := listNativeExpected(4+k.shift, k.large, true, true)
				for j, x := range r.values {
					if !r.valids[j] {
						x = 0
					}
					if v.Values.IsValid(j) != r.valids[j] || v.Values.Values[j] != x {
						t.Fatal("large column exact", i, j)
					}
				}
				c := native.cursors[i]
				if c.outputLease != ctx.leases[i] || c.stageLease != nil || c.stage != nil || &v.Values.Values[0] != ctx.ptrs[i] {
					t.Fatal("atomic stage lease/buffer transfer", i)
				}
			}
			if ok, e = st.Next(context.Background(), memory.NewBatch()); ok || e != nil {
				t.Fatal("all columns EOF", ok, e)
			}
		}
		st.Close()
		if m.Used() != 0 || m.peak.Load() > opts.BatchBytes {
			t.Fatal("allstage peak/cleanup")
		}
	}
}
