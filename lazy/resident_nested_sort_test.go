package lazy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/memory"
)

func residentNestedRecord(t *testing.T, part int) arrow.RecordBatch {
	t.Helper()
	base := nestedListCArrow(t, am.DefaultAllocator, 8)
	defer base.Release()
	ls := listStructBArrow(t, am.DefaultAllocator, 7, 65)
	defer ls.Release()
	seq, key := array.NewInt64Builder(am.DefaultAllocator), array.NewInt64Builder(am.DefaultAllocator)
	defer seq.Release()
	defer key.Release()
	for i := 0; i < 65; i++ {
		seq.Append(int64(part*65 + i))
		if i%7 == 0 {
			key.AppendNull()
		} else {
			key.Append(int64(i % 5))
		}
	}
	sa, ka := seq.NewArray(), key.NewArray()
	defer sa.Release()
	defer ka.Release()
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	fields = append(fields, arrow.Field{Name: "recordOuter", Type: ls.Column(2).DataType(), Nullable: true}, arrow.Field{Name: "seq", Type: arrow.PrimitiveTypes.Int64}, arrow.Field{Name: "key", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	cols := append([]arrow.Array(nil), base.Columns()...)
	cols = append(cols, ls.Column(2), sa, ka)
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 65)
}
func residentNestedQuery(t *testing.T, s *Session) LazyFrame {
	t.Helper()
	a, b, c := residentNestedRecord(t, 0), residentNestedRecord(t, 1), residentNestedRecord(t, 2)
	defer a.Release()
	defer b.Release()
	defer c.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, a, b, c)
	if e != nil {
		t.Fatal(e)
	}
	return q
}
func residentNestedOrder(desc bool) []int {
	rows := make([]int, 195)
	for i := range rows {
		rows[i] = i
	}
	// Default Sort puts NULL last; stable ties retain global input ordinal.
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i]%65, rows[j]%65
		an, bn := a%7 == 0, b%7 == 0
		if an != bn {
			return !an
		}
		if an {
			return false
		}
		if desc {
			return a%5 > b%5
		}
		return a%5 < b%5
	})
	return rows
}
func residentNestedCheck(t *testing.T, df DataFrame, order []int) {
	t.Helper()
	for _, name := range []string{"ints", "texts", "outer", "id", "index", "oldints", "records", "recordOuter", "seq", "key"} {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := make([]any, len(order))
		for i, seq := range order {
			r := seq % 65
			switch name {
			case "seq":
				want[i] = int64(seq)
			case "key":
				if r%7 != 0 {
					want[i] = int64(r % 5)
				}
			case "recordOuter":
				want[i] = listStructBOuter(r, 1, 7)
			default:
				want[i] = nestedListCRaw(r, 8)[name]
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("resident sorted %s got=%#v want=%#v", name, got, want)
		}
	}
}
func TestResidentNestedSortEndToEnd(t *testing.T) {
	for _, workers := range []int{1, 3} {
		for _, desc := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers%d/desc%v", workers, desc), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.Workers = workers
				opts.PreparedCacheBytes = 0
				opts.MemoryLimitBytes = 64 << 20
				// No spill directory or disk quota. This must stay resident, not external fallback.
				s := NewSession(opts)
				defer s.Close()
				input := residentNestedQuery(t, s)
				key := Asc("key")
				if desc {
					key = Desc("key")
				}
				q := input.Sort(key)
				want := residentNestedOrder(desc)
				budget, e := s.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				df, e := q.Collect(memory.WithQueryBudget(context.Background(), budget))
				if e != nil {
					budget.Close()
					t.Fatal(e)
				}
				defer df.Close()
				residentNestedCheck(t, df, want)
				if budget.Snapshot().SpilledBytes != 0 {
					t.Fatal("resident silently spilled")
				}
				budget.Close()
				rd, e := q.CollectBatches(context.Background(), BatchOptions{})
				if e != nil {
					t.Fatal(e)
				}
				if !rd.Next(context.Background()) {
					t.Fatal(rd.Err())
				}
				view := rd.Batch()
				rd.Close()
				if view.Valid() {
					t.Fatal("resident borrow survived Close")
				}
				ad := structNestedIPC(t, s, q, filepath.Join(t.TempDir(), "sorted.arrow"))
				defer ad.Close()
				residentNestedCheck(t, ad, want)
				path := filepath.Join(t.TempDir(), "sorted.parquet")
				if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
					t.Fatal(e)
				}
				pq, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				pd, e := pq.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer pd.Close()
				residentNestedCheck(t, pd, want)
				s.Close()
				residentNestedCheck(t, df, want)
				residentNestedCheck(t, ad, want)
				residentNestedCheck(t, pd, want)
				df.Close()
				ad.Close()
				pd.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("resident ownership leak")
				}
			})
		}
	}
}
func TestResidentNestedSortBoundaries(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()
	q := residentNestedQuery(t, s)
	for _, bad := range []LazyFrame{q.Sort(Asc("ints")), q.Sort(Asc("records"))} {
		df, e := bad.Collect(context.Background())
		if e == nil {
			df.Close()
			t.Fatal("nested key boundary expanded")
		}
	}
	top, te := q.TopK(3, Asc("key")).Collect(context.Background())
	if te != nil {
		t.Fatal(te)
	}
	residentNestedCheck(t, top, residentNestedOrder(false)[:3])
	top.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e := q.Sort(Asc("key")).Collect(ctx)
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, context.Canceled) {
		t.Fatal("cancel identity", e)
	}
	tinyOpts := opts
	tinyOpts.MemoryLimitBytes = 1
	tiny := NewSession(tinyOpts)
	defer tiny.Close()
	df, e = residentNestedQuery(t, tiny).Sort(Asc("key")).Collect(context.Background())
	if e == nil {
		df.Close()
	}
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("budget identity", e)
	}
	tiny.Close()
	if tiny.Engine().Memory().Used() != 0 {
		t.Fatal("denial cleanup")
	}
}

func TestResidentNestedSortBinaryOpaqueAndDecimalBoundary(t *testing.T) {
	base := residentNestedRecord(t, 0)
	defer base.Release()
	bin := array.NewBinaryBuilder(am.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer bin.Release()
	opaque := array.NewLargeStringBuilder(am.DefaultAllocator)
	defer opaque.Release()
	decType := &arrow.Decimal128Type{Precision: 9, Scale: 2}
	dec := array.NewDecimal128Builder(am.DefaultAllocator, decType)
	defer dec.Release()
	for r := 0; r < 65; r++ {
		bin.Append([]byte{byte(r), 0, 255})
		opaque.Append(fmt.Sprintf("opaque-%d-中", r))
		dec.Append(decimal128.FromI64(int64(r)))
	}
	ba, oa, da := bin.NewArray(), opaque.NewArray(), dec.NewArray()
	defer ba.Release()
	defer oa.Release()
	defer da.Release()
	fields := append([]arrow.Field(nil), base.Schema().Fields()...)
	fields = append(fields, arrow.Field{Name: "binary", Type: arrow.BinaryTypes.Binary}, arrow.Field{Name: "opaque", Type: arrow.BinaryTypes.LargeString})
	cols := append([]arrow.Array(nil), base.Columns()...)
	cols = append(cols, ba, oa)
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 65)
	defer rec.Release()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, rec)
	if e != nil {
		t.Fatal(e)
	}
	sorted := q.Sort(Desc("seq"))
	df, e := sorted.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	c, e := df.Column("ints")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	for i, v := range got {
		if !reflect.DeepEqual(v, nestedListBRaw(64-i, false)) {
			t.Fatal("opaque mixed native payload", i)
		}
	}
	checkOpaque := func() {
		oc, err := df.Column("opaque")
		if err != nil {
			t.Fatal(err)
		}
		oc.state.mu.RLock()
		defer oc.state.mu.RUnlock()
		spans, err := oc.lockedSpans()
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for _, span := range spans {
			v := span.batch.Opaque["opaque"]
			if v == nil {
				t.Fatal("missing retained opaque")
			}
			arr, ok := v.Value.(*array.LargeString)
			if !ok {
				t.Fatalf("opaque type %T", v.Value)
			}
			for row := span.start; row < span.end; row++ {
				if got := arr.Value(row); got != fmt.Sprintf("opaque-%d-中", 64-seen) {
					t.Fatal("opaque order", seen, got)
				}
				seen++
			}
		}
		if seen != 65 {
			t.Fatal("opaque row count", seen)
		}
	}
	checkOpaque()
	// Generic IPC export never admitted TypeArrow without a dedicated adapter.
	path := filepath.Join(t.TempDir(), "mixed.arrow")
	usedBeforeRejectedIPC := s.Engine().Memory().Used()
	if _, e = sorted.SinkIPC(context.Background(), path, IPCSinkOptions{}); e == nil || !strings.Contains(e.Error(), "explicit Arrow type adapter") {
		t.Fatal("opaque IPC boundary changed", e)
	}
	if s.Engine().Memory().Used() != usedBeforeRejectedIPC {
		t.Fatal("opaque IPC denial retained memory", usedBeforeRejectedIPC, s.Engine().Memory().Used())
	}
	if _, e = sorted.Select("binary", "seq").SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer file.Close()
	rd, e := ipc.NewFileReader(file)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	seen := 0
	for i := 0; i < rd.NumRecords(); i++ {
		r, e := rd.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		for j := 0; j < int(r.NumRows()); j++ {
			want := 64 - seen
			b := r.Column(0).(*array.Binary).Value(j)
			ordinal := r.Column(1).(*array.Int64).Value(j)
			if !reflect.DeepEqual(b, []byte{byte(want), 0, 255}) || ordinal != int64(want) {
				t.Fatal("binary IPC order", seen, b, ordinal)
			}
			seen++
		}
		r.Release()
	}
	if seen != 65 {
		t.Fatal("binary IPC rows", seen)
	}
	decimalRec := array.NewRecordBatch(arrow.NewSchema(append(fields, arrow.Field{Name: "decimal", Type: decType}), nil), append(cols, da), 65)
	defer decimalRec.Release()
	dq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, decimalRec)
	if e != nil {
		t.Fatal(e)
	}
	dd, e := dq.Select("decimal").Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	dc, e := dd.Column("decimal")
	if e != nil {
		t.Fatal(e)
	}
	chunks, e := dc.Decimal128Chunks()
	if e != nil || len(chunks) == 0 {
		t.Fatal(e)
	}
	v, valid, e := chunks[0].Value(0)
	if e != nil || !valid || v != decimal128.FromI64(0) {
		t.Fatal("D1 changed", v, valid, e)
	}
	dd.Close()
	retainedFirst := s.Engine().Memory().Used()
	t.Logf("first mixed owned result retained before independent Decimal case: %d", retainedFirst)
	coexistent, e := dq.Sort(Asc("seq")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			coexistent.Close()
		}
		t.Fatal("expected simultaneous owned-results budget denial", e)
	}
	if s.Engine().Memory().Used() != retainedFirst {
		t.Fatal("failed second Collect changed first owner charge")
	}
	s.Close()
	checkOpaque()
	again, e := c.ToAnySlice()
	if e != nil || !reflect.DeepEqual(got, again) {
		t.Fatal("mixed source-close ownership", e)
	}
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("mixed owner leak")
	}
	// Complete original source-close contract before a separately owned 65-row
	// Decimal result. Same finite 64 MiB limit; no overlapping unrelated result.
	s2 := NewSession(opts)
	defer s2.Close()
	dq2, e := s2.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, decimalRec)
	if e != nil {
		t.Fatal(e)
	}
	decimalSorted, e := dq2.Sort(Asc("seq")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	decimalColumn, e := decimalSorted.Column("decimal")
	if e != nil {
		t.Fatal(e)
	}
	decimalChunks, e := decimalColumn.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	decimalSeen := 0
	for _, ch := range decimalChunks {
		if ch.Precision() != 9 || ch.Scale() != 2 {
			t.Fatal("decimal payload metadata")
		}
		for i := 0; i < ch.Len(); i++ {
			v, ok, e := ch.Value(i)
			if e != nil || !ok || v != decimal128.FromI64(int64(decimalSeen)) {
				t.Fatal("decimal payload order", decimalSeen, e)
			}
			decimalSeen++
		}
	}
	if decimalSeen != 65 {
		t.Fatal(decimalSeen)
	}
	decimalSorted.Close()

	s2.Close()
	if s2.Engine().Memory().Used() != 0 {
		t.Fatal("independent Decimal owner leak")
	}

}
