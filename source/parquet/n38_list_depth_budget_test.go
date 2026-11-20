package parquet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

func n38BudgetTree(depth, n int, text bool, bytes int) any {
	a := make([]any, n)
	for i := range a {
		if i%7 == 1 {
			a[i] = nil
		} else if text {
			a[i] = strings.Repeat("界", bytes/3) + fmt.Sprintf("/%d\x00", i)
		} else {
			a[i] = int64(i*13 - 17)
		}
	}
	var v any = a
	for d := 1; d < depth; d++ {
		v = []any{nil, []any{}, v}
	}
	return v
}
func n38BudgetAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch dst := b.(type) {
	case *array.ListBuilder:
		dst.Append(true)
		for _, x := range v.([]any) {
			n38BudgetAppend(dst.ValueBuilder(), x)
		}
	case *array.Int64Builder:
		dst.Append(v.(int64))
	case *array.StringBuilder:
		dst.Append(v.(string))
	default:
		panic("unsupported independent fixture builder")
	}
}
func n38BudgetFile(t *testing.T, depth int, text bool, rows []any) string {
	t.Helper()
	var typ arrow.DataType = arrow.PrimitiveTypes.Int64
	if text {
		typ = arrow.BinaryTypes.String
	}
	for i := 0; i < depth; i++ {
		typ = arrow.ListOfField(arrow.Field{Name: "item", Type: typ, Nullable: true})
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "items", Type: typ, Nullable: true}}, nil)
	b := array.NewBuilder(am.DefaultAllocator, typ)
	defer b.Release()
	for _, r := range rows {
		n38BudgetAppend(b, r)
	}
	a := b.NewArray()
	defer a.Release()
	r := array.NewRecordBatch(schema, []arrow.Array{a}, int64(len(rows)))
	defer r.Release()
	path := filepath.Join(t.TempDir(), "depth.parquet")
	fp, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := pqarrow.NewFileWriter(schema, fp, pq.NewWriterProperties(pq.WithDictionaryDefault(false)), pqarrow.DefaultWriterProps())
	if e != nil {
		fp.Close()
		t.Fatal(e)
	}
	if e = w.Write(r); e != nil {
		w.Close()
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	return path
}
func n38BudgetValue(v *memory.ArrayNode, row int) any {
	if !v.IsValid(row) {
		return nil
	}
	if v.Kind == memory.TypeInt64 {
		return v.Int64.Values[row]
	}
	if v.Kind == memory.TypeString {
		lo, hi := v.Strings.Offsets[row], v.Strings.Offsets[row+1]
		return string(v.Strings.Data[lo:hi])
	}
	lo, hi, _ := v.ValueRange(row)
	out := make([]any, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, n38BudgetValue(v.Child, i))
	}
	return out
}

type n38BudgetProbe struct {
	liveStage     func() int64
	overlaps      int
	assertOverlap func(bool)
	memory.Manager
	calls, failAt, cancelAt int
	peak, maxUsedBefore     int64
	trace                   []string
	cancel                  context.CancelFunc
}

func (m *n38BudgetProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	used := m.Used()
	m.trace = append(m.trace, fmt.Sprintf("%d:%d+%d", m.calls, used, n))
	if used > m.maxUsedBefore {
		m.maxUsedBefore = used
	}
	// Exhaustion persists across the reader's documented shrink/retry path.
	if m.failAt > 0 && m.calls >= m.failAt || m.failAt < 0 && m.calls == -m.failAt {
		return nil, memory.ErrBudgetExceeded
	}
	oldStage := int64(0)
	if m.liveStage != nil {
		oldStage = m.liveStage()
	}
	r, e := m.Manager.Reserve(ctx, n)
	if e == nil && oldStage > 0 {
		m.overlaps++
		m.assertOverlap(used >= oldStage && m.Used() >= used+n)
		m.trace = append(m.trace, fmt.Sprintf("oldstage=%d new=%d simultaneous=%d", oldStage, n, m.Used()))
	}
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if e == nil && m.calls == m.cancelAt {
		m.cancel()
	}
	return r, e
}

type n38BudgetMeasure struct {
	peak    int64
	calls   int
	trace   []string
	overlap int64
	growth  int
}

func n38BudgetRun(t *testing.T, path string, wantRows []any, limit, batchBytes int64, fail, cancelAt int, want error, keepOwner bool) n38BudgetMeasure {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &n38BudgetProbe{Manager: memory.NewBudgetManager(limit), failAt: fail, cancelAt: cancelAt, cancel: cancel}
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	field, ok := src.Schema().Field("items")
	if !ok {
		t.Fatal("missing field")
	}
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m, BatchRows: 3, BatchBytes: batchBytes}
	st, e := src.NewBatchStream(ctx, opts)
	m.assertOverlap = func(ok bool) {
		if !ok {
			t.Fatal("old stage released before replacement reservation")
		}
	}
	if raw, ok := st.(*nestedListStream); ok {
		m.liveStage = func() int64 {
			var n int64
			for _, c := range raw.cursors {
				if c.array != nil && c.array.stage.lease != nil {
					n += c.array.stage.charge
				}
			}
			return n
		}
	}
	rows := 0
	var last *memory.OwnedNestedList
	var lastExpected []any
	var combined int64
	if e == nil {
		b := memory.NewBatch()
		for {
			var ok bool
			ok, e = st.Next(ctx, b)
			if e != nil || !ok {
				break
			}
			node := b.Arrays["items"]
			if node == nil {
				t.Fatal("missing typed depth tree")
			}
			for i := 0; i < b.Rows; i++ {
				if !reflect.DeepEqual(n38BudgetValue(node, i), wantRows[rows+i]) {
					t.Fatalf("independent row oracle %d", rows+i)
				}
			}
			checkNode, checkField := node, field
			for {
				if checkNode == nil || checkNode.Kind != checkField.Type || !checkField.Nullable || checkField.ArrowType != "" {
					t.Fatal("native nullable metadata changed")
				}
				if checkField.Type != memory.TypeList {
					break
				}
				if len(checkField.Children) != 1 {
					t.Fatal("list child schema")
				}
				checkNode, checkField = checkNode.Child, checkField.Children[0]
			}
			before := m.Used()
			var owner *memory.OwnedNestedList
			owner, e = memory.CloneNestedListRangeOwned(ctx, m, node, field, memory.NestedListParentDomain{}, 0, b.Rows)
			if e != nil {
				break
			}
			if m.Used() < before+owner.Bytes {
				t.Fatal("full upstream and retained owner not shared")
			}
			if m.Used() > combined {
				combined = m.Used()
			}
			if last != nil {
				for i, w := range lastExpected {
					if !reflect.DeepEqual(n38BudgetValue(last.Vector, i), w) {
						t.Fatal("previous owned result expired on source Next")
					}
				}
				last.Close()
			}
			last = owner
			lastExpected = append([]any(nil), wantRows[rows:rows+b.Rows]...)
			rows += b.Rows
			if !keepOwner {
				last.Close()
				last = nil
				lastExpected = nil
			}
		}
	}
	measured := n38BudgetMeasure{m.peak, m.calls, append([]string(nil), m.trace...), combined, m.overlaps}
	// A single transient exhaustion may legitimately emit a smaller batch and retry.
	// Success still requires every original row and the unchanged typed schema.
	if want == nil || fail < 0 && e == nil {
		if e != nil {
			t.Fatal(e)
		}
		if len(wantRows) > 1 && m.overlaps == 0 {
			t.Fatal("no live-old-stage/new-reservation overlap observed")
		}
		if rows != len(wantRows) {
			t.Fatal("row count", rows)
		}
	} else if !errors.Is(e, want) {
		t.Fatalf("want=%v got=%v trace=%v", want, e, m.trace)
	}
	m.failAt = 0
	m.cancelAt = 0
	if st != nil {
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
		if e := st.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if last != nil {
		if m.Used() != last.Bytes {
			t.Fatalf("postscan only owner expected used=%d owner=%d", m.Used(), last.Bytes)
		}
		for i, w := range lastExpected {
			if !reflect.DeepEqual(n38BudgetValue(last.Vector, i), w) {
				t.Fatal("owner changed after scanClose")
			}
		}
		last.Close()
		last.Close()
	}
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
	return measured
}
func n38BudgetRows(depth int, text bool) []any {
	return []any{nil, []any{}, n38BudgetTree(depth, 1, text, 3), n38BudgetTree(depth, 65, text, 127), n38BudgetTree(depth, 3, text, 13), n38BudgetTree(depth, 1, text, 4096)}
}
func TestN38ListDepthBudgetExactAndRegression(t *testing.T) {
	for _, depth := range []int{2, 3} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth%d_string%v", depth, text), func(t *testing.T) {
				rows := n38BudgetRows(depth, text)
				path := n38BudgetFile(t, depth, text, rows)
				a := n38BudgetRun(t, path, rows, 8<<20, 1<<20, 0, 0, nil, false)
				t.Logf("depth=%d text=%v rows=%d rowgroups1 BatchRows3 workers1 peak=%d sharedScanOwned=%d calls=%d trace=%v", depth, text, len(rows), a.peak, a.overlap, a.calls, a.trace)
				t.Run("exact", func(t *testing.T) { n38BudgetRun(t, path, rows, a.peak, 1<<20, 0, 0, nil, false) })
				t.Run("minus_one", func(t *testing.T) {
					n38BudgetRun(t, path, rows, a.peak-1, 1<<20, 0, 0, memory.ErrBudgetExceeded, false)
				})
				t.Run("owner_survives", func(t *testing.T) { n38BudgetRun(t, path, rows, 8<<20, 1<<20, 0, 0, nil, true) })
			})
		}
	}
}
func TestN38ListDepthBudgetEveryReserve(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			rows := n38BudgetRows(3, text)
			path := n38BudgetFile(t, 3, text, rows)
			a := n38BudgetRun(t, path, rows, 8<<20, 1<<20, 0, 0, nil, false)
			for at := 1; at <= a.calls; at++ {
				t.Run(fmt.Sprintf("fail_%03d", at), func(t *testing.T) { n38BudgetRun(t, path, rows, 8<<20, 1<<20, at, 0, memory.ErrBudgetExceeded, false) })
				t.Run(fmt.Sprintf("transient_%03d", at), func(t *testing.T) { n38BudgetRun(t, path, rows, 8<<20, 1<<20, -at, 0, memory.ErrBudgetExceeded, false) })
				t.Run(fmt.Sprintf("cancel_%03d", at), func(t *testing.T) { n38BudgetRun(t, path, rows, 8<<20, 1<<20, 0, at, context.Canceled, false) })
			}
		})
	}
}
func TestN38ListDepthBudgetOversizedRow(t *testing.T) {
	for _, text := range []bool{false, true} {
		t.Run(fmt.Sprintf("string%v", text), func(t *testing.T) {
			n, size := 2048, 0
			if text {
				n = 1
				size = 32768
			}
			rows := []any{n38BudgetTree(3, n, text, size)}
			path := n38BudgetFile(t, 3, text, rows)
			n38BudgetRun(t, path, rows, 8<<20, 1024, 0, 0, memory.ErrBudgetExceeded, false)
		})
	}
}
