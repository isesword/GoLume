package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestN13DecimalExternalMultiPass(t *testing.T) {
	for _, desc := range []bool{false, true} {
		for _, nullLast := range []bool{false, true} {
			t.Run(fmt.Sprintf("desc%v/nullLast%v", desc, nullLast), func(t *testing.T) {
				a, _ := memory.Decimal128Field("a", true, 38, 9)
				b, _ := memory.Decimal128Field("b", false, 1, 0)
				schema := memory.NewSchema(a, b, memory.Field{Name: "id", Type: memory.TypeInt64})
				var batches []*memory.Batch
				type row struct {
					a, b  memory.Decimal128
					valid bool
					id    int64
				}
				var rows []row
				for part := 0; part < 24; part++ {
					batch := memory.NewBatch()
					batch.Rows = 9
					av := &memory.Decimal128Vector{Precision: 38, Scale: 9, Validity: memory.EnsureBitmap(nil, 9)}
					bv := &memory.Decimal128Vector{Precision: 1, Scale: 0}
					ids := &memory.Int64Vector{}
					for i := 0; i < 9; i++ {
						id := part*9 + i
						coeff := decimal128.New(int64(id%3-1), uint64(9007199254740993+id%5))
						valid := id%7 != 0
						if valid {
							memory.Set(av.Validity, i)
						}
						small := decimal128.FromI64(int64(id%9 - 4))
						av.Values = append(av.Values, coeff)
						bv.Values = append(bv.Values, small)
						ids.Values = append(ids.Values, int64(id))
						rows = append(rows, row{coeff, small, valid, int64(id)})
					}
					batch.Decimals["a"] = av
					batch.Decimals["b"] = bv
					batch.Int64s["id"] = ids
					batches = append(batches, batch)
				}
				sort.SliceStable(rows, func(i, j int) bool {
					x, y := rows[i], rows[j]
					if x.valid != y.valid {
						return x.valid == nullLast
					}
					c := 0
					if x.valid {
						c = x.a.BigInt().Cmp(y.a.BigInt())
					}
					if c == 0 {
						c = x.b.BigInt().Cmp(y.b.BigInt())
					}
					if desc {
						return c > 0
					}
					return c < 0
				})
				input, e := inmemory.New(schema, batches...)
				if e != nil {
					t.Fatal(e)
				}
				root := memory.NewEngineBudget(1 << 20)
				dir := t.TempDir()
				q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 16 << 20}})
				if e != nil {
					t.Fatal(e)
				}
				defer q.Close()
				ordered, e := NewSortSourceWithOptions(input, []SortKey{{Column: "a", Descending: desc, NullsLast: nullLast}, {Column: "b", Descending: desc}}, root, SortSourceOptions{Spillable: true})
				if e != nil {
					t.Fatal(e)
				}
				ctx := memory.WithQueryBudget(context.Background(), q)
				st, e := source.OpenBatchStream(ctx, ordered, testReadOptions())
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				out := memory.NewBatch()
				seen := 0
				for {
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					for i := 0; i < out.Rows; i++ {
						w := rows[seen]
						av := out.Decimals["a"]
						bv := out.Decimals["b"]
						if out.Int64s["id"].Values[i] != w.id || av.IsValid(i) != w.valid || av.Precision != 38 || av.Scale != 9 || w.valid && av.Values[i] != w.a || bv.Values[i] != w.b {
							t.Fatalf("row %d mismatch", seen)
						}
						seen++
					}
				}
				if e = st.Close(); e != nil {
					t.Fatal(e)
				}
				s := st.(*externalSortStream)
				if seen != len(rows) || s.mergeLevels < 2 || q.Used() != 0 {
					t.Fatalf("rows %d levels %d used %d", seen, s.mergeLevels, q.Used())
				}
				e = filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
					if e != nil {
						return e
					}
					if !d.IsDir() {
						return fmt.Errorf("spill file remains %s", p)
					}
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}

func n13SmallSource(t *testing.T, parts int) source.Source {
	t.Helper()
	f, _ := memory.Decimal128Field("amount", true, 38, 9)
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, f)
	var batches []*memory.Batch
	for p := 0; p < parts; p++ {
		b := memory.NewBatch()
		b.Rows = 4
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{4, 3, 2, 1}}
		b.Decimals["amount"] = &memory.Decimal128Vector{Precision: 38, Scale: 9, Values: []memory.Decimal128{decimal128.FromI64(4), decimal128.FromI64(3), decimal128.FromI64(2), decimal128.FromI64(1)}}
		batches = append(batches, b)
	}
	s, e := inmemory.New(schema, batches...)
	if e != nil {
		t.Fatal(e)
	}
	return s
}

type n13Faults struct{ calls, fail int }
type n13Manager struct {
	memory.Manager
	f *n13Faults
}

func (m n13Manager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.f.calls++
	if m.f.calls == m.f.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.Manager.Reserve(ctx, n)
}
func n13FilesGone(t *testing.T, dir string) {
	t.Helper()
	if e := filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			return fmt.Errorf("file remains %s", p)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
func n13BudgetRun(t *testing.T, hard int64, fail, parts int, pathExtra ...string) (int, int64, error) {
	t.Helper()
	dir := t.TempDir()
	if len(pathExtra) > 0 {
		name := pathExtra[0]
		if target, e := strconv.Atoi(name); e == nil {
			n := target - len(dir) - 2 - len("golume-q-") - 10
			if n < 1 {
				t.Fatal("cannot pad requested path", target, dir)
			}
			name = strings.Repeat("p", n)
		}
		dir = filepath.Join(dir, name)
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
	}
	s, q, spy, ctx := openSortLifecycle(t, hard, n13SmallSource(t, parts), dir)
	// MkdirTemp uses a variable-width decimal random suffix. Keep its metadata
	// input length fixed for exact/-1 comparisons; no execution fees are changed.
	for len(filepath.Base(spy.Directory())) != len("golume-q-")+10 {
		_ = s.Close()
		_ = q.Close()
		n13FilesGone(t, dir)
		s, q, spy, ctx = openSortLifecycle(t, hard, n13SmallSource(t, parts), dir)
	}
	if fail == 0 {
		t.Logf("diagnostic hard=%d dir=%s bytes=%d", hard, spy.Directory(), len(spy.Directory()))
	}
	s.chunkRows = 4
	s.runBytes = 1024
	f := &n13Faults{fail: fail}
	s.workspace.parent = n13Manager{s.workspace.parent, f}
	s.sortBudget = n13Manager{s.sortBudget, f}
	s.encode = n13Manager{s.encode, f}
	s.restore = n13Manager{s.restore, f}
	s.metadata = n13Manager{s.metadata, f}
	s.store = spillmeta.NewStore(spy, s.metadata)
	out := memory.NewBatch()
	var result error
	seen := 0
	for {
		ok, e := s.Next(ctx, out)
		if e != nil {
			result = e
			break
		}
		if !ok {
			break
		}
		for i := 0; i < out.Rows; i++ {
			if out.Decimals["amount"].Values[i] != decimal128.FromI64(out.Int64s["key"].Values[i]) {
				t.Fatal("payload lost")
			}
			seen++
		}
	}
	if result == nil && seen != parts*4 {
		t.Fatal(seen)
	}
	if result == nil && parts >= 24 && s.mergeLevels < 2 {
		t.Fatal("not multi-pass", s.mergeLevels)
	}
	if e := s.Close(); result == nil {
		result = e
	}
	peak := q.Snapshot().PeakBytes
	if q.Used() != 0 || spy.Used() != 0 {
		t.Fatalf("cleanup RAM=%d spill=%d", q.Used(), spy.Used())
	}
	n13FilesGone(t, dir)
	return f.calls, peak, result
}
func TestN13DecimalEveryReserveAndExact(t *testing.T) {
	for _, parts := range []int{3, 24} {
		t.Run(fmt.Sprintf("parts%d", parts), func(t *testing.T) {
			calls, peak, e := n13BudgetRun(t, 1<<20, 0, parts)
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("calls=%d peak=%d", calls, peak)
			for i := 1; i <= calls; i++ {
				_, _, e = n13BudgetRun(t, 1<<20, i, parts)
				if !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatalf("reserve %d: %v", i, e)
				}
			}
			_, _, e = n13BudgetRun(t, peak, 0, parts)
			if e != nil {
				t.Fatalf("exact: %v", e)
			}
			_, _, e = n13BudgetRun(t, peak-1, 0, parts)
			if !errors.Is(e, memory.ErrBudgetExceeded) {
				t.Fatalf("minus one: %v", e)
			}
		})
	}
}

func TestN13DecimalSpillCancelAndWriteCleanup(t *testing.T) {
	for _, mode := range []string{"input", "merge", "write", "output"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s, q, spy, ctx := openSortLifecycle(t, 1<<20, n13SmallSource(t, 24), dir)
			s.chunkRows = 4
			s.runBytes = 1024
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			want := error(context.Canceled)
			switch mode {
			case "input":
				cancel()
			case "merge":
				spy.cancelAfter = 12
				spy.cancel = cancel
			case "write":
				spy.failWriteAt = 12
				want = spy.failure
			case "output":
				out := memory.NewBatch()
				ok, e := s.Next(ctx, out)
				if !ok || e != nil {
					t.Fatal(ok, e)
				}
				if len(out.Decimals["amount"].Values) == 0 {
					t.Fatal("no owned payload")
				}
				cancel()
			}
			_, e := s.Next(ctx, memory.NewBatch())
			if !errors.Is(e, want) {
				t.Fatal(e)
			}
			_ = s.Close()
			if q.Used() != 0 || spy.Used() != 0 {
				t.Fatal(q.Used(), spy.Used())
			}
			n13FilesGone(t, dir)
		})
	}
}

func n13CheckSpillPayload(t *testing.T, input source.Source, key string, want []memory.Decimal128, valid []bool) {
	t.Helper()
	root := memory.NewEngineBudget(1 << 20)
	dir := t.TempDir()
	q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 16 << 20}})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	sorted, e := NewSortSourceWithOptions(input, []SortKey{{Column: key}}, root, SortSourceOptions{Spillable: true})
	if e != nil {
		t.Fatal(e)
	}
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := source.OpenBatchStream(ctx, sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	out := memory.NewBatch()
	seen := 0
	for {
		ok, e := st.Next(ctx, out)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for i := 0; i < out.Rows; i++ {
			v := out.Decimals["amount"]
			if seen >= len(want) || v.IsValid(i) != valid[seen] || valid[seen] && v.Values[i] != want[seen] {
				t.Fatal("spill Decimal payload", seen)
			}
			seen++
		}
	}
	if seen != len(want) {
		t.Fatal(seen)
	}
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
	n13FilesGone(t, dir)
}

func TestN13DecimalAllBatchValidation(t *testing.T) {
	for _, mode := range []string{"coefficient", "scale", "nonnull", "empty"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := memory.Decimal128Field("amount", false, 1, 0)
			schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, f)
			good := memory.NewBatch()
			good.Rows = 1
			good.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
			good.Decimals["amount"] = &memory.Decimal128Vector{Precision: 1, Values: []memory.Decimal128{decimal128.FromI64(1)}}
			bad := memory.NewBatch()
			bad.Rows = 1
			bad.Int64s["key"] = &memory.Int64Vector{Values: []int64{2}}
			bad.Decimals["amount"] = &memory.Decimal128Vector{Precision: 1, Values: []memory.Decimal128{decimal128.FromI64(2)}}
			switch mode {
			case "coefficient":
				bad.Decimals["amount"].Values[0] = decimal128.FromI64(10)
			case "scale":
				bad.Decimals["amount"].Scale = 1
			case "nonnull":
				bad.Decimals["amount"].Validity = memory.Bitmap{0}
			case "empty":
				bad.Rows = 0
				bad.Int64s["key"].Values = nil
			}
			input := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{good, bad}}, stable: false}
			dir := t.TempDir()
			s, q, spy, ctx := openSortLifecycle(t, 1<<20, input, dir)
			_, e := s.Next(ctx, memory.NewBatch())
			if e == nil {
				t.Fatal("bad later batch accepted")
			}
			_ = s.Close()
			if q.Used() != 0 || spy.Used() != 0 {
				t.Fatal(q.Used(), spy.Used())
			}
			n13FilesGone(t, dir)
		})
	}
}
func TestN13DecimalMixedContainers(t *testing.T) {
	schema, b := n10Input(t)
	input := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b, b}}, stable: false}
	root := memory.NewEngineBudget(64 << 20)
	dir := t.TempDir()
	q, e := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: 64 << 20, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 64 << 20}})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	sorted, e := NewSortSourceWithOptions(input, []SortKey{{Column: "decimal", NullsLast: true}}, root, SortSourceOptions{Spillable: true})
	if e != nil {
		t.Fatal(e)
	}
	ctx := memory.WithQueryBudget(context.Background(), q)
	st, e := source.OpenBatchStream(ctx, sorted, testReadOptions())
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	want := []int{0, 0, 2, 2, 1, 1, 3, 3}
	seen := 0
	out := memory.NewBatch()
	var previous *memory.Decimal128Vector
	for {
		ok, e := st.Next(ctx, out)
		if previous != nil && len(previous.Values) != 0 {
			t.Fatal("expired Decimal header still live")
		}
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		if input.closed != 1 {
			t.Fatal("input not closed before owned output", input.closed)
		}
		nestedGatherCheck(t, out, 8, want[seen:seen+out.Rows])
		for i, r := range want[seen : seen+out.Rows] {
			v := out.Decimals["decimal"]
			if v.IsValid(i) != (r != 3) || r != 3 && v.Values[i] != b.Decimals["decimal"].Values[r] {
				t.Fatal("mixed Decimal value")
			}
		}
		previous = out.Decimals["decimal"]
		seen += out.Rows
	}
	if seen != 8 {
		t.Fatal(seen)
	}
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
	n13FilesGone(t, dir)
}

func TestN13Path(t *testing.T) {
	var peaks []int64
	// Include enough padding for all fresh sibling directories, without
	// assuming /tmp or a particular platform's absolute path prefix.
	baseLength := len(t.TempDir()) + 2 + len("golume-q-") + 10 + 64
	for delta := 0; delta < 3; delta++ {
		n := strconv.Itoa(baseLength + delta)
		_, p, e := n13BudgetRun(t, 1<<20, 0, 24, n)
		if e != nil {
			t.Fatal(e)
		}
		peaks = append(peaks, p)
		_, _, e = n13BudgetRun(t, p, 0, 24, n)
		if e != nil {
			t.Fatal("exact path", n, e)
		}
		_, _, e = n13BudgetRun(t, p-1, 0, 24, n)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal("minus one path", n, e)
		}
	}
	t.Logf("controlled path base=%d adjacent peaks=%v", baseLength, peaks)
	if peaks[1]-peaks[0] != 48 || peaks[2]-peaks[0] != 96 {
		t.Fatal("unexpected controlled path peaks", peaks)
	}
}
