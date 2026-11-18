package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/arrowrecord"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Reuse only literal input fixture generation, not another expression's result.
func structConstructInput(tc structGetCase, n int) (memory.Schema, *memory.Batch) {
	schema, b, _, _ := structGetFixture(tc, n)
	parent := b.Structs["root"]
	return memory.NewSchema(schema.Fields[0].Children...), parent.Values
}
func structConstructValue(tc structGetCase, row int) any {
	if row%5 == 0 {
		return nil
	}
	if tc.typ == memory.TypeBool {
		return true
	}
	if tc.typ == memory.TypeList {
		if tc.vector == "int64" {
			return []any{int64(row + 1), nil}
		}
		return []any{"中", nil}
	}
	return tc.value
}
func structConstructCheck(t *testing.T, df DataFrame, tc structGetCase, rows []int) {
	t.Helper()
	col, e := df.Column("made")
	if e != nil {
		t.Fatal(e)
	}
	got, e := col.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := make([]any, len(rows))
	for i, r := range rows {
		want[i] = map[string]any{"value": structConstructValue(tc, r), "tag": int64(r + 10)}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("constructed got=%#v want=%#v", got, want)
	}
	if col.NullCount() != 0 {
		t.Fatal("parent must always be valid")
	}
}

func structConstructRun(t *testing.T, tc structGetCase, entry string, mode kernel.Mode, node expr.Expr) {
	opts := engine.DefaultOptions()
	opts.KernelMode = mode
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer func() {
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Error("leak")
		}
	}()
	schema, b := structConstructInput(tc, 65)
	var scan LazyFrame
	var owner *arrowrecord.Source
	var rows []int
	if entry == "memory" {
		src, e := inmemory.New(schema, b)
		if e != nil {
			t.Fatal(e)
		}
		scan = s.FromSource(src)
		for i := 0; i < 65; i++ {
			rows = append(rows, i)
		}
	} else {
		alloc := am.NewCheckedAllocator(am.DefaultAllocator)
		defer alloc.AssertSize(t, 0)
		rec, e := memory.ArrowRecord(alloc, schema, b)
		if e != nil {
			t.Fatal(e)
		}
		slice := rec.NewSlice(1, 65)
		rec.Release()
		owner, e = arrowrecord.NewSource(slice)
		slice.Release()
		if e != nil {
			t.Fatal(e)
		}
		defer owner.Close()
		scan = s.FromSource(owner)
		for i := 1; i < 65; i++ {
			rows = append(rows, i)
		}
	}
	q := scan.SelectExprs(Named(node, "made"))
	qs, e := q.Schema()
	if e != nil {
		t.Fatal(e)
	}
	parent := qs.Fields[0]
	if parent.Type != memory.TypeStruct || parent.Nullable || len(parent.Children) != 2 || parent.Children[0].Type != tc.typ || !parent.Children[0].Nullable || parent.Children[1].Nullable {
		t.Fatal("constructed metadata", parent)
	}
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	structConstructCheck(t, df, tc, rows)
	df.Close()
	value := expr.StructGet(Col("made"), "value")
	projection := q.SelectExprs(Named(value, "v"), expr.StructGet(Col("made"), "tag").Alias("tag"))
	df, e = projection.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	c, e := df.Column("v")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := make([]any, len(rows))
	for i, r := range rows {
		want[i] = structConstructValue(tc, r)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("StructGet", got, want)
	}
	df.Close()
	if tc.typ == memory.TypeList {
		listq := q.SelectExprs(expr.ListLength(value).Alias("n"), expr.ListGet(value, expr.I64(0)).Alias("first"))
		df, e := listq.Filter(expr.EQ(Col("n"), expr.I64(2))).Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		c, e := df.Column("first")
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		var want []any
		for _, r := range rows {
			if r%5 != 0 {
				if tc.vector == "int64" {
					want = append(want, int64(r+1))
				} else {
					want = append(want, "中")
				}
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("list downstream", got, want)
		}
		df.Close()
	}
	path := filepath.Join(t.TempDir(), "struct.arrow")
	if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	reader, e := ipc.NewFileReader(file)
	if e != nil {
		t.Fatal(e)
	}
	var records []arrow.RecordBatch
	for i := 0; i < reader.NumRecords(); i++ {
		rec, e := reader.RecordBatchAt(i)
		if e != nil {
			t.Fatal(e)
		}
		records = append(records, rec)
	}
	round, e := s.ScanArrow(records...)
	for _, r := range records {
		r.Release()
	}
	reader.Close()
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	df, e = round.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	structConstructCheck(t, df, tc, rows)
	df.Close()
	if tc.typ == memory.TypeString || tc.typ == memory.TypeList {
		path := filepath.Join(t.TempDir(), "struct.parquet")
		if _, e := q.SinkParquet(context.Background(), path, ParquetSinkOptions{}); e != nil {
			t.Fatal(e)
		}
		pq, err := s.ScanParquet(path)
		if tc.typ == memory.TypeList {
			if err == nil || !strings.Contains(err.Error(), "nested/repeated") {
				t.Fatalf("repeated leaf reader boundary %v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			flatSchema, err := pq.Schema()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := flatSchema.Field("made"); ok {
				t.Fatal("unexpected Struct reconstruction")
			}
			flat, err := pq.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"made.value", "made.tag"} {
				col, err := flat.Column(name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := col.ToAnySlice()
				if err != nil {
					t.Fatal(err)
				}
				want := make([]any, len(rows))
				for i, r := range rows {
					if name == "made.value" {
						want[i] = structConstructValue(tc, r)
					} else {
						want[i] = int64(r + 10)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("flat leaf Parquet", name, got, want)
				}
			}
			flat.Close()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	df, e = q.Collect(ctx)
	if !errors.Is(e, context.Canceled) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	early, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 8192})
	if e != nil {
		t.Fatal(e)
	}
	if !early.Next(context.Background()) {
		t.Fatal(early.Err())
	}
	early.Close()
	// Collect owns its output, independently of the Arrow input source lifetime.
	df, e = q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if owner != nil {
		owner.Close()
	}
	structConstructCheck(t, df, tc, rows)
	df.Close()
}

func TestStructConstructEntrypointOracle(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, entry := range []string{"memory", "arrow"} {
			for i, tc := range structGetCases() {
				t.Run(fmt.Sprintf("%d/%s/%d_%s", mode, entry, i, tc.typ), func(t *testing.T) {
					node := StructFrom(Col("a.b:中").Alias("value"), Col("sibling").Alias("tag"))
					structConstructRun(t, tc, entry, mode, node)
				})
			}
		}
	}
}
func TestStructConstructPayloadSortJoinAndBudget(t *testing.T) {
	tc := structGetCases()[19]
	schema, b := structConstructInput(tc, 65)
	src, e := inmemory.New(schema, b, b)
	if e != nil {
		t.Fatal(e)
	}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 512 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
	s := NewSession(opts)
	defer func() {
		s.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Error("leak")
		}
	}()
	q := s.FromSource(src).SelectExprs(Col("sibling").Alias("key"), StructFrom(Col("a.b:中").Alias("value"), Col("sibling").Alias("tag")).Alias("made"))
	sorted := q.Sort(Desc("key"))
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	df, e := sorted.Collect(memory.WithQueryBudget(context.Background(), budget))
	if e != nil {
		budget.Close()
		t.Fatal(e)
	}
	var rows []int
	for r := 64; r >= 0; r-- {
		rows = append(rows, r, r)
	}
	structConstructCheck(t, df, tc, rows)
	df.Close()
	snap := budget.Snapshot()
	if snap.SpilledBytes == 0 {
		t.Fatal("expected actual payload spill")
	}
	t.Logf("Struct payload sort observed spill bytes=%d peak=%d", snap.SpilledBytes, snap.PeakBytes)
	budget.Close()
	rb := memory.NewBatch()
	rb.Reset(2)
	rb.Int64s["rk"] = &memory.Int64Vector{Values: []int64{10, 11}}
	rb.Int64s["match"] = &memory.Int64Vector{Values: []int64{100, 101}}
	right, e := inmemory.New(memory.NewSchema(memory.Field{Name: "rk", Type: memory.TypeInt64}, memory.Field{Name: "match", Type: memory.TypeInt64}), rb)
	if e != nil {
		t.Fatal(e)
	}
	joined := q.Join(s.FromSource(right), JoinOptions{Type: LeftJoin, LeftOn: []string{"key"}, RightOn: []string{"rk"}})
	df, e = joined.Sort(Asc("key")).Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	rows = nil
	for r := 0; r < 65; r++ {
		rows = append(rows, r, r)
	}
	structConstructCheck(t, df, tc, rows)
	c, e := df.Column("match")
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.ToAnySlice()
	if e != nil {
		t.Fatal(e)
	}
	want := make([]any, 130)
	for i, r := range rows {
		if r < 2 {
			want[i] = int64(100 + r)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("join payload", got, want)
	}
	df.Close()
	tinyOpts := engine.DefaultOptions()
	tinyOpts.PreparedCacheBytes = 0
	tinyOpts.MemoryLimitBytes = 1
	tiny := NewSession(tinyOpts)
	defer tiny.Close()
	df, e = tiny.FromSource(src).SelectExprs(StructFrom(Col("a.b:中").Alias("value"), Col("sibling").Alias("tag")).Alias("made")).Collect(context.Background())
	if !errors.Is(e, memory.ErrBudgetExceeded) {
		if e == nil {
			df.Close()
		}
		t.Fatal(e)
	}
	if tiny.Engine().Memory().Used() != 0 {
		t.Fatal("budget denial leaked")
	}
}

func TestStructConstructImplicitNamesSurviveProjectionSwap(t *testing.T) {
	b := memory.NewBatch()
	b.Reset(3)
	b.Int64s["x"] = &memory.Int64Vector{Values: []int64{-10, 20, -30}}
	b.Int64s["y"] = &memory.Int64Vector{Values: []int64{1, -2, 3}}
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	src, e := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "y", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}), b)
	if e != nil {
		t.Fatal(e)
	}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	s := NewSession(opts)
	defer s.Close()
	// Field names are determined in the projected schema, before optimizer input
	// renaming. Swapping source x/y must not silently swap StructFrom child names.
	record := StructFrom(Named(Col("x"), ""), Named(Col("y"), ""))
	q := s.FromSource(src).SelectExprs(Col("y").Alias("x"), Col("x").Alias("y"), Col("id").Alias("id")).Filter(expr.GT(expr.StructGet(record, "x"), expr.I64(0))).SelectExprs(Col("id").Alias("id"), Col("x").Alias("x"), Col("y").Alias("y"))
	df, e := q.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	for name, want := range map[string][]any{"id": {int64(0), int64(2)}, "x": {int64(1), int64(3)}, "y": {int64(-10), int64(-30)}} {
		col, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := col.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s got=%v want=%v", name, got, want)
		}
	}
}
