package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func nestedListDFixture(t *testing.T, depth int) string {
	t.Helper()
	a := am.NewCheckedAllocator(am.DefaultAllocator)
	defer a.AssertSize(t, 0)
	r := nestedListCArrow(t, a, depth)
	defer r.Release()
	tab := array.NewTableFromRecords(r.Schema(), []arrow.RecordBatch{r, r, r, r})
	defer tab.Release()
	path := filepath.Join(t.TempDir(), "nested.parquet")
	out, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	version := pq.DataPageV1
	dict := depth == 8
	if dict {
		version = pq.DataPageV2
	}
	e = pqarrow.WriteTable(tab, out, 65, pq.NewWriterProperties(pq.WithDataPageVersion(version), pq.WithDictionaryDefault(dict), pq.WithDataPageSize(64), pq.WithBatchSize(16)), pqarrow.DefaultWriterProps())
	out.Close()
	if e != nil {
		t.Fatal(e)
	}
	in, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer in.Close()
	rd, e := file.NewParquetReader(in)
	if e != nil {
		t.Fatal(e)
	}
	defer rd.Close()
	if rd.NumRowGroups() != 4 {
		t.Fatal("groups", rd.NumRowGroups())
	}
	data, dictionaries := 0, 0
	for leaf := 0; leaf < rd.MetaData().Schema.NumColumns(); leaf++ {
		p, e := rd.RowGroup(0).GetColumnPageReader(leaf)
		if e != nil {
			t.Fatal(e)
		}
		for p.Next() {
			switch p.Page().Type() {
			case file.PageTypeDictionaryPage:
				dictionaries++
			case file.PageTypeDataPage, file.PageTypeDataPageV2:
				data++
				want := file.PageTypeDataPage
				if dict {
					want = file.PageTypeDataPageV2
				}
				if p.Page().Type() != want {
					t.Fatal("version")
				}
			}
		}
		e = p.Err()
		p.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	if data < 2 || (dict && dictionaries == 0) || (!dict && dictionaries != 0) {
		t.Fatal("pages", data, dictionaries)
	}
	t.Logf("actual groups=4 all leaves data=%d dictionaries=%d", data, dictionaries)
	return path
}
func nestedListDCheck(t *testing.T, df DataFrame, depth int, filtered bool, names []string) {
	t.Helper()
	for _, name := range names {
		c, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		got, e := c.ToAnySlice()
		if e != nil {
			t.Fatal(e)
		}
		want := []any{}
		for repeat := 0; repeat < 4; repeat++ {
			for row := 0; row < 65; row++ {
				if filtered && row <= 31 {
					continue
				}
				var v any
				switch name {
				case "inner":
					v = nestedListBRaw(row, false)
					if v != nil {
						a := v.([]any)
						v = nil
						if len(a) > 0 {
							v = a[0]
						}
					}
				case "value":
					v = nestedListBRaw(row, false)
					if v != nil {
						a := v.([]any)
						v = nil
						if len(a) > 0 && a[0] != nil {
							b := a[0].([]any)
							if len(b) > 0 {
								v = b[0]
							}
						}
					}
				default:
					v = nestedListCRaw(row, depth)[name]
				}
				want = append(want, v)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("D oracle %s got%v want%v", name, got, want)
		}
	}
}
func TestParquetNestedListDReader(t *testing.T) {
	for _, depth := range []int{1, 8} {
		t.Run(fmt.Sprintf("outer%d", depth), func(t *testing.T) {
			path := nestedListDFixture(t, depth)
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			inner := ListGet(Col("ints"), expr.I64(0))
			derived := q.WithColumns(inner.Alias("inner"), ListGet(inner, expr.I64(0)).Alias("value"))
			names := []string{"ints", "texts", "outer", "id", "index", "oldints", "records", "inner", "value"}
			df, e := derived.Filter(expr.GT(Col("id"), expr.I64(31))).Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			nestedListDCheck(t, df, depth, true, names)
			// Drop the first physical root to exercise logical projection mapping.
			pd, e := q.Select("texts", "outer", "records").Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer pd.Close()
			nestedListDCheck(t, pd, depth, false, []string{"texts", "outer", "records"})

			sc := df.Schema()
			f := sc.Fields[0]
			if f.Type != memory.TypeList || f.Children[0].Type != memory.TypeList || f.Children[0].Children[0].Type != memory.TypeInt64 || !f.Nullable || !f.Children[0].Nullable || !f.Children[0].Children[0].Nullable {
				t.Fatal("nested metadata", f)
			}
			f = sc.Fields[2]
			for level := 1; level <= depth; level++ {
				if f.Type != memory.TypeStruct || f.Nullable != (level%2 == 1) {
					t.Fatal("ancestor metadata", level, f)
				}
				if level < depth {
					if f.Children[0].Name != "node.中" {
						t.Fatal("literal name")
					}
					f = f.Children[0]
				} else if f.Children[0].Name != "values.中" || f.Children[0].Children[0].Children[0].Type != memory.TypeString {
					t.Fatal("terminal metadata", f)
				}
			}
			ip := filepath.Join(t.TempDir(), "nested.arrow")
			if _, e = derived.SinkIPC(context.Background(), ip, IPCSinkOptions{}); e != nil {
				t.Fatal(e)
			}
			in, e := os.Open(ip)
			if e != nil {
				t.Fatal(e)
			}
			ir, e := ipc.NewFileReader(in)
			if e != nil {
				t.Fatal(e)
			}
			var records []arrow.RecordBatch
			for i := 0; i < ir.NumRecords(); i++ {
				r, e := ir.RecordBatchAt(i)
				if e != nil {
					t.Fatal(e)
				}
				records = append(records, r)
			}
			iq, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, records...)
			for _, r := range records {
				r.Release()
			}
			ir.Close()
			in.Close()
			if e != nil {
				t.Fatal(e)
			}
			idf, e := iq.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer idf.Close()
			nestedListDCheck(t, idf, depth, false, names)
			reader, e := q.CollectBatches(context.Background(), BatchOptions{TargetBatchBytes: 1024, MaxBatchBytes: 2 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if !reader.Next(context.Background()) {
				t.Fatal(reader.Err())
			}
			view := reader.Batch()
			reader.Close()
			if view.Valid() {
				t.Fatal("borrowed view survived close")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			cd, ce := q.Collect(ctx)
			if ce == nil {
				cd.Close()
			}
			if !errors.Is(ce, context.Canceled) {
				t.Fatal("cancel", ce)
			}
			tinyOpts := engine.DefaultOptions()
			tinyOpts.MemoryLimitBytes = 1
			tiny := NewSession(tinyOpts)
			tq, te := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if te != nil {
				t.Fatal(te)
			}
			td, te := tq.Collect(context.Background())
			if te == nil {
				td.Close()
			}
			tiny.Close()
			if !errors.Is(te, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
				t.Fatal("tiny budget", te)
			}
			s.Close()
			nestedListDCheck(t, idf, depth, false, names)
			nestedListDCheck(t, df, depth, true, names)
			opts := engine.DefaultOptions()
			opts.PreparedCacheBytes = 0
			opts.ExecutionPolicy = engine.ExecutionBounded
			opts.MemoryLimitBytes = 8 << 20
			dir := t.TempDir()
			opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
			bs := NewSession(opts)
			defer bs.Close()
			bq, e := bs.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			budget, e := bs.Engine().BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
			done, stopped := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(stopped)
				for {
					select {
					case <-done:
						return
					default:
						observer.Err()
						runtime.Gosched()
					}
				}
			}()
			sd, e := bq.Sort(Desc("id")).Collect(observer)
			close(done)
			<-stopped
			if e != nil {
				t.Fatal(e)
			}
			defer sd.Close()
			if budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
				t.Fatal("actual runs", budget.Snapshot(), observer.maxRuns)
			}
			t.Logf("Parquet nested spill=%d runs=%d disk=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
			nestedListCCheck(t, sd, depth)
			budget.Close()
			recursiveCNoFiles(t, dir)
			bs.Close()
			nestedListCCheck(t, sd, depth)
			sd.Close()
			if bs.Engine().Memory().Used() != 0 {
				t.Fatal("owner leak")
			}
		})
	}
}

func nestedListDWant(row, depth int, name string) any {
	if name != "inner" && name != "value" {
		return nestedListCRaw(row, depth)[name]
	}
	v := nestedListBRaw(row, false)
	if v == nil {
		return nil
	}
	a := v.([]any)
	if len(a) == 0 {
		return nil
	}
	v = a[0]
	if name == "inner" || v == nil {
		return v
	}
	a = v.([]any)
	if len(a) == 0 {
		return nil
	}
	return a[0]
}

func nestedListDArrowField(t *testing.T, got, want arrow.Field, checkName bool) {
	t.Helper()
	if (checkName && got.Name != want.Name) || got.Nullable != want.Nullable || got.Type.ID() != want.Type.ID() {
		t.Fatalf("Arrow field metadata got=%v want=%v", got, want)
	}
	switch w := want.Type.(type) {
	case *arrow.ListType:
		nestedListDArrowField(t, got.Type.(*arrow.ListType).ElemField(), w.ElemField(), false)
	case *arrow.StructType:
		g := got.Type.(*arrow.StructType).Fields()
		wf := w.Fields()
		if len(g) != len(wf) {
			t.Fatal("Struct arity")
		}
		for i := range wf {
			nestedListDArrowField(t, g[i], wf[i], true)
		}
	default:
		if !arrow.TypeEqual(got.Type, want.Type) {
			t.Fatalf("Arrow terminal metadata got=%v want=%v", got, want)
		}
	}
}

// Compare independent native and Arrow readers to the literal oracle, not to one another.
func nestedListDSinkRead(t *testing.T, files []string, depth int, rows []int, derived bool) {
	t.Helper()
	names := []string{"ints", "texts", "outer", "id", "index", "oldints", "records"}
	if derived {
		names = append(names, "inner", "value")
	}
	base := nestedListCArrow(t, am.DefaultAllocator, depth)
	wantFields := append([]arrow.Field(nil), base.Schema().Fields()...)
	base.Release()
	if derived {
		wantFields = append(wantFields, arrow.Field{Name: "inner", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true}, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	}
	native, external := map[string][]any{}, map[string][]any{}
	for _, path := range files {
		s := NewSession(engine.DefaultOptions())
		q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
		if e != nil {
			t.Fatal(e)
		}
		df, e := q.Collect(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		s.Close()
		sc := df.Schema()
		if sc.Fields[0].Children[0].Children[0].Type != memory.TypeInt64 || sc.Fields[1].Children[0].Children[0].Type != memory.TypeString {
			t.Fatal("native nested metadata")
		}
		for _, name := range names {
			c, e := df.Column(name)
			if e != nil {
				t.Fatal(e)
			}
			v, e := c.ToAnySlice()
			if e != nil {
				t.Fatal(e)
			}
			native[name] = append(native[name], v...)
		}
		df.Close()
		if s.Engine().Memory().Used() != 0 {
			t.Fatal("readback owner")
		}
		rd, e := file.OpenParquetFile(path, false)
		if e != nil {
			t.Fatal(e)
		}
		ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
		if e != nil {
			t.Fatal(e)
		}
		tab, e := ar.ReadTable(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if len(tab.Schema().Fields()) != len(wantFields) {
			t.Fatal("output field count")
		}
		for i, f := range tab.Schema().Fields() {
			nestedListDArrowField(t, f, wantFields[i], true)
			if i < 2 {
				outer, ok := f.Type.(*arrow.ListType)
				if !ok || !f.Nullable || !outer.ElemField().Nullable {
					t.Fatal("outer metadata", f)
				}
				inner, ok := outer.Elem().(*arrow.ListType)
				if !ok || !inner.ElemField().Nullable {
					t.Fatal("inner metadata", f)
				}
				want := arrow.INT64
				if i == 1 {
					want = arrow.STRING
				}
				if inner.Elem().ID() != want {
					t.Fatal("leaf metadata", f)
				}
			}
			for _, chunk := range tab.Column(i).Data().Chunks() {
				for row := 0; row < chunk.Len(); row++ {
					external[f.Name] = append(external[f.Name], listStructSinkArrowValue(chunk, row))
				}
			}
		}
		tab.Release()
		rd.Close()
	}
	for _, name := range names {
		want := []any{}
		for _, row := range rows {
			want = append(want, nestedListDWant(row, depth, name))
		}
		for label, got := range map[string][]any{"native": native[name], "independentArrow": external[name]} {
			if len(want) == 0 && len(got) == 0 {
				continue
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s sink oracle %s got%v want%v", label, name, got, want)
			}
		}
	}
}
func TestParquetNestedListDSink(t *testing.T) {
	for _, depth := range []int{1, 8} {
		t.Run(fmt.Sprintf("outer%d", depth), func(t *testing.T) {
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			path := nestedListDFixture(t, depth)
			q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			inner := ListGet(Col("ints"), expr.I64(0))
			q = q.WithColumns(inner.Alias("inner"), ListGet(inner, expr.I64(0)).Alias("value")).Filter(expr.GT(Col("id"), expr.I64(31)))
			rows := []int{}
			for repeat := 0; repeat < 4; repeat++ {
				for row := 32; row < 65; row++ {
					rows = append(rows, row)
				}
			}
			for _, shard := range []bool{false, true} {
				t.Run(fmt.Sprintf("shard%v", shard), func(t *testing.T) {
					o := ParquetSinkOptions{RowGroupRows: 17, SinkOptions: SinkOptions{Batch: BatchOptions{TargetBatchBytes: 4096, MaxBatchBytes: 2 << 20}}}
					if shard {
						o.MaxRowsPerFile = 37
					}
					result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "out"), o)
					if e != nil {
						t.Fatal(e)
					}
					if result.Rows != 132 || result.Bytes <= 0 {
						t.Fatal(result)
					}
					if shard && len(result.Files) != 4 {
						t.Fatal("row shards", result)
					}
					groups := 0
					for _, p := range result.Files {
						r, e := file.OpenParquetFile(p, false)
						if e != nil {
							t.Fatal(e)
						}
						groups += r.NumRowGroups()
						r.Close()
					}
					if groups < 2 {
						t.Fatal("actual groups", groups)
					}
					t.Logf("actual sink files=%d rowgroups=%d", len(result.Files), groups)
					nestedListDSinkRead(t, result.Files, depth, rows, true)
				})
			}
			empty, e := q.Filter(expr.GT(Col("id"), expr.I64(100))).SinkParquet(context.Background(), filepath.Join(t.TempDir(), "empty.parquet"), ParquetSinkOptions{})
			if e != nil || empty.Rows != 0 || len(empty.Files) != 1 {
				t.Fatal("empty", empty, e)
			}
			nestedListDSinkRead(t, empty.Files, depth, nil, true)
			parent := t.TempDir()
			target := filepath.Join(parent, "existing")
			sentinel := []byte("unchanged original")
			if e = os.WriteFile(target, sentinel, 0600); e != nil {
				t.Fatal(e)
			}
			if _, e = q.SinkParquet(context.Background(), target, ParquetSinkOptions{}); !errors.Is(e, ErrSinkTargetExists) {
				t.Fatal("exists", e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, e = q.SinkParquet(ctx, target, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}}); !errors.Is(e, context.Canceled) {
				t.Fatal("cancel", e)
			}
			tinyOptions := engine.DefaultOptions()
			tinyOptions.MemoryLimitBytes = 1
			tiny := NewSession(tinyOptions)
			tq, e := tiny.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if e != nil {
				t.Fatal(e)
			}
			_, e = tq.SinkParquet(context.Background(), target, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
			tiny.Close()
			if !errors.Is(e, memory.ErrBudgetExceeded) || tiny.Engine().Memory().Used() != 0 {
				t.Fatal("budget", e)
			}
			content, e := os.ReadFile(target)
			if e != nil || !reflect.DeepEqual(content, sentinel) {
				t.Fatal("precommit target changed", e)
			}
			entries, e := os.ReadDir(parent)
			if e != nil || len(entries) != 1 {
				t.Fatal("staging", entries, e)
			}
			result, e := q.SinkParquet(context.Background(), target, ParquetSinkOptions{SinkOptions: SinkOptions{Overwrite: true}})
			if e != nil {
				t.Fatal(e)
			}
			s.Close()
			nestedListDSinkRead(t, result.Files, depth, rows, true)
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("sink owner")
			}
		})
	}
}

func TestParquetNestedListDByteShards(t *testing.T) {
	a := am.NewCheckedAllocator(am.DefaultAllocator)
	defer a.AssertSize(t, 0)
	base := nestedListBArrow(t, a, 1, 0)
	fields := append([]arrow.Field(nil), base.Schema().Fields()[:2]...)
	base.Release()
	rb := array.NewRecordBuilder(a, arrow.NewSchema(fields, nil))
	wants := map[string][]any{"ints": {}, "texts": {}}
	for row := 0; row < 5; row++ {
		var ints, texts []any
		if row == 0 {
			ints = make([]any, 4000)
			texts = make([]any, 4000)
			for i := range ints {
				ints[i] = []any{}
				texts[i] = []any{}
			}
		} else {
			n := 2048
			if row == 1 {
				n = 8192
			}
			ints = []any{[]any{int64(row), nil}}
			texts = []any{[]any{strings.Repeat("x", n), nil, ""}}
		}
		listStructBAppend(rb.Field(0), ints)
		listStructBAppend(rb.Field(1), texts)
		wants["ints"] = append(wants["ints"], ints)
		wants["texts"] = append(wants["texts"], texts)
	}
	r := rb.NewRecordBatch()
	rb.Release()
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, r)
	r.Release()
	if e != nil {
		t.Fatal(e)
	}
	result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "bytes"), ParquetSinkOptions{SinkOptions: SinkOptions{TargetBytesPerFile: 1024}})
	if e != nil {
		t.Fatal(e)
	}
	if result.Rows != 5 || len(result.Files) != 5 {
		t.Fatal("byte-only soft target", result)
	}
	for _, native := range []bool{false, true} {
		got := map[string][]any{}
		for _, p := range result.Files {
			if native {
				rq, e := s.ScanParquetWithOptions(p, ParquetScanOptions{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				df, e := rq.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				for _, name := range []string{"ints", "texts"} {
					c, e := df.Column(name)
					if e != nil {
						t.Fatal(e)
					}
					v, e := c.ToAnySlice()
					if e != nil {
						t.Fatal(e)
					}
					got[name] = append(got[name], v...)
				}
				df.Close()
			} else {
				rd, e := file.OpenParquetFile(p, false)
				if e != nil {
					t.Fatal(e)
				}
				ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, a)
				if e != nil {
					t.Fatal(e)
				}
				tab, e := ar.ReadTable(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				for i, f := range tab.Schema().Fields() {
					for _, chunk := range tab.Column(i).Data().Chunks() {
						for row := 0; row < chunk.Len(); row++ {
							got[f.Name] = append(got[f.Name], listStructSinkArrowValue(chunk, row))
						}
					}
				}
				tab.Release()
				rd.Close()
			}
		}
		if !reflect.DeepEqual(got, wants) {
			t.Fatal("byte shards complete oracle", native)
		}
	}
	s.Close()
}

func TestParquetNestedListDSpillSink(t *testing.T) {
	path := nestedListDFixture(t, 8)
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.MemoryLimitBytes = 8 << 20
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	s := NewSession(opts)
	defer s.Close()
	q, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	budget, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	observer := &recursiveCRunContext{Context: memory.WithQueryBudget(context.Background(), budget), dir: dir}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
				observer.Err()
				runtime.Gosched()
			}
		}
	}()
	result, e := q.Sort(Desc("id")).SinkParquet(observer, filepath.Join(t.TempDir(), "sorted.parquet"), ParquetSinkOptions{RowGroupRows: 17, SinkOptions: SinkOptions{Batch: BatchOptions{TargetBatchBytes: 4096, MaxBatchBytes: 2 << 20}}})
	close(done)
	<-stopped
	if e != nil {
		t.Fatal(e)
	}
	if result.Rows != 260 || budget.Snapshot().SpilledBytes <= 0 || observer.maxRuns < 2 || observer.diskBytes <= 0 {
		t.Fatal("actual sink spill", result, budget.Snapshot(), observer.maxRuns)
	}
	t.Logf("actual Parquet→sort→sink spill=%d runs=%d disk=%d", budget.Snapshot().SpilledBytes, observer.maxRuns, observer.diskBytes)
	budget.Close()
	s.Close()
	recursiveCNoFiles(t, dir)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("sink spill memory")
	}
	rows := []int{}
	for row := 64; row >= 0; row-- {
		for repeat := 0; repeat < 4; repeat++ {
			rows = append(rows, row)
		}
	}
	nestedListDSinkRead(t, result.Files, 8, rows, false)
}

func TestParquetNestedListDNullableProfiles(t *testing.T) {
	for _, text := range []bool{false, true} {
		for mask := 0; mask < 8; mask++ {
			t.Run(fmt.Sprintf("text%v-mask%d", text, mask), func(t *testing.T) {
				var typ arrow.DataType = arrow.PrimitiveTypes.Int64
				var value any = int64(42)
				if text {
					typ = arrow.BinaryTypes.String
					value = "中\x00🙂"
				}
				field := arrow.Field{Name: "literal.中", Nullable: mask&1 != 0, Type: arrow.ListOfField(arrow.Field{Name: "item", Nullable: mask&2 != 0, Type: arrow.ListOfField(arrow.Field{Name: "item", Type: typ, Nullable: mask&4 != 0})})}
				want := []any{[]any{[]any{value}}, []any{}, []any{[]any{}}}
				if mask&1 != 0 {
					want = append(want, nil)
				}
				if mask&2 != 0 {
					want = append(want, []any{nil})
				}
				if mask&4 != 0 {
					want = append(want, []any{[]any{nil}})
				}
				a := am.NewCheckedAllocator(am.DefaultAllocator)
				defer a.AssertSize(t, 0)
				rb := array.NewRecordBuilder(a, arrow.NewSchema([]arrow.Field{field}, nil))
				for _, v := range want {
					listStructBAppend(rb.Field(0), v)
				}
				r := rb.NewRecordBatch()
				rb.Release()
				s := NewSession(engine.DefaultOptions())
				defer s.Close()
				q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true}, r)
				r.Release()
				if e != nil {
					t.Fatal(e)
				}
				result, e := q.SinkParquet(context.Background(), filepath.Join(t.TempDir(), "nullable.parquet"), ParquetSinkOptions{RowGroupRows: 2})
				if e != nil {
					t.Fatal(e)
				}
				nq, e := s.ScanParquetWithOptions(result.Files[0], ParquetScanOptions{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				df, e := nq.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				f := df.Schema().Fields[0]
				if f.Nullable != (mask&1 != 0) || f.Children[0].Nullable != (mask&2 != 0) || f.Children[0].Children[0].Nullable != (mask&4 != 0) {
					t.Fatal("native nullable", f)
				}
				c, e := df.Column(field.Name)
				if e != nil {
					t.Fatal(e)
				}
				got, e := c.ToAnySlice()
				if e != nil || !reflect.DeepEqual(got, want) {
					t.Fatal("native values", got, want, e)
				}
				rd, e := file.OpenParquetFile(result.Files[0], false)
				if e != nil {
					t.Fatal(e)
				}
				defer rd.Close()
				ar, e := pqarrow.NewFileReader(rd, pqarrow.ArrowReadProperties{}, a)
				if e != nil {
					t.Fatal(e)
				}
				tab, e := ar.ReadTable(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer tab.Release()
				af := tab.Schema().Field(0)
				inner := af.Type.(*arrow.ListType).ElemField()
				leaf := inner.Type.(*arrow.ListType).ElemField()
				if af.Nullable != (mask&1 != 0) || inner.Nullable != (mask&2 != 0) || leaf.Nullable != (mask&4 != 0) || leaf.Type.ID() != typ.ID() {
					t.Fatal("Arrow nullable/type", af)
				}
				got = nil
				for _, chunk := range tab.Column(0).Data().Chunks() {
					for row := 0; row < chunk.Len(); row++ {
						got = append(got, listStructSinkArrowValue(chunk, row))
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("independent values", got, want)
				}
				s.Close()
			})
		}
	}
}
