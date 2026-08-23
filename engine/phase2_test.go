package engine_test

import (
	"context"
	"fmt"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	parquetsource "gocolumn/source/parquet"
	"sync"
	"testing"
)

func baseQuery(group []string, force string, workers int) expr.Query {
	return expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(0)), GroupBy: group, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Selectivity: .8, Cardinality: 64, ForceStrategy: force, ForceWorkerCount: workers}}
}

func TestDictionaryStringGroupByAndMaterialize(t *testing.T) {
	off := []uint32{0, 1, 2, 3}
	data := []byte("abc")
	mk := func(pred []int64, idx []uint32, vals []int64) *memory.Batch {
		b := memory.NewBatch()
		b.Rows = len(pred)
		b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
		b.Int64s["value"] = &memory.Int64Vector{Values: vals}
		b.DictStrings["key"] = &memory.DictionaryStringVector{Indices: idx, Offsets: off, Data: data, DictionaryDomain: "dict-v1", Encoding: memory.EncodingDictionary}
		return b
	}
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: "dict-v1"}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, mk([]int64{1, 2, 3, 4}, []uint32{0, 1, 2, 0}, []int64{1, 2, 3, 4}), mk([]int64{5, 6, 7, 8}, []uint32{2, 1, 0, 1}, []int64{5, 6, 7, 8}))
	if err != nil {
		t.Fatal(err)
	}
	src.WithDictionary("key", source.Dictionary{Domain: "dict-v1", Offsets: off, Data: data})
	for _, force := range []string{"direct-fused", "bitmap-direct", "radix"} {
		t.Run(force, func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.Workers = 2
			eng := engine.New(opts)
			defer eng.Close()
			q := baseQuery([]string{"key"}, force, 2)
			r, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
			if err != nil {
				t.Fatal(err)
			}
			if r.GroupCount != 3 || r.SelectedRows != 8 || r.Checksum != 36 {
				t.Fatalf("result=%+v", r)
			}
			got := map[string]int64{}
			for _, row := range r.Rows {
				got[row.Keys[0].String] = row.Sum
			}
			if got["a"] != 12 || got["b"] != 16 || got["c"] != 8 {
				t.Fatalf("groups=%v", got)
			}
		})
	}
}

func stringBatch(keys []string, vals []int64) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(keys)
	pred := make([]int64, len(keys))
	for i := range pred {
		pred[i] = 1
	}
	offs := make([]uint32, len(keys)+1)
	var data []byte
	for i, k := range keys {
		data = append(data, k...)
		offs[i+1] = uint32(len(data))
	}
	b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
	b.Int64s["value"] = &memory.Int64Vector{Values: vals}
	b.Strings["key"] = &memory.StringViewVector{Offsets: offs, Data: data, Encoding: memory.EncodingView}
	return b
}
func TestPlainStringViewPersistsAcrossBatches(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingView}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, stringBatch([]string{"aa", "bb", "aa", "cc"}, []int64{1, 2, 3, 4}), stringBatch([]string{"bb", "dd", "aa"}, []int64{5, 6, 7}))
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	eng := engine.New(opts)
	defer eng.Close()
	r, err := eng.Execute(context.Background(), src, baseQuery([]string{"key"}, "direct-fused", 1), engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != 4 || r.Checksum != 28 || r.SelectedRows != 7 {
		t.Fatalf("result=%+v", r)
	}
	got := map[string]int64{}
	for _, row := range r.Rows {
		got[row.Keys[0].String] = row.Sum
	}
	if got["aa"] != 11 || got["bb"] != 7 || got["cc"] != 4 || got["dd"] != 6 {
		t.Fatalf("groups=%v", got)
	}
}

func pairBatch(a []int32, bk []int64, vals []int64) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(a)
	pred := make([]int64, len(a))
	for i := range pred {
		pred[i] = 1
	}
	b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
	b.Int32s["a"] = &memory.Int32Vector{Values: a}
	b.Int64s["b"] = &memory.Int64Vector{Values: bk}
	b.Int64s["value"] = &memory.Int64Vector{Values: vals}
	return b
}
func TestMultiColumnGroupByRadix(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeInt32}, memory.Field{Name: "b", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, pairBatch([]int32{1, 1, 1, 2}, []int64{10, 10, 20, 10}, []int64{1, 2, 3, 4}), pairBatch([]int32{2, 1, 3, 3}, []int64{10, 20, 30, 30}, []int64{5, 6, 7, 8}))
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 2
	eng := engine.New(opts)
	defer eng.Close()
	q := baseQuery([]string{"a", "b"}, "radix", 2)
	q.Hints.Cardinality = 8
	r, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != 4 || r.Checksum != 36 || r.SelectedRows != 8 {
		t.Fatalf("result=%+v", r)
	}
	type k struct {
		a int64
		b int64
	}
	got := map[k]int64{}
	for _, row := range r.Rows {
		got[k{row.Keys[0].Int64, row.Keys[1].Int64}] = row.Sum
	}
	want := map[k]int64{{1, 10}: 3, {1, 20}: 9, {2, 10}: 9, {3, 30}: 15}
	for key, v := range want {
		if got[key] != v {
			t.Fatalf("key=%v got=%d want=%d all=%v", key, got[key], v, got)
		}
	}
}

func TestCompositePredicateUsesBitmapProgram(t *testing.T) {
	valid := make(memory.Bitmap, 1)
	memory.Set(valid, 0)
	memory.Set(valid, 1)
	memory.Set(valid, 3)
	b := memory.NewBatch()
	b.Rows = 4
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{5, 15, 0, 35}, Validity: valid}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4}}
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, _ := inmemory.New(schema, b)
	opts := engine.DefaultOptions()
	eng := engine.New(opts)
	defer eng.Close()
	q := expr.Query{Filter: expr.And(expr.GT(expr.Col("pred"), expr.I64(10)), expr.LT(expr.Col("pred"), expr.I64(40))), GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("count")}, Hints: expr.Hints{Cardinality: 4, Selectivity: .5}}
	p, err := eng.Prepare(context.Background(), src, q, engine.PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Plan().Strategy.Kind.String() != "bitmap-direct" {
		t.Fatalf("plan=%+v", p.Plan())
	}
	r, err := p.Execute(context.Background(), engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.SelectedRows != 2 || r.GroupCount != 2 || r.Checksum != 6 {
		t.Fatalf("result=%+v", r)
	}
}

func TestPreparedQueryRetainsAndReleasesWorkspace(t *testing.T) {
	src, err := parquetsource.Open(testPath(t), parquetsource.Columns{PredicateIndex: 0, GroupIndex: 1, ValueIndex: 2})
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 4
	eng := engine.New(opts)
	defer eng.Close()
	p, err := eng.Prepare(context.Background(), src, query("radix", 4), engine.PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if eng.Memory().Used() != 0 {
		t.Fatalf("workspace should be lazy, used=%d", eng.Memory().Used())
	}
	r1, err := p.Execute(context.Background(), engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	used1 := eng.Memory().Used()
	if used1 <= 0 {
		t.Fatal("expected retained workspace reservation")
	}
	r2, err := p.Execute(context.Background(), engine.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if used2 := eng.Memory().Used(); used2 != used1 {
		t.Fatalf("workspace was not reused: %d -> %d", used1, used2)
	}
	if r1.GroupCount != r2.GroupCount || r1.Checksum != r2.Checksum {
		t.Fatal("repeated prepared result mismatch")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if eng.Memory().Used() != 0 {
		t.Fatalf("prepared close leaked memory: %d", eng.Memory().Used())
	}
}

func TestPreparedConcurrentExecute(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, _ := inmemory.New(schema, pairToI32Batch())
	opts := engine.DefaultOptions()
	opts.Workers = 2
	eng := engine.New(opts)
	defer eng.Close()
	q := baseQuery([]string{"key"}, "bitmap-direct", 1)
	p, err := eng.Prepare(context.Background(), src, q, engine.PrepareOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := p.Execute(context.Background(), engine.ExecuteOptions{})
			if e == nil && (r.GroupCount != 3 || r.Checksum != 15) {
				e = fmt.Errorf("bad result %+v", r)
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}
func pairToI32Batch() *memory.Batch {
	b := memory.NewBatch()
	b.Rows = 5
	b.Int64s["pred"] = &memory.Int64Vector{Values: []int64{1, 1, 1, 1, 1}}
	b.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 2, 1, 3, 2}}
	b.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 2, 3, 4, 5}}
	return b
}

func TestInt64GroupByRadix(t *testing.T) {
	mk := func(keys []int64, vals []int64) *memory.Batch {
		b := memory.NewBatch()
		b.Rows = len(keys)
		pred := make([]int64, len(keys))
		for i := range pred {
			pred[i] = 1
		}
		b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
		b.Int64s["key64"] = &memory.Int64Vector{Values: keys}
		b.Int64s["value"] = &memory.Int64Vector{Values: vals}
		return b
	}
	schema := memory.NewSchema(memory.Field{Name: "pred", Type: memory.TypeInt64}, memory.Field{Name: "key64", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, mk([]int64{-1, 7, 7}, []int64{1, 2, 3}), mk([]int64{-1, 9, 7}, []int64{4, 5, 6}))
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.Workers = 2
	eng := engine.New(opts)
	defer eng.Close()
	q := baseQuery([]string{"key64"}, "radix", 2)
	q.Hints.Cardinality = 4
	r, err := eng.Execute(context.Background(), src, q, engine.ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.GroupCount != 3 || r.SelectedRows != 6 || r.Checksum != 21 {
		t.Fatalf("result=%+v", r)
	}
	got := map[int64]int64{}
	for _, row := range r.Rows {
		got[row.Keys[0].Int64] = row.Sum
	}
	if got[-1] != 5 || got[7] != 11 || got[9] != 5 {
		t.Fatalf("groups=%v", got)
	}
}
