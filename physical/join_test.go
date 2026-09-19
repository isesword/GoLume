package physical

import (
	"context"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/scheduler"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestJoinSourceFeedsColumnarPayload(t *testing.T) {
	leftSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32}, memory.Field{Name: "amount", Type: memory.TypeInt64})
	lb := memory.NewBatch()
	lb.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	lb.Int64s["amount"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	lb.Reset(3)
	left, _ := inmemory.New(leftSchema, lb)
	rightSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32}, memory.Field{Name: "bonus", Type: memory.TypeInt64})
	rb := memory.NewBatch()
	rb.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 2, 3}}
	rb.Int64s["bonus"] = &memory.Int64Vector{Values: []int64{5, 7, 11}}
	rb.Reset(3)
	right, _ := inmemory.New(rightSchema, rb)
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "_r", memory.NewBudgetManager(1<<20), kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	parts, _ := js.Partitions(context.Background())
	if len(parts) != 1 || parts[0].Rows < 3 {
		t.Fatalf("parts=%v", parts)
	}
	stats, _ := js.Stats(context.Background())
	if !stats.RowsUpperBound {
		t.Fatalf("join stats should mark row count as upper bound: %+v", stats)
	}
	r, err := openTestReader(context.Background(), js, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	ids, _ := out.Int32("id")
	bonus, _ := out.Int64("bonus")
	amount, _ := out.Int64("amount")
	if len(ids.Values) != 3 {
		t.Fatalf("rows=%d", out.Rows)
	}
	var sum int64
	for i := range bonus.Values {
		sum += amount.Values[i] + bonus.Values[i]
	}
	if sum != 93 {
		t.Fatalf("sum=%d ids=%v", sum, ids.Values)
	}
}

func TestJoinSourceNativeInt8KeyAndPayload(t *testing.T) {
	leftSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt8}, memory.Field{Name: "amount", Type: memory.TypeInt64})
	lb := memory.NewBatch()
	lb.Int8s["id"] = &memory.Int8Vector{Values: []int8{1, 2, 2}}
	lb.Int64s["amount"] = &memory.Int64Vector{Values: []int64{10, 20, 30}}
	lb.Reset(3)
	left, err := inmemory.New(leftSchema, lb)
	if err != nil {
		t.Fatal(err)
	}
	rightSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt8}, memory.Field{Name: "bonus", Type: memory.TypeFloat32})
	rb := memory.NewBatch()
	rb.Int8s["id"] = &memory.Int8Vector{Values: []int8{2, 2}}
	rb.Float32s["bonus"] = &memory.Float32Vector{Values: []float32{1.5, 2.5}}
	rb.Reset(2)
	right, err := inmemory.New(rightSchema, rb)
	if err != nil {
		t.Fatal(err)
	}
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "_r", memory.NewBudgetManager(1<<20), kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	parts, _ := js.Partitions(context.Background())
	r, err := openTestReader(context.Background(), js, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	ids, err := out.Int8("id")
	if err != nil {
		t.Fatal(err)
	}
	bonus, err := out.Float32("bonus")
	if err != nil {
		t.Fatal(err)
	}
	if out.Rows != 4 || len(ids.Values) != 4 || len(bonus.Values) != 4 {
		t.Fatalf("rows=%d ids=%v bonus=%v", out.Rows, ids.Values, bonus.Values)
	}
}

type countingSource struct {
	source.Source
	reads int
}

type countingReader struct {
	parent *countingSource
	inner  source.Reader
}

func (s *countingSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	r, err := openTestReader(ctx, s.Source, opts)
	if err != nil {
		return nil, err
	}
	return &countingReader{parent: s, inner: r}, nil
}

func (r *countingReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	r.parent.reads++
	return r.inner.ReadPartition(ctx, p, dst)
}

func (r *countingReader) Close() error { return r.inner.Close() }

func TestJoinSourcePrepareDoesNotReadProbeSide(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32})
	lb := memory.NewBatch()
	lb.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	lb.Reset(3)
	baseLeft, _ := inmemory.New(schema, lb)
	left := &countingSource{Source: baseLeft}
	rb := memory.NewBatch()
	rb.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 3}}
	rb.Reset(2)
	right, _ := inmemory.New(schema, rb)
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSource(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "", memory.NewBudgetManager(1<<20), kernels)
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	if left.reads != 0 {
		t.Fatalf("prepare read probe side %d times", left.reads)
	}
	parts, _ := js.Partitions(context.Background())
	r, err := openTestReader(context.Background(), js, source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	if left.reads != 1 {
		t.Fatalf("execute probe reads=%d want=1", left.reads)
	}
}

func TestJoinSourceUsesRadixForLargeFixedBuild(t *testing.T) {
	const buildRows = 16384
	leftSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32})
	lb := memory.NewBatch()
	lb.Int32s["id"] = &memory.Int32Vector{Values: []int32{7, buildRows - 1, buildRows + 1}}
	lb.Reset(3)
	left, _ := inmemory.New(leftSchema, lb)

	rb := memory.NewBatch()
	ids := make([]int32, buildRows)
	for i := range ids {
		ids[i] = int32(i)
	}
	rb.Int32s["id"] = &memory.Int32Vector{Values: ids}
	rb.Reset(buildRows)
	right, _ := inmemory.New(leftSchema, rb)

	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSourceWithOptions(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "", memory.NewBudgetManager(32<<20), JoinSourceOptions{Kernels: kernels, RadixBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	if got := js.Strategy(); got != "radix-hash/16" {
		t.Fatalf("strategy=%q", got)
	}
	parts, _ := js.Partitions(context.Background())
	r, _ := openTestReader(context.Background(), js, source.ReadOptions{Kernels: kernels})
	defer r.Close()
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	if out.Rows != 2 {
		t.Fatalf("rows=%d", out.Rows)
	}
}

func TestParallelRadixJoinSource(t *testing.T) {
	leftSchema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt32})
	lb := memory.NewBatch()
	lb.Int32s["id"] = &memory.Int32Vector{Values: []int32{1, 2, 3, 4}}
	lb.Reset(4)
	left, _ := inmemory.New(leftSchema, lb)
	rb := memory.NewBatch()
	rb.Int32s["id"] = &memory.Int32Vector{Values: []int32{2, 2, 4}}
	rb.Reset(3)
	right, _ := inmemory.New(leftSchema, rb)
	pool := scheduler.NewPool(4)
	defer pool.Close()
	kernels := kernel.New(kernel.ModeScalar)
	js, err := NewJoinSourceWithOptions(context.Background(), left, right, join.Inner, []JoinKey{{Left: "id", Right: "id"}}, "", memory.NewBudgetManager(1<<20), JoinSourceOptions{Kernels: kernels, RadixBits: 2, Workers: 4, Executor: pool})
	if err != nil {
		t.Fatal(err)
	}
	defer js.Close()
	if got := js.Strategy(); got != "parallel-radix-hash/4/w4" {
		t.Fatalf("strategy=%q", got)
	}
	parts, _ := js.Partitions(context.Background())
	r, _ := openTestReader(context.Background(), js, source.ReadOptions{Kernels: kernels})
	defer r.Close()
	out := memory.NewBatch()
	if err := r.ReadPartition(context.Background(), parts[0], out); err != nil {
		t.Fatal(err)
	}
	if out.Rows != 3 {
		t.Fatalf("rows=%d want=3", out.Rows)
	}
}

func TestParallelRadixJoinHeuristic(t *testing.T) {
	if shouldUseParallelRadixJoin(1<<18, 1<<22, 8) {
		t.Fatal("small build should stay on single table")
	}
	if shouldUseParallelRadixJoin(1<<20, 1<<20, 8) {
		t.Fatal("small probe should stay on single table")
	}
	if shouldUseParallelRadixJoin(1<<20, 1<<22, 2) {
		t.Fatal("insufficient workers should stay on single table")
	}
	if !shouldUseParallelRadixJoin(1<<20, 1<<22, 8) {
		t.Fatal("large fixed join should select parallel radix")
	}
}

func TestBoundedBroadcastDictionaryJoinNormalizesDomains(t *testing.T) {
	leftDictionary := source.Dictionary{Domain: "left", Offsets: []uint32{0, 3, 7}, Data: []byte("redblue")}
	rightDictionary := source.Dictionary{Domain: "right", Offsets: []uint32{0, 4, 7, 12}, Data: []byte("blueredgreen")}
	leftBatch := memory.NewBatch()
	leftBatch.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0}, Offsets: leftDictionary.Offsets, Data: leftDictionary.Data, DictionaryDomain: leftDictionary.Domain, Encoding: memory.EncodingDictionary}
	leftBatch.Reset(3)
	rightBatch := memory.NewBatch()
	rightBatch.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 2}, Offsets: rightDictionary.Offsets, Data: rightDictionary.Data, DictionaryDomain: rightDictionary.Domain, Encoding: memory.EncodingDictionary}
	rightBatch.Reset(3)
	left, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: leftDictionary.Domain}), leftBatch)
	right, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: rightDictionary.Domain}), rightBatch)
	left.WithDictionary("key", leftDictionary)
	right.WithDictionary("key", rightDictionary)
	kernels := kernel.New(kernel.ModeScalar)
	joinSource, err := NewJoinSourceWithOptions(context.Background(), left, right, join.Inner, []JoinKey{{Left: "key", Right: "key"}}, "", memory.NewBudgetManager(256<<10), JoinSourceOptions{
		Kernels: kernels, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: 256 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer joinSource.Close()
	if joinSource.strategy != "broadcast-hash" || joinSource.dict == nil {
		t.Fatalf("strategy=%s dict=%v", joinSource.strategy, joinSource.dict != nil)
	}
	stream, err := joinSource.NewBatchStream(context.Background(), source.ReadOptions{Kernels: kernels})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	ok, err := stream.Next(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || batch.Rows != 3 {
		t.Fatalf("ok=%v rows=%d diagnostics=%v", ok, batch.Rows, stream.(source.BatchStreamDiagnostics).StreamDiagnostics())
	}
	_ = stream.Close()
}
