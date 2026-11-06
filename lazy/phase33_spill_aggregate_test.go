package lazy

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/engine"
	"gocolumn/hashagg"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func spillAggregateInput(t testing.TB, batches, rows, groups int) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true},
	)
	input := make([]*memory.Batch, 0, batches)
	ordinal := 0
	for batchIndex := 0; batchIndex < batches; batchIndex++ {
		batch := memory.NewBatch()
		keys := &memory.Int32Vector{Values: make([]int32, rows), Validity: memory.EnsureBitmap(nil, rows)}
		values := &memory.Int64Vector{Values: make([]int64, rows), Validity: memory.EnsureBitmap(nil, rows)}
		for row := 0; row < rows; row++ {
			key := ordinal % groups
			keys.Values[row] = int32(key)
			values.Values[row] = int64(key%17 - 8)
			if key != 0 {
				memory.Set(keys.Validity, row)
			}
			if ordinal%11 != 0 {
				memory.Set(values.Validity, row)
			}
			ordinal++
		}
		batch.Int32s["key"] = keys
		batch.Int64s["value"] = values
		batch.Reset(rows)
		input = append(input, batch)
	}
	src, err := inmemory.New(schema, input...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestBoundedAggregateSpillsAndMatchesResidentResults(t *testing.T) {
	src := spillAggregateInput(t, 8, 512, 1024)
	base := engine.DefaultOptions()
	resident := NewSession(base)
	defer resident.Close()
	query := func(session *Session) LazyFrame {
		return session.FromSource(src).GroupBy("key").Agg(
			Sum("value", "sum"), Count("rows"), CountColumn("value", "values"),
			Min("value", "min"), Max("value", "max"), Avg("value", "avg"),
		)
	}
	want, err := query(resident).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer want.Close()

	spillDir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 192 << 10
	opts.QueryMemorySoftLimitBytes = 96 << 10
	opts.Spill = engine.SpillOptions{Directory: spillDir, MaxBytes: 32 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	bounded := NewSession(opts)
	defer bounded.Close()
	got, err := query(bounded).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Height() != want.Height() {
		t.Fatalf("bounded groups=%d resident=%d", got.Height(), want.Height())
	}
	if !strings.Contains(mustExplainPhysical(t, query(bounded)), "SPILLABLE Aggregate") {
		t.Fatal("physical explain does not identify spillable aggregate")
	}
	assertAggregateFramesEqualByKey(t, got, want)
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
	if used := bounded.Engine().Memory().Used(); used != 0 {
		t.Fatalf("bounded aggregate leaked %d reserved bytes", used)
	}
	entries, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("query spill directories remain: %v", entries)
	}
}

func TestBoundedAggregateExplainAnalyzeReportsSpillAndSkew(t *testing.T) {
	src := spillAggregateInput(t, 8, 512, 1024)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 192 << 10
	opts.QueryMemorySoftLimitBytes = 96 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()

	report, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).ExplainAnalyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"spill_runs=", "spill_bytes=", "peak_memory_bytes=", "max_radix_depth=", "max_partition_groups="} {
		if !strings.Contains(report, field) {
			t.Fatalf("ExplainAnalyze missing %q:\n%s", field, report)
		}
	}
}

func TestBoundedAggregateRequiresFiniteMemoryAndSpillQuota(t *testing.T) {
	src := spillAggregateInput(t, 1, 8, 4)
	query := func(opts engine.Options) error {
		session := NewSession(opts)
		defer session.Close()
		_, err := session.FromSource(src).WithExecution(ExecutionOptions{Policy: ExecutionBounded}).GroupBy("key").Agg(Count("n")).Prepare(context.Background())
		return err
	}
	noMemory := engine.DefaultOptions()
	if err := query(noMemory); !errors.Is(err, engine.ErrBoundedMemoryLimitRequired) {
		t.Fatalf("missing memory error=%v", err)
	}
	noSpill := engine.DefaultOptions()
	noSpill.MemoryLimitBytes = 64 << 10
	if err := query(noSpill); !errors.Is(err, engine.ErrBoundedSpillRequired) {
		t.Fatalf("missing spill error=%v", err)
	}
}

func TestBoundedAggregateDrainHandlesInputMuchLargerThanBudget(t *testing.T) {
	src := spillAggregateInput(t, 64, 1024, 8192)
	spillDir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 256 << 10
	opts.QueryMemorySoftLimitBytes = 128 << 10
	opts.Spill = engine.SpillOptions{Directory: spillDir, MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	queryBudget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), queryBudget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.DrainBatchStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 8192 || stats.Batches < 2 {
		t.Fatalf("drain stats=%+v", stats)
	}
	if snapshot := queryBudget.Snapshot(); snapshot.SpillRequests == 0 || snapshot.SpilledBytes == 0 || snapshot.PeakBytes > opts.MemoryLimitBytes {
		t.Fatalf("aggregate did not spill under pressure: %+v", snapshot)
	}
	if err := queryBudget.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("drain leaked %d reserved bytes", used)
	}
	if matches, err := filepath.Glob(filepath.Join(spillDir, "golume-q-*")); err != nil || len(matches) != 0 {
		t.Fatalf("spill cleanup matches=%v err=%v", matches, err)
	}
}

func TestBoundedAggregatePreservesStringDictionaryAndCompositeKeys(t *testing.T) {
	newSession := func(t *testing.T) *Session {
		t.Helper()
		opts := engine.DefaultOptions()
		opts.MemoryLimitBytes = 96 << 10
		opts.QueryMemorySoftLimitBytes = 48 << 10
		opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 8 << 20}
		opts.ExecutionPolicy = engine.ExecutionBounded
		return NewSession(opts)
	}
	t.Run("plain-string", func(t *testing.T) {
		schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingView})
		batch := memory.NewBatch()
		validity := memory.EnsureBitmap(nil, 5)
		for _, row := range []int{0, 1, 3, 4} {
			memory.Set(validity, row)
		}
		batch.Strings["key"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 3, 3, 7, 10}, Data: []byte("中🙂中"), Validity: validity, Encoding: memory.EncodingView}
		batch.Reset(5)
		src, err := inmemory.New(schema, batch)
		if err != nil {
			t.Fatal(err)
		}
		session := newSession(t)
		defer session.Close()
		df, err := session.FromSource(src).GroupBy("key").Agg(Count("n")).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer df.Close()
		seen := map[string]uint64{}
		for _, out := range df.batches() {
			keys, counts := out.Strings["key"], out.Uint64s["n"]
			if err := keys.Validate(out.Rows); err != nil {
				t.Fatal(err)
			}
			for row := 0; row < out.Rows; row++ {
				name := "NULL"
				if keys.IsValid(row) {
					name = string(keys.ValueBytes(row))
				}
				seen[name] = counts.Values[row]
			}
		}
		if seen[""] != 1 || seen["中"] != 2 || seen["🙂"] != 1 || seen["NULL"] != 1 {
			t.Fatalf("string groups=%v", seen)
		}
	})
	t.Run("dictionary", func(t *testing.T) {
		schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary, DictionaryDomain: "colors"})
		batch := memory.NewBatch()
		validity := memory.EnsureBitmap(nil, 4)
		for _, row := range []int{0, 1, 3} {
			memory.Set(validity, row)
		}
		batch.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0, 1}, Offsets: []uint32{0, 3, 7}, Data: []byte("redblue"), Validity: validity, DictionaryDomain: "colors", Encoding: memory.EncodingDictionary}
		batch.Reset(4)
		src, err := inmemory.New(schema, batch)
		if err != nil {
			t.Fatal(err)
		}
		src.WithDictionary("key", source.Dictionary{Domain: "colors", Offsets: []uint32{0, 3, 7}, Data: []byte("redblue")})
		session := newSession(t)
		defer session.Close()
		df, err := session.FromSource(src).GroupBy("key").Agg(Count("n")).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer df.Close()
		if df.Height() != 3 {
			t.Fatalf("dictionary groups=%d", df.Height())
		}
		for _, out := range df.batches() {
			if got := out.DictStrings["key"].DictionaryDomain; got != "colors" {
				t.Fatalf("dictionary domain=%q", got)
			}
		}
	})
	t.Run("composite", func(t *testing.T) {
		schema := memory.NewSchema(
			memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true},
			memory.Field{Name: "b", Type: memory.TypeUint32, Nullable: true},
		)
		batch := memory.NewBatch()
		av, bv := memory.EnsureBitmap(nil, 5), memory.EnsureBitmap(nil, 5)
		for _, row := range []int{0, 1, 3, 4} {
			memory.Set(av, row)
		}
		for _, row := range []int{0, 2, 3, 4} {
			memory.Set(bv, row)
		}
		batch.Int64s["a"] = &memory.Int64Vector{Values: []int64{1, 1, 9, 2, 1}, Validity: av}
		batch.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{7, 8, 7, 9, 7}, Validity: bv}
		batch.Reset(5)
		src, err := inmemory.New(schema, batch)
		if err != nil {
			t.Fatal(err)
		}
		session := newSession(t)
		defer session.Close()
		df, err := session.FromSource(src).GroupBy("a", "b").Agg(Count("n")).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer df.Close()
		if df.Height() != 4 {
			t.Fatalf("composite groups=%d want=4", df.Height())
		}
	})
}

func TestBoundedSortedAggregateNeedsNoSpillQuota(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true},
	)
	valid := memory.EnsureBitmap(nil, 6)
	for _, row := range []int{1, 2, 3, 4, 5} {
		memory.Set(valid, row)
	}
	batch := memory.NewBatch()
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{0, 1, 1, 2, 2, 2}, Validity: valid}
	batch.Int64s["value"] = &memory.Int64Vector{Values: []int64{4, 1, 2, 3, 4, 5}}
	batch.Reset(6)
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	src.WithOrdering(source.OrderingColumn{Name: "key", NullsLast: false})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n"))
	prepared, err := query.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if plan := source.StreamPlanOf(prepared.src); plan.Class != source.StreamBounded || !strings.Contains(plan.Detail, "sorted-adjacent") {
		t.Fatalf("sorted aggregate stream plan=%+v", plan)
	}
	df, err := prepared.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if got := frameInt64Column(t, df, "sum"); len(got) != 3 || got[0] != 4 || got[1] != 3 || got[2] != 12 {
		t.Fatalf("sorted sums=%v", got)
	}
}

func TestBoundedSortedAggregateStreamsResultLargerThanBudget(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	)
	batches := make([]*memory.Batch, 10)
	for batchIndex := range batches {
		batch := memory.NewBatch()
		keys := make([]int32, 1000)
		values := make([]int64, 1000)
		for row := range keys {
			keys[row] = int32(batchIndex*1000 + row)
			values[row] = 1
		}
		batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(len(keys))
		batches[batchIndex] = batch
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	src.WithOrdering(source.OrderingColumn{Name: "key"})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 32 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum")).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	queryBudget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), queryBudget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.DrainBatchStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 10000 || stats.Batches < 10 {
		t.Fatalf("sorted drain stats=%+v", stats)
	}
	if snapshot := queryBudget.Snapshot(); snapshot.SpillRequests != 0 || snapshot.SpilledBytes != 0 {
		t.Fatalf("sorted aggregation unexpectedly spilled: %+v", snapshot)
	}
	if err := queryBudget.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("sorted aggregate leaked %d reserved bytes", used)
	}
}

func TestBoundedSpillAggregatePreparedRepeatsAndCancellationCleansUp(t *testing.T) {
	src := spillAggregateInput(t, 8, 256, 512)
	spillDir := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 128 << 10
	opts.QueryMemorySoftLimitBytes = 64 << 10
	opts.Spill = engine.SpillOptions{Directory: spillDir, MaxBytes: 16 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for execution := 0; execution < 3; execution++ {
		df, err := prepared.Execute(context.Background())
		if err != nil {
			t.Fatalf("execution %d: %v", execution, err)
		}
		if df.Height() != 512 {
			t.Fatalf("execution %d groups=%d", execution, df.Height())
		}
		if err := df.Close(); err != nil {
			t.Fatal(err)
		}
		if used := session.Engine().Memory().Used(); used != 0 {
			t.Fatalf("execution %d retained %d bytes", execution, used)
		}
		entries, err := os.ReadDir(spillDir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("execution %d spill cleanup entries=%v err=%v", execution, entries, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepared.Execute(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution error=%v", err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("canceled execution retained %d bytes", used)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPhase33SpillableAggregateDrain(b *testing.B) {
	src := spillAggregateInput(b, 64, 1024, 8192)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 256 << 10
	opts.QueryMemorySoftLimitBytes = 128 << 10
	opts.Spill = engine.SpillOptions{Directory: b.TempDir(), MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).Prepare(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer prepared.Close()
	var totalSpill int64
	var maxPeak int64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		queryBudget, err := session.Engine().BeginQueryMemory()
		if err != nil {
			b.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), queryBudget)
		stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
		if err != nil {
			_ = queryBudget.Close()
			b.Fatal(err)
		}
		stats, err := source.DrainBatchStream(ctx, stream)
		if err != nil || stats.Rows != 8192 {
			_ = queryBudget.Close()
			b.Fatalf("drain stats=%+v err=%v", stats, err)
		}
		snapshot := queryBudget.Snapshot()
		totalSpill += snapshot.SpilledBytes
		if snapshot.PeakBytes > maxPeak {
			maxPeak = snapshot.PeakBytes
		}
		if err := queryBudget.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(64*1024*b.N)/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(float64(totalSpill)/float64(b.N), "spill-B/op")
	b.ReportMetric(float64(maxPeak), "peak-B")
}

func TestBoundedAggregateRecursivelyRepartitionsOversizedRadixBucket(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64})
	keys := make([]int32, 0, 512)
	for candidate := int32(0); len(keys) < cap(keys); candidate++ {
		if hashagg.Hash64(uint64(uint32(candidate)))&15 == 0 {
			keys = append(keys, candidate)
		}
	}
	batch := memory.NewBatch()
	batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
	batch.Int64s["value"] = &memory.Int64Vector{Values: make([]int64, len(keys))}
	batch.Reset(len(keys))
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 32 << 10
	opts.QueryMemorySoftLimitBytes = 16 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).GroupBy("key").Agg(Sum("value", "sum"), Count("n")).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	queryBudget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), queryBudget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, ok := stream.(interface {
		Stats() engine.AggregateStreamStats
	})
	if !ok {
		t.Fatalf("stream %T has no aggregate diagnostics", stream)
	}
	stats, err := source.DrainBatchStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	aggregateStats := diagnostics.Stats()
	if stats.Rows != 512 || aggregateStats.MaxRadixDepth < 2 || aggregateStats.SpillRuns <= 4 {
		t.Fatalf("drain=%+v aggregate=%+v", stats, aggregateStats)
	}
	if peak := queryBudget.Snapshot().PeakBytes; peak <= 0 || peak > opts.MemoryLimitBytes {
		t.Fatalf("recursive aggregate peak=%d limit=%d", peak, opts.MemoryLimitBytes)
	}
	if err := queryBudget.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("recursive aggregate leaked %d bytes", used)
	}
}

func mustExplainPhysical(t *testing.T, frame LazyFrame) string {
	t.Helper()
	explain, err := frame.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return explain
}

func assertAggregateFramesEqualByKey(t *testing.T, got, want DataFrame) {
	t.Helper()
	type row struct {
		sum, rows, values, min, max int64
		avg                         float64
		valid                       [4]bool
	}
	read := func(frame DataFrame) map[string]row {
		out := make(map[string]row)
		for _, batch := range frame.batches() {
			keys := batch.Int32s["key"]
			sums := batch.Int64s["sum"]
			rows := batch.Uint64s["rows"]
			values := batch.Uint64s["values"]
			mins := batch.Int64s["min"]
			maxs := batch.Int64s["max"]
			avgs := batch.Float64s["avg"]
			for i := 0; i < batch.Rows; i++ {
				key := "NULL"
				if keys.IsValid(i) {
					key = fmt.Sprint(keys.Values[i])
				}
				out[key] = row{
					sum: sums.Values[i], rows: int64(rows.Values[i]), values: int64(values.Values[i]),
					min: mins.Values[i], max: maxs.Values[i], avg: avgs.Values[i],
					valid: [4]bool{sums.IsValid(i), mins.IsValid(i), maxs.IsValid(i), avgs.IsValid(i)},
				}
			}
		}
		return out
	}
	a, b := read(got), read(want)
	if len(a) != len(b) {
		t.Fatalf("group maps differ got=%d want=%d", len(a), len(b))
	}
	for key, expected := range b {
		if actual, ok := a[key]; !ok || actual != expected {
			t.Fatalf("group %s got=%+v want=%+v present=%v", key, actual, expected, ok)
		}
	}
}
