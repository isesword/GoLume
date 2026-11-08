package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/hashagg"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/optimizer"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func nullableJoinSource(t testing.TB, name string, keys []int32, validRows []int, values []int64) *inmemory.Source {
	t.Helper()
	validity := memory.EnsureBitmap(nil, len(keys))
	for _, row := range validRows {
		memory.Set(validity, row)
	}
	batch := memory.NewBatch()
	batch.Int32s[name] = &memory.Int32Vector{Values: append([]int32(nil), keys...), Validity: validity}
	batch.Int64s["value"] = &memory.Int64Vector{Values: append([]int64(nil), values...)}
	batch.Reset(len(keys))
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: name, Type: memory.TypeInt32, Nullable: true},
		memory.Field{Name: "value", Type: memory.TypeInt64},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func boundedJoinSession(t testing.TB, limit int64) *Session {
	t.Helper()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = limit
	opts.QueryMemorySoftLimitBytes = limit * 3 / 4
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	return NewSession(opts)
}

func multiBatchJoinSource(t testing.TB, keys []int32, batchRows int) *inmemory.Source {
	t.Helper()
	batches := make([]*memory.Batch, 0, (len(keys)+batchRows-1)/batchRows)
	for offset := 0; offset < len(keys); offset += batchRows {
		end := offset + batchRows
		if end > len(keys) {
			end = len(keys)
		}
		batch := memory.NewBatch()
		batch.Int32s["key"] = &memory.Int32Vector{Values: append([]int32(nil), keys[offset:end]...)}
		values := make([]int64, end-offset)
		for row := range values {
			values[row] = int64(offset + row)
		}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(end - offset)
		batches = append(batches, batch)
	}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64},
	), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func lowRadixCollisionKeys(count int) []int32 {
	keys := make([]int32, 0, count)
	for candidate := int32(1); len(keys) < count; candidate++ {
		if hashagg.Hash128(hashagg.Key128{A: uint64(uint32(candidate))})&15 == 0 {
			keys = append(keys, candidate)
		}
	}
	// Preserve the first-radix collision, but cluster the next digit so
	// repartitioning exercises recursion rather than thousands of tiny files.
	sort.Slice(keys, func(i, j int) bool {
		return hashagg.Hash128(hashagg.Key128{A: uint64(uint32(keys[i]))})>>4&15 < hashagg.Hash128(hashagg.Key128{A: uint64(uint32(keys[j]))})>>4&15
	})
	return keys
}

func radixGroupedJoinKeys(rowsPerPartition int) []int32 {
	buckets := make([][]int32, 16)
	remaining := rowsPerPartition * len(buckets)
	for candidate := int32(1); remaining > 0; candidate++ {
		partition := int(hashagg.Hash128(hashagg.Key128{A: uint64(uint32(candidate))}) & 15)
		if len(buckets[partition]) == rowsPerPartition {
			continue
		}
		buckets[partition] = append(buckets[partition], candidate)
		remaining--
	}
	keys := make([]int32, 0, rowsPerPartition*len(buckets))
	for _, bucket := range buckets {
		// Keep recursive child runs large enough to admit their metadata.
		sort.Slice(bucket, func(i, j int) bool {
			return hashagg.Hash128(hashagg.Key128{A: uint64(uint32(bucket[i]))})>>4&15 < hashagg.Hash128(hashagg.Key128{A: uint64(uint32(bucket[j]))})>>4&15
		})
		keys = append(keys, bucket...)
	}
	return keys
}

func TestBoundedJoinUsesActualBytesForSmallBroadcastAndPreservesNulls(t *testing.T) {
	left := nullableJoinSource(t, "key", []int32{1, 2, 2, 3, 0}, []int{0, 1, 2, 3}, []int64{10, 20, 21, 30, 40})
	right := nullableJoinSource(t, "key", []int32{2, 2, 4, 0}, []int{0, 1, 2}, []int64{200, 201, 400, 500})
	for _, tc := range []struct {
		typ  JoinType
		want int64
	}{
		{InnerJoin, 4}, {LeftJoin, 7}, {RightJoin, 6}, {FullJoin, 9}, {SemiJoin, 2}, {AntiJoin, 3},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			session := boundedJoinSession(t, 256<<10)
			query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: tc.typ, LeftOn: []string{"key"}, RightOn: []string{"key"}})
			physical, err := query.ExplainPhysical(context.Background())
			if err != nil {
				session.Close()
				t.Fatal(err)
			}
			if !strings.Contains(physical, "BOUNDED Join strategy=broadcast-hash") {
				session.Close()
				t.Fatalf("physical plan does not expose proven-small bounded broadcast:\n%s", physical)
			}
			if (tc.typ == RightJoin || tc.typ == FullJoin) && !strings.Contains(physical, "PIPELINE id=p00 workers=1") {
				session.Close()
				t.Fatalf("right/full broadcast must use one query-wide matched bitmap:\n%s", physical)
			}
			frame, err := query.Collect(context.Background())
			if err != nil {
				session.Close()
				t.Fatal(err)
			}
			if got := frame.Height(); got != tc.want {
				_ = frame.Close()
				session.Close()
				t.Fatalf("%s rows=%d want=%d", tc.typ, got, tc.want)
			}
			if tc.typ == RightJoin || tc.typ == FullJoin {
				field, _ := frame.Schema().Field("key")
				if !field.Nullable {
					t.Fatal("right/full join must make left payload nullable")
				}
			}
			if err := frame.Close(); err != nil {
				t.Fatal(err)
			}
			session.Close()
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("join leaked %d bytes", used)
			}
		})
	}
}

func TestBoundedJoinAllNullKeysNeverMatch(t *testing.T) {
	left := nullableJoinSource(t, "key", []int32{0, 0, 0}, nil, []int64{1, 2, 3})
	right := nullableJoinSource(t, "key", []int32{0, 0}, nil, []int64{4, 5})
	for _, tc := range []struct {
		typ  JoinType
		want int64
	}{{InnerJoin, 0}, {LeftJoin, 3}, {RightJoin, 2}, {FullJoin, 5}, {SemiJoin, 0}, {AntiJoin, 3}} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			session := boundedJoinSession(t, 128<<10)
			frame, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: tc.typ, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
			if err != nil {
				session.Close()
				t.Fatal(err)
			}
			if frame.Height() != tc.want {
				_ = frame.Close()
				session.Close()
				t.Fatalf("rows=%d want=%d", frame.Height(), tc.want)
			}
			_ = frame.Close()
			session.Close()
		})
	}
}

func TestBoundedJoinRequiresSpillQuota(t *testing.T) {
	left := nullableJoinSource(t, "key", []int32{1}, []int{0}, []int64{1})
	right := nullableJoinSource(t, "key", []int32{1}, []int{0}, []int64{1})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	_, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if !errors.Is(err, engine.ErrBoundedSpillRequired) {
		t.Fatalf("err=%v want ErrBoundedSpillRequired", err)
	}
}

func TestBoundedSpillJoinAmplifiedOutputDrainsInChunks(t *testing.T) {
	left := nullableJoinSource(t, "key", []int32{7, 7}, []int{0, 1}, []int64{1, 2})
	rightBatches := make([]*memory.Batch, 5)
	for batchIndex := range rightBatches {
		batch := memory.NewBatch()
		keys := make([]int32, 2000)
		values := make([]int64, 2000)
		for row := range keys {
			keys[row], values[row] = 7, int64(batchIndex*2000+row)
		}
		batch.Int32s["key"] = &memory.Int32Vector{Values: keys}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(2000)
		rightBatches[batchIndex] = batch
	}
	right, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64},
	), rightBatches...)
	if err != nil {
		t.Fatal(err)
	}
	// The 20,000-row result is materially larger than the 320 KiB execution
	// budget, so only a draining batch sink can complete this skewed join.
	session := boundedJoinSession(t, 320<<10)
	defer session.Close()
	prepared, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
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
	if stats.Bytes <= queryBudget.Limit() {
		t.Fatalf("output bytes=%d must exceed budget=%d", stats.Bytes, queryBudget.Limit())
	}
	if stats.Rows != 20000 || stats.Batches < 3 {
		t.Fatalf("amplified drain=%+v", stats)
	}
	if snapshot := queryBudget.Snapshot(); snapshot.PeakBytes > queryBudget.Limit() || snapshot.SpilledBytes == 0 {
		t.Fatalf("bounded join snapshot=%+v", snapshot)
	}
	if err := queryBudget.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("amplified join leaked %d bytes", used)
	}
}

func TestBoundedSpillJoinInputOverTenTimesBudgetDrains(t *testing.T) {
	// Each right row contains a 4-byte key and 8-byte payload. The 786,432-row
	// input alone is 9 MiB: twelve times the 768 KiB query hard limit.
	// Radix-grouped morsels also make this a deterministic manifest/cleanup
	// regression instead of relying on a lucky source batch distribution.
	rightKeys := radixGroupedJoinKeys(49152)
	left := multiBatchJoinSource(t, rightKeys[:16], 16)
	right := multiBatchJoinSource(t, rightKeys, 4096)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 768 << 10
	opts.QueryMemorySoftLimitBytes = 576 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	if inputBytes := int64(len(rightKeys)) * 12; inputBytes <= 10*opts.MemoryLimitBytes {
		t.Fatalf("input bytes=%d must exceed ten times budget=%d", inputBytes, opts.MemoryLimitBytes)
	}
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), budget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := source.DrainBatchStream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 16 {
		t.Fatalf("rows=%d want=16", stats.Rows)
	}
	snapshot := budget.Snapshot()
	if snapshot.PeakBytes > budget.Limit() || snapshot.SpilledBytes == 0 {
		t.Fatalf("bounded join snapshot=%+v", snapshot)
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedSpillJoinExplainAnalyzeReportsResourceAndSkew(t *testing.T) {
	left := nullableJoinSource(t, "key", []int32{1, 2, 3}, []int{0, 1, 2}, []int64{10, 20, 30})
	const buildRows = 4096
	keys, values, valid := make([]int32, buildRows), make([]int64, buildRows), make([]int, buildRows)
	for row := range keys {
		keys[row], values[row], valid[row] = int32(row), int64(row), row
	}
	right := nullableJoinSource(t, "key", keys, valid, values)
	session := boundedJoinSession(t, 256<<10)
	defer session.Close()
	query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: LeftJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}})
	optimized, err := optimizer.Optimize(query.plan)
	if err != nil {
		t.Fatal(err)
	}
	report, err := query.ExplainAnalyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"strategy=partitioned-hash-spill", "build_rows=", "build_bytes=", "probe_rows=", "probe_bytes=", "output_rows=", "spill_bytes=", "peak_memory_bytes=", "max_multiplicity="} {
		if !strings.Contains(report, field) {
			t.Fatalf("ExplainAnalyze missing %q:\n%s", field, report)
		}
	}
	feedback, ok := session.Engine().RuntimeFeedback(planFeedbackKey(optimized.Plan))
	if !ok || feedback.JoinBuildBytes <= 0 || feedback.JoinProbeBytes <= 0 || feedback.JoinSpillBytes <= 0 || feedback.JoinMaxMultiplicity < 1 {
		t.Fatalf("join runtime feedback=%+v ok=%v", feedback, ok)
	}
}

func TestBoundedSpillJoinDictionaryDomainsAndNestedPayload(t *testing.T) {
	t.Run("dictionary-domains", func(t *testing.T) {
		leftDictionary := source.Dictionary{Domain: "left-colors", Offsets: []uint32{0, 3, 7}, Data: []byte("redblue")}
		rightDictionary := source.Dictionary{Domain: "right-colors", Offsets: []uint32{0, 4, 7, 12}, Data: []byte("blueredgreen")}
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
		session := boundedJoinSession(t, 256<<10)
		defer session.Close()
		frame, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer frame.Close()
		if frame.Height() != 3 {
			t.Fatalf("dictionary rows=%d want=3", frame.Height())
		}
	})

	t.Run("list-and-struct", func(t *testing.T) {
		leftBatch := memory.NewBatch()
		leftBatch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
		leftBatch.Reset(3)
		left, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}), leftBatch)

		profileField := memory.StructField("profile", true, memory.Field{Name: "score", Type: memory.TypeInt64})
		rightBatch := memory.NewBatch()
		rightBatch.Int32s["key"] = &memory.Int32Vector{Values: []int32{2, 3}}
		rightBatch.StringLists["tags"] = &memory.StringListVector{
			Offsets: []uint32{0, 2, 2}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 4}, Data: []byte("x中"), Encoding: memory.EncodingView}, Encoding: memory.EncodingView,
		}
		children := memory.NewBatch()
		children.Int64s["score"] = &memory.Int64Vector{Values: []int64{20, 30}}
		children.Reset(2)
		rightBatch.Structs["profile"] = &memory.StructVector{Rows: 2, Fields: profileField.Children, Values: children, Validity: memory.Bitmap{0b11}, Encoding: memory.EncodingView}
		rightBatch.Reset(2)
		right, err := inmemory.New(memory.NewSchema(
			memory.Field{Name: "key", Type: memory.TypeInt32}, memory.ListStringField("tags", false, false), profileField,
		), rightBatch)
		if err != nil {
			t.Fatal(err)
		}
		session := boundedJoinSession(t, 256<<10)
		defer session.Close()
		frame, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: LeftJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer frame.Close()
		if frame.Height() != 3 {
			t.Fatalf("nested rows=%d", frame.Height())
		}
		for _, batch := range frame.batches() {
			if err := batch.StringLists["tags"].Validate(batch.Rows); err != nil {
				t.Fatal(err)
			}
			field, _ := frame.Schema().Field("profile")
			if err := batch.Structs["profile"].Validate(field, batch.Rows); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestBoundedSpillJoinFixedCompositeKeyAndNullComponent(t *testing.T) {
	const buildRows = 4096
	rightBatches := make([]*memory.Batch, 0, buildRows/512)
	for offset := 0; offset < buildRows; offset += 512 {
		batch := memory.NewBatch()
		a, b, values := make([]int32, 512), make([]uint32, 512), make([]int64, 512)
		for row := range a {
			value := offset + row
			a[row], b[row], values[row] = int32(value), uint32(value*3), int64(value)
		}
		batch.Int32s["a"] = &memory.Int32Vector{Values: a}
		batch.Uint32s["b"] = &memory.Uint32Vector{Values: b}
		batch.Int64s["value"] = &memory.Int64Vector{Values: values}
		batch.Reset(512)
		rightBatches = append(rightBatches, batch)
	}
	right, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt32}, memory.Field{Name: "b", Type: memory.TypeUint32}, memory.Field{Name: "value", Type: memory.TypeInt64},
	), rightBatches...)
	if err != nil {
		t.Fatal(err)
	}
	validity := memory.EnsureBitmap(nil, 4)
	for _, row := range []int{0, 1, 3} {
		memory.Set(validity, row)
	}
	leftBatch := memory.NewBatch()
	leftBatch.Int32s["a"] = &memory.Int32Vector{Values: []int32{10, 11, 20, 12}}
	leftBatch.Uint32s["b"] = &memory.Uint32Vector{Values: []uint32{30, 33, 60, 36}, Validity: validity}
	leftBatch.Reset(4)
	left, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "a", Type: memory.TypeInt32}, memory.Field{Name: "b", Type: memory.TypeUint32, Nullable: true},
	), leftBatch)
	if err != nil {
		t.Fatal(err)
	}
	session := boundedJoinSession(t, 384<<10)
	defer session.Close()
	query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{
		Type: InnerJoin, LeftOn: []string{"a", "b"}, RightOn: []string{"a", "b"},
	})
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SPILLABLE Join strategy=partitioned-hash-spill") {
		t.Fatalf("composite join did not spill:\n%s", plan)
	}
	frame, err := query.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 3 {
		t.Fatalf("rows=%d want=3", frame.Height())
	}
}

func TestSpillJoinKernelModesAreSemanticallyIdentical(t *testing.T) {
	keys := make([]int32, 1024)
	for row := range keys {
		keys[row] = int32(row)
	}
	left := multiBatchJoinSource(t, keys[:8], 8)
	right := multiBatchJoinSource(t, keys, 256)
	for _, mode := range []struct {
		name string
		mode kernel.Mode
	}{{"scalar", kernel.ModeScalar}, {"auto", kernel.ModeAuto}, {"forced-simd", kernel.ModeSIMD}} {
		t.Run(mode.name, func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode.mode
			opts.MemoryLimitBytes = 160 << 10
			opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
			opts.ExecutionPolicy = engine.ExecutionBounded
			session := NewSession(opts)
			frame, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
			if err != nil {
				session.Close()
				t.Fatal(err)
			}
			if frame.Height() != 8 {
				_ = frame.Close()
				session.Close()
				t.Fatalf("rows=%d want=8", frame.Height())
			}
			_ = frame.Close()
			session.Close()
			if used := session.Engine().Memory().Used(); used != 0 {
				t.Fatalf("mode %s leaked %d bytes", mode.name, used)
			}
		})
	}
}

func TestSortedInputsSelectIncrementalMergeJoin(t *testing.T) {
	makeSource := func(batches ...[]int32) *inmemory.Source {
		input := make([]*memory.Batch, len(batches))
		for index, keys := range batches {
			batch := memory.NewBatch()
			batch.Int32s["key"] = &memory.Int32Vector{Values: append([]int32(nil), keys...)}
			values := make([]int64, len(keys))
			for row, key := range keys {
				values[row] = int64(key)
			}
			batch.Int64s["value"] = &memory.Int64Vector{Values: values}
			batch.Reset(len(keys))
			input[index] = batch
		}
		src, err := inmemory.New(memory.NewSchema(
			memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "value", Type: memory.TypeInt64},
		), input...)
		if err != nil {
			t.Fatal(err)
		}
		src.WithOrdering(source.OrderingColumn{Name: "key"})
		return src
	}
	left := makeSource([]int32{1, 2, 2}, []int32{4})
	right := makeSource([]int32{2, 2}, []int32{3, 4})
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	for _, tc := range []struct {
		typ  JoinType
		want int64
	}{{InnerJoin, 5}, {LeftJoin, 6}, {RightJoin, 6}, {FullJoin, 7}, {SemiJoin, 3}, {AntiJoin, 1}} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: tc.typ, LeftOn: []string{"key"}, RightOn: []string{"key"}})
			plan, err := query.ExplainPhysical(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plan, "strategy=merge") {
				t.Fatalf("merge strategy missing:\n%s", plan)
			}
			frame, err := query.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if frame.Height() != tc.want {
				_ = frame.Close()
				t.Fatalf("%s rows=%d want=%d", tc.typ, frame.Height(), tc.want)
			}
			_ = frame.Close()
		})
	}
}

func TestMergeJoinRetainsNestedGroupsUntilMaterialized(t *testing.T) {
	leftBatch := memory.NewBatch()
	leftBatch.Int32s["key"] = &memory.Int32Vector{Values: []int32{1, 2}}
	leftBatch.Reset(2)
	left, _ := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}), leftBatch)
	left.WithOrdering(source.OrderingColumn{Name: "key"})

	profileField := memory.StructField("profile", true, memory.Field{Name: "score", Type: memory.TypeInt64})
	rightBatch := memory.NewBatch()
	rightBatch.Int32s["key"] = &memory.Int32Vector{Values: []int32{2, 3}}
	rightBatch.StringLists["tags"] = &memory.StringListVector{
		Offsets: []uint32{0, 2, 3}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 4, 5}, Data: []byte("x中y"), Encoding: memory.EncodingView}, Encoding: memory.EncodingView,
	}
	children := memory.NewBatch()
	children.Int64s["score"] = &memory.Int64Vector{Values: []int64{20, 30}}
	children.Reset(2)
	rightBatch.Structs["profile"] = &memory.StructVector{Rows: 2, Fields: profileField.Children, Values: children, Encoding: memory.EncodingView}
	rightBatch.Reset(2)
	right, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt32}, memory.ListStringField("tags", false, false), profileField,
	), rightBatch)
	if err != nil {
		t.Fatal(err)
	}
	right.WithOrdering(source.OrderingColumn{Name: "key"})

	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: LeftJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if frame.Height() != 2 {
		t.Fatalf("rows=%d", frame.Height())
	}
	for _, batch := range frame.batches() {
		if err := batch.StringLists["tags"].Validate(batch.Rows); err != nil {
			t.Fatal(err)
		}
		if err := batch.Structs["profile"].Validate(profileField, batch.Rows); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMergeJoinOversizedDuplicateGroupHonorsQueryBudget(t *testing.T) {
	keys := make([]int32, 4096)
	for row := range keys {
		keys[row] = 7
	}
	left := multiBatchJoinSource(t, keys, len(keys))
	right := multiBatchJoinSource(t, keys, len(keys))
	left.WithOrdering(source.OrderingColumn{Name: "key"})
	right.WithOrdering(source.OrderingColumn{Name: "key"})
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 32 << 10
	session := NewSession(opts)
	query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}})
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		session.Close()
		t.Fatal(err)
	}
	if !strings.Contains(plan, "BLOCKING Join strategy=merge") {
		session.Close()
		t.Fatalf("merge resource class missing:\n%s", plan)
	}
	_, err = query.Collect(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		session.Close()
		t.Fatalf("err=%v want ErrBudgetExceeded", err)
	}
	session.Close()
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("merge budget failure leaked %d bytes", used)
	}
}

func TestSpillJoinRecursivelyRepartitionsLowBitCollisions(t *testing.T) {
	keys := lowRadixCollisionKeys(4096)
	left := multiBatchJoinSource(t, keys[:16], 8)
	right := multiBatchJoinSource(t, keys, 512)
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 192 << 10
	opts.QueryMemorySoftLimitBytes = 96 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	report, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).ExplainAnalyze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"strategy=partitioned-hash-spill", "max_radix_depth=2", "output_rows=16"} {
		if !strings.Contains(report, field) {
			t.Fatalf("recursive join missing %q:\n%s", field, report)
		}
	}
}

func TestSpillJoinPreparedRepeatAndEarlyCancellationCleanup(t *testing.T) {
	keys := lowRadixCollisionKeys(4096)
	left := multiBatchJoinSource(t, keys[:128], 32)
	right := multiBatchJoinSource(t, keys, 512)
	spillRoot := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 192 << 10
	opts.QueryMemorySoftLimitBytes = 96 << 10
	opts.Spill = engine.SpillOptions{Directory: spillRoot, MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()

	for execution := 0; execution < 2; execution++ {
		budget, err := session.Engine().BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		ctx := memory.WithQueryBudget(context.Background(), budget)
		stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
		if err != nil {
			t.Fatal(err)
		}
		stats, err := source.DrainBatchStream(ctx, stream)
		if err != nil || stats.Rows != 128 {
			t.Fatalf("execution=%d stats=%+v err=%v", execution, stats, err)
		}
		if snapshot := budget.Snapshot(); snapshot.UsedBytes != 0 || snapshot.SpilledBytes == 0 {
			t.Fatalf("execution=%d snapshot=%+v", execution, snapshot)
		}
		store, err := budget.SpillManager()
		if err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(store.(*memory.QuerySpillStore).Directory())
		if err != nil || len(entries) != 0 {
			t.Fatalf("execution=%d remaining spill files=%v err=%v", execution, entries, err)
		}
		if err := budget.Close(); err != nil {
			t.Fatal(err)
		}
	}

	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	base, cancel := context.WithCancel(context.Background())
	ctx := memory.WithQueryBudget(base, budget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	if ok, err := stream.Next(ctx, batch); err != nil || !ok {
		t.Fatalf("first batch ok=%v err=%v", ok, err)
	}
	cancel()
	if _, err := stream.Next(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot := budget.Snapshot(); snapshot.UsedBytes != 0 {
		t.Fatalf("cancel leaked query bytes: %+v", snapshot)
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(spillRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "golume-query-") {
			t.Fatalf("orphan query spill directory %s", filepath.Join(spillRoot, entry.Name()))
		}
	}
}

func TestSpillJoinCorruptOutputRunFailsAndCleansUp(t *testing.T) {
	keys := lowRadixCollisionKeys(4096)
	left := multiBatchJoinSource(t, keys[:2048], 256)
	right := multiBatchJoinSource(t, keys, 512)
	spillRoot := t.TempDir()
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 192 << 10
	opts.QueryMemorySoftLimitBytes = 96 << 10
	opts.Spill = engine.SpillOptions{Directory: spillRoot, MaxBytes: 128 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), budget)
	stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	if ok, err := stream.Next(ctx, batch); err != nil || !ok {
		t.Fatalf("first batch ok=%v err=%v", ok, err)
	}
	store, err := budget.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	dir := store.(*memory.QuerySpillStore).Directory()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".gls" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > 0 {
			data[len(data)-1] ^= 0xff
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			corrupted++
		}
	}
	if corrupted == 0 {
		t.Fatal("expected at least one pending spill run")
	}
	if _, err := stream.Next(ctx, batch); !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("corrupt run err=%v", err)
	}
	_ = stream.Close()
	if snapshot := budget.Snapshot(); snapshot.UsedBytes != 0 {
		t.Fatalf("corrupt run leaked query bytes: %+v", snapshot)
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSpillJoinResourceFailuresReleaseMemory(t *testing.T) {
	keys := lowRadixCollisionKeys(4096)
	left := multiBatchJoinSource(t, keys[:16], 8)

	t.Run("disk-quota", func(t *testing.T) {
		right := multiBatchJoinSource(t, keys, 512)
		opts := engine.DefaultOptions()
		opts.MemoryLimitBytes = 128 << 10
		opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 512}
		opts.ExecutionPolicy = engine.ExecutionBounded
		session := NewSession(opts)
		_, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
		var resource *memory.SpillResourceError
		if !errors.As(err, &resource) {
			session.Close()
			t.Fatalf("err=%v want SpillResourceError", err)
		}
		session.Close()
		if used := session.Engine().Memory().Used(); used != 0 {
			t.Fatalf("disk quota failure leaked %d bytes", used)
		}
	})

	t.Run("memory-budget", func(t *testing.T) {
		// One wide input morsel cannot satisfy the reserve-before-gather
		// contract at this deliberately tiny hard limit.
		right := multiBatchJoinSource(t, keys, len(keys))
		opts := engine.DefaultOptions()
		opts.MemoryLimitBytes = 32 << 10
		opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
		opts.ExecutionPolicy = engine.ExecutionBounded
		session := NewSession(opts)
		_, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Collect(context.Background())
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			session.Close()
			t.Fatalf("err=%v want ErrBudgetExceeded", err)
		}
		session.Close()
		if used := session.Engine().Memory().Used(); used != 0 {
			t.Fatalf("memory failure leaked %d bytes", used)
		}
	})
}

func BenchmarkPhase34SpillableJoinDrain(b *testing.B) {
	const rows = 1 << 15
	leftKeys, rightKeys := make([]int32, rows), make([]int32, rows)
	leftValues, rightValues := make([]int64, rows), make([]int64, rows)
	for row := 0; row < rows; row++ {
		leftKeys[row], rightKeys[row] = int32(row&8191), int32(row&8191)
		leftValues[row], rightValues[row] = int64(row), int64(row*2)
	}
	valid := make([]int, rows)
	for row := range valid {
		valid[row] = row
	}
	left := nullableJoinSource(b, "key", leftKeys, valid, leftValues)
	right := nullableJoinSource(b, "key", rightKeys, valid, rightValues)
	for _, mode := range []struct {
		name string
		mode kernel.Mode
	}{{"scalar", kernel.ModeScalar}, {"auto", kernel.ModeAuto}, {"forced-simd", kernel.ModeSIMD}} {
		b.Run(mode.name, func(b *testing.B) {
			opts := engine.DefaultOptions()
			opts.KernelMode = mode.mode
			opts.MemoryLimitBytes = 2 << 20
			opts.QueryMemorySoftLimitBytes = 1536 << 10
			opts.Spill = engine.SpillOptions{Directory: b.TempDir(), MaxBytes: 128 << 20}
			opts.ExecutionPolicy = engine.ExecutionBounded
			session := NewSession(opts)
			defer session.Close()
			prepared, err := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: []string{"key"}, RightOn: []string{"key"}}).Prepare(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			defer prepared.Close()
			var totalSpill, maxPeak int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				budget, err := session.Engine().BeginQueryMemory()
				if err != nil {
					b.Fatal(err)
				}
				ctx := memory.WithQueryBudget(context.Background(), budget)
				stream, err := source.OpenBatchStream(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels})
				if err != nil {
					b.Fatal(err)
				}
				stats, err := source.DrainBatchStream(ctx, stream)
				if err != nil || stats.Rows != rows*4 {
					b.Fatalf("stats=%+v err=%v", stats, err)
				}
				snapshot := budget.Snapshot()
				totalSpill += snapshot.SpilledBytes
				if snapshot.PeakBytes > maxPeak {
					maxPeak = snapshot.PeakBytes
				}
				if err := budget.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(rows*2*b.N)/b.Elapsed().Seconds(), "input-rows/s")
			b.ReportMetric(float64(totalSpill)/float64(b.N), "spill-B/op")
			b.ReportMetric(float64(maxPeak), "peak-B")
		})
	}
}
