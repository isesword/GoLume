package lazy

import (
	"context"
	"errors"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

func externalSortSemanticSource(t testing.TB) *inmemory.Source {
	t.Helper()
	validA := memory.EnsureBitmap(nil, 4)
	validB := memory.EnsureBitmap(nil, 4)
	for _, row := range []int{0, 1, 2} {
		memory.Set(validA, row)
		memory.Set(validB, row)
	}
	makeBatch := func(ids []int64, keys, ties []float64, validity memory.Bitmap) *memory.Batch {
		batch := memory.NewBatch()
		batch.Int64s["id"] = &memory.Int64Vector{Values: append([]int64(nil), ids...)}
		batch.Float64s["key"] = &memory.Float64Vector{Values: append([]float64(nil), keys...), Validity: validity}
		batch.Float64s["tie"] = &memory.Float64Vector{Values: append([]float64(nil), ties...)}
		batch.Reset(len(ids))
		return batch
	}
	first := makeBatch(
		[]int64{0, 1, 2, 3},
		[]float64{math.NaN(), 2, 2, 0},
		[]float64{0, 1, 1, 9}, validA,
	)
	second := makeBatch(
		[]int64{4, 5, 6, 7},
		[]float64{1, 2, math.NaN(), 0},
		[]float64{2, 1, 0, 8}, validB,
	)
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "key", Type: memory.TypeFloat64, Nullable: true},
		memory.Field{Name: "tie", Type: memory.TypeFloat64},
	), first, second)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestBoundedExternalSortStableMultiKeyNullAndNaN(t *testing.T) {
	session := boundedJoinSession(t, 128<<10)
	query := session.FromSource(externalSortSemanticSource(t)).SortBy(
		SortExpr{Expr: Col("key"), NullsLast: true},
		SortExpr{Expr: Col("tie"), Descending: true},
	)
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		session.Close()
		t.Fatal(err)
	}
	if !strings.Contains(plan, "STREAM SPILLABLE Sort strategy=external-stable") {
		session.Close()
		t.Fatalf("external sort plan missing:\n%s", plan)
	}
	frame, err := query.Select("id").Collect(context.Background())
	if err != nil {
		session.Close()
		t.Fatal(err)
	}
	if got, want := frameInt64Column(t, frame, "id"), []int64{4, 1, 2, 5, 0, 6, 3, 7}; !equalI64(got, want) {
		_ = frame.Close()
		session.Close()
		t.Fatalf("external stable order=%v want=%v", got, want)
	}
	if err := frame.Close(); err != nil {
		t.Fatal(err)
	}
	session.Close()
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("external sort leaked %d bytes", used)
	}
}

func TestBoundedSortRequiresSpillQuota(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	_, err := session.FromSource(externalSortSemanticSource(t)).Sort(Asc("key")).Prepare(context.Background())
	if !errors.Is(err, engine.ErrBoundedSpillRequired) {
		t.Fatalf("err=%v want ErrBoundedSpillRequired", err)
	}
}

func TestAutoSortKeepsSmallInputResident(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 1 << 20
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 32 << 20}
	session := NewSession(opts)
	defer session.Close()
	plan, err := session.FromSource(externalSortSemanticSource(t)).Sort(Asc("key")).ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "STREAM BLOCKING Sort strategy=resident-stable") {
		t.Fatalf("small Auto sort should avoid disk spill:\n%s", plan)
	}
}

func TestBoundedExternalSortInputOverTenTimesBudgetDrains(t *testing.T) {
	const rows = 196608 // 3 MiB of fixed payload, twelve times 256 KiB.
	batches := make([]*memory.Batch, 0, rows/4096)
	for offset := 0; offset < rows; offset += 4096 {
		batch := memory.NewBatch()
		keys, ids := make([]int64, 4096), make([]int64, 4096)
		for row := range keys {
			id := offset + row
			keys[row], ids[row] = int64((rows-id)%8192), int64(id)
		}
		batch.Int64s["key"] = &memory.Int64Vector{Values: keys}
		batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
		batch.Reset(len(keys))
		batches = append(batches, batch)
	}
	src, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64},
	), batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 256 << 10
	opts.QueryMemorySoftLimitBytes = 192 << 10
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 256 << 20}
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	prepared, err := session.FromSource(src).Sort(Asc("key"), Asc("id")).Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	budget, err := session.Engine().BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), budget)
	stream, err := source.OpenBatchStreamWithPartitions(ctx, prepared.src, source.ReadOptions{Kernels: prepared.kernels}, prepared.parts)
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	defer batch.ReleaseOwned()
	var outputRows int64
	batchesOut := 0
	first := true
	var lastKey, lastID int64
	for {
		ok, nextErr := stream.Next(ctx, batch)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if !ok {
			break
		}
		keys, ids := batch.Int64s["key"], batch.Int64s["id"]
		for row := 0; row < batch.Rows; row++ {
			if !first && (keys.Values[row] < lastKey || keys.Values[row] == lastKey && ids.Values[row] < lastID) {
				t.Fatalf("external sort order regressed at output row %d: (%d,%d) after (%d,%d)", outputRows, keys.Values[row], ids.Values[row], lastKey, lastID)
			}
			first = false
			lastKey, lastID = keys.Values[row], ids.Values[row]
			outputRows++
		}
		batchesOut++
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if outputRows != rows || batchesOut < 2 {
		t.Fatalf("external sort rows=%d batches=%d", outputRows, batchesOut)
	}
	snapshot := budget.Snapshot()
	if snapshot.PeakBytes > budget.Limit() || snapshot.SpilledBytes == 0 {
		t.Fatalf("external sort snapshot=%+v", snapshot)
	}
	if err := budget.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedExternalWindowRestoresInputOrder(t *testing.T) {
	schema := memory.NewSchema(
		memory.Field{Name: "id", Type: memory.TypeInt64},
		memory.Field{Name: "grp", Type: memory.TypeInt32},
		memory.Field{Name: "score", Type: memory.TypeInt64},
		memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true},
	)
	first := memory.NewBatch()
	first.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	first.Int32s["grp"] = &memory.Int32Vector{Values: []int32{1, 2, 1}}
	first.Int64s["score"] = &memory.Int64Vector{Values: []int64{20, 5, 10}}
	first.Int64s["value"] = &memory.Int64Vector{Values: []int64{1, 0, 3}, Validity: windowValidity(3, 0, 2)}
	first.Reset(3)
	second := memory.NewBatch()
	second.Int64s["id"] = &memory.Int64Vector{Values: []int64{3, 4, 5}}
	second.Int32s["grp"] = &memory.Int32Vector{Values: []int32{2, 1, 2}}
	second.Int64s["score"] = &memory.Int64Vector{Values: []int64{5, 10, 7}}
	second.Int64s["value"] = &memory.Int64Vector{Values: []int64{4, 5, 6}}
	second.Reset(3)
	src, err := inmemory.New(schema, first, second)
	if err != nil {
		t.Fatal(err)
	}
	session := boundedJoinSession(t, 256<<10)
	query := session.FromSource(src).WithWindows(
		NewWindow(PartitionBy("grp"), OrderBy(Asc("score"))),
		RowNumber("rn"), Rank("rank"), DenseRank("dense"),
		RollingCountRows(RowsBetween(Preceding(1), CurrentRow), "rows"),
		RollingCount("value", RowsBetween(Preceding(1), CurrentRow), "count"),
		RollingSum("value", RowsBetween(Preceding(1), CurrentRow), "sum"),
		RollingMin("value", RowsBetween(Preceding(1), CurrentRow), "min"),
		RollingMax("value", RowsBetween(Preceding(1), CurrentRow), "max"),
		RollingAvg("value", RowsBetween(Preceding(1), CurrentRow), "avg"),
	)
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		session.Close()
		t.Fatal(err)
	}
	if !strings.Contains(plan, "STREAM SPILLABLE Window strategy=external-sort-restore") {
		session.Close()
		t.Fatalf("bounded external window plan missing:\n%s", plan)
	}
	frame, err := query.Collect(context.Background())
	if err != nil {
		session.Close()
		t.Fatal(err)
	}
	if got := frameInt64Column(t, frame, "id"); !equalI64(got, []int64{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("window reordered input rows: %v", got)
	}
	if got := frameInt64Column(t, frame, "rn"); !equalI64(got, []int64{3, 1, 1, 2, 2, 3}) {
		t.Fatalf("row number=%v", got)
	}
	if got := frameInt64Column(t, frame, "sum"); !equalI64(got, []int64{6, 0, 3, 4, 8, 10}) {
		t.Fatalf("rolling sum=%v", got)
	}
	if values, valid := frameInt64WithValidity(t, frame, "min"); !equalI64(values, []int64{1, 0, 3, 4, 3, 4}) || !equalBools(valid, []bool{true, false, true, true, true, true}) {
		t.Fatalf("rolling min=%v valid=%v", values, valid)
	}
	if values, valid := frameInt64WithValidity(t, frame, "max"); !equalI64(values, []int64{5, 0, 3, 4, 5, 6}) || !equalBools(valid, []bool{true, false, true, true, true, true}) {
		t.Fatalf("rolling max=%v valid=%v", values, valid)
	}
	if got := frameFloat64Column(t, frame, "avg"); len(got) != 6 || got[0] != 3 || got[2] != 3 || got[3] != 4 || got[4] != 4 || got[5] != 5 {
		t.Fatalf("rolling avg=%v", got)
	}
	if err := frame.Close(); err != nil {
		t.Fatal(err)
	}
	session.Close()
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("external window leaked %d bytes", used)
	}
}

type orderedWindowTestSource struct {
	source.Source
	ordering []source.OrderingColumn
}

func (s orderedWindowTestSource) Ordering() []source.OrderingColumn {
	return append([]source.OrderingColumn(nil), s.ordering...)
}

func (s *orderedWindowTestSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return source.OpenBatchStream(ctx, s.Source, opts)
}

func (s *orderedWindowTestSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return source.OpenBatchStreamWithPartitions(ctx, s.Source, opts, parts)
}

func TestBoundedOrderedWindowNeedsNoSpill(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int32s["grp"] = &memory.Int32Vector{Values: []int32{1, 1, 2, 2}}
	batch.Int64s["score"] = &memory.Int64Vector{Values: []int64{1, 2, 1, 2}}
	batch.Reset(4)
	base, err := inmemory.New(memory.NewSchema(
		memory.Field{Name: "grp", Type: memory.TypeInt32}, memory.Field{Name: "score", Type: memory.TypeInt64},
	), batch)
	if err != nil {
		t.Fatal(err)
	}
	ordered := &orderedWindowTestSource{Source: base, ordering: []source.OrderingColumn{{Name: "grp"}, {Name: "score", NullsLast: true}}}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	defer session.Close()
	query := session.FromSource(ordered).WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("score"))), RowNumber("rn"))
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "STREAM BOUNDED Window strategy=ordered-state") {
		t.Fatalf("ordered bounded window plan missing:\n%s", plan)
	}
	frame, err := query.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	if got := frameInt64Column(t, frame, "rn"); !equalI64(got, []int64{1, 2, 1, 2}) {
		t.Fatalf("row number=%v", got)
	}
}

func TestBoundedWindowFollowingRejectedDuringPrepare(t *testing.T) {
	session := boundedJoinSession(t, 64<<10)
	defer session.Close()
	query := session.FromSource(externalSortSemanticSource(t)).WithWindows(
		NewWindow(nil, OrderBy(Asc("id"))),
		RollingCountRows(RowsBetween(CurrentRow, Following(1)), "count"),
	)
	_, err := query.Prepare(context.Background())
	var unsupported *engine.UnsupportedBoundedPlanError
	if !errors.As(err, &unsupported) || unsupported.Operator != "Window" {
		t.Fatalf("err=%v want bounded Window capability error", err)
	}
}

func TestBoundedOrderedWindowStateBudgetFailsBeforeAllocation(t *testing.T) {
	batch := memory.NewBatch()
	batch.Int64s["score"] = &memory.Int64Vector{Values: []int64{1}}
	batch.Reset(1)
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "score", Type: memory.TypeInt64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	ordered := &orderedWindowTestSource{Source: base, ordering: []source.OrderingColumn{{Name: "score", NullsLast: true}}}
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 32 << 10
	opts.ExecutionPolicy = engine.ExecutionBounded
	session := NewSession(opts)
	query := session.FromSource(ordered).WithWindows(
		NewWindow(nil, OrderBy(Asc("score"))),
		RollingMin("score", RowsBetween(Preceding(1024), CurrentRow), "min"),
	)
	_, err = query.Collect(context.Background())
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		session.Close()
		t.Fatalf("err=%v want ErrBudgetExceeded", err)
	}
	session.Close()
	if used := session.Engine().Memory().Used(); used != 0 {
		t.Fatalf("window state budget failure leaked %d bytes", used)
	}
}
