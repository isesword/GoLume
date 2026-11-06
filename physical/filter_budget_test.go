package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

type filterRecordingManager struct {
	base           *memory.BudgetManager
	deny           bool
	requests, live []int64
}

func (m *filterRecordingManager) Used() int64  { return m.base.Used() }
func (m *filterRecordingManager) Limit() int64 { return m.base.Limit() }
func (m *filterRecordingManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.requests = append(m.requests, n)
	m.live = append(m.live, m.Used())
	if m.deny {
		return nil, memory.ErrBudgetExceeded
	}
	return m.base.Reserve(ctx, n)
}

func filterNumericBatch(values ...int64) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(values)
	b.Int64s["x"] = &memory.Int64Vector{Values: values}
	return b
}
func filterBase(t *testing.T, batches ...*memory.Batch) *inmemory.Source {
	t.Helper()
	s, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64, Nullable: true}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFilterCompactionReserveCapacityOverlapAndRollback(t *testing.T) {
	ctx := context.Background()
	m := &filterRecordingManager{base: memory.NewBudgetManager(0), deny: true}
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	b := filterNumericBatch(1, 2, 3)
	selection := memory.Bitmap{5}
	if _, err := w.compact(ctx, b, []int{0, 2}, selection, testReadOptions().Kernels); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.batch != nil || m.Used() != 0 {
		t.Fatal("denied output allocated")
	}
	m.deny = false
	first, err := w.compact(ctx, b, []int{0, 2}, selection, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	pointer := &first.Int64s["x"].Values[0]
	if m.Used() != 16 {
		t.Fatalf("charge=%d", m.Used())
	}
	smaller, err := w.compact(ctx, b, []int{2}, memory.Bitmap{4}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	if &smaller.Int64s["x"].Values[0] != pointer || m.Used() != 16 {
		t.Fatal("capacity not reused/charged")
	}
	bigger := filterNumericBatch(1, 2, 3, 4, 5)
	m.deny = true
	if _, err = w.compact(ctx, bigger, []int{0, 2, 4}, memory.Bitmap{21}, testReadOptions().Kernels); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if w.batch != first || m.Used() != 16 || m.requests[len(m.requests)-1] != 24 || m.live[len(m.live)-1] != 16 {
		t.Fatal("replacement overlap/rollback failed")
	}
	m.deny = false
	if _, err = w.compact(ctx, bigger, []int{0, 2, 4}, memory.Bitmap{21}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 24 {
		t.Fatal("replacement charge wrong")
	}
	w.release()
	w.release()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}

func TestFilterPredicateIndexAndOutputAdmissionFailures(t *testing.T) {
	base := filterBase(t, filterNumericBatch(1, 0, 1, 0))
	predicate := expr.GT(expr.Col("x"), expr.I64(0))
	program, _, err := expr.CompilePredicate(base.Schema(), predicate)
	if err != nil {
		t.Fatal(err)
	}
	static, err := program.WorkspaceBytes(4)
	if err != nil {
		t.Fatal(err)
	}
	indexBytes := 2 * int64(unsafe.Sizeof(int(0)))
	for stage, limit := range []int64{static - 1, static + indexBytes - 1, static + indexBytes + 16 - 1} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			filtered, err := NewFilterSource(base, predicate)
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(limit)
			opts := testReadOptions()
			opts.MemoryManager = m
			stream, err := filtered.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			ok, err := stream.Next(context.Background(), out)
			if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("stage %d limit=%d: ok=%v err=%v", stage, limit, ok, err)
			}
			if m.Used() != 0 || out.Rows != 0 {
				t.Fatal("terminal failure did not release buffers")
			}
			state := stream.(*filterBatchStream)
			if state.eval.MemoryBytes() != 0 || state.rows != nil || state.output.batch != nil {
				t.Fatal("failed allocation path retained workspace")
			}
			if _, again := stream.Next(context.Background(), out); again != err {
				t.Fatal("failure not terminal")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if m.Used() != 0 || out.Rows != 0 {
				t.Fatalf("leaked %d bytes or output rows %d", m.Used(), out.Rows)
			}
		})
	}
}

func TestFilterProjectionSubsetsNullsAndDestinations(t *testing.T) {
	ctx := context.Background()
	a := filterNumericBatch(1, 0, 3)
	a.Int64s["x"].Validity = memory.Bitmap{3}
	b := filterNumericBatch(4, 5)
	base := filterBase(t, a, b).WithOrdering(source.OrderingColumn{Name: "x"})
	filtered, err := NewFilterProjectSource(base, expr.GT(expr.Col("x"), expr.I64(0)), []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.(source.OrderingProvider).Ordering()) != 1 || source.ReplayabilityOf(filtered) != source.ReplayStable {
		t.Fatal("capabilities lost")
	}
	parts, _ := base.Partitions(ctx)
	sequence := []source.Partition{parts[1], parts[0], parts[1]}
	before := append([]source.Partition(nil), sequence...)
	m := memory.NewBudgetManager(1 << 20)
	opts := testReadOptions()
	opts.MemoryManager = m
	stream, err := filtered.(source.PreparedBatchStreamProvider).NewBatchStreamWithPartitions(ctx, opts, sequence)
	if err != nil {
		t.Fatal(err)
	}
	var values []int64
	var previous *memory.Batch
	for {
		out := memory.NewBatch()
		ok, err := stream.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && previous.Rows != 0 {
			t.Fatal("different destination retained previous borrow")
		}
		if !ok {
			break
		}
		values = append(values, out.Int64s["x"].Values...)
		previous = out
	}
	if !reflect.DeepEqual(values, []int64{4, 5, 1, 4, 5}) || !reflect.DeepEqual(sequence, before) {
		t.Fatalf("values=%v sequence=%v", values, sequence)
	}
	if m.Used() != 0 {
		t.Fatal("EOF leaked")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for _, subset := range [][]source.Partition{nil, {}} {
		stream, err := filtered.(source.PreparedBatchStreamProvider).NewBatchStreamWithPartitions(ctx, opts, subset)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := stream.Next(ctx, memory.NewBatch()); ok || err != nil {
			t.Fatal("empty subset fell back")
		}
		stream.Close()
	}
	zero, err := NewFilterProjectSource(base, expr.GT(expr.Col("x"), expr.I64(0)), []string{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err = zero.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(ctx, out); !ok || err != nil || out.Rows != 1 || len(out.Int64s) != 0 {
		t.Fatalf("zero-column result ok=%v err=%v rows=%d", ok, err, out.Rows)
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("early Close leaked")
	}
}

func TestFilterGenericCapacityAndLargeStrings(t *testing.T) {
	ctx := context.Background()
	b := filterNumericBatch(1, 0, 1, 0)
	text := strings.Repeat("a", 1<<18)
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(text)), uint32(len(text)), uint32(len(text)), uint32(len(text))}, Data: []byte(text), Validity: memory.Bitmap{15}}
	b.DictStrings["d"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 1, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte("ab"), Validity: memory.Bitmap{15}}
	b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 2, 2, 3, 3}, Validity: memory.Bitmap{15}, Values: &memory.StringViewVector{Offsets: []uint32{0, 1, 1, 2}, Data: []byte("xy"), Validity: memory.Bitmap{5}}}
	child := filterNumericBatch(10, 20, 30, 40)
	b.Structs["struct"] = &memory.StructVector{Rows: 4, Values: child, Validity: memory.Bitmap{11}, Fields: []memory.Field{{Name: "x", Type: memory.TypeInt64}}}
	m := memory.NewBudgetManager(1 << 20)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	out, err := w.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	bytes, _, err := compactLayout(b, nil, []int{0, 2}, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if m.Used() != filterOwnedCapacity(out) {
		t.Fatalf("actual output capacity %d != charge %d", filterOwnedCapacity(out), m.Used())
	}
	if m.Used() != bytes || cap(out.Strings["s"].Data) != len(text) || string(out.Strings["s"].ValueBytes(0)) != text {
		t.Fatal("string exact capacity failed")
	}
	if &out.DictStrings["d"].Data[0] != &b.DictStrings["d"].Data[0] {
		t.Fatal("dictionary payload copied")
	}
	if !reflect.DeepEqual(out.StringLists["list"].Offsets, []uint32{0, 2, 3}) || out.StringLists["list"].Values.Validity[0] != 5 {
		t.Fatal("list/null compaction failed")
	}
	if !reflect.DeepEqual(out.Structs["struct"].Values.Int64s["x"].Values, []int64{10, 0}) || out.Structs["struct"].Validity[0] != 1 {
		t.Fatal("struct compaction failed")
	}
	oldCharge := m.Used()
	if _, err = w.compact(ctx, b, []int{2}, memory.Bitmap{4}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if m.Used() != oldCharge {
		t.Fatal("smaller selection undercharged retained capacities")
	}
	clearCompactBorrows(w.batch)
	if w.batch.DictStrings["d"].Data != nil {
		t.Fatal("dictionary borrow retained across Next")
	}
	w.release()
	if m.Used() != 0 {
		t.Fatal("generic output leak")
	}
	low := compactWorkspace{manager: memory.NewBudgetManager(1 << 17), bounded: true}
	defer low.release()
	if _, err := low.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels); !errors.Is(err, memory.ErrBudgetExceeded) || low.batch != nil {
		t.Fatal("large payload allocated before rejection")
	}
}

func TestFilterOpaqueBoundedUnboundedAndBorrowing(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{10, 20, 30, 40}, nil)
	values := builder.NewInt64Array()
	builder.Release()
	defer values.Release()
	input := filterNumericBatch(1, 0, 1, 0)
	input.Opaque["raw"] = &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, ArrowType: "int64", Value: values, Owned: true}
	base, err := inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "int64"}), input)
	if err != nil {
		t.Fatal(err)
	}
	for _, bounded := range []bool{false, true} {
		for _, all := range []bool{false, true} {
			predicate := expr.GT(expr.Col("x"), expr.I64(0))
			if all {
				predicate = expr.GE(expr.Col("x"), expr.I64(0))
			}
			filtered, err := NewFilterSource(base, predicate)
			if err != nil {
				t.Fatal(err)
			}
			opts := testReadOptions()
			m := memory.NewBudgetManager(1 << 20)
			if bounded {
				opts.MemoryManager = m
			}
			stream, err := filtered.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			ok, err := stream.Next(context.Background(), out)
			if bounded && !all {
				var resource *memory.ResourceError
				if ok || !errors.As(err, &resource) {
					t.Fatalf("bounded partial ok=%v err=%v", ok, err)
				}
			} else {
				if !ok || err != nil {
					t.Fatal(err)
				}
				raw := out.Opaque["raw"]
				if raw.Owned {
					t.Fatal("borrowed opaque claims ownership")
				}
				want := []int64{10, 30}
				if all {
					want = []int64{10, 20, 30, 40}
				}
				if !reflect.DeepEqual(raw.Value.(*array.Int64).Int64Values(), want) {
					t.Fatal("opaque gather values")
				}
				out.ClearColumns()
			}
			stream.Close()
			stream.Close()
			if m.Used() != 0 || !input.Opaque["raw"].Owned || values.Value(3) != 40 {
				t.Fatal("opaque source ownership or charge changed")
			}
		}
	}
}

func TestFilterExternalBudgetCancellationAndWorkers(t *testing.T) {
	ctx := context.Background()
	base := filterBase(t, filterNumericBatch(1, 0, 1, 0))
	filtered, err := NewFilterSource(base, expr.GT(expr.Col("x"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	q, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	queryCtx := memory.WithQueryBudget(ctx, q)
	fallback := memory.NewBudgetManager(1)
	opts := testReadOptions()
	opts.MemoryManager = fallback
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := filtered.NewBatchStream(queryCtx, opts)
			if err != nil {
				t.Error(err)
				return
			}
			out := memory.NewBatch()
			if ok, err := stream.Next(queryCtx, out); !ok || err != nil {
				t.Error(err)
			}
			cancelled, cancel := context.WithCancel(queryCtx)
			cancel()
			if ok, err := stream.Next(cancelled, memory.NewBatch()); ok || !errors.Is(err, context.Canceled) {
				t.Errorf("cancel err=%v", err)
			}
			stream.Close()
			if out.Rows != 0 {
				t.Error("cancellation retained previous destination")
			}
		}()
	}
	wg.Wait()
	if q.Used() != 0 || fallback.Used() != 0 {
		t.Fatalf("external/query leak q=%d fallback=%d", q.Used(), fallback.Used())
	}
}

func TestFilterEmptyNullAndProjectedLargePayload(t *testing.T) {
	ctx := context.Background()
	batch := filterNumericBatch(1, 2, 3)
	batch.Int64s["x"].Validity = memory.Bitmap{0}
	base := filterBase(t, batch)
	filtered, err := NewFilterSource(base, expr.GT(expr.Col("x"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(1 << 20)
	opts := testReadOptions()
	opts.MemoryManager = m
	stream, err := filtered.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := stream.Next(ctx, memory.NewBatch()); ok || err != nil {
		t.Fatal("all NULL should produce EOF")
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("NULL filter leaked")
	}
	batch.Int64s["x"].Validity = nil
	large := strings.Repeat("z", 1<<20)
	batch.Strings["large"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(large)), uint32(len(large)), uint32(len(large))}, Data: []byte(large)}
	base, err = inmemory.New(memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "large", Type: memory.TypeString}), batch)
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{"x"}
	filtered, err = NewFilterProjectSource(base, expr.GT(expr.Col("x"), expr.I64(1)), columns)
	if err != nil {
		t.Fatal(err)
	}
	columns[0] = "large" // The immutable source must not retain the caller's slice.
	m = memory.NewBudgetManager(4096)
	opts.MemoryManager = m
	stream, err = filtered.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(ctx, out); !ok || err != nil {
		t.Fatal(err)
	}
	if len(out.Strings) != 0 || !reflect.DeepEqual(out.Int64s["x"].Values, []int64{2, 3}) {
		t.Fatal("projection copied hidden large payload or mutated columns")
	}
	if _, err := stream.Next(nil, out); err == nil {
		t.Fatal("nil context accepted")
	}
	if m.Used() != 0 || out.Rows != 0 {
		t.Fatal("invalid Next failed to clean up")
	}
	stream.Close()
}

func TestCompactAllNumericTypesAndRepresentationChange(t *testing.T) {
	ctx := context.Background()
	b := filterNumericBatch(1, 2, 3)
	b.Int8s["i8"] = &memory.Int8Vector{Values: []int8{1, 2, 3}, Validity: memory.Bitmap{5}}
	b.Int16s["i16"] = &memory.Int16Vector{Values: []int16{1, 2, 3}}
	b.Int32s["i32"] = &memory.Int32Vector{Values: []int32{1, 2, 3}}
	b.Uint8s["u8"] = &memory.Uint8Vector{Values: []uint8{1, 2, 3}}
	b.Uint16s["u16"] = &memory.Uint16Vector{Values: []uint16{1, 2, 3}}
	b.Uint32s["u32"] = &memory.Uint32Vector{Values: []uint32{1, 2, 3}}
	b.Uint64s["u64"] = &memory.Uint64Vector{Values: []uint64{1, 2, 3}}
	b.Float32s["f32"] = &memory.Float32Vector{Values: []float32{1, 2, 3}}
	b.Float64s["f64"] = &memory.Float64Vector{Values: []float64{1, 2, 3}}
	m := memory.NewBudgetManager(4096)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	out, err := w.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	if out.Int8s["i8"].Values[1] != 3 || out.Int8s["i8"].Validity[0] != 3 || out.Int16s["i16"].Values[1] != 3 || out.Int32s["i32"].Values[1] != 3 || out.Uint8s["u8"].Values[1] != 3 || out.Uint16s["u16"].Values[1] != 3 || out.Uint32s["u32"].Values[1] != 3 || out.Uint64s["u64"].Values[1] != 3 || out.Float32s["f32"].Values[1] != 3 || out.Float64s["f64"].Values[1] != 3 {
		t.Fatal("numeric compaction failed")
	}
	if m.Used() != memory.BatchBytes(out) {
		t.Fatal("numeric backing capacities differ from charge")
	}
	replacement := filterNumericBatch(4, 5, 6)
	replacement.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc")}
	out, err = w.compact(ctx, replacement, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Int8s) != 0 || string(out.Strings["s"].Data) != "ac" {
		t.Fatal("old schema bindings retained")
	}
	w.release()
	if m.Used() != 0 {
		t.Fatal("numeric charge leak")
	}
}

// Counts actual owned capacities independently from the compaction preflight.
// Dictionary payloads and opaque arrays are borrowed and excluded.
func filterOwnedCapacity(b *memory.Batch) int64 {
	if b == nil {
		return 0
	}
	var n int64
	for _, v := range b.Int8s {
		n += int64(cap(v.Values))*1 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int16s {
		n += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Int64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint8s {
		n += int64(cap(v.Values))*1 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint16s {
		n += int64(cap(v.Values))*2 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Uint64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float32s {
		n += int64(cap(v.Values))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Float64s {
		n += int64(cap(v.Values))*8 + int64(cap(v.Validity))*8
	}
	for _, v := range b.Strings {
		n += int64(cap(v.Offsets))*4 + int64(cap(v.Data)) + int64(cap(v.Validity))*8
	}
	for _, v := range b.DictStrings {
		n += int64(cap(v.Indices))*4 + int64(cap(v.Validity))*8
	}
	for _, v := range b.StringLists {
		n += int64(cap(v.Offsets))*4 + int64(cap(v.Validity))*8
		if v.Values != nil {
			n += int64(cap(v.Values.Offsets))*4 + int64(cap(v.Values.Data)) + int64(cap(v.Values.Validity))*8
		}
	}
	for _, v := range b.Structs {
		n += int64(cap(v.Validity))*8 + filterOwnedCapacity(v.Values)
	}
	for _, v := range b.Opaque {
		n += int64(cap(v.Validity)) * 8
	}
	return n
}

func TestCompactRetainsValidityAcrossAllValidAndGrowth(t *testing.T) {
	ctx := context.Background()
	m := memory.NewBudgetManager(1 << 20)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	b := filterNumericBatch(1, 2, 3)
	b.Int64s["x"].Validity = memory.Bitmap{7}
	if _, err := w.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	pointer := &w.batch.Int64s["x"].Validity[0]
	b.Int64s["x"].Validity = nil
	if _, err := w.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if &w.batch.Int64s["x"].Validity[0] != pointer || w.batch.Int64s["x"].Validity[0] != 3 || m.Used() != filterOwnedCapacity(w.batch) {
		t.Fatal("validity capacity lost or charged incorrectly")
	}
	b.Int64s["x"].Validity = memory.Bitmap{1}
	if _, err := w.compact(ctx, b, []int{0, 2}, memory.Bitmap{5}, testReadOptions().Kernels); err != nil {
		t.Fatal(err)
	}
	if &w.batch.Int64s["x"].Validity[0] != pointer || w.batch.Int64s["x"].Validity[0] != 1 {
		t.Fatal("nullable churn reallocated")
	}
	w.release()
	for _, opaque := range []bool{false, true} {
		m := memory.NewBudgetManager(1 << 20)
		w := compactWorkspace{manager: m, bounded: true}
		input := memory.NewBatch()
		input.Rows = 130
		builder := array.NewInt64Builder(arrowmem.DefaultAllocator)
		builder.AppendValues(make([]int64, 130), nil)
		arr := builder.NewArray()
		builder.Release()
		if opaque {
			input.Opaque["x"] = &memory.OpaqueVector{Rows: 130, Value: arr, Validity: memory.Bitmap{1, 0, 0}}
		} else {
			child := memory.NewBatch()
			child.Rows = 130
			input.Structs["x"] = &memory.StructVector{Rows: 130, Values: child, Validity: memory.Bitmap{1, 0, 0}}
		}
		if _, err := w.compact(ctx, input, []int{0}, memory.Bitmap{1, 0, 0}, testReadOptions().Kernels); err != nil {
			t.Fatal(err)
		}
		if m.Used() != 8 {
			t.Fatalf("first charge=%d", m.Used())
		}
		if opaque {
			input.Opaque["x"].Validity = nil
		} else {
			input.Structs["x"].Validity = nil
		}
		rows := make([]int, 129)
		for i := range rows {
			rows[i] = i
		}
		if _, err := w.compact(ctx, input, rows, memory.Bitmap{^uint64(0), ^uint64(0), 1}, testReadOptions().Kernels); err != nil {
			t.Fatal(err)
		}
		if m.Used() != filterOwnedCapacity(w.batch) {
			t.Fatalf("opaque=%v actual=%d charge=%d", opaque, filterOwnedCapacity(w.batch), m.Used())
		}
		w.release()
		arr.Release()
		if m.Used() != 0 {
			t.Fatal("growth leaked")
		}
	}
}

func TestCompactNullHiddenPayloadDoesNotConsumeBudget(t *testing.T) {
	hidden := strings.Repeat("h", 1<<20)
	childStrings := &memory.StringViewVector{Offsets: []uint32{0, uint32(len(hidden)), uint32(len(hidden) + 1), uint32(len(hidden) + 1)}, Data: []byte(hidden + "x")}
	child := memory.NewBatch()
	child.Rows = 3
	child.Strings["s"] = childStrings
	root := filterNumericBatch(1, 1, 0)
	root.Structs["record"] = &memory.StructVector{Rows: 3, Values: child, Validity: memory.Bitmap{6}, Fields: []memory.Field{{Name: "s", Type: memory.TypeString}}}
	root.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0, 1, 3, 3}, Validity: memory.Bitmap{6}, Values: &memory.StringViewVector{Offsets: []uint32{0, uint32(len(hidden)), uint32(2 * len(hidden)), uint32(2*len(hidden) + 1)}, Data: []byte(hidden + hidden + "y"), Validity: memory.Bitmap{5}}}
	m := memory.NewBudgetManager(512)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	out, err := w.compact(context.Background(), root, []int{0, 1}, memory.Bitmap{3}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Structs["record"].Values.Strings["s"].Data) != "x" || !reflect.DeepEqual(out.Structs["record"].Values.Strings["s"].Offsets, []uint32{0, 0, 1}) {
		t.Fatal("NULL struct child payload copied")
	}
	list := out.StringLists["list"]
	if string(list.Values.Data) != "y" || !reflect.DeepEqual(list.Offsets, []uint32{0, 0, 2}) || !reflect.DeepEqual(list.Values.Offsets, []uint32{0, 0, 1}) {
		t.Fatalf("NULL list/element payload copied: offsets=%v child=%v bytes=%d", list.Offsets, list.Values.Offsets, len(list.Values.Data))
	}
	if m.Used() != filterOwnedCapacity(out) {
		t.Fatalf("NULL capacity mismatch actual=%d charge=%d", filterOwnedCapacity(out), m.Used())
	}
}

type filterProbeSource struct {
	schema               memory.Schema
	next                 func(context.Context, *memory.Batch) (bool, error)
	close                func() error
	opens, nexts, closes int
}

func (s *filterProbeSource) Schema() memory.Schema                                  { return s.schema }
func (s *filterProbeSource) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *filterProbeSource) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *filterProbeSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return &filterProbeStream{s: s}, nil
}

type filterProbeStream struct{ s *filterProbeSource }

func (s *filterProbeStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	s.s.nexts++
	return s.s.next(ctx, b)
}
func (s *filterProbeStream) Close() error {
	s.s.closes++
	if s.s.close != nil {
		return s.s.close()
	}
	return nil
}

func TestFilterQueryDriftRejectedBeforeChildAdvance(t *testing.T) {
	background := context.Background()
	q1, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q1.Close()
	q2, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q2.Close()
	contexts := []context.Context{background, memory.WithQueryBudget(background, q1), memory.WithQueryBudget(background, q2)}
	for i, openCtx := range contexts {
		for j, nextCtx := range contexts {
			if i == j || j == 0 {
				continue
			}
			probe := &filterProbeSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}), next: func(context.Context, *memory.Batch) (bool, error) { return false, nil }}
			filtered, err := NewFilterSource(probe, expr.GT(expr.Col("x"), expr.I64(0)))
			if err != nil {
				t.Fatal(err)
			}
			stream, err := filtered.NewBatchStream(openCtx, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := stream.Next(nextCtx, memory.NewBatch()); ok || err == nil {
				t.Fatal("query drift accepted")
			}
			if probe.nexts != 0 || probe.closes != 1 || q1.Used() != 0 || q2.Used() != 0 {
				t.Fatal("query drift advanced child or leaked")
			}
			stream.Close()
		}
	}
}

func TestFilterMalformedVisibleOutputFailsBeforeBorrow(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, kind := range []string{"struct", "numeric", "string", "list"} {
			bad := filterNumericBatch(1, 0)
			field := memory.Field{Name: "bad"}
			switch kind {
			case "struct":
				field.Type = memory.TypeStruct
				bad.Structs["bad"] = &memory.StructVector{Rows: 2}
			case "numeric":
				field.Type = memory.TypeInt32
				bad.Int32s["bad"] = &memory.Int32Vector{Values: []int32{1}}
			case "string":
				field.Type = memory.TypeString
				bad.Strings["bad"] = &memory.StringViewVector{Offsets: []uint32{0}}
			case "list":
				field.Type = memory.TypeList
				bad.StringLists["bad"] = &memory.StringListVector{Offsets: []uint32{0}}
			}
			probe := &filterProbeSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, field)}
			probe.next = func(_ context.Context, dst *memory.Batch) (bool, error) {
				bindAll(dst, bad)
				dst.Rows = bad.Rows
				return true, nil
			}
			predicate := expr.GE(expr.Col("x"), expr.I64(0))
			if partial {
				predicate = expr.GT(expr.Col("x"), expr.I64(0))
			}
			filtered, err := NewFilterSource(probe, predicate)
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(4096)
			opts := testReadOptions()
			opts.MemoryManager = m
			stream, err := filtered.NewBatchStream(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			out := memory.NewBatch()
			if ok, err := stream.Next(context.Background(), out); ok || err == nil {
				t.Fatalf("malformed %s partial=%v succeeded", kind, partial)
			}
			stream.Close()
			if m.Used() != 0 || probe.closes != 1 || out.Rows != 0 {
				t.Fatal("malformed error cleanup")
			}
		}
	}
}

func TestFilterContiguousOpaqueNestedBorrowLifecycle(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, mode := range []string{"next", "error", "cancel", "close"} {
			t.Run(fmt.Sprintf("nested=%v/%s", nested, mode), func(t *testing.T) {
				allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
				defer allocator.AssertSize(t, 0)
				builder := array.NewInt64Builder(allocator)
				builder.AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7}, nil)
				whole := builder.NewArray()
				builder.Release()
				sliced := array.NewSlice(whole, 2, 6)
				whole.Release()
				raw := &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, ArrowType: "int64", Value: sliced, Owned: true, Validity: memory.Bitmap{15}}
				input := filterNumericBatch(0, 1, 1, 0)
				fields := []memory.Field{{Name: "x", Type: memory.TypeInt64}}
				if nested {
					child := memory.NewBatch()
					child.Rows = 4
					child.Opaque["raw"] = raw
					field := memory.Field{Name: "record", Type: memory.TypeStruct, Children: []memory.Field{{Name: "raw", Type: memory.TypeArrow, ArrowType: "int64"}}}
					input.Structs["record"] = &memory.StructVector{Rows: 4, Values: child, Fields: field.Children}
					fields = append(fields, field)
				} else {
					input.Opaque["raw"] = raw
					fields = append(fields, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: "int64"})
				}
				nextErr, closeErr := errors.New("child next failed"), errors.New("child close failed")
				probe := &filterProbeSource{schema: memory.NewSchema(fields...)}
				var stream *filterBatchStream
				firstDst := memory.NewBatch()
				probe.next = func(_ context.Context, dst *memory.Batch) (bool, error) {
					if probe.nexts == 1 {
						bindAll(dst, input)
						dst.Rows = 4
						return true, nil
					}
					if firstDst.Rows != 0 {
						t.Error("previous destination not cleared before child Next")
					}
					if view := stream.output.batch; view != nil {
						if nested {
							view = view.Structs["record"].Values
						}
						if view.Opaque["raw"].Value != nil || view.Opaque["raw"].Owned {
							t.Error("borrowed slice retained across child Next")
						}
					}
					if !raw.Owned {
						t.Error("filter changed source ownership")
					}
					input.ReleaseOwned()
					allocator.AssertSize(t, 0)
					if mode == "error" {
						return false, nextErr
					}
					return false, nil
				}
				probe.close = func() error {
					input.ReleaseOwned()
					allocator.AssertSize(t, 0)
					if mode == "error" {
						return closeErr
					}
					return nil
				}
				filtered, err := NewFilterSource(probe, expr.GT(expr.Col("x"), expr.I64(0)))
				if err != nil {
					t.Fatal(err)
				}
				m := memory.NewBudgetManager(4096)
				opts := testReadOptions()
				opts.MemoryManager = m
				opened, err := filtered.NewBatchStream(context.Background(), opts)
				if err != nil {
					t.Fatal(err)
				}
				stream = opened.(*filterBatchStream)
				if ok, err := stream.Next(context.Background(), firstDst); !ok || err != nil {
					t.Fatal(err)
				}
				view := firstDst
				if nested {
					view = view.Structs["record"].Values
				}
				out := view.Opaque["raw"]
				if out.Owned || !raw.Owned || !reflect.DeepEqual(out.Value.(*array.Int64).Int64Values(), []int64{3, 4}) {
					t.Fatal("sliced input values/ownership corrupted")
				}
				if stream.output.lease.Bytes() != filterOwnedCapacity(stream.output.batch) {
					t.Fatal("opaque slice validity charge differs from capacity")
				}
				switch mode {
				case "next", "error":
					ok, err := stream.Next(context.Background(), memory.NewBatch())
					if ok {
						t.Fatal("unexpected second output")
					}
					if mode == "error" {
						if !errors.Is(err, nextErr) || !errors.Is(err, closeErr) {
							t.Fatalf("lost joined child errors: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				case "cancel":
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					if ok, err := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				case "close":
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
				_ = stream.Close()
				_ = stream.Close()
				if firstDst.Rows != 0 || m.Used() != 0 || probe.closes != 1 {
					t.Fatalf("borrow/budget/close leak rows=%d used=%d closes=%d", firstDst.Rows, m.Used(), probe.closes)
				}
			})
		}
	}
}

func TestFilterProjectedOpaqueDoesNotRequireGather(t *testing.T) {
	input := filterNumericBatch(1, 0, 1, 0)
	// A source-native opaque value lacking any owned-gather contract is harmless
	// when it is not in the output projection.
	input.Opaque["hidden"] = &memory.OpaqueVector{Rows: 4, Type: memory.TypeArrow, Value: struct{}{}, Owned: false}
	probe := &filterProbeSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64}, memory.Field{Name: "hidden", Type: memory.TypeArrow})}
	probe.next = func(_ context.Context, dst *memory.Batch) (bool, error) {
		if probe.nexts > 1 {
			return false, nil
		}
		bindAll(dst, input)
		dst.Rows = 4
		return true, nil
	}
	filtered, err := NewFilterProjectSource(probe, expr.GT(expr.Col("x"), expr.I64(0)), []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewBudgetManager(4096)
	opts := testReadOptions()
	opts.MemoryManager = m
	stream, err := filtered.NewBatchStream(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		t.Fatal(err)
	}
	if len(out.Opaque) != 0 || out.Rows != 2 {
		t.Fatal("opaque projection failed")
	}
	stream.Close()
	if m.Used() != 0 {
		t.Fatal("projected filter leaked")
	}
	stats, err := filtered.Stats(context.Background())
	if err != nil || !stats.RowsUpperBound {
		t.Fatal("filter statistics lost upper-bound contract")
	}
}

func TestCompactStructOwnNullChildrenNormalizeWithAllValidParent(t *testing.T) {
	child := memory.NewBatch()
	child.Rows = 3
	child.Int64s["number"] = &memory.Int64Vector{Values: []int64{99, 42, 7}, Validity: memory.Bitmap{6}}
	child.DictStrings["text"] = &memory.DictionaryStringVector{Indices: []uint32{2, 1, 0}, Offsets: []uint32{0, 1, 2, 3}, Data: []byte("abc"), Validity: memory.Bitmap{6}}
	root := memory.NewBatch()
	root.Rows = 3
	root.Structs["record"] = &memory.StructVector{Rows: 3, Values: child}
	m := memory.NewBudgetManager(4096)
	w := compactWorkspace{manager: m, bounded: true}
	defer w.release()
	out, err := w.compact(context.Background(), root, []int{0, 1}, memory.Bitmap{3}, testReadOptions().Kernels)
	if err != nil {
		t.Fatal(err)
	}
	children := out.Structs["record"].Values
	if !reflect.DeepEqual(children.Int64s["number"].Values, []int64{0, 42}) || !reflect.DeepEqual(children.DictStrings["text"].Indices, []uint32{0, 1}) || children.Int64s["number"].Validity[0] != 2 || children.DictStrings["text"].Validity[0] != 2 {
		t.Fatal("own-null children under all-valid struct not normalized")
	}
	if filterOwnedCapacity(out) != m.Used() {
		t.Fatal("own-null child capacity charge mismatch")
	}
}

func TestFilterPlainContextKeepsOpeningOwnerBeforeChildWork(t *testing.T) {
	type valueKey struct{}
	plain := context.WithValue(context.Background(), valueKey{}, "caller-value")
	q, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q.Close()
	owned := memory.WithQueryBudget(plain, q)
	input := filterNumericBatch(1, 0, 1, 0)
	probe := &filterProbeSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})}
	probe.next = func(ctx context.Context, dst *memory.Batch) (bool, error) {
		if memory.QueryBudgetFromContext(ctx) != q {
			t.Error("child did not receive opening owner")
		}
		if ctx.Value(valueKey{}) != "caller-value" {
			t.Error("binding dropped caller values")
		}
		// A context-selected child reservation must remain on q even when Next's
		// caller omitted it; fallback/ambient ownership is never adopted.
		lease, err := memory.QueryBudgetFromContext(ctx).Reserve(ctx, 32)
		if err != nil {
			return false, err
		}
		lease.Release()
		if probe.nexts > 3 {
			return false, nil
		}
		bindAll(dst, input)
		dst.Rows = input.Rows
		return true, nil
	}
	filtered, err := NewFilterSource(probe, expr.GT(expr.Col("x"), expr.I64(0)))
	if err != nil {
		t.Fatal(err)
	}
	opts := testReadOptions()
	opts.MemoryManager = memory.NewBudgetManager(1)
	stream, err := filtered.NewBatchStream(owned, opts)
	if err != nil {
		t.Fatal(err)
	}
	var previous *memory.Batch
	for _, ctx := range []context.Context{plain, owned, plain} {
		out := memory.NewBatch()
		if ok, err := stream.Next(ctx, out); !ok || err != nil {
			t.Fatal(err)
		}
		if previous != nil && previous.Rows != 0 {
			t.Fatal("old destination still bound")
		}
		if out.Rows != 2 || q.Used() == 0 {
			t.Fatal("result or accounting lost")
		}
		previous = out
	}
	if ok, err := stream.Next(plain, memory.NewBatch()); ok || err != nil {
		t.Fatalf("plain EOF ok=%v err=%v", ok, err)
	}
	if previous.Rows != 0 || q.Used() != 0 || opts.MemoryManager.Used() != 0 || probe.nexts != 4 {
		t.Fatal("EOF owner/borrow leak")
	}
	stream.Close()
}

func TestFilterExplicitOwnerMismatchBeforeWouldBeEOF(t *testing.T) {
	plain := context.Background()
	q1, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q1.Close()
	q2, _ := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{})
	defer q2.Close()
	owned := memory.WithQueryBudget(plain, q1)
	other := memory.WithQueryBudget(plain, q2)
	for _, openCtx := range []context.Context{plain, owned} {
		input := filterNumericBatch(1, 0)
		probe := &filterProbeSource{schema: memory.NewSchema(memory.Field{Name: "x", Type: memory.TypeInt64})}
		probe.next = func(_ context.Context, dst *memory.Batch) (bool, error) {
			if probe.nexts > 1 {
				return false, nil
			}
			bindAll(dst, input)
			dst.Rows = input.Rows
			return true, nil
		}
		filtered, err := NewFilterSource(probe, expr.GT(expr.Col("x"), expr.I64(0)))
		if err != nil {
			t.Fatal(err)
		}
		fallback := memory.NewBudgetManager(4096)
		opts := testReadOptions()
		opts.MemoryManager = fallback
		stream, err := filtered.NewBatchStream(openCtx, opts)
		if err != nil {
			t.Fatal(err)
		}
		out := memory.NewBatch()
		if ok, err := stream.Next(plain, out); !ok || err != nil {
			t.Fatal(err)
		}
		// Next would read EOF, but an explicit new owner must fail before that read.
		if ok, err := stream.Next(other, memory.NewBatch()); ok || err == nil {
			t.Fatal("owner mismatch accepted on EOF path")
		}
		if probe.nexts != 1 || probe.closes != 1 || q1.Used() != 0 || q2.Used() != 0 || fallback.Used() != 0 || out.Rows != 0 {
			t.Fatal("owner mismatch advanced or leaked child work")
		}
		stream.Close()
	}
}
