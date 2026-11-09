package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

const joinCustomSourceAliases = 4096

// This source deliberately exposes more runtime bindings than its schema. It
// must not delegate to inmemory: projecting that source's advertised schema
// would remove the very bindings whose gather admission is under test.
// Leaving replayability unknown also prevents a small-build broadcast shortcut.
type joinAccountingCustomSource struct {
	schema memory.Schema
	batch  *memory.Batch

	opens, nextCalls, closeCalls int
	emittedBatches               int
	emittedInt8s                 int
	emittedUint8s                int
	emittedOpaque                int
}

func newJoinAccountingCustomSource(keys []int64, extras bool) *joinAccountingCustomSource {
	b := memory.NewBatch()
	b.Rows = len(keys)
	b.Int64s["key"] = &memory.Int64Vector{Values: keys, Validity: memory.Bitmap{3}}
	if extras {
		alias := &memory.Int8Vector{Values: []int8{3, 4, 5}, Validity: memory.Bitmap{5}}
		for i := 0; i < joinCustomSourceAliases; i++ {
			b.Int8s[fmt.Sprintf("extra_%04d", i)] = alias
		}
		// A duplicate name in a different physical map is a separate gather.
		b.Uint8s["extra_0000"] = &memory.Uint8Vector{Values: []uint8{9, 8, 7}, Validity: memory.Bitmap{5}}
	}
	return &joinAccountingCustomSource{
		schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true}),
		batch:  b,
	}
}

func (s *joinAccountingCustomSource) Schema() memory.Schema { return s.schema }

func (s *joinAccountingCustomSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(s.batch.Rows), Partitions: 1, Bytes: memory.BatchBytes(s.batch)}, nil
}

func (s *joinAccountingCustomSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(s.batch.Rows)}}, nil
}

func (s *joinAccountingCustomSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.opens++
	return &joinAccountingCustomStream{source: s}, nil
}

type joinAccountingCustomStream struct {
	source       *joinAccountingCustomSource
	done, closed bool
}

func (s *joinAccountingCustomStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.source.nextCalls++
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dst.ClearColumns()
	if s.done {
		return false, nil
	}
	s.done = true
	b := s.source.batch
	dst.Rows = b.Rows
	// Copy bindings into caller-owned maps, never share the source's maps: the
	// join clears its input Batch after partitioning, even when admission fails.
	for name, v := range b.Int64s {
		dst.Int64s[name] = v
	}
	for name, v := range b.Int8s {
		dst.Int8s[name] = v
	}
	for name, v := range b.Uint8s {
		dst.Uint8s[name] = v
	}
	for name, v := range b.Opaque {
		dst.Opaque[name] = v
	}
	s.source.emittedBatches++
	s.source.emittedInt8s += len(dst.Int8s)
	s.source.emittedUint8s += len(dst.Uint8s)
	s.source.emittedOpaque += len(dst.Opaque)
	return true, nil
}

func (s *joinAccountingCustomStream) Close() error {
	s.source.closeCalls++
	s.closed = true
	return nil
}

var _ source.Source = (*joinAccountingCustomSource)(nil)

// Observe actual store entry, including a write which fails before a run is
// committed. spillRuns alone would miss such an attempted write.
type joinCustomSourceWriteCounter struct {
	memory.SpillManager
	calls int
}

func (s *joinCustomSourceWriteCounter) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	s.calls++
	return s.SpillManager.Spill(ctx, object)
}

func (s *joinCustomSourceWriteCounter) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	s.calls++
	return s.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, object, manager)
}

func countJoinCustomSourceWrites(t *testing.T, stream *spillableJoinStream, query *memory.QueryBudget) *joinCustomSourceWriteCounter {
	t.Helper()
	base, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := base.(memory.MetadataSpillManager); !ok {
		t.Fatal("query spill store must preserve metadata admission")
	}
	if err := stream.store.Close(); err != nil {
		t.Fatal(err)
	}
	counter := &joinCustomSourceWriteCounter{SpillManager: base}
	stream.store = spillmeta.NewStore(counter, stream.metadata)
	return counter
}

func joinCustomSourceBindingCount(b *memory.Batch) int {
	return len(b.Int8s) + len(b.Int16s) + len(b.Int32s) + len(b.Int64s) +
		len(b.Uint8s) + len(b.Uint16s) + len(b.Uint32s) + len(b.Uint64s) +
		len(b.Float32s) + len(b.Float64s) + len(b.Decimals) + len(b.Strings) +
		len(b.DictStrings) + len(b.StringLists) + len(b.Structs) + len(b.Opaque)
}

// Capture both vector identity and contents. In particular, copying just the
// Batch header would let a destructive ClearColumns erase the expected maps.
func snapshotJoinCustomSource(t *testing.T, s *joinAccountingCustomSource) func() {
	t.Helper()
	b := s.batch
	rows, bindings := b.Rows, joinCustomSourceBindingCount(b)
	fields := append([]memory.Field(nil), s.schema.Fields...)
	key := b.Int64s["key"]
	keyValues := append([]int64(nil), key.Values...)
	keyValidity := append(memory.Bitmap(nil), key.Validity...)
	aliases := make(map[string]*memory.Int8Vector, len(b.Int8s))
	for name, v := range b.Int8s {
		aliases[name] = v
	}
	unsigned := b.Uint8s["extra_0000"]
	opaque := b.Opaque["extra_opaque"]
	var opaqueBefore memory.OpaqueVector
	if opaque != nil {
		opaqueBefore = *opaque
	}
	return func() {
		t.Helper()
		if s.batch != b || b.Rows != rows || joinCustomSourceBindingCount(b) != bindings || !reflect.DeepEqual(s.schema.Fields, fields) {
			t.Fatal("join modified the custom source's batch or advertised schema")
		}
		if len(b.Int64s) != 1 || b.Int64s["key"] != key || !reflect.DeepEqual(key.Values, keyValues) || !reflect.DeepEqual(key.Validity, keyValidity) {
			t.Fatal("join modified the source key vector")
		}
		if len(b.Int8s) != len(aliases) {
			t.Fatal("join removed source aliases")
		}
		for name, v := range aliases {
			if b.Int8s[name] != v || !reflect.DeepEqual(v.Values, []int8{3, 4, 5}) || !reflect.DeepEqual(v.Validity, memory.Bitmap{5}) {
				t.Fatalf("join modified source alias %q", name)
			}
		}
		if b.Uint8s["extra_0000"] != unsigned || (unsigned != nil && (!reflect.DeepEqual(unsigned.Values, []uint8{9, 8, 7}) || !reflect.DeepEqual(unsigned.Validity, memory.Bitmap{5}))) {
			t.Fatal("join modified the source's cross-map alias")
		}
		if b.Opaque["extra_opaque"] != opaque || (opaque != nil && !reflect.DeepEqual(*opaque, opaqueBefore)) {
			t.Fatal("join modified the source-owned opaque vector")
		}
	}
}

func TestJoinCustomSourceRuntimeBindingsPreflight(t *testing.T) {
	const limit = int64(256 << 10)
	left := newJoinAccountingCustomSource([]int64{7, 9, 111}, false)
	right := newJoinAccountingCustomSource([]int64{7, 11, 222}, true)
	checkLeft, checkRight := snapshotJoinCustomSource(t, left), snapshotJoinCustomSource(t, right)
	defer checkLeft()
	defer checkRight()
	ctx, owner, stream, query, directory := openByteSpill(t, left, right, join.Full, []JoinKey{{"key", "key"}}, limit, 64<<20)
	if !owner.spillable || left.opens != 0 || right.opens != 0 {
		t.Fatal("fixture must enter native spill preparation without an eager source scan")
	}
	writes := countJoinCustomSourceWrites(t, stream, query)
	schemaMetadata, err := joinBatchMetadataBytes(right.Schema().Fields)
	if err != nil {
		t.Fatal(err)
	}
	legacyGather := 2*memory.BatchBytes(right.batch) + schemaMetadata
	encoded, err := memory.EstimateColumnarRunRangeBytes(right.Schema(), right.batch, 0, right.batch.Rows)
	if err != nil {
		t.Fatal(err)
	}
	partitionScratch := int64(right.batch.Rows)*16 + joinRadixPartitions*24
	// This is a fixed query budget, not an isolated helper manager. Even the
	// complete batch's encoding plus the legacy gather charge fits after the
	// stream's real base/pair reservations and partition scratch are charged.
	if query.Used()+partitionScratch+legacyGather+encoded >= limit {
		t.Fatalf("fixture no longer distinguishes old admission: base=%d scratch=%d legacy=%d encoded=%d limit=%d", query.Used(), partitionScratch, legacyGather, encoded, limit)
	}
	before := query.Used()
	out := memory.NewBatch()
	ok, err := stream.Next(ctx, out)
	var resource *memory.ResourceError
	if ok || !errors.Is(err, memory.ErrBudgetExceeded) || !errors.As(err, &resource) {
		t.Fatalf("want runtime gather pre-admission failure, got ok=%t err=%v", ok, err)
	}
	// This error identifies the first reservation inside spillRows, before
	// creating a gathered Batch or entering the encoder or spill store.
	if resource.Operator != "JoinPartitionGather" || resource.Class != memory.MemoryWorkspace || resource.RequestedBytes <= limit || resource.RequestedBytes <= legacyGather {
		t.Fatalf("failure did not come from runtime binding admission: %+v", resource)
	}
	if writes.calls != 0 || stream.spillRuns != 0 || stream.spillBytes != 0 || out.Rows != 0 || joinCustomSourceBindingCount(out) != 0 {
		t.Fatalf("preflight allocated output or entered spill write: writes=%d runs=%d bytes=%d rows=%d", writes.calls, stream.spillRuns, stream.spillBytes, out.Rows)
	}
	if right.opens != 1 || right.nextCalls != 1 || right.closeCalls != 1 || right.emittedBatches != 1 || right.emittedInt8s != joinCustomSourceAliases || right.emittedUint8s != 1 || left.opens != 0 {
		t.Fatalf("custom runtime batch was filtered, retried, or leaked: left=%+v right=%+v", left, right)
	}
	if peak := query.Snapshot().PeakBytes; peak > before+partitionScratch {
		t.Fatalf("rejected gather or encoder was admitted: peak=%d base+scratch=%d", peak, before+partitionScratch)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, query, directory)
}

func TestJoinCustomSourceRuntimeBindingsFullJoin(t *testing.T) {
	for _, tc := range []struct {
		name   string
		extras bool
		limit  int64
	}{
		{"declared_only_same_budget", false, 256 << 10},
		{"runtime_aliases_sufficient_budget", true, 8 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := newJoinAccountingCustomSource([]int64{7, 9, 111}, tc.extras)
			right := newJoinAccountingCustomSource([]int64{7, 11, 222}, tc.extras)
			checkLeft, checkRight := snapshotJoinCustomSource(t, left), snapshotJoinCustomSource(t, right)
			defer checkLeft()
			defer checkRight()
			ctx, owner, stream, query, directory := openByteSpill(t, left, right, join.Full, []JoinKey{{"key", "key"}}, tc.limit, 64<<20)
			if !owner.spillable || len(owner.Schema().Fields) != 2 {
				t.Fatal("fixture must retain the schema-only bounded spill strategy")
			}
			out := memory.NewBatch()
			var got [][2]int64
			for {
				ok, err := stream.Next(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if len(out.Int64s) != 2 || joinCustomSourceBindingCount(out) != 2 || out.Int64s["key"] == nil || out.Int64s["key_right"] == nil {
					t.Fatalf("runtime extras leaked into joined output: int64s=%v bindings=%d", out.Int64s, joinCustomSourceBindingCount(out))
				}
				for row := 0; row < out.Rows; row++ {
					pair := [2]int64{-1, -1}
					for i, name := range []string{"key", "key_right"} {
						if v := out.Int64s[name]; v.IsValid(row) {
							pair[i] = v.Values[row]
						}
					}
					got = append(got, pair)
				}
			}
			want := [][2]int64{{7, 7}, {9, -1}, {-1, 11}, {-1, -1}, {-1, -1}}
			sortBytePairs(got)
			sortBytePairs(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("FULL JOIN values or SQL NULL semantics changed: got=%v want=%v", got, want)
			}
			for _, src := range []*joinAccountingCustomSource{left, right} {
				wantExtras, wantUnsigned := 0, 0
				if tc.extras {
					wantExtras, wantUnsigned = joinCustomSourceAliases, 1
				}
				if src.opens != 1 || src.nextCalls != 2 || src.closeCalls != 1 || src.emittedBatches != 1 || src.emittedInt8s != wantExtras || src.emittedUint8s != wantUnsigned {
					t.Fatalf("native source lifecycle or runtime bindings changed: %+v", src)
				}
			}
			if stream.spillRuns == 0 {
				t.Fatal("custom source never reached spill execution")
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			assertByteSpillClean(t, owner, query, directory)
		})
	}
}

func TestJoinCustomSourceOpaqueExtraRejectedBeforeWrite(t *testing.T) {
	allocator := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	builder := array.NewInt64Builder(allocator)
	builder.AppendValues([]int64{31, 32, 33}, []bool{true, false, true})
	arr := builder.NewArray()
	builder.Release()
	defer func() { arr.Release(); allocator.AssertSize(t, 0) }()
	left := newJoinAccountingCustomSource([]int64{7, 9, 111}, false)
	right := newJoinAccountingCustomSource([]int64{7, 11, 222}, false)
	right.batch.Opaque["extra_opaque"] = &memory.OpaqueVector{Rows: 3, Type: memory.TypeInt64, Value: arr}
	checkLeft, checkRight := snapshotJoinCustomSource(t, left), snapshotJoinCustomSource(t, right)
	defer checkLeft()
	defer checkRight()
	ctx, owner, stream, query, directory := openByteSpill(t, left, right, join.Full, []JoinKey{{"key", "key"}}, 8<<20, 64<<20)
	writes := countJoinCustomSourceWrites(t, stream, query)
	before := query.Used()
	out := memory.NewBatch()
	ok, err := stream.Next(ctx, out)
	if ok || err == nil || !strings.Contains(err.Error(), "opaque") || errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("want explicit schema-extra opaque rejection, got ok=%t err=%v", ok, err)
	}
	if writes.calls != 0 || stream.spillRuns != 0 || out.Rows != 0 || joinCustomSourceBindingCount(out) != 0 {
		t.Fatal("opaque rejection happened after output or a spill write")
	}
	if peak := query.Snapshot().PeakBytes; peak > before+int64(right.batch.Rows)*16+joinRadixPartitions*24 {
		t.Fatal("opaque rejection admitted gather or encoding buffers")
	}
	if right.opens != 1 || right.nextCalls != 1 || right.closeCalls != 1 || right.emittedBatches != 1 || right.emittedOpaque != 1 || left.opens != 0 {
		t.Fatalf("opaque runtime binding was filtered, retried, or leaked: left=%+v right=%+v", left, right)
	}
	if native := arr.(*array.Int64); native.Value(0) != 31 || !native.IsNull(1) || native.Value(2) != 33 {
		t.Fatal("rejected join changed source-owned Arrow values")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, query, directory)
}
