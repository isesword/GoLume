package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"reflect"
	"strings"
	"testing"
)

func TestJoinEncodingAdmissionRollsBack(t *testing.T) {
	for _, limit := range []int64{99, 149} {
		manager := memory.NewBudgetManager(limit)
		vectors, encoded, err := reserveJoinEncoding(context.Background(), manager, manager, 100, 50)
		if !errors.Is(err, memory.ErrBudgetExceeded) || vectors != nil || encoded != nil || manager.Used() != 0 {
			t.Fatalf("limit=%d vectors=%v encoded=%v used=%d err=%v", limit, vectors, encoded, manager.Used(), err)
		}
	}
	manager := memory.NewBudgetManager(150)
	vectors, encoded, err := reserveJoinEncoding(context.Background(), manager, manager, 100, 50)
	if err != nil || manager.Used() != 150 {
		t.Fatal(err, manager.Used())
	}
	vectors.Release()
	if manager.Used() != 50 {
		t.Fatal("encoded bytes released with vectors")
	}
	encoded.Release()
	if manager.Used() != 0 {
		t.Fatal("reservation leak")
	}
}

func joinEncodingFixture(t *testing.T) (context.Context, *JoinSource, *spillableJoinStream, *memory.QueryBudget, string, *memory.Batch, *loadedJoinBuild, []join.Pair) {
	t.Helper()
	batch := memory.NewBatch()
	batch.Rows = 512
	batch.Int64s["key"] = &memory.Int64Vector{Values: make([]int64, batch.Rows)}
	batch.Int64s["row"] = &memory.Int64Vector{Values: make([]int64, batch.Rows)}
	for i := 0; i < batch.Rows; i++ {
		batch.Int64s["row"].Values[i] = int64(i)
	}
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "row", Type: memory.TypeInt64})
	src, err := inmemory.New(schema, batch)
	if err != nil {
		t.Fatal(err)
	}
	ctx, owner, stream, query, directory := openByteSpill(t, src, src, join.Inner, []JoinKey{{"key", "key"}}, 1<<20, 64<<20)
	build := &loadedJoinBuild{batches: []*memory.Batch{batch}, locations: []rowLocation{{batch: 0, row: 0}}}
	pairs := make([]join.Pair, batch.Rows)
	for i := range pairs {
		pairs[i] = join.Pair{ProbeRow: i, BuildRow: 0}
	}
	return ctx, owner, stream, query, directory, batch, build, pairs
}

func TestJoinEncodingPreflightSplitsBeforeWrite(t *testing.T) {
	ctx, owner, stream, query, directory, batch, build, pairs := joinEncodingFixture(t)
	logical, err := estimateJoinMaterializationBytes(stream.ownerWithBuild(build), batch, pairs)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := estimateJoinEncodingBytes(stream.ownerWithBuild(build), logical)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := joinBatchMetadataBytes(owner.schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	available := logical + metadata - 1024 + encoded - 1
	hold, err := query.Operator("OtherOperator", memory.MemoryRetainedState).Reserve(ctx, query.Limit()-query.Used()-available)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	before := query.Used()
	_, preflight, err := stream.flushPairs(ctx, build, batch, pairs, 0)
	if !preflight || !errors.Is(err, memory.ErrBudgetExceeded) || stream.spillRuns != 0 || query.Used() != before {
		t.Fatalf("preflight=%t err=%v writes=%d delta=%d", preflight, err, stream.spillRuns, query.Used()-before)
	}
	var created spillmeta.Slice[joinRunRef]
	defer created.Release()
	if err := stream.appendOutputPairs(ctx, build, batch, pairs, 0, &created); err != nil {
		t.Fatal(err)
	}
	if len(created.Values) != 2 || stream.outputCount != int64(len(pairs)) {
		t.Fatalf("runs=%d rows=%d", len(created.Values), stream.outputCount)
	}
	next := int64(0)
	for _, ref := range created.Values {
		decoded, err := stream.restoreRun(ctx, owner.schema, stream.outputFP, ref)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range decoded.Batch.Int64s["row"].Values {
			if row != next {
				t.Fatalf("row=%d want=%d", row, next)
			}
			next++
		}
		decoded.Close()
	}
	hold.Release()
	created.Release()
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, query, directory)
}

type joinEncodingFailSpill struct {
	memory.SpillManager
	err          error
	calls        int
	encodedBytes int64
	query        *memory.QueryBudget
}

func (f *joinEncodingFailSpill) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	f.calls++
	f.encodedBytes = f.query.Snapshot().Operators["JoinEncode"]
	if f.encodedBytes < int64(len(object.Payload)) {
		return nil, fmt.Errorf("unpaid encoding: charged=%d bytes=%d", f.encodedBytes, len(object.Payload))
	}
	return nil, f.err
}

func TestJoinEncodingWriteFailureNeverRetries(t *testing.T) {
	ctx, owner, stream, query, directory, batch, build, pairs := joinEncodingFixture(t)
	base, err := query.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	stream.store.Close()
	failure := &joinEncodingFailSpill{SpillManager: base, err: fmt.Errorf("write admission: %w", memory.ErrBudgetExceeded), query: query}
	stream.store = spillmeta.NewStore(failure, stream.metadata)
	before := query.Used()
	var created spillmeta.Slice[joinRunRef]
	defer created.Release()
	err = stream.appendOutputPairs(ctx, build, batch, pairs, 0, &created)
	if !errors.Is(err, memory.ErrBudgetExceeded) || failure.calls != 1 || len(created.Values) != 0 || query.Used() != before {
		t.Fatalf("err=%v writes=%d refs=%d delta=%d", err, failure.calls, len(created.Values), query.Used()-before)
	}
	stream.Close()
	assertByteSpillClean(t, owner, query, directory)
}

func TestJoinBoundedMaterializationExactNestedCapacities(t *testing.T) {
	fields := make([]memory.Field, 0, 66)
	values := memory.NewBatch()
	values.Rows = 2
	for i := 0; i < 64; i++ {
		name := fmt.Sprintf("n%d", i)
		fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64})
		values.Int64s[name] = &memory.Int64Vector{Values: []int64{int64(i), 99}}
	}
	fields = append(fields, memory.Field{Name: "s", Type: memory.TypeString}, memory.ListStringField("l", true, true))
	values.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1024, 1025}, Data: []byte(strings.Repeat("s", 1024) + "x")}
	list := &memory.StringListVector{Offsets: []uint32{0, 65, 65}, Values: &memory.StringViewVector{Offsets: make([]uint32, 66), Data: []byte(strings.Repeat("v", 65))}}
	for i := range list.Values.Offsets {
		list.Values.Offsets[i] = uint32(i)
	}
	values.StringLists["l"] = list
	field := memory.Field{Name: "record", Type: memory.TypeStruct, Nullable: true, Children: fields}
	left := memory.NewBatch()
	left.Rows = 2
	left.Structs["record"] = &memory.StructVector{Rows: 2, Fields: fields, Values: values, Validity: memory.Bitmap{1}}
	owner := &JoinSource{schema: memory.NewSchema(field), bindings: []joinOutputBinding{{in: "record", out: "record", field: field}}}
	pairs := make([]join.Pair, 65)
	for i := range pairs {
		row := i % 3
		if row == 2 {
			row = -1
		}
		pairs[i] = join.Pair{ProbeRow: row, BuildRow: -1}
	}
	dst := memory.NewBatch()
	if err := materializeJoinBatchMode(dst, owner, left, pairs, true); err != nil {
		t.Fatal(err)
	}
	v := dst.Structs["record"]
	if &v.Fields[0] != &fields[0] {
		t.Fatal("bounded materializer copied field descriptors")
	}
	validRows := 0
	for i := range pairs {
		want := i%3 == 0
		if v.IsValid(i) != want {
			t.Fatalf("parent validity row=%d", i)
		}
		if want {
			validRows++
		}
	}
	for name, col := range v.Values.Int64s {
		if cap(col.Values) != len(pairs) || cap(col.Validity) != memory.WordsFor(len(pairs)) {
			t.Fatalf("%s grew beyond exact capacity", name)
		}
		for i := range pairs {
			if col.IsValid(i) != (i%3 == 0) {
				t.Fatalf("child %s NULL row=%d", name, i)
			}
		}
	}
	str := v.Values.Strings["s"]
	if len(str.Data) != validRows*1024 || cap(str.Data) != len(str.Data) || cap(str.Offsets) != 66 {
		t.Fatal("string did not use exact capacity")
	}
	out := v.Values.StringLists["l"]
	if cap(out.Offsets) != 66 || cap(out.Validity) != 2 || cap(out.Values.Offsets) != validRows*65+1 || cap(out.Values.Data) != validRows*65 || cap(out.Values.Validity) != memory.WordsFor(validRows*65) {
		t.Fatal("list did not use exact capacities")
	}
	logical, err := estimateJoinMaterializationBytes(owner, left, pairs)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := estimateJoinEncodingBytes(owner, logical)
	if err != nil {
		t.Fatal(err)
	}
	run, err := memory.EncodeColumnarRun(context.Background(), owner.schema, dst, memory.NewBudgetManager(encoded))
	if err != nil {
		t.Fatal(err)
	}
	run.Close()
	metadata, err := joinBatchMetadataBytes(owner.schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	if metadata < 64*512 {
		t.Fatalf("wide schema not charged: %d", metadata)
	}
	bad := field
	bad.Children = []memory.Field{{Name: "dictionary", Type: memory.TypeString, Encoding: memory.EncodingDictionary}}
	if bad.ValidateStruct() == nil {
		t.Fatal("unsupported dictionary child accepted")
	}
}

// Track only the original two run-array reservations, while forwarding every
// byte to the real query. Descendant handles have their own ordinary accounting.
type joinArrayTracker struct {
	memory.Manager
	used int64
}
type joinArrayReservation struct {
	memory.Reservation
	owner    *joinArrayTracker
	released bool
}

func (m *joinArrayTracker) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	r, err := m.Manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	m.used += bytes
	return &joinArrayReservation{Reservation: r, owner: m}, nil
}
func (r *joinArrayReservation) Release() {
	if !r.released {
		r.released = true
		r.owner.used -= r.Bytes()
		r.Reservation.Release()
	}
}

type joinRecursiveRestoreCheck struct {
	memory.SpillManager
	parents map[string]bool
	check   func() error
}

func (s *joinRecursiveRestoreCheck) SpillWithMetadata(ctx context.Context, o memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	return s.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, o, m)
}
func (s *joinRecursiveRestoreCheck) Restore(ctx context.Context, h memory.SpillHandle, fp [32]byte, m memory.Manager) (*memory.RestoredSpill, error) {
	if s.parents != nil && !s.parents[h.ID()] && s.check != nil {
		if err := s.check(); err != nil {
			return nil, err
		}
	}
	return s.SpillManager.Restore(ctx, h, fp, m)
}

func TestJoinRecursiveParentsReleasedBeforeDescendant(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			key := int32(8)
			partition := int(hashagg.Hash128(hashagg.Key128{A: 7}) & (joinRadixPartitions - 1))
			for int(hashagg.Hash128(hashagg.Key128{A: uint64(key)})&(joinRadixPartitions-1)) != partition {
				key++
			}
			rows := []skewBudgetRow{{key: 7}, {key: key}}
			ctx, owner, s, q, dir := openByteSpill(t, skewBudgetSource(t, rows, 2, 0, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, rows, 2, 0, false)}, join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
			pressure := installSkewBudgetStore(t, s, q, nil)
			if err := s.store.Close(); err != nil {
				t.Fatal(err)
			}
			checked := &joinRecursiveRestoreCheck{SpillManager: pressure}
			s.store = spillmeta.NewStore(checked, s.metadata)
			if err := s.prepare(ctx); err != nil {
				t.Fatal(err)
			}
			pressure.arm(s)
			checked.parents = make(map[string]bool)
			tracker := &joinArrayTracker{Manager: s.metadata}
			for _, original := range []*spillmeta.Slice[joinRunRef]{&s.leftRuns[partition], &s.rightRuns[partition]} {
				var tracked spillmeta.Slice[joinRunRef]
				if err := tracked.Append(ctx, tracker, original.Values...); err != nil {
					t.Fatal(err)
				}
				for _, ref := range original.Values {
					checked.parents[ref.handle.ID()] = true
				}
				original.Release()
				*original = tracked
			}
			if tracker.used == 0 {
				t.Fatal("parent backing not tracked")
			}
			visits := 0
			sentinel := errors.New("descendant restore failure")
			checked.check = func() error {
				visits++
				if tracker.used != 0 || s.leftRuns[partition].Values != nil || s.rightRuns[partition].Values != nil {
					t.Fatalf("parent owner survived recursive descent: bytes=%d", tracker.used)
				}
				if fail {
					return sentinel
				}
				return nil
			}
			got, err := drainByteSpill(ctx, s)
			if visits == 0 {
				t.Fatal("no descendant restore reached")
			}
			if fail {
				if !errors.Is(err, sentinel) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				sortBytePairs(got)
				want := [][2]int64{{0, 0}, {1, 1}}
				if !reflect.DeepEqual(got, want) {
					t.Fatal(got)
				}
			}
			if tracker.used != 0 {
				t.Fatal("parent reservation leak")
			}
			assertSkewBudgetReleased(t, s, q, dir)
			s.Close()
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}

func TestJoinBoundedNullStructAllScalarChildren(t *testing.T) {
	types := []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64, memory.TypeString, memory.TypeBool, memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration}
	children := make([]memory.Field, 0, len(types)+1)
	for i, typ := range types {
		children = append(children, memory.Field{Name: fmt.Sprintf("v%d", i), Type: typ})
	}
	children = append(children, memory.ListStringField("list", false, false))
	field := memory.StructField("record", true, children...)
	owner := &JoinSource{schema: memory.NewSchema(field), bindings: []joinOutputBinding{{in: "record", out: "record", field: field}}}
	pairs := []join.Pair{{ProbeRow: -1, BuildRow: -1}, {ProbeRow: -1, BuildRow: -1}}
	dst := memory.NewBatch()
	if err := materializeJoinBatchMode(dst, owner, nil, pairs, true); err != nil {
		t.Fatal(err)
	}
	if dst.Rows != 2 || dst.Structs["record"].IsValid(0) || dst.Structs["record"].IsValid(1) {
		t.Fatal("NULL parent not preserved")
	}
	estimate, err := estimateJoinMaterializationBytes(owner, nil, pairs)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := estimateJoinEncodingBytes(owner, estimate)
	if err != nil {
		t.Fatal(err)
	}
	run, err := memory.EncodeColumnarRun(context.Background(), owner.schema, dst, memory.NewBudgetManager(encoded))
	if err != nil {
		t.Fatal(err)
	}
	run.Close()
}

type joinRejectReferenceAppend struct {
	memory.Manager
	calls int
}

func (m *joinRejectReferenceAppend) Reserve(context.Context, int64) (memory.Reservation, error) {
	m.calls++
	return nil, memory.ErrBudgetExceeded
}

func TestJoinEncodingReferenceFailureKeepsCommittedRun(t *testing.T) {
	ctx, owner, s, q, dir, batch, build, pairs := joinEncodingFixture(t)
	// The Store keeps its original admitted manager. Fail only the subsequent
	// output-reference append, after the output file has already been committed.
	reject := &joinRejectReferenceAppend{Manager: s.metadata}
	s.metadata = reject
	before := q.Used()
	var created spillmeta.Slice[joinRunRef]
	err := s.appendOutputPairs(ctx, build, batch, pairs, 0, &created)
	if !errors.Is(err, memory.ErrBudgetExceeded) || reject.calls != 1 || s.spillRuns != 1 || s.outputCount != int64(len(pairs)) || len(created.Values) != 0 {
		t.Fatalf("err=%v append=%d writes=%d rows=%d refs=%d", err, reject.calls, s.spillRuns, s.outputCount, len(created.Values))
	}
	if q.Used() <= before {
		t.Fatal("committed handle lost ownership after reference failure")
	}
	created.Release()
	s.Close()
	assertByteSpillClean(t, owner, q, dir)
}

func TestJoinEncodingGatherPaysSelectedPayload(t *testing.T) {
	ctx, owner, s, q, dir, batch, _, _ := joinEncodingFixture(t)
	schema := owner.left.Schema()
	rows := make([]int, 32)
	for i := range rows {
		rows[i] = i
	}
	encoded, err := memory.EstimateColumnarRunSelectionBytes(schema, batch, rows)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := joinBatchMetadataBytes(schema.Fields)
	if err != nil {
		t.Fatal(err)
	}
	available := 2*memory.BatchBytes(batch) + metadata + encoded + 4096
	fullEncoded, err := memory.EstimateColumnarRunRangeBytes(schema, batch, 0, batch.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if fullEncoded <= encoded+4096 {
		t.Fatal("fixture must distinguish full-source prepayment")
	}
	hold, err := q.Operator("OtherOperator", memory.MemoryRetainedState).Reserve(ctx, q.Limit()-q.Used()-available)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	ref, err := s.spillRows(ctx, schema, memory.SchemaFingerprint(schema), batch, rows, 0)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := s.restoreRun(ctx, schema, memory.SchemaFingerprint(schema), ref)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range decoded.Batch.Int64s["row"].Values {
		if v != int64(i) {
			t.Fatal("gather changed selected values")
		}
	}
	decoded.Close()
	hold.Release()
	s.Close()
	assertByteSpillClean(t, owner, q, dir)
}
