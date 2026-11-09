package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

// Unlike inmemory, this source deliberately preserves every top-level runtime
// binding. The destination owns its maps; clearing it cannot clear the input.
// Stats never walks payloads, so malformed graph tests reach sort preflight.
type sortInitialRuntimeSource struct {
	schema                                         memory.Schema
	batches                                        []*memory.Batch
	opens, nexts, closes, emitted, emittedBindings int
}

func (s *sortInitialRuntimeSource) Schema() memory.Schema { return s.schema }
func (s *sortInitialRuntimeSource) Stats(context.Context) (source.Stats, error) {
	var rows int64
	for _, b := range s.batches {
		rows += int64(b.Rows)
	}
	return source.Stats{Rows: rows, Partitions: len(s.batches)}, nil
}
func (s *sortInitialRuntimeSource) Partitions(context.Context) ([]source.Partition, error) {
	parts := make([]source.Partition, len(s.batches))
	for i, b := range s.batches {
		parts[i] = source.Partition{ID: i, Rows: int64(b.Rows)}
	}
	return parts, nil
}
func (s *sortInitialRuntimeSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.opens++
	return &sortInitialRuntimeStream{owner: s}, nil
}

type sortInitialRuntimeStream struct {
	owner  *sortInitialRuntimeSource
	at     int
	closed bool
}

func (s *sortInitialRuntimeStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.owner.nexts++
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dst.ClearColumns()
	if s.at == len(s.owner.batches) {
		return false, nil
	}
	b := s.owner.batches[s.at]
	s.at++
	dst.Rows = b.Rows
	from, to := reflect.ValueOf(b).Elem(), reflect.ValueOf(dst).Elem()
	for i := 0; i < from.NumField(); i++ {
		if from.Field(i).Kind() != reflect.Map {
			continue
		}
		entries := from.Field(i).MapRange()
		for entries.Next() {
			to.Field(i).SetMapIndex(entries.Key(), entries.Value())
			s.owner.emittedBindings++
		}
	}
	s.owner.emitted++
	return true, nil
}
func (s *sortInitialRuntimeStream) Close() error { s.owner.closes++; s.closed = true; return nil }

var _ source.Source = (*sortInitialRuntimeSource)(nil)

func sortInitialAliasSource() *sortInitialRuntimeSource {
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
	alias := &memory.Int8Vector{Values: []int8{3}}
	for i := 0; i < 4096; i++ {
		b.Int8s[fmt.Sprintf("extra_%04d", i)] = alias
	}
	// Equal names in distinct physical maps still require distinct descriptors.
	b.Uint8s["extra_0000"] = &memory.Uint8Vector{Values: []uint8{9}}
	return &sortInitialRuntimeSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}), batches: []*memory.Batch{b}}
}

// Count only public vector descriptors, Batch headers and Struct.Fields. This
// independent lower bound excludes maps, backing payloads, reservation objects,
// allocator rounding and planner constants. Aliased bindings count separately.
// These fixtures contain only Fields-selected children in gathered structs.
func sortInitialDescriptorMinimum(b *memory.Batch) int64 {
	total := int64(unsafe.Sizeof(memory.Batch{}))
	v := reflect.ValueOf(b).Elem()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() != reflect.Map {
			continue
		}
		entries := v.Field(i).MapRange()
		for entries.Next() {
			if !entries.Value().IsNil() {
				total += int64(entries.Value().Elem().Type().Size())
			}
		}
	}
	for range b.StringLists {
		total += int64(unsafe.Sizeof(memory.StringViewVector{}))
	}
	for _, record := range b.Structs {
		total += int64(len(record.Fields)) * int64(unsafe.Sizeof(memory.Field{}))
		total += sortInitialDescriptorMinimum(record.Values)
	}
	return total
}

// Snapshot binding identity and every descriptor/slice independently. Nested
// Batch pointers are visited once, which also makes this safe for bad graphs.
func snapshotSortInitialInput(t *testing.T, batches ...*memory.Batch) func() {
	t.Helper()
	var checks []func()
	seen := make(map[*memory.Batch]bool)
	var visit func(*memory.Batch)
	visit = func(b *memory.Batch) {
		if b == nil || seen[b] {
			return
		}
		seen[b] = true
		rows := b.Rows
		checks = append(checks, func() {
			if b.Rows != rows {
				t.Error("initial gather changed source row count")
			}
		})
		v := reflect.ValueOf(b).Elem()
		for i := 0; i < v.NumField(); i++ {
			columns := v.Field(i)
			if columns.Kind() != reflect.Map {
				continue
			}
			bindings := reflect.MakeMapWithSize(columns.Type(), columns.Len())
			entries := columns.MapRange()
			for entries.Next() {
				name, pointer := entries.Key(), entries.Value()
				bindings.SetMapIndex(name, pointer)
				if pointer.IsNil() {
					continue
				}
				descriptor := pointer.Elem()
				before := reflect.New(descriptor.Type()).Elem()
				before.Set(descriptor)
				for j := 0; j < descriptor.NumField(); j++ {
					field := descriptor.Field(j)
					if field.Kind() == reflect.Slice && !field.IsNil() {
						copy := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
						reflect.Copy(copy, field)
						if field.Type() == reflect.TypeOf([]memory.Field{}) {
							copy = reflect.ValueOf(memory.NewSchema(field.Interface().([]memory.Field)...).Fields)
						}
						before.Field(j).Set(copy)
					}
				}
				checks = append(checks, func() {
					if !reflect.DeepEqual(pointer.Elem().Interface(), before.Interface()) {
						t.Errorf("initial gather changed source descriptor %q", name.String())
					}
				})
			}
			checks = append(checks, func() {
				if columns.Len() != bindings.Len() {
					t.Error("initial gather changed source map size")
					return
				}
				entries := bindings.MapRange()
				for entries.Next() {
					got := columns.MapIndex(entries.Key())
					if !got.IsValid() || got.Pointer() != entries.Value().Pointer() {
						t.Errorf("initial gather changed source binding %q", entries.Key().String())
					}
				}
			})
		}
		for _, list := range b.StringLists {
			if list == nil || list.Values == nil {
				continue
			}
			values := list.Values
			before := *values
			before.Offsets = append([]uint32(nil), values.Offsets...)
			before.Data = append([]byte(nil), values.Data...)
			before.Validity = append(memory.Bitmap(nil), values.Validity...)
			checks = append(checks, func() {
				if !reflect.DeepEqual(*values, before) {
					t.Error("initial gather changed source list child")
				}
			})
		}
		for _, record := range b.Structs {
			if record != nil {
				visit(record.Values)
			}
		}
	}
	for _, b := range batches {
		visit(b)
	}
	return func() {
		t.Helper()
		for _, check := range checks {
			check()
		}
	}
}

type sortInitialTokenProbe struct {
	memory.Manager
	live, peak         int64
	reserves, releases int
}

func (p *sortInitialTokenProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, err := p.Manager.Reserve(ctx, n)
	if err != nil {
		return nil, err
	}
	p.reserves++
	p.live += r.Bytes()
	p.peak = max(p.peak, p.live)
	return &sortInitialProbeReservation{Reservation: r, probe: p}, nil
}

type sortInitialProbeReservation struct {
	memory.Reservation
	probe    *sortInitialTokenProbe
	released bool
}

func (r *sortInitialProbeReservation) Release() {
	if r.released {
		return
	}
	r.released = true
	r.probe.live -= r.Bytes()
	r.probe.releases++
	r.Reservation.Release()
}

type sortInitialEncodeProbe struct {
	memory.Manager
	initial     *sortInitialTokenProbe
	owned       *sortInitialTokenProbe
	calls       int
	liveAtEntry []int64
	failure     error
	cancel      context.CancelFunc
}

func (p *sortInitialEncodeProbe) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	p.calls++
	p.liveAtEntry = append(p.liveAtEntry, p.initial.live)
	if p.cancel != nil {
		p.cancel()
	}
	if p.failure != nil {
		return nil, p.failure
	}
	return p.owned.Reserve(ctx, n)
}
func observeSortInitial(s *externalSortStream) (*sortInitialTokenProbe, *sortInitialEncodeProbe) {
	initial := &sortInitialTokenProbe{Manager: s.sortBudget}
	encode := &sortInitialEncodeProbe{Manager: s.encode, initial: initial, owned: &sortInitialTokenProbe{Manager: s.encode}}
	s.sortBudget, s.encode = initial, encode
	return initial, encode
}

// Observe actual store entry, not merely completed-run counters. The encoded
// payload is independently owned; the gathered graph must already be released.
type sortInitialStoreProbe struct {
	memory.SpillManager
	onWrite       func(memory.SpillObject)
	deleteFailure error
}

func (p *sortInitialStoreProbe) Spill(ctx context.Context, object memory.SpillObject) (memory.SpillHandle, error) {
	if p.onWrite != nil {
		p.onWrite(object)
	}
	return p.SpillManager.Spill(ctx, object)
}
func (p *sortInitialStoreProbe) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	if p.onWrite != nil {
		p.onWrite(object)
	}
	return p.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, object, manager)
}
func (p *sortInitialStoreProbe) Delete(ctx context.Context, handle memory.SpillHandle) error {
	if p.deleteFailure != nil {
		return p.deleteFailure
	}
	return p.SpillManager.Delete(ctx, handle)
}
func observeSortInitialStore(t *testing.T, s *externalSortStream, spy *sortLifecycleStore, initial *sortInitialTokenProbe, encode *sortInitialEncodeProbe) *sortInitialStoreProbe {
	t.Helper()
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	store := &sortInitialStoreProbe{SpillManager: spy, onWrite: func(object memory.SpillObject) {
		if initial.live != 0 {
			t.Errorf("store entered with gathered graph still live: %d", initial.live)
		}
		if encode.owned.live < int64(len(object.Payload)) || encode.owned.live == 0 {
			t.Errorf("store reads unadmitted encoded payload: live=%d payload=%d", encode.owned.live, len(object.Payload))
		}
	}}
	s.store = spillmeta.NewStore(store, s.metadata)
	return store
}

func assertSortInitialClosed(t *testing.T, s *externalSortStream, q *memory.QueryBudget, spy *sortLifecycleStore, directory string) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Error(err)
	}
	if err := s.Close(); err != nil {
		t.Error("repeated close", err)
	}
	if err := q.Close(); err != nil {
		t.Error(err)
	}
	if s.workspace.Used() != 0 || q.Used() != 0 || spy.Used() != 0 {
		t.Errorf("initial gather leaked workspace=%d query=%d spill=%d", s.workspace.Used(), q.Used(), spy.Used())
	}
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			t.Errorf("spill file remains after cleanup: %s", path)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func TestExternalSortInitialRuntimeBindingsBudget(t *testing.T) {
	const limit = int64(128 << 10)
	src := sortInitialAliasSource()
	defer snapshotSortInitialInput(t, src.batches...)()
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, limit, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	b := src.batches[0]
	estimate, err := memory.EstimateBatchRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := sortBindingBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	old := 2*estimate + 16 + 1024 + binding
	minimum := sortInitialDescriptorMinimum(b)
	if old >= limit || minimum <= limit {
		t.Fatalf("fixture does not separate old payload floor (%d) from descriptor minimum (%d), limit=%d", old, minimum, limit)
	}
	t.Logf("fixed limit=%d old payload floor=%d independent descriptor minimum=%d", limit, old, minimum)
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Errorf("runtime descriptor admission must reject before gather/encode, got %v", err)
	}
	if encode.calls != 0 || spy.attempts != 0 || spy.writes != 0 || len(runs.Values) != 0 {
		t.Errorf("rejected initial gather reached encode/store: encode=%d attempts=%d writes=%d runs=%d", encode.calls, spy.attempts, spy.writes, len(runs.Values))
	}
	if src.opens != 1 || src.emitted != 1 || src.closes != 1 || src.emittedBindings != 4098 {
		t.Errorf("source lifecycle/bindings: %+v", src)
	}
	if initial.live != 0 || s.workspace.Used() != 0 {
		t.Errorf("rejection retained an initial token: %d workspace=%d", initial.live, s.workspace.Used())
	}
}

func TestExternalSortInitialInmemoryProjectsRuntimeExtras(t *testing.T) {
	custom := sortInitialAliasSource()
	defer snapshotSortInitialInput(t, custom.batches...)()
	src, err := inmemory.New(custom.schema, custom.batches...)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := src.NewBatchStream(context.Background(), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	view := memory.NewBatch()
	if ok, err := reader.Next(context.Background(), view); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if len(view.Int8s) != 0 || len(view.Uint8s) != 0 || len(view.Int64s) != 1 {
		t.Fatal("inmemory should expose only schema-declared top-level bindings")
	}
	view.ClearColumns()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 128<<10, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if err != nil || len(runs.Values) != 1 || spy.writes != 1 {
		t.Fatalf("projected narrow batch should fit: runs=%d writes=%d err=%v", len(runs.Values), spy.writes, err)
	}
}

func TestExternalSortInitialDeclaredStructBudget(t *testing.T) {
	const columns = 1024
	const limit = int64(640 << 10)
	fields := make([]memory.Field, columns)
	children := memory.NewBatch()
	children.Rows = 1
	for i := range fields {
		name := fmt.Sprintf("child_%04d", i)
		fields[i] = memory.Field{Name: name, Type: memory.TypeInt8}
		children.Int8s[name] = &memory.Int8Vector{Values: []int8{int8(i)}}
	}
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{7}}
	b.Structs["record"] = &memory.StructVector{Rows: 1, Fields: fields, Values: children, Encoding: memory.EncodingView}
	defer snapshotSortInitialInput(t, b)()
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "record", Type: memory.TypeStruct, Children: fields})
	src, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, limit, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	estimate, err := memory.EstimateBatchRangeBytes(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := memory.EstimateColumnarRunRangeBytes(schema, b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := sortBindingBytes(1)
	if err != nil {
		t.Fatal(err)
	}
	old := 2*estimate + 16 + 1024 + binding
	peak := old + encoded + 8 + 4
	frontier := 2 * int64(unsafe.Sizeof([externalSortLevels]externalSortLevel{}))
	oldWithCredits := q.Used() + frontier + peak + ((peak+4095)/4096)*sortWorkspaceCreditOverhead
	minimum := q.Used() + sortInitialDescriptorMinimum(b)
	if oldWithCredits >= limit || minimum <= limit {
		t.Fatalf("fixture must fit old admission including credits but not actual descriptors/Fields: old=%d minimum=%d limit=%d", oldWithCredits, minimum, limit)
	}
	t.Logf("fixed limit=%d old admission plus credits=%d base plus descriptor/Fields minimum=%d", limit, oldWithCredits, minimum)
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Errorf("declared struct metadata must be rejected before gather: %v", err)
	}
	if encode.calls != 0 || spy.attempts != 0 || len(runs.Values) != 0 {
		t.Errorf("struct rejection reached encode/store: encode=%d attempts=%d runs=%d", encode.calls, spy.attempts, len(runs.Values))
	}
	if initial.live != 0 || s.workspace.Used() != 0 {
		t.Error("struct rejection leaked an initial token")
	}
}

func TestExternalSortInitialTokenCoversDescriptorsDuringEncode(t *testing.T) {
	src := sortInitialAliasSource()
	defer snapshotSortInitialInput(t, src.batches...)()
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 16<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	observeSortInitialStore(t, s, spy, initial, encode)
	minimum := sortInitialDescriptorMinimum(src.batches[0]) + int64(unsafe.Sizeof(memory.Uint64Vector{}))
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if err != nil {
		t.Fatal(err)
	}
	if encode.calls != 1 || spy.writes != 1 || len(runs.Values) != 1 {
		t.Fatal("fixture must encode exactly one initial run", encode.calls, spy.writes, len(runs.Values))
	}
	if encode.liveAtEntry[0] < minimum {
		t.Errorf("live initial reservation at encode=%d, below independent descriptor-only minimum=%d", encode.liveAtEntry[0], minimum)
	}
	if initial.reserves == 0 || initial.reserves != initial.releases || initial.live != 0 || s.workspace.Used() != 0 {
		t.Errorf("initial reservation lifetime mismatch: %+v workspace=%d", initial, s.workspace.Used())
	}
	if src.closes != 1 {
		t.Fatal("source close count", src.closes)
	}
}

func TestExternalSortInitialMixedRuntimeExtrasStable(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64})
	src := &sortInitialRuntimeSource{schema: schema}
	type row struct {
		key, id int64
		valid   bool
	}
	var want []row
	for part := 0; part < 5; part++ {
		b := joinGatherTestFixture()
		// The fixture's extra Values map is deliberately not selected by
		// Struct.Fields. It is validated but not part of the gathered graph.
		b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1, 2}, Validity: memory.Bitmap{5}}
		b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(part * 3), int64(part*3 + 1), int64(part*3 + 2)}}
		// Runtime-only native decimal, list, dictionary, nullable struct, and
		// every scalar map remain legal even though only key/id are encoded.
		b.Uint64s["__golume_sort_ordinal"] = &memory.Uint64Vector{Values: []uint64{999, 0, 777}}
		src.batches = append(src.batches, b)
		for i, key := range b.Int64s["key"].Values {
			want = append(want, row{key, b.Int64s["id"].Values[i], b.Int64s["key"].IsValid(i)})
		}
	}
	defer snapshotSortInitialInput(t, src.batches...)()
	sort.SliceStable(want, func(i, j int) bool {
		if want[i].valid != want[j].valid {
			return want[i].valid
		}
		return want[i].valid && want[i].key < want[j].key
	})
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 8<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	s.chunkRows = 2
	dst := memory.NewBatch()
	defer dst.ReleaseOwned()
	var got []row
	for {
		ok, err := s.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if len(dst.Uint64s) != 0 || len(dst.Int8s) != 0 || len(dst.Structs) != 0 || len(dst.Int64s) != 2 {
			t.Fatal("runtime extras escaped declared public output")
		}
		for i, key := range dst.Int64s["key"].Values {
			got = append(got, row{key, dst.Int64s["id"].Values[i], dst.Int64s["key"].IsValid(i)})
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime ordinal collision changed stable declared output:\n got=%v\nwant=%v", got, want)
	}
	if src.opens != 1 || src.emitted != len(src.batches) || src.closes != 1 || spy.writes < len(src.batches) {
		t.Fatal("did not exercise multibatch run generation", src.opens, src.emitted, src.closes, spy.writes)
	}
}

func TestExternalSortInitialFailureCleanup(t *testing.T) {
	for _, mode := range []string{"encode", "store", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			src := sortInitialAliasSource()
			defer snapshotSortInitialInput(t, src.batches...)()
			directory := t.TempDir()
			s, q, spy, ctx := openSortLifecycle(t, 16<<20, src, directory)
			defer assertSortInitialClosed(t, s, q, spy, directory)
			initial, encode := observeSortInitial(s)
			observeSortInitialStore(t, s, spy, initial, encode)
			failure := errors.New("injected initial gather encode failure")
			want := failure
			switch mode {
			case "encode":
				encode.failure = failure
			case "store":
				spy.failWriteAt = 1
				want = spy.failure
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				encode.cancel = cancel
				want = context.Canceled
			}
			runs, err := s.generateRuns(ctx)
			defer releaseSortRuns(&runs)
			if !errors.Is(err, want) {
				t.Errorf("want %v, got %v", want, err)
			}
			if len(runs.Values) != 0 || initial.live != 0 || encode.owned.live != 0 || initial.reserves != initial.releases || s.workspace.Used() != 0 {
				t.Errorf("failed initial run retained ownership: runs=%d token=%+v workspace=%d", len(runs.Values), initial, s.workspace.Used())
			}
			minimum := sortInitialDescriptorMinimum(src.batches[0])
			if encode.calls != 1 || encode.liveAtEntry[0] < minimum {
				t.Errorf("failure did not occur while complete gathered graph was admitted: calls=%d live=%v minimum=%d", encode.calls, encode.liveAtEntry, minimum)
			}
			if src.closes != 1 || spy.writes != 0 || (mode == "store" && spy.attempts != 1) || (mode != "store" && spy.attempts != 0) {
				t.Error("unexpected failure lifecycle", src.closes, spy.writes, spy.attempts)
			}
		})
	}
}

func TestExternalSortInitialFailedDeletionRemainsQueryOwned(t *testing.T) {
	src := sortInitialAliasSource()
	defer snapshotSortInitialInput(t, src.batches...)()
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 16<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	store := observeSortInitialStore(t, s, spy, initial, encode)
	failure := errors.New("persistent initial run deletion failure")
	store.deleteFailure = failure
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if err != nil || len(runs.Values) != 1 || spy.writes != 1 {
		t.Fatal("expected one completed initial run", err, len(runs.Values), spy.writes)
	}
	releaseSortRuns(&runs)
	if err := s.Close(); !errors.Is(err, failure) {
		t.Errorf("close must report deletion failure: %v", err)
	}
	if initial.live != 0 || encode.owned.live != 0 || s.workspace.Used() != 0 {
		t.Error("failed deletion retained gather or encode workspace")
	}
	if q.Used() == 0 || spy.Used() == 0 {
		t.Error("failed deletion dropped query-owned file accounting")
	}
	files := 0
	if err := filepath.WalkDir(directory, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if files != 1 {
		t.Errorf("failed deletion should leave one query-owned file, got %d", files)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if q.Used() != 0 || spy.Used() != 0 {
		t.Error("query close failed to release retained file ownership")
	}
	if src.closes != 1 {
		t.Error("source closed more than once", src.closes)
	}
}

func TestExternalSortInitialOpaqueExtraRejectedBeforeEncode(t *testing.T) {
	builder := array.NewInt64Builder(arrowmem.DefaultAllocator)
	builder.Append(17)
	value := builder.NewArray()
	builder.Release()
	defer value.Release()
	b := memory.NewBatch()
	b.Rows = 1
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
	b.Opaque["runtime_only"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeInt64, Value: value}
	src := &sortInitialRuntimeSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}), batches: []*memory.Batch{b}}
	defer snapshotSortInitialInput(t, b)()
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 2<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "opaque") {
		t.Errorf("bounded initial gather must explicitly reject opaque runtime extras, got %v", err)
	}
	if encode.calls != 0 || spy.attempts != 0 || initial.reserves != 0 || initial.live != 0 || len(runs.Values) != 0 {
		t.Error("opaque rejection occurred after initial allocation or encode/store", encode.calls, spy.attempts, initial.reserves)
	}
	if src.closes != 1 || value.Len() != 1 {
		t.Error("opaque rejection damaged source lifecycle")
	}
}

func TestExternalSortInitialRejectsMalformedRuntimeGraph(t *testing.T) {
	// A regression must fail the test rather than overflow the main test
	// process's stack. Only the cyclic cases need a bounded subprocess.
	if mode := os.Getenv("GOLUME_SORT_INITIAL_GRAPH_HELPER"); mode != "" {
		debug.SetMaxStack(1 << 20)
		testSortInitialBadGraph(t, "cycle", mode)
		return
	}
	for _, entry := range []string{"generate", "direct"} {
		t.Run("cycle/"+entry, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExternalSortInitialRejectsMalformedRuntimeGraph$", "-test.count=1")
			cmd.Env = append(os.Environ(), "GOLUME_SORT_INITIAL_GRAPH_HELPER="+entry)
			if output, err := cmd.CombinedOutput(); err != nil {
				if len(output) > 4096 {
					output = output[:4096]
				}
				t.Fatalf("cyclic runtime graph was not rejected safely: %v\n%s", err, output)
			}
		})
		t.Run("depth/"+entry, func(t *testing.T) { testSortInitialBadGraph(t, "depth", entry) })
	}
}

func testSortInitialBadGraph(t *testing.T, shape, entry string) {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1}}
	if shape == "cycle" {
		b.Structs["hidden_cycle"] = &memory.StructVector{Rows: 2, Values: b}
	} else {
		at := b
		for i := 0; i < 65; i++ {
			next := memory.NewBatch()
			next.Rows = 2
			at.Structs["hidden_depth"] = &memory.StructVector{Rows: 2, Values: next}
			at = next
		}
	}
	src := &sortInitialRuntimeSource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}), batches: []*memory.Batch{b}}
	defer snapshotSortInitialInput(t, b)()
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 2<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	var err error
	if entry == "generate" {
		s.chunkRows = 2 // force the shrinking estimator path before makeInitialRun
		runs, runErr := s.generateRuns(ctx)
		defer releaseSortRuns(&runs)
		err = runErr
		if len(runs.Values) != 0 || src.closes != 1 {
			t.Error("malformed graph retained runs or child", len(runs.Values), src.closes)
		}
	} else {
		run, runErr := s.makeInitialRun(ctx, b, 0, b.Rows, 0)
		defer run.segments.Release()
		err = runErr
		if len(run.segments.Values) != 0 || src.opens != 0 {
			t.Error("direct malformed graph created a run or opened source")
		}
	}
	wantReason := "cyclic"
	if shape == "depth" {
		wantReason = "nesting"
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), wantReason) {
		t.Errorf("malformed runtime graph must report %s before the recursive range estimator, got %v", wantReason, err)
	}
	if initial.reserves != 0 || initial.live != 0 || encode.calls != 0 || spy.attempts != 0 || s.workspace.Used() != 0 {
		t.Error("bad graph reached allocation, encoding or storage", initial.reserves, encode.calls, spy.attempts)
	}
}

// An owner-aware child can lend its entire graph and restore the caller's
// original containers when its borrow ends, even if Next reports an error.
// The retained destination lets the test inspect generateRuns' final defer.
type sortInitialRestoringSource struct {
	*sortInitialRuntimeSource
	nextError, closeError error
	eof                   bool
	destination           *memory.Batch
	saved                 memory.Batch
	closeSawEmpty         bool
	restored              bool
}

func (s *sortInitialRestoringSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.opens++
	return &sortInitialRestoringStream{owner: s}, nil
}

type sortInitialRestoringStream struct {
	owner  *sortInitialRestoringSource
	closed bool
}

func (s *sortInitialRestoringStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.owner.nexts++
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.owner.destination != nil {
		return false, fmt.Errorf("restoring child must not be advanced after its terminal result")
	}
	s.owner.destination = dst
	s.owner.saved = *dst
	*dst = *s.owner.batches[0]
	s.owner.emitted++
	if s.owner.nextError != nil {
		return false, s.owner.nextError
	}
	return !s.owner.eof, nil
}

func (s *sortInitialRestoringStream) Close() error {
	s.owner.closes++
	s.closed = true
	if s.owner.destination != nil {
		s.owner.closeSawEmpty = reflect.ValueOf(*s.owner.destination).IsZero()
		*s.owner.destination = s.owner.saved
		s.owner.restored = true
	}
	return s.owner.closeError
}

func TestExternalSortInitialDetachesFailedChildBorrow(t *testing.T) {
	if mode := os.Getenv("GOLUME_SORT_INITIAL_CHILD_HELPER"); mode != "" {
		debug.SetMaxStack(1 << 20)
		testSortInitialFailedChildBorrow(t, mode)
		return
	}
	for _, mode := range []string{"next_error", "next_and_close_error", "validation_error", "eof", "eof_close_error"} {
		t.Run(mode, func(t *testing.T) {
			// Both the error return and cleanup own a cyclic borrowed graph.
			// Keep a regression's stack overflow out of the parent test process.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExternalSortInitialDetachesFailedChildBorrow$", "-test.count=1")
			cmd.Env = append(os.Environ(), "GOLUME_SORT_INITIAL_CHILD_HELPER="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				if len(output) > 4096 {
					output = output[:4096]
				}
				t.Fatalf("failed child borrow was not detached safely: %v\n%s", err, output)
			}
		})
	}
}

func testSortInitialFailedChildBorrow(t *testing.T, mode string) {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["key"] = &memory.Int64Vector{Values: []int64{2, 1}}
	b.Structs["hidden_cycle"] = &memory.StructVector{Rows: 2, Values: b}
	defer snapshotSortInitialInput(t, b)()
	src := &sortInitialRestoringSource{sortInitialRuntimeSource: &sortInitialRuntimeSource{
		schema:  memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}),
		batches: []*memory.Batch{b},
	}}
	nextFailure := errors.New("injected child Next failure after binding a cycle")
	closeFailure := errors.New("injected child Close failure after restoring containers")
	switch mode {
	case "next_error":
		src.nextError = nextFailure
	case "next_and_close_error":
		src.nextError, src.closeError = nextFailure, closeFailure
	case "validation_error":
		src.closeError = closeFailure
	case "eof":
		src.eof = true
	case "eof_close_error":
		src.eof, src.closeError = true, closeFailure
	default:
		t.Fatalf("unknown restoring child fixture %q", mode)
	}
	directory := t.TempDir()
	s, q, spy, ctx := openSortLifecycle(t, 2<<20, src, directory)
	defer assertSortInitialClosed(t, s, q, spy, directory)
	initial, encode := observeSortInitial(s)
	runs, err := s.generateRuns(ctx)
	defer releaseSortRuns(&runs)
	if src.nextError != nil && !errors.Is(err, nextFailure) {
		t.Errorf("child Next error was lost: %v", err)
	}
	if src.closeError != nil && !errors.Is(err, closeFailure) {
		t.Errorf("child Close error was lost: %v", err)
	}
	if mode == "validation_error" && (err == nil || !strings.Contains(err.Error(), "cyclic")) {
		t.Errorf("validation error was lost when child Close failed: %v", err)
	}
	if mode == "eof" && err != nil {
		t.Errorf("clean child EOF must succeed: %v", err)
	}
	if (src.nextError != nil || src.eof) && err != nil && strings.Contains(err.Error(), "cyclic") {
		t.Errorf("terminal child result must bypass runtime graph validation: %v", err)
	}
	if src.opens != 1 || src.nexts != 1 || src.emitted != 1 || src.closes != 1 {
		t.Errorf("unexpected child lifecycle: opens=%d nexts=%d emitted=%d closes=%d", src.opens, src.nexts, src.emitted, src.closes)
	}
	if !src.closeSawEmpty {
		t.Error("child Close must see a completely detached header, before recursive cleanup")
	}
	if !src.restored || src.destination == nil {
		t.Fatal("fixture did not restore the saved caller containers")
	}
	saved := reflect.ValueOf(src.saved)
	containers := 0
	for i := 0; i < saved.NumField(); i++ {
		if saved.Field(i).Kind() != reflect.Map {
			continue
		}
		if saved.Field(i).IsNil() || saved.Field(i).Len() != 0 {
			t.Error("saved caller maps must be allocated and empty")
		}
		containers++
	}
	if containers == 0 || src.saved.Rows != 0 {
		t.Fatal("fixture did not retain original empty caller containers")
	}
	if !reflect.ValueOf(*src.destination).IsZero() {
		t.Error("generateRuns final defer retained the containers restored by child Close")
	}
	if initial.reserves != 0 || initial.live != 0 || encode.calls != 0 || spy.attempts != 0 || spy.writes != 0 || len(runs.Values) != 0 || s.workspace.Used() != 0 {
		t.Error("terminal child borrow reached gather allocation, encoding or storage", initial.reserves, encode.calls, spy.attempts, len(runs.Values))
	}
}
