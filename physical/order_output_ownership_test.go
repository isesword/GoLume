package physical

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"reflect"
	"testing"

	"gocolumn/internal/spillmeta"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// These tests deliberately do not name the output owner or its fields. They
// exercise the borrowed Batch contract and can also run against the old sorter.
func sortOutputOwnershipSource(t *testing.T, rows int) (*inmemory.Source, func()) {
	t.Helper()
	children := []memory.Field{{Name: "number", Type: memory.TypeInt64}, {Name: "text", Type: memory.TypeString}}
	schema := memory.NewSchema(
		memory.Field{Name: "key", Type: memory.TypeInt64},
		memory.Field{Name: "text", Type: memory.TypeString},
		memory.Field{Name: "dictionary", Type: memory.TypeString, Encoding: memory.EncodingDictionary},
		memory.Field{Name: "binary", Type: memory.TypeBinary, Encoding: memory.EncodingDictionary},
		memory.ListStringField("list", false, false),
		memory.Field{Name: "record", Type: memory.TypeStruct, Children: children},
	)
	var batches []*memory.Batch
	for offset := 0; offset < rows; offset += 11 {
		n := min(11, rows-offset)
		b, child := memory.NewBatch(), memory.NewBatch()
		b.Rows, child.Rows = n, n
		b.Int64s["key"] = &memory.Int64Vector{}
		b.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0}}
		b.DictStrings["dictionary"] = &memory.DictionaryStringVector{Offsets: []uint32{0, 3, 6, 11}, Data: []byte("redblugreen"), DictionaryDomain: "source-strings", Encoding: memory.EncodingDictionary}
		b.DictStrings["binary"] = &memory.DictionaryStringVector{Offsets: []uint32{0, 2, 3, 5}, Data: []byte{0, 255, 128, 'a', 0}, DictionaryDomain: "source-binary", Encoding: memory.EncodingDictionary}
		b.StringLists["list"] = &memory.StringListVector{Offsets: []uint32{0}, Values: &memory.StringViewVector{Offsets: []uint32{0}}}
		child.Int64s["number"] = &memory.Int64Vector{}
		child.Strings["text"] = &memory.StringViewVector{Offsets: []uint32{0}}
		b.Structs["record"] = &memory.StructVector{Rows: n, Fields: children, Values: child}
		appendText := func(v *memory.StringViewVector, value string) {
			v.Data = append(v.Data, value...)
			v.Offsets = append(v.Offsets, uint32(len(v.Data)))
		}
		for i := 0; i < n; i++ {
			key := rows - 1 - offset - i
			label := fmt.Sprintf("row-%03d", key)
			b.Int64s["key"].Values = append(b.Int64s["key"].Values, int64(key))
			appendText(b.Strings["text"], label)
			b.DictStrings["dictionary"].Indices = append(b.DictStrings["dictionary"].Indices, uint32(key%3))
			b.DictStrings["binary"].Indices = append(b.DictStrings["binary"].Indices, uint32(key%3))
			list := b.StringLists["list"]
			appendText(list.Values, label)
			appendText(list.Values, "tail")
			list.Offsets = append(list.Offsets, uint32(list.Values.Len()))
			child.Int64s["number"].Values = append(child.Int64s["number"].Values, int64(key*7))
			appendText(child.Strings["text"], label)
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func() []byte {
		data, err := json.Marshal(struct {
			Fields  []memory.Field
			Batches []*memory.Batch
		}{src.Schema().Fields, batches})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := snapshot()
	return src, func() {
		t.Helper()
		if !bytes.Equal(snapshot(), before) {
			t.Error("sort output destruction modified source schema or source vector/dictionary storage")
		}
	}
}

func checkSortOutputOwnershipRows(t *testing.T, b *memory.Batch, start int) int {
	t.Helper()
	if b.Rows <= 0 || b.Int64s["key"] == nil || b.Strings["text"] == nil || b.DictStrings["dictionary"] == nil || b.DictStrings["binary"] == nil || b.StringLists["list"] == nil || b.Structs["record"] == nil {
		t.Fatal("borrowed output lost a required vector")
	}
	strings := []string{"red", "blu", "green"}
	binary := [][]byte{{0, 255}, {128}, {'a', 0}}
	for i := 0; i < b.Rows; i++ {
		key := start + i
		label := fmt.Sprintf("row-%03d", key)
		dict, bin := b.DictStrings["dictionary"], b.DictStrings["binary"]
		if b.Int64s["key"].Values[i] != int64(key) || string(b.Strings["text"].ValueBytes(i)) != label || string(dict.DictionaryValue(dict.Indices[i])) != strings[key%3] || !bytes.Equal(bin.DictionaryValue(bin.Indices[i]), binary[key%3]) {
			t.Fatalf("live output row %d changed or was sorted incorrectly", key)
		}
		list, record := b.StringLists["list"], b.Structs["record"]
		lo, hi, ok := list.ValueRange(i)
		if !ok || hi-lo != 2 || string(list.Values.ValueBytes(lo)) != label || string(list.Values.ValueBytes(lo+1)) != "tail" {
			t.Fatalf("live list row %d changed", key)
		}
		if len(record.Fields) != 2 || record.Fields[0].Name != "number" || record.Values == nil || record.Values.Int64s["number"].Values[i] != int64(key*7) || string(record.Values.Strings["text"].ValueBytes(i)) != label {
			t.Fatalf("live struct row %d changed", key)
		}
	}
	return start + b.Rows
}

type sortOutputOwnershipAliases struct {
	maps    []reflect.Value
	vectors []reflect.Value
	fields  [][]memory.Field
}

func captureSortOutputOwnershipAliases(b *memory.Batch) *sortOutputOwnershipAliases {
	a := &sortOutputOwnershipAliases{}
	var visit func(*memory.Batch)
	visit = func(batch *memory.Batch) {
		v := reflect.ValueOf(*batch)
		for i := 0; i < v.NumField(); i++ {
			m := v.Field(i)
			if m.Kind() != reflect.Map {
				continue
			}
			a.maps = append(a.maps, m)
			for _, key := range m.MapKeys() {
				vector := m.MapIndex(key)
				if !vector.IsNil() {
					a.vectors = append(a.vectors, vector.Elem())
				}
			}
		}
		for _, list := range batch.StringLists {
			if list != nil && list.Values != nil {
				a.vectors = append(a.vectors, reflect.ValueOf(list.Values).Elem())
			}
		}
		for _, record := range batch.Structs {
			if record != nil {
				a.fields = append(a.fields, record.Fields)
				if record.Values != nil {
					visit(record.Values)
				}
			}
		}
	}
	visit(b)
	return a
}

func (a *sortOutputOwnershipAliases) checkCleared(t *testing.T) {
	t.Helper()
	for _, m := range a.maps {
		if m.Len() != 0 {
			t.Errorf("ended output still retains %s bindings", m.Type())
		}
	}
	for _, v := range a.vectors {
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			switch f.Kind() {
			case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Interface:
				if !f.IsNil() {
					t.Errorf("ended output retains %s.%s storage", v.Type(), v.Type().Field(i).Name)
				}
			case reflect.String:
				if f.Len() != 0 {
					t.Errorf("ended output retains %s.%s identity", v.Type(), v.Type().Field(i).Name)
				}
			}
		}
	}
	for _, fields := range a.fields {
		for i := range fields {
			if !reflect.DeepEqual(fields[i], memory.Field{}) {
				t.Errorf("ended output retains Struct.Fields[%d] references", i)
			}
		}
	}
}

// Insert through the saved maps, rather than merely comparing their contents:
// a fresh empty map is not a restoration of the caller's original container.
func checkSortOutputOriginalContainers(t *testing.T, dst *memory.Batch, original memory.Batch) {
	t.Helper()
	if dst.Rows != 0 {
		t.Errorf("ended destination has %d rows", dst.Rows)
	}
	old, current := reflect.ValueOf(original), reflect.ValueOf(*dst)
	for i := 0; i < old.NumField(); i++ {
		m := old.Field(i)
		if m.Kind() != reflect.Map {
			continue
		}
		if current.Field(i).Len() != 0 {
			t.Errorf("ended destination %s is not empty", old.Type().Field(i).Name)
		}
		key := reflect.ValueOf("original-container-marker")
		marker := reflect.New(m.Type().Elem().Elem())
		m.SetMapIndex(key, marker)
		got := current.Field(i).MapIndex(key)
		if !got.IsValid() || got.Pointer() != marker.Pointer() {
			t.Errorf("destination %s did not restore the original map", old.Type().Field(i).Name)
		}
		m.SetMapIndex(key, reflect.Value{})
	}
}

// Observe actual query-credit release, not a post-hoc zero balance. Wrapping
// the workspace parent also works when its child tokens have a concrete type.
type sortOutputReleaseObserver struct {
	memory.Manager
	beforeRelease func()
	observations  int
}
type sortOutputObservedReservation struct {
	memory.Reservation
	observer *sortOutputReleaseObserver
}

func (m *sortOutputReleaseObserver) Reserve(ctx context.Context, bytes int64) (memory.Reservation, error) {
	r, err := m.Manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	return &sortOutputObservedReservation{Reservation: r, observer: m}, nil
}
func (r *sortOutputObservedReservation) Release() {
	if check := r.observer.beforeRelease; check != nil {
		r.observer.beforeRelease = nil
		r.observer.observations++
		check()
	}
	r.Reservation.Release()
}

func observeSortOutputRelease(s *externalSortStream) *sortOutputReleaseObserver {
	o := &sortOutputReleaseObserver{Manager: s.workspace.parent}
	s.workspace.parent = o
	return o
}

func checkSortOutputFilesGone(t *testing.T, dir string, q *memory.QueryBudget, store *sortLifecycleStore) {
	t.Helper()
	if q.Used() != 0 || store.Used() != 0 || len(store.live) != 0 {
		t.Errorf("output cleanup retained query=%d spill=%d files=%d", q.Used(), store.Used(), len(store.live))
	}
	checkSortOutputDiskEmpty(t, dir)
}
func checkSortOutputDiskEmpty(t *testing.T, dir string) {
	t.Helper()
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			t.Errorf("output cleanup left spill file %s", path)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func TestExternalSortOutputOwnershipDestinations(t *testing.T) {
	for _, mode := range []string{"alternating", "same", "caller clears"} {
		t.Run(mode, func(t *testing.T) {
			const rows = 37
			src, unchanged := sortOutputOwnershipSource(t, rows)
			defer unchanged()
			dir := t.TempDir()
			s, q, store, ctx := openSortLifecycle(t, 2<<20, src, dir)
			s.chunkRows, s.runBytes = 4, 4096
			observer := observeSortOutputRelease(s)
			a, b := memory.NewBatch(), memory.NewBatch()
			originalA, originalB := *a, *b
			var previous *sortOutputOwnershipAliases
			var previousDst *memory.Batch
			seen, calls := 0, 0
			for {
				dst := a
				if mode == "alternating" && calls%2 != 0 {
					dst = b
				}
				if mode == "caller clears" && previousDst != nil {
					previousDst.ClearColumns()
					// The caller explicitly detached these descriptors. The
					// owner need not zero arbitrary aliases removed by its user.
					previous = nil
				}
				if previous != nil {
					old := previous
					observer.beforeRelease = func() { old.checkCleared(t) }
				}
				ok, err := s.Next(ctx, dst)
				if err != nil {
					t.Fatal(err)
				}
				if previous != nil {
					previous.checkCleared(t)
				}
				if previousDst != nil && previousDst != dst {
					original := originalA
					if previousDst == b {
						original = originalB
					}
					checkSortOutputOriginalContainers(t, previousDst, original)
				}
				calls++
				if !ok {
					break
				}
				seen = checkSortOutputOwnershipRows(t, dst, seen)
				previous, previousDst = captureSortOutputOwnershipAliases(dst), dst
			}
			if seen != rows || calls < 4 {
				t.Fatalf("rows=%d calls=%d: did not exercise repeated destinations", seen, calls)
			}
			checkSortOutputOriginalContainers(t, a, originalA)
			checkSortOutputOriginalContainers(t, b, originalB)
			if mode != "caller clears" && observer.observations == 0 {
				t.Error("did not observe a charged release after borrowing output")
			}
			if ok, err := s.Next(ctx, a); ok || err != nil {
				t.Fatalf("repeated EOF: ok=%v err=%v", ok, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			checkSortOutputFilesGone(t, dir, q, store)
		})
	}
}

func sortOutputNextWithoutPanic(t *testing.T, s *externalSortStream, ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("Next panicked instead of terminating the borrow: %v", recovered)
		}
	}()
	return s.Next(ctx, dst)
}

func TestExternalSortOutputOwnershipTerminalPaths(t *testing.T) {
	for _, mode := range []string{"close", "cancel", "nil context", "nil destination"} {
		t.Run(mode, func(t *testing.T) {
			src, unchanged := sortOutputOwnershipSource(t, 37)
			defer unchanged()
			dir := t.TempDir()
			s, q, store, ctx := openSortLifecycle(t, 2<<20, src, dir)
			s.chunkRows, s.runBytes = 4, 4096
			observer := observeSortOutputRelease(s)
			dst := memory.NewBatch()
			original := *dst
			if ok, err := s.Next(ctx, dst); !ok || err != nil {
				t.Fatalf("first: ok=%v err=%v", ok, err)
			}
			checkSortOutputOwnershipRows(t, dst, 0)
			old := captureSortOutputOwnershipAliases(dst)
			observer.beforeRelease = func() { old.checkCleared(t) }
			var terminal error
			switch mode {
			case "close":
				terminal = s.Close()
				if terminal != nil {
					t.Fatal(terminal)
				}
			case "cancel":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				_, terminal = s.Next(canceled, memory.NewBatch())
				if !errors.Is(terminal, context.Canceled) {
					t.Fatalf("cancel: %v", terminal)
				}
			case "nil context":
				_, terminal = sortOutputNextWithoutPanic(t, s, nil, memory.NewBatch())
			case "nil destination":
				_, terminal = sortOutputNextWithoutPanic(t, s, ctx, nil)
			}
			old.checkCleared(t)
			checkSortOutputOriginalContainers(t, dst, original)
			if observer.observations == 0 {
				t.Error("terminal path never released charged workspace")
			}
			if mode != "close" {
				if terminal == nil {
					t.Fatal("invalid or canceled Next did not return a terminal error")
				}
				for i := 0; i < 2; i++ {
					if ok, err := s.Next(ctx, memory.NewBatch()); ok || err != terminal {
						t.Fatalf("terminal error changed: ok=%v got=%v want=%v", ok, err, terminal)
					}
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			checkSortOutputFilesGone(t, dir, q, store)
		})
	}
}

// Arm only after a successful output, so restoration errors exercise both the
// previous borrowed result and a partially built subsequent merge result.
type sortOutputFailureStore struct {
	*sortLifecycleStore
	restoreErr error
	deleteErr  error
	deleteID   string
	deleteAll  bool
}

func (s *sortOutputFailureStore) Restore(ctx context.Context, h memory.SpillHandle, schema [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	if s.restoreErr != nil {
		return nil, s.restoreErr
	}
	return s.sortLifecycleStore.Restore(ctx, h, schema, manager)
}
func (s *sortOutputFailureStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.deleteErr != nil && (s.deleteAll || h.ID() == s.deleteID) {
		return s.deleteErr
	}
	return s.sortLifecycleStore.Delete(ctx, h)
}

func TestExternalSortOutputOwnershipFailureAfterOutput(t *testing.T) {
	for _, mode := range []string{"restore", "restore and cleanup", "EOF cleanup"} {
		t.Run(mode, func(t *testing.T) {
			const rows = 97
			src, unchanged := sortOutputOwnershipSource(t, rows)
			defer unchanged()
			dir := t.TempDir()
			s, q, store, ctx := openSortLifecycle(t, 2<<20, src, dir)
			s.chunkRows, s.runBytes = 3, 4096
			if err := s.store.Close(); err != nil {
				t.Fatal(err)
			}
			faults := &sortOutputFailureStore{sortLifecycleStore: store}
			s.store = spillmeta.NewStore(faults, s.metadata)
			observer := observeSortOutputRelease(s)
			dst := memory.NewBatch()
			original := *dst
			if ok, err := s.Next(ctx, dst); !ok || err != nil {
				t.Fatalf("first: ok=%v err=%v", ok, err)
			}
			seen := checkSortOutputOwnershipRows(t, dst, 0)
			primary, cleanup := errors.New("restore after output"), errors.New("delete after output")
			if mode != "EOF cleanup" {
				faults.restoreErr = primary
			}
			if mode == "restore and cleanup" {
				faults.deleteErr, faults.deleteAll = cleanup, true
			}
			if mode == "EOF cleanup" {
				// Retain one operator-owned file that is never a merge input.
				// Its deletion fails only when the fully drained stream closes.
				h, err := s.store.Spill(ctx, memory.SpillObject{Kind: memory.SpillColumnarRun, Codec: "cleanup-sentinel", Payload: []byte("still query-owned")})
				if err != nil {
					t.Fatal(err)
				}
				faults.deleteErr, faults.deleteID = cleanup, h.ID()
			}
			var terminal error
			for calls := 0; calls <= rows; calls++ {
				old := captureSortOutputOwnershipAliases(dst)
				observer.beforeRelease = func() { old.checkCleared(t) }
				ok, err := s.Next(ctx, dst)
				old.checkCleared(t)
				if err != nil {
					if ok {
						t.Fatal("failed Next exposed a partial batch")
					}
					terminal = err
					break
				}
				if !ok {
					t.Fatal("injected failure was lost at EOF")
				}
				seen = checkSortOutputOwnershipRows(t, dst, seen)
			}
			if terminal == nil {
				t.Fatal("restoration failure was not reached")
			}
			if mode != "EOF cleanup" && !errors.Is(terminal, primary) {
				t.Fatalf("lost primary restoration failure: %v", terminal)
			}
			if mode != "restore" && !errors.Is(terminal, cleanup) {
				t.Fatalf("lost cleanup failure: %v", terminal)
			}
			if mode == "EOF cleanup" && seen != rows {
				t.Fatalf("cleanup error preceded EOF: rows=%d want=%d", seen, rows)
			}
			checkSortOutputOriginalContainers(t, dst, original)
			for i := 0; i < 3; i++ {
				if ok, err := s.Next(ctx, dst); ok || err != terminal {
					t.Fatalf("terminal error is not stable: ok=%v got=%v want=%v", ok, err, terminal)
				}
			}
			if s.workspace.Used() != 0 || observer.observations == 0 {
				t.Errorf("failed output retained workspace=%d observed releases=%d", s.workspace.Used(), observer.observations)
			}
			for i := 0; i < 2; i++ {
				if err := s.Close(); err != nil && !errors.Is(err, cleanup) {
					t.Fatal(err)
				}
			}
			if mode == "restore" {
				checkSortOutputFilesGone(t, dir, q, store)
			} else {
				// A persistent delete failure must remain charged to the
				// query store, even though the operator released its buffers.
				if q.Used() == 0 || store.Used() == 0 || len(store.live) == 0 {
					t.Error("failed deletion lost file ownership or accounting")
				}
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 || store.Used() != 0 {
					t.Errorf("query close retained memory=%d spill=%d", q.Used(), store.Used())
				}
				// Query.Close bypasses the injected wrapper and its live map.
				checkSortOutputDiskEmpty(t, dir)
			}
		})
	}
}
