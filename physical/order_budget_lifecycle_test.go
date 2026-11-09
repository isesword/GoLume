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
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type sortLifecycleStore struct {
	*memory.QuerySpillStore
	live                            map[string]struct{}
	peak, writes, attempts, deletes int
	failWriteAt, failDeleteAfter    int
	failure                         error
	cancelAfter                     int
	cancel                          context.CancelFunc
}

func (s *sortLifecycleStore) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	s.attempts++
	if s.failWriteAt > 0 && s.attempts == s.failWriteAt {
		return nil, s.failure
	}
	h, err := s.QuerySpillStore.SpillWithMetadata(ctx, object, manager)
	if err == nil {
		s.writes++
		s.live[h.ID()] = struct{}{}
		s.peak = max(s.peak, len(s.live))
		if s.cancelAfter > 0 && s.writes == s.cancelAfter {
			s.cancel()
		}
	}
	return h, err
}
func (s *sortLifecycleStore) Delete(ctx context.Context, h memory.SpillHandle) error {
	if s.failDeleteAfter > 0 && s.deletes >= s.failDeleteAfter {
		return s.failure
	}
	if err := s.QuerySpillStore.Delete(ctx, h); err != nil {
		return err
	}
	if _, ok := s.live[h.ID()]; ok {
		delete(s.live, h.ID())
		s.deletes++
	}
	return nil
}

func openSortLifecycle(t *testing.T, hard int64, src source.Source, dir string) (*externalSortStream, *memory.QueryBudget, *sortLifecycleStore, context.Context) {
	t.Helper()
	root := memory.NewEngineBudget(hard)
	q, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 128 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	owner, err := NewSortSourceWithOptions(src, []SortKey{{Column: "key", NullsLast: true}}, root, SortSourceOptions{Spillable: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := memory.WithQueryBudget(context.Background(), q)
	s, err := newExternalSortStream(ctx, owner.(*SortSource), testReadOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	base, err := q.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	spy := &sortLifecycleStore{QuerySpillStore: base.(*memory.QuerySpillStore), live: make(map[string]struct{}), failure: errors.New("injected sort spill failure")}
	if err = s.store.Close(); err != nil {
		t.Fatal(err)
	}
	s.store = spillmeta.NewStore(spy, s.metadata)
	return s, q, spy, ctx
}

func TestExternalSortIncrementalManyBatchesLongPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("d", 120))
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	const rows = 65536
	s, q, spy, ctx := openSortLifecycle(t, 512<<10, externalSortTestSource(t, rows, 64), dir)
	b := memory.NewBatch()
	defer b.ReleaseOwned()
	seen := 0
	var lastKey, lastID int64
	for {
		ok, err := s.Next(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i, key := range b.Int64s["key"].Values {
			id := b.Int64s["id"].Values[i]
			if seen > 0 && (key < lastKey || key == lastKey && id < lastID) {
				t.Fatal("unstable order", seen, key, id, lastKey, lastID)
			}
			seen++
			lastKey, lastID = key, id
		}
	}
	if seen != rows || spy.writes <= 1024 || spy.peak >= 128 {
		t.Fatal(seen, spy.writes, spy.peak)
	}
	if q.Used() != 0 || spy.Used() != 0 || len(spy.live) != 0 {
		t.Fatal(q.Used(), spy.Used(), len(spy.live))
	}
	t.Logf("writes=%d peak live spill handles=%d budget peak=%d", spy.writes, spy.peak, q.Snapshot().PeakBytes)
}

func TestExternalSortIncrementalNullableStableValues(t *testing.T) {
	const rows = 8192
	type row struct {
		key, id int64
		valid   bool
	}
	want := make([]row, 0, rows)
	var batches []*memory.Batch
	for offset := 0; offset < rows; offset += 73 {
		n := min(73, rows-offset)
		b := memory.NewBatch()
		b.Rows = n
		keys := &memory.Int64Vector{Values: make([]int64, n), Validity: memory.EnsureBitmap(nil, n)}
		ids := &memory.Int64Vector{Values: make([]int64, n)}
		values := &memory.StringViewVector{Offsets: make([]uint32, n+1)}
		for i := 0; i < n; i++ {
			id := int64(offset + i)
			key := (id * 17) % 11
			valid := id%9 != 0
			keys.Values[i] = key
			ids.Values[i] = id
			if valid {
				memory.Set(keys.Validity, i)
			}
			values.Data = append(values.Data, fmt.Sprintf("row-%d", id)...)
			values.Offsets[i+1] = uint32(len(values.Data))
			want = append(want, row{key, id, valid})
		}
		b.Int64s["key"], b.Int64s["id"], b.Strings["value"] = keys, ids, values
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(want, func(i, j int) bool {
		if want[i].valid != want[j].valid {
			return want[i].valid
		}
		return want[i].valid && want[i].key < want[j].key
	})
	s, q, _, ctx := openSortLifecycle(t, 256<<10, src, t.TempDir())
	b := memory.NewBatch()
	defer b.ReleaseOwned()
	seen := 0
	for {
		ok, err := s.Next(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for i := 0; i < b.Rows; i++ {
			expected := want[seen]
			got := row{b.Int64s["key"].Values[i], b.Int64s["id"].Values[i], b.Int64s["key"].IsValid(i)}
			if got.id != expected.id || got.valid != expected.valid || got.valid && got.key != expected.key || string(b.Strings["value"].ValueBytes(i)) != fmt.Sprintf("row-%d", expected.id) {
				t.Fatalf("row %d got=%+v want=%+v", seen, got, expected)
			}
			seen++
		}
	}
	if seen != rows || q.Used() != 0 || s.mergeLevels < 2 {
		t.Fatal(seen, q.Used(), s.mergeLevels)
	}
}

func TestExternalSortIncrementalWriteDeleteCancelFailure(t *testing.T) {
	for _, mode := range []string{"write", "delete", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, q, spy, ctx := openSortLifecycle(t, 512<<10, externalSortTestSource(t, 8192, 128), t.TempDir())
			s.chunkRows, s.runBytes = 128, 4096
			want := spy.failure
			switch mode {
			case "write":
				spy.failWriteAt = 12
			case "delete":
				spy.failDeleteAfter = 22
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				spy.cancelAfter, spy.cancel = 12, cancel
				want = context.Canceled
			}
			_, err := s.Next(ctx, memory.NewBatch())
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			if _, err = s.Next(ctx, memory.NewBatch()); !errors.Is(err, want) {
				t.Fatal("failure was not terminal", err)
			}
			if spy.writes < 8 {
				t.Fatal("did not exercise intermediate merging", spy.writes)
			}
			if s.workspace.Used() != 0 {
				t.Fatal("live work buffers after abort", s.workspace.Used())
			}
			if mode == "delete" {
				if spy.deletes != 22 || spy.writes <= 32 || q.Used() == 0 || spy.Used() == 0 {
					t.Fatal("failed files lost query ownership", spy.deletes, q.Used(), spy.Used())
				}
			} else if q.Used() != 0 || spy.Used() != 0 {
				t.Fatal(q.Used(), spy.Used())
			}
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 || spy.Used() != 0 {
				t.Fatal("query cleanup leaked", q.Used(), spy.Used())
			}
		})
	}
}

func TestExternalSortLoadedSegmentReleasesReferencesNotDecodedBatch(t *testing.T) {
	s, q, spy, ctx := openSortLifecycle(t, 128<<10, externalSortTestSource(t, 32, 32), t.TempDir())
	runs, err := s.generateRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseSortRuns(&runs)
	if len(runs.Values) != 1 || len(runs.Values[0].segments.Values) != 1 {
		t.Fatal("expected one run")
	}
	before := q.Snapshot().Operators["ExternalSortMetadata"]
	cursors, _, err := s.openCursors(ctx, runs.Values)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Values[0].segments.Values) != 0 || spy.Used() != 0 || len(spy.live) != 0 {
		t.Fatal("consumed file or array still owned")
	}
	if cursors[0].decoded == nil || cursors[0].decoded.Batch.Rows != 32 || s.workspace.Used() == 0 {
		t.Fatal("decoded batch lost ownership")
	}
	if q.Snapshot().Operators["ExternalSortMetadata"] >= before {
		t.Fatal("deleted metadata still charged")
	}
	if s.workspace.Limit() < s.workspace.Used() {
		t.Fatal("unbacked decoded allocation")
	}
	for _, c := range cursors {
		c.close()
	}
	releaseSortRuns(&runs)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}

func TestExternalSortOutputRetainsLargeDictionaryDomain(t *testing.T) {
	for _, parts := range []int{1, 8} {
		t.Run(fmt.Sprint(parts), func(t *testing.T) {
			domain := strings.Repeat("domain", 16000)
			var batches []*memory.Batch
			for i := 0; i < parts; i++ {
				b := memory.NewBatch()
				b.Rows = 2
				b.Int64s["key"] = &memory.Int64Vector{Values: []int64{int64(2*i + 1), int64(2 * i)}}
				b.DictStrings["value"] = &memory.DictionaryStringVector{Indices: []uint32{0, 0}, Offsets: []uint32{0, 1}, Data: []byte("x"), DictionaryDomain: domain, Encoding: memory.EncodingDictionary}
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingDictionary, DictionaryDomain: domain}), batches...)
			if err != nil {
				t.Fatal(err)
			}
			s, q, _, ctx := openSortLifecycle(t, 2<<20, src, t.TempDir())
			// Keep the ordinary output reservation far below the domain size.
			s.runBytes = 4096
			b := memory.NewBatch()
			defer b.ReleaseOwned()
			seen := 0
			for {
				ok, err := s.Next(ctx, b)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				if b.DictStrings["value"].DictionaryDomain != domain {
					t.Fatal("domain changed")
				}
				if s.outputOwner.reservation.Bytes() < int64(len(domain))+memory.BatchBytes(s.outputOwner.batch) {
					t.Fatal("output domain not covered", s.outputOwner.reservation.Bytes(), len(domain), memory.BatchBytes(s.outputOwner.batch))
				}
				if q.Used() < int64(len(domain)) {
					t.Fatal("parent admission lost domain", q.Used())
				}
				for _, key := range b.Int64s["key"].Values {
					if key != int64(seen) {
						t.Fatal(key, seen)
					}
					seen++
				}
			}
			if seen != parts*2 || q.Used() != 0 {
				t.Fatal(seen, q.Used())
			}
		})
	}
}

func TestExternalSortMergesDifferentDictionaryCodes(t *testing.T) {
	for _, sameDomain := range []bool{false, true} {
		t.Run(fmt.Sprint(sameDomain), func(t *testing.T) {
			const parts = 32
			field := memory.Field{Name: "value", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary}
			fields := []memory.Field{{Name: "key", Type: memory.TypeInt64}, {Name: "id", Type: memory.TypeInt64}, field}
			var batches []*memory.Batch
			for i := 0; i < parts; i++ {
				b := memory.NewBatch()
				b.Rows = 4
				b.Int64s["key"] = &memory.Int64Vector{Values: []int64{3, 2, 1, 0}}
				b.Int64s["id"] = &memory.Int64Vector{Values: []int64{int64(i * 4), int64(i*4 + 1), int64(i*4 + 2), int64(i*4 + 3)}}
				// Every batch's code 0 has a different meaning, including a large value.
				value := strings.Repeat(fmt.Sprintf("value-%02d", i), 20)
				d := &memory.DictionaryStringVector{Indices: []uint32{0, 0, 99, 0}, Offsets: []uint32{0, uint32(len(value))}, Data: []byte(value), Validity: memory.Bitmap{11}, DictionaryDomain: fmt.Sprint(i), Encoding: memory.EncodingDictionary}
				if sameDomain {
					d.DictionaryDomain = "same claimed identity"
				}
				b.DictStrings["value"] = d
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(fields...), batches...)
			if err != nil {
				t.Fatal(err)
			}
			s, q, _, ctx := openSortLifecycle(t, 512<<10, src, t.TempDir())
			s.runBytes, s.chunkRows = 4096, 4
			b := memory.NewBatch()
			seen := 0
			for {
				ok, err := s.Next(ctx, b)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				d := b.DictStrings["value"]
				for i, key := range b.Int64s["key"].Values {
					expectedPart := seen % parts
					expectedKey := seen / parts
					id := int64(expectedPart*4 + 3 - expectedKey)
					if key != int64(expectedKey) || b.Int64s["id"].Values[i] != id {
						t.Fatal(seen, key, id)
					}
					if d.IsValid(i) != (expectedKey != 1) {
						t.Fatal("validity changed", seen)
					}
					if d.IsValid(i) && string(d.DictionaryValue(d.Indices[i])) != strings.Repeat(fmt.Sprintf("value-%02d", expectedPart), 20) {
						t.Fatal("wrong dictionary value", seen, string(d.DictionaryValue(d.Indices[i])))
					}
					seen++
				}
			}
			if seen != parts*4 || q.Used() != 0 || s.mergeLevels < 2 {
				t.Fatal(seen, q.Used(), s.mergeLevels)
			}
		})
	}
}

func TestExternalSortSharedQueryConcurrentAndNested(t *testing.T) {
	for _, mode := range []string{"concurrent", "sibling-close", "nested"} {
		t.Run(mode, func(t *testing.T) {
			const hard = 1 << 20
			root := memory.NewEngineBudget(hard)
			q, err := root.BeginQuery(memory.BudgetOptions{HardLimitBytes: hard, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			ctx := memory.WithQueryBudget(context.Background(), q)
			makeOwner := func(input source.Source, keys []SortKey) *SortSource {
				owner, err := NewSortSourceWithOptions(input, keys, root, SortSourceOptions{Spillable: true})
				if err != nil {
					t.Fatal(err)
				}
				return owner.(*SortSource)
			}
			input := externalSortTestSource(t, 8192, 64)
			owner := makeOwner(input, []SortKey{{Column: "key"}})
			if mode == "nested" {
				owner = makeOwner(owner, []SortKey{{Column: "id", Descending: true}})
			}
			left, err := newExternalSortStream(ctx, owner, testReadOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer left.Close()
			left.runBytes, left.chunkRows = 4096, 128
			drain := func(s *externalSortStream, reverse, early bool) error {
				b := memory.NewBatch()
				seen := 0
				var lastKey, lastID int64
				for {
					ok, err := s.Next(ctx, b)
					if err != nil {
						return err
					}
					if !ok {
						break
					}
					for i, key := range b.Int64s["key"].Values {
						id := b.Int64s["id"].Values[i]
						if reverse {
							if id != int64(8191-seen) {
								return fmt.Errorf("nested borrowed output changed at %d: %d", seen, id)
							}
						} else if seen > 0 && (key < lastKey || key == lastKey && id < lastID) {
							return fmt.Errorf("unstable shared-query output")
						}
						lastKey, lastID = key, id
						seen++
					}
					if early {
						return s.Close()
					}
				}
				if seen != 8192 {
					return fmt.Errorf("rows=%d", seen)
				}
				return nil
			}
			if mode == "nested" {
				if err := drain(left, true, false); err != nil {
					t.Fatal(err)
				}
			} else {
				right, err := newExternalSortStream(ctx, makeOwner(input, []SortKey{{Column: "key"}}), testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer right.Close()
				right.runBytes, right.chunkRows = 4096, 128
				results := make(chan error, 2)
				go func() { results <- drain(left, false, mode == "sibling-close") }()
				go func() { results <- drain(right, false, false) }()
				for i := 0; i < 2; i++ {
					if err := <-results; err != nil {
						t.Error(err)
					}
				}
			}
			if q.Used() != 0 {
				t.Fatal("query ownership leaked", q.Used())
			}
		})
	}
}

func TestExternalSortDictionaryDomainEarlyCloseAndRejection(t *testing.T) {
	for _, hard := range []int64{128 << 10, 1 << 20} {
		t.Run(fmt.Sprint(hard), func(t *testing.T) {
			domain := strings.Repeat("d", 192<<10)
			b := memory.NewBatch()
			b.Rows = 1
			b.Int64s["key"] = &memory.Int64Vector{Values: []int64{1}}
			b.DictStrings["value"] = &memory.DictionaryStringVector{Indices: []uint32{0}, Offsets: []uint32{0, 1}, Data: []byte("x"), DictionaryDomain: domain, Encoding: memory.EncodingDictionary}
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingDictionary}), b)
			if err != nil {
				t.Fatal(err)
			}
			s, q, spy, ctx := openSortLifecycle(t, hard, src, t.TempDir())
			s.runBytes = 4096
			dst := memory.NewBatch()
			ok, err := s.Next(ctx, dst)
			if hard < int64(len(domain)) {
				if !errors.Is(err, memory.ErrBudgetExceeded) || ok || spy.attempts != 0 {
					t.Fatal("oversize metadata not rejected before spill", ok, err, spy.attempts)
				}
			} else {
				if err != nil || !ok || q.Used() < int64(len(domain)) || s.finalHeap.Len() != 0 || spy.Used() != 0 {
					t.Fatal(ok, err, q.Used(), spy.Used())
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if q.Used() != 0 || spy.Used() != 0 {
				t.Fatal("close/rejection retained admission", q.Used(), spy.Used())
			}
		})
	}
}

func TestExternalSortDictionaryKeysStableAcrossLevels(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprint(descending), func(t *testing.T) {
			type wantRow struct {
				value string
				id    int64
				valid bool
			}
			var want []wantRow
			var batches []*memory.Batch
			for part := 0; part < 32; part++ {
				b := memory.NewBatch()
				b.Rows = 4
				data := string(rune('a'+part%5)) + "common"
				b.DictStrings["key"] = &memory.DictionaryStringVector{Indices: []uint32{1, 0, 99, 1}, Offsets: []uint32{0, 1, 7}, Data: []byte(data), Validity: memory.Bitmap{11}, DictionaryDomain: fmt.Sprint(part), Encoding: memory.EncodingDictionary}
				ids := make([]int64, 4)
				for row := range ids {
					ids[row] = int64(part*4 + row)
					value := "common"
					if row == 1 {
						value = data[:1]
					}
					want = append(want, wantRow{value, ids[row], row != 2})
				}
				b.Int64s["id"] = &memory.Int64Vector{Values: ids}
				batches = append(batches, b)
			}
			sort.SliceStable(want, func(i, j int) bool {
				if want[i].valid != want[j].valid {
					return want[i].valid
				}
				if !want[i].valid {
					return false
				}
				if descending {
					return want[i].value > want[j].value
				}
				return want[i].value < want[j].value
			})
			src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary}, memory.Field{Name: "id", Type: memory.TypeInt64}), batches...)
			if err != nil {
				t.Fatal(err)
			}
			s, q, _, ctx := openSortLifecycle(t, 512<<10, src, t.TempDir())
			s.owner.keys[0].Descending = descending
			s.chunkRows, s.runBytes = 4, 4096
			dst := memory.NewBatch()
			seen := 0
			for {
				ok, err := s.Next(ctx, dst)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				d := dst.DictStrings["key"]
				for row, id := range dst.Int64s["id"].Values {
					w := want[seen]
					if id != w.id || d.IsValid(row) != w.valid || w.valid && string(d.DictionaryValue(d.Indices[row])) != w.value {
						t.Fatal("dictionary key order", seen, id, w)
					}
					seen++
				}
			}
			if seen != len(want) || q.Used() != 0 || s.mergeLevels < 2 {
				t.Fatal(seen, q.Used(), s.mergeLevels)
			}
		})
	}
}

func TestExternalSortRepeatedDictionaryCodesStayWithinEncodedBound(t *testing.T) {
	var batches []*memory.Batch
	for part := 0; part < 2; part++ {
		b := memory.NewBatch()
		b.Rows = 1024
		keys := make([]int64, 1024)
		for row := range keys {
			keys[row] = int64(row*2 + part)
		}
		b.Int64s["key"] = &memory.Int64Vector{Values: keys}
		b.DictStrings["value"] = &memory.DictionaryStringVector{Indices: make([]uint32, 1024), Offsets: []uint32{0, 1024}, Data: []byte(strings.Repeat(string(rune('a'+part)), 1024)), DictionaryDomain: fmt.Sprint(part), Encoding: memory.EncodingDictionary}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "value", Type: memory.TypeString, Encoding: memory.EncodingDictionary}), batches...)
	if err != nil {
		t.Fatal(err)
	}
	s, q, _, ctx := openSortLifecycle(t, 512<<10, src, t.TempDir())
	s.runBytes, s.chunkRows = 1<<20, 4096
	dst := memory.NewBatch()
	seen := 0
	for {
		ok, err := s.Next(ctx, dst)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		d := dst.DictStrings["value"]
		if d.DictionaryLen() > 2 || int64(len(d.DictionaryDomain))+memory.BatchBytes(dst) > s.outputOwner.reservation.Bytes() {
			t.Fatal("dictionary amplification", d.DictionaryLen(), memory.BatchBytes(dst), s.outputOwner.reservation.Bytes())
		}
		for row, key := range dst.Int64s["key"].Values {
			if key != int64(seen) || string(d.DictionaryValue(d.Indices[row])) != strings.Repeat(string(rune('a'+seen%2)), 1024) {
				t.Fatal(seen, key)
			}
			seen++
		}
	}
	if seen != 2048 || q.Used() != 0 {
		t.Fatal(seen, q.Used())
	}
}
