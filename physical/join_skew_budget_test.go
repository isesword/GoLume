package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/hashagg"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type skewBudgetRow struct {
	key  int32
	null bool
}

// Dictionary inputs deliberately use opposite code assignments and different
// domains. Right-side canonical IDs are 1..dictionaryKeys, so numeric and
// dictionary fixtures exercise the same radix collision without equating codes.
func skewBudgetSource(t testing.TB, rows []skewBudgetRow, chunk, dictionaryKeys int, left bool) *inmemory.Source {
	t.Helper()
	field := memory.Field{Name: "id", Type: memory.TypeInt32, Nullable: true}
	var dictionary source.Dictionary
	if dictionaryKeys > 0 {
		field.Type = memory.TypeString
		field.Encoding = memory.EncodingDictionary
		field.DictionaryDomain = "skew-right"
		if left {
			field.DictionaryDomain = "skew-left"
		}
		dictionary.Domain = field.DictionaryDomain
		dictionary.Offsets = []uint32{0}
		for code := 0; code < dictionaryKeys; code++ {
			key := code + 1
			if left {
				key = dictionaryKeys - code
			}
			dictionary.Data = append(dictionary.Data, fmt.Sprintf("key-%d", key)...)
			dictionary.Offsets = append(dictionary.Offsets, uint32(len(dictionary.Data)))
		}
	}
	schema := memory.NewSchema(field, memory.Field{Name: "row", Type: memory.TypeInt64})
	var batches []*memory.Batch
	for start := 0; start < len(rows); start += chunk {
		end := minInt(start+chunk, len(rows))
		b := memory.NewBatch()
		b.Rows = end - start
		validity := memory.EnsureBitmap(nil, b.Rows)
		ids := &memory.Int32Vector{Values: make([]int32, b.Rows), Validity: validity}
		codes := &memory.DictionaryStringVector{Validity: validity, Offsets: dictionary.Offsets, Data: dictionary.Data, DictionaryDomain: dictionary.Domain, Encoding: memory.EncodingDictionary}
		if dictionaryKeys > 0 {
			codes.Indices = make([]uint32, b.Rows)
		}
		b.Int64s["row"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		for i, row := range rows[start:end] {
			ids.Values[i] = row.key
			if !row.null {
				memory.Set(validity, i)
			}
			if dictionaryKeys > 0 {
				code := int(row.key) - 1
				if left {
					code = dictionaryKeys - int(row.key)
				}
				if code < 0 || code >= dictionaryKeys {
					t.Fatalf("key %d outside fixture dictionary", row.key)
				}
				codes.Indices[i] = uint32(code)
			}
			b.Int64s["row"].Values[i] = int64(start + i)
		}
		if dictionaryKeys > 0 {
			b.DictStrings["id"] = codes
		} else {
			b.Int32s["id"] = ids
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	if dictionaryKeys > 0 {
		src.WithDictionary("id", dictionary)
	}
	return src
}

// Small fixtures must exercise a genuinely spillable owner, not a resident
// broadcast owner with a manually opened spill cursor. Unknown row statistics
// disable that optional planning shortcut. Embed the concrete source so its
// dictionary, replay, identity and partition-stream capabilities are preserved.
type skewBudgetUnknownStatsSource struct {
	*inmemory.Source
}

func (s skewBudgetUnknownStatsSource) Stats(ctx context.Context) (source.Stats, error) {
	stats, err := s.Source.Stats(ctx)
	stats.Rows, stats.Bytes = -1, 0
	return stats, err
}

func skewBudgetExpected(left, right []skewBudgetRow, typ join.Type) [][2]int64 {
	convert := func(rows []skewBudgetRow) []genericJoinRow {
		out := make([]genericJoinRow, len(rows))
		for i, row := range rows {
			out[i].s = fmt.Sprint(row.key)
			if row.null {
				out[i].null = 1
			}
		}
		return out
	}
	return genericJoinExpected(convert(left), convert(right), 1, typ)
}

// Check before owner.Close or query.Close can conceal a stream ownership leak.
func assertSkewBudgetReleased(t testing.TB, s *spillableJoinStream, q *memory.QueryBudget, directory string) {
	t.Helper()
	if snapshot := q.Snapshot(); snapshot.UsedBytes != 0 || snapshot.PeakBytes > q.Limit() {
		t.Fatalf("stream cleanup: %+v", snapshot)
	}
	for i := range s.leftRuns {
		if s.leftRuns[i].Values != nil || s.rightRuns[i].Values != nil {
			t.Fatalf("partition %d retained input reference backing", i)
		}
	}
	if s.outputRuns.Values != nil {
		t.Fatal("retained output reference backing")
	}
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return fmt.Errorf("stream retained spill file %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type skewBudgetTempDir struct {
	testing.TB
	directory string
}

func (t skewBudgetTempDir) TempDir() string { return t.directory }

func TestJoinSkewBudgetAmplification320KiB(t *testing.T) {
	const rightRows = 5 * 2000
	left := []skewBudgetRow{{key: 7}, {key: 7}}
	right := make([]skewBudgetRow, rightRows)
	for i := range right {
		right[i].key = 7
	}
	// Include realistic long run paths in the retained-metadata budget instead
	// of relying on an unusually short /tmp path to make the regression pass.
	dir := filepath.Join(t.TempDir(), strings.Repeat("long-spill-directory-", 6))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, owner, s, q, dir := openByteSpill(skewBudgetTempDir{TB: t, directory: dir}, skewBudgetSource(t, left, 2, 0, true), skewBudgetSource(t, right, 2000, 0, false), join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	partition := int(hashagg.Hash128(hashagg.Key128{A: 7}) & (joinRadixPartitions - 1))
	if runs := len(s.rightRuns[partition].Values); runs != 5 {
		t.Fatalf("fixture must preserve five 2000-row build runs: got %d", runs)
	}
	before := q.Used()
	build, err := s.loadBuild(ctx, s.rightRuns[partition].Values)
	build.close()
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("fixture did not exert real build pressure: %v", err)
	}
	if q.Used() != before {
		t.Fatalf("failed build retained bytes: before=%d after=%d", before, q.Used())
	}
	got, err := drainByteSpill(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	// Every payload ID must appear exactly once for each left row. Counts
	// alone would miss duplicate, missing, or misbound right-row payloads.
	want := make([][2]int64, 0, 2*rightRows)
	for l := int64(0); l < 2; l++ {
		for r := int64(0); r < rightRows; r++ {
			want = append(want, [2]int64{l, r})
		}
	}
	sortBytePairs(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("amplified pair oracle mismatch: got=%d want=%d", len(got), len(want))
	}
	if s.maxDepth != 1 || s.maxMultiplicity != rightRows || s.maxBuildRows != 0 {
		t.Fatalf("skew diagnostics: depth=%d multiplicity=%d buildRows=%d", s.maxDepth, s.maxMultiplicity, s.maxBuildRows)
	}
	assertSkewBudgetReleased(t, s, q, dir)
	if ok, err := s.Next(ctx, memory.NewBatch()); ok || err != nil {
		t.Fatalf("repeated EOF: ok=%t err=%v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertSkewBudgetReleased(t, s, q, dir)
	assertByteSpillClean(t, owner, q, dir)
}

// Reject only the first build restore in each populated right partition. This
// exercises loadBuild rollback and early skew on small exact-oracle fixtures,
// without lowering the query budget or imposing tiny artificial output limits.
// Successful right restores are counted separately from injected rejections.
type skewBudgetStore struct {
	memory.SpillManager
	pressure      map[string]bool
	rightRestores map[string]int
	rejections    int
	onRestore     func()
}

func (s *skewBudgetStore) SpillWithMetadata(ctx context.Context, object memory.SpillObject, manager memory.Manager) (memory.SpillHandle, error) {
	return s.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, object, manager)
}

func (s *skewBudgetStore) Restore(ctx context.Context, handle memory.SpillHandle, fingerprint [32]byte, manager memory.Manager) (*memory.RestoredSpill, error) {
	if s.pressure[handle.ID()] {
		delete(s.pressure, handle.ID())
		s.rejections++
		return nil, memory.ErrBudgetExceeded
	}
	restored, err := s.SpillManager.Restore(ctx, handle, fingerprint, manager)
	if err == nil {
		if _, tracked := s.rightRestores[handle.ID()]; tracked {
			s.rightRestores[handle.ID()]++
			if s.onRestore != nil {
				callback := s.onRestore
				s.onRestore = nil
				callback()
			}
		}
	}
	return restored, err
}

func installSkewBudgetStore(t testing.TB, s *spillableJoinStream, q *memory.QueryBudget, deleteErr error) *skewBudgetStore {
	t.Helper()
	if !s.owner.spillable || s.owner.boundedBuild || s.owner.Strategy() != "partitioned-hash-spill" {
		t.Fatalf("fixture must use a spillable owner, got %s", s.owner.Strategy())
	}
	base, err := q.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	if deleteErr != nil {
		base = &byteSpillDeleteOnce{SpillManager: base, err: deleteErr}
	}
	wrapped := &skewBudgetStore{SpillManager: base, pressure: make(map[string]bool), rightRestores: make(map[string]int)}
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	s.store = spillmeta.NewStore(wrapped, s.metadata)
	return wrapped
}

func (s *skewBudgetStore) arm(stream *spillableJoinStream) int {
	partitions := 0
	for i := range stream.rightRuns {
		refs := stream.rightRuns[i].Values
		if len(refs) == 0 {
			continue
		}
		partitions++
		s.pressure[refs[0].handle.ID()] = true
		for _, ref := range refs {
			s.rightRestores[ref.handle.ID()] = 0
		}
	}
	return partitions
}

func TestJoinSkewBudgetEarlyAllModes(t *testing.T) {
	collision := int32(8)
	partition := hashagg.Hash128(hashagg.Key128{A: 7}) & (joinRadixPartitions - 1)
	for hashagg.Hash128(hashagg.Key128{A: uint64(collision)})&(joinRadixPartitions-1) != partition {
		collision++
		if collision > 1024 {
			t.Fatal("no bounded fixture collision found")
		}
	}
	right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7, null: true}, {key: 7}}
	for _, dictionary := range []bool{false, true} {
		for _, noMatch := range []bool{false, true} {
			for typ := join.Inner; typ <= join.Full; typ++ {
				t.Run(fmt.Sprintf("dictionary=%t/noMatch=%t/%s", dictionary, noMatch, typ), func(t *testing.T) {
					left := []skewBudgetRow{{key: 7}, {key: collision}, {key: 7, null: true}, {key: 7}}
					if noMatch {
						left = []skewBudgetRow{{key: collision}, {key: 7, null: true}}
					}
					dictionaryKeys := 0
					if dictionary {
						dictionaryKeys = int(collision)
					}
					ctx, owner, s, q, dir := openByteSpill(t, skewBudgetSource(t, left, 2, dictionaryKeys, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 2, dictionaryKeys, false)}, typ, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
					if (s.owner.shape == joinDictionary) != dictionary || s.owner.shape == joinBytes {
						t.Fatalf("wrong fixture key path: %v", s.owner.shape)
					}
					store := installSkewBudgetStore(t, s, q, nil)
					if err := s.prepare(ctx); err != nil {
						t.Fatal(err)
					}
					if len(s.leftRuns[partition].Values) == 0 || len(s.rightRuns[partition].Values) == 0 {
						t.Fatal("fixture must put the different probe key in the skew build partition")
					}
					partitions := store.arm(s)
					got, err := drainByteSpill(ctx, s)
					if err != nil {
						t.Fatal(err)
					}
					want := skewBudgetExpected(left, right, typ)
					sortBytePairs(got)
					sortBytePairs(want)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("early skew pair oracle: got=%v want=%v", got, want)
					}
					if store.rejections != partitions || partitions == 0 || len(store.pressure) != 0 || s.maxBuildRows != 0 {
						t.Fatalf("build pressure coverage: rejected=%d partitions=%d pending=%d buildRows=%d", store.rejections, partitions, len(store.pressure), s.maxBuildRows)
					}
					if s.maxDepth != 1 || s.maxMultiplicity != 3 {
						t.Fatalf("early skew diagnostics: depth=%d multiplicity=%d", s.maxDepth, s.maxMultiplicity)
					}
					if noMatch {
						// One validation pass is necessary. Only right/full joins
						// need another pass to emit unmatched right payloads.
						wantRestores := 1
						if typ == join.Right || typ == join.Full {
							wantRestores++
						}
						for id, restores := range store.rightRestores {
							if restores != wantRestores {
								t.Fatalf("unmatched probe restored right run %s %d times, want %d", id, restores, wantRestores)
							}
						}
					}
					assertSkewBudgetReleased(t, s, q, dir)
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					assertByteSpillClean(t, owner, q, dir)
				})
			}
		}
	}
}

func TestJoinSkewBudgetEarlyFailureCleanup(t *testing.T) {
	left := []skewBudgetRow{{key: 7}, {key: 7}}
	right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}}
	for _, dictionary := range []bool{false, true} {
		for _, failure := range []string{"delete", "cancel"} {
			t.Run(fmt.Sprintf("dictionary=%t/%s", dictionary, failure), func(t *testing.T) {
				dictionaryKeys := 0
				if dictionary {
					dictionaryKeys = 7
				}
				ctx, owner, s, q, dir := openByteSpill(t, skewBudgetSource(t, left, 2, dictionaryKeys, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 2, dictionaryKeys, false)}, join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
				var deleteErr error
				wantErr := context.Canceled
				if failure == "delete" {
					deleteErr = errors.New("injected early skew delete failure")
					wantErr = deleteErr
				}
				store := installSkewBudgetStore(t, s, q, deleteErr)
				if err := s.prepare(ctx); err != nil {
					t.Fatal(err)
				}
				store.arm(s)
				if failure == "cancel" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					defer cancel()
					// Cancel after loadBuild has failed and the skew validation
					// has acquired a restored run, rather than at Next's entry.
					store.onRestore = cancel
				}
				_, err := s.Next(ctx, memory.NewBatch())
				if !errors.Is(err, wantErr) {
					t.Fatalf("lost early skew %s failure: %v", failure, err)
				}
				if store.rejections != 1 || s.maxDepth != 1 {
					t.Fatalf("did not fail on early skew path: rejected=%d depth=%d", store.rejections, s.maxDepth)
				}
				if failure == "delete" && s.outputCount == 0 {
					t.Fatal("delete failure did not exercise partial output ownership")
				}
				_, retry := s.Next(context.Background(), memory.NewBatch())
				if retry != err {
					t.Fatalf("failure is not sticky: first=%v retry=%v", err, retry)
				}
				assertSkewBudgetReleased(t, s, q, dir)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				assertSkewBudgetReleased(t, s, q, dir)
				assertByteSpillClean(t, owner, q, dir)
			})
		}
	}
}

func TestJoinSkewBudgetNullAndEmptyBuilds(t *testing.T) {
	left := []skewBudgetRow{{key: 7}, {key: 7, null: true}}
	for _, dictionary := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			for typ := join.Inner; typ <= join.Full; typ++ {
				t.Run(fmt.Sprintf("dictionary=%t/empty=%t/%s", dictionary, empty, typ), func(t *testing.T) {
					right := []skewBudgetRow{{key: 7, null: true}, {key: 8, null: true}, {key: 7, null: true}}
					if empty {
						right = nil
					}
					dictionaryKeys := 0
					if dictionary {
						dictionaryKeys = 8
					}
					ctx, owner, s, q, dir := openByteSpill(t, skewBudgetSource(t, left, 2, dictionaryKeys, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 2, dictionaryKeys, false)}, typ, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
					store := installSkewBudgetStore(t, s, q, nil)
					if err := s.prepare(ctx); err != nil {
						t.Fatal(err)
					}
					partitions := store.arm(s)
					got, err := drainByteSpill(ctx, s)
					if err != nil {
						t.Fatal(err)
					}
					want := skewBudgetExpected(left, right, typ)
					sortBytePairs(got)
					sortBytePairs(want)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("NULL/empty build pair oracle: got=%v want=%v", got, want)
					}
					wantPartitions := 1
					if empty {
						wantPartitions = 0
					}
					if partitions != wantPartitions || store.rejections != wantPartitions || len(store.pressure) != 0 {
						t.Fatalf("NULL/empty build coverage: partitions=%d rejected=%d pending=%d", partitions, store.rejections, len(store.pressure))
					}
					if s.maxDepth != 1 || s.maxMultiplicity != 0 || s.maxBuildRows != 0 {
						t.Fatalf("NULL/empty diagnostics: depth=%d multiplicity=%d buildRows=%d", s.maxDepth, s.maxMultiplicity, s.maxBuildRows)
					}
					assertSkewBudgetReleased(t, s, q, dir)
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					assertByteSpillClean(t, owner, q, dir)
				})
			}
		}
	}
}

func skewBudgetPairSource(t testing.TB, keys [][2]int32) *inmemory.Source {
	t.Helper()
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt32}, memory.Field{Name: "b", Type: memory.TypeInt32}, memory.Field{Name: "row", Type: memory.TypeInt64})
	var batches []*memory.Batch
	for start := 0; start < len(keys); start += 2 {
		end := minInt(start+2, len(keys))
		b := memory.NewBatch()
		b.Rows = end - start
		b.Int32s["a"] = &memory.Int32Vector{Values: make([]int32, b.Rows)}
		b.Int32s["b"] = &memory.Int32Vector{Values: make([]int32, b.Rows)}
		b.Int64s["row"] = &memory.Int64Vector{Values: make([]int64, b.Rows)}
		for i, key := range keys[start:end] {
			b.Int32s["a"].Values[i] = key[0]
			b.Int32s["b"].Values[i] = key[1]
			b.Int64s["row"].Values[i] = int64(start + i)
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestJoinSkewBudgetCompositeExactEquality(t *testing.T) {
	collision := int32(12)
	partition := hashagg.Hash128(hashagg.Key128{A: 7, B: 11}) & (joinRadixPartitions - 1)
	for hashagg.Hash128(hashagg.Key128{A: 7, B: uint64(collision)})&(joinRadixPartitions-1) != partition {
		collision++
		if collision > 1024 {
			t.Fatal("no bounded composite collision found")
		}
	}
	for _, mixedBuild := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixedBuild=%t", mixedBuild), func(t *testing.T) {
			left := [][2]int32{{7, 11}, {7, collision}, {7, collision + 1}}
			right := [][2]int32{{7, 11}, {7, 11}, {7, 11}}
			want := [][2]int64{{0, 0}, {0, 1}, {0, 2}}
			if mixedBuild {
				// These share A and the first radix partition, but B differs.
				// Exact skew validation must reject this as a Cartesian group.
				right[1][1] = collision
				want = [][2]int64{{0, 0}, {0, 2}, {1, 1}}
			}
			ctx, owner, s, q, dir := openByteSpill(t, skewBudgetPairSource(t, left), skewBudgetUnknownStatsSource{skewBudgetPairSource(t, right)}, join.Inner, []JoinKey{{"a", "a"}, {"b", "b"}}, 320<<10, 64<<20)
			store := installSkewBudgetStore(t, s, q, nil)
			if err := s.prepare(ctx); err != nil {
				t.Fatal(err)
			}
			if len(s.rightRuns[partition].Values) != 2 {
				t.Fatal("composite fixture must keep both build keys in one partition")
			}
			if partitions := store.arm(s); partitions != 1 {
				t.Fatalf("composite build partition count=%d", partitions)
			}
			got, err := drainByteSpill(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			sortBytePairs(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("composite key pair oracle: got=%v want=%v", got, want)
			}
			if store.rejections != 1 || len(store.pressure) != 0 {
				t.Fatalf("composite pressure coverage: rejected=%d pending=%d", store.rejections, len(store.pressure))
			}
			if mixedBuild {
				if s.maxDepth < 2 || s.maxMultiplicity != 2 {
					t.Fatalf("different B keys were not repartitioned: depth=%d multiplicity=%d", s.maxDepth, s.maxMultiplicity)
				}
			} else if s.maxDepth != 1 || s.maxMultiplicity != 3 || s.maxBuildRows != 0 {
				t.Fatalf("composite early skew: depth=%d multiplicity=%d buildRows=%d", s.maxDepth, s.maxMultiplicity, s.maxBuildRows)
			}
			assertSkewBudgetReleased(t, s, q, dir)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}

func TestJoinSkewBudgetBorrowedOutputEarlyClose(t *testing.T) {
	left := []skewBudgetRow{{key: 7}, {key: 7}}
	right := []skewBudgetRow{{key: 7}, {key: 7}, {key: 7}}
	for _, dictionary := range []bool{false, true} {
		t.Run(fmt.Sprintf("dictionary=%t", dictionary), func(t *testing.T) {
			dictionaryKeys := 0
			if dictionary {
				dictionaryKeys = 7
			}
			ctx, owner, s, q, dir := openByteSpill(t, skewBudgetSource(t, left, 2, dictionaryKeys, true), skewBudgetUnknownStatsSource{skewBudgetSource(t, right, 1, dictionaryKeys, false)}, join.Inner, []JoinKey{{"id", "id"}}, 320<<10, 64<<20)
			store := installSkewBudgetStore(t, s, q, nil)
			if err := s.prepare(ctx); err != nil {
				t.Fatal(err)
			}
			store.arm(s)
			first, second := memory.NewBatch(), memory.NewBatch()
			if ok, err := s.Next(ctx, first); err != nil || !ok || first.Rows != 2 {
				t.Fatalf("first output: ok=%t rows=%d err=%v", ok, first.Rows, err)
			}
			if ok, err := s.Next(ctx, second); err != nil || !ok || second.Rows != 2 {
				t.Fatalf("second output: ok=%t rows=%d err=%v", ok, second.Rows, err)
			}
			assertCleared := func(label string, b *memory.Batch) {
				t.Helper()
				if b.Rows != 0 || len(b.Int32s) != 0 || len(b.Int64s) != 0 || len(b.DictStrings) != 0 {
					t.Fatalf("%s still borrows released output: rows=%d int32s=%d int64s=%d dictionaries=%d", label, b.Rows, len(b.Int32s), len(b.Int64s), len(b.DictStrings))
				}
			}
			assertCleared("previous destination", first)
			if store.rejections != 1 || s.maxDepth != 1 || len(s.outputRuns.Values) == 0 {
				t.Fatalf("early close must retain pending skew outputs: rejected=%d depth=%d outputs=%d", store.rejections, s.maxDepth, len(s.outputRuns.Values))
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertCleared("current destination after Close", second)
			assertSkewBudgetReleased(t, s, q, dir)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if ok, err := s.Next(ctx, first); ok || !errors.Is(err, source.ErrBatchStreamClosed) {
				t.Fatalf("Next after Close: ok=%t err=%v", ok, err)
			}
			assertCleared("destination after closed Next", first)
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}
