package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/join"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func openByteSpill(t testing.TB, left, right source.Source, typ join.Type, keys []JoinKey, limit, quota int64) (context.Context, *JoinSource, *spillableJoinStream, *memory.QueryBudget, string) {
	t.Helper()
	directory := t.TempDir()
	engine := memory.NewEngineBudget(limit)
	query, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: limit, Spill: memory.SpillOptions{Directory: directory, MaxBytes: quota}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := query.Close(); err != nil {
			t.Error(err)
		}
		if engine.Used() != 0 {
			t.Errorf("engine leaked %d", engine.Used())
		}
	})
	ctx := memory.WithQueryBudget(context.Background(), query)
	k := kernel.New(kernel.ModeAuto)
	owner, err := NewJoinSourceWithOptions(ctx, left, right, typ, keys, "", engine, JoinSourceOptions{Kernels: k, Spillable: true, RequireBounded: true, SpillConfigured: true, MemoryLimitBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	stream, err := newSpillableJoinStream(ctx, owner, source.ReadOptions{Kernels: k})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, owner, stream, query, directory
}

func drainByteSpill(ctx context.Context, stream source.BatchStream) (pairs [][2]int64, err error) {
	b := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, b)
		if err != nil || !ok {
			return pairs, err
		}
		for row := 0; row < b.Rows; row++ {
			l, r := int64(-1), int64(-1)
			if v := b.Int64s["row"]; v != nil && v.IsValid(row) {
				l = v.Values[row]
			}
			if v := b.Int64s["row_right"]; v != nil && v.IsValid(row) {
				r = v.Values[row]
			}
			pairs = append(pairs, [2]int64{l, r})
		}
	}
}

func sortBytePairs(pairs [][2]int64) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
}

func assertByteSpillClean(t testing.TB, owner *JoinSource, query *memory.QueryBudget, directory string) {
	t.Helper()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if snap := query.Snapshot(); snap.UsedBytes != 0 || snap.PeakBytes > query.Limit() {
		t.Fatalf("query=%+v", snap)
	}
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return fmt.Errorf("remaining run %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGenericSpillAllKeysModesAndDictionaries(t *testing.T) {
	left := []genericJoinRow{{"a", "bc", 1, 7, 0}, {"ab", "c", 1, 7, 0}, {"", "", 0, 0, 0}, {"汉🙂\x00", "x", 2, 9, 0}, {"a", "bc", 1, 7, 3}, {"missing", "", 0, 0, 0}, {"a", "bc", 1, 7, 1}}
	right := []genericJoinRow{{"a", "bc", 1, 7, 0}, {"a", "bc", 1, 7, 0}, {"ab", "c", 1, 7, 0}, {"", "", 0, 0, 0}, {"汉🙂\x00", "x", 2, 9, 0}, {"汉🙂\x00", "x", 2, 8, 0}, {"a", "bc", 9, 7, 0}, {"a", "bc", 1, 7, 1}, {"right only", "", 0, 0, 0}}
	names := []string{"s", "x", "n", "u"}
	for keyCount := 1; keyCount <= 4; keyCount++ {
		for _, ld := range []bool{false, true} {
			for _, rd := range []bool{false, true} {
				if keyCount == 1 && ld && rd {
					continue
				}
				for typ := join.Inner; typ <= join.Full; typ++ {
					t.Run(fmt.Sprintf("keys%d/ld%t/rd%t/%s", keyCount, ld, rd, typ), func(t *testing.T) {
						keys := make([]JoinKey, keyCount)
						for i := range keys {
							keys[i] = JoinKey{names[i], names[i]}
						}
						ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, ld, 3), genericJoinSource(t, right, rd, 4), typ, keys, 1<<20, 64<<20)
						got, err := drainByteSpill(ctx, stream)
						if err != nil {
							t.Fatal(err)
						}
						want := genericJoinExpected(left, right, keyCount, typ)
						sortBytePairs(got)
						sortBytePairs(want)
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("got=%v want=%v", got, want)
						}
						if stream.spillRuns == 0 {
							t.Fatal("did not spill")
						}
						assertByteSpillClean(t, owner, q, dir)
					})
				}
			}
		}
	}
}

func TestGenericSpillBudgetQuotaCancellationAndRetry(t *testing.T) {
	rows := make([]genericJoinRow, 128)
	for i := range rows {
		rows[i] = genericJoinRow{s: fmt.Sprintf("k%d", i), x: strings.Repeat("x", 100)}
	}
	for _, mode := range []string{"budget", "quota", "cancel", "success"} {
		t.Run(mode, func(t *testing.T) {
			limit, quota := int64(1<<20), int64(64<<20)
			if mode == "budget" {
				limit = 24 << 10
			}
			if mode == "quota" {
				quota = 3000
			}
			ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, rows, false, 16), genericJoinSource(t, rows, false, 16), join.Inner, []JoinKey{{"s", "s"}}, limit, quota)
			if mode == "cancel" {
				if err := stream.prepare(ctx); err != nil {
					t.Fatal(err)
				}
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, err := drainByteSpill(ctx, stream)
			if mode == "success" {
				if err != nil || len(got) != 128 {
					t.Fatalf("rows=%d err=%v", len(got), err)
				}
			} else {
				if err == nil {
					t.Fatal("expected failure")
				}
				if mode == "budget" && !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatal(err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if mode == "quota" && !errors.Is(err, memory.ErrSpillQuotaExceeded) {
					t.Fatal(err)
				}
				_, retry := stream.Next(context.Background(), memory.NewBatch())
				if retry != err {
					t.Fatalf("failure was not sticky: first=%v retry=%v", err, retry)
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}

func TestGenericSpillSingleKeySkewAllModes(t *testing.T) {
	left := []genericJoinRow{{s: "duplicate"}, {s: "missing"}, {s: "duplicate", null: 1}}
	right := make([]genericJoinRow, 800)
	for i := range right {
		right[i] = genericJoinRow{s: "duplicate", x: strings.Repeat("payload", 8)}
		if i%31 == 0 {
			right[i].null = 1
		}
	}
	for typ := join.Inner; typ <= join.Full; typ++ {
		t.Run(typ.String(), func(t *testing.T) {
			ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 3), genericJoinSource(t, right, false, 100), typ, []JoinKey{{"s", "s"}}, 256<<10, 64<<20)
			got, err := drainByteSpill(ctx, stream)
			if err != nil {
				t.Fatal(err)
			}
			want := genericJoinExpected(left, right, 1, typ)
			sortBytePairs(got)
			sortBytePairs(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got=%d want=%d", len(got), len(want))
			}
			if stream.maxBuildRows >= len(right) {
				t.Fatal("skew did not exercise bounded run-by-run path")
			}
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}

func TestGenericSpillEmptyAndNullOnlyBuild(t *testing.T) {
	for _, rows := range [][]genericJoinRow{nil, {{s: "a", null: 1}, {s: "b", null: 1}}} {
		for typ := join.Inner; typ <= join.Full; typ++ {
			t.Run(fmt.Sprintf("rows%d/%s", len(rows), typ), func(t *testing.T) {
				left := []genericJoinRow{{s: "a"}, {s: "b", null: 1}}
				ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 2), genericJoinSource(t, rows, false, 2), typ, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
				got, err := drainByteSpill(ctx, stream)
				if err != nil {
					t.Fatal(err)
				}
				want := genericJoinExpected(left, rows, 1, typ)
				sortBytePairs(got)
				sortBytePairs(want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got=%v want=%v", got, want)
				}
				assertByteSpillClean(t, owner, q, dir)
			})
		}
	}
}

func TestGenericSpillRepartitionsExactKeys(t *testing.T) {
	owner := &JoinSource{keys: []JoinKey{{"s", "s"}}, keyFields: []memory.Field{{Type: memory.TypeString}}}
	right := make([]genericJoinRow, 0, 1536)
	b := memory.NewBatch()
	b.Rows = 1
	for i := 0; len(right) < cap(right); i++ {
		key := fmt.Sprintf("collision-%06d", i)
		b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(key))}, Data: []byte(key)}
		h, err := owner.bytePartitionHash(b, false, 0)
		if err != nil {
			t.Fatal(err)
		}
		if h&15 == 0 {
			right = append(right, genericJoinRow{s: key, x: strings.Repeat("payload", 8)})
		}
	}
	// Group the next radix digit so child runs do not fragment into tiny files.
	sort.Slice(right, func(i, j int) bool {
		hash := func(v string) uint64 {
			b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, uint32(len(v))}, Data: []byte(v)}
			h, _ := owner.bytePartitionHash(b, false, 0)
			return h
		}
		return (hash(right[i].s) >> 4 & 15) < (hash(right[j].s) >> 4 & 15)
	})
	left := append([]genericJoinRow(nil), right[100:140]...)
	ctx, js, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 40), genericJoinSource(t, right, false, 128), join.Full, []JoinKey{{"s", "s"}}, 512<<10, 64<<20)
	got, err := drainByteSpill(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	want := genericJoinExpected(left, right, 1, join.Full)
	sortBytePairs(got)
	sortBytePairs(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%d want=%d", len(got), len(want))
	}
	if stream.maxDepth < 2 {
		t.Fatalf("did not repartition: %v", stream.StreamDiagnostics())
	}
	assertByteSpillClean(t, js, q, dir)
}

func TestGenericSpillWideOutputAndBorrowLifetime(t *testing.T) {
	left := []genericJoinRow{{s: "key"}}
	right := make([]genericJoinRow, 60)
	for i := range right {
		right[i] = genericJoinRow{s: "key", x: strings.Repeat("data", 1024)}
	}
	ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 1), genericJoinSource(t, right, false, 8), join.Inner, []JoinKey{{"s", "s"}}, 512<<10, 64<<20)
	first, second := memory.NewBatch(), memory.NewBatch()
	ok, err := stream.Next(ctx, first)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	rows := first.Rows
	ok, err = stream.Next(context.Background(), second)
	if err != nil || !ok {
		t.Fatalf("ok=%t err=%v", ok, err)
	}
	if len(first.Int64s) != 0 || len(first.Strings) != 0 {
		t.Fatal("prior output kept released buffers")
	}
	rows += second.Rows
	rest, err := drainByteSpill(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if rows+len(rest) != len(right) {
		t.Fatalf("rows=%d want=%d", rows+len(rest), len(right))
	}
	assertByteSpillClean(t, owner, q, dir)
}

func TestGenericSpillFatalCallsReleaseBuffers(t *testing.T) {
	for _, mode := range []string{"nil-context", "nil-destination", "foreign-query", "close"} {
		t.Run(mode, func(t *testing.T) {
			rows := []genericJoinRow{{s: "a"}, {s: "b"}, {s: "c"}}
			ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, rows, false, 1), genericJoinSource(t, rows, false, 1), join.Inner, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
			out := memory.NewBatch()
			if ok, err := stream.Next(ctx, out); err != nil || !ok {
				t.Fatalf("ok=%t err=%v", ok, err)
			}
			var err error
			switch mode {
			case "nil-context":
				_, err = stream.Next(nil, memory.NewBatch())
			case "nil-destination":
				_, err = stream.Next(ctx, nil)
			case "foreign-query":
				other, e := memory.NewEngineBudget(1 << 20).BeginQuery(memory.BudgetOptions{HardLimitBytes: 1 << 20})
				if e != nil {
					t.Fatal(e)
				}
				defer other.Close()
				_, err = stream.Next(memory.WithQueryBudget(context.Background(), other), memory.NewBatch())
			case "close":
				err = stream.Close()
			}
			if mode != "close" && err == nil {
				t.Fatal("expected failure")
			}
			if len(out.Strings) != 0 || len(out.Int64s) != 0 {
				t.Fatal("live aliases after close/failure")
			}
			assertByteSpillClean(t, owner, q, dir)
		})
	}
}

func TestGenericSpillOrderingReplayAndUpperBounds(t *testing.T) {
	left := []genericJoinRow{{s: "a"}, {s: "b"}, {s: "a"}, {s: "c"}}
	for _, right := range [][]genericJoinRow{nil, {{s: "a"}, {s: "a"}, {s: "c"}}} {
		for _, typ := range []join.Type{join.Left, join.Full} {
			t.Run(fmt.Sprintf("right%d/%s", len(right), typ), func(t *testing.T) {
				ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 2), genericJoinSource(t, right, false, 2), typ, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
				first, err := drainByteSpill(ctx, stream)
				if err != nil {
					t.Fatal(err)
				}
				stats, err := owner.Stats(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if int64(len(first)) > stats.Rows {
					t.Fatalf("actual=%d upper bound=%d", len(first), stats.Rows)
				}
				second, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: owner.kernels})
				if err != nil {
					t.Fatal(err)
				}
				replay, err := drainByteSpill(ctx, second)
				if err != nil {
					t.Fatal(err)
				}
				if err := second.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(first, replay) {
					t.Fatalf("replay order changed: first=%v second=%v", first, replay)
				}
				assertByteSpillClean(t, owner, q, dir)
			})
		}
	}
}

type byteSpillQuerySource struct {
	source.Source
	query     *memory.QueryBudget
	unrelated memory.Manager
}

func (s byteSpillQuerySource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if memory.QueryBudgetFromContext(ctx) != s.query {
		return nil, fmt.Errorf("child scan lost query ownership")
	}
	if opts.MemoryManager == nil || opts.MemoryManager == s.unrelated {
		return nil, fmt.Errorf("child scan escaped query budget")
	}
	return s.Source.NewBatchStream(ctx, opts)
}

func TestGenericSpillPinsChildScanBudget(t *testing.T) {
	rows := []genericJoinRow{{s: "a"}, {s: "b"}}
	ctx, owner, stream, q, dir := openByteSpill(t, genericJoinSource(t, rows, false, 1), genericJoinSource(t, rows, false, 1), join.Full, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	unrelated := memory.NewBudgetManager(0)
	owner.left = byteSpillQuerySource{owner.left, q, unrelated}
	owner.right = byteSpillQuerySource{owner.right, q, unrelated}
	replacement, err := owner.NewBatchStream(ctx, source.ReadOptions{Kernels: owner.kernels, MemoryManager: unrelated})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drainByteSpill(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	assertByteSpillClean(t, owner, q, dir)
}
