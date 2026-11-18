package lazy

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type ntileRow struct {
	id, grp, k2          int64
	k1                   float64
	groupValid, keyValid bool
	pad                  string
}
type ntileExpected struct {
	rank, row, dense    int64
	percent, cumulative float64
	pair                bool
	total               int64
}

func ntileCompare(a, b ntileRow, desc bool) int {
	if a.keyValid != b.keyValid {
		if !a.keyValid {
			return -1
		}
		return 1
	}
	c := 0
	if a.keyValid {
		switch {
		case math.IsNaN(a.k1) && math.IsNaN(b.k1):
		case math.IsNaN(a.k1):
			c = 1
		case math.IsNaN(b.k1):
			c = -1
		case a.k1 < b.k1:
			c = -1
		case a.k1 > b.k1:
			c = 1
		}
		if desc {
			c = -c
		}
	}
	if c != 0 {
		return c
	}
	if a.k2 < b.k2 {
		return -1
	}
	if a.k2 > b.k2 {
		return 1
	}
	return 0
}
func ntileSameGroup(a, b ntileRow) bool {
	return a.groupValid == b.groupValid && (!a.groupValid || a.grp == b.grp)
}
func ntileOrder(rows []ntileRow, desc bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.groupValid != b.groupValid {
			return !a.groupValid
		}
		if a.groupValid && a.grp != b.grp {
			return a.grp < b.grp
		}
		return ntileCompare(a, b, desc) < 0
	})
}
func ntileOracle(rows []ntileRow, desc bool, pair bool) map[int64]ntileExpected {
	ordered := append([]ntileRow(nil), rows...)
	ntileOrder(ordered, desc)
	out := map[int64]ntileExpected{}
	for begin := 0; begin < len(ordered); {
		end := begin + 1
		for end < len(ordered) && ntileSameGroup(ordered[begin], ordered[end]) {
			end++
		}
		// Independent group-at-a-time oracle: peerEnd is exclusive and is
		// measured from the partition start, never from a transport page.
		dense := int64(0)
		for peerBegin := begin; peerBegin < end; {
			peerEnd := peerBegin + 1
			for peerEnd < end && ntileCompare(ordered[peerBegin], ordered[peerEnd], desc) == 0 {
				peerEnd++
			}
			dense++
			rank := int64(peerBegin - begin + 1)
			p := 0.0
			if end-begin > 1 {
				p = float64(rank-1) / float64(end-begin-1)
			}
			for i := peerBegin; i < peerEnd; i++ {
				out[ordered[i].id] = ntileExpected{rank: rank, row: int64(i - begin + 1), dense: dense, percent: p, cumulative: float64(peerEnd-begin) / float64(end-begin), pair: pair, total: int64(end - begin)}
			}
			peerBegin = peerEnd
		}
		begin = end
	}
	return out
}
func ntileSmallRows() []ntileRow {
	vals := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, 1, math.Inf(1), math.NaN(), math.Float64frombits(0x7ff8000000000011), 0}
	var rows []ntileRow
	for i := 0; i < 27; i++ {
		rows = append(rows, ntileRow{id: int64(i), grp: 0, k1: vals[i%9], k2: int64((i / 9) % 2), groupValid: true, keyValid: i%9 != 8, pad: strings.Repeat(string(rune('a'+i%26)), 64)})
	}
	rows = append(rows, ntileRow{id: 27, grp: 1, k1: 2, groupValid: true, keyValid: true, pad: strings.Repeat("s", 64)})
	for i := 28; i < 33; i++ {
		rows = append(rows, ntileRow{id: int64(i), grp: 2, k1: 7, groupValid: i < 31, keyValid: true, pad: strings.Repeat("p", 64)})
	}
	for i := len(rows) - 1; i > 0; i-- {
		j := (i*17 + 3) % (i + 1)
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}
func ntileLargeRows() []ntileRow {
	rows := make([]ntileRow, 32768)
	for i := range rows {
		rows[i] = ntileRow{id: int64(i), grp: 0, k1: float64((i / 128) % 8), k2: int64((i / 2048) % 2), groupValid: true, keyValid: i%193 != 0, pad: strings.Repeat(string(rune('a'+i%26)), 64)}
	}
	rows[len(rows)-1].grp = 1
	for i := len(rows) - 1; i > 0; i-- {
		j := (i*17 + 3) % (i + 1)
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}

// This source opens only once per query, and overwrites all returned buffers on
// every Next and Close. Retaining the original borrowed batch is incorrect.
type ntileSource struct {
	rows          []ntileRow
	batchRows     int
	ordered, desc bool
	opens, closes int
	cancelAfter   int
	cancel        context.CancelFunc
}

func (s *ntileSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "k1", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "k2", Type: memory.TypeInt64}, memory.Field{Name: "pad", Type: memory.TypeString, Encoding: memory.EncodingView})
}
func (s *ntileSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1, Bytes: int64(len(s.rows)) * 104, RowsUpperBound: true}, nil
}
func (s *ntileSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *ntileSource) Ordering() []source.OrderingColumn {
	if !s.ordered {
		return nil
	}
	return []source.OrderingColumn{{Name: "grp"}, {Name: "k1", Descending: s.desc}, {Name: "k2", NullsLast: true}}
}
func (s *ntileSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	return s.NewBatchStreamWithPartitions(ctx, o, []source.Partition{{ID: 0, Rows: int64(len(s.rows))}})
}
func (s *ntileSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if e := o.Validate(); e != nil {
		return nil, e
	}
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("NTILE reopened original source")
	}
	if len(parts) != 1 || parts[0].ID != 0 {
		return nil, fmt.Errorf("unexpected source partitions")
	}
	n := s.batchRows
	if o.BatchRows > 0 {
		n = min(n, o.BatchRows)
	}
	return &ntileStream{owner: s, cap: n, id: make([]int64, n), grp: make([]int64, n), k2: make([]int64, n), k1: make([]float64, n), gv: memory.EnsureBitmap(nil, n), kv: memory.EnsureBitmap(nil, n), offsets: make([]uint32, n+1), data: make([]byte, n*64)}, nil
}

type ntileStream struct {
	owner       *ntileSource
	cap, pos    int
	closed      bool
	id, grp, k2 []int64
	k1          []float64
	gv, kv      memory.Bitmap
	offsets     []uint32
	data        []byte
}

func (s *ntileStream) poison() {
	for i := range s.id {
		s.id[i] = -777
		s.grp[i] = -777
		s.k2[i] = -777
		s.k1[i] = -777
	}
	for i := range s.data {
		s.data[i] = '!'
	}
	clear(s.gv)
	clear(s.kv)
	clear(s.offsets)
}
func (s *ntileStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	s.poison()
	if s.owner.cancel != nil && s.pos >= s.owner.cancelAfter {
		s.owner.cancel()
	}
	if e := ctx.Err(); e != nil {
		return false, e
	}
	if s.pos == len(s.owner.rows) {
		return false, nil
	}
	n := min(s.cap, len(s.owner.rows)-s.pos)
	for i := 0; i < n; i++ {
		r := s.owner.rows[s.pos+i]
		s.id[i], s.grp[i], s.k2[i], s.k1[i] = r.id, r.grp, r.k2, r.k1
		if r.groupValid {
			memory.Set(s.gv, i)
		}
		if r.keyValid {
			memory.Set(s.kv, i)
		}
		copy(s.data[i*64:(i+1)*64], r.pad)
		s.offsets[i+1] = uint32((i + 1) * 64)
	}
	dst.ClearColumns()
	dst.Rows = n
	dst.Int64s["id"] = &memory.Int64Vector{Values: s.id[:n]}
	dst.Int64s["grp"] = &memory.Int64Vector{Values: s.grp[:n], Validity: s.gv[:memory.WordsFor(n)]}
	dst.Int64s["k2"] = &memory.Int64Vector{Values: s.k2[:n]}
	dst.Float64s["k1"] = &memory.Float64Vector{Values: s.k1[:n], Validity: s.kv[:memory.WordsFor(n)]}
	dst.Strings["pad"] = &memory.StringViewVector{Offsets: s.offsets[:n+1], Data: s.data[:n*64], Encoding: memory.EncodingView}
	s.pos += n
	return true, nil
}
func (s *ntileStream) Close() error {
	if !s.closed {
		s.closed = true
		s.poison()
		s.owner.closes++
	}
	return nil
}

func ntileQuery(t *testing.T, mode string, rows []ntileRow, batchRows int, desc, mixed bool, hard int64, soft int64, pair bool) (*Session, LazyFrame, *ntileSource, string, map[int64]ntileExpected) {
	t.Helper()
	input := append([]ntileRow(nil), rows...)
	if mode == "ordered" {
		ntileOrder(input, desc)
	}
	src := &ntileSource{rows: input, batchRows: batchRows, ordered: mode == "ordered", desc: desc}
	opts := engine.DefaultOptions()
	opts.PreparedCacheBytes = 0
	opts.MemoryLimitBytes = hard
	opts.QueryMemorySoftLimitBytes = soft
	dir := t.TempDir()
	opts.Spill = engine.SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	if len(rows) == 32768 {
		opts.Spill.MaxBytes = 128 << 20
	}
	if mode == "resident" {
		// Auto may legitimately choose external sorting for this fixed dataset.
		// Lock the resident acceptance path without changing its memory budget.
		opts.ExecutionPolicy = engine.ExecutionMemoryOnly
	} else if mode == "external" {
		opts.ExecutionPolicy = engine.ExecutionBounded
	}
	s := NewSession(opts)
	keys := OrderBy(AscNullsFirst("k1"), Asc("k2"))
	if desc {
		keys = OrderBy(DescNullsFirst("k1"), Asc("k2"))
	}
	expressions := []ConfiguredWindowExpr{NTile(3, "nt3"), NTile(7, "nt7"), UseWindow(RowNumber("rn"))}
	if pair {
		expressions = []ConfiguredWindowExpr{NTile(7, "nt7"), UseWindow(PercentRank("pr")), UseWindow(CumeDist("cd"))}
	}
	if mixed {
		expressions = append(expressions, NTile(7, "repeat7"), NTile(1, "one"), NTile(2, "two"), NTile(27, "equalN"), NTile(28, "overN"), NTile(math.MaxInt64, "huge"), NTile(3, "k2"), UseWindow(DenseRank("dense")))
	}
	q := s.FromSource(src).WithConfiguredWindows(NewWindow(PartitionBy("grp"), keys), expressions...)
	plan, e := q.ExplainPhysical(context.Background())
	if e != nil {
		s.Close()
		t.Fatal(e)
	}
	strategy := map[string]string{"resident": "resident", "ordered": "ordered-partition-spool-replay", "external": "external-sort-restore"}[mode]
	if !strings.Contains(plan, "strategy="+strategy) {
		s.Close()
		t.Fatalf("expected %s: %s", strategy, plan)
	}
	sch, e := q.Schema()
	if e != nil {
		s.Close()
		t.Fatal(e)
	}
	for _, name := range ntileColumns(mixed, pair)[5:] {
		f, ok := sch.Field(name)
		wantType := memory.TypeInt64
		if name == "pr" || name == "cd" {
			wantType = memory.TypeFloat64
		}
		if !ok || f.Type != wantType || f.Nullable {
			s.Close()
			t.Fatal("NTILE output schema", name, f)
		}
	}
	return s, q, src, dir, ntileOracle(input, desc, pair)
}
func ntileValueEqual(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return af == bf || math.IsNaN(af) && math.IsNaN(bf)
	}
	return reflect.DeepEqual(a, b)
}
func ntileWant(r ntileRow, e ntileExpected, name string, mixed bool) any {
	switch name {
	case "id":
		return r.id
	case "grp":
		if !r.groupValid {
			return nil
		}
		return r.grp
	case "k1":
		if !r.keyValid {
			return nil
		}
		return r.k1
	case "k2":
		if mixed {
			return ntileBucket(e.row, e.total, 3)
		}
		return r.k2
	case "pad":
		return r.pad
	case "nt3":
		return ntileBucket(e.row, e.total, 3)
	case "nt7", "repeat7":
		return ntileBucket(e.row, e.total, 7)
	case "one":
		return int64(1)
	case "two":
		return ntileBucket(e.row, e.total, 2)
	case "equalN":
		return ntileBucket(e.row, e.total, 27)
	case "overN":
		return ntileBucket(e.row, e.total, 28)
	case "huge":
		return e.row
	case "cd":
		return e.cumulative
	case "pr":
		return e.percent
	case "rk":
		return e.rank
	case "rn":
		return e.row
	case "dense":
		return e.dense
	}
	panic(name)
}

// Position is one-based. Only r*(q+1) is multiplied, bounded by total.
func ntileBucket(position, total, buckets int64) int64 {
	q, r := total/buckets, total%buckets
	if q == 0 {
		return position
	}
	large := r * (q + 1)
	if position <= large {
		return (position-1)/(q+1) + 1
	}
	return r + (position-large-1)/q + 1
}
func ntileColumns(mixed, pair bool) []string {
	n := []string{"id", "grp", "k1", "k2", "pad", "nt3", "nt7", "rn"}
	if pair {
		n = []string{"id", "grp", "k1", "k2", "pad", "nt7", "pr", "cd"}
	}
	if mixed {
		n = append(n, "repeat7", "one", "two", "equalN", "overN", "huge", "dense")
	}
	return n
}
func ntileCheck(t *testing.T, column func(string) (Series, error), rows []ntileRow, want map[int64]ntileExpected, mixed bool) {
	t.Helper()
	pair := false
	if len(rows) > 0 {
		pair = want[rows[0].id].pair
	}
	for _, name := range ntileColumns(mixed, pair) {
		c, e := column(name)
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil || len(v) != len(rows) {
			t.Fatal(name, len(v), len(rows), e)
		}
		for i, r := range rows {
			w := ntileWant(r, want[r.id], name, mixed)
			if !ntileValueEqual(v[i], w) {
				t.Fatalf("%s row%d id%d got%v want%v", name, i, r.id, v[i], w)
			}
		}
	}
}
func ntileClean(t *testing.T, s *Session, src *ntileSource, dir string) {
	t.Helper()
	s.Close()
	if src.opens != 1 || src.closes != 1 {
		t.Fatal("source opens/closes", src.opens, src.closes)
	}
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("query owner leak", s.Engine().Memory().Used())
	}
	if e := filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			t.Errorf("spill remains: %s", path)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
func runTestNTileLazySemantics(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, bs := range []int{1, 2, 7} {
			for _, desc := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/batch%d/desc%t", mode, bs, desc), func(t *testing.T) {
					s, q, src, dir, want := ntileQuery(t, mode, ntileSmallRows(), bs, desc, true, 8<<20, 0, pair)
					defer s.Close()
					df, e := q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					defer df.Close()
					ntileCheck(t, df.Column, src.rows, want, true)
					s.Close()
					ntileCheck(t, df.Column, src.rows, want, true)
					df.Close()
					ntileClean(t, s, src, dir)
				})
			}
		}
	}
}
func runTestNTileLazyRequiresOrder(t *testing.T, pair bool) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	src := &ntileSource{rows: ntileSmallRows(), batchRows: 7}
	expressions := []ConfiguredWindowExpr{NTile(7, "nt7")}
	if pair {
		expressions = append(expressions, UseWindow(PercentRank("pr")))
	}
	q := s.FromSource(src).WithConfiguredWindows(NewWindow(PartitionBy("grp"), nil), expressions...)
	if prepared, e := q.Prepare(context.Background()); e == nil {
		prepared.Close()
		t.Fatal("missing ORDER BY accepted")
	}
	if src.opens != 0 {
		t.Fatal("invalid plan opened source")
	}
}

// Resource profiles are intentionally separate: streaming hard=8MiB and owned
// Collect hard=64MiB use identical 32768-row inputs. Neither replaces the other.
func runTestNTileLazyLargeOwned(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, want := ntileQuery(t, mode, ntileLargeRows(), 64, false, false, 64<<20, 48<<10, pair)
			defer s.Close()
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			ntileCheck(t, df.Column, src.rows, want, false)
			s.Close()
			ntileCheck(t, df.Column, src.rows, want, false)
			df.Close()
			ntileClean(t, s, src, dir)
		})
	}
}
func ntilePositive(t *testing.T, d map[string]string, key string) {
	t.Helper()
	n, e := strconv.ParseUint(d[key], 10, 64)
	if e != nil || n == 0 {
		t.Fatalf("missing actual %s diagnostics: %v", key, d)
	}
}
func runTestNTileLazyLargeStreamLifecycle(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, stop := range []string{"complete", "early", "cancel"} {
			t.Run(mode+"/"+stop, func(t *testing.T) {
				s, q, src, dir, want := ntileQuery(t, mode, ntileLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
				defer s.Close()
				budget, e := s.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				defer budget.Close()
				ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
				defer cancel()
				reader, e := q.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 32 << 10})
				if e != nil {
					t.Fatal(e)
				}
				defer reader.Close()
				reader.captureDiagnostics = true
				seen := 0
				var borrowed BatchView
				for reader.Next(ctx) {
					view := reader.Batch()
					ntileCheck(t, view.Column, src.rows[seen:seen+int(view.Len())], want, false)
					seen += int(view.Len())
					borrowed = view
					if stop == "early" {
						break
					}
					if stop == "cancel" {
						cancel()
						if reader.Next(ctx) || !errors.Is(reader.Err(), context.Canceled) {
							t.Fatal("cancel", reader.Err())
						}
						break
					}
				}
				if stop == "complete" && (seen != 32768 || reader.Err() != nil) {
					t.Fatal("stream incomplete", seen, reader.Err())
				}
				for closeAttempt := 0; closeAttempt < 2; closeAttempt++ {
					e := reader.Close()
					if stop == "cancel" {
						// Close preserves the terminal execution error. Check cleanup
						// independently so errors.Join cannot hide a cleanup failure.
						if !errors.Is(e, context.Canceled) || reader.closeErr != nil {
							t.Fatalf("cancel close %d: terminal=%v cleanup=%v", closeAttempt+1, e, reader.closeErr)
						}
					} else if e != nil {
						t.Fatal(e)
					}
				}
				if borrowed.Valid() {
					t.Fatal("borrowed view survived close")
				}
				snapshot := budget.Snapshot()
				if snapshot.PeakBytes > 8<<20 || budget.Used() != 0 {
					t.Fatal("stream budget", snapshot, budget.Used())
				}
				if stop == "complete" && mode != "resident" {
					ntilePositive(t, reader.diagnostics, "partition_spool_pages")
					ntilePositive(t, reader.diagnostics, "partition_spool_bytes")
					ntilePositive(t, reader.diagnostics, "partition_spool_partitions")
					if pair {
						ntilePositive(t, reader.diagnostics, "peer_lookahead_pages")
					}
					ntilePositive(t, reader.diagnostics, "partition_replay_pages")
					if mode == "external" {
						for _, key := range []string{"front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes"} {
							ntilePositive(t, reader.diagnostics, key)
						}
					}
				}
				t.Logf("mode=%s stop=%s rows=%d peak=%d spill=%d diagnostics=%v", mode, stop, seen, snapshot.PeakBytes, snapshot.SpilledBytes, reader.diagnostics)
				budget.Close()
				ntileClean(t, s, src, dir)
			})
		}
	}
}
func ntileArrowCheck(t *testing.T, r arrow.RecordBatch, rows []ntileRow, want map[int64]ntileExpected) {
	t.Helper()
	if int(r.NumRows()) != len(rows) {
		t.Fatal("record rows")
	}
	for c, f := range r.Schema().Fields() {
		if (f.Name == "pr" || f.Name == "cd") && (f.Nullable || f.Type.ID() != arrow.FLOAT64) {
			t.Fatal("Arrow distribution schema", f)
		}
		if (f.Name == "nt3" || f.Name == "nt7" || f.Name == "rn") && (f.Nullable || f.Type.ID() != arrow.INT64) {
			t.Fatal("Arrow NTILE schema", f)
		}
		a := r.Column(c)
		for i, row := range rows {
			var got any
			if !a.IsNull(i) {
				switch v := a.(type) {
				case *array.Int64:
					got = v.Value(i)
				case *array.Float64:
					got = v.Value(i)
				case *array.String:
					got = v.Value(i)
				default:
					t.Fatalf("unexpected array %T", a)
				}
			}
			if w := ntileWant(row, want[row.id], f.Name, false); !ntileValueEqual(got, w) {
				t.Fatal("record oracle", f.Name, row.id, got, w)
			}
		}
	}
}
func runTestNTileLazyLargeStreamingSinks(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, format := range []string{"ipc", "parquet"} {
			t.Run(mode+"/"+format, func(t *testing.T) {
				s, q, src, dir, want := ntileQuery(t, mode, ntileLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
				defer s.Close()
				budget, e := s.Engine().BeginQueryMemory()
				if e != nil {
					t.Fatal(e)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				path := filepath.Join(t.TempDir(), "rank."+format)
				if format == "ipc" {
					_, e = q.SinkIPC(ctx, path, IPCSinkOptions{})
				} else {
					_, e = q.SinkParquet(ctx, path, ParquetSinkOptions{RowGroupRows: 127})
				}
				if e != nil {
					t.Fatal(e)
				}
				if budget.Used() != 0 || budget.Snapshot().PeakBytes > 8<<20 {
					t.Fatal("streaming sink budget", budget.Snapshot(), budget.Used())
				}
				seen := 0
				if format == "ipc" {
					f, e := os.Open(path)
					if e != nil {
						t.Fatal(e)
					}
					rd, e := ipc.NewFileReader(f)
					if e != nil {
						f.Close()
						t.Fatal(e)
					}
					for i := 0; i < rd.NumRecords(); i++ {
						r, e := rd.RecordBatchAt(i)
						if e != nil {
							t.Fatal(e)
						}
						n := int(r.NumRows())
						ntileArrowCheck(t, r, src.rows[seen:seen+n], want)
						seen += n
						r.Release()
					}
					rd.Close()
					f.Close()
				} else {
					f, e := file.OpenParquetFile(path, false)
					if e != nil {
						t.Fatal(e)
					}
					ar, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: 127}, am.DefaultAllocator)
					if e != nil {
						f.Close()
						t.Fatal(e)
					}
					rr, e := ar.GetRecordReader(context.Background(), nil, nil)
					if e != nil {
						f.Close()
						t.Fatal(e)
					}
					for rr.Next() {
						r := rr.RecordBatch()
						n := int(r.NumRows())
						ntileArrowCheck(t, r, src.rows[seen:seen+n], want)
						seen += n
					}
					if e := rr.Err(); e != nil {
						t.Fatal(e)
					}
					rr.Release()
					f.Close()
				}
				if seen != 32768 {
					t.Fatal("streaming sink readback", seen)
				}
				t.Logf("mode=%s format=%s peak=%d spill=%d", mode, format, budget.Snapshot().PeakBytes, budget.Snapshot().SpilledBytes)
				budget.Close()
				ntileClean(t, s, src, dir)
			})
		}
	}
}

func runTestNTileLazyCancelBeforeOutput(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, _ := ntileQuery(t, mode, ntileLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
			defer s.Close()
			budget, e := s.Engine().BeginQueryMemory()
			if e != nil {
				t.Fatal(e)
			}
			defer budget.Close()
			ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
			defer cancel()
			src.cancelAfter = 128
			src.cancel = cancel
			reader, e := q.CollectBatches(ctx, BatchOptions{TargetBatchBytes: 8 << 10, MaxBatchBytes: 32 << 10})
			if e == nil {
				if reader.Next(ctx) {
					reader.Close()
					t.Fatal("emitted before denominator was known")
				}
				e = reader.Err()
				for attempt := 0; attempt < 2; attempt++ {
					if ce := reader.Close(); !errors.Is(ce, context.Canceled) || reader.closeErr != nil {
						t.Fatalf("input cancel close %d: %v cleanup=%v", attempt+1, ce, reader.closeErr)
					}
				}
			}
			if !errors.Is(e, context.Canceled) {
				t.Fatal("input-phase cancellation", e)
			}
			if budget.Used() != 0 {
				t.Fatal("canceled input-phase budget leak", budget.Used())
			}
			budget.Close()
			ntileClean(t, s, src, dir)
		})
	}
}

func TestNTileLazySemantics(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazySemantics(t, pair) })
	}
}

func TestNTileLazyRequiresOrder(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazyRequiresOrder(t, pair) })
	}
}

func TestNTileLazyLargeOwned(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazyLargeOwned(t, pair) })
	}
}

func TestNTileLazyLargeStreamLifecycle(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazyLargeStreamLifecycle(t, pair) })
	}
}

func TestNTileLazyLargeStreamingSinks(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazyLargeStreamingSinks(t, pair) })
	}
}

func TestNTileLazyCancelBeforeOutput(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "nt3-nt7-row"
		if pair {
			name = "nt7-pr-cd"
		}
		t.Run(name, func(t *testing.T) { runTestNTileLazyCancelBeforeOutput(t, pair) })
	}
}

func TestNTileLazyInvalidBuckets(t *testing.T) {
	for _, buckets := range []int64{0, -1, math.MinInt64} {
		t.Run(fmt.Sprint(buckets), func(t *testing.T) {
			s := NewSession(engine.DefaultOptions())
			defer s.Close()
			src := &ntileSource{rows: ntileSmallRows(), batchRows: 7}
			q := s.FromSource(src).WithConfiguredWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("k1"))), NTile(buckets, "nt"))
			if prepared, e := q.Prepare(context.Background()); e == nil {
				prepared.Close()
				t.Fatal("invalid buckets accepted", buckets)
			}
			if src.opens != 0 {
				t.Fatal("invalid configuration opened source", src.opens)
			}
		})
	}
}

// A literal hand-checked oracle distinguishes balanced buckets from the
// superficially plausible floor((row-1)*buckets/N)+1 formula.
func TestNTileLazyOracleKnownValues(t *testing.T) {
	for _, tc := range []struct {
		n, b int64
		want []int64
	}{
		{8, 3, []int64{1, 1, 1, 2, 2, 2, 3, 3}},
		{6, 4, []int64{1, 1, 2, 2, 3, 4}},
		{3, 7, []int64{1, 2, 3}},
		{1, math.MaxInt64, []int64{1}},
	} {
		for i, w := range tc.want {
			if got := ntileBucket(int64(i+1), tc.n, tc.b); got != w {
				t.Fatal(tc.n, tc.b, i, got, w)
			}
		}
	}
}

func TestNTileLazyPartitionAndOrderAlias(t *testing.T) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, _, src, dir, want := ntileQuery(t, mode, ntileSmallRows(), 2, false, false, 8<<20, 0, false)
			defer s.Close()
			q := s.FromSource(src).WithConfiguredWindows(NewWindow(PartitionBy("grp"), OrderBy(AscNullsFirst("k1"), Asc("k2"))), NTile(3, "grp"), NTile(7, "k2"))
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			for _, name := range []string{"id", "grp", "k2"} {
				col, e := df.Column(name)
				if e != nil {
					t.Fatal(e)
				}
				values, e := col.ToAnySlice()
				if e != nil || len(values) != len(src.rows) {
					t.Fatal(name, len(values), e)
				}
				for i, row := range src.rows {
					expected := row.id
					if name == "grp" {
						expected = ntileBucket(want[row.id].row, want[row.id].total, 3)
					}
					if name == "k2" {
						expected = ntileBucket(want[row.id].row, want[row.id].total, 7)
					}
					if values[i] != expected {
						t.Fatal("alias changed original partition/order", name, row.id, values[i], expected)
					}
				}
			}
			df.Close()
			ntileClean(t, s, src, dir)
		})
	}
}
