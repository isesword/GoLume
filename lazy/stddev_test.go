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
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type stddevRow struct {
	id, grp, k2          int64
	k1                   float64
	groupValid, keyValid bool
	pad                  string
}
type stddevExpected struct {
	rank, row, dense    int64
	percent, cumulative float64
	pair                bool
	total               int64
	pop, samp, prefix   any
	variance            any
}

func stddevCompare(a, b stddevRow, desc bool) int {
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
func stddevSameGroup(a, b stddevRow) bool {
	return a.groupValid == b.groupValid && (!a.groupValid || a.grp == b.grp)
}
func stddevOrder(rows []stddevRow, desc bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.groupValid != b.groupValid {
			return !a.groupValid
		}
		if a.groupValid && a.grp != b.grp {
			return a.grp < b.grp
		}
		return stddevCompare(a, b, desc) < 0
	})
}
func stddevOracle(rows []stddevRow, desc bool, pair bool) map[int64]stddevExpected {
	ordered := append([]stddevRow(nil), rows...)
	stddevOrder(ordered, desc)
	out := map[int64]stddevExpected{}
	for begin := 0; begin < len(ordered); {
		end := begin + 1
		for end < len(ordered) && stddevSameGroup(ordered[begin], ordered[end]) {
			end++
		}
		// Independent group-at-a-time oracle: peerEnd is exclusive and is
		// measured from the partition start, never from a transport page.
		dense := int64(0)
		for peerBegin := begin; peerBegin < end; {
			peerEnd := peerBegin + 1
			for peerEnd < end && stddevCompare(ordered[peerBegin], ordered[peerEnd], desc) == 0 {
				peerEnd++
			}
			dense++
			rank := int64(peerBegin - begin + 1)
			p := 0.0
			if end-begin > 1 {
				p = float64(rank-1) / float64(end-begin-1)
			}
			for i := peerBegin; i < peerEnd; i++ {
				out[ordered[i].id] = stddevExpected{rank: rank, row: int64(i - begin + 1), dense: dense, percent: p, cumulative: float64(peerEnd-begin) / float64(end-begin), pair: pair, total: int64(end - begin)}
			}
			peerBegin = peerEnd
		}
		sums := make([]int64, end-begin+1)
		squares := make([]int64, end-begin+1)
		for i := begin; i < end; i++ {
			v := ordered[i].k2
			sums[i-begin+1] = sums[i-begin] + v
			squares[i-begin+1] = squares[i-begin] + v*v
		}
		for i := begin; i < end; i++ {
			lo, hi := max(begin, i-7), min(end, i+4)
			n := int64(hi - lo)
			sum := sums[hi-begin] - sums[lo-begin]
			sq := squares[hi-begin] - squares[lo-begin]
			e := out[ordered[i].id]
			e.pop = stddevIntegerMoment(n, sum, sq, 0)
			e.variance, _ = new(big.Rat).SetFrac(big.NewInt(n*sq-sum*sum), big.NewInt(n*n)).Float64()
			e.samp = stddevIntegerMoment(n, sum, sq, 1)
			n = int64(i - begin + 1)
			e.prefix = stddevIntegerMoment(n, sums[i-begin+1], squares[i-begin+1], 0)
			out[ordered[i].id] = e
		}
		begin = end
	}
	return out
}
func stddevSmallRows() []stddevRow {
	vals := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, 1, math.Inf(1), math.NaN(), math.Float64frombits(0x7ff8000000000011), 0}
	var rows []stddevRow
	for i := 0; i < 27; i++ {
		rows = append(rows, stddevRow{id: int64(i), grp: 0, k1: vals[i%9], k2: int64((i / 9) % 2), groupValid: true, keyValid: i%9 != 8, pad: strings.Repeat(string(rune('a'+i%26)), 64)})
	}
	rows = append(rows, stddevRow{id: 27, grp: 1, k1: 2, groupValid: true, keyValid: true, pad: strings.Repeat("s", 64)})
	for i := 28; i < 33; i++ {
		rows = append(rows, stddevRow{id: int64(i), grp: 2, k1: 7, groupValid: i < 31, keyValid: true, pad: strings.Repeat("p", 64)})
	}
	for i := len(rows) - 1; i > 0; i-- {
		j := (i*17 + 3) % (i + 1)
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}
func stddevLargeRows() []stddevRow {
	rows := make([]stddevRow, 32768)
	for i := range rows {
		rows[i] = stddevRow{id: int64(i), grp: 0, k1: float64((i / 128) % 8), k2: int64((i / 2048) % 2), groupValid: true, keyValid: i%193 != 0, pad: strings.Repeat(string(rune('a'+i%26)), 64)}
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
type stddevSource struct {
	rows          []stddevRow
	batchRows     int
	ordered, desc bool
	opens, closes int
	cancelAfter   int
	cancel        context.CancelFunc
}

func (s *stddevSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "k1", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "k2", Type: memory.TypeInt64}, memory.Field{Name: "pad", Type: memory.TypeString, Encoding: memory.EncodingView})
}
func (s *stddevSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1, Bytes: int64(len(s.rows)) * 104, RowsUpperBound: true}, nil
}
func (s *stddevSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *stddevSource) Ordering() []source.OrderingColumn {
	if !s.ordered {
		return nil
	}
	return []source.OrderingColumn{{Name: "grp"}, {Name: "k1", Descending: s.desc}, {Name: "k2", NullsLast: true}}
}
func (s *stddevSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	return s.NewBatchStreamWithPartitions(ctx, o, []source.Partition{{ID: 0, Rows: int64(len(s.rows))}})
}
func (s *stddevSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if e := o.Validate(); e != nil {
		return nil, e
	}
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("stddev reopened original source")
	}
	if len(parts) != 1 || parts[0].ID != 0 {
		return nil, fmt.Errorf("unexpected source partitions")
	}
	n := s.batchRows
	if o.BatchRows > 0 {
		n = min(n, o.BatchRows)
	}
	return &stddevStream{owner: s, cap: n, id: make([]int64, n), grp: make([]int64, n), k2: make([]int64, n), k1: make([]float64, n), gv: memory.EnsureBitmap(nil, n), kv: memory.EnsureBitmap(nil, n), offsets: make([]uint32, n+1), data: make([]byte, n*64)}, nil
}

type stddevStream struct {
	owner       *stddevSource
	cap, pos    int
	closed      bool
	id, grp, k2 []int64
	k1          []float64
	gv, kv      memory.Bitmap
	offsets     []uint32
	data        []byte
}

func (s *stddevStream) poison() {
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
func (s *stddevStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
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
func (s *stddevStream) Close() error {
	if !s.closed {
		s.closed = true
		s.poison()
		s.owner.closes++
	}
	return nil
}

func stddevQuery(t *testing.T, mode string, rows []stddevRow, batchRows int, desc, mixed bool, hard int64, soft int64, pair bool) (*Session, LazyFrame, *stddevSource, string, map[int64]stddevExpected) {
	t.Helper()
	input := append([]stddevRow(nil), rows...)
	if mode == "ordered" {
		stddevOrder(input, desc)
	}
	src := &stddevSource{rows: input, batchRows: batchRows, ordered: mode == "ordered", desc: desc}
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
	frame := RowsBetween(Preceding(7), Following(3))
	expressions := []ConfiguredWindowExpr{UseWindow(RollingStdDevPop("k2", frame, "vp")), UseWindow(RollingStdDevSamp("k2", frame, "vs")), UseWindow(RowNumber("rn"))}
	if pair {
		expressions = []ConfiguredWindowExpr{UseWindow(RollingStdDevPop("k2", RowsBetween(UnboundedPreceding, CurrentRow), "prefix")), UseWindow(PercentRank("pr")), NTile(7, "nt7")}
	}
	if mixed {
		expressions = append(expressions, UseWindow(CumeDist("cd")), UseWindow(RollingVarPop("k2", frame, "variance")), UseWindow(RollingStdDevPop("k2", frame, "k2")))
	}
	q := s.FromSource(src).WithConfiguredWindows(NewWindow(PartitionBy("grp"), keys), expressions...)
	plan, e := q.ExplainPhysical(context.Background())
	if e != nil {
		s.Close()
		t.Fatal(e)
	}
	strategy := map[string]string{"resident": "resident", "ordered": "ordered-partition-spool-replay", "external": "external-sort-restore"}[mode]
	if mode == "ordered" && !pair && !mixed {
		strategy = "ordered-state"
	}
	if !strings.Contains(plan, "strategy="+strategy) {
		s.Close()
		t.Fatalf("expected %s: %s", strategy, plan)
	}
	sch, e := q.Schema()
	if e != nil {
		s.Close()
		t.Fatal(e)
	}
	for _, name := range stddevColumns(mixed, pair)[5:] {
		f, ok := sch.Field(name)
		wantType := memory.TypeInt64
		if name == "pr" || name == "cd" || name == "vp" || name == "vs" || name == "prefix" || name == "variance" {
			wantType = memory.TypeFloat64
		}
		nullable := name == "vp" || name == "vs" || name == "prefix" || name == "variance"
		if !ok || f.Type != wantType || f.Nullable != nullable {
			s.Close()
			t.Fatal("stddev output schema", name, f)
		}
	}
	return s, q, src, dir, stddevOracle(input, desc, pair)
}
func stddevValueEqual(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return af == bf || math.IsNaN(af) && math.IsNaN(bf)
	}
	return reflect.DeepEqual(a, b)
}
func stddevWant(r stddevRow, e stddevExpected, name string, mixed bool) any {
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
			return e.pop
		}
		return r.k2
	case "pad":
		return r.pad
	case "variance":
		return e.variance
	case "vp":
		return e.pop
	case "vs":
		return e.samp
	case "prefix":
		return e.prefix
	case "nt3":
		return stddevBucket(e.row, e.total, 3)
	case "nt7", "repeat7":
		return stddevBucket(e.row, e.total, 7)
	case "one":
		return int64(1)
	case "two":
		return stddevBucket(e.row, e.total, 2)
	case "equalN":
		return stddevBucket(e.row, e.total, 27)
	case "overN":
		return stddevBucket(e.row, e.total, 28)
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
func stddevBucket(position, total, buckets int64) int64 {
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

// Oracle-only memoization is outside query accounting, just like input fixtures.
// Repeated query matrices reuse exact moment triples, not rounded variances.
var stddevMomentOracleCache sync.Map

func stddevIntegerMoment(n, sum, squares, ddof int64) any {
	if n <= ddof {
		return nil
	}
	key := [4]int64{n, sum, squares, ddof}
	if v, ok := stddevMomentOracleCache.Load(key); ok {
		return v
	}
	v := stddevExactRoot(new(big.Rat).SetFrac(big.NewInt(n*squares-sum*sum), big.NewInt(n*(n-ddof))))
	stddevMomentOracleCache.Store(key, v)
	return v
}
func stddevColumns(mixed, pair bool) []string {
	n := []string{"id", "grp", "k1", "k2", "pad", "vp", "vs", "rn"}
	if pair {
		n = []string{"id", "grp", "k1", "k2", "pad", "prefix", "pr", "nt7"}
	}
	if mixed {
		n = append(n, "cd", "variance")
	}
	return n
}
func stddevCheck(t *testing.T, column func(string) (Series, error), rows []stddevRow, want map[int64]stddevExpected, mixed bool) {
	t.Helper()
	pair := false
	if len(rows) > 0 {
		pair = want[rows[0].id].pair
	}
	for _, name := range stddevColumns(mixed, pair) {
		c, e := column(name)
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil || len(v) != len(rows) {
			t.Fatal(name, len(v), len(rows), e)
		}
		for i, r := range rows {
			w := stddevWant(r, want[r.id], name, mixed)
			if !stddevValueEqual(v[i], w) {
				t.Fatalf("%s row%d id%d got%v want%v", name, i, r.id, v[i], w)
			}
		}
	}
}
func stddevClean(t *testing.T, s *Session, src *stddevSource, dir string) {
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
func runTestStdDevLazySemantics(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, bs := range []int{1, 2, 7} {
			for _, desc := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/batch%d/desc%t", mode, bs, desc), func(t *testing.T) {
					s, q, src, dir, want := stddevQuery(t, mode, stddevSmallRows(), bs, desc, true, 8<<20, 0, pair)
					defer s.Close()
					df, e := q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					defer df.Close()
					stddevCheck(t, df.Column, src.rows, want, true)
					s.Close()
					stddevCheck(t, df.Column, src.rows, want, true)
					df.Close()
					stddevClean(t, s, src, dir)
				})
			}
		}
	}
}

// Resource profiles are intentionally separate: streaming hard=8MiB and owned
// Collect hard=64MiB use identical 32768-row inputs. Neither replaces the other.
func runTestStdDevLazyLargeOwned(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, want := stddevQuery(t, mode, stddevLargeRows(), 64, false, false, 64<<20, 48<<10, pair)
			defer s.Close()
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			stddevCheck(t, df.Column, src.rows, want, false)
			s.Close()
			stddevCheck(t, df.Column, src.rows, want, false)
			df.Close()
			stddevClean(t, s, src, dir)
		})
	}
}
func stddevPositive(t *testing.T, d map[string]string, key string) {
	t.Helper()
	n, e := strconv.ParseUint(d[key], 10, 64)
	if e != nil || n == 0 {
		t.Fatalf("missing actual %s diagnostics: %v", key, d)
	}
}
func runTestStdDevLazyLargeStreamLifecycle(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, stop := range []string{"complete", "early", "cancel"} {
			t.Run(mode+"/"+stop, func(t *testing.T) {
				s, q, src, dir, want := stddevQuery(t, mode, stddevLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
					stddevCheck(t, view.Column, src.rows[seen:seen+int(view.Len())], want, false)
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
					if pair {
						stddevPositive(t, reader.diagnostics, "partition_spool_pages")
						stddevPositive(t, reader.diagnostics, "partition_spool_bytes")
						stddevPositive(t, reader.diagnostics, "partition_spool_partitions")

						stddevPositive(t, reader.diagnostics, "partition_replay_pages")
					} else {
						for _, key := range []string{"partition_spool_pages", "partition_spool_bytes", "partition_spool_partitions", "partition_replay_pages", "peer_lookahead_pages"} {
							value := reader.diagnostics[key]
							if value != "" && value != "0" {
								t.Fatal("finite stddev unexpectedly used partition spool", key, value)
							}
						}
					}
					if mode == "external" {
						for _, key := range []string{"front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes"} {
							stddevPositive(t, reader.diagnostics, key)
						}
					}
				}
				t.Logf("mode=%s stop=%s rows=%d peak=%d spill=%d diagnostics=%v", mode, stop, seen, snapshot.PeakBytes, snapshot.SpilledBytes, reader.diagnostics)
				budget.Close()
				stddevClean(t, s, src, dir)
			})
		}
	}
}
func stddevArrowCheck(t *testing.T, r arrow.RecordBatch, rows []stddevRow, want map[int64]stddevExpected) {
	t.Helper()
	if int(r.NumRows()) != len(rows) {
		t.Fatal("record rows")
	}
	for c, f := range r.Schema().Fields() {
		if (f.Name == "pr" || f.Name == "cd") && (f.Nullable || f.Type.ID() != arrow.FLOAT64) {
			t.Fatal("Arrow distribution schema", f)
		}
		if (f.Name == "nt7" || f.Name == "rn") && (f.Nullable || f.Type.ID() != arrow.INT64) {
			t.Fatal("Arrow NTILE schema", f)
		}
		if (f.Name == "vp" || f.Name == "vs" || f.Name == "prefix") && (!f.Nullable || f.Type.ID() != arrow.FLOAT64) {
			t.Fatal("stddev Arrow schema", f)
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
			if w := stddevWant(row, want[row.id], f.Name, false); !stddevValueEqual(got, w) {
				t.Fatal("record oracle", f.Name, row.id, got, w)
			}
		}
	}
}
func runTestStdDevLazyLargeStreamingSinks(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, format := range []string{"ipc", "parquet"} {
			t.Run(mode+"/"+format, func(t *testing.T) {
				s, q, src, dir, want := stddevQuery(t, mode, stddevLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
						stddevArrowCheck(t, r, src.rows[seen:seen+n], want)
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
						stddevArrowCheck(t, r, src.rows[seen:seen+n], want)
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
				stddevClean(t, s, src, dir)
			})
		}
	}
}

func runTestStdDevLazyCancelBeforeOutput(t *testing.T, pair bool) {
	if !pair {
		return
	}
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, _ := stddevQuery(t, mode, stddevLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
			stddevClean(t, s, src, dir)
		})
	}
}

func TestStdDevLazySemantics(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "finite-pop-samp-row"
		if pair {
			name = "prefix-pr-nt7"
		}
		t.Run(name, func(t *testing.T) { runTestStdDevLazySemantics(t, pair) })
	}
}

func TestStdDevLazyLargeOwned(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "finite-pop-samp-row"
		if pair {
			name = "prefix-pr-nt7"
		}
		t.Run(name, func(t *testing.T) { runTestStdDevLazyLargeOwned(t, pair) })
	}
}

func TestStdDevLazyLargeStreamLifecycle(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "finite-pop-samp-row"
		if pair {
			name = "prefix-pr-nt7"
		}
		t.Run(name, func(t *testing.T) { runTestStdDevLazyLargeStreamLifecycle(t, pair) })
	}
}

func TestStdDevLazyLargeStreamingSinks(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "finite-pop-samp-row"
		if pair {
			name = "prefix-pr-nt7"
		}
		t.Run(name, func(t *testing.T) { runTestStdDevLazyLargeStreamingSinks(t, pair) })
	}
}

func TestStdDevLazyCancelBeforeOutput(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "finite-pop-samp-row"
		if pair {
			name = "prefix-pr-nt7"
		}
		t.Run(name, func(t *testing.T) { runTestStdDevLazyCancelBeforeOutput(t, pair) })
	}
}

// Exact rational oracle independent of the production moment accumulator.
// nil is NULL; nonfinite values count toward n but poison only after ddof.
func stddevRatOracle(values []*big.Rat, special []bool, lo, hi, ddof int) any {
	n := int64(0)
	poison := false
	sum, sq := new(big.Rat), new(big.Rat)
	for i := lo; i < hi; i++ {
		if special[i] {
			n++
			poison = true
			continue
		}
		if values[i] == nil {
			continue
		}
		n++
		sum.Add(sum, values[i])
		sq.Add(sq, new(big.Rat).Mul(values[i], values[i]))
	}
	if n <= int64(ddof) {
		return nil
	}
	if poison {
		return math.NaN()
	}
	numerator := new(big.Rat).Sub(new(big.Rat).Mul(big.NewRat(n, 1), sq), new(big.Rat).Mul(sum, sum))
	numerator.Quo(numerator, big.NewRat(n*(n-int64(ddof)), 1))
	return stddevExactRoot(numerator)
}

func TestStdDevLazyTypedExactAndSpecial(t *testing.T) {
	cases := []struct {
		name   string
		typ    arrow.DataType
		values []string
	}{
		{"i8", arrow.PrimitiveTypes.Int8, []string{"127", "126", "", "-128", "-127", "0", "1", ""}},
		{"i16", arrow.PrimitiveTypes.Int16, []string{"32767", "32766", "", "-32768", "0", "1", "2", ""}},
		{"i32", arrow.PrimitiveTypes.Int32, []string{"2147483647", "2147483646", "", "-2147483648", "0", "1", "2", ""}},
		{"i64", arrow.PrimitiveTypes.Int64, []string{"9223372036854775807", "9223372036854775806", "", "-9223372036854775808", "0", "1", "2", ""}},
		{"u8", arrow.PrimitiveTypes.Uint8, []string{"255", "254", "", "0", "1", "2", "3", ""}},
		{"u16", arrow.PrimitiveTypes.Uint16, []string{"65535", "65534", "", "0", "1", "2", "3", ""}},
		{"u32", arrow.PrimitiveTypes.Uint32, []string{"4294967295", "4294967294", "", "0", "1", "2", "3", ""}},
		{"u64", arrow.PrimitiveTypes.Uint64, []string{"18446744073709551615", "18446744073709551614", "", "0", "1", "2", "3", ""}},
		{"f32", arrow.PrimitiveTypes.Float32, []string{"NaN", "", "+Inf", "-Inf", "0.1", "0.2", "3", "4", "5", ""}},
		{"f64", arrow.PrimitiveTypes.Float64, []string{"NaN", "", "+Inf", "-Inf", "1.7976931348623157e308", "-1.7976931348623157e308", "1", "2", "3", "4", "5", ""}},
		{"huge", arrow.PrimitiveTypes.Float64, []string{"1e200", "-1e200", "", "1", "2", "3"}},
		{"tiny", arrow.PrimitiveTypes.Float64, []string{"1e-200", "-1e-200", "", "1", "2", "3"}},
		{"tie-even-down", arrow.PrimitiveTypes.Float64, []string{"2", "-2.220446049250313e-16"}},
		{"tie-even-up", arrow.PrimitiveTypes.Float64, []string{"2", "-6.661338147750939e-16"}},
		{"subnormal-half", arrow.PrimitiveTypes.Float64, []string{"5e-324", "0"}},
	}
	for _, tc := range cases {
		for _, bounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bounded%t", tc.name, bounded), func(t *testing.T) {
				opts := engine.DefaultOptions()
				opts.PreparedCacheBytes = 0
				opts.MemoryLimitBytes = 8 << 20
				opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
				opts.ExecutionPolicy = engine.ExecutionMemoryOnly
				if bounded {
					opts.ExecutionPolicy = engine.ExecutionBounded
				}
				s := NewSession(opts)
				defer s.Close()
				rb := array.NewRecordBuilder(am.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "x", Type: tc.typ, Nullable: true}}, nil))
				values := make([]*big.Rat, len(tc.values))
				special := make([]bool, len(tc.values))
				for i, v := range tc.values {
					rb.Field(0).(*array.Int64Builder).Append(int64(i))
					if v == "" {
						rb.Field(1).AppendNull()
						continue
					}
					if e := rb.Field(1).AppendValueFromString(v); e != nil {
						t.Fatal(e)
					}
					if tc.typ.ID() == arrow.FLOAT32 || tc.typ.ID() == arrow.FLOAT64 {
						bits := 64
						if tc.typ.ID() == arrow.FLOAT32 {
							bits = 32
						}
						f, e := strconv.ParseFloat(v, bits)
						if e != nil {
							t.Fatal(e)
						}
						if math.IsInf(f, 0) || math.IsNaN(f) {
							special[i] = true
						} else {
							values[i] = new(big.Rat).SetFloat64(f)
						}
					} else {
						values[i] = new(big.Rat)
						if _, ok := values[i].SetString(v); !ok {
							t.Fatal(v)
						}
					}
				}
				rec := rb.NewRecordBatch()
				rb.Release()
				var chunks []arrow.RecordBatch
				for i := range tc.values {
					chunks = append(chunks, rec.NewSlice(int64(i), int64(i+1)))
				}
				q, e := s.ScanArrow(chunks...)
				for _, r := range chunks {
					r.Release()
				}
				rec.Release()
				if e != nil {
					t.Fatal(e)
				}
				frames := []RowsFrame{RowsBetween(Preceding(2), CurrentRow), RowsBetween(Following(2), Following(3)), RowsBetween(UnboundedPreceding, CurrentRow), RowsBetween(CurrentRow, CurrentRow)}
				var windows []WindowExpr
				for j, f := range frames {
					windows = append(windows, RollingStdDevPop("x", f, fmt.Sprintf("p%d", j)), RollingStdDevSamp("x", f, fmt.Sprintf("s%d", j)))
				}
				q = q.WithWindows(NewWindow(nil, OrderBy(Asc("id"))), windows...)
				sch, e := q.Schema()
				if e != nil {
					t.Fatal(e)
				}
				for _, w := range windows {
					f, ok := sch.Field(w.Alias)
					if !ok || f.Type != memory.TypeFloat64 || !f.Nullable {
						t.Fatal("stddev metadata", f)
					}
				}
				df, e := q.Collect(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer df.Close()
				for j := range frames {
					for ddof := 0; ddof < 2; ddof++ {
						prefix := "p"
						if ddof == 1 {
							prefix = "s"
						}
						col, e := df.Column(fmt.Sprintf("%s%d", prefix, j))
						if e != nil {
							t.Fatal(e)
						}
						got, e := col.ToAnySlice()
						if e != nil || len(got) != len(values) {
							t.Fatal(e, len(got))
						}
						for i := range values {
							lo, hi := max(0, i-2), i+1
							switch j {
							case 1:
								lo, hi = min(len(values), i+2), min(len(values), i+4)
							case 2:
								lo = 0
							case 3:
								lo = i
							}
							want := stddevRatOracle(values, special, lo, hi, ddof)
							if !stddevValueEqual(got[i], want) {
								t.Fatalf("frame%d ddof%d row%d got%v want%v", j, ddof, i, got[i], want)
							}
						}
					}
				}
				df.Close()
				s.Close()
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("typed owner leak")
				}
			})
		}
	}
}

func TestStdDevLazyRejectUnsupportedFrames(t *testing.T) {
	for _, frame := range []RowsFrame{RowsBetween(UnboundedPreceding, Following(1)), RowsBetween(CurrentRow, UnboundedFollowing), RowsBetween(UnboundedPreceding, UnboundedFollowing), RowsBetween(Preceding(-1), CurrentRow)} {
		for _, sample := range []bool{false, true} {
			s := NewSession(engine.DefaultOptions())
			src := &stddevSource{rows: stddevSmallRows(), batchRows: 2}
			w := RollingStdDevPop("k2", frame, "v")
			if sample {
				w = RollingStdDevSamp("k2", frame, "v")
			}
			q := s.FromSource(src).WithWindows(NewWindow(nil, OrderBy(Asc("id"))), w)
			if p, e := q.Prepare(context.Background()); e == nil {
				p.Close()
				s.Close()
				t.Fatal("unsupported frame accepted", frame)
			}
			if src.opens != 0 {
				t.Fatal("invalid frame opened input")
			}
			s.Close()
		}
	}
}

// Independent oracle: high-precision sqrt gives only a candidate. Exact
// rational squared midpoints certify the final nearest-even binary64 result.
// It never converts the variance to binary64 before taking the root.
func stddevExactRoot(x *big.Rat) float64 {
	if x.Sign() == 0 {
		return 0
	}
	if x.Sign() < 0 {
		panic("negative oracle variance")
	}
	approx := new(big.Float).SetPrec(256).SetMode(big.ToNearestEven).SetRat(x)
	approx.Sqrt(approx)
	candidate, _ := approx.Float64()
	finiteRat := func(v float64) *big.Rat {
		if math.IsInf(v, 1) {
			return new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 1024))
		}
		return new(big.Rat).SetFloat64(v)
	}
	midpointSquare := func(a, b float64) *big.Rat {
		m := new(big.Rat).Add(finiteRat(a), finiteRat(b))
		m.Quo(m, big.NewRat(2, 1))
		return m.Mul(m, m)
	}
	for {
		bits := math.Float64bits(candidate)
		if candidate > 0 {
			lower := math.Nextafter(candidate, 0)
			cmp := x.Cmp(midpointSquare(lower, candidate))
			if cmp < 0 || cmp == 0 && bits&1 != 0 {
				candidate = lower
				continue
			}
		}
		if !math.IsInf(candidate, 1) {
			upper := math.Nextafter(candidate, math.Inf(1))
			cmp := x.Cmp(midpointSquare(candidate, upper))
			if cmp > 0 || cmp == 0 && bits&1 != 0 {
				candidate = upper
				continue
			}
		}
		return candidate
	}
}

func TestStdDevLazyExactRootOracleKnownValues(t *testing.T) {
	cases := []struct {
		x    *big.Rat
		want float64
	}{
		{big.NewRat(1, 1), 1},
		{new(big.Rat).Mul(new(big.Rat).SetFloat64(1e200), new(big.Rat).SetFloat64(1e200)), 1e200},
		{new(big.Rat).Mul(new(big.Rat).SetFloat64(1e-200), new(big.Rat).SetFloat64(1e-200)), 1e-200},
	}
	for _, eps := range []float64{math.Ldexp(1, -52), 3 * math.Ldexp(1, -52)} {
		root := new(big.Rat).Add(big.NewRat(1, 1), new(big.Rat).Quo(new(big.Rat).SetFloat64(eps), big.NewRat(2, 1)))
		want := 1.0
		if eps > math.Ldexp(1, -52) {
			want = 1 + math.Ldexp(1, -51)
		}
		cases = append(cases, struct {
			x    *big.Rat
			want float64
		}{new(big.Rat).Mul(root, root), want})
	}
	halfSub := new(big.Rat).Quo(new(big.Rat).SetFloat64(math.SmallestNonzeroFloat64), big.NewRat(2, 1))
	cases = append(cases, struct {
		x    *big.Rat
		want float64
	}{new(big.Rat).Mul(halfSub, halfSub), 0})
	overflowMid := new(big.Rat).Add(new(big.Rat).SetFloat64(math.MaxFloat64), new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 1024)))
	overflowMid.Quo(overflowMid, big.NewRat(2, 1))
	cases = append(cases, struct {
		x    *big.Rat
		want float64
	}{new(big.Rat).Mul(overflowMid, overflowMid), math.Inf(1)})
	for i, tc := range cases {
		if got := stddevExactRoot(tc.x); math.Float64bits(got) != math.Float64bits(tc.want) {
			t.Fatal(i, got, tc.want)
		}
	}
}
