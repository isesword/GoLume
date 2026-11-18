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

type cumeDistRow struct {
	id, grp, k2          int64
	k1                   float64
	groupValid, keyValid bool
	pad                  string
}
type cumeDistExpected struct {
	rank, row, dense    int64
	percent, cumulative float64
	pair                bool
}

func cumeDistCompare(a, b cumeDistRow, desc bool) int {
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
func cumeDistSameGroup(a, b cumeDistRow) bool {
	return a.groupValid == b.groupValid && (!a.groupValid || a.grp == b.grp)
}
func cumeDistOrder(rows []cumeDistRow, desc bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.groupValid != b.groupValid {
			return !a.groupValid
		}
		if a.groupValid && a.grp != b.grp {
			return a.grp < b.grp
		}
		return cumeDistCompare(a, b, desc) < 0
	})
}
func cumeDistOracle(rows []cumeDistRow, desc bool, pair bool) map[int64]cumeDistExpected {
	ordered := append([]cumeDistRow(nil), rows...)
	cumeDistOrder(ordered, desc)
	out := map[int64]cumeDistExpected{}
	for begin := 0; begin < len(ordered); {
		end := begin + 1
		for end < len(ordered) && cumeDistSameGroup(ordered[begin], ordered[end]) {
			end++
		}
		// Independent group-at-a-time oracle: peerEnd is exclusive and is
		// measured from the partition start, never from a transport page.
		dense := int64(0)
		for peerBegin := begin; peerBegin < end; {
			peerEnd := peerBegin + 1
			for peerEnd < end && cumeDistCompare(ordered[peerBegin], ordered[peerEnd], desc) == 0 {
				peerEnd++
			}
			dense++
			rank := int64(peerBegin - begin + 1)
			p := 0.0
			if end-begin > 1 {
				p = float64(rank-1) / float64(end-begin-1)
			}
			for i := peerBegin; i < peerEnd; i++ {
				out[ordered[i].id] = cumeDistExpected{rank: rank, row: int64(i - begin + 1), dense: dense, percent: p, cumulative: float64(peerEnd-begin) / float64(end-begin), pair: pair}
			}
			peerBegin = peerEnd
		}
		begin = end
	}
	return out
}
func cumeDistSmallRows() []cumeDistRow {
	vals := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, 1, math.Inf(1), math.NaN(), math.Float64frombits(0x7ff8000000000011), 0}
	var rows []cumeDistRow
	for i := 0; i < 27; i++ {
		rows = append(rows, cumeDistRow{id: int64(i), grp: 0, k1: vals[i%9], k2: int64((i / 9) % 2), groupValid: true, keyValid: i%9 != 8, pad: strings.Repeat(string(rune('a'+i%26)), 64)})
	}
	rows = append(rows, cumeDistRow{id: 27, grp: 1, k1: 2, groupValid: true, keyValid: true, pad: strings.Repeat("s", 64)})
	for i := 28; i < 33; i++ {
		rows = append(rows, cumeDistRow{id: int64(i), grp: 2, k1: 7, groupValid: i < 31, keyValid: true, pad: strings.Repeat("p", 64)})
	}
	for i := len(rows) - 1; i > 0; i-- {
		j := (i*17 + 3) % (i + 1)
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}
func cumeDistLargeRows() []cumeDistRow {
	rows := make([]cumeDistRow, 32768)
	for i := range rows {
		rows[i] = cumeDistRow{id: int64(i), grp: 0, k1: float64((i / 128) % 8), k2: int64((i / 2048) % 2), groupValid: true, keyValid: i%193 != 0, pad: strings.Repeat(string(rune('a'+i%26)), 64)}
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
type cumeDistSource struct {
	rows          []cumeDistRow
	batchRows     int
	ordered, desc bool
	opens, closes int
	cancelAfter   int
	cancel        context.CancelFunc
}

func (s *cumeDistSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "grp", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "k1", Type: memory.TypeFloat64, Nullable: true}, memory.Field{Name: "k2", Type: memory.TypeInt64}, memory.Field{Name: "pad", Type: memory.TypeString, Encoding: memory.EncodingView})
}
func (s *cumeDistSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: int64(len(s.rows)), Partitions: 1, Bytes: int64(len(s.rows)) * 104, RowsUpperBound: true}, nil
}
func (s *cumeDistSource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: int64(len(s.rows))}}, nil
}
func (s *cumeDistSource) Ordering() []source.OrderingColumn {
	if !s.ordered {
		return nil
	}
	return []source.OrderingColumn{{Name: "grp"}, {Name: "k1", Descending: s.desc}, {Name: "k2", NullsLast: true}}
}
func (s *cumeDistSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	return s.NewBatchStreamWithPartitions(ctx, o, []source.Partition{{ID: 0, Rows: int64(len(s.rows))}})
}
func (s *cumeDistSource) NewBatchStreamWithPartitions(ctx context.Context, o source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if e := o.Validate(); e != nil {
		return nil, e
	}
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("PercentRank reopened original source")
	}
	if len(parts) != 1 || parts[0].ID != 0 {
		return nil, fmt.Errorf("unexpected source partitions")
	}
	n := s.batchRows
	if o.BatchRows > 0 {
		n = min(n, o.BatchRows)
	}
	return &cumeDistStream{owner: s, cap: n, id: make([]int64, n), grp: make([]int64, n), k2: make([]int64, n), k1: make([]float64, n), gv: memory.EnsureBitmap(nil, n), kv: memory.EnsureBitmap(nil, n), offsets: make([]uint32, n+1), data: make([]byte, n*64)}, nil
}

type cumeDistStream struct {
	owner       *cumeDistSource
	cap, pos    int
	closed      bool
	id, grp, k2 []int64
	k1          []float64
	gv, kv      memory.Bitmap
	offsets     []uint32
	data        []byte
}

func (s *cumeDistStream) poison() {
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
func (s *cumeDistStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
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
func (s *cumeDistStream) Close() error {
	if !s.closed {
		s.closed = true
		s.poison()
		s.owner.closes++
	}
	return nil
}

func cumeDistQuery(t *testing.T, mode string, rows []cumeDistRow, batchRows int, desc, mixed bool, hard int64, soft int64, pair bool) (*Session, LazyFrame, *cumeDistSource, string, map[int64]cumeDistExpected) {
	t.Helper()
	input := append([]cumeDistRow(nil), rows...)
	if mode == "ordered" {
		cumeDistOrder(input, desc)
	}
	src := &cumeDistSource{rows: input, batchRows: batchRows, ordered: mode == "ordered", desc: desc}
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
	expressions := []WindowExpr{CumeDist("cd"), Rank("rk"), RowNumber("rn")}
	if pair {
		expressions = []WindowExpr{PercentRank("pr"), CumeDist("cd"), RowNumber("rn")}
	}
	if mixed {
		expressions = append(expressions, DenseRank("dense"), CumeDist("k2"))
	}
	q := s.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), keys), expressions...)
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
	floatOutputs := []string{"cd"}
	if pair {
		floatOutputs = append(floatOutputs, "pr")
	}
	for _, name := range floatOutputs {
		f, ok := sch.Field(name)
		if !ok || f.Type != memory.TypeFloat64 || f.Nullable {
			s.Close()
			t.Fatal("distribution schema", f)
		}
	}
	return s, q, src, dir, cumeDistOracle(input, desc, pair)
}
func cumeDistValueEqual(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return af == bf || math.IsNaN(af) && math.IsNaN(bf)
	}
	return reflect.DeepEqual(a, b)
}
func cumeDistWant(r cumeDistRow, e cumeDistExpected, name string, mixed bool) any {
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
			return e.cumulative
		}
		return r.k2
	case "pad":
		return r.pad
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
func cumeDistColumns(mixed, pair bool) []string {
	n := []string{"id", "grp", "k1", "k2", "pad", "cd", "rk", "rn"}
	if pair {
		n[6] = "pr"
	}
	if mixed {
		n = append(n, "dense")
	}
	return n
}
func cumeDistCheck(t *testing.T, column func(string) (Series, error), rows []cumeDistRow, want map[int64]cumeDistExpected, mixed bool) {
	t.Helper()
	pair := false
	if len(rows) > 0 {
		pair = want[rows[0].id].pair
	}
	for _, name := range cumeDistColumns(mixed, pair) {
		c, e := column(name)
		if e != nil {
			t.Fatal(e)
		}
		v, e := c.ToAnySlice()
		if e != nil || len(v) != len(rows) {
			t.Fatal(name, len(v), len(rows), e)
		}
		for i, r := range rows {
			w := cumeDistWant(r, want[r.id], name, mixed)
			if !cumeDistValueEqual(v[i], w) {
				t.Fatalf("%s row%d id%d got%v want%v", name, i, r.id, v[i], w)
			}
		}
	}
}
func cumeDistClean(t *testing.T, s *Session, src *cumeDistSource, dir string) {
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
func runTestCumeDistLazySemantics(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, bs := range []int{1, 2, 7} {
			for _, desc := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/batch%d/desc%t", mode, bs, desc), func(t *testing.T) {
					s, q, src, dir, want := cumeDistQuery(t, mode, cumeDistSmallRows(), bs, desc, true, 8<<20, 0, pair)
					defer s.Close()
					df, e := q.Collect(context.Background())
					if e != nil {
						t.Fatal(e)
					}
					defer df.Close()
					cumeDistCheck(t, df.Column, src.rows, want, true)
					s.Close()
					cumeDistCheck(t, df.Column, src.rows, want, true)
					df.Close()
					cumeDistClean(t, s, src, dir)
				})
			}
		}
	}
}
func runTestCumeDistLazyRequiresOrder(t *testing.T, pair bool) {
	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	src := &cumeDistSource{rows: cumeDistSmallRows(), batchRows: 7}
	expressions := []WindowExpr{CumeDist("cd")}
	if pair {
		expressions = append(expressions, PercentRank("pr"))
	}
	q := s.FromSource(src).WithWindows(NewWindow(PartitionBy("grp"), nil), expressions...)
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
func runTestCumeDistLazyLargeOwned(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, want := cumeDistQuery(t, mode, cumeDistLargeRows(), 64, false, false, 64<<20, 48<<10, pair)
			defer s.Close()
			df, e := q.Collect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			defer df.Close()
			cumeDistCheck(t, df.Column, src.rows, want, false)
			s.Close()
			cumeDistCheck(t, df.Column, src.rows, want, false)
			df.Close()
			cumeDistClean(t, s, src, dir)
		})
	}
}
func cumeDistPositive(t *testing.T, d map[string]string, key string) {
	t.Helper()
	n, e := strconv.ParseUint(d[key], 10, 64)
	if e != nil || n == 0 {
		t.Fatalf("missing actual %s diagnostics: %v", key, d)
	}
}
func runTestCumeDistLazyLargeStreamLifecycle(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, stop := range []string{"complete", "early", "cancel"} {
			t.Run(mode+"/"+stop, func(t *testing.T) {
				s, q, src, dir, want := cumeDistQuery(t, mode, cumeDistLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
					cumeDistCheck(t, view.Column, src.rows[seen:seen+int(view.Len())], want, false)
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
					cumeDistPositive(t, reader.diagnostics, "partition_spool_pages")
					cumeDistPositive(t, reader.diagnostics, "partition_spool_bytes")
					cumeDistPositive(t, reader.diagnostics, "partition_spool_partitions")
					cumeDistPositive(t, reader.diagnostics, "peer_lookahead_pages")
					cumeDistPositive(t, reader.diagnostics, "partition_replay_pages")
					if mode == "external" {
						for _, key := range []string{"front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes"} {
							cumeDistPositive(t, reader.diagnostics, key)
						}
					}
				}
				t.Logf("mode=%s stop=%s rows=%d peak=%d spill=%d diagnostics=%v", mode, stop, seen, snapshot.PeakBytes, snapshot.SpilledBytes, reader.diagnostics)
				budget.Close()
				cumeDistClean(t, s, src, dir)
			})
		}
	}
}
func cumeDistArrowCheck(t *testing.T, r arrow.RecordBatch, rows []cumeDistRow, want map[int64]cumeDistExpected) {
	t.Helper()
	if int(r.NumRows()) != len(rows) {
		t.Fatal("record rows")
	}
	for c, f := range r.Schema().Fields() {
		if (f.Name == "pr" || f.Name == "cd") && (f.Nullable || f.Type.ID() != arrow.FLOAT64) {
			t.Fatal("Arrow distribution schema", f)
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
			if w := cumeDistWant(row, want[row.id], f.Name, false); !cumeDistValueEqual(got, w) {
				t.Fatal("record oracle", f.Name, row.id, got, w)
			}
		}
	}
}
func runTestCumeDistLazyLargeStreamingSinks(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		for _, format := range []string{"ipc", "parquet"} {
			t.Run(mode+"/"+format, func(t *testing.T) {
				s, q, src, dir, want := cumeDistQuery(t, mode, cumeDistLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
						cumeDistArrowCheck(t, r, src.rows[seen:seen+n], want)
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
						cumeDistArrowCheck(t, r, src.rows[seen:seen+n], want)
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
				cumeDistClean(t, s, src, dir)
			})
		}
	}
}

func runTestCumeDistLazyCancelBeforeOutput(t *testing.T, pair bool) {
	for _, mode := range []string{"resident", "ordered", "external"} {
		t.Run(mode, func(t *testing.T) {
			s, q, src, dir, _ := cumeDistQuery(t, mode, cumeDistLargeRows(), 64, false, false, 8<<20, 48<<10, pair)
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
			cumeDistClean(t, s, src, dir)
		})
	}
}

func TestCumeDistLazySemantics(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazySemantics(t, pair) })
	}
}

func TestCumeDistLazyRequiresOrder(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazyRequiresOrder(t, pair) })
	}
}

func TestCumeDistLazyLargeOwned(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazyLargeOwned(t, pair) })
	}
}

func TestCumeDistLazyLargeStreamLifecycle(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazyLargeStreamLifecycle(t, pair) })
	}
}

func TestCumeDistLazyLargeStreamingSinks(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazyLargeStreamingSinks(t, pair) })
	}
}

func TestCumeDistLazyCancelBeforeOutput(t *testing.T) {
	for _, pair := range []bool{false, true} {
		name := "cd-rank-row"
		if pair {
			name = "pr-cd-row"
		}
		t.Run(name, func(t *testing.T) { runTestCumeDistLazyCancelBeforeOutput(t, pair) })
	}
}
