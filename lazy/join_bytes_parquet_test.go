package lazy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
)

type parquetJoinRow struct {
	s, x  string
	n     int64
	valid bool
}

func plainJoinParquet(tb testing.TB, id string, count int) (source.Source, []parquetJoinRow) {
	tb.Helper()
	text, err := schema.NewPrimitiveNodeLogical("s", arrowparquet.Repetitions.Optional, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if err != nil {
		tb.Fatal(err)
	}
	extra, err := schema.NewPrimitiveNodeLogical("x", arrowparquet.Repetitions.Required, schema.StringLogicalType{}, arrowparquet.Types.ByteArray, -1, -1)
	if err != nil {
		tb.Fatal(err)
	}
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{text, extra, schema.NewInt64Node("n", arrowparquet.Repetitions.Required, -1), schema.NewInt64Node(id, arrowparquet.Repetitions.Required, -1)}, -1)
	if err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(tb.TempDir(), id+".parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(false))))
	rows := make([]parquetJoinRow, count)
	for i := range rows {
		s := fmt.Sprintf("用户🙂\x00%d", i%11)
		if i%23 == 0 {
			s = ""
		}
		rows[i] = parquetJoinRow{s: s, x: fmt.Sprintf("x%d", i%2), n: int64(i % 3), valid: i%17 != 0}
	}
	for start := 0; start < count; start += 64 {
		end := start + 64
		if end > count {
			end = count
		}
		rg := writer.AppendRowGroup()
		var strings, xs []arrowparquet.ByteArray
		defs := make([]int16, end-start)
		ns, ids := make([]int64, end-start), make([]int64, end-start)
		for i, row := range rows[start:end] {
			if row.valid {
				strings = append(strings, arrowparquet.ByteArray(row.s))
				defs[i] = 1
			}
			xs = append(xs, arrowparquet.ByteArray(row.x))
			ns[i] = row.n
			ids[i] = int64(start + i)
		}
		for col := 0; col < 4; col++ {
			c, err := rg.NextColumn()
			if err != nil {
				tb.Fatal(err)
			}
			switch col {
			case 0:
				_, err = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(strings, defs, nil)
			case 1:
				_, err = c.(*file.ByteArrayColumnChunkWriter).WriteBatch(xs, nil, nil)
			case 2:
				_, err = c.(*file.Int64ColumnChunkWriter).WriteBatch(ns, nil, nil)
			case 3:
				_, err = c.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil)
			}
			if err != nil {
				tb.Fatal(err)
			}
			if err = c.Close(); err != nil {
				tb.Fatal(err)
			}
		}
		if err = rg.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		tb.Fatal(err)
	}
	src, err := parquetsource.OpenFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	field, _ := src.Schema().Field("s")
	if field.Type != memory.TypeString || field.Encoding == memory.EncodingDictionary {
		tb.Fatalf("expected actual non-dictionary UTF8 source, got %+v", field)
	}
	return src, rows
}

func parquetJoinReference(left, right []parquetJoinRow, keys int, typ JoinType) []string {
	var got []string
	matched := make([]bool, len(right))
	for li, l := range left {
		found := false
		for ri, r := range right {
			if !l.valid || !r.valid || l.s != r.s || (keys >= 2 && l.x != r.x) || (keys >= 3 && l.n != r.n) {
				continue
			}
			found = true
			matched[ri] = true
			if typ != AntiJoin {
				if typ == SemiJoin {
					got = append(got, fmt.Sprintf("%d/-1", li))
					break
				}
				got = append(got, fmt.Sprintf("%d/%d", li, ri))
			}
		}
		if !found && (typ == LeftJoin || typ == FullJoin || typ == AntiJoin) {
			got = append(got, fmt.Sprintf("%d/-1", li))
		}
	}
	if typ == RightJoin || typ == FullJoin {
		for ri, m := range matched {
			if !m {
				got = append(got, fmt.Sprintf("-1/%d", ri))
			}
		}
	}
	sort.Strings(got)
	return got
}

func TestPlainStringAndCompositeParquetJoins(t *testing.T) {
	left, lr := plainJoinParquet(t, "lid", 137)
	right, rr := plainJoinParquet(t, "rid", 75)
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = engine.ExecutionMemoryOnly
	opts.MemoryLimitBytes = 32 << 20
	s := NewSession(opts)
	defer s.Close()
	names := []string{"s", "x", "n"}
	for keys := 1; keys <= 3; keys++ {
		for typ := InnerJoin; typ <= FullJoin; typ++ {
			t.Run(fmt.Sprintf("keys%d/%s", keys, typ), func(t *testing.T) {
				frame := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: typ, LeftOn: names[:keys], RightOn: names[:keys]})
				want := parquetJoinReference(lr, rr, keys, typ)
				for repeat := 0; repeat < 2; repeat++ {
					result, err := frame.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					var got []string
					for _, batch := range result.batches() {
						for i := 0; i < batch.Rows; i++ {
							li, ri := int64(-1), int64(-1)
							if v := batch.Int64s["lid"]; v != nil && v.IsValid(i) {
								li = v.Values[i]
							}
							if v := batch.Int64s["rid"]; v != nil && v.IsValid(i) {
								ri = v.Values[i]
							}
							got = append(got, fmt.Sprintf("%d/%d", li, ri))
						}
					}
					result.Close()
					sort.Strings(got)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("repeat=%d got=%d want=%d (pair mismatch)", repeat, len(got), len(want))
					}
				}
			})
		}
	}
}

func TestBoundedStringAndCompositeParquetJoins(t *testing.T) {
	left, lr := plainJoinParquet(t, "lid", 137)
	right, rr := plainJoinParquet(t, "rid", 75)
	opts := engine.DefaultOptions()
	opts.ExecutionPolicy = engine.ExecutionBounded
	opts.Spill = engine.SpillOptions{Directory: t.TempDir(), MaxBytes: 128 << 20}
	opts.MemoryLimitBytes = 4 << 20
	s := NewSession(opts)
	defer s.Close()
	names := []string{"s", "x", "n"}
	for keys := 1; keys <= 3; keys++ {
		for typ := InnerJoin; typ <= FullJoin; typ++ {
			t.Run(fmt.Sprintf("keys%d/%s", keys, typ), func(t *testing.T) {
				frame := s.FromSource(left).Join(s.FromSource(right), JoinOptions{Type: typ, LeftOn: names[:keys], RightOn: names[:keys]})
				want := parquetJoinReference(lr, rr, keys, typ)
				for repeat := 0; repeat < 2; repeat++ {
					result, err := frame.Collect(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					var got []string
					for _, batch := range result.batches() {
						for i := 0; i < batch.Rows; i++ {
							li, ri := int64(-1), int64(-1)
							if v := batch.Int64s["lid"]; v != nil && v.IsValid(i) {
								li = v.Values[i]
							}
							if v := batch.Int64s["rid"]; v != nil && v.IsValid(i) {
								ri = v.Values[i]
							}
							got = append(got, fmt.Sprintf("%d/%d", li, ri))
						}
					}
					result.Close()
					sort.Strings(got)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("repeat=%d got=%d want=%d (pair mismatch)", repeat, len(got), len(want))
					}
				}
			})
		}
	}
}

func BenchmarkPlainStringParquetJoin(b *testing.B) {
	left, _ := plainJoinParquet(b, "lid", 4096)
	right, _ := plainJoinParquet(b, "rid", 512)
	names := []string{"s", "x", "n"}
	for _, keys := range []int{1, 3} {
		b.Run(fmt.Sprintf("Keys%d", keys), func(b *testing.B) {
			opts := engine.DefaultOptions()
			opts.ExecutionPolicy = engine.ExecutionMemoryOnly
			opts.Workers = 1
			session := NewSession(opts)
			defer session.Close()
			frame := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: InnerJoin, LeftOn: names[:keys], RightOn: names[:keys]})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := frame.Collect(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if result.Height() == 0 {
					b.Fatal("no result")
				}
				result.Close()
			}
		})
	}
}

func TestGenericParquetJoinSortTopKWindowComposition(t *testing.T) {
	left, lr := plainJoinParquet(t, "lid", 137)
	right, rr := plainJoinParquet(t, "rid", 75)
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			opts := engine.DefaultOptions()
			opts.ExecutionPolicy = engine.ExecutionMemoryOnly
			opts.MemoryLimitBytes = 32 << 20
			opts.Workers = workers
			session := NewSession(opts)
			defer session.Close()
			query := session.FromSource(left).Join(session.FromSource(right), JoinOptions{Type: FullJoin, LeftOn: []string{"s", "x", "n"}, RightOn: []string{"s", "x", "n"}})
			reference := parquetJoinReference(lr, rr, 3, FullJoin)
			pairs := make([][2]int64, len(reference))
			for i, pair := range reference {
				if _, err := fmt.Sscanf(pair, "%d/%d", &pairs[i][0], &pairs[i][1]); err != nil {
					t.Fatal(err)
				}
			}
			lessInt := func(a, b int64, desc bool) bool {
				if a < 0 || b < 0 {
					return a >= 0 && b < 0
				}
				if desc {
					return a > b
				}
				return a < b
			}
			stringKey := func(pair [2]int64) (string, bool) {
				if pair[0] < 0 || !lr[pair[0]].valid {
					return "", false
				}
				return lr[pair[0]].s, true
			}
			sort.Slice(pairs, func(i, j int) bool {
				a, b := pairs[i], pairs[j]
				av, aok := stringKey(a)
				bv, bok := stringKey(b)
				if aok != bok {
					return aok
				}
				if av != bv {
					return av < bv
				}
				if a[0] != b[0] {
					return lessInt(a[0], b[0], false)
				}
				return lessInt(a[1], b[1], true)
			})
			order := []SortExpr{{Expr: Col("s"), NullsLast: true}, {Expr: Col("lid"), NullsLast: true}, {Expr: Col("rid"), Descending: true, NullsLast: true}}
			rank := make(map[[2]int64]int64, len(pairs))
			var previous string
			var previousNull bool
			var current int64
			for i, pair := range pairs {
				group, valid := stringKey(pair)
				null := !valid
				if i == 0 || null != previousNull || group != previous {
					current = 0
				}
				current++
				rank[pair] = current
				previous, previousNull = group, null
			}
			variants := []struct {
				name   string
				frame  LazyFrame
				limit  int
				window bool
			}{
				{"sort", query.SortBy(order...), len(pairs), false},
				{"sort-limit", query.SortBy(order...).Limit(37), 37, false},
				{"topk", query.TopK(37, order...), 37, false},
				{"window-sort", query.WithWindows(NewWindow(PartitionBy("s"), OrderBy(order[1:]...)), RowNumber("rn")).SortBy(order...), len(pairs), true},
			}
			for _, variant := range variants {
				t.Run(variant.name, func(t *testing.T) {
					prepared, err := variant.frame.Prepare(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer prepared.Close()
					for repeat := 0; repeat < 2; repeat++ {
						budget, err := session.Engine().BeginQueryMemory()
						if err != nil {
							t.Fatal(err)
						}
						result, err := prepared.Execute(memory.WithQueryBudget(context.Background(), budget))
						if err != nil {
							budget.Close()
							t.Fatal(err)
						}
						at := 0
						for _, batch := range result.batches() {
							for row := 0; row < batch.Rows; row++ {
								pair := [2]int64{-1, -1}
								if batch.Int64s["lid"].IsValid(row) {
									pair[0] = batch.Int64s["lid"].Values[row]
								}
								if batch.Int64s["rid"].IsValid(row) {
									pair[1] = batch.Int64s["rid"].Values[row]
								}
								if at >= variant.limit || pair != pairs[at] {
									t.Fatalf("row=%d got=%v expected=%v", at, pair, pairs)
								}
								if variant.window && batch.Int64s["rn"].Values[row] != rank[pair] {
									t.Fatalf("pair=%v rank=%d want=%d", pair, batch.Int64s["rn"].Values[row], rank[pair])
								}
								at++
							}
						}
						if at != variant.limit {
							t.Fatalf("rows=%d want=%d", at, variant.limit)
						}
						if err := result.Close(); err != nil {
							t.Fatal(err)
						}
						snapshot := budget.Snapshot()
						if snapshot.UsedBytes != 0 || snapshot.PeakBytes > budget.Limit() {
							t.Fatalf("leaked/exceeded query budget: %+v", snapshot)
						}
						if err := budget.Close(); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
