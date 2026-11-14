package parquet

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

// The data alternates 1/100, so every data page and the footer retain a query
// for absent 50. NULLs are explicit. No external fixture is used or changed.
func recoveryBloomFixture(t testing.TB, rows int, enabled bool, ndv int64, customValue ...func(int) int64) *Source {
	return recoveryBloomConfiguredFixture(t, rows, 1, enabled, ndv, nil, customValue...)
}
func recoveryBloomConfiguredFixture(t testing.TB, rows, groups int, enabled bool, ndv int64, extra []pq.WriterProperty, customValue ...func(int) int64) *Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "independent-bloom.parquet")
	root, err := schema.NewGroupNode("schema", pq.Repetitions.Required, schema.FieldList{
		schema.NewInt64Node("x", pq.Repetitions.Optional, -1), schema.NewInt64Node("id", pq.Repetitions.Required, -1),
	}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	props := pq.NewWriterProperties(append([]pq.WriterProperty{pq.WithDictionaryDefault(false), pq.WithPageIndexEnabled(true), pq.WithBloomFilterEnabled(enabled), pq.WithBloomFilterNDV(ndv), pq.WithDataPageSize(8192)}, extra...)...)
	w := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	for group := 0; group < groups; group++ {
		rg := w.AppendRowGroup()
		x, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		var values []int64
		defs := make([]int16, rows)
		ids := make([]int64, rows)
		for i := 0; i < rows; i++ {
			ids[i] = int64(group*rows + i)
			if rows <= 2 || i%7 != 0 {
				defs[i] = 1
				v := int64(1)
				if i%2 != 0 {
					v = 100
				}
				if len(customValue) > 0 {
					v = customValue[0](i)
				}
				values = append(values, v)
			}
		}
		if _, err = x.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err = x.Close(); err != nil {
			t.Fatal(err)
		}
		id, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = id.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err = id.Close(); err != nil {
			t.Fatal(err)
		}
		if err = rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return openBatchStreamFixture(t, path)
}

func TestRecoveryBloomIndependentSQLTruth(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2)
	x := expr.Col("x")
	null := expr.Null(memory.TypeInt64)
	for _, tc := range []struct {
		name  string
		p     expr.Expr
		match func(int) bool
	}{
		{"miss", expr.EQ(x, expr.I64(50)), func(i int) bool { return false }},
		{"reversed_miss", expr.EQ(expr.I64(50), x), func(i int) bool { return false }},
		{"hit", expr.EQ(x, expr.I64(1)), func(i int) bool { return i%7 != 0 && i%2 == 0 }},
		{"not_miss", expr.Not(expr.EQ(x, expr.I64(50))), func(i int) bool { return i%7 != 0 }},
		{"in_miss_null", expr.In(x, expr.I64(50), null), func(i int) bool { return false }},
		{"not_in_miss_null", expr.Not(expr.In(x, expr.I64(50), null)), func(i int) bool { return false }},
		{"in_hit_null", expr.In(x, expr.I64(1), null), func(i int) bool { return i%7 != 0 && i%2 == 0 }},
		{"or_other", expr.Or(expr.EQ(x, expr.I64(50)), expr.EQ(expr.Col("id"), expr.I64(11))), func(i int) bool { return i == 11 }},
		{"null_or_miss", expr.Or(expr.EQ(x, expr.I64(50)), expr.IsNull(x)), func(i int) bool { return i%7 == 0 }},
		{"and_miss", expr.And(expr.EQ(x, expr.I64(50)), expr.EQ(expr.Col("id"), expr.I64(11))), func(i int) bool { return false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.p})
			if err != nil {
				t.Fatal(err)
			}
			var want []int64
			for i := 0; i < 16384; i++ {
				if tc.match(i) {
					want = append(want, int64(i))
				}
			}
			for _, input := range []source.Source{src, pushed} {
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					got := literalInMatchedIDs(t, input, tc.p, mode, 257)
					if !slices.Equal(got, want) {
						t.Fatalf("mode=%v got=%d want=%d", mode, len(got), len(want))
					}
				}
			}
		})
	}
}

// Every timed iteration opens, reads, evaluates the residual and closes the
// same prepared source. The only arm difference is the per-stream Bloom flag.
func BenchmarkRecoveryBloomEndToEnd(b *testing.B) {
	for _, tc := range []struct {
		name         string
		rows, groups int
		enabled      bool
		ndv, value   int64
	}{
		{"miss", 16384, 1, true, 2, 50}, {"hit", 16384, 1, true, 2, 1},
		{"absent", 16384, 1, false, 2, 50}, {"oversized", 16384, 1, true, 0, 50}, {"tiny", 2, 1, true, 2, 50}, {"many_miss", 4096, 128, true, 2, 50}, {"many_hit", 4096, 128, true, 2, 1},
	} {
		src := recoveryBloomConfiguredFixture(b, tc.rows, tc.groups, tc.enabled, tc.ndv, nil)
		predicate := expr.EQ(expr.Col("x"), expr.I64(tc.value))
		prepared, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			b.Fatal(err)
		}
		pushed := prepared.(*Source)
		want := 0
		for i := 0; i < tc.rows; i++ {
			if (tc.rows <= 2 || i%7 != 0) && i%2 == 0 && tc.value == 1 {
				want++
			}
		}
		want *= tc.groups
		for _, arm := range []struct {
			name    string
			disable bool
		}{{"baseline", true}, {"candidate", false}} {
			b.Run(tc.name+"/"+arm.name, func(b *testing.B) {
				ctx := context.Background()
				opts := testReadOptions().Normalize()
				program, _, err := expr.CompilePredicate(pushed.Schema(), predicate)
				if err != nil {
					b.Fatal(err)
				}
				ws, err := expr.NewWorkspace(ctx, program, nil)
				if err != nil {
					b.Fatal(err)
				}
				defer ws.Close()
				batch := memory.NewBatch()
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					input, err := pushed.NewBatchStream(ctx, opts)
					if err != nil {
						b.Fatal(err)
					}
					stream := input.(*batchStream)
					stream.disableBloomPruning = arm.disable
					matches := 0
					for {
						ok, err := stream.Next(ctx, batch)
						if err != nil {
							b.Fatal(err)
						}
						if !ok {
							break
						}
						selected, err := ws.EvaluateBool(ctx, batch, opts.Kernels)
						if err != nil {
							b.Fatal(err)
						}
						for row := 0; row < batch.Rows; row++ {
							if memory.IsSet(selected.Truth, row) && (selected.Validity == nil || memory.IsSet(selected.Validity, row)) {
								matches++
							}
						}
					}
					if err = stream.Close(); err != nil {
						b.Fatal(err)
					}
					if matches != want {
						b.Fatalf("matches=%d want=%d", matches, want)
					}
				}
			})
		}
	}
}

func recoveryBloomDecoded(t *testing.T, src *Source, disable bool) (rows int, bytes int64) {
	t.Helper()
	ctx := context.Background()
	opts := testReadOptions().Normalize()
	f, err := os.Open(src.path)
	if err != nil {
		t.Fatal(err)
	}
	counted := &pagePruningIOReader{f: f}
	props := pq.NewReaderProperties(nil)
	props.BufferedStreamEnabled = true
	props.PageStreamingEnabled = true
	r, err := file.NewParquetReader(counted, file.WithReadProps(props))
	if err != nil {
		t.Fatal(err)
	}
	stream := src.batchStreamFromReader(ctx, opts, src.parts, r)
	stream.disableBloomPruning = disable
	defer stream.Close()
	batch := memory.NewBatch()
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rows += batch.Rows
	}
	for _, interval := range counted.reads {
		bytes += interval.end - interval.first
	}
	return
}

func TestRecoveryBloomIndependentReadsAndFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    int
		enabled bool
		ndv     int64
		corrupt bool
		prunes  bool
	}{
		{"miss", 16384, true, 2, false, true}, {"absent", 16384, false, 2, false, false},
		{"oversized", 16384, true, 0, false, false}, {"tiny", 2, true, 2, false, false},
		{"corrupt_header", 16384, true, 2, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := recoveryBloomFixture(t, tc.rows, tc.enabled, tc.ndv)
			if tc.corrupt {
				cc, err := src.meta.RowGroup(0).ColumnChunk(0)
				if err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(src.path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte{0xff}, cc.BloomFilterOffset())
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.EQ(expr.Col("x"), expr.I64(50))})
			if err != nil {
				t.Fatal(err)
			}
			baseline, baseBytes := recoveryBloomDecoded(t, pushed.(*Source), true)
			candidate, candBytes := recoveryBloomDecoded(t, pushed.(*Source), false)
			if baseline != tc.rows {
				t.Fatalf("baseline decoded=%d want=%d", baseline, tc.rows)
			}
			if tc.prunes {
				if candidate != 0 || candBytes >= baseBytes {
					t.Fatalf("miss rows=%d candidate bytes=%d base bytes=%d", candidate, candBytes, baseBytes)
				}
			} else if candidate != baseline {
				t.Fatalf("fallback decoded=%d baseline=%d", candidate, baseline)
			}
			t.Logf("decoded baseline=%d candidate=%d; actual read bytes baseline=%d candidate=%d", baseline, candidate, baseBytes, candBytes)
		})
	}
}

func TestRecoveryBloomIndependentExtremeInt64(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2, func(i int) int64 {
		if i%2 == 0 {
			return math.MinInt64
		}
		return math.MaxInt64
	})
	for _, v := range []int64{math.MinInt64, math.MaxInt64, -50, 50} {
		predicate := expr.EQ(expr.Col("x"), expr.I64(v))
		pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
		if err != nil {
			t.Fatal(err)
		}
		var want []int64
		for i := 0; i < 16384; i++ {
			if i%7 != 0 && ((i%2 == 0 && v == math.MinInt64) || (i%2 != 0 && v == math.MaxInt64)) {
				want = append(want, int64(i))
			}
		}
		got := literalInMatchedIDs(t, pushed, predicate, kernel.ModeScalar, 257)
		if !slices.Equal(got, want) {
			t.Fatalf("value=%d got=%d want=%d", v, len(got), len(want))
		}
		if v == -50 || v == 50 {
			rows, _ := recoveryBloomDecoded(t, pushed.(*Source), false)
			if rows != 0 {
				t.Fatalf("extreme fixture miss decoded %d", rows)
			}
		}
	}
}

func TestRecoveryBloomIndependentMetadataAliases(t *testing.T) {
	for _, which := range []string{"other_bloom", "data", "column_index", "offset_index"} {
		t.Run(which, func(t *testing.T) {
			src := recoveryBloomFixture(t, 16384, true, 2)
			target := src.meta.RowGroups[0].Columns[0]
			other := src.meta.RowGroups[0].Columns[1]
			switch which {
			case "other_bloom":
				other.MetaData.BloomFilterOffset = target.MetaData.BloomFilterOffset
				other.MetaData.BloomFilterLength = target.MetaData.BloomFilterLength
			case "data":
				off := target.MetaData.DataPageOffset
				target.MetaData.BloomFilterOffset = &off
			case "column_index":
				target.MetaData.BloomFilterOffset = target.ColumnIndexOffset
				target.MetaData.BloomFilterLength = target.ColumnIndexLength
			case "offset_index":
				target.MetaData.BloomFilterOffset = target.OffsetIndexOffset
				target.MetaData.BloomFilterLength = target.OffsetIndexLength
			}
			reopened := reopenLiteralInFooter(t, src)
			pushed, _, err := reopened.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.EQ(expr.Col("x"), expr.I64(50))})
			if err != nil {
				t.Fatal(err)
			}
			rows, _ := recoveryBloomDecoded(t, pushed.(*Source), false)
			if rows != 16384 {
				t.Fatalf("invalid alias unexpectedly pruned: decoded=%d", rows)
			}
		})
	}
}

func TestRecoveryBloomIndependentLosslessEncodings(t *testing.T) {
	for _, codec := range []compress.Compression{compress.Codecs.Snappy, compress.Codecs.Zstd} {
		for _, dictionary := range []bool{false, true} {
			src := recoveryBloomConfiguredFixture(t, 16384, 1, true, 2, []pq.WriterProperty{pq.WithCompression(codec), pq.WithDictionaryDefault(dictionary)})
			for _, value := range []int64{1, 50} {
				predicate := expr.EQ(expr.Col("x"), expr.I64(value))
				pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
				if err != nil {
					t.Fatal(err)
				}
				var want []int64
				for i := 0; i < 16384; i++ {
					if value == 1 && i%7 != 0 && i%2 == 0 {
						want = append(want, int64(i))
					}
				}
				got := literalInMatchedIDs(t, pushed, predicate, kernel.ModeScalar, 257)
				if !slices.Equal(got, want) {
					t.Fatalf("codec=%v dictionary=%v value=%d got=%d want=%d", codec, dictionary, value, len(got), len(want))
				}
			}
		}
	}
}
func TestRecoveryBloomIndependentManyGroups(t *testing.T) {
	src := recoveryBloomConfiguredFixture(t, 4096, 128, true, 2, nil)
	predicate := expr.EQ(expr.Col("x"), expr.I64(50))
	pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	base, baseBytes := recoveryBloomDecoded(t, pushed.(*Source), true)
	cand, candBytes := recoveryBloomDecoded(t, pushed.(*Source), false)
	if base != 128*4096 || cand != 0 || candBytes >= baseBytes {
		t.Fatalf("base=%d/%d candidate=%d/%d", base, baseBytes, cand, candBytes)
	}
	t.Logf("128 row groups decoded %d -> %d; actual read bytes %d -> %d", base, cand, baseBytes, candBytes)
}
