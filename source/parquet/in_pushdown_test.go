package parquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

func TestLiteralInRowGroupPruning(t *testing.T) {
	large := make([]expr.Expr, 64)
	for i := range large {
		large[i] = expr.I64(-1000 - int64(i))
	}
	x, null := expr.Col("x"), expr.Null(memory.TypeInt64)
	for _, dictionary := range []bool{false, true} {
		for _, stats := range []bool{false, true} {
			t.Run(fmt.Sprintf("dictionary=%v/stats=%v", dictionary, stats), func(t *testing.T) {
				src := openLiteralInFixture(t, dictionary, stats)
				for _, tc := range []struct {
					name         string
					predicate    expr.Expr
					groups       []int
					ids          []int64
					withoutStats []int
				}{
					{"members", expr.In(x, expr.I64(-5), expr.I64(20)), []int{0, 1, 4}, []int64{1, 4}, nil},
					{"no_members", expr.In(x, expr.I64(25), expr.I64(30)), []int{4}, nil, nil},
					{"singleton_not_in", expr.Not(expr.In(x, expr.I64(50))), []int{0, 1, 4}, []int64{0, 1, 2, 3, 4, 12, 13, 14}, nil},
					{"only_null", expr.In(x, null), []int{}, nil, []int{}},
					{"not_only_null", expr.Not(expr.In(x, null)), []int{}, nil, []int{}},
					{"not_in_with_null", expr.Not(expr.In(x, expr.I64(20), null)), []int{}, nil, []int{}},
					{"not_in_null_first", expr.Not(expr.In(x, null, expr.I64(20))), []int{}, nil, []int{}},
					{"in_with_null", expr.In(x, expr.I64(20), null), []int{1, 4}, []int64{4}, nil},
					{"in_null_first", expr.In(x, null, expr.I64(20)), []int{1, 4}, []int64{4}, nil},
					{"integer_edges", expr.In(x, expr.I64(math.MinInt64), expr.I64(math.MaxInt64)), []int{4}, []int64{12, 13}, nil},
					{"exact_above_2p53", expr.In(x, expr.I64(1<<53), expr.I64(1<<53+1)), []int{4}, []int64{14}, nil},
					{"duplicate_literals", expr.In(x, expr.I64(50), expr.I64(50)), []int{2, 4}, []int64{6, 7, 8}, nil},
					{"large_list", expr.In(x, large...), []int{4}, nil, nil},
					{"null_or_member", expr.Or(expr.In(x, expr.I64(-5)), expr.IsNull(x)), []int{0, 1, 3, 4}, []int64{1, 5, 9, 10, 11}, nil},
					{"nested_not_and", expr.Not(expr.And(expr.In(x, expr.I64(50)), expr.In(x, expr.I64(50), null))), []int{0, 1, 4}, []int64{0, 1, 2, 3, 4, 12, 13, 14}, nil},
					{"non_literal_fallback", expr.In(x, expr.Col("id")), []int{0, 1, 2, 3, 4}, nil, nil},
					// Integer-to-float comparisons stay exact in the residual evaluator.
					{"mixed_numeric_fallback", expr.In(x, expr.F64(1<<53)), []int{0, 1, 2, 3, 4}, nil, nil},
				} {
					t.Run(tc.name, func(t *testing.T) {
						want := tc.groups
						if !stats {
							want = tc.withoutStats
							if want == nil {
								want = []int{0, 1, 2, 3, 4}
							}
						}
						pushed := assertLiteralInParts(t, src, tc.predicate, want)
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
							for _, batchRows := range []int{1, 7} {
								for _, input := range []source.Source{src, pushed} {
									got := literalInMatchedIDs(t, input, tc.predicate, mode, batchRows)
									if !slices.Equal(got, tc.ids) {
										t.Fatalf("mode=%s batch=%d IDs=%v want=%v", mode, batchRows, got, tc.ids)
									}
								}
							}
						}
					})
				}
			})
		}
	}
}

func TestLiteralInInvalidAndFalliblePredicates(t *testing.T) {
	src := openLiteralInFixture(t, false, true)
	for _, predicate := range []expr.Expr{expr.In(expr.Col("x")), expr.Not(expr.In(expr.Col("x")))} {
		if _, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate}); err == nil || !strings.Contains(err.Error(), "at least one") {
			t.Fatalf("empty IN accepted: %v", err)
		}
	}
	bad := expr.CastTo(expr.Col("x"), memory.TypeInt8)
	for _, predicate := range []expr.Expr{
		expr.In(bad, expr.I64(500)),
		expr.In(expr.Col("id"), expr.I64(-1), bad),
		expr.Not(expr.In(expr.Col("id"), expr.Null(memory.TypeInt64), bad)),
		expr.And(expr.In(expr.Col("x"), expr.I64(25)), expr.GT(bad, expr.I64(0))),
	} {
		assertLiteralInParts(t, src, predicate, []int{0, 1, 2, 3, 4})
	}
}

func TestLiteralInMissingAndInvalidStatistics(t *testing.T) {
	for _, corruption := range []string{"missing", "no_min_max", "no_null_count", "negative_null_count", "excess_null_count", "reversed_bounds"} {
		t.Run(corruption, func(t *testing.T) {
			src := openLiteralInFixture(t, false, true)
			raw := src.meta.FileMetaData.RowGroups[0].Columns[1].MetaData
			switch corruption {
			case "missing":
				raw.Statistics = nil
			case "no_min_max":
				raw.Statistics.Min, raw.Statistics.Max, raw.Statistics.MinValue, raw.Statistics.MaxValue = nil, nil, nil, nil
			case "no_null_count":
				raw.Statistics.NullCount = nil
			case "negative_null_count":
				n := int64(-1)
				raw.Statistics.NullCount = &n
			case "excess_null_count":
				n := int64(100)
				raw.Statistics.NullCount = &n
			case "reversed_bounds":
				raw.Statistics.MinValue = binary.LittleEndian.AppendUint64(nil, uint64(1))
				raw.Statistics.MaxValue = binary.LittleEndian.AppendUint64(nil, uint64(0))
				raw.Statistics.Min, raw.Statistics.Max = raw.Statistics.MinValue, raw.Statistics.MaxValue
			}
			// The file itself remains unchanged. A matching physical row must not
			// disappear when the in-memory metadata cannot safely describe it.
			predicate := expr.In(expr.Col("x"), expr.I64(-5))
			pushed := assertLiteralInParts(t, src, predicate, []int{0, 4})
			if got := literalInMatchedIDs(t, pushed, predicate, kernel.ModeScalar, 2); !slices.Equal(got, []int64{1}) {
				t.Fatalf("IDs=%v", got)
			}
			if corruption == "negative_null_count" || corruption == "excess_null_count" {
				assertLiteralInParts(t, src, expr.IsNotNull(expr.Col("x")), []int{0, 1, 2, 4})
			}
		})
	}
}

func TestLiteralInRawFooterStatisticsPresence(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, missing := range []string{"min", "max", "short_min", "short_max", "empty_min", "long_max", "null_count", "unselected_pair"} {
			t.Run(fmt.Sprintf("legacy=%v/%s", legacy, missing), func(t *testing.T) {
				first := []int64{-10, -5, 0}
				literal := int64(-5)
				if missing == "max" || missing == "short_max" || missing == "long_max" {
					first, literal = []int64{0, 5, 10}, 5
				}
				src := openLiteralInFixture(t, true, true, first)
				group := 0
				if missing == "null_count" {
					group = 1
				}
				raw := src.meta.RowGroups[group].Columns[1].MetaData.Statistics
				min, max := &raw.MinValue, &raw.MaxValue
				if legacy {
					src.meta.ColumnOrders = nil
					min, max = &raw.Min, &raw.Max
				}
				switch missing {
				case "min":
					*min = nil
				case "max":
					*max = nil
				case "short_min":
					*min = []byte{1}
				case "short_max":
					*max = []byte{1}
				case "empty_min":
					*min = []byte{}
				case "long_max":
					*max = append(append([]byte{}, (*max)...), 0)
				case "null_count":
					raw.NullCount = nil
				case "unselected_pair":
					if legacy {
						raw.MinValue = []byte{1}
					} else {
						raw.Min = []byte{1}
					}
				}
				src = reopenLiteralInFooter(t, src)
				predicate := expr.Expr(expr.In(expr.Col("x"), expr.I64(literal)))
				groups, ids := []int{0, 4}, []int64{1}
				if missing == "null_count" {
					predicate = expr.Or(expr.In(expr.Col("x"), expr.I64(25)), expr.IsNull(expr.Col("x")))
					groups, ids = []int{1, 3, 4}, []int64{5, 9, 10, 11}
				}
				pushed := assertLiteralInParts(t, src, predicate, groups)
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					for _, input := range []source.Source{src, pushed} {
						if got := literalInMatchedIDs(t, input, predicate, mode, 2); !slices.Equal(got, ids) {
							t.Fatalf("IDs=%v want=%v", got, ids)
						}
					}
				}
			})
		}
	}
}

// Reopen an actual rewritten footer so the tests cover serialization, column
// order selection, OpenFile, raw presence checks, and physical row decoding.
func reopenLiteralInFooter(t *testing.T, src *Source) *Source {
	t.Helper()
	data, err := os.ReadFile(src.path)
	if err != nil {
		t.Fatal(err)
	}
	footerStart := len(data) - 8 - int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	footer, err := src.meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := append(data[:footerStart:footerStart], footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, "PAR1"...)
	if err = os.WriteFile(src.path, out, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFile(src.path)
	if err != nil {
		t.Fatal(err)
	}
	return reopened
}

func TestLiteralInFloatBoundaryAndNaNSemantics(t *testing.T) {
	for _, tc := range []struct {
		name            string
		values          []float64
		predicate       expr.Expr
		matched, pruned int
	}{
		{"in_nan", []float64{1, math.NaN()}, expr.In(expr.Col("f"), expr.F64(math.NaN())), 0, 1},
		{"not_in_nan", []float64{1, math.NaN()}, expr.Not(expr.In(expr.Col("f"), expr.F64(math.NaN()))), 2, 0},
		{"not_in_keeps_nan", []float64{1, math.NaN()}, expr.Not(expr.In(expr.Col("f"), expr.F64(1))), 1, 0},
		{"nan_and_null", []float64{1, math.NaN()}, expr.Not(expr.In(expr.Col("f"), expr.F64(1), expr.Null(memory.TypeFloat64))), 0, 1},
		{"no_member", []float64{1, math.NaN()}, expr.In(expr.Col("f"), expr.F64(2), expr.F64(3)), 0, 1},
		{"unrepresentable_integer", []float64{1 << 53}, expr.Not(expr.In(expr.Col("f"), expr.I64(1<<53+1))), 1, 0},
		{"int64_max_boundary", []float64{0x1p63}, expr.Not(expr.In(expr.Col("f"), expr.I64(math.MaxInt64))), 1, 0},
		{"infinities", []float64{math.Inf(-1), 1, math.Inf(1)}, expr.In(expr.Col("f"), expr.F64(math.Inf(-1)), expr.F64(math.Inf(1))), 2, 0},
		{"signed_zero", []float64{math.Copysign(0, -1)}, expr.In(expr.Col("f"), expr.F64(0)), 1, 0},
		{"all_nan", []float64{math.NaN()}, expr.Not(expr.In(expr.Col("f"), expr.F64(1))), 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "float.parquet")
			writeDoubleBoolFixture(t, path, tc.values, nil, make([]bool, len(tc.values)), nil)
			src, err := OpenFile(path)
			if err != nil {
				t.Fatal(err)
			}
			pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: tc.predicate})
			if err != nil || report.PrunedPartitions != tc.pruned {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				for _, input := range []source.Source{src, pushed} {
					if got := countExactFloatMatches(t, input, tc.predicate, mode); got != tc.matched {
						t.Fatalf("matched=%d want=%d", got, tc.matched)
					}
				}
			}
		})
	}
	for _, bound := range []float64{math.NaN(), 2} {
		path := filepath.Join(t.TempDir(), "bad-float-stats.parquet")
		writeDoubleBoolFixture(t, path, []float64{1}, nil, []bool{false}, nil)
		src, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		raw := src.meta.FileMetaData.RowGroups[0].Columns[0].MetaData.Statistics
		raw.MinValue, raw.MaxValue = binary.LittleEndian.AppendUint64(nil, math.Float64bits(bound)), binary.LittleEndian.AppendUint64(nil, math.Float64bits(0))
		raw.Min, raw.Max = raw.MinValue, raw.MaxValue
		assertLiteralInParts(t, src, expr.In(expr.Col("f"), expr.F64(1)), []int{0})
	}
}

func TestLiteralInStringAndTemporalFallbacks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporal.parquet")
	stamps := writeTemporalPushdownFixture(t, path)
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	assertLiteralInParts(t, src, expr.In(expr.Col("ts"), expr.TimestampMillis(stamps[1].UnixMilli())), []int{1})
	assertLiteralInParts(t, src, expr.In(expr.Col("d"), expr.Date32(int32(stamps[2].Unix()/86400))), []int{2})
	// A prefix-truncated max must not be treated as an exact upper string bound.
	for _, group := range src.meta.FileMetaData.RowGroups[1:] {
		st := group.Columns[0].MetaData.Statistics
		st.MinValue, st.MaxValue, st.Min, st.Max = []byte("n"), []byte("n"), []byte("n"), []byte("n")
	}
	predicate := expr.In(expr.Col("s"), expr.Str("needle"))
	pushed := assertLiteralInParts(t, src, predicate, []int{1, 2})
	if got := countExactFloatMatches(t, pushed, predicate, kernel.ModeScalar); got != 1 {
		t.Fatalf("string matches=%d", got)
	}
	// Arrow rejects known-bad writer statistics. IN must not bypass that gate.
	*src.meta.WriterVersion() = *metadata.NewAppVersion("parquet-mr version 1.7.0")
	assertLiteralInParts(t, src, predicate, []int{0, 1, 2})
}

func TestLiteralInUnsignedAndDecimalFallbacks(t *testing.T) {
	for _, stats := range []bool{false, true} {
		path := writeUnsignedFixture(t, arrowparquet.NewWriterProperties(arrowparquet.WithStats(stats)))
		src, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, column := range []string{"u32n", "u64n"} {
			predicate := expr.In(expr.Col(column), expr.I64(-1), expr.I64(math.MaxInt64), expr.I64(math.MaxUint32))
			want := []int{0, 1, 2}
			if stats {
				want = []int{0, 1}
			}
			pushed := assertLiteralInParts(t, src, predicate, want)
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				original := countExactFloatMatches(t, src, predicate, mode)
				if got := countExactFloatMatches(t, pushed, predicate, mode); got != original {
					t.Fatalf("unsigned original=%d pushed=%d", original, got)
				}
			}
			assertLiteralInParts(t, src, expr.Not(expr.In(expr.Col(column), expr.I64(-1), expr.Null(memory.TypeInt64))), nil)
		}
		path = writeDecimalFixture(t, arrowparquet.Types.Int64, -1, 18, 2, true, []int{3, 3, 3, 3}, decimalValues(18), arrowparquet.WithStats(stats))
		src, err = OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// An unscaled physical coefficient must never be used as an ordinary
		// integer range. Decimal literals preserve their independent scale.
		predicate := expr.In(expr.Col("amount"), expr.Decimal128(memory.Decimal128{}, 18, 4))
		want := []int{0, 1, 2, 3}
		if stats {
			want = []int{0, 1, 2}
		}
		pushed := assertLiteralInParts(t, src, predicate, want)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			for _, input := range []source.Source{src, pushed} {
				if got := countExactFloatMatches(t, input, predicate, mode); got != 3 {
					t.Fatalf("decimal matches=%d", got)
				}
			}
		}
		assertLiteralInParts(t, src, expr.In(expr.Col("amount"), expr.NullDecimal128(18, 2)), nil)
		assertLiteralInParts(t, src, expr.Not(expr.In(expr.Col("amount"), expr.Decimal128(memory.Decimal128{}, 18, 4), expr.NullDecimal128(18, 2))), nil)
	}
}

func TestLiteralInBooleanFloat32AndNarrowFallbacks(t *testing.T) {
	for _, domain := range []string{"bool", "float32", "int8", "uint16"} {
		t.Run(domain, func(t *testing.T) {
			var node schema.Node
			var err error
			var member expr.Expr
			matches := 1
			switch domain {
			case "bool":
				node, member, matches = schema.NewBooleanNode("x", arrowparquet.Repetitions.Optional, -1), expr.Bool(true), 2
			case "float32":
				node, member = schema.NewFloat32Node("x", arrowparquet.Repetitions.Optional, -1), expr.F64(1.5)
			case "int8":
				node, err = schema.NewPrimitiveNodeLogical("x", arrowparquet.Repetitions.Optional, schema.NewIntLogicalType(8, true), arrowparquet.Types.Int32, -1, -1)
				member = expr.I64(-128)
			case "uint16":
				node, err = schema.NewPrimitiveNodeLogical("x", arrowparquet.Repetitions.Optional, schema.NewIntLogicalType(16, false), arrowparquet.Types.Int32, -1, -1)
				member = expr.I64(65535)
			}
			if err != nil {
				t.Fatal(err)
			}
			root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{node}, -1)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "fallback.parquet")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			writer := file.NewParquetWriter(f, root)
			rg := writer.AppendRowGroup()
			col, err := rg.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			defs := []int16{1, 0, 1, 1}
			switch domain {
			case "bool":
				_, err = col.(*file.BooleanColumnChunkWriter).WriteBatch([]bool{true, false, true}, defs, nil)
			case "float32":
				_, err = col.(*file.Float32ColumnChunkWriter).WriteBatch([]float32{float32(math.Inf(-1)), 1.5, float32(math.NaN())}, defs, nil)
			case "int8":
				_, err = col.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{-128, 0, 127}, defs, nil)
			case "uint16":
				_, err = col.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{0, 32768, 65535}, defs, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = col.Close(); err != nil {
				t.Fatal(err)
			}
			if err = rg.Close(); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			src, err := OpenFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, negate := range []bool{false, true} {
				predicate := expr.Expr(expr.In(expr.Col("x"), member))
				want := matches
				if negate {
					predicate, want = expr.Not(predicate), 3-matches
				}
				pushed := assertLiteralInParts(t, src, predicate, []int{0})
				for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
					for _, input := range []source.Source{src, pushed} {
						if got := countExactFloatMatches(t, input, predicate, mode); got != want {
							t.Fatalf("negate=%v matches=%d want=%d", negate, got, want)
						}
					}
				}
			}
		})
	}
}

func assertLiteralInParts(t *testing.T, src *Source, predicate expr.Expr, want []int) source.Source {
	t.Helper()
	pushed, report, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: predicate})
	if err != nil {
		t.Fatal(err)
	}
	parts, err := pushed.Partitions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int, len(parts))
	for i, p := range parts {
		got[i] = p.ID
	}
	if !slices.Equal(got, want) || report.PrunedPartitions != len(src.allParts)-len(want) {
		t.Fatalf("%s parts=%v want=%v report=%+v", predicate, got, want, report)
	}
	return pushed
}

func literalInMatchedIDs(t *testing.T, input source.Source, predicate expr.Expr, mode kernel.Mode, batchRows int) []int64 {
	t.Helper()
	ctx := context.Background()
	program, err := expr.Compile(input.Schema(), predicate)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := expr.NewWorkspace(ctx, program, memory.NewBudgetManager(0))
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	kernels := kernel.New(mode)
	stream, err := input.NewBatchStream(ctx, source.ReadOptions{Kernels: kernels, BatchRows: batchRows})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	batch := memory.NewBatch()
	var ids []int64
	for {
		ok, err := stream.Next(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return ids
		}
		value, err := workspace.EvaluateBool(ctx, batch, kernels)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < batch.Rows; i++ {
			mask := uint64(1) << uint(i&63)
			if value.Truth[i/64]&mask != 0 && (value.Validity == nil || value.Validity[i/64]&mask != 0) {
				ids = append(ids, batch.Int64s["id"].Values[i])
			}
		}
	}
}

func openLiteralInFixture(t *testing.T, dictionary, stats bool, firstGroup ...[]int64) *Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "in.parquet")
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1), schema.NewInt64Node("x", arrowparquet.Repetitions.Optional, -1)}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(dictionary), arrowparquet.WithStats(stats))))
	groups := []struct {
		values []int64
		defs   []int16
	}{
		{[]int64{-10, -5, 0}, []int16{1, 1, 1}},
		{[]int64{10, 20}, []int16{1, 1, 0}},
		{[]int64{50, 50, 50}, []int16{1, 1, 1}},
		{nil, []int16{0, 0, 0}},
		{[]int64{math.MaxInt64, math.MinInt64, 1<<53 + 1}, []int16{1, 1, 1}},
	}
	if len(firstGroup) != 0 {
		groups[0].values = firstGroup[0]
	}
	for i, g := range groups {
		rg := w.AppendRowGroup()
		writeI64Column(t, rg, []int64{int64(i * 3), int64(i*3 + 1), int64(i*3 + 2)})
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = col.(*file.Int64ColumnChunkWriter).WriteBatch(g.values, g.defs, nil); err != nil {
			t.Fatal(err)
		}
		if err = col.Close(); err != nil {
			t.Fatal(err)
		}
		if err = rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return src
}
