package parquet_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/lazy"
	"gocolumn/memory"
	"gocolumn/source"
	parquetsource "gocolumn/source/parquet"
)

// Embedding only Source hides ApplyPushdown for the independent full-scan
// control while preserving the same physical Parquet decoder and expressions.
type inFullScanSource struct{ source.Source }

type inObservedSource struct {
	*parquetsource.Source
	report source.PushdownReport
}

func (s *inObservedSource) ApplyPushdown(ctx context.Context, req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	next, report, err := s.Source.ApplyPushdown(ctx, req)
	s.report = report
	return next, report, err
}

func TestLazyLiteralInParquetPruningMatchesFullScan(t *testing.T) {
	const groupRows = 129
	x, null := expr.Col("x"), expr.Null(memory.TypeInt64)
	large := make([]expr.Expr, 32)
	largeIDs := make([]int64, 32)
	for i := range large {
		large[i], largeIDs[i] = expr.I64(1000+int64(i)), groupRows+int64(i)
	}
	notIDs := make([]int64, groupRows*3-1)
	for i := range notIDs {
		notIDs[i] = int64(i + 1)
	}
	nullIDs := []int64{25}
	for i := 0; i < groupRows; i++ {
		nullIDs = append(nullIDs, int64(groupRows*3+i))
	}
	for _, dictionary := range []bool{false, true} {
		path := writeLazyInFixture(t, dictionary, groupRows)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
			for _, workers := range []int{1, 3} {
				for _, tc := range []struct {
					name      string
					predicate expr.Expr
					ids       []int64
					groups    int
				}{
					{"members", expr.In(x, expr.I64(1020), expr.I64(1021), expr.I64(2001)), []int64{149, 150, 259}, 2},
					{"large_set", expr.In(x, large...), largeIDs, 1},
					{"in_null", expr.In(x, null, expr.I64(1020)), []int64{149}, 1},
					{"not_in_null", expr.Not(expr.In(x, expr.I64(1020), null)), nil, 0},
					{"not_in", expr.Not(expr.In(x, expr.I64(0))), notIDs, 3},
					{"null_or_in", expr.Or(expr.In(x, expr.I64(25)), expr.IsNull(x)), nullIDs, 2},
				} {
					t.Run(fmt.Sprintf("dictionary=%v/%s/workers=%d/%s", dictionary, mode, workers, tc.name), func(t *testing.T) {
						base, err := parquetsource.OpenFile(path)
						if err != nil {
							t.Fatal(err)
						}
						observed := &inObservedSource{Source: base}
						for _, input := range []source.Source{inFullScanSource{base}, observed} {
							opts := engine.DefaultOptions()
							opts.Workers, opts.KernelMode = workers, mode
							opts.Pipeline.Workers = workers
							session := lazy.NewSession(opts)
							frame, err := session.FromSource(input).Filter(tc.predicate).Select("id").Collect(context.Background())
							if err != nil {
								session.Close()
								t.Fatal(err)
							}
							col, err := frame.Column("id")
							if err != nil {
								frame.Close()
								session.Close()
								t.Fatal(err)
							}
							ids, _, err := col.CopyInt64()
							if err != nil {
								frame.Close()
								session.Close()
								t.Fatal(err)
							}
							slices.Sort(ids)
							if !slices.Equal(ids, tc.ids) {
								frame.Close()
								session.Close()
								t.Fatalf("%T IDs=%v want=%v", input, ids, tc.ids)
							}
							if err = frame.Close(); err != nil {
								t.Fatal(err)
							}
							if err = session.Close(); err != nil {
								t.Fatal(err)
							}
						}
						if observed.report.SelectedPartitions != tc.groups || observed.report.SelectedRows != int64(tc.groups*groupRows) || observed.report.SelectedBytes >= observed.report.TotalBytes {
							t.Fatalf("pushdown report=%+v want %d groups", observed.report, tc.groups)
						}
					})
				}
			}
		}
	}
}

func TestLazyLiteralInPreservesCandidateErrorOrder(t *testing.T) {
	path := writeLazyInFixture(t, true, 129)
	bad := expr.CastTo(expr.F64(math.NaN()), memory.TypeInt64)
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		for _, tc := range []struct {
			name      string
			predicate expr.Expr
			wantError bool
		}{
			{"matched_candidate_guards_error", expr.In(expr.Col("id"), expr.Col("id"), bad), false},
			{"unmatched_candidate_demands_error", expr.In(expr.Col("id"), expr.I64(-1), bad), true},
			{"null_candidate_does_not_guard_error", expr.Not(expr.In(expr.Col("id"), expr.Null(memory.TypeInt64), bad)), true},
			{"later_match_does_not_guard_error", expr.In(expr.Col("id"), bad, expr.Col("id")), true},
		} {
			t.Run(fmt.Sprintf("%s/%s", mode, tc.name), func(t *testing.T) {
				base, err := parquetsource.OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				observed := &inObservedSource{Source: base}
				for _, input := range []source.Source{inFullScanSource{base}, observed} {
					opts := engine.DefaultOptions()
					opts.KernelMode = mode
					opts.Workers = 1
					opts.Pipeline.Workers = 1
					session := lazy.NewSession(opts)
					frame, err := session.FromSource(input).Filter(tc.predicate).Select("id").Collect(context.Background())
					if (err != nil) != tc.wantError {
						session.Close()
						t.Fatalf("%T err=%v wantError=%v", input, err, tc.wantError)
					}
					if err == nil {
						if frame.Height() != 4*129 {
							frame.Close()
							session.Close()
							t.Fatalf("guarded rows=%d", frame.Height())
						}
						if err = frame.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if err = session.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if observed.report.SelectedPartitions != 4 {
					t.Fatalf("fallible IN lost row groups: %+v", observed.report)
				}
			})
		}
	}
}

func writeLazyInFixture(t *testing.T, dictionary bool, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lazy-in.parquet")
	root, err := schema.NewGroupNode("schema", arrowparquet.Repetitions.Required, schema.FieldList{schema.NewInt64Node("id", arrowparquet.Repetitions.Required, -1), schema.NewInt64Node("x", arrowparquet.Repetitions.Optional, -1)}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := file.NewParquetWriter(f, root, file.WithWriterProps(arrowparquet.NewWriterProperties(arrowparquet.WithDictionaryDefault(dictionary))))
	for group := 0; group < 4; group++ {
		rg := writer.AppendRowGroup()
		ids, values, defs := make([]int64, rows), make([]int64, 0, rows), make([]int16, rows)
		for row := range ids {
			ids[row] = int64(group*rows + row)
			if group != 3 {
				values = append(values, int64(group*1000+row))
				defs[row] = 1
			}
		}
		writeLazyTypedI64(t, rg, ids)
		col, err := rg.NextColumn()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = col.(*file.Int64ColumnChunkWriter).WriteBatch(values, defs, nil); err != nil {
			t.Fatal(err)
		}
		if err = col.Close(); err != nil {
			t.Fatal(err)
		}
		if err = rg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
