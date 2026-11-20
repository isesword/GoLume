package lazy

import (
	"context"
	"encoding/binary"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/expr"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestItem25RangeFilterEndpoint(t *testing.T) {
	rows := []n44OracleRow{{0, 2, true}, {0, 8, true}, {1, 0, false}, {2, -3, true}, {2, 7, true}}
	all := n44OracleExpected(rows, 1)
	var want [][]any
	for i, r := range rows {
		if r.valid && r.value >= 7 {
			want = append(want, []any{all[i][0], all[i][4], all[i][5]})
		}
	}
	if len(want) != 2 || want[0][1] != int64(10) || want[1][1] != int64(4) {
		t.Fatal("independent oracle fixture")
	}
	for _, kind := range []string{"ipc", "parquet"} {
		t.Run(kind, func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			q := s.FromSource(n44OracleInput(t, rows, 1)).WithTimestampRange(n44OracleSpec(1)).Filter(expr.GE(expr.Col("v"), expr.I64(7))).Select("ts", "sum", "avg")
			path := filepath.Join(t.TempDir(), "range."+kind)
			at := 0
			check := func(r arrow.RecordBatch) {
				t.Helper()
				if r.NumCols() != 3 {
					t.Fatal("schema width")
				}
				for j, name := range []string{"ts", "sum", "avg"} {
					f := r.Schema().Field(j)
					typ := arrow.INT64
					if j == 0 {
						typ = arrow.TIMESTAMP
					}
					if j == 2 {
						typ = arrow.FLOAT64
					}
					if f.Name != name || f.Type.ID() != typ || f.Nullable != (j > 0) {
						t.Fatalf("field%d=%v", j, f)
					}
				}
				for i := 0; i < int(r.NumRows()); i++ {
					if at >= len(want) {
						t.Fatal("extra row")
					}
					for j, w := range want[at] {
						got := n44OracleRead(r.Column(j), i)
						if wf, ok := w.(float64); ok {
							gf, ok := got.(float64)
							if !ok || math.Float64bits(gf) != math.Float64bits(wf) {
								t.Fatalf("AVG %v want%v", got, w)
							}
						} else if got != w {
							t.Fatalf("row%d col%d=%v want%v", at, j, got, w)
						}
					}
					at++
				}
			}
			if kind == "ipc" {
				if _, e := q.SinkIPC(context.Background(), path, IPCSinkOptions{}); e != nil {
					t.Fatal(e)
				}
				f, e := os.Open(path)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				r, e := ipc.NewFileReader(f)
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				for i := 0; i < r.NumRecords(); i++ {
					rec, e := r.RecordBatchAt(i)
					if e != nil {
						t.Fatal(e)
					}
					check(rec)
					rec.Release()
				}
			} else {
				if _, e := q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 1}); e != nil {
					t.Fatal(e)
				}
				f, e := file.OpenParquetFile(path, false)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				r, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, am.DefaultAllocator)
				if e != nil {
					t.Fatal(e)
				}
				tab, e := r.ReadTable(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				defer tab.Release()
				rr := array.NewTableReader(tab, 1)
				defer rr.Release()
				for rr.Next() {
					check(rr.RecordBatch())
				}
				if rr.Err() != nil {
					t.Fatal(rr.Err())
				}
			}
			if at != 2 || s.engine.Memory().Used() != 0 {
				t.Fatalf("rows%d used%d", at, s.engine.Memory().Used())
			}
			t.Logf("inputRows=5 inputBatchRows=1 widthNS=1 outputRows=%d sink=%s engineUsed=0; expected sums10,4 (pushed-filter would8,7)", at, kind)
		})
	}
}

func TestItem25CalendarRangeRejectsPreopen(t *testing.T) {
	// Minimal captured TZif v1 UTC: no transitions, one zero-offset type.
	raw := make([]byte, 54)
	copy(raw, "TZif")
	binary.BigEndian.PutUint32(raw[36:40], 1)
	binary.BigEndian.PutUint32(raw[40:44], 4)
	copy(raw[50:], "UTC\x00")
	zone, e := expr.TimeZoneFromTZData("item25-UTC", raw)
	if e != nil {
		t.Fatal(e)
	}
	resolver, e := expr.NewCivilTimeResolver(context.Background(), zone, expr.CivilResolveOptions{Fold: expr.FoldReject, Gap: expr.GapReject})
	if e != nil {
		t.Fatal(e)
	}
	for _, terminal := range []string{"direct", "select", "count", "limit0"} {
		t.Run(terminal, func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			src := &n44OracleCounted{Source: n44OracleInput(t, []n44OracleRow{{0, 2, true}, {86400000000000, 3, true}}, 1)}
			q := s.FromSource(src).SelectExprs(AddCalendarDays(expr.Col("ts"), expr.I64(1), resolver).Alias("ts"), expr.Col("v").Alias("v")).WithTimestampRange(n44OracleSpec(1))
			switch terminal {
			case "select":
				q = q.Select("ts")
			case "count":
				q = q.Select("ts").Agg(Count("n"))
			case "limit0":
				q = q.Limit(0)
			}
			check := func(e error) {
				t.Helper()
				if e == nil || !strings.Contains(e.Error(), "only original Scan and direct Projection") {
					t.Fatalf("wrong admission error: %v", e)
				}
			}
			_, e = q.Schema()
			check(e)
			_, e = q.ExplainOptimized()
			check(e)
			p, e := q.Prepare(context.Background())
			if e == nil {
				p.Close()
			}
			check(e)
			if src.opens.Load() != 0 || src.nexts.Load() != 0 || s.engine.Memory().Used() != 0 {
				t.Fatal("rejected plan touched input or leaked")
			}
			t.Log("captured valid UTC AddCalendarDays → RANGE rejected before input open; original ExprProjection retained")
		})
	}
}
