package lazy

import (
	"context"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/observer"
	"gocolumn/source"
	"math"
	"strings"
	"testing"
)

func n44ReviewSpec() TimestampRangeSpec {
	return TimestampRangeSpec{OrderBy: "ts", Column: "v", Frame: TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: 1}, CountRowsAlias: "rows", CountAlias: "count", SumAlias: "sum", AvgAlias: "avg"}
}
func n44ReviewDrain(f LazyFrame, ctx context.Context) (int, error) {
	r, e := f.CollectBatches(ctx, BatchOptions{})
	if e != nil {
		return 0, e
	}
	defer r.Close()
	n := 0
	for r.Next(ctx) {
		n += int(r.Batch().Len())
	}
	return n, r.Err()
}
func TestN44ReviewProjectionCannotEraseErrors(t *testing.T) {
	for _, kind := range []string{"sum-overflow", "false-order"} {
		t.Run(kind, func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			rows := []n44OracleRow{{1, math.MaxInt64, true}, {1, 1, true}}
			if kind == "false-order" {
				rows = []n44OracleRow{{2, 1, true}, {1, 2, true}}
			}
			f := s.FromSource(n44OracleInput(t, rows, 1)).WithTimestampRange(n44ReviewSpec()).Select("ts")
			n, e := n44ReviewDrain(f, context.Background())
			if e == nil || n != 0 || strings.Contains(e.Error(), "not implemented") {
				t.Fatal("projection erased RANGE error", n, e)
			}
		})
	}
}
func TestN44ReviewPreOptimizationReservations(t *testing.T) {
	for _, kind := range []string{"true-filter", "input-limit", "computed-value", "renamed-key", "restored-key", "sort"} {
		t.Run(kind, func(t *testing.T) {
			s := n44OracleSession()
			defer s.Close()
			f := s.FromSource(n44OracleInput(t, []n44OracleRow{{1, 2, true}}, 1))
			switch kind {
			case "true-filter":
				f = f.Filter(expr.Bool(true))
			case "input-limit":
				f = f.Limit(1)
			case "computed-value":
				f = f.SelectExprs(expr.Col("ts").Alias("ts"), expr.Col("v").Add(expr.I64(0)).Alias("v"))
			case "renamed-key":
				f = f.SelectExprs(expr.Col("ts").Alias("renamed"), expr.Col("v").Alias("v"))
			case "restored-key":
				f = f.SelectExprs(expr.Col("ts").Alias("renamed"), expr.Col("v").Alias("v")).SelectExprs(expr.Col("renamed").Alias("ts"), expr.Col("v").Alias("v"))
			case "sort":
				f = f.Sort(Asc("ts"))
			}
			sp := n44ReviewSpec()
			if kind == "renamed-key" {
				sp.OrderBy = "renamed"
			}
			f = f.WithTimestampRange(sp).Select(sp.OrderBy)
			if _, e := f.Schema(); e == nil {
				t.Fatal("Schema bypassed reservation")
			}
			if _, e := f.ExplainOptimized(); e == nil {
				t.Fatal("preoptimization reservation removed", kind)
			}
			if _, e := n44ReviewDrain(f, context.Background()); e == nil {
				t.Fatal("unsafe route executed", kind)
			}
		})
	}
}
func TestN44ReviewProfilingParityAndPhysicalPlan(t *testing.T) {
	s := n44OracleSession()
	defer s.Close()
	f := s.FromSource(n44OracleInput(t, []n44OracleRow{{1, 2, true}, {1, 4, true}, {2, 8, true}}, 1)).Select("ts", "v").WithTimestampRange(n44ReviewSpec()).Select("ts", "avg").Limit(2)
	ex, e := f.ExplainPhysical(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(strings.ToLower(ex), "timestamprange") {
		t.Fatal(ex)
	}
	n, e := n44ReviewDrain(f, context.Background())
	if e != nil || n != 2 {
		t.Fatal(n, e)
	}
	ctx := observer.WithOperatorTrace(context.Background(), observer.NewOperatorTrace())
	if _, e = f.ExplainPhysical(ctx); e == nil || !strings.Contains(e.Error(), "profiling") {
		t.Fatal("trace explain must explicitly reject", e)
	}
	if n, e = n44ReviewDrain(f, ctx); e == nil || n != 0 || !strings.Contains(e.Error(), "profiling") {
		t.Fatal("trace must explicitly reject", n, e)
	}
	report, e := f.Analyze(context.Background())
	if e == nil || report.OutputRows != 0 || report.Termination != AnalyzeFailed || !strings.Contains(e.Error(), "profiling") {
		t.Fatal(report.String(), e)
	}
}
func TestN44ReviewFiniteMemoryOnlyBeforeElision(t *testing.T) {
	for _, mode := range []engine.ExecutionPolicy{engine.ExecutionAuto, engine.ExecutionBounded} {
		t.Run(mode.String(), func(t *testing.T) {
			o := engine.DefaultOptions()
			o.MemoryLimitBytes = 16 << 20
			o.ExecutionPolicy = mode
			o.Workers = 1
			o.Pipeline.Workers = 1
			s := NewSession(o)
			defer s.Close()
			f := s.FromSource(n44OracleInput(t, []n44OracleRow{{1, 2, true}}, 1)).WithTimestampRange(n44ReviewSpec()).Select("ts").Limit(0)
			if _, e := f.Schema(); e == nil {
				t.Fatal("Schema bypassed reservation")
			}
			if _, e := f.ExplainOptimized(); e == nil {
				t.Fatal("mode elided")
			}
			if _, e := n44ReviewDrain(f, context.Background()); e == nil {
				t.Fatal("mode executed")
			}
		})
	}
}

// This wrapper exposes no order witness; the real inner source being ordered
// must not authorize the outer source by its plan label alone.
type n44ReviewNoOrder struct{ source.Source }

func TestN44ReviewNoFabricatedOrdering(t *testing.T) {
	s := n44OracleSession()
	defer s.Close()
	src := &n44ReviewNoOrder{n44OracleInput(t, []n44OracleRow{{1, 2, true}}, 1)}
	f := s.FromSource(src).WithTimestampRange(n44ReviewSpec())
	if _, e := f.Schema(); e == nil {
		t.Fatal("Schema bypassed reservation")
	}
	if _, e := f.ExplainOptimized(); e == nil {
		t.Fatal("fabricated original source proof")
	}
	if _, e := n44ReviewDrain(f, context.Background()); e == nil {
		t.Fatal("unproved source executed")
	}
}

type n44ReviewOpenProbe struct {
	source.Source
	opens int
}

func (s *n44ReviewOpenProbe) Ordering() []source.OrderingColumn {
	return s.Source.(source.OrderingProvider).Ordering()
}
func (s *n44ReviewOpenProbe) StreamPlan() source.StreamPlanNode { return source.StreamPlanOf(s.Source) }
func (s *n44ReviewOpenProbe) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return source.OpenBatchStream(ctx, s.Source, opts)
}
func TestN44ReviewProfilingRejectsBeforeOpen(t *testing.T) {
	s := n44OracleSession()
	defer s.Close()
	probe := &n44ReviewOpenProbe{Source: n44OracleInput(t, []n44OracleRow{{1, 2, true}, {1, 4, true}}, 1)}
	f := s.FromSource(probe).Select("ts", "v").WithTimestampRange(n44ReviewSpec()).Select("avg")
	ctx := observer.WithOperatorTrace(context.Background(), observer.NewOperatorTrace())
	for _, terminal := range []string{"explain", "prepare", "batches", "analyze"} {
		t.Run(terminal, func(t *testing.T) {
			var e error
			switch terminal {
			case "explain":
				_, e = f.ExplainPhysical(ctx)
			case "prepare":
				var p *PreparedDataFrame
				p, e = f.Prepare(ctx)
				if p != nil {
					p.Close()
				}
			case "batches":
				_, e = n44ReviewDrain(f, ctx)
			case "analyze":
				_, e = f.Analyze(context.Background())
			}
			if e == nil || !strings.Contains(e.Error(), "profiling") || probe.opens != 0 {
				t.Fatal(terminal, e, probe.opens)
			}
		})
	}
	p, e := f.Prepare(context.Background())
	if e != nil {
		t.Fatal("plain prepare", e)
	}
	defer p.Close()
	if probe.opens != 0 {
		t.Fatal("prepare consumed")
	}
	graph := source.StreamPlanOf(p.src)
	ranges, scans := 0, 0
	var walk func(source.StreamPlanNode, int) int
	walk = func(n source.StreamPlanNode, d int) int {
		if n.Operator == "TimestampRange" {
			ranges++
			if n.Class != source.StreamBounded {
				t.Fatal(n)
			}
		}
		if n.Operator == "Scan" {
			scans++
		}
		max := d
		for _, c := range n.Children {
			v := walk(c, d+1)
			if v > max {
				max = v
			}
		}
		return max
	}
	depth := walk(graph, 1)
	if ranges != 1 || scans != 1 || depth < 3 {
		t.Fatal("actual graph", ranges, scans, depth, source.FormatStreamPlan(graph))
	}
	t.Logf("real executable graph depth=%d\n%s", depth, source.FormatStreamPlan(graph))
	n, e := n44ReviewDrain(f, context.Background())
	if e != nil || n != 2 || probe.opens != 1 {
		t.Fatal("plain actual source", n, e, probe.opens)
	}
}
