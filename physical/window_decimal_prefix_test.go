package physical

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

type n27PrefixFixture struct {
	schema               memory.Schema
	pages                []*memory.Batch
	ordering             []source.OrderingColumn
	plan                 source.StreamPlanNode
	opens, closes, reads int
	hook                 func(int)
	wait                 func(context.Context) error
}

func (s *n27PrefixFixture) Schema() memory.Schema                                  { return s.schema }
func (s *n27PrefixFixture) Stats(context.Context) (source.Stats, error)            { return source.Stats{}, nil }
func (s *n27PrefixFixture) Partitions(context.Context) ([]source.Partition, error) { return nil, nil }
func (s *n27PrefixFixture) Ordering() []source.OrderingColumn                      { return s.ordering }
func (s *n27PrefixFixture) StreamPlan() source.StreamPlanNode                      { return s.plan }
func (s *n27PrefixFixture) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return &n27PrefixFixtureStream{fixture: s}, nil
}

type n27PrefixFixtureStream struct {
	fixture *n27PrefixFixture
	next    int
}

func (s *n27PrefixFixtureStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.fixture.reads++
	if s.fixture.wait != nil {
		return false, s.fixture.wait(ctx)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dst.ClearColumns()
	if s.next == len(s.fixture.pages) {
		return false, nil
	}
	page := s.fixture.pages[s.next]
	dst.Rows = page.Rows
	dst.Int64s["k"] = page.Int64s["k"]
	dst.Decimals["v"] = page.Decimals["v"]
	s.next++
	if s.fixture.hook != nil {
		s.fixture.hook(s.next)
	}
	return true, nil
}
func (s *n27PrefixFixtureStream) Close() error { s.fixture.closes++; return nil }
func n27Source(t *testing.T, p, s int32, pages ...*memory.Batch) *n27PrefixFixture {
	t.Helper()
	val, err := memory.Decimal128Field("v", true, p, s)
	if err != nil {
		t.Fatal(err)
	}
	return &n27PrefixFixture{schema: memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64, Encoding: memory.EncodingPlain}, val), pages: pages, ordering: []source.OrderingColumn{{Name: "k"}}, plan: source.StreamPlanNode{Class: source.StreamPipeline}}
}
func n27Page(p, s int32, base int64, bits memory.Bitmap, n ...int64) *memory.Batch {
	b := memory.NewBatch()
	b.Rows = len(n)
	keys := make([]int64, len(n))
	values := make([]memory.Decimal128, len(n))
	for i, v := range n {
		keys[i] = base + int64(i)
		values[i] = decimal128.FromI64(v)
	}
	b.Int64s["k"] = &memory.Int64Vector{Values: keys, Encoding: memory.EncodingPlain}
	b.Decimals["v"] = &memory.Decimal128Vector{Values: values, Validity: bits, Precision: p, Scale: s}
	return b
}
func n27Spec(p, s int32, m aggregate.DecimalAverageRoundingMode) DecimalPrefixAverage {
	return DecimalPrefixAverage{Column: "v", OrderBy: "k", Alias: "m", Precision: p, Scale: s, Rounding: m, Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
}
func n27Opts(m memory.Manager) source.ReadOptions {
	return source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), MemoryManager: m}
}
func n27Open(t *testing.T, ctx context.Context, src source.Source, cfg DecimalPrefixAverage, opts source.ReadOptions) source.BatchStream {
	t.Helper()
	got, err := NewDecimalPrefixAverageSource(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	st, err := got.NewBatchStream(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestN27DecimalPrefixExact(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		sign  int64
		mode  aggregate.DecimalAverageRoundingMode
		first int64
	}{
		{"positive-even", 1, aggregate.DecimalAverageHalfEven, 2},
		{"negative-even", -1, aggregate.DecimalAverageHalfEven, -2},
		{"positive-away", 1, aggregate.DecimalAverageHalfAwayFromZero, 3},
		{"negative-away", -1, aggregate.DecimalAverageHalfAwayFromZero, -3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := n27Page(2, 1, -2, memory.Bitmap{2}, 99999, 25*tc.sign)
			b := n27Page(2, 1, 0, memory.Bitmap{1}, 35*tc.sign, -99999)
			fixture := n27Source(t, 2, 1, a, b)
			m := memory.NewBudgetManager(1 << 20)
			cfg := n27Spec(3, 0, tc.mode)
			op, err := NewDecimalPrefixAverageSource(fixture, cfg)
			if err != nil {
				t.Fatal(err)
			}
			field, ok := op.Schema().Field("m")
			if !ok || field.Type != memory.TypeDecimal || !field.Nullable || field.DecimalPrecision != 3 || field.DecimalScale != 0 {
				t.Fatalf("field %+v", field)
			}
			if p := source.StreamPlanOf(op); p.Class != source.StreamBounded || p.PartitionParallel {
				t.Fatalf("plan %+v", p)
			}
			st, err := op.NewBatchStream(ctx, n27Opts(m))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			dst := memory.NewBatch()
			for i, w := range [][]int64{{0, tc.first}, {3 * tc.sign, 3 * tc.sign}} {
				ok, err = st.Next(ctx, dst)
				if !ok || err != nil {
					t.Fatal(i, ok, err)
				}
				got := dst.Decimals["m"]
				for j, v := range w {
					if got.IsValid(j) != (i != 0 || j != 0) || got.Values[j] != decimal128.FromI64(v) {
						t.Fatalf("%d/%d got %v valid %v want %d", i, j, got.Values[j], got.IsValid(j), v)
					}
				}
				if dst.Int64s["k"] != fixture.pages[i].Int64s["k"] || dst.Decimals["v"] != fixture.pages[i].Decimals["v"] {
					t.Fatal("pass-through is not borrowed input")
				}
			}
			if ok, err = st.Next(ctx, dst); ok || err != nil || m.Used() != 0 || fixture.closes != 1 {
				t.Fatal(ok, err, m.Used(), fixture.closes)
			}
		})
	}
	p := n27Page(38, 0, 1, nil, 0, 0, 0)
	n := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	n.Sub(n, big.NewInt(1))
	wide := decimal128.FromBigInt(n)
	for i := range p.Decimals["v"].Values {
		p.Decimals["v"].Values[i] = wide
	}
	st := n27Open(t, ctx, n27Source(t, 38, 0, p), n27Spec(38, 0, aggregate.DecimalAverageHalfEven), n27Opts(memory.NewBudgetManager(1<<20)))
	defer st.Close()
	dst := memory.NewBatch()
	if ok, err := st.Next(ctx, dst); !ok || err != nil {
		t.Fatal(ok, err)
	}
	for _, got := range dst.Decimals["m"].Values {
		if got != wide {
			t.Fatalf("wide prefix got %v want %v", got, wide)
		}
	}
}

func TestN27DecimalPrefixFailureOwnership(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pages   []*memory.Batch
		later   bool
		want    error
		message string
		limit   int64
	}{
		{"first-overflow", []*memory.Batch{n27Page(3, 0, 0, nil, 0, 20)}, false, aggregate.ErrDecimalAverageOverflow, "", 1 << 20},
		{"later-overflow", []*memory.Batch{n27Page(3, 0, 0, nil, 0), n27Page(3, 0, 1, nil, 0, 30)}, true, aggregate.ErrDecimalAverageOverflow, "", 1 << 20},
		{"later-tie", []*memory.Batch{n27Page(3, 0, 0, nil, 0), n27Page(3, 0, 0, nil, 0)}, true, nil, "strictly ascending", 1 << 20},
		{"later-decreasing", []*memory.Batch{n27Page(3, 0, 0, nil, 0), n27Page(3, 0, -1, nil, 0)}, true, nil, "strictly ascending", 1 << 20},
		{"limit-minus-one", []*memory.Batch{n27Page(3, 0, 0, nil, 0, 0)}, false, memory.ErrBudgetExceeded, "", 551},
		{"base-cancel-after-read", []*memory.Batch{n27Page(3, 0, 0, nil, 0)}, false, context.Canceled, "", 1 << 20},
		{"next-cancel-after-read", []*memory.Batch{n27Page(3, 0, 0, nil, 0)}, false, context.Canceled, "", 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, cancelBase := context.WithCancel(context.Background())
			defer cancelBase()
			call, cancelCall := context.WithCancel(context.Background())
			defer cancelCall()
			fixture := n27Source(t, 3, 0, tc.pages...)
			if tc.name == "base-cancel-after-read" {
				fixture.hook = func(int) { cancelBase() }
			}
			if tc.name == "next-cancel-after-read" {
				fixture.hook = func(int) { cancelCall() }
			}
			m := memory.NewBudgetManager(tc.limit)
			st := n27Open(t, base, fixture, n27Spec(1, 0, aggregate.DecimalAverageHalfEven), n27Opts(m))
			dst := memory.NewBatch()
			if tc.later {
				if ok, err := st.Next(call, dst); !ok || err != nil || dst.Rows != 1 || m.Used() != 536 {
					t.Fatal(ok, err, dst.Rows, m.Used())
				}
			}
			ok, err := st.Next(call, dst)
			if ok || err == nil || tc.want != nil && !errors.Is(err, tc.want) || tc.message != "" && !strings.Contains(err.Error(), tc.message) || dst.Rows != 0 || len(dst.Int64s) != 0 || len(dst.Decimals) != 0 || m.Used() != 0 || fixture.closes != 1 {
				t.Fatalf("failure: ok=%v err=%v batch=%+v used=%d closes=%d", ok, err, dst, m.Used(), fixture.closes)
			}
			if err = st.Close(); err != nil || fixture.closes != 1 || m.Used() != 0 {
				t.Fatal(err, fixture.closes, m.Used())
			}
		})
	}
}

func TestN27DecimalPrefixBudgetAndAdmission(t *testing.T) {
	ctx := context.Background()
	cfg := n27Spec(4, 0, aggregate.DecimalAverageHalfEven)
	fixture := n27Source(t, 3, 0, n27Page(3, 0, 0, nil, 2, 6))
	op, err := NewDecimalPrefixAverageSource(fixture, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []memory.Manager{nil, memory.NewBudgetManager(0), memory.NewBudgetManager(511)} {
		if st, err := op.NewBatchStream(ctx, n27Opts(m)); err == nil || st != nil || fixture.opens != 0 {
			t.Fatal(st, err, fixture.opens)
		}
	}
	m := memory.NewBudgetManager(552)
	st, err := op.NewBatchStream(ctx, n27Opts(m))
	if err != nil {
		t.Fatal(err)
	}
	dst := memory.NewBatch()
	if ok, err := st.Next(ctx, dst); !ok || err != nil || m.Used() != 552 {
		t.Fatal(ok, err, m.Used())
	}
	if err := st.Close(); err != nil || m.Used() != 0 || fixture.closes != 1 {
		t.Fatal(err, m.Used(), fixture.closes)
	}
	for _, name := range []string{"descending", "absent-proof", "spill", "alias", "frame", "rounding", "precision"} {
		t.Run(name, func(t *testing.T) {
			local := n27Source(t, 3, 0)
			spec := cfg
			switch name {
			case "descending":
				local.ordering[0].Descending = true
			case "absent-proof":
				local.ordering = nil
			case "spill":
				local.plan.Children = []source.StreamPlanNode{{Class: source.StreamSpillable}}
			case "alias":
				spec.Alias = "v"
			case "frame":
				spec.Frame.Start.Offset = 1
			case "rounding":
				spec.Rounding = 0
			case "precision":
				spec.Precision = 0
			}
			if op, err := NewDecimalPrefixAverageSource(local, spec); err == nil || op != nil || local.opens != 0 {
				t.Fatal(op, err, local.opens)
			}
		})
	}
}

func TestN27DecimalPrefixCancelBlockedChild(t *testing.T) {
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	entered := make(chan struct{})
	fixture := n27Source(t, 3, 0)
	fixture.wait = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	m := memory.NewBudgetManager(1 << 20)
	st := n27Open(t, base, fixture, n27Spec(4, 0, aggregate.DecimalAverageHalfEven), n27Opts(m))
	call, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()
	dst := memory.NewBatch()
	type outcome struct {
		ok  bool
		err error
	}
	done := make(chan outcome, 1)
	go func() { ok, err := st.Next(call, dst); done <- outcome{ok, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("child never entered")
	}
	cancelBase()
	select {
	case result := <-done:
		if result.ok || !errors.Is(result.err, context.Canceled) || call.Err() != nil || dst.Rows != 0 || m.Used() != 0 || fixture.closes != 1 {
			t.Fatal(result, call.Err(), m.Used(), fixture.closes)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("base cancellation did not wake child")
	}
	if err := st.Close(); err != nil || fixture.closes != 1 {
		t.Fatal(err, fixture.closes)
	}
}

func TestN27DecimalPrefixHardCaps(t *testing.T) {
	ctx := context.Background()
	batch := n27Page(3, 0, 0, memory.Bitmap{3}, 0, 1)
	for _, tc := range []struct {
		name                 string
		rows                 int
		bytes, payload, soft int64
		openFail, pass       bool
	}{
		{name: "exact", rows: 2, bytes: 40, payload: 96, pass: true},
		{name: "row-minus-one", rows: 1, bytes: 40, payload: 96},
		{name: "output-minus-one", rows: 2, bytes: 39, payload: 96},
		{name: "payload-minus-one", rows: 2, bytes: 40, payload: 95},
		{name: "soft-unsupported", rows: 2, bytes: 40, payload: 96, soft: 95, openFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := n27Source(t, 3, 0, batch)
			op, err := NewDecimalPrefixAverageSource(fx, n27Spec(4, 0, aggregate.DecimalAverageHalfEven))
			if err != nil {
				t.Fatal(err)
			}
			m := memory.NewBudgetManager(1 << 20)
			opts := n27Opts(m)
			opts.BatchRows = tc.rows
			opts.BatchBytes = tc.bytes
			opts.BatchPayloadBytes = tc.payload
			opts.BatchPayloadTargetBytes = tc.soft
			st, err := op.NewBatchStream(ctx, opts)
			if tc.openFail {
				if err == nil || st != nil || m.Used() != 0 || fx.opens != 0 {
					t.Fatal(st, err, m.Used(), fx.opens)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			dst := memory.NewBatch()
			ok, err := st.Next(ctx, dst)
			if tc.pass {
				if !ok || err != nil || dst.Rows != 2 {
					t.Fatal(ok, err, dst.Rows)
				}
			} else if ok || !errors.Is(err, memory.ErrBudgetExceeded) || dst.Rows != 0 || m.Used() != 0 || fx.closes != 1 {
				t.Fatal(ok, err, dst.Rows, m.Used(), fx.closes)
			}
		})
	}
}

func TestN27DecimalPrefixDoesNotBroadenLegacyWindow(t *testing.T) {
	fx := n27Source(t, 3, 0)
	cfg := n27Spec(4, 0, aggregate.DecimalAverageHalfEven)
	output := []memory.Field{{Name: "m", Type: memory.TypeFloat64, Nullable: true, Encoding: memory.EncodingPlain}}
	expressions := []WindowExpression{{Column: cfg.Column, Alias: cfg.Alias, Function: WindowAvg, Frame: cfg.Frame}}
	keys := []SortKey{{Column: "k"}}
	m := memory.NewBudgetManager(1 << 20)
	if op, err := NewWindowSource(fx, nil, keys, expressions, output, m); op != nil || err == nil || !strings.Contains(strings.ToLower(err.Error()), "decimal") {
		t.Fatal(op, err)
	}
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		if op, err := NewWindowSourceWithOptions(fx, nil, keys, expressions, output, m, WindowSourceOptions{Mode: mode}); op != nil || err == nil || !strings.Contains(strings.ToLower(err.Error()), "decimal") {
			t.Fatal(mode, op, err)
		}
	}
	if fx.opens != 0 || m.Used() != 0 {
		t.Fatal(fx.opens, m.Used())
	}
}
