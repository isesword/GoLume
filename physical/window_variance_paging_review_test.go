package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

// Emulates a front-sort owner publishing one large borrowed page regardless of
// downstream BatchRows. It keeps its complete owned buffer and lease until Next.
type variancePageReviewSource struct {
	source.Source
	fixture              *memory.Batch
	manager              memory.Manager
	failSecond           bool
	opens, nexts, closes int
	live                 *variancePageReviewStream
}
type variancePageReviewStream struct {
	owner  *variancePageReviewSource
	batch  *memory.Batch
	lease  memory.Reservation
	closed bool
}

var variancePageReviewFailure = errors.New("variance paging upstream sentinel")

func (s *variancePageReviewSource) NewBatchStream(ctx context.Context, _ source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("source reopened")
	}
	bytes, err := futureCloneAdmissionBytes(s.fixture)
	if err != nil {
		return nil, err
	}
	lease, err := s.manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	clone, err := memory.CloneBatchRange(s.fixture, 0, s.fixture.Rows)
	if err != nil {
		lease.Release()
		return nil, err
	}
	st := &variancePageReviewStream{owner: s, batch: clone, lease: lease}
	s.live = st
	return st, nil
}
func (s *variancePageReviewStream) release() {
	if s.batch != nil {
		for _, v := range s.batch.Int64s {
			for i := range v.Values {
				v.Values[i] = -999
			}
		}
		s.batch.ClearColumns()
		s.batch = nil
	}
	if s.lease != nil {
		s.lease.Release()
		s.lease = nil
	}
}
func (s *variancePageReviewStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.owner.nexts++
	dst.ClearColumns()
	if s.owner.nexts == 1 {
		bindAll(dst, s.batch)
		dst.Rows = s.batch.Rows
		return true, nil
	}
	s.release()
	if s.owner.failSecond {
		return false, variancePageReviewFailure
	}
	return false, nil
}
func (s *variancePageReviewStream) Close() error {
	if !s.closed {
		s.closed = true
		s.owner.closes++
		s.release()
	}
	return nil
}

func variancePageReviewFixture(t *testing.T, q *memory.QueryBudget, n int, paged bool) (*futureWindowSource, *variancePageReviewSource) {
	t.Helper()
	b := memory.NewBatch()
	b.Reset(n)
	ids, values := make([]int64, n), make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = int64(i % 17)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	b.Int64s["v"] = &memory.Int64Vector{Values: values}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64})
	base, err := inmemory.New(schema, b)
	if err != nil {
		t.Fatal(err)
	}
	src := &variancePageReviewSource{Source: base, fixture: b, manager: q}
	field := memory.Field{Name: "hidden", Type: memory.TypeFloat64, Nullable: true}
	wrapper := &futureWindowSource{input: src, schema: memory.NewSchema(append(append([]memory.Field(nil), schema.Fields...), field)...), expressions: []WindowExpression{{Function: WindowVarPop, Column: "v", Alias: "variance", Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 1}}}}, fields: []memory.Field{field}, names: []string{"hidden"}, maxLead: 1, manager: q, pageVariance: paged}
	return wrapper, src
}

func TestVariancePagingReviewTailAndCap(t *testing.T) {
	for _, requested := range []int{0, 1, 7, 1024, 8192} {
		t.Run(fmt.Sprint(requested), func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			wrapper, src := variancePageReviewFixture(t, q, 2051, true)
			st, err := wrapper.NewBatchStream(ctx, source.ReadOptions{BatchRows: requested, Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			capRows := requested
			if capRows <= 0 || capRows > 1024 {
				capRows = 1024
			}
			seen := 0
			for {
				out := memory.NewBatch()
				ok, e := st.Next(ctx, out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				if out.Rows < 1 || out.Rows > capRows {
					t.Fatal("page cap", out.Rows, capRows)
				}
				if seen == 0 {
					if src.nexts != 1 || src.live.batch == nil || src.live.lease == nil {
						t.Fatal("advanced source before consuming tail", src.nexts)
					}
					concrete := st.(*futureWindowStream)
					if concrete.scratchOffset >= 2051 || concrete.scratchOffset <= 0 {
						t.Fatal("missing partial scratch tail", concrete.scratchOffset)
					}
				}
				for row, id := range out.Int64s["id"].Values {
					if id != int64(seen) {
						t.Fatal("lost/duplicated/poisoned tail", id, seen)
					}
					lo, hi := max(0, seen-1), min(2050, seen+1)
					var xs []float64
					var valid []bool
					for i := lo; i <= hi; i++ {
						xs = append(xs, float64(i%17))
						valid = append(valid, true)
					}
					want, wv := varianceReviewFloatOracle(xs, valid, false)
					v := out.Float64s["hidden"]
					varianceReviewAssert(t, v.Values[row], v.IsValid(row), want, wv)
					seen++
				}
			}
			if seen != 2051 || src.nexts != 2 {
				t.Fatal("source pagination", seen, src.nexts)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if src.opens != 1 || src.closes != 1 || q.Used() != 0 {
				t.Fatal("lifecycle", src.opens, src.closes, q.Used())
			}
		})
	}
}

func TestVariancePagingReviewLegacyWholePage(t *testing.T) {
	ctx, q := percentRankReviewQuery(t, 8<<20, true)
	wrapper, src := variancePageReviewFixture(t, q, 2051, false)
	st, err := wrapper.NewBatchStream(ctx, source.ReadOptions{BatchRows: 7, Kernels: offsetTestKernels()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	out := memory.NewBatch()
	ok, err := st.Next(ctx, out)
	if err != nil || !ok || out.Rows != 2051 || src.nexts != 2 {
		t.Fatal("legacy whole page changed", ok, err, out.Rows, src.nexts)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	if q.Used() != 0 {
		t.Fatal("leak", q.Used())
	}
}

func TestVariancePagingReviewTailCleanup(t *testing.T) {
	for _, kind := range []string{"close", "cancel", "upstream-error"} {
		t.Run(kind, func(t *testing.T) {
			base, q := percentRankReviewQuery(t, 8<<20, true)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			wrapper, src := variancePageReviewFixture(t, q, 31, true)
			src.failSecond = kind == "upstream-error"
			st, err := wrapper.NewBatchStream(ctx, source.ReadOptions{BatchRows: 7, Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ok, err := st.Next(ctx, memory.NewBatch())
			if err != nil || !ok || src.nexts != 1 || src.live.lease == nil {
				t.Fatal("first yield must retain full source", ok, err, src.nexts)
			}
			if kind == "cancel" {
				cancel()
				_, err = st.Next(ctx, memory.NewBatch())
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			if kind == "upstream-error" {
				for {
					ok, err = st.Next(ctx, memory.NewBatch())
					if err != nil || !ok {
						break
					}
				}
				if !errors.Is(err, variancePageReviewFailure) {
					t.Fatal("lost source error", err)
				}
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if src.closes != 1 || src.live.batch != nil || src.live.lease != nil || q.Used() != 0 {
				t.Fatal("cleanup", src.closes, q.Used())
			}
			concrete := st.(*futureWindowStream)
			if concrete.scratch != nil || concrete.current != nil || concrete.head != nil {
				t.Fatal("retained owner graph")
			}
		})
	}
}

func TestVariancePagingReviewActivationScope(t *testing.T) {
	for _, kind := range []string{"external-variance", "ordered-variance", "external-legacy", "external-variance-shift", "external-variance-rank", "external-variance-pr"} {
		t.Run(kind, func(t *testing.T) {
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			_, src := variancePageReviewFixture(t, q, 31, true)
			mode := WindowExecutionExternalSort
			if kind == "ordered-variance" {
				mode = WindowExecutionOrdered
			}
			frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 1}}
			es := []WindowExpression{{Function: WindowVarPop, Column: "v", Alias: "variance", Frame: frame}}
			fields := []memory.Field{{Name: "variance", Type: memory.TypeFloat64, Nullable: true}}
			switch kind {
			case "external-legacy":
				es[0].Function = WindowAvg
			case "external-variance-shift":
				bound := WindowBound{Kind: WindowFollowing, Offset: 1}
				es = append(es, WindowExpression{Function: WindowShift, Column: "v", Alias: "lead", Frame: WindowFrame{Start: bound, End: bound}})
				fields = append(fields, memory.Field{Name: "lead", Type: memory.TypeInt64, Nullable: true})
			case "external-variance-rank":
				es = append(es, WindowExpression{Function: WindowRank, Alias: "rank"})
				fields = append(fields, memory.Field{Name: "rank", Type: memory.TypeInt64})
			case "external-variance-pr":
				es = append(es, WindowExpression{Function: WindowPercentRank, Alias: "pr"})
				fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
			}
			planned, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, es, fields, q, WindowSourceOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			owner := planned.(*WindowSource)
			// Invoke the internal evaluator entry with the original mode, exactly as the
			// external pipeline does after front-sort construction. No source reads needed.
			st, err := newOrderedWindowStream(ctx, owner, src, owner.output, source.ReadOptions{BatchRows: 7, Kernels: offsetTestKernels()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ordered := st.(*orderedWindowStream)
			future, ok := ordered.child.(*futureWindowStream)
			if !ok {
				t.Fatalf("expected lookahead, got %T", ordered.child)
			}
			want := kind == "external-variance" || kind == "external-variance-rank"
			if future.owner.pageVariance != want || (future.pageRows > 0) != want {
				t.Fatal("activation", kind, future.owner.pageVariance, future.pageRows, want)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if q.Used() != 0 {
				t.Fatal("scope lifecycle", q.Used())
			}
		})
	}
}

func TestVariancePagingReviewTailCloneAdmissionFailure(t *testing.T) {
	ctx, q := percentRankReviewQuery(t, 8<<20, true)
	wrapper, src := variancePageReviewFixture(t, q, 31, true)
	manager := &rankSpoolFailManager{Manager: q}
	wrapper.manager = manager
	st, err := wrapper.NewBatchStream(ctx, source.ReadOptions{BatchRows: 7, Kernels: offsetTestKernels()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ok, err := st.Next(ctx, memory.NewBatch())
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	concrete := st.(*futureWindowStream)
	if concrete.scratchOffset != 14 || src.nexts != 1 || src.live.lease == nil {
		t.Fatal("expected held input tail before admission failure", concrete.scratchOffset, src.nexts)
	}
	manager.fail = manager.calls + 1
	ok, err = st.Next(ctx, memory.NewBatch())
	if ok || !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal("expected next clone admission failure", ok, err)
	}
	if src.nexts != 1 {
		t.Fatal("failed clone advanced upstream", src.nexts)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	if src.closes != 1 || src.live.batch != nil || src.live.lease != nil || q.Used() != 0 {
		t.Fatal("admission failure retained owners", src.closes, q.Used())
	}
}
