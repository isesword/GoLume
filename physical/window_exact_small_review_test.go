package physical

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// This matrix deliberately mixes matching and nonmatching states and replaces
// the input alias before/after its siblings. Expected values enumerate logical
// frames with big.Rat, independent of production cursors and accumulation.
func TestWindowExactSmallReviewMatrix(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			for _, typ := range []memory.DataType{memory.TypeFloat64, memory.TypeFloat32, memory.TypeInt64, memory.TypeUint64} {
				for _, reverse := range []bool{false, true} {
					t.Run(fmt.Sprintf("width%d/mode%d/type%d/reverse%v", width, mode, typ, reverse), func(t *testing.T) {
						exactSmallReviewRun(t, width, mode, typ, reverse, false, "")
					})
				}
			}
		}
		t.Run(fmt.Sprintf("spill/width%d", width), func(t *testing.T) {
			exactSmallReviewRun(t, width, WindowExecutionExternalSort, memory.TypeFloat64, true, true, "")
		})
	}
}

func TestWindowExactSmallReviewCleanup(t *testing.T) {
	for _, width := range []int{1, 64} {
		for _, stop := range []string{"early", "cancel", "admission"} {
			t.Run(fmt.Sprintf("width%d/%s", width, stop), func(t *testing.T) {
				exactSmallReviewRun(t, width, WindowExecutionOrdered, memory.TypeFloat64, false, false, stop)
			})
		}
	}
}

func exactSmallReviewRun(t *testing.T, width int, mode WindowExecutionMode, typ memory.DataType, reverse, spill bool, stop string) {
	t.Helper()
	ctx, _, query := spillOutputQuery(t)
	if spill {
		engine := memory.NewEngineBudget(4 << 20)
		var err error
		query, err = engine.BeginQuery(memory.BudgetOptions{SoftLimitBytes: 48 << 10, HardLimitBytes: 4 << 20, Spill: memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := query.Close(); err != nil {
				t.Error(err)
			}
		})
		ctx = memory.WithQueryBudget(context.Background(), query)
	}
	const partition = 137
	n := partition * 3
	if spill {
		n = 2192
	}
	rats := make([]*big.Rat, n)
	specials := make([]float64, n)
	valid := make([]bool, n)
	groups := make([]int64, n)
	pattern := []float64{math.Copysign(0, -1), 1e16, 1, -1e16, math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64, math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 2, 3, math.NaN(), 4, math.Inf(1), math.Inf(-1), 5}
	for i := 0; i < n; i++ {
		groups[i] = int64(i / partition)
		valid[i] = i/partition != 1 && i%13 != 6
		switch typ {
		case memory.TypeInt64:
			rats[i] = new(big.Rat).SetInt64([]int64{1<<53 | 1, -(1 << 53), math.MaxInt64, math.MinInt64, 1, 2}[i%6])
		case memory.TypeUint64:
			rats[i] = new(big.Rat).SetInt(new(big.Int).SetUint64([]uint64{1<<53 | 1, 1<<53 | 2, math.MaxUint64, math.MaxUint64 - 2, 1, 2}[i%6]))
		default:
			v := pattern[i%len(pattern)]
			if typ == memory.TypeFloat32 {
				v = float64(float32(v))
				if math.IsInf(v, 0) && !math.IsInf(pattern[i%len(pattern)], 0) {
					v = math.Copysign(math.MaxFloat32, v)
				}
			}
			specials[i] = v
			rats[i] = new(big.Rat).SetFloat64(v)
		}
	}
	var batches []*memory.Batch
	// Uneven physical chunks cross partitions and frame wraparound points.
	for first := 0; first < n; {
		size := min(n-first, []int{1, 3, 17, 2, 65}[len(batches)%5])
		b := memory.NewBatch()
		b.Rows = size
		ids := make([]int64, size)
		gs := make([]int64, size)
		f64 := make([]float64, size)
		f32 := make([]float32, size)
		iv := make([]int64, size)
		uv := make([]uint64, size)
		other := make([]float64, size)
		bitmap := memory.EnsureBitmap(nil, size)
		for j := 0; j < size; j++ {
			i := first + j
			if spill {
				i = n - 1 - i
			}
			ids[j] = int64(i)
			gs[j] = groups[i]
			other[j] = 7
			if valid[i] {
				memory.Set(bitmap, j)
			}
			switch typ {
			case memory.TypeInt64:
				iv[j] = rats[i].Num().Int64()
			case memory.TypeUint64:
				uv[j] = rats[i].Num().Uint64()
			default:
				f64[j] = specials[i]
				f32[j] = float32(specials[i])
			}
		}
		b.Int64s["id"] = &memory.Int64Vector{Values: ids}
		b.Int64s["g"] = &memory.Int64Vector{Values: gs}
		b.Float64s["other"] = &memory.Float64Vector{Values: other}
		switch typ {
		case memory.TypeInt64:
			b.Int64s["v"] = &memory.Int64Vector{Values: iv, Validity: bitmap}
		case memory.TypeUint64:
			b.Uint64s["v"] = &memory.Uint64Vector{Values: uv, Validity: bitmap}
		case memory.TypeFloat32:
			b.Float32s["v"] = &memory.Float32Vector{Values: f32, Validity: bitmap}
		default:
			b.Float64s["v"] = &memory.Float64Vector{Values: f64, Validity: bitmap}
		}
		batches = append(batches, b)
		first += size
	}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: typ, Nullable: true}, memory.Field{Name: "other", Type: memory.TypeFloat64})
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: int64(width - 1)}, End: WindowBound{Kind: WindowCurrentRow}}
	current := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	expr := []WindowExpression{{Function: WindowAvg, Column: "v", Alias: "v", Frame: frame}, {Function: WindowAvg, Column: "v", Alias: "a2", Frame: frame}, {Function: WindowAvg, Column: "v", Alias: "current", Frame: current}, {Function: WindowSum, Column: "other", Alias: "otherSum", Frame: frame}}
	if typ == memory.TypeFloat32 || typ == memory.TypeFloat64 {
		expr = append(expr, WindowExpression{Function: WindowSum, Column: "v", Alias: "sum", Frame: frame}, WindowExpression{Function: WindowSum, Column: "v", Alias: "s2", Frame: frame})
	}
	if reverse {
		for i, j := 0, len(expr)-1; i < j; i, j = i+1, j-1 {
			expr[i], expr[j] = expr[j], expr[i]
		}
	}
	fields := make([]memory.Field, len(expr))
	for i, e := range expr {
		fields[i] = memory.Field{Name: e.Alias, Type: memory.TypeFloat64, Nullable: true}
	}
	var manager memory.Manager = query
	if stop == "admission" {
		manager = memory.NewBudgetManager(1)
		ctx = context.Background()
	}
	win, err := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, expr, fields, manager, WindowSourceOptions{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 11})
	if stop == "admission" {
		if stream != nil {
			stream.Close()
		}
		if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
			t.Fatalf("admission err=%v retained=%d", err, manager.Used())
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := 0
	for {
		ok, err := stream.Next(ctx, out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for j := 0; j < out.Rows; j++ {
			row := int(out.Int64s["id"].Values[j])
			seen++
			for _, e := range expr {
				lo := 1 - width
				if e.Alias == "current" {
					lo = 0
				}
				want, present := exactReviewOracle(rats, specials, valid, groups, row, lo, 0, e.Function == WindowAvg)
				if e.Alias == "otherSum" {
					want = float64(min(width, row%partition+1)) * 7
					present = true
				}
				got := out.Float64s[e.Alias]
				if got == nil || got.IsValid(j) != present {
					t.Fatalf("row%d %s validity mismatch", row, e.Alias)
				}
				if present && !(math.IsNaN(want) && math.IsNaN(got.Values[j])) && math.Float64bits(want) != math.Float64bits(got.Values[j]) {
					t.Fatalf("row%d %s got=%g [%x] want=%g [%x]", row, e.Alias, got.Values[j], math.Float64bits(got.Values[j]), want, math.Float64bits(want))
				}
			}
		}
		if stop == "early" {
			break
		}
		if stop == "cancel" {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			if ok, err := stream.Next(cancelCtx, out); ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel ok=%v err=%v", ok, err)
			}
			break
		}
	}
	if stop == "" && seen != n {
		t.Fatalf("rows=%d want=%d", seen, n)
	}
	if spill && query.Snapshot().SpilledBytes == 0 {
		t.Fatal("external mode did not actually spill")
	}
	out.ClearColumns()
	checkSpillOutputCleanup(t, stream, query)
	// Closing and alias installation must not modify any source vector.
	for _, b := range batches {
		for j, id := range b.Int64s["id"].Values {
			i := int(id)
			switch typ {
			case memory.TypeFloat64:
				if math.Float64bits(b.Float64s["v"].Values[j]) != math.Float64bits(specials[i]) {
					t.Fatal("source mutated")
				}
			case memory.TypeFloat32:
				if math.Float32bits(b.Float32s["v"].Values[j]) != math.Float32bits(float32(specials[i])) {
					t.Fatal("source mutated")
				}
			}
		}
	}
}

// A source may recycle its previous batch as soon as Next is called again.
// Poison all borrowed numeric payloads to catch retained vector bindings/ring
// entries, rather than merely asserting that the input itself is unmodified.
type exactSmallPoisonStream struct {
	source.BatchStream
	previous *memory.Batch
}

func (s *exactSmallPoisonStream) Next(ctx context.Context, b *memory.Batch) (bool, error) {
	if s.previous != nil {
		for _, v := range s.previous.Float64s {
			for i := range v.Values {
				v.Values[i] = math.NaN()
			}
		}
	}
	ok, err := s.BatchStream.Next(ctx, b)
	s.previous = b
	return ok, err
}
func TestWindowExactSmallReviewBorrowedInput(t *testing.T) {
	for _, width := range []int{1, 2, 4, 8, 16, 32, 64} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			manager := memory.NewBudgetManager(1 << 20)
			owner := exactBudgetWindow(t, manager)
			for i := range owner.expressions {
				owner.expressions[i].Frame = WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: int64(width - 1)}, End: WindowBound{Kind: WindowCurrentRow}}
			}
			stream, err := newOrderedWindowStream(context.Background(), owner, owner.input, owner.output, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			ordered := stream.(*orderedWindowStream)
			ordered.child = &exactSmallPoisonStream{BatchStream: ordered.child}
			values := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1}
			rats := make([]*big.Rat, 4)
			for i, v := range values {
				rats[i] = new(big.Rat).SetFloat64(v)
			}
			out := memory.NewBatch()
			seen := 0
			for {
				ok, err := stream.Next(context.Background(), out)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				for j := 0; j < out.Rows; j++ {
					for _, name := range []string{"s", "a"} {
						want, _ := exactReviewOracle(rats, values, []bool{true, true, true, true}, []int64{0, 0, 0, 0}, seen, 1-width, 0, name == "a")
						got := out.Float64s[name]
						if !got.IsValid(j) || math.Float64bits(got.Values[j]) != math.Float64bits(want) {
							t.Fatalf("row%d %s got=%g want=%g", seen, name, got.Values[j], want)
						}
					}
					seen++
				}
			}
			if seen != 4 {
				t.Fatal(seen)
			}
			out.ClearColumns()
			if err := stream.Close(); err != nil || manager.Used() != 0 {
				t.Fatal(err, manager.Used())
			}
		})
	}
}

func TestWindowExactSmallReviewPlanAndStreamIsolation(t *testing.T) {
	manager := memory.NewBudgetManager(1 << 20)
	owner := exactBudgetWindow(t, manager)
	current := WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
	precedingZero := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 0}, End: WindowBound{Kind: WindowCurrentRow}}
	expressions := []WindowExpression{
		{Function: WindowAvg, Column: "v", Alias: "a", Frame: current},
		{Function: WindowSum, Column: "v", Alias: "s", Frame: precedingZero},
		{Function: WindowAvg, Column: "v", Alias: "trailing", Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 2}, End: WindowBound{Kind: WindowCurrentRow}}},
		{Function: WindowShift, Column: "v", Alias: "future", Frame: WindowFrame{Start: WindowBound{Kind: WindowFollowing, Offset: 1}, End: WindowBound{Kind: WindowFollowing, Offset: 1}}},
	}
	fields := []memory.Field{{Name: "a", Type: memory.TypeFloat64, Nullable: true}, {Name: "s", Type: memory.TypeFloat64, Nullable: true}, {Name: "trailing", Type: memory.TypeFloat64, Nullable: true}, {Name: "future", Type: memory.TypeFloat64, Nullable: true}}
	src, err := NewWindowSourceWithOptions(owner.input, nil, []SortKey{{Column: "id"}}, expressions, fields, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	owner = src.(*WindowSource)
	if owner.exactPlans[0] != owner.exactPlans[1] || owner.exactPlans[0] == owner.exactPlans[2] || owner.exactPlans[3] != nil {
		t.Fatal("incorrect normalized grouping")
	}
	streams := make([]*orderedWindowStream, 2)
	for i := range streams {
		stream, err := source.OpenBatchStream(context.Background(), src, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		streams[i] = stream.(*orderedWindowStream)
	}
	if streams[0].owner == owner || owner.expressions[3].Function != WindowShift || owner.expressions[3].Column != "v" {
		t.Fatal("lookahead clone mutated original owner")
	}
	for i := range streams {
		if streams[i].states[0] != streams[i].states[1] || streams[i].states[0] == streams[i].states[2] {
			t.Fatal("execution sharing differs from plan")
		}
		if streams[i].states[0].exactSum != nil || len(streams[i].states[0].queue.values) != 0 {
			t.Fatal("singleton retained accumulator/ring")
		}
	}
	if streams[0].states[0] == streams[1].states[0] || streams[0].states[2].exactSum == streams[1].states[2].exactSum {
		t.Fatal("streams share mutable state")
	}
	values := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1}
	rats := make([]*big.Rat, 4)
	for i, v := range values {
		rats[i] = new(big.Rat).SetFloat64(v)
	}
	for row := 0; row < 4; row++ {
		for _, s := range streams {
			out := memory.NewBatch()
			ok, err := s.Next(context.Background(), out)
			if !ok || err != nil {
				t.Fatal(ok, err)
			}
			for _, state := range s.states {
				if state.exactInput.f64 != nil || state.exactInput.other != nil {
					t.Fatal("borrowed binding retained across Next")
				}
			}
			for _, v := range s.exactOutputs {
				if v != nil {
					t.Fatal("output binding retained across Next")
				}
			}
			want, _ := exactReviewOracle(rats, values, []bool{true, true, true, true}, []int64{0, 0, 0, 0}, row, -2, 0, true)
			if math.Float64bits(out.Float64s["trailing"].Values[0]) != math.Float64bits(want) || out.Float64s["s"].Values[0] != values[row] || out.Float64s["a"].Values[0] != values[row] {
				t.Fatal("interleaved stream result mismatch")
			}
			out.ClearColumns()
		}
	}
	for _, s := range streams {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if manager.Used() != 0 {
		t.Fatalf("retained=%d", manager.Used())
	}
}

func TestWindowExactSmallReviewNonCurrentIntegerSingleton(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		for _, offset := range []int64{-math.MaxInt64, -4, -1, 0, 1, 4, math.MaxInt64} {
			b := memory.NewBatch()
			b.Rows = 5
			typ := memory.TypeInt64
			rats := make([]*big.Rat, 5)
			if unsigned {
				typ = memory.TypeUint64
				values := []uint64{math.MaxUint64, 1<<53 | 1, 0, 1<<53 | 3, 1}
				b.Uint64s["v"] = &memory.Uint64Vector{Values: values, Validity: memory.Bitmap{0b11011}}
				for i, v := range values {
					rats[i] = new(big.Rat).SetInt(new(big.Int).SetUint64(v))
				}
			} else {
				values := []int64{math.MinInt64, 1<<53 | 1, 0, -(1<<53 | 3), math.MaxInt64}
				b.Int64s["v"] = &memory.Int64Vector{Values: values, Validity: memory.Bitmap{0b11011}}
				for i, v := range values {
					rats[i] = new(big.Rat).SetInt64(v)
				}
			}
			bound := WindowBound{Kind: WindowCurrentRow}
			if offset < 0 {
				bound = WindowBound{Kind: WindowPreceding, Offset: -offset}
			} else if offset > 0 {
				bound = WindowBound{Kind: WindowFollowing, Offset: offset}
			}
			expr := WindowExpression{Function: WindowAvg, Column: "v", Alias: "v", Frame: WindowFrame{Start: bound, End: bound}}
			output := memory.NewBatch()
			output.Rows = 5
			if err := evaluateExactWindowFloat(context.Background(), b, memory.NewSchema(memory.Field{Name: "v", Type: typ, Nullable: true}), output, []int{0, 1, 2, 3, 4}, expr, true); err != nil {
				t.Fatal(err)
			}
			for row := 0; row < 5; row++ {
				lo := int(max(int64(-5), min(int64(5), offset)))
				want, valid := exactReviewOracle(rats, nil, []bool{true, true, false, true, true}, []int64{0, 0, 0, 0, 0}, row, lo, lo, true)
				got := output.Float64s["v"]
				if got.IsValid(row) != valid || valid && math.Float64bits(got.Values[row]) != math.Float64bits(want) {
					t.Fatalf("unsigned%v offset%d row%d got=%g/%v want=%g/%v", unsigned, offset, row, got.Values[row], got.IsValid(row), want, valid)
				}
			}
		}
	}
}
