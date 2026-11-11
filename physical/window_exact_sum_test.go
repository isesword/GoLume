package physical

import (
	"context"
	"errors"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/bits"
	"testing"
	"unsafe"
)

func exactBudgetWindow(t *testing.T, manager memory.Manager) *WindowSource {
	t.Helper()
	batch := memory.NewBatch()
	batch.Rows = 4
	batch.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3}}
	batch.Float64s["v"] = &memory.Float64Vector{Values: []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1}}
	src, err := inmemory.New(memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64}), batch)
	if err != nil {
		t.Fatal(err)
	}
	frame := WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
	win, err := NewWindowSourceWithOptions(src, nil, []SortKey{{Column: "id"}}, []WindowExpression{{Function: WindowSum, Column: "v", Alias: "s", Frame: frame}, {Function: WindowAvg, Column: "v", Alias: "a", Frame: frame}}, []memory.Field{{Name: "s", Type: memory.TypeFloat64, Nullable: true}, {Name: "a", Type: memory.TypeFloat64, Nullable: true}}, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	return win.(*WindowSource)
}

func TestWindowExactStateAdmissionAndRelease(t *testing.T) {
	if bytes := int64(unsafe.Sizeof(windowExactSum{})); bytes > windowExactSumBytes {
		t.Fatalf("exact state size=%d admitted=%d", bytes, windowExactSumBytes)
	}
	const admitted = 512 + 2*(windowStateCellBytes+windowExactSumBytes)
	for _, limit := range []int64{admitted - 1, admitted, admitted + 4096} {
		manager := memory.NewBudgetManager(limit)
		owner := exactBudgetWindow(t, manager)
		stream, err := newOrderedWindowStream(context.Background(), owner, owner.input, owner.output, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
		if limit < admitted {
			if !errors.Is(err, memory.ErrBudgetExceeded) || manager.Used() != 0 {
				t.Fatalf("rejected open err=%v retained=%d", err, manager.Used())
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		ordered := stream.(*orderedWindowStream)
		if manager.Used() != admitted || ordered.baseBytes != admitted {
			t.Fatalf("used=%d base=%d want=%d", manager.Used(), ordered.baseBytes, admitted)
		}
		for _, state := range ordered.states {
			if state.exactSum == nil {
				t.Fatal("missing admitted exact state")
			}
		}
		out := memory.NewBatch()
		ok, nextErr := stream.Next(context.Background(), out)
		if limit == admitted {
			if ok || !errors.Is(nextErr, memory.ErrBudgetExceeded) {
				t.Fatalf("output admission got=%v err=%v", ok, nextErr)
			}
		} else if !ok || nextErr != nil {
			t.Fatal(ok, nextErr)
		}
		out.ClearColumns()
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 0 || ordered.states != nil {
			t.Fatalf("closed state retained: used=%d states=%d", manager.Used(), len(ordered.states))
		}
	}
}

func TestWindowExactCanceledOpen(t *testing.T) {
	manager := memory.NewBudgetManager(4096)
	owner := exactBudgetWindow(t, manager)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream, err := newOrderedWindowStream(ctx, owner, owner.input, owner.output, source.ReadOptions{Kernels: offsetTestKernels()})
	if stream != nil {
		stream.Close()
	}
	if !errors.Is(err, context.Canceled) || manager.Used() != 0 {
		t.Fatal(err, manager.Used())
	}
}

func TestWindowExactCoreDoesNotAllocate(t *testing.T) {
	var total windowExactSum
	var value, conversionValue float64
	// -d=checkptr=2 makes the standard-library unsafe bit conversions escape.
	// Measure the same three decodes and one encode so instrumentation does not
	// masquerade as accumulator storage. In normal and race builds both are 0.
	conversions := testing.AllocsPerRun(100, func() {
		for i := 0; i < 64; i++ {
			encoded := math.Float64bits(math.MaxFloat64)
			encoded ^= math.Float64bits(math.SmallestNonzeroFloat64)
			encoded ^= math.Float64bits(math.MaxFloat64)
			conversionValue = math.Float64frombits(encoded)
		}
	})
	allocs := testing.AllocsPerRun(100, func() {
		for i := 0; i < 64; i++ {
			total.addFloat(math.MaxFloat64, 1)
			total.addFloat(math.SmallestNonzeroFloat64, 1)
			total.addFloat(math.MaxFloat64, -1)
			value = total.result(int64(i + 1))
		}
		total = windowExactSum{}
	})
	if allocs != conversions {
		t.Fatalf("core allocated %g times beyond %g bit-conversion allocations", allocs-conversions, conversions)
	}
	if value != math.SmallestNonzeroFloat64 || conversionValue != value {
		t.Fatal(value, conversionValue)
	}
}

func TestWindowExactRejectsPositionOverflow(t *testing.T) {
	expr := WindowExpression{Function: WindowAvg, Frame: WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}}
	state, err := newRollingWindowState(expr, memory.TypeFloat64, memory.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	state.initialize()
	state.position = math.MaxInt64
	if err := state.advance(windowNumericCell{valid: true, f64: 1}); err == nil {
		t.Fatal("overflowing partition position accepted")
	}
	if state.position != math.MaxInt64 || state.validCount != 0 || state.exactSum.result(1) != 0 {
		t.Fatal("rejected row mutated state")
	}
}

func TestWindowExactRoundingBoundaries(t *testing.T) {
	cases := []struct {
		values []float64
		count  int64
		want   float64
	}{
		{[]float64{1, math.Ldexp(1, -53)}, 1, 1},
		{[]float64{math.Nextafter(1, 2), math.Ldexp(1, -53)}, 1, 1 + math.Ldexp(1, -51)},
		{[]float64{math.MaxFloat64, math.Ldexp(1, 970)}, 1, math.Inf(1)},
		{[]float64{math.MaxFloat64, math.Nextafter(math.Ldexp(1, 970), 0)}, 1, math.MaxFloat64},
		{[]float64{math.MaxFloat64, math.MaxFloat64}, 2, math.MaxFloat64},
		{[]float64{-math.MaxFloat64, -math.MaxFloat64}, 2, -math.MaxFloat64},
		{[]float64{math.SmallestNonzeroFloat64}, 2, 0},
		{[]float64{-math.SmallestNonzeroFloat64}, 2, math.Copysign(0, -1)},
		{[]float64{3 * math.SmallestNonzeroFloat64}, 2, 2 * math.SmallestNonzeroFloat64},
		{[]float64{math.Ldexp(1, -1022), math.Nextafter(math.Ldexp(1, -1022), 0)}, 2, math.Ldexp(1, -1022)},
	}
	for _, tc := range cases {
		var sum windowExactSum
		for _, value := range tc.values {
			sum.addFloat(value, 1)
		}
		if got := sum.result(tc.count); math.Float64bits(got) != math.Float64bits(tc.want) {
			t.Fatalf("values=%v /%d got=%g want=%g", tc.values, tc.count, got, tc.want)
		}
	}
}

func TestWindowExactCapacityAtMaximumCount(t *testing.T) {
	// Construct the exact total of MaxInt64 copies of MaxFloat64 without
	// iterating an impossible number of rows. Both halves use production adds.
	hi, lo := bits.Mul64(uint64(1)<<53-1, uint64(math.MaxInt64))
	for _, negative := range []bool{false, true} {
		var sum windowExactSum
		sum.addMantissa(lo, 2045, negative)
		sum.addMantissa(hi, 2109, negative)
		want := math.MaxFloat64
		if negative {
			want = -want
		}
		if got := sum.result(math.MaxInt64); got != want {
			t.Fatalf("maximum-count avg got=%g want=%g", got, want)
		}
		if got := sum.result(1); !math.IsInf(got, 0) || math.Signbit(got) != negative {
			t.Fatalf("maximum-count sum got=%g", got)
		}
		sum.addMantissa(lo, 2045, !negative)
		sum.addMantissa(hi, 2109, !negative)
		if math.Float64bits(sum.result(1)) != 0 {
			t.Fatal("maximum total did not reverse to +0")
		}
	}
}

func TestWindowExactCanceledAfterOutput(t *testing.T) {
	manager := memory.NewBudgetManager(8192)
	owner := exactBudgetWindow(t, manager)
	stream, err := newOrderedWindowStream(context.Background(), owner, owner.input, owner.output, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	out := memory.NewBatch()
	if ok, err := stream.Next(context.Background(), out); !ok || err != nil {
		stream.Close()
		t.Fatal(ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, err := stream.Next(ctx, out); ok || !errors.Is(err, context.Canceled) {
		stream.Close()
		t.Fatal(ok, err)
	}
	out.ClearColumns()
	if err := stream.Close(); err != nil || manager.Used() != 0 {
		t.Fatal(err, manager.Used())
	}
}
