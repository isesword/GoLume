package kernel

import (
	"gocolumn/aggregate"
	"gocolumn/memory"
	"math/bits"
	"testing"
)

// benchmarkMaskedSumState includes the scalar state boundary that a tight
// reduction-only loop misses. On amd64 the aggregate state borrow/copy uses
// legacy SSE instructions, so uncleared AVX upper lanes can dominate this
// otherwise allocation-free 64-row SUM/COUNT path.
func benchmarkMaskedSumState[T sumInteger](b *testing.B, typ memory.DataType, call func([]T, uint64) T, mask uint64) {
	vector, err := aggregate.NewNumericVector(1, typ, aggregate.Layout{Sum: true, CountRows: true, CountValues: true})
	if err != nil {
		b.Fatal(err)
	}
	var values [64]T
	for i := range values {
		values[i] = T(i * 137)
	}
	valid := uint64(bits.OnesCount64(mask))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var state aggregate.NumericSumCountState
		if !vector.BorrowSumCountState(0, &state) {
			b.Fatal("SUM/COUNT state borrow failed")
		}
		sum := T(state.SumBits()) + call(values[:], mask)
		state.Update(uint64(sum), 64, valid)
	}
	b.StopTimer()
	var state aggregate.NumericSumCountState
	vector.BorrowSumCountState(0, &state)
	maskedSumSink = state.SumBits()
}

func BenchmarkMaskedIntegerSumScalarState(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, m := range []struct {
			name string
			mask uint64
		}{{"dense", ^uint64(0)}, {"half", 0xaaaaaaaaaaaaaaaa}} {
			name := mode.String() + "/" + m.name + "/"
			b.Run(name+"int32", func(b *testing.B) { benchmarkMaskedSumState(b, memory.TypeInt32, set.SumInt32Masked, m.mask) })
			b.Run(name+"uint32", func(b *testing.B) { benchmarkMaskedSumState(b, memory.TypeUint32, set.SumUint32Masked, m.mask) })
			b.Run(name+"int64", func(b *testing.B) { benchmarkMaskedSumState(b, memory.TypeInt64, set.SumInt64Masked, m.mask) })
			b.Run(name+"uint64", func(b *testing.B) { benchmarkMaskedSumState(b, memory.TypeUint64, set.SumUint64Masked, m.mask) })
		}
	}
}
