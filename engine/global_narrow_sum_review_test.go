package engine

import (
	"context"
	"fmt"
	"gocolumn/aggregate"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math/bits"
	"testing"
)

// Exercise arbitrary prior Int64 state, including rollover, independently of
// the query oracle. Guard values outside the slice must never enter the sum.
func TestGlobalNarrowSumReviewPriorStateAndBounds(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		set := kernel.New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
			t.Run(fmt.Sprintf("%v/%v", mode, typ), func(t *testing.T) {
				a, b, c, d := make([]int8, 66), make([]int16, 66), make([]uint8, 66), make([]uint16, 66)
				for i := range a {
					a[i] = int8(i*83 - 128)
					b[i] = int16(i*19001 - 32768)
					c[i] = uint8(i*83 + 255)
					d[i] = uint16(i*19001 + 65535)
				}
				bound := globalAggregateLaneBinding{typ: typ, int8s: &memory.Int8Vector{Values: a[1:]}, int16s: &memory.Int16Vector{Values: b[1:]}, uint8s: &memory.Uint8Vector{Values: c[1:]}, uint16s: &memory.Uint16Vector{Values: d[1:]}}
				for n := 0; n <= 64; n++ {
					for _, mask := range []uint64{0, 1, 1 << 63, 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, ^uint64(0)} {
						for _, prior := range []uint64{0, 1 << 32, 1<<63 - 1, 1 << 63, ^uint64(0)} {
							want := prior
							selected := mask & memory.TailMask(n)
							for i := 0; i < n; i++ {
								if selected&(uint64(1)<<i) == 0 {
									continue
								}
								var value int64
								switch typ {
								case memory.TypeInt8:
									value = int64(a[i+1])
								case memory.TypeInt16:
									value = int64(b[i+1])
								case memory.TypeUint8:
									value = int64(c[i+1])
								case memory.TypeUint16:
									value = int64(d[i+1])
								}
								want += uint64(value)
							}
							owner, err := aggregate.NewNumericVector(1, typ, aggregate.Layout{Sum: true})
							if err != nil {
								t.Fatal(err)
							}
							var state aggregate.NumericSumCountState
							if !owner.BorrowSumCountState(0, &state) {
								t.Fatal("cannot borrow state")
							}
							state.Update(prior, 0, 1)
							if err := bound.consumeBatch(context.Background(), set, memory.Bitmap{mask}, n, uint64(bits.OnesCount64(selected)), state); err != nil {
								t.Fatal(err)
							}
							if got := state.SumBits(); got != want {
								t.Fatalf("n=%d mask=%x prior=%x got=%x want=%x", n, mask, prior, got, want)
							}
						}
					}
				}
			})
		}
	}
}
