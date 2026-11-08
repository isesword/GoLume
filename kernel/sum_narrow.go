package kernel

import (
	"gocolumn/memory"
	"math/bits"
)

type narrowSumInteger interface{ int8 | int16 | uint8 | uint16 }

func scalarNarrowSumMasked[T narrowSumInteger](v []T, mask uint64) (sum int64) {
	mask = checkedSumMask(len(v), mask)
	if mask == memory.TailMask(len(v)) {
		for _, x := range v {
			sum += int64(x)
		}
		return
	}
	for mask != 0 {
		sum += int64(v[bits.TrailingZeros64(mask)])
		mask &= mask - 1
	}
	return
}

// SumInt8Masked sums selected elements after widening to int64, without overflow.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumInt8Masked(v []int8, mask uint64) int64 { return s.d.sumInt8Masked(v, mask) }

// SumInt16Masked sums selected elements after widening to int64, without overflow.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumInt16Masked(v []int16, mask uint64) int64 { return s.d.sumInt16Masked(v, mask) }

// SumUint8Masked sums selected elements after widening to int64, without overflow.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumUint8Masked(v []uint8, mask uint64) int64 { return s.d.sumUint8Masked(v, mask) }

// SumUint16Masked sums selected elements after widening to int64, without overflow.
// Bit i selects v[i]; bits beyond len(v) are ignored. Empty inputs return zero.
// It performs no allocations and panics if len(v) exceeds 64.
func (s Set) SumUint16Masked(v []uint16, mask uint64) int64 { return s.d.sumUint16Masked(v, mask) }
