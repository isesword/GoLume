// Package hashmix owns the exact SplitMix64 constants and nullable pair
// normalization shared by scalar aggregation and architecture kernels.
package hashmix

import "math/bits"

const (
	// Gamma is the canonical SplitMix64 additive constant.
	Gamma = uint64(0x9e3779b97f4a7c15)
	// Mul1 is the canonical first SplitMix64 multiplicative constant.
	Mul1 = uint64(0xbf58476d1ce4e5b9)
	// Mul2 is the canonical second SplitMix64 multiplicative constant.
	Mul2 = uint64(0x94d049bb133111eb)
)

// Mix64 returns the repository's canonical SplitMix64-derived hash.
func Mix64(x uint64) uint64 {
	x += Gamma
	x = (x ^ (x >> 30)) * Mul1
	x = (x ^ (x >> 27)) * Mul2
	return x ^ (x >> 31)
}

// PairInput normalizes invalid components and combines a nullable key pair.
func PairInput(a, b uint64, validMask uint8) uint64 {
	validMask &= 3
	if validMask&1 == 0 {
		a = 0
	}
	if validMask&2 == 0 {
		b = 0
	}
	return PairValues(a, b) ^ uint64(validMask)*Gamma
}

// PairValues combines a fully valid pair without adding a validity tag.
func PairValues(a, b uint64) uint64 { return a ^ uint64(bits.RotateLeft64(b, 31)) }

// Pair returns the canonical hash of a normalized nullable key pair.
func Pair(a, b uint64, validMask uint8) uint64 {
	return Mix64(PairInput(a, b, validMask))
}
