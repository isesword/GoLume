package hashagg

import "gocolumn/internal/hashmix"

// Hash64 returns the engine's SplitMix64-derived integer hash.
func Hash64(x uint64) uint64 {
	return hashmix.Mix64(x)
}
