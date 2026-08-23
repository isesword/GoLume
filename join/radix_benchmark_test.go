package join

import (
	"gocolumn/hashagg"
	"testing"
)

func BenchmarkFixedJoinProbe(b *testing.B) {
	const buildRows = 262144
	const probeRows = 1048576
	build := make([]hashagg.Key128, buildRows)
	for i := range build {
		build[i] = hashagg.Key128{A: uint64(i), B: uint64(i*17 + 11)}
	}
	probe := make([]hashagg.Key128, probeRows)
	for i := range probe {
		key := i % (buildRows * 2)
		probe[i] = hashagg.Key128{A: uint64(key), B: uint64(key*17 + 11)}
	}

	b.Run("single-table", func(b *testing.B) {
		table := NewPairTable(buildRows)
		if err := table.Build(build, nil); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := table.Probe(probe, nil, Inner, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("radix-256", func(b *testing.B) {
		table := NewRadixPairTable(8)
		if err := table.Build(build, nil); err != nil {
			b.Fatal(err)
		}
		var workspace RadixProbeWorkspace
		if _, err := table.ProbeWithWorkspace(probe, nil, Inner, nil, &workspace); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := table.ProbeWithWorkspace(probe, nil, Inner, nil, &workspace); err != nil {
				b.Fatal(err)
			}
		}
	})
}
