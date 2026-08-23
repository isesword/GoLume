package join

import (
	"context"
	"gocolumn/hashagg"
	"gocolumn/scheduler"
	"sort"
	"strconv"
	"testing"
)

func TestParallelRadixPairMatchesSingleTable(t *testing.T) {
	build := []hashagg.Key128{{A: 1, B: 10}, {A: 2, B: 20}, {A: 2, B: 20}, {A: 4, B: 40}}
	buildValid := []uint8{3, 3, 3, 3}
	probe := []hashagg.Key128{{A: 2, B: 20}, {A: 3, B: 30}, {A: 4, B: 40}, {A: 9, B: 90}}
	probeValid := []uint8{3, 3, 3, 0}
	pool := scheduler.NewPool(4)
	defer pool.Close()

	for _, typ := range []Type{Inner, Left, Semi, Anti} {
		base := NewPairTable(len(build))
		if err := base.Build(build, buildValid); err != nil {
			t.Fatal(err)
		}
		var want []Pair
		ws, err := base.Probe(probe, probeValid, typ, func(p Pair) error { want = append(want, p); return nil })
		if err != nil {
			t.Fatal(err)
		}

		radix := NewRadixPairTable(3)
		if err := radix.BuildParallel(context.Background(), build, buildValid, 4, pool); err != nil {
			t.Fatal(err)
		}
		var got []Pair
		var workspace ParallelRadixProbeWorkspace
		gs, err := radix.ProbeParallelWithWorkspace(context.Background(), probe, probeValid, typ, func(p Pair) error { got = append(got, p); return nil }, 4, pool, &workspace)
		if err != nil {
			t.Fatal(err)
		}
		if ws != gs {
			t.Fatalf("type=%s summary got=%+v want=%+v", typ, gs, ws)
		}
		sortPairs := func(v []Pair) {
			sort.Slice(v, func(i, j int) bool {
				if v[i].ProbeRow != v[j].ProbeRow {
					return v[i].ProbeRow < v[j].ProbeRow
				}
				return v[i].BuildRow < v[j].BuildRow
			})
		}
		sortPairs(want)
		sortPairs(got)
		if len(got) != len(want) {
			t.Fatalf("type=%s pairs=%v want=%v", typ, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("type=%s pair[%d]=%+v want=%+v", typ, i, got[i], want[i])
			}
		}
	}
}

func BenchmarkParallelRadixFixedJoinProbe(b *testing.B) {
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
	for _, workers := range []int{2, 4, 8} {
		b.Run("workers-"+strconv.Itoa(workers), func(b *testing.B) {
			pool := scheduler.NewPool(workers)
			defer pool.Close()
			table := NewRadixPairTable(8)
			if err := table.BuildParallel(context.Background(), build, nil, workers, pool); err != nil {
				b.Fatal(err)
			}
			var workspace ParallelRadixProbeWorkspace
			if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, nil, workers, pool, &workspace); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, nil, workers, pool, &workspace); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFixedJoinProbeWithPairs(b *testing.B) {
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
		pairs := make([]Pair, 0, probeRows/2)
		emit := func(p Pair) error { pairs = append(pairs, p); return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pairs = pairs[:0]
			if _, err := table.Probe(probe, nil, Inner, emit); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, workers := range []int{4, 8} {
		b.Run("parallel-radix-"+strconv.Itoa(workers), func(b *testing.B) {
			pool := scheduler.NewPool(workers)
			defer pool.Close()
			table := NewRadixPairTable(8)
			if err := table.BuildParallel(context.Background(), build, nil, workers, pool); err != nil {
				b.Fatal(err)
			}
			pairs := make([]Pair, 0, probeRows/2)
			emit := func(p Pair) error { pairs = append(pairs, p); return nil }
			var workspace ParallelRadixProbeWorkspace
			if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, emit, workers, pool, &workspace); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pairs = pairs[:0]
				if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, emit, workers, pool, &workspace); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLargeFixedJoinProbeWithPairs(b *testing.B) {
	const buildRows = 1 << 20
	const probeRows = 1 << 22
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
		pairs := make([]Pair, 0, probeRows/2)
		emit := func(p Pair) error { pairs = append(pairs, p); return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pairs = pairs[:0]
			if _, err := table.Probe(probe, nil, Inner, emit); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, workers := range []int{4, 8} {
		b.Run("parallel-radix-"+strconv.Itoa(workers), func(b *testing.B) {
			pool := scheduler.NewPool(workers)
			defer pool.Close()
			table := NewRadixPairTable(8)
			if err := table.BuildParallel(context.Background(), build, nil, workers, pool); err != nil {
				b.Fatal(err)
			}
			pairs := make([]Pair, 0, probeRows/2)
			emit := func(p Pair) error { pairs = append(pairs, p); return nil }
			var workspace ParallelRadixProbeWorkspace
			if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, emit, workers, pool, &workspace); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pairs = pairs[:0]
				if _, err := table.ProbeParallelWithWorkspace(context.Background(), probe, nil, Inner, emit, workers, pool, &workspace); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
